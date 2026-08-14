package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

const (
	defaultHTTPTimeout = 30 * time.Second
	maxResponseBytes   = 1 << 20
)

type Protocol interface {
	Register(context.Context, api.RegisterWorkerRequest) (api.RegisterWorkerResponse, error)
	AcquireLeases(context.Context, string, api.AcquireLeasesRequest) (api.AcquireLeasesResponse, error)
	Heartbeat(context.Context, string, api.HeartbeatRequest) (api.HeartbeatResponse, error)
	StartAttempt(context.Context, string, api.StartAttemptRequest) error
	CompleteAttempt(context.Context, string, api.CompleteAttemptRequest) (domain.CompletionReceipt, error)
}

type BatchAttemptStartProtocol interface {
	StartAttempts(
		context.Context,
		string,
		api.StartAttemptsRequest,
	) (api.StartAttemptsResponse, error)
}

type BatchCompletionProtocol interface {
	CompleteAttempts(
		context.Context,
		string,
		api.CompleteAttemptsRequest,
	) (api.CompleteAttemptsResponse, error)
}

type TransportRetryPolicy struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func DefaultTransportRetryPolicy() TransportRetryPolicy {
	return TransportRetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     time.Second,
	}
}

type RetryWaitFunc func(context.Context, time.Duration) error

type HTTPClientOption func(*HTTPClient)

func WithTransportRetry(policy TransportRetryPolicy, wait RetryWaitFunc) HTTPClientOption {
	return func(client *HTTPClient) {
		client.retry = policy
		if wait != nil {
			client.wait = wait
		}
	}
}

type HTTPClient struct {
	baseURL string
	client  *http.Client
	retry   TransportRetryPolicy
	wait    RetryWaitFunc
}

func NewHTTPClient(baseURL string, client *http.Client, options ...HTTPClientOption) (*HTTPClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse worker API base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("worker API base URL must use http or https")
	}
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("worker API base URL must include a host and no query or fragment")
	}
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}

	result := &HTTPClient{
		baseURL: strings.TrimRight(parsed.String(), "/"),
		client:  client,
		retry:   DefaultTransportRetryPolicy(),
		wait:    waitForRetry,
	}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	if err := result.retry.Validate(); err != nil {
		return nil, err
	}
	return result, nil
}

func (p TransportRetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("transport retry max attempts must be positive")
	}
	if p.InitialBackoff < 0 {
		return errors.New("transport retry initial backoff must not be negative")
	}
	if p.MaxBackoff < p.InitialBackoff {
		return errors.New("transport retry max backoff must be at least the initial backoff")
	}
	return nil
}

func (c *HTTPClient) Register(
	ctx context.Context,
	request api.RegisterWorkerRequest,
) (api.RegisterWorkerResponse, error) {
	var response api.RegisterWorkerResponse
	err := c.doJSON(ctx, http.MethodPost, "/v1/workers/register", request, &response)
	return response, err
}

func (c *HTTPClient) AcquireLeases(
	ctx context.Context,
	workerID string,
	request api.AcquireLeasesRequest,
) (api.AcquireLeasesResponse, error) {
	var response api.AcquireLeasesResponse
	err := c.doJSON(ctx, http.MethodPost, workerPath(workerID, "leases/acquire"), request, &response)
	return response, err
}

func (c *HTTPClient) Heartbeat(
	ctx context.Context,
	workerID string,
	request api.HeartbeatRequest,
) (api.HeartbeatResponse, error) {
	var response api.HeartbeatResponse
	err := c.doJSON(ctx, http.MethodPost, workerPath(workerID, "heartbeats"), request, &response)
	return response, err
}

func (c *HTTPClient) StartAttempt(
	ctx context.Context,
	workerID string,
	request api.StartAttemptRequest,
) error {
	return c.doJSON(ctx, http.MethodPost, workerPath(workerID, "attempts/start"), request, nil)
}

func (c *HTTPClient) StartAttempts(
	ctx context.Context,
	workerID string,
	request api.StartAttemptsRequest,
) (api.StartAttemptsResponse, error) {
	var response api.StartAttemptsResponse
	err := c.doJSON(
		ctx,
		http.MethodPost,
		workerPath(workerID, "attempts/start-batch"),
		request,
		&response,
	)
	if err == nil {
		return response, nil
	}
	if !batchRouteUnavailable(err) {
		return api.StartAttemptsResponse{}, err
	}

	response.Results = make([]api.StartResult, len(request.Leases))
	for index, ref := range request.Leases {
		result := &response.Results[index]
		result.JobID = ref.JobID
		startErr := c.StartAttempt(
			ctx,
			workerID,
			api.StartAttemptRequest{LeaseRef: ref},
		)
		if startErr == nil {
			result.Started = true
			continue
		}
		errorResponse, ok := apiErrorResponse(startErr)
		if !ok {
			return api.StartAttemptsResponse{}, startErr
		}
		result.Error = errorResponse
	}
	return response, nil
}

func (c *HTTPClient) CompleteAttempt(
	ctx context.Context,
	workerID string,
	request api.CompleteAttemptRequest,
) (domain.CompletionReceipt, error) {
	var response domain.CompletionReceipt
	err := c.doJSON(ctx, http.MethodPost, workerPath(workerID, "attempts/complete"), request, &response)
	return response, err
}

func (c *HTTPClient) CompleteAttempts(
	ctx context.Context,
	workerID string,
	request api.CompleteAttemptsRequest,
) (api.CompleteAttemptsResponse, error) {
	var response api.CompleteAttemptsResponse
	err := c.doJSON(
		ctx,
		http.MethodPost,
		workerPath(workerID, "attempts/complete-batch"),
		request,
		&response,
	)
	if err == nil {
		return response, nil
	}
	if !batchRouteUnavailable(err) {
		return api.CompleteAttemptsResponse{}, err
	}

	response.Results = make([]api.CompletionResult, len(request.Completions))
	for index, completion := range request.Completions {
		result := &response.Results[index]
		result.JobID = completion.JobID
		receipt, completionErr := c.CompleteAttempt(
			ctx,
			workerID,
			api.CompleteAttemptRequest{Completion: completion},
		)
		if completionErr == nil {
			result.Receipt = &receipt
			continue
		}
		errorResponse, ok := apiErrorResponse(completionErr)
		if !ok {
			return api.CompleteAttemptsResponse{}, completionErr
		}
		result.Error = errorResponse
	}
	return response, nil
}

func (c *HTTPClient) doJSON(ctx context.Context, method string, path string, request any, response any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode %s %s request: %w", method, path, err)
	}

	backoff := c.retry.InitialBackoff
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		httpRequest, requestErr := http.NewRequestWithContext(
			ctx,
			method,
			c.baseURL+path,
			bytes.NewReader(body),
		)
		if requestErr != nil {
			return fmt.Errorf("build %s %s request: %w", method, path, requestErr)
		}
		httpRequest.Header.Set("Accept", "application/json")
		httpRequest.Header.Set("Content-Type", "application/json")

		httpResponse, requestErr := c.client.Do(httpRequest)
		if requestErr == nil {
			decodeErr := decodeHTTPResponse(method, path, httpResponse, response)
			var readErr *responseReadError
			if !errors.As(decodeErr, &readErr) {
				return decodeErr
			}
			requestErr = readErr
			httpResponse = nil
		}
		if httpResponse != nil && httpResponse.Body != nil {
			_ = httpResponse.Body.Close()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if attempt == c.retry.MaxAttempts {
			return &TransportError{
				Method:   method,
				Path:     path,
				Attempts: attempt,
				Err:      requestErr,
			}
		}
		if err := c.wait(ctx, backoff); err != nil {
			return err
		}
		backoff = nextBackoff(backoff, c.retry.MaxBackoff)
	}

	return errors.New("worker transport retry loop exited unexpectedly")
}

type responseReadError struct {
	err error
}

func (e *responseReadError) Error() string {
	return e.err.Error()
}

func (e *responseReadError) Unwrap() error {
	return e.err
}

type TransportError struct {
	Method   string
	Path     string
	Attempts int
	Err      error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("%s %s transport failed after %d attempts: %v", e.Method, e.Path, e.Attempts, e.Err)
}

func (e *TransportError) Unwrap() error {
	return e.Err
}

func decodeHTTPResponse(method string, path string, response *http.Response, destination any) error {
	defer func() {
		_ = response.Body.Close()
	}()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return &responseReadError{err: fmt.Errorf("read %s %s response: %w", method, path, err)}
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("%s %s response exceeds %d bytes", method, path, maxResponseBytes)
	}

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		apiErr := parseAPIError(response, body)
		apiErr.Method = method
		apiErr.Path = path
		return apiErr
	}
	if destination == nil || len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, destination); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RetryAfter time.Duration
	Method     string
	Path       string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("worker API returned HTTP %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("worker API returned HTTP %d (%s): %s", e.StatusCode, e.Code, e.Message)
}

func (e *APIError) Is(target error) bool {
	switch e.Code {
	case "stale_lease":
		return target == domain.ErrStaleLease
	case "not_found":
		return target == domain.ErrNotFound
	case "queue_full":
		return target == domain.ErrQueueFull
	case "idempotency_conflict":
		return target == domain.ErrIdempotencyConflict
	case "cycle_detected":
		return target == domain.ErrCycleDetected
	default:
		return false
	}
}

func parseAPIError(response *http.Response, body []byte) *APIError {
	var payload api.ErrorResponse
	if len(bytes.TrimSpace(body)) > 0 {
		_ = json.Unmarshal(body, &payload)
	}
	if payload.Message == "" {
		payload.Message = strings.TrimSpace(string(body))
	}
	if payload.Message == "" {
		payload.Message = http.StatusText(response.StatusCode)
	}

	retryAfter := time.Duration(payload.RetryAfter) * time.Second
	if retryAfter == 0 {
		if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
			retryAfter = time.Duration(seconds) * time.Second
		}
	}
	return &APIError{
		StatusCode: response.StatusCode,
		Code:       payload.Code,
		Message:    payload.Message,
		RetryAfter: retryAfter,
	}
}

func batchRouteUnavailable(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) &&
		!workerRegistrationMissing(err) &&
		(apiErr.StatusCode == http.StatusNotFound ||
			apiErr.StatusCode == http.StatusMethodNotAllowed)
}

func workerRegistrationMissing(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) ||
		apiErr.StatusCode != http.StatusNotFound ||
		apiErr.Code != "not_found" ||
		apiErr.Message != "worker is not registered" {
		return false
	}
	return strings.HasPrefix(apiErr.Path, "/v1/workers/") &&
		apiErr.Path != "/v1/workers/register"
}

func apiErrorResponse(err error) (*api.ErrorResponse, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return nil, false
	}
	return &api.ErrorResponse{
		Code:       apiErr.Code,
		Message:    apiErr.Message,
		RetryAfter: int(apiErr.RetryAfter / time.Second),
	}, true
}

func workerPath(workerID string, suffix string) string {
	return "/v1/workers/" + url.PathEscape(workerID) + "/" + suffix
}

func nextBackoff(current time.Duration, maximum time.Duration) time.Duration {
	if current <= 0 || current >= maximum {
		return maximum
	}
	if current > maximum-current {
		return maximum
	}
	return min(current*2, maximum)
}

func waitForRetry(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
