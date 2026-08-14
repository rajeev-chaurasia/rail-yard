package p5

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const actorHeader = "X-Rail-Yard-Actor"

type Client struct {
	baseURL       string
	prometheusURL string
	actor         string
	http          *http.Client
}

func NewClient(baseURL, prometheusURL, actor string, timeout time.Duration) (*Client, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	prometheusURL = strings.TrimRight(prometheusURL, "/")
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("parse Rail Yard URL: %w", err)
	}
	if _, err := url.ParseRequestURI(prometheusURL); err != nil {
		return nil, fmt.Errorf("parse Prometheus URL: %w", err)
	}
	if strings.TrimSpace(actor) == "" {
		return nil, fmt.Errorf("operator actor is required")
	}
	return &Client{
		baseURL:       baseURL,
		prometheusURL: prometheusURL,
		actor:         actor,
		http:          &http.Client{Timeout: timeout},
	}, nil
}

func (c *Client) Ready(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodGet, c.baseURL+"/health/ready", "", nil, nil, http.StatusOK)
}

func (c *Client) SubmitWorkflow(
	ctx context.Context,
	key string,
	request WorkflowRequest,
) (WorkflowResponse, error) {
	var response WorkflowResponse
	err := c.doControlJSON(
		ctx,
		http.MethodPost,
		"/v1/operations/dags",
		key,
		request,
		&response,
		http.StatusCreated,
		http.StatusOK,
	)
	return response, err
}

func (c *Client) SubmitJob(
	ctx context.Context,
	key string,
	request SubmitJobRequest,
) (SubmitJobResponse, error) {
	var response SubmitJobResponse
	err := c.doControlJSON(
		ctx,
		http.MethodPost,
		"/v1/operations/jobs",
		key,
		request,
		&response,
		http.StatusCreated,
		http.StatusOK,
	)
	return response, err
}

func (c *Client) GetJob(ctx context.Context, jobID string) (Job, error) {
	var job Job
	err := c.doJSON(
		ctx,
		http.MethodGet,
		c.baseURL+"/v1/operations/jobs/"+url.PathEscape(jobID),
		"",
		nil,
		&job,
		http.StatusOK,
	)
	return job, err
}

func (c *Client) ListDeadLetters(ctx context.Context) (DeadLetterList, error) {
	var response DeadLetterList
	err := c.doJSON(
		ctx,
		http.MethodGet,
		c.baseURL+"/ops/api/dead-letters",
		"",
		nil,
		&response,
		http.StatusOK,
	)
	if err != nil {
		return DeadLetterList{}, fmt.Errorf(
			"required P5 dashboard mount GET /ops/api/dead-letters unavailable: %w",
			err,
		)
	}
	return response, err
}

func (c *Client) Workers(ctx context.Context) (WorkerHealthResponse, error) {
	var response WorkerHealthResponse
	err := c.doJSON(
		ctx,
		http.MethodGet,
		c.baseURL+"/v1/operations/workers",
		"",
		nil,
		&response,
		http.StatusOK,
	)
	if err != nil {
		return WorkerHealthResponse{}, fmt.Errorf(
			"required P5 operations mount GET /v1/operations/workers unavailable: %w",
			err,
		)
	}
	return response, nil
}

func (c *Client) ForceDeadLetter(
	ctx context.Context,
	key string,
	jobID string,
	reason string,
) (ActionReceipt, error) {
	var response ActionReceipt
	err := c.doControlJSON(
		ctx,
		http.MethodPost,
		"/v1/operations/jobs/"+url.PathEscape(jobID)+"/force",
		key,
		ForceJobRequest{Action: "dead_letter", Reason: reason},
		&response,
		http.StatusOK,
	)
	return response, err
}

func (c *Client) Redrive(
	ctx context.Context,
	key string,
	jobID string,
) (RedriveResponse, error) {
	var response RedriveResponse
	err := c.doControlJSON(
		ctx,
		http.MethodPost,
		"/v1/operations/dead-letters/"+url.PathEscape(jobID)+"/redrive",
		key,
		struct{}{},
		&response,
		http.StatusCreated,
		http.StatusOK,
	)
	return response, err
}

func (c *Client) RecordOperatorAction(
	ctx context.Context,
	key string,
	action OperatorActionRequest,
) (AuditEvent, error) {
	var response OperatorActionResponse
	err := c.doControlJSON(
		ctx,
		http.MethodPost,
		"/v1/operations/operator-actions",
		key,
		action,
		&response,
		http.StatusCreated,
		http.StatusOK,
	)
	if err != nil {
		return AuditEvent{}, fmt.Errorf(
			"required P5 hook POST /v1/operations/operator-actions unavailable: %w",
			err,
		)
	}
	return response.Event, nil
}

func (c *Client) AuditEvents(
	ctx context.Context,
	since time.Time,
) (AuditEventList, error) {
	query := url.Values{}
	query.Set("since", since.UTC().Format(time.RFC3339Nano))
	query.Set("actor", c.actor)
	var response AuditEventList
	err := c.doJSON(
		ctx,
		http.MethodGet,
		c.baseURL+"/v1/operations/audit-events?"+query.Encode(),
		"",
		nil,
		&response,
		http.StatusOK,
	)
	if err != nil {
		return AuditEventList{}, fmt.Errorf(
			"required P5 hook GET /v1/operations/audit-events unavailable: %w",
			err,
		)
	}
	return response, nil
}

func (c *Client) AlertState(ctx context.Context, name string) (string, error) {
	var response prometheusAlertsResponse
	err := c.doJSON(
		ctx,
		http.MethodGet,
		c.prometheusURL+"/api/v1/alerts",
		"",
		nil,
		&response,
		http.StatusOK,
	)
	if err != nil {
		return "", err
	}
	if response.Status != "success" {
		return "", fmt.Errorf("prometheus alerts API status is %q", response.Status)
	}
	for _, alert := range response.Data.Alerts {
		if alert.Labels["alertname"] == name {
			return alert.State, nil
		}
	}
	return "inactive", nil
}

func (c *Client) AlertRules(ctx context.Context) (map[string]bool, error) {
	var response prometheusRulesResponse
	err := c.doJSON(
		ctx,
		http.MethodGet,
		c.prometheusURL+"/api/v1/rules?type=alert",
		"",
		nil,
		&response,
		http.StatusOK,
	)
	if err != nil {
		return nil, err
	}
	if response.Status != "success" {
		return nil, fmt.Errorf("prometheus rules API status is %q", response.Status)
	}
	rules := make(map[string]bool)
	for _, group := range response.Data.Groups {
		for _, rule := range group.Rules {
			if rule.Type == "alerting" {
				rules[rule.Name] = true
			}
		}
	}
	return rules, nil
}

func (c *Client) doControlJSON(
	ctx context.Context,
	method string,
	path string,
	idempotencyKey string,
	requestBody any,
	responseBody any,
	statuses ...int,
) error {
	return c.doJSON(
		ctx,
		method,
		c.baseURL+path,
		idempotencyKey,
		requestBody,
		responseBody,
		statuses...,
	)
}

func (c *Client) doJSON(
	ctx context.Context,
	method string,
	requestURL string,
	idempotencyKey string,
	requestBody any,
	responseBody any,
	statuses ...int,
) error {
	var body io.Reader
	if requestBody != nil {
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode %s %s: %w", method, requestURL, err)
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return fmt.Errorf("create %s %s: %w", method, requestURL, err)
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
		request.Header.Set(actorHeader, c.actor)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, requestURL, err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if !containsStatus(statuses, response.StatusCode) {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return fmt.Errorf(
			"%s %s returned %d: %s",
			method,
			requestURL,
			response.StatusCode,
			strings.TrimSpace(string(message)),
		)
	}
	if responseBody == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(responseBody); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, requestURL, err)
	}
	return nil
}

func containsStatus(statuses []int, status int) bool {
	for _, expected := range statuses {
		if expected == status {
			return true
		}
	}
	return false
}
