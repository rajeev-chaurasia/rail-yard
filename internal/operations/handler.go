package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

const (
	defaultMaxBodyBytes       = 1 << 20
	defaultHistoryPageSize    = 100
	defaultMaxHistoryPageSize = 500
	defaultActorHeader        = "X-Rail-Yard-Actor"
	maxHeaderValueBytes       = 256
	maxIdentifierBytes        = 128
	maxReasonBytes            = 1024
)

type Config struct {
	MaxBodyBytes       int64
	HistoryPageSize    int
	MaxHistoryPageSize int
	RequestTimeout     time.Duration
	ActorHeader        string
	Now                func() time.Time
}

func DefaultConfig() Config {
	return Config{
		MaxBodyBytes:       defaultMaxBodyBytes,
		HistoryPageSize:    defaultHistoryPageSize,
		MaxHistoryPageSize: defaultMaxHistoryPageSize,
		RequestTimeout:     5 * time.Second,
		ActorHeader:        defaultActorHeader,
		Now:                time.Now,
	}
}

type Handler struct {
	repositories Repositories
	config       Config
	mux          *http.ServeMux
}

func New(repositories Repositories, config Config) (*Handler, error) {
	if err := validateRepositories(repositories); err != nil {
		return nil, fmt.Errorf("create operations handler: %w", err)
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create operations handler: %w", err)
	}

	handler := &Handler{
		repositories: repositories,
		config:       normalized,
		mux:          http.NewServeMux(),
	}
	handler.registerRoutes()
	return handler, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), h.config.RequestTimeout)
	defer cancel()
	h.mux.ServeHTTP(w, r.WithContext(ctx))
}

func (h *Handler) Mount(mux *http.ServeMux) {
	mux.Handle("/v1/operations/", h)
}

func (h *Handler) registerRoutes() {
	h.mux.HandleFunc("/v1/operations/jobs", h.handleSubmitJob)
	h.mux.HandleFunc("/v1/operations/dags", h.handleSubmitDAG)
	h.mux.HandleFunc("/v1/operations/jobs/{job_id}", h.handleGetJob)
	h.mux.HandleFunc("/v1/operations/jobs/{job_id}/history", h.handleGetJobHistory)
	h.mux.HandleFunc("/v1/operations/jobs/{job_id}/cancel", h.handleCancelJob)
	h.mux.HandleFunc(
		"/v1/operations/dead-letters/{job_id}/redrive",
		h.handleRedriveDeadLetter,
	)
	h.mux.HandleFunc(
		"/v1/operations/tenants/{tenant_id}/queues",
		h.handleListTenantQueueDepth,
	)
	h.mux.HandleFunc("/v1/operations/workers", h.handleListWorkerHealth)
	h.mux.HandleFunc("/v1/operations/dags/{dag_id}", h.handleGetDAG)
	h.mux.HandleFunc("/v1/operations/jobs/{job_id}/force", h.handleForceJob)
	h.mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found", 0)
	})
}

func (h *Handler) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	key, requestErr := requiredHeader(r, "Idempotency-Key")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	var request api.SubmitJobRequest
	if requestErr := h.decodeJSON(w, r, &request); requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	request = normalizeJobRequest(request)
	digest, err := stableDigest(request)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	response, err := h.repositories.JobSubmitter.SubmitJob(r.Context(), SubmitJobCommand{
		Request:        request,
		IdempotencyKey: key,
		RequestDigest:  digest,
		RequestedAt:    h.config.Now().UTC(),
	})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	status := http.StatusCreated
	if response.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, response)
}

func (h *Handler) handleSubmitDAG(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	key, requestErr := requiredHeader(r, "Idempotency-Key")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	var request api.SubmitWorkflowRequest
	if requestErr := h.decodeJSON(w, r, &request); requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	request = normalizeDAGRequest(request)
	digest, err := stableDigest(request)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	response, err := h.repositories.DAGSubmitter.SubmitDAG(r.Context(), SubmitDAGCommand{
		Request:        request,
		IdempotencyKey: key,
		RequestDigest:  digest,
		RequestedAt:    h.config.Now().UTC(),
	})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	status := http.StatusCreated
	if response.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, response)
}

func (h *Handler) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	jobID, requestErr := pathIdentifier(r, "job_id")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	job, err := h.repositories.JobReader.GetJob(r.Context(), jobID)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h *Handler) handleGetJobHistory(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	jobID, requestErr := pathIdentifier(r, "job_id")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	query, requestErr := h.historyQuery(r)
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	page, err := h.repositories.JobHistoryReader.GetJobHistory(r.Context(), jobID, query)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	if page.Events == nil {
		page.Events = []JobEvent{}
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Handler) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	jobID, actor, key, ok := h.mutationHeaders(w, r)
	if !ok {
		return
	}
	var request CancelJobRequest
	if requestErr := h.decodeJSON(w, r, &request); requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	if requestErr := validateReason(request.Reason); requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	digest, err := stableDigest(struct {
		JobID string `json:"job_id"`
		Actor string `json:"actor"`
		CancelJobRequest
	}{JobID: jobID, Actor: actor, CancelJobRequest: request})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	receipt, err := h.repositories.JobCanceller.CancelJob(r.Context(), CancelJobCommand{
		JobID:          jobID,
		Actor:          actor,
		Reason:         request.Reason,
		IdempotencyKey: key,
		RequestDigest:  digest,
		RequestedAt:    h.config.Now().UTC(),
	})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (h *Handler) handleRedriveDeadLetter(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	jobID, actor, key, ok := h.mutationHeaders(w, r)
	if !ok {
		return
	}
	digest, err := stableDigest(struct {
		JobID string `json:"job_id"`
		Actor string `json:"actor"`
	}{JobID: jobID, Actor: actor})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	response, err := h.repositories.DeadLetterRedriver.RedriveDeadLetter(
		r.Context(),
		RedriveCommand{
			JobID:          jobID,
			Actor:          actor,
			IdempotencyKey: key,
			RequestDigest:  digest,
			RequestedAt:    h.config.Now().UTC(),
		},
	)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	status := http.StatusCreated
	if response.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, response)
}

func (h *Handler) handleListTenantQueueDepth(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	tenantID, requestErr := pathIdentifier(r, "tenant_id")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	queues, err := h.repositories.QueueDepthReader.ListTenantQueueDepth(r.Context(), tenantID)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	if queues == nil {
		queues = []QueueDepth{}
	}
	writeJSON(w, http.StatusOK, QueueDepthResponse{Queues: queues})
}

func (h *Handler) handleListWorkerHealth(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	workers, err := h.repositories.WorkerHealthReader.ListWorkerHealth(r.Context())
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	if workers == nil {
		workers = []WorkerHealth{}
	}
	writeJSON(w, http.StatusOK, WorkerHealthResponse{Workers: workers})
}

func (h *Handler) handleGetDAG(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	dagID, requestErr := pathIdentifier(r, "dag_id")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	dag, err := h.repositories.DAGReader.GetDAG(r.Context(), dagID)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	if dag.Jobs == nil {
		dag.Jobs = []domain.Job{}
	}
	if dag.Edges == nil {
		dag.Edges = []DAGEdge{}
	}
	writeJSON(w, http.StatusOK, dag)
}

func (h *Handler) handleForceJob(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	jobID, actor, key, ok := h.mutationHeaders(w, r)
	if !ok {
		return
	}
	var request ForceJobRequest
	if requestErr := h.decodeJSON(w, r, &request); requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	if !request.Action.Valid() {
		writeRequestError(w, &RequestError{
			Message: "action must be release, fail, or dead_letter",
		})
		return
	}
	if requestErr := validateReason(request.Reason); requestErr != nil {
		writeRequestError(w, requestErr)
		return
	}
	digest, err := stableDigest(struct {
		JobID string `json:"job_id"`
		Actor string `json:"actor"`
		ForceJobRequest
	}{JobID: jobID, Actor: actor, ForceJobRequest: request})
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	receipt, err := h.repositories.ForceJobController.ForceJobAction(
		r.Context(),
		ForceJobCommand{
			JobID:          jobID,
			Action:         request.Action,
			Actor:          actor,
			Reason:         request.Reason,
			IdempotencyKey: key,
			RequestDigest:  digest,
			RequestedAt:    h.config.Now().UTC(),
		},
	)
	if err != nil {
		writeRepositoryError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (h *Handler) mutationHeaders(
	w http.ResponseWriter,
	r *http.Request,
) (string, string, string, bool) {
	jobID, requestErr := pathIdentifier(r, "job_id")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return "", "", "", false
	}
	actor, requestErr := requiredHeader(r, h.config.ActorHeader)
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return "", "", "", false
	}
	key, requestErr := requiredHeader(r, "Idempotency-Key")
	if requestErr != nil {
		writeRequestError(w, requestErr)
		return "", "", "", false
	}
	return jobID, actor, key, true
}

func (h *Handler) historyQuery(r *http.Request) (HistoryQuery, *RequestError) {
	query := HistoryQuery{Limit: h.config.HistoryPageSize}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > h.config.MaxHistoryPageSize {
			return HistoryQuery{}, &RequestError{Message: fmt.Sprintf(
				"limit must be between 1 and %d",
				h.config.MaxHistoryPageSize,
			)}
		}
		query.Limit = value
	}
	if raw := r.URL.Query().Get("before_seq"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1 {
			return HistoryQuery{}, &RequestError{Message: "before_seq must be a positive integer"}
		}
		query.BeforeSeq = value
	}
	return query, nil
}

func (h *Handler) decodeJSON(
	w http.ResponseWriter,
	r *http.Request,
	target any,
) *RequestError {
	body := http.MaxBytesReader(w, r.Body, h.config.MaxBodyBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return decodeError(err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err != nil {
			return decodeError(err)
		}
		return &RequestError{Message: "request body must contain exactly one JSON value"}
	}
	return nil
}

func normalizeJobRequest(request api.SubmitJobRequest) api.SubmitJobRequest {
	request.Job = normalizeJobSpec(request.Job)
	return request
}

func normalizeDAGRequest(request api.SubmitWorkflowRequest) api.SubmitWorkflowRequest {
	if request.TenantID == "" {
		request.TenantID = "default"
	}
	for index := range request.Nodes {
		if request.Nodes[index].Job.TenantID == "" {
			request.Nodes[index].Job.TenantID = request.TenantID
		}
		request.Nodes[index].Job = normalizeJobSpec(request.Nodes[index].Job)
	}
	sort.Slice(request.Nodes, func(left, right int) bool {
		return request.Nodes[left].Key < request.Nodes[right].Key
	})
	return request
}

func normalizeJobSpec(spec domain.JobSpec) domain.JobSpec {
	spec = spec.Normalize()
	if !spec.AvailableAt.IsZero() {
		spec.AvailableAt = spec.AvailableAt.UTC()
	}
	sort.Strings(spec.DependsOn)
	return spec
}

func stableDigest(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode request digest: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func pathIdentifier(r *http.Request, name string) (string, *RequestError) {
	value := r.PathValue(name)
	if requestErr := validateIdentifier(name, value); requestErr != nil {
		return "", requestErr
	}
	return value, nil
}

func validateIdentifier(name, value string) *RequestError {
	if strings.TrimSpace(value) == "" {
		return &RequestError{Message: name + " is required"}
	}
	if strings.TrimSpace(value) != value {
		return &RequestError{Message: name + " must not have surrounding whitespace"}
	}
	if len(value) > maxIdentifierBytes {
		return &RequestError{Message: fmt.Sprintf("%s exceeds %d bytes", name, maxIdentifierBytes)}
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return &RequestError{Message: name + " contains a control character"}
		}
	}
	return nil
}

func validateReason(reason string) *RequestError {
	if strings.TrimSpace(reason) == "" {
		return &RequestError{Message: "reason is required"}
	}
	if strings.TrimSpace(reason) != reason {
		return &RequestError{Message: "reason must not have surrounding whitespace"}
	}
	if len(reason) > maxReasonBytes {
		return &RequestError{Message: fmt.Sprintf("reason exceeds %d bytes", maxReasonBytes)}
	}
	return nil
}

func requiredHeader(r *http.Request, name string) (string, *RequestError) {
	values := r.Header.Values(name)
	if len(values) != 1 {
		return "", &RequestError{Message: fmt.Sprintf("exactly one %s header is required", name)}
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return "", &RequestError{Message: name + " must not be empty"}
	}
	if len(value) > maxHeaderValueBytes {
		return "", &RequestError{Message: fmt.Sprintf("%s exceeds %d bytes", name, maxHeaderValueBytes)}
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return "", &RequestError{
				Message: name + " must contain printable ASCII without spaces",
			}
		}
	}
	return value, nil
}

func decodeError(err error) *RequestError {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		return &RequestError{Message: "request body exceeds the configured limit"}
	}
	if errors.Is(err, io.EOF) {
		return &RequestError{Message: "request body is required"}
	}
	if strings.HasPrefix(err.Error(), "json: unknown field ") {
		return &RequestError{Message: "request body contains an unknown field"}
	}
	return &RequestError{Message: "request body contains malformed JSON"}
}

func validateRepositories(repositories Repositories) error {
	required := []struct {
		name       string
		repository any
	}{
		{"job submitter", repositories.JobSubmitter},
		{"DAG submitter", repositories.DAGSubmitter},
		{"job reader", repositories.JobReader},
		{"job history reader", repositories.JobHistoryReader},
		{"job canceller", repositories.JobCanceller},
		{"dead-letter redriver", repositories.DeadLetterRedriver},
		{"queue depth reader", repositories.QueueDepthReader},
		{"worker health reader", repositories.WorkerHealthReader},
		{"DAG reader", repositories.DAGReader},
		{"force job controller", repositories.ForceJobController},
	}
	for _, value := range required {
		if value.repository == nil {
			return errors.New(value.name + " is required")
		}
	}
	return nil
}

func normalizeConfig(config Config) (Config, error) {
	defaults := DefaultConfig()
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if config.HistoryPageSize == 0 {
		config.HistoryPageSize = defaults.HistoryPageSize
	}
	if config.MaxHistoryPageSize == 0 {
		config.MaxHistoryPageSize = defaults.MaxHistoryPageSize
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = defaults.RequestTimeout
	}
	if config.ActorHeader == "" {
		config.ActorHeader = defaults.ActorHeader
	}
	if config.Now == nil {
		config.Now = defaults.Now
	}
	if config.MaxBodyBytes < 1 {
		return Config{}, errors.New("max body bytes must be positive")
	}
	if config.HistoryPageSize < 1 || config.MaxHistoryPageSize < config.HistoryPageSize {
		return Config{}, errors.New("history page limits are invalid")
	}
	if config.RequestTimeout < 1 {
		return Config{}, errors.New("request timeout must be positive")
	}
	if strings.TrimSpace(config.ActorHeader) != config.ActorHeader ||
		config.ActorHeader == "" {
		return Config{}, errors.New("actor header is invalid")
	}
	return config, nil
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", 0)
	return false
}

func writeRequestError(w http.ResponseWriter, err *RequestError) {
	code := "invalid_request"
	status := http.StatusBadRequest
	if err.Message == "request body exceeds the configured limit" {
		code = "request_too_large"
		status = http.StatusRequestEntityTooLarge
	}
	writeError(w, status, code, err.Message, 0)
}

func writeRepositoryError(w http.ResponseWriter, err error) {
	var requestErr *RequestError
	switch {
	case errors.As(err, &requestErr):
		writeRequestError(w, requestErr)
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found", 0)
	case errors.Is(err, domain.ErrQueueFull):
		writeError(w, http.StatusTooManyRequests, "queue_full", "tenant queue is full", 1)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeError(
			w,
			http.StatusConflict,
			"idempotency_conflict",
			"idempotency key conflicts with an existing request",
			0,
		)
	case errors.Is(err, domain.ErrCycleDetected):
		writeError(
			w,
			http.StatusConflict,
			"cycle_detected",
			"workflow contains a dependency cycle",
			0,
		)
	case errors.Is(err, domain.ErrStaleLease):
		writeError(w, http.StatusConflict, "stale_lease", "lease is stale", 0)
	case errors.Is(err, domain.ErrTerminalJob):
		writeError(w, http.StatusConflict, "terminal_job", "job is already terminal", 0)
	case errors.Is(err, domain.ErrDeadLetterRedriven):
		writeError(
			w,
			http.StatusConflict,
			"dead_letter_redriven",
			"dead letter was already redriven",
			0,
		)
	case errors.Is(err, ErrConflict):
		writeError(
			w,
			http.StatusConflict,
			"state_conflict",
			"operation conflicts with the current resource state",
			0,
		)
	case errors.Is(err, context.DeadlineExceeded):
		writeError(
			w,
			http.StatusGatewayTimeout,
			"request_timeout",
			"request deadline exceeded",
			0,
		)
	case errors.Is(err, context.Canceled):
		writeError(
			w,
			http.StatusRequestTimeout,
			"request_canceled",
			"request was canceled",
			0,
		)
	default:
		writeError(
			w,
			http.StatusInternalServerError,
			"internal_error",
			"internal server error",
			0,
		)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string, retryAfter int) {
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	}
	writeJSON(w, status, api.ErrorResponse{
		Code:       code,
		Message:    message,
		RetryAfter: retryAfter,
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
