package operations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

var fixedNow = time.Date(2026, time.August, 14, 9, 0, 0, 0, time.UTC)

type fakeRepository struct {
	submitJob            func(context.Context, SubmitJobCommand) (api.SubmitJobResponse, error)
	submitDAG            func(context.Context, SubmitDAGCommand) (SubmitDAGResponse, error)
	getJob               func(context.Context, string) (domain.Job, error)
	getJobHistory        func(context.Context, string, HistoryQuery) (JobHistoryPage, error)
	cancelJob            func(context.Context, CancelJobCommand) (ActionReceipt, error)
	redrive              func(context.Context, RedriveCommand) (api.RedriveDeadLetterResponse, error)
	listQueueDepth       func(context.Context, string) ([]QueueDepth, error)
	listWorkerHealth     func(context.Context) ([]WorkerHealth, error)
	getDAG               func(context.Context, string) (DAGDetail, error)
	forceJob             func(context.Context, ForceJobCommand) (ActionReceipt, error)
	recordOperatorAction func(context.Context, OperatorActionCommand) (OperatorActionResponse, error)
	listAuditEvents      func(context.Context, AuditEventQuery) (AuditEventResponse, error)
}

func (f *fakeRepository) SubmitJob(
	ctx context.Context,
	command SubmitJobCommand,
) (api.SubmitJobResponse, error) {
	if f.submitJob == nil {
		return api.SubmitJobResponse{}, nil
	}
	return f.submitJob(ctx, command)
}

func (f *fakeRepository) SubmitDAG(
	ctx context.Context,
	command SubmitDAGCommand,
) (SubmitDAGResponse, error) {
	if f.submitDAG == nil {
		return SubmitDAGResponse{}, nil
	}
	return f.submitDAG(ctx, command)
}

func (f *fakeRepository) GetJob(ctx context.Context, jobID string) (domain.Job, error) {
	if f.getJob == nil {
		return domain.Job{}, nil
	}
	return f.getJob(ctx, jobID)
}

func (f *fakeRepository) GetJobHistory(
	ctx context.Context,
	jobID string,
	query HistoryQuery,
) (JobHistoryPage, error) {
	if f.getJobHistory == nil {
		return JobHistoryPage{}, nil
	}
	return f.getJobHistory(ctx, jobID, query)
}

func (f *fakeRepository) CancelJob(
	ctx context.Context,
	command CancelJobCommand,
) (ActionReceipt, error) {
	if f.cancelJob == nil {
		return ActionReceipt{}, nil
	}
	return f.cancelJob(ctx, command)
}

func (f *fakeRepository) RedriveDeadLetter(
	ctx context.Context,
	command RedriveCommand,
) (api.RedriveDeadLetterResponse, error) {
	if f.redrive == nil {
		return api.RedriveDeadLetterResponse{}, nil
	}
	return f.redrive(ctx, command)
}

func (f *fakeRepository) ListTenantQueueDepth(
	ctx context.Context,
	tenantID string,
) ([]QueueDepth, error) {
	if f.listQueueDepth == nil {
		return nil, nil
	}
	return f.listQueueDepth(ctx, tenantID)
}

func (f *fakeRepository) ListWorkerHealth(ctx context.Context) ([]WorkerHealth, error) {
	if f.listWorkerHealth == nil {
		return nil, nil
	}
	return f.listWorkerHealth(ctx)
}

func (f *fakeRepository) GetDAG(ctx context.Context, dagID string) (DAGDetail, error) {
	if f.getDAG == nil {
		return DAGDetail{}, nil
	}
	return f.getDAG(ctx, dagID)
}

func (f *fakeRepository) ForceJobAction(
	ctx context.Context,
	command ForceJobCommand,
) (ActionReceipt, error) {
	if f.forceJob == nil {
		return ActionReceipt{}, nil
	}
	return f.forceJob(ctx, command)
}

func (f *fakeRepository) RecordOperatorAction(
	ctx context.Context,
	command OperatorActionCommand,
) (OperatorActionResponse, error) {
	if f.recordOperatorAction == nil {
		return OperatorActionResponse{}, nil
	}
	return f.recordOperatorAction(ctx, command)
}

func (f *fakeRepository) ListAuditEvents(
	ctx context.Context,
	query AuditEventQuery,
) (AuditEventResponse, error) {
	if f.listAuditEvents == nil {
		return AuditEventResponse{}, nil
	}
	return f.listAuditEvents(ctx, query)
}

func TestOperationsRoutes(t *testing.T) {
	called := make(map[string]bool)
	repository := &fakeRepository{
		submitJob: func(_ context.Context, command SubmitJobCommand) (api.SubmitJobResponse, error) {
			called["submit_job"] = command.IdempotencyKey == "key" &&
				command.Request.Job.TenantID == "default" &&
				command.Actor == "operator-1"
			return api.SubmitJobResponse{Job: domain.Job{ID: "job-1"}}, nil
		},
		submitDAG: func(_ context.Context, command SubmitDAGCommand) (SubmitDAGResponse, error) {
			called["submit_dag"] = command.IdempotencyKey == "key" &&
				command.Request.TenantID == "tenant-1" &&
				command.Actor == "operator-1"
			return SubmitDAGResponse{DAGID: "dag-1", Jobs: []domain.Job{}}, nil
		},
		getJob: func(_ context.Context, jobID string) (domain.Job, error) {
			called["get_job"] = jobID == "job-1"
			return domain.Job{ID: jobID}, nil
		},
		getJobHistory: func(
			_ context.Context,
			jobID string,
			query HistoryQuery,
		) (JobHistoryPage, error) {
			called["history"] = jobID == "job-1" && query.Limit == 2 && query.BeforeSeq == 10
			return JobHistoryPage{}, nil
		},
		cancelJob: func(_ context.Context, command CancelJobCommand) (ActionReceipt, error) {
			called["cancel"] = command.JobID == "job-1" &&
				command.Actor == "operator-1" &&
				command.Reason == "stop requested"
			return ActionReceipt{JobID: command.JobID, Action: "cancel"}, nil
		},
		redrive: func(
			_ context.Context,
			command RedriveCommand,
		) (api.RedriveDeadLetterResponse, error) {
			called["redrive"] = command.JobID == "job-1" && command.Actor == "operator-1"
			return api.RedriveDeadLetterResponse{Job: domain.Job{ID: "job-2"}}, nil
		},
		listQueueDepth: func(_ context.Context, tenantID string) ([]QueueDepth, error) {
			called["queues"] = tenantID == "tenant-1"
			return nil, nil
		},
		listWorkerHealth: func(context.Context) ([]WorkerHealth, error) {
			called["workers"] = true
			return nil, nil
		},
		getDAG: func(_ context.Context, dagID string) (DAGDetail, error) {
			called["get_dag"] = dagID == "dag-1"
			return DAGDetail{ID: dagID}, nil
		},
		forceJob: func(_ context.Context, command ForceJobCommand) (ActionReceipt, error) {
			called["force"] = command.JobID == "job-1" &&
				command.Action == ForceRelease &&
				command.Actor == "operator-1"
			return ActionReceipt{JobID: command.JobID, Action: string(command.Action)}, nil
		},
	}
	handler := newTestHandler(t, repository, nil)

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		mutation   bool
		idempotent bool
		wantStatus int
	}{
		{
			name:       "submit job",
			method:     http.MethodPost,
			path:       "/v1/operations/jobs",
			body:       `{"job":{"payload":{"type":"noop"}}}`,
			mutation:   true,
			idempotent: true,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "submit DAG",
			method:     http.MethodPost,
			path:       "/v1/operations/dags",
			body:       `{"tenant_id":"tenant-1","nodes":[{"key":"a","job":{"payload":{"type":"noop"}}}]}`,
			mutation:   true,
			idempotent: true,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "get job",
			method:     http.MethodGet,
			path:       "/v1/operations/jobs/job-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get history",
			method:     http.MethodGet,
			path:       "/v1/operations/jobs/job-1/history?limit=2&before_seq=10",
			wantStatus: http.StatusOK,
		},
		{
			name:       "cancel job",
			method:     http.MethodPost,
			path:       "/v1/operations/jobs/job-1/cancel",
			body:       `{"reason":"stop requested"}`,
			mutation:   true,
			idempotent: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "redrive dead letter",
			method:     http.MethodPost,
			path:       "/v1/operations/dead-letters/job-1/redrive",
			mutation:   true,
			idempotent: true,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "list queue depth",
			method:     http.MethodGet,
			path:       "/v1/operations/tenants/tenant-1/queues",
			wantStatus: http.StatusOK,
		},
		{
			name:       "list worker health",
			method:     http.MethodGet,
			path:       "/v1/operations/workers",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get DAG",
			method:     http.MethodGet,
			path:       "/v1/operations/dags/dag-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "force job",
			method:     http.MethodPost,
			path:       "/v1/operations/jobs/job-1/force",
			body:       `{"action":"release","reason":"worker is unavailable"}`,
			mutation:   true,
			idempotent: true,
			wantStatus: http.StatusOK,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			if test.idempotent {
				request.Header.Set("Idempotency-Key", "key")
			}
			if test.mutation {
				request.Header.Set(defaultActorHeader, "operator-1")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type = %q", response.Header().Get("Content-Type"))
			}
		})
	}

	for _, operation := range []string{
		"submit_job",
		"submit_dag",
		"get_job",
		"history",
		"cancel",
		"redrive",
		"queues",
		"workers",
		"get_dag",
		"force",
	} {
		if !called[operation] {
			t.Errorf("%s repository method was not called correctly", operation)
		}
	}
}

func TestOperatorMutationsRequireActorAndIdempotencyHeaders(t *testing.T) {
	repository := &fakeRepository{
		submitJob: func(context.Context, SubmitJobCommand) (api.SubmitJobResponse, error) {
			t.Fatal("SubmitJob called for invalid request")
			return api.SubmitJobResponse{}, nil
		},
		submitDAG: func(context.Context, SubmitDAGCommand) (SubmitDAGResponse, error) {
			t.Fatal("SubmitDAG called for invalid request")
			return SubmitDAGResponse{}, nil
		},
		cancelJob: func(context.Context, CancelJobCommand) (ActionReceipt, error) {
			t.Fatal("CancelJob called for invalid request")
			return ActionReceipt{}, nil
		},
	}
	handler := newTestHandler(t, repository, nil)

	for _, request := range []*http.Request{
		httptest.NewRequest(
			http.MethodPost,
			"/v1/operations/jobs",
			strings.NewReader(`{"job":{"payload":{"type":"noop"}}}`),
		),
		httptest.NewRequest(
			http.MethodPost,
			"/v1/operations/dags",
			strings.NewReader(
				`{"tenant_id":"tenant-a","nodes":[{"key":"a","job":{"payload":{"type":"noop"}}}]}`,
			),
		),
	} {
		request.Header.Set("Idempotency-Key", "key")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assertAPIError(t, response, http.StatusBadRequest, "invalid_request")
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/operations/jobs/job-1/cancel",
		strings.NewReader(`{"reason":"stop"}`),
	)
	request.Header.Set("Idempotency-Key", "key")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertAPIError(t, response, http.StatusBadRequest, "invalid_request")

	request = httptest.NewRequest(
		http.MethodPost,
		"/v1/operations/jobs/job-1/cancel",
		strings.NewReader(`{"reason":"stop"}`),
	)
	request.Header.Set(defaultActorHeader, "operator-1")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	assertAPIError(t, response, http.StatusBadRequest, "invalid_request")
}

func TestStrictBoundedJSON(t *testing.T) {
	repository := &fakeRepository{
		submitJob: func(context.Context, SubmitJobCommand) (api.SubmitJobResponse, error) {
			t.Fatal("SubmitJob called for invalid request")
			return api.SubmitJobResponse{}, nil
		},
	}
	handler := newTestHandler(t, repository, func(config *Config) {
		config.MaxBodyBytes = 128
	})

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "unknown field",
			body:       `{"job":{"payload":{"type":"noop"}},"extra":true}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "multiple values",
			body:       `{"job":{"payload":{"type":"noop"}}}{}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "too large",
			body:       `{"job":{"name":"` + strings.Repeat("x", 200) + `","payload":{"type":"noop"}}}`,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "request_too_large",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/operations/jobs",
				strings.NewReader(test.body),
			)
			request.Header.Set("Idempotency-Key", "key")
			request.Header.Set(defaultActorHeader, "operator-1")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			assertAPIError(t, response, test.wantStatus, test.wantCode)
		})
	}
}

func TestSubmissionDigestUsesNormalizedRequest(t *testing.T) {
	var digests []string
	repository := &fakeRepository{
		submitJob: func(
			_ context.Context,
			command SubmitJobCommand,
		) (api.SubmitJobResponse, error) {
			digests = append(digests, command.RequestDigest)
			return api.SubmitJobResponse{Duplicate: len(digests) > 1}, nil
		},
	}
	handler := newTestHandler(t, repository, nil)

	bodies := []string{
		`{"job":{"payload":{"type":"noop"},"depends_on":["b","a"]}}`,
		`{"job":{"tenant_id":"default","queue":"default","slot_cost":1,` +
			`"payload":{"type":"noop"},"retry":{"max_attempts":5,"retryable":true},` +
			`"depends_on":["a","b"]}}`,
	}
	for _, body := range bodies {
		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/operations/jobs",
			strings.NewReader(body),
		)
		request.Header.Set("Idempotency-Key", "key")
		request.Header.Set(defaultActorHeader, "operator-1")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusCreated && response.Code != http.StatusOK {
			t.Fatalf("status = %d; body = %s", response.Code, response.Body.String())
		}
	}
	if len(digests) != 2 || digests[0] != digests[1] || len(digests[0]) != 64 {
		t.Fatalf("digests = %v", digests)
	}
}

func TestStableErrorsAndMethodHandling(t *testing.T) {
	repository := &fakeRepository{
		getJob: func(context.Context, string) (domain.Job, error) {
			return domain.Job{}, errors.Join(errors.New("wrapped"), domain.ErrNotFound)
		},
	}
	handler := newTestHandler(t, repository, nil)

	response := httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/v1/operations/jobs/missing", nil),
	)
	assertAPIError(t, response, http.StatusNotFound, "not_found")

	response = httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodDelete, "/v1/operations/jobs/missing", nil),
	)
	assertAPIError(t, response, http.StatusMethodNotAllowed, "method_not_allowed")
	if response.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("Allow = %q", response.Header().Get("Allow"))
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/v1/operations/unknown", nil),
	)
	assertAPIError(t, response, http.StatusNotFound, "not_found")
}

func TestHistoryQueryValidation(t *testing.T) {
	repository := &fakeRepository{
		getJobHistory: func(
			context.Context,
			string,
			HistoryQuery,
		) (JobHistoryPage, error) {
			t.Fatal("GetJobHistory called for invalid query")
			return JobHistoryPage{}, nil
		},
	}
	handler := newTestHandler(t, repository, nil)
	for _, path := range []string{
		"/v1/operations/jobs/job-1/history?limit=0",
		"/v1/operations/jobs/job-1/history?before_seq=-1",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		assertAPIError(t, response, http.StatusBadRequest, "invalid_request")
	}
}

func TestMountRegistersOperationsSubtree(t *testing.T) {
	handler := newTestHandler(t, &fakeRepository{}, nil)
	mux := http.NewServeMux()
	handler.Mount(mux)

	response := httptest.NewRecorder()
	mux.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/v1/operations/workers", nil),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body = %s", response.Code, response.Body.String())
	}
}

func newTestHandler(
	t *testing.T,
	repository *fakeRepository,
	mutateConfig func(*Config),
) *Handler {
	t.Helper()
	config := DefaultConfig()
	config.Now = func() time.Time { return fixedNow }
	if mutateConfig != nil {
		mutateConfig(&config)
	}
	handler, err := New(allRepositories(repository), config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return handler
}

func allRepositories(repository *fakeRepository) Repositories {
	return Repositories{
		JobSubmitter:           repository,
		DAGSubmitter:           repository,
		JobReader:              repository,
		JobHistoryReader:       repository,
		JobCanceller:           repository,
		DeadLetterRedriver:     repository,
		QueueDepthReader:       repository,
		WorkerHealthReader:     repository,
		DAGReader:              repository,
		ForceJobController:     repository,
		OperatorActionRecorder: repository,
		AuditEventReader:       repository,
	}
}

func assertAPIError(
	t *testing.T,
	response *httptest.ResponseRecorder,
	wantStatus int,
	wantCode string,
) {
	t.Helper()
	if response.Code != wantStatus {
		t.Fatalf(
			"status = %d, want %d; body = %s",
			response.Code,
			wantStatus,
			response.Body.String(),
		)
	}
	var value api.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode error: %v; body = %s", err, response.Body.String())
	}
	if value.Code != wantCode {
		t.Fatalf("code = %q, want %q", value.Code, wantCode)
	}
}
