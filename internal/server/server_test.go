package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

var testNow = time.Date(2026, time.August, 14, 7, 30, 0, 0, time.UTC)

func TestRecoveryReserve(t *testing.T) {
	tests := []struct {
		slots int
		want  int
	}{
		{slots: 1, want: 0},
		{slots: 2, want: 1},
		{slots: 8, want: 2},
		{slots: 16, want: 4},
	}
	for _, test := range tests {
		if got := recoveryReserve(test.slots); got != test.want {
			t.Errorf("recoveryReserve(%d) = %d, want %d", test.slots, got, test.want)
		}
	}
}

func TestControlRoutes(t *testing.T) {
	job := domain.Job{ID: "job-1", State: domain.StatePending}
	jobStore := &fakeStore{
		submitJob: func(
			context.Context,
			store.Submission,
			time.Time,
		) (domain.Job, bool, error) {
			return job, false, nil
		},
		submitWorkflow: func(
			context.Context,
			store.WorkflowSubmission,
			time.Time,
		) ([]domain.Job, bool, error) {
			return []domain.Job{job}, false, nil
		},
		getJob: func(_ context.Context, jobID string) (domain.Job, error) {
			if jobID != job.ID {
				t.Fatalf("GetJob received ID %q", jobID)
			}
			return job, nil
		},
	}
	server := newTestServer(t, jobStore, nil)

	tests := []struct {
		name           string
		method         string
		path           string
		body           string
		idempotencyKey string
		wantStatus     int
	}{
		{
			name:       "live",
			method:     http.MethodGet,
			path:       "/health/live",
			wantStatus: http.StatusOK,
		},
		{
			name:       "ready",
			method:     http.MethodGet,
			path:       "/health/ready",
			wantStatus: http.StatusOK,
		},
		{
			name:           "submit job",
			method:         http.MethodPost,
			path:           "/v1/jobs",
			body:           `{"job":{"payload":{"type":"noop"}}}`,
			idempotencyKey: "submit-job",
			wantStatus:     http.StatusCreated,
		},
		{
			name:       "get job",
			method:     http.MethodGet,
			path:       "/v1/jobs/job-1",
			wantStatus: http.StatusOK,
		},
		{
			name:           "submit workflow",
			method:         http.MethodPost,
			path:           "/v1/workflows",
			body:           `{"tenant_id":"tenant","nodes":[{"key":"a","job":{"payload":{"type":"noop"}}}]}`,
			idempotencyKey: "submit-workflow",
			wantStatus:     http.StatusCreated,
		},
		{
			name:       "known route wrong method",
			method:     http.MethodGet,
			path:       "/v1/jobs",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "unknown route",
			method:     http.MethodGet,
			path:       "/v1/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "trailing slash rejected",
			method:     http.MethodGet,
			path:       "/health/live/",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performRequest(
				server,
				test.method,
				test.path,
				test.body,
				test.idempotencyKey,
			)
			if response.Code != test.wantStatus {
				t.Fatalf(
					"status = %d, want %d; body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			if response.Header().Get("Content-Type") != "application/json" &&
				response.Code != http.StatusNoContent {
				t.Fatalf("Content-Type = %q", response.Header().Get("Content-Type"))
			}
		})
	}
}

func TestWorkerRoutes(t *testing.T) {
	lease := domain.Lease{
		JobID:      "job-1",
		AttemptNo:  1,
		Generation: 1,
		Token:      "token",
		WorkerID:   "worker-1",
		SlotCost:   1,
		Payload:    domain.Payload{Type: domain.PayloadNoop},
	}
	receipt := domain.CompletionReceipt{
		JobID:        lease.JobID,
		State:        domain.StateSucceeded,
		StateVersion: 3,
		CommittedAt:  testNow,
	}

	var started, heartbeated, completed bool
	jobStore := &fakeStore{
		acquire: func(
			_ context.Context,
			workerID string,
			availableSlots int,
			limit int,
			_ time.Time,
			leaseTTL time.Duration,
		) ([]domain.Lease, error) {
			if workerID != "worker-1" || availableSlots != 2 || limit != 1 {
				t.Fatalf(
					"Acquire received worker=%q slots=%d limit=%d",
					workerID,
					availableSlots,
					limit,
				)
			}
			if leaseTTL != 2500*time.Millisecond {
				t.Fatalf("lease TTL = %s", leaseTTL)
			}
			return []domain.Lease{lease}, nil
		},
		markRunning: func(
			_ context.Context,
			workerID string,
			ref domain.LeaseRef,
			_ time.Time,
		) error {
			started = workerID == "worker-1" && ref.Token == "token"
			return nil
		},
		heartbeat: func(
			_ context.Context,
			workerID string,
			refs []domain.LeaseRef,
			_ time.Time,
			_ time.Duration,
		) ([]api.HeartbeatResult, error) {
			heartbeated = workerID == "worker-1" &&
				len(refs) == 1 &&
				refs[0].Generation == 1
			return []api.HeartbeatResult{{
				JobID:     "job-1",
				Accepted:  true,
				ExpiresAt: testNow.Add(3 * time.Second),
			}}, nil
		},
		complete: func(
			_ context.Context,
			completion domain.Completion,
			_ time.Time,
		) (domain.CompletionReceipt, error) {
			completed = completion.WorkerID == "worker-1" &&
				completion.OutputDigest == "digest"
			return receipt, nil
		},
	}
	server := newTestServer(t, jobStore, nil)

	register := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/register",
		`{"worker_id":"worker-1","slots":2}`,
		"",
	)
	if register.Code != http.StatusOK {
		t.Fatalf("register status = %d; body = %s", register.Code, register.Body.String())
	}
	var registration api.RegisterWorkerResponse
	if err := json.Unmarshal(register.Body.Bytes(), &registration); err != nil {
		t.Fatalf("decode registration: %v", err)
	}
	if registration.HeartbeatEvery != time.Second ||
		registration.LeaseTTL != 2500*time.Millisecond {
		t.Fatalf("unexpected registration settings: %+v", registration)
	}

	requests := []struct {
		path       string
		body       string
		wantStatus int
	}{
		{
			path:       "/v1/workers/worker-1/leases/acquire",
			body:       `{"available_slots":2,"limit":1}`,
			wantStatus: http.StatusOK,
		},
		{
			path:       "/v1/workers/worker-1/attempts/start",
			body:       `{"job_id":"job-1","attempt_no":1,"generation":1,"token":"token"}`,
			wantStatus: http.StatusNoContent,
		},
		{
			path:       "/v1/workers/worker-1/heartbeats",
			body:       `{"leases":[{"job_id":"job-1","attempt_no":1,"generation":1,"token":"token"}]}`,
			wantStatus: http.StatusOK,
		},
		{
			path: "/v1/workers/worker-1/attempts/complete",
			body: `{"job_id":"job-1","attempt_no":1,"generation":1,"token":"token",` +
				`"success":true,"output_digest":"digest"}`,
			wantStatus: http.StatusOK,
		},
	}
	for _, request := range requests {
		response := performRequest(server, http.MethodPost, request.path, request.body, "")
		if response.Code != request.wantStatus {
			t.Fatalf(
				"%s status = %d, want %d; body = %s",
				request.path,
				response.Code,
				request.wantStatus,
				response.Body.String(),
			)
		}
	}
	if !started || !heartbeated || !completed {
		t.Fatalf(
			"worker calls: started=%v heartbeated=%v completed=%v",
			started,
			heartbeated,
			completed,
		)
	}
}

func TestAttemptStartBatchReturnsPerLeaseResults(t *testing.T) {
	jobStore := &fakeStore{
		markRunningBatch: func(
			_ context.Context,
			workerID string,
			refs []domain.LeaseRef,
			_ time.Time,
		) ([]store.AttemptStartResult, error) {
			if workerID != "worker-1" || len(refs) != 2 {
				t.Fatalf("worker = %q, refs = %d", workerID, len(refs))
			}
			return []store.AttemptStartResult{
				{},
				{Err: domain.ErrStaleLease},
			}, nil
		},
	}
	server := newTestServer(t, jobStore, nil)
	register := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/register",
		`{"worker_id":"worker-1","slots":2}`,
		"",
	)
	if register.Code != http.StatusOK {
		t.Fatalf("register status = %d; body = %s", register.Code, register.Body.String())
	}

	response := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/worker-1/attempts/start-batch",
		`{"leases":[`+
			`{"job_id":"job-1","attempt_no":1,"generation":1,"token":"token-1"},`+
			`{"job_id":"job-2","attempt_no":1,"generation":1,"token":"token-2"}]}`,
		"",
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body = %s", response.Code, response.Body.String())
	}
	var payload api.StartAttemptsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Results) != 2 || !payload.Results[0].Started {
		t.Fatalf("first result = %+v", payload.Results)
	}
	if payload.Results[1].Started ||
		payload.Results[1].Error == nil ||
		payload.Results[1].Error.Code != "stale_lease" {
		t.Fatalf("second result = %+v, want stale lease", payload.Results[1])
	}
}

func TestCompletionBatchReturnsPerItemResults(t *testing.T) {
	jobStore := &fakeStore{
		completeBatch: func(
			_ context.Context,
			completions []domain.Completion,
			_ time.Time,
		) ([]store.CompletionResult, error) {
			if len(completions) != 2 {
				t.Fatalf("completions = %d, want 2", len(completions))
			}
			for _, completion := range completions {
				if completion.WorkerID != "worker-1" {
					t.Fatalf("worker ID = %q, want worker-1", completion.WorkerID)
				}
			}
			return []store.CompletionResult{
				{Receipt: domain.CompletionReceipt{
					JobID:        completions[0].JobID,
					State:        domain.StateSucceeded,
					StateVersion: 3,
					CommittedAt:  testNow,
				}},
				{Err: domain.ErrStaleLease},
			}, nil
		},
	}
	server := newTestServer(t, jobStore, nil)
	register := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/register",
		`{"worker_id":"worker-1","slots":2}`,
		"",
	)
	if register.Code != http.StatusOK {
		t.Fatalf("register status = %d; body = %s", register.Code, register.Body.String())
	}

	response := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/worker-1/attempts/complete-batch",
		`{"completions":[`+
			`{"job_id":"job-1","attempt_no":1,"generation":1,"token":"token-1",`+
			`"success":true,"output_digest":"digest-1"},`+
			`{"job_id":"job-2","attempt_no":1,"generation":1,"token":"token-2",`+
			`"success":true,"output_digest":"digest-2"}]}`,
		"",
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body = %s", response.Code, response.Body.String())
	}
	var payload api.CompleteAttemptsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Results) != 2 ||
		payload.Results[0].Receipt == nil ||
		payload.Results[0].Receipt.State != domain.StateSucceeded {
		t.Fatalf("first result = %+v", payload.Results)
	}
	if payload.Results[1].Error == nil || payload.Results[1].Error.Code != "stale_lease" {
		t.Fatalf("second result = %+v, want stale lease", payload.Results[1])
	}
}

func TestSubmissionRequiresIdempotencyAndUsesStableDigest(t *testing.T) {
	var submissions []store.Submission
	jobStore := &fakeStore{
		submitJob: func(
			_ context.Context,
			submission store.Submission,
			_ time.Time,
		) (domain.Job, bool, error) {
			submissions = append(submissions, submission)
			return domain.Job{ID: "job-1"}, len(submissions) > 1, nil
		},
	}
	server := newTestServer(t, jobStore, nil)

	missingKey := performRequest(
		server,
		http.MethodPost,
		"/v1/jobs",
		`{"job":{"payload":{"type":"noop"}}}`,
		"",
	)
	assertErrorResponse(t, missingKey, http.StatusBadRequest, "invalid_request")
	if len(submissions) != 0 {
		t.Fatalf("SubmitJob called without idempotency key")
	}

	first := performRequest(
		server,
		http.MethodPost,
		"/v1/jobs",
		`{"job":{"payload":{"type":"noop"}}}`,
		"stable-key",
	)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d; body = %s", first.Code, first.Body.String())
	}

	second := performRequest(
		server,
		http.MethodPost,
		"/v1/jobs",
		`{
			"job": {
				"tenant_id": "default",
				"queue": "default",
				"slot_cost": 1,
				"payload": {"type": "noop"},
				"retry": {"max_attempts": 5, "retryable": true},
				"priority": 0
			}
		}`,
		"stable-key",
	)
	if second.Code != http.StatusOK {
		t.Fatalf("duplicate status = %d; body = %s", second.Code, second.Body.String())
	}
	if len(submissions) != 2 {
		t.Fatalf("SubmitJob calls = %d, want 2", len(submissions))
	}
	if submissions[0].IdempotencyKey != "stable-key" {
		t.Fatalf("idempotency key = %q", submissions[0].IdempotencyKey)
	}
	if submissions[0].RequestDigest != submissions[1].RequestDigest {
		t.Fatalf(
			"semantically identical requests have different digests: %s != %s",
			submissions[0].RequestDigest,
			submissions[1].RequestDigest,
		)
	}
	if len(submissions[0].RequestDigest) != 64 {
		t.Fatalf("digest length = %d", len(submissions[0].RequestDigest))
	}
}

func TestStrictBoundedJSON(t *testing.T) {
	var calls atomic.Int32
	jobStore := &fakeStore{
		submitJob: func(
			context.Context,
			store.Submission,
			time.Time,
		) (domain.Job, bool, error) {
			calls.Add(1)
			return domain.Job{}, false, nil
		},
	}
	server := newTestServer(t, jobStore, func(config *Config) {
		config.MaxBodyBytes = 256
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
			name:       "trailing value",
			body:       `{"job":{"payload":{"type":"noop"}}}{}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "malformed",
			body:       `{"job":`,
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "empty",
			body:       "",
			wantStatus: http.StatusBadRequest,
			wantCode:   "invalid_request",
		},
		{
			name:       "too large",
			body:       `{"job":{"name":"` + strings.Repeat("x", 300) + `","payload":{"type":"noop"}}}`,
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "request_too_large",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performRequest(
				server,
				http.MethodPost,
				"/v1/jobs",
				test.body,
				"key",
			)
			assertErrorResponse(t, response, test.wantStatus, test.wantCode)
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("SubmitJob calls = %d", calls.Load())
	}
}

func TestDomainErrorMapping(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"not found", domain.ErrNotFound, http.StatusNotFound, "not_found"},
		{"queue full", domain.ErrQueueFull, http.StatusTooManyRequests, "queue_full"},
		{
			"idempotency conflict",
			domain.ErrIdempotencyConflict,
			http.StatusConflict,
			"idempotency_conflict",
		},
		{"cycle", domain.ErrCycleDetected, http.StatusConflict, "cycle_detected"},
		{"stale lease", domain.ErrStaleLease, http.StatusConflict, "stale_lease"},
		{"terminal job", domain.ErrTerminalJob, http.StatusConflict, "terminal_job"},
		{
			"deadline",
			context.DeadlineExceeded,
			http.StatusGatewayTimeout,
			"request_timeout",
		},
		{"unknown", errors.New("database failed"), http.StatusInternalServerError, "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			writeStoreError(response, errors.Join(errors.New("wrapped"), test.err))
			assertErrorResponse(t, response, test.wantStatus, test.wantCode)
		})
	}
}

func TestFencingResponses(t *testing.T) {
	jobStore := &fakeStore{
		markRunning: func(
			context.Context,
			string,
			domain.LeaseRef,
			time.Time,
		) error {
			return domain.ErrStaleLease
		},
		heartbeat: func(
			context.Context,
			string,
			[]domain.LeaseRef,
			time.Time,
			time.Duration,
		) ([]api.HeartbeatResult, error) {
			return nil, domain.ErrStaleLease
		},
		complete: func(
			context.Context,
			domain.Completion,
			time.Time,
		) (domain.CompletionReceipt, error) {
			return domain.CompletionReceipt{}, domain.ErrStaleLease
		},
	}
	server := newTestServer(t, jobStore, nil)
	registerTestWorker(t, server)

	requests := []struct {
		path string
		body string
	}{
		{
			path: "/v1/workers/worker-1/attempts/start",
			body: `{"job_id":"job-1","attempt_no":1,"generation":1,"token":"stale"}`,
		},
		{
			path: "/v1/workers/worker-1/heartbeats",
			body: `{"leases":[{"job_id":"job-1","attempt_no":1,` +
				`"generation":1,"token":"stale"}]}`,
		},
		{
			path: "/v1/workers/worker-1/attempts/complete",
			body: `{"job_id":"job-1","attempt_no":1,"generation":1,"token":"stale",` +
				`"success":true,"output_digest":"digest"}`,
		},
	}
	for _, request := range requests {
		response := performRequest(server, http.MethodPost, request.path, request.body, "")
		assertErrorResponse(t, response, http.StatusConflict, "stale_lease")
	}
}

func TestHeartbeatPreservesPerLeaseFencingResults(t *testing.T) {
	jobStore := &fakeStore{
		heartbeat: func(
			_ context.Context,
			_ string,
			_ []domain.LeaseRef,
			_ time.Time,
			_ time.Duration,
		) ([]api.HeartbeatResult, error) {
			return []api.HeartbeatResult{{
				JobID:    "job-1",
				Accepted: false,
			}}, nil
		},
	}
	server := newTestServer(t, jobStore, nil)
	registerTestWorker(t, server)

	response := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/worker-1/heartbeats",
		`{"leases":[{"job_id":"job-1","attempt_no":1,"generation":1,"token":"stale"}]}`,
		"",
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body = %s", response.Code, response.Body.String())
	}
	var heartbeat api.HeartbeatResponse
	if err := json.Unmarshal(response.Body.Bytes(), &heartbeat); err != nil {
		t.Fatalf("decode heartbeat response: %v", err)
	}
	if len(heartbeat.Results) != 1 || heartbeat.Results[0].Accepted {
		t.Fatalf("heartbeat results = %+v", heartbeat.Results)
	}
}

func TestAcquireLongPollUsesDiscreteStoreCalls(t *testing.T) {
	var active atomic.Int32
	var maximumActive atomic.Int32
	var calls atomic.Int32
	jobStore := &fakeStore{
		acquire: func(
			context.Context,
			string,
			int,
			int,
			time.Time,
			time.Duration,
		) ([]domain.Lease, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				maximum := maximumActive.Load()
				if current <= maximum || maximumActive.CompareAndSwap(maximum, current) {
					break
				}
			}
			call := calls.Add(1)
			if call < 3 {
				return nil, nil
			}
			return []domain.Lease{{
				JobID:      "job-1",
				AttemptNo:  1,
				Generation: 1,
				Token:      "token",
				WorkerID:   "worker-1",
			}}, nil
		},
	}
	server := newTestServer(t, jobStore, func(config *Config) {
		config.LongPollTimeout = 100 * time.Millisecond
		config.AcquirePollInterval = time.Millisecond
		config.RequestTimeout = 50 * time.Millisecond
	})
	registerTestWorker(t, server)

	response := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/worker-1/leases/acquire",
		`{"available_slots":1,"limit":1}`,
		"",
	)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body = %s", response.Code, response.Body.String())
	}
	if calls.Load() < 3 {
		t.Fatalf("Acquire calls = %d, want at least 3", calls.Load())
	}
	if maximumActive.Load() != 1 {
		t.Fatalf("simultaneous Acquire calls = %d", maximumActive.Load())
	}
}

func TestRequestTimeout(t *testing.T) {
	jobStore := &fakeStore{
		getJob: func(ctx context.Context, _ string) (domain.Job, error) {
			<-ctx.Done()
			return domain.Job{}, ctx.Err()
		},
	}
	server := newTestServer(t, jobStore, func(config *Config) {
		config.RequestTimeout = 5 * time.Millisecond
	})

	response := performRequest(server, http.MethodGet, "/v1/jobs/job-1", "", "")
	assertErrorResponse(t, response, http.StatusGatewayTimeout, "request_timeout")
}

func TestBackgroundLoopsAndShutdown(t *testing.T) {
	reaped := make(chan struct{})
	promoted := make(chan struct{})
	var reapOnce sync.Once
	var promoteOnce sync.Once
	jobStore := &fakeStore{
		reap: func(context.Context, time.Time, int) ([]domain.ReapedLease, error) {
			reapOnce.Do(func() { close(reaped) })
			return nil, nil
		},
		promoteDue: func(context.Context, time.Time, int) (int, error) {
			promoteOnce.Do(func() { close(promoted) })
			return 0, nil
		},
	}
	server := newTestServer(t, jobStore, nil)

	select {
	case <-reaped:
	case <-time.After(time.Second):
		t.Fatal("startup reaper did not run")
	}
	select {
	case <-promoted:
	case <-time.After(time.Second):
		t.Fatal("startup due promotion did not run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	if jobStore.numberOfCloseCalls() != 1 {
		t.Fatalf("Close calls = %d", jobStore.numberOfCloseCalls())
	}

	response := performRequest(server, http.MethodGet, "/health/ready", "", "")
	assertErrorResponse(t, response, http.StatusServiceUnavailable, "shutting_down")
}

func newTestServer(
	t *testing.T,
	jobStore *fakeStore,
	mutateConfig func(*Config),
) *Server {
	t.Helper()
	config := DefaultConfig()
	config.Now = func() time.Time { return testNow }
	config.ReaperInterval = time.Hour
	config.DuePromotionInterval = time.Hour
	config.BackgroundOperationTimeout = time.Second
	config.RequestTimeout = 100 * time.Millisecond
	config.LongPollTimeout = 20 * time.Millisecond
	config.AcquirePollInterval = time.Millisecond
	if mutateConfig != nil {
		mutateConfig(&config)
	}

	server, err := New(jobStore, config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return server
}

func registerTestWorker(t *testing.T, server *Server) {
	t.Helper()
	response := performRequest(
		server,
		http.MethodPost,
		"/v1/workers/register",
		`{"worker_id":"worker-1","slots":2}`,
		"",
	)
	if response.Code != http.StatusOK {
		t.Fatalf("register status = %d; body = %s", response.Code, response.Body.String())
	}
}

func performRequest(
	handler http.Handler,
	method string,
	path string,
	body string,
	idempotencyKey string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertErrorResponse(
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
	var apiError api.ErrorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &apiError); err != nil {
		t.Fatalf("decode error response: %v; body = %s", err, response.Body.String())
	}
	if apiError.Code != wantCode {
		t.Fatalf("code = %q, want %q; body = %s", apiError.Code, wantCode, response.Body.String())
	}
}
