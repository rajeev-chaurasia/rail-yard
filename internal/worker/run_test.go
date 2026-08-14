package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/executor"
)

func TestWorkerExecutesConcurrentlyWithinSlotCapacity(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID: "worker-1",
		Slots:    3,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	request := receive(t, protocol.acquireRequests)
	if request.AvailableSlots != 3 || request.Limit != 3 {
		t.Fatalf("acquire request = %+v, want three available slots", request)
	}
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{
			testLease("job-a", 2),
			testLease("job-b", 1),
			testLease("job-c", 1),
		},
	}}

	first := receive(t, runner.started)
	second := receive(t, runner.started)
	keys := []string{first.IdempotencyKey, second.IdempotencyKey}
	sort.Strings(keys)
	if want := []string{"job-a-key", "job-b-key"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("executed keys = %v, want %v", keys, want)
	}
	select {
	case unexpected := <-runner.started:
		t.Fatalf("capacity overflow executed %+v", unexpected)
	default:
	}

	runner.results <- successfulResult()
	runner.results <- successfulResult()
	completed := []domain.Completion{
		receive(t, protocol.completions),
		receive(t, protocol.completions),
	}
	for _, completion := range completed {
		if !completion.Success || completion.WorkerID != "worker-1" {
			t.Fatalf("completion = %+v", completion)
		}
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerStartsAcquiredLeasesInBoundedBatches(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID:              "worker-1",
		Slots:                 5,
		AttemptStartBatchSize: 2,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{
			testLease("start-a", 1),
			testLease("start-b", 1),
			testLease("start-c", 1),
			testLease("start-d", 1),
			testLease("start-e", 1),
		},
	}}

	sizes := []int{
		len(receive(t, protocol.startBatches)),
		len(receive(t, protocol.startBatches)),
		len(receive(t, protocol.startBatches)),
	}
	sort.Ints(sizes)
	if want := []int{1, 2, 2}; !reflect.DeepEqual(sizes, want) {
		t.Fatalf("start batch sizes = %v, want %v", sizes, want)
	}
	for range 5 {
		_ = receive(t, runner.started)
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerExecutesOnlyCurrentBatchStarts(t *testing.T) {
	protocol := newFakeProtocol()
	protocol.startBatch = func(request api.StartAttemptsRequest) (api.StartAttemptsResponse, error) {
		return api.StartAttemptsResponse{Results: []api.StartResult{
			{JobID: request.Leases[0].JobID, Started: true},
			{
				JobID: request.Leases[1].JobID,
				Error: &api.ErrorResponse{Code: "stale_lease", Message: "stale"},
			},
		}}, nil
	}
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID:              "worker-1",
		Slots:                 2,
		AttemptStartBatchSize: 2,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{
			testLease("current-start", 1),
			testLease("stale-start", 1),
		},
	}}
	batch := receive(t, protocol.startBatches)
	if len(batch) != 2 {
		t.Fatalf("start batch = %d, want 2", len(batch))
	}
	started := receive(t, runner.started)
	if started.IdempotencyKey != "current-start-key" {
		t.Fatalf("executor started %q, want current-start-key", started.IdempotencyKey)
	}
	select {
	case unexpected := <-runner.started:
		t.Fatalf("stale attempt executed: %+v", unexpected)
	default:
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerCancelsAttemptWhenHeartbeatRejectsLease(t *testing.T) {
	protocol := newFakeProtocol()
	protocol.heartbeat = func(request api.HeartbeatRequest) (api.HeartbeatResponse, error) {
		results := make([]api.HeartbeatResult, len(request.Leases))
		for index, reference := range request.Leases {
			results[index] = api.HeartbeatResult{JobID: reference.JobID, Accepted: false}
		}
		return api.HeartbeatResponse{Results: results}, nil
	}
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID: "worker-1",
		Slots:    1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{testLease("stale-job", 1)},
	}}
	_ = receive(t, runner.started)

	ticker.Tick()
	heartbeat := receive(t, protocol.heartbeats)
	if len(heartbeat.Leases) != 1 || heartbeat.Leases[0].JobID != "stale-job" {
		t.Fatalf("heartbeat = %+v, want stale-job", heartbeat)
	}
	if canceled := receive(t, runner.canceled); canceled != "stale-job-key" {
		t.Fatalf("canceled key = %q", canceled)
	}
	secondAcquire := receive(t, protocol.acquireRequests)
	if secondAcquire.AvailableSlots != 1 {
		t.Fatalf("available slots after stale lease = %d, want 1", secondAcquire.AvailableSlots)
	}
	select {
	case completion := <-protocol.completions:
		t.Fatalf("stale attempt was completed: %+v", completion)
	default:
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestIdleWorkerSendsEmptyHeartbeat(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID: "worker-idle",
		Slots:    2,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	registration := receive(t, protocol.registerRequests)
	if registration.WorkerID != "worker-idle" || registration.Slots != 2 {
		t.Fatalf("registration = %+v", registration)
	}
	_ = receive(t, protocol.acquireRequests)

	ticker.Tick()
	heartbeat := receive(t, protocol.heartbeats)
	if heartbeat.Leases == nil || len(heartbeat.Leases) != 0 {
		t.Fatalf("idle heartbeat leases = %#v, want empty batch", heartbeat.Leases)
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerDoesNotExecuteWhenStartIsStale(t *testing.T) {
	protocol := newFakeProtocol()
	protocol.start = func(api.StartAttemptRequest) error {
		return domain.ErrStaleLease
	}
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID: "worker-1",
		Slots:    1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{testLease("stale-start", 1)},
	}}
	started := receive(t, protocol.starts)
	if started.JobID != "stale-start" {
		t.Fatalf("started = %+v", started)
	}
	_ = receive(t, protocol.acquireRequests)
	select {
	case request := <-runner.started:
		t.Fatalf("executor ran after stale start: %+v", request)
	default:
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerSendsFinalHeartbeatAndCancelsOnShutdown(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID:        "worker-1",
		Slots:           1,
		ShutdownTimeout: time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{testLease("shutdown-job", 1)},
	}}
	_ = receive(t, runner.started)

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
	if canceled := receive(t, runner.canceled); canceled != "shutdown-job-key" {
		t.Fatalf("canceled key = %q", canceled)
	}
	heartbeat := receive(t, protocol.heartbeats)
	if len(heartbeat.Leases) != 1 || heartbeat.Leases[0].JobID != "shutdown-job" {
		t.Fatalf("final heartbeat = %+v", heartbeat)
	}
}

func TestWorkerCoalescesCompletionsWhileHeartbeatsContinue(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID:            "worker-1",
		Slots:               3,
		CompletionBatchSize: 4,
		CompletionBatchWait: 500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{
			testLease("batch-a", 1),
			testLease("batch-b", 1),
			testLease("batch-c", 1),
		},
	}}
	for range 3 {
		_ = receive(t, runner.started)
	}
	for range 3 {
		runner.results <- successfulResult()
	}

	ticker.Tick()
	heartbeat := receive(t, protocol.heartbeats)
	if len(heartbeat.Leases) != 3 {
		t.Fatalf("heartbeat leases = %d, want 3 while completions are pending", len(heartbeat.Leases))
	}
	batch := receive(t, protocol.completionBatches)
	if len(batch) != 3 {
		t.Fatalf("completion batch = %d, want 3", len(batch))
	}
	for _, completion := range batch {
		if !completion.Success || completion.WorkerID != "worker-1" {
			t.Fatalf("completion = %+v", completion)
		}
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRetriesFailedBatchItem(t *testing.T) {
	protocol := newFakeProtocol()
	attempt := 0
	protocol.completeBatch = func(request api.CompleteAttemptsRequest) (api.CompleteAttemptsResponse, error) {
		attempt++
		completion := request.Completions[0]
		result := api.CompletionResult{JobID: completion.JobID}
		if attempt == 1 {
			result.Error = &api.ErrorResponse{Code: "internal_error", Message: "temporary"}
		} else {
			result.Receipt = &domain.CompletionReceipt{
				JobID: completion.JobID,
				State: domain.StateSucceeded,
			}
		}
		return api.CompleteAttemptsResponse{Results: []api.CompletionResult{result}}, nil
	}
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID:            "worker-1",
		Slots:               1,
		CompletionBatchSize: 1,
		CompletionBatchWait: time.Nanosecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{testLease("retry-completion", 1)},
	}}
	_ = receive(t, runner.started)
	runner.results <- successfulResult()
	first := receive(t, protocol.completionBatches)
	second := receive(t, protocol.completionBatches)
	if len(first) != 1 || len(second) != 1 ||
		first[0].JobID != "retry-completion" ||
		second[0].JobID != "retry-completion" {
		t.Fatalf("completion batches = %+v, %+v", first, second)
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerFallsBackToSingleCompletionProtocol(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(
		t,
		struct{ Protocol }{Protocol: protocol},
		runner,
		ticker,
		Config{WorkerID: "worker-1", Slots: 1},
	)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{testLease("single-completion", 1)},
	}}
	_ = receive(t, runner.started)
	runner.results <- successfulResult()
	completion := receive(t, protocol.completions)
	if completion.JobID != "single-completion" {
		t.Fatalf("completion = %+v", completion)
	}
	select {
	case batch := <-protocol.completionBatches:
		t.Fatalf("unexpected completion batch: %+v", batch)
	default:
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerReregistersAfterAcquireLosesRegistration(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID: "worker-1",
		Slots:    3,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	initialRegistration := receive(t, protocol.registerRequests)
	if initialRegistration.WorkerID != "worker-1" || initialRegistration.Slots != 3 {
		t.Fatalf("initial registration = %+v", initialRegistration)
	}
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{err: missingWorkerError(
		"/v1/workers/worker-1/leases/acquire",
	)}

	restartedRegistration := receive(t, protocol.registerRequests)
	if restartedRegistration != initialRegistration {
		t.Fatalf(
			"restart registration = %+v, want %+v",
			restartedRegistration,
			initialRegistration,
		)
	}
	resumedAcquire := receive(t, protocol.acquireRequests)
	if resumedAcquire.AvailableSlots != 3 {
		t.Fatalf("available slots = %d, want 3", resumedAcquire.AvailableSlots)
	}
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{testLease("after-restart", 1)},
	}}
	if started := receive(t, runner.started); started.IdempotencyKey != "after-restart-key" {
		t.Fatalf("started = %+v", started)
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRetriesFencedStartAcrossRepeatedMissingRegistration(t *testing.T) {
	protocol := newFakeProtocol()
	var startCalls atomic.Int32
	protocol.start = func(api.StartAttemptRequest) error {
		if startCalls.Add(1) <= 2 {
			return missingWorkerError("/v1/workers/worker-1/attempts/start")
		}
		return nil
	}
	runner := newBlockingExecutor()
	worker := newTestWorker(
		t,
		struct{ Protocol }{Protocol: protocol},
		runner,
		newManualTicker(),
		Config{WorkerID: "worker-1", Slots: 1},
	)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.registerRequests)
	_ = receive(t, protocol.acquireRequests)
	lease := testLease("repeated-start", 1)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{lease},
	}}
	firstStart := receive(t, protocol.starts)
	_ = receive(t, protocol.registerRequests)
	secondStart := receive(t, protocol.starts)
	_ = receive(t, protocol.registerRequests)
	thirdStart := receive(t, protocol.starts)
	if !sameLease(firstStart, secondStart) || !sameLease(secondStart, thirdStart) {
		t.Fatalf("start retries changed fencing: %+v, %+v, %+v", firstStart, secondStart, thirdStart)
	}
	if started := receive(t, runner.started); started.IdempotencyKey != lease.IdempotencyKey {
		t.Fatalf("execution = %+v", started)
	}
	assertNoValue(t, protocol.registerRequests, "unexpected fourth registration")

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerCoalescesConcurrentMissingRegistrationResponses(t *testing.T) {
	protocol := newFakeProtocol()
	var registerCalls atomic.Int32
	reregistering := make(chan struct{})
	allowRegistration := make(chan struct{})
	protocol.register = func(
		ctx context.Context,
		_ api.RegisterWorkerRequest,
	) (api.RegisterWorkerResponse, error) {
		if registerCalls.Add(1) == 1 {
			return protocol.registerResponse, nil
		}
		select {
		case <-reregistering:
		default:
			close(reregistering)
		}
		select {
		case <-allowRegistration:
			return protocol.registerResponse, nil
		case <-ctx.Done():
			return api.RegisterWorkerResponse{}, ctx.Err()
		}
	}
	var registrationMissing atomic.Bool
	completionBlocked := make(chan struct{})
	heartbeatBlocked := make(chan struct{})
	releaseMissingResponses := make(chan struct{})
	protocol.heartbeat = func(request api.HeartbeatRequest) (api.HeartbeatResponse, error) {
		if registrationMissing.Load() {
			close(heartbeatBlocked)
			<-releaseMissingResponses
			return api.HeartbeatResponse{}, missingWorkerError(
				"/v1/workers/worker-1/heartbeats",
			)
		}
		results := make([]api.HeartbeatResult, len(request.Leases))
		for index, reference := range request.Leases {
			results[index] = api.HeartbeatResult{JobID: reference.JobID, Accepted: true}
		}
		return api.HeartbeatResponse{Results: results}, nil
	}
	protocol.completeBatch = func(
		request api.CompleteAttemptsRequest,
	) (api.CompleteAttemptsResponse, error) {
		if registrationMissing.Load() {
			close(completionBlocked)
			<-releaseMissingResponses
			return api.CompleteAttemptsResponse{}, missingWorkerError(
				"/v1/workers/worker-1/attempts/complete-batch",
			)
		}
		results := make([]api.CompletionResult, len(request.Completions))
		for index, completion := range request.Completions {
			results[index] = api.CompletionResult{
				JobID: completion.JobID,
				Receipt: &domain.CompletionReceipt{
					JobID: completion.JobID,
					State: domain.StateSucceeded,
				},
			}
		}
		return api.CompleteAttemptsResponse{Results: results}, nil
	}

	runner := newBlockingExecutor()
	ticker := newManualTicker()
	worker := newTestWorker(t, protocol, runner, ticker, Config{
		WorkerID:            "worker-1",
		Slots:               2,
		CompletionBatchSize: 1,
		CompletionBatchWait: time.Nanosecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.registerRequests)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{response: api.AcquireLeasesResponse{
		Leases: []domain.Lease{
			testLease("concurrent-a", 1),
			testLease("concurrent-b", 1),
		},
	}}
	_ = receive(t, runner.started)
	_ = receive(t, runner.started)

	registrationMissing.Store(true)
	runner.results <- successfulResult()
	firstCompletion := receive(t, protocol.completionBatches)
	<-completionBlocked
	ticker.Tick()
	firstHeartbeat := receive(t, protocol.heartbeats)
	<-heartbeatBlocked
	close(releaseMissingResponses)
	<-reregistering
	restartedRegistration := receive(t, protocol.registerRequests)
	if restartedRegistration.Slots != 2 {
		t.Fatalf("restart registration = %+v", restartedRegistration)
	}
	assertNoValue(t, protocol.registerRequests, "duplicate restart registration")

	registrationMissing.Store(false)
	close(allowRegistration)
	secondCompletion := receive(t, protocol.completionBatches)
	secondHeartbeat := receive(t, protocol.heartbeats)
	if len(firstCompletion) != 1 || len(secondCompletion) != 1 ||
		!sameLease(firstCompletion[0].LeaseRef, secondCompletion[0].LeaseRef) {
		t.Fatalf("completion retry changed fencing: first=%+v second=%+v", firstCompletion, secondCompletion)
	}
	if len(firstHeartbeat.Leases) != 2 || len(secondHeartbeat.Leases) == 0 {
		t.Fatalf("heartbeats before=%+v after=%+v", firstHeartbeat, secondHeartbeat)
	}
	if registerCalls.Load() != 2 {
		t.Fatalf("register calls = %d, want 2", registerCalls.Load())
	}
	assertNoValue(t, runner.canceled, "active lease cancellation")

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerCancellationStopsReregistration(t *testing.T) {
	protocol := newFakeProtocol()
	var registerCalls atomic.Int32
	reregistering := make(chan struct{})
	registerCanceled := make(chan struct{})
	protocol.register = func(
		ctx context.Context,
		_ api.RegisterWorkerRequest,
	) (api.RegisterWorkerResponse, error) {
		if registerCalls.Add(1) == 1 {
			return protocol.registerResponse, nil
		}
		close(reregistering)
		<-ctx.Done()
		close(registerCanceled)
		return api.RegisterWorkerResponse{}, ctx.Err()
	}
	worker := newTestWorker(t, protocol, newBlockingExecutor(), newManualTicker(), Config{
		WorkerID: "worker-1",
		Slots:    1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)
	_ = receive(t, protocol.registerRequests)
	_ = receive(t, protocol.acquireRequests)
	protocol.acquireResults <- acquireResult{err: missingWorkerError(
		"/v1/workers/worker-1/leases/acquire",
	)}
	<-reregistering
	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
	<-registerCanceled
	if registerCalls.Load() != 2 {
		t.Fatalf("register calls = %d, want 2", registerCalls.Load())
	}
}

func TestWorkerRecoversAcrossServerRestart(t *testing.T) {
	var mu sync.Mutex
	registered := false
	registrations := 0
	registrationSlots := make([]int, 0, 2)
	firstLeased := false
	secondLeased := false
	firstCompleted := false
	var missingOnce sync.Once
	missingSeen := make(chan struct{})
	var reregisteredOnce sync.Once
	reregistered := make(chan struct{})
	var completedOnce sync.Once
	completed := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v1/workers/register":
			var payload api.RegisterWorkerRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode registration: %v", err)
			}
			mu.Lock()
			registered = true
			registrations++
			registrationSlots = append(registrationSlots, payload.Slots)
			count := registrations
			mu.Unlock()
			if count > 1 {
				reregisteredOnce.Do(func() { close(reregistered) })
			}
			_ = json.NewEncoder(writer).Encode(api.RegisterWorkerResponse{
				WorkerID:       payload.WorkerID,
				HeartbeatEvery: time.Second,
			})
		case "/v1/workers/worker-1/leases/acquire":
			mu.Lock()
			if !registered {
				mu.Unlock()
				writeMissingWorker(writer)
				return
			}
			var leases []domain.Lease
			switch {
			case !firstLeased:
				firstLeased = true
				leases = []domain.Lease{testLease("restart-first", 1)}
			case firstCompleted && !secondLeased:
				secondLeased = true
				leases = []domain.Lease{testLease("restart-second", 1)}
			}
			mu.Unlock()
			_ = json.NewEncoder(writer).Encode(api.AcquireLeasesResponse{Leases: leases})
		case "/v1/workers/worker-1/attempts/start":
			mu.Lock()
			isRegistered := registered
			mu.Unlock()
			if !isRegistered {
				writeMissingWorker(writer)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
		case "/v1/workers/worker-1/attempts/complete":
			var payload api.CompleteAttemptRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode completion: %v", err)
			}
			mu.Lock()
			if !registered {
				mu.Unlock()
				missingOnce.Do(func() { close(missingSeen) })
				writeMissingWorker(writer)
				return
			}
			if payload.JobID == "restart-first" {
				firstCompleted = true
			}
			mu.Unlock()
			completedOnce.Do(func() { close(completed) })
			_ = json.NewEncoder(writer).Encode(domain.CompletionReceipt{
				JobID: payload.JobID,
				State: domain.StateSucceeded,
			})
		case "/v1/workers/worker-1/heartbeats":
			mu.Lock()
			isRegistered := registered
			mu.Unlock()
			if !isRegistered {
				writeMissingWorker(writer)
				return
			}
			_ = json.NewEncoder(writer).Encode(api.HeartbeatResponse{Results: []api.HeartbeatResult{}})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	runner := newBlockingExecutor()
	worker := newTestWorker(
		t,
		struct{ Protocol }{Protocol: client},
		runner,
		newManualTicker(),
		Config{WorkerID: "worker-1", Slots: 1},
	)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := runWorker(worker, ctx)

	first := receive(t, runner.started)
	if first.IdempotencyKey != "restart-first-key" {
		t.Fatalf("first execution = %+v", first)
	}
	mu.Lock()
	registered = false
	mu.Unlock()
	runner.results <- successfulResult()
	<-missingSeen
	<-reregistered
	<-completed

	second := receive(t, runner.started)
	if second.IdempotencyKey != "restart-second-key" {
		t.Fatalf("second execution = %+v", second)
	}
	mu.Lock()
	gotRegistrations := registrations
	gotSlots := append([]int(nil), registrationSlots...)
	mu.Unlock()
	if gotRegistrations != 2 || !reflect.DeepEqual(gotSlots, []int{1, 1}) {
		t.Fatalf("registrations = %d, slots = %v", gotRegistrations, gotSlots)
	}

	cancel()
	if err := receive(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerRejectsInvalidConfiguration(t *testing.T) {
	protocol := newFakeProtocol()
	runner := newBlockingExecutor()
	_, err := New(protocol, runner, Config{WorkerID: "worker", Slots: 0})
	if err == nil {
		t.Fatal("expected invalid slot count error")
	}
	_, err = New(protocol, runner, Config{WorkerID: "", Slots: 1})
	if err == nil {
		t.Fatal("expected missing worker ID error")
	}
}

func newTestWorker(
	t *testing.T,
	protocol Protocol,
	runner executor.Executor,
	ticker *manualTicker,
	config Config,
) *Worker {
	t.Helper()
	worker, err := New(protocol, runner, config, WithTickerFactory(func(time.Duration) Ticker {
		return ticker
	}))
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func runWorker(worker *Worker, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()
	return done
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for test event")
		var zero T
		return zero
	}
}

func assertNoValue[T any](t *testing.T, channel <-chan T, description string) {
	t.Helper()
	select {
	case value := <-channel:
		t.Fatalf("%s: %+v", description, value)
	default:
	}
}

func missingWorkerError(path string) error {
	return &APIError{
		StatusCode: http.StatusNotFound,
		Code:       "not_found",
		Message:    "worker is not registered",
		Method:     http.MethodPost,
		Path:       path,
	}
}

func writeMissingWorker(writer http.ResponseWriter) {
	writer.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(writer, `{"code":"not_found","message":"worker is not registered"}`)
}

func testLease(jobID string, slotCost int) domain.Lease {
	return domain.Lease{
		JobID:          jobID,
		AttemptNo:      1,
		Generation:     1,
		Token:          jobID + "-token",
		WorkerID:       "worker-1",
		IdempotencyKey: jobID + "-key",
		SlotCost:       slotCost,
		Payload:        domain.Payload{Type: domain.PayloadNoop},
	}
}

func successfulResult() executor.Result {
	return executor.Result{
		Success:      true,
		ExitCode:     0,
		OutputDigest: "digest",
	}
}

type manualTicker struct {
	channel chan time.Time
	once    sync.Once
}

func newManualTicker() *manualTicker {
	return &manualTicker{channel: make(chan time.Time, 16)}
}

func (t *manualTicker) C() <-chan time.Time {
	return t.channel
}

func (t *manualTicker) Stop() {
	t.once.Do(func() {})
}

func (t *manualTicker) Tick() {
	t.channel <- time.Time{}
}

type fakeProtocol struct {
	registerResponse  api.RegisterWorkerResponse
	registerErr       error
	registerRequests  chan api.RegisterWorkerRequest
	acquireRequests   chan api.AcquireLeasesRequest
	acquireResults    chan acquireResult
	starts            chan domain.LeaseRef
	startBatches      chan []domain.LeaseRef
	heartbeats        chan api.HeartbeatRequest
	completions       chan domain.Completion
	completionBatches chan []domain.Completion
	start             func(api.StartAttemptRequest) error
	startBatch        func(api.StartAttemptsRequest) (api.StartAttemptsResponse, error)
	heartbeat         func(api.HeartbeatRequest) (api.HeartbeatResponse, error)
	complete          func(api.CompleteAttemptRequest) (domain.CompletionReceipt, error)
	completeBatch     func(api.CompleteAttemptsRequest) (api.CompleteAttemptsResponse, error)
	register          func(context.Context, api.RegisterWorkerRequest) (api.RegisterWorkerResponse, error)
}

func newFakeProtocol() *fakeProtocol {
	return &fakeProtocol{
		registerResponse: api.RegisterWorkerResponse{
			WorkerID:       "worker-1",
			HeartbeatEvery: time.Second,
		},
		registerRequests:  make(chan api.RegisterWorkerRequest, 16),
		acquireRequests:   make(chan api.AcquireLeasesRequest, 16),
		acquireResults:    make(chan acquireResult, 16),
		starts:            make(chan domain.LeaseRef, 16),
		startBatches:      make(chan []domain.LeaseRef, 16),
		heartbeats:        make(chan api.HeartbeatRequest, 16),
		completions:       make(chan domain.Completion, 16),
		completionBatches: make(chan []domain.Completion, 16),
		start: func(api.StartAttemptRequest) error {
			return nil
		},
		heartbeat: func(request api.HeartbeatRequest) (api.HeartbeatResponse, error) {
			results := make([]api.HeartbeatResult, len(request.Leases))
			for index, reference := range request.Leases {
				results[index] = api.HeartbeatResult{JobID: reference.JobID, Accepted: true}
			}
			return api.HeartbeatResponse{Results: results}, nil
		},
		complete: func(request api.CompleteAttemptRequest) (domain.CompletionReceipt, error) {
			return domain.CompletionReceipt{JobID: request.JobID}, nil
		},
	}
}

func (p *fakeProtocol) Register(
	ctx context.Context,
	request api.RegisterWorkerRequest,
) (api.RegisterWorkerResponse, error) {
	select {
	case p.registerRequests <- request:
	case <-ctx.Done():
		return api.RegisterWorkerResponse{}, ctx.Err()
	}
	if p.register != nil {
		return p.register(ctx, request)
	}
	return p.registerResponse, p.registerErr
}

func (p *fakeProtocol) AcquireLeases(
	ctx context.Context,
	_ string,
	request api.AcquireLeasesRequest,
) (api.AcquireLeasesResponse, error) {
	select {
	case p.acquireRequests <- request:
	case <-ctx.Done():
		return api.AcquireLeasesResponse{}, ctx.Err()
	}
	select {
	case result := <-p.acquireResults:
		return result.response, result.err
	case <-ctx.Done():
		return api.AcquireLeasesResponse{}, ctx.Err()
	}
}

func (p *fakeProtocol) Heartbeat(
	ctx context.Context,
	_ string,
	request api.HeartbeatRequest,
) (api.HeartbeatResponse, error) {
	select {
	case p.heartbeats <- request:
	case <-ctx.Done():
		return api.HeartbeatResponse{}, ctx.Err()
	}
	return p.heartbeat(request)
}

func (p *fakeProtocol) StartAttempt(
	ctx context.Context,
	_ string,
	request api.StartAttemptRequest,
) error {
	select {
	case p.starts <- request.LeaseRef:
	case <-ctx.Done():
		return ctx.Err()
	}
	return p.start(request)
}

func (p *fakeProtocol) StartAttempts(
	ctx context.Context,
	_ string,
	request api.StartAttemptsRequest,
) (api.StartAttemptsResponse, error) {
	refs := append([]domain.LeaseRef(nil), request.Leases...)
	select {
	case p.startBatches <- refs:
	case <-ctx.Done():
		return api.StartAttemptsResponse{}, ctx.Err()
	}
	for _, ref := range refs {
		select {
		case p.starts <- ref:
		case <-ctx.Done():
			return api.StartAttemptsResponse{}, ctx.Err()
		}
	}
	if p.startBatch != nil {
		return p.startBatch(request)
	}
	results := make([]api.StartResult, len(refs))
	for index, ref := range refs {
		err := p.start(api.StartAttemptRequest{LeaseRef: ref})
		results[index] = api.StartResult{JobID: ref.JobID, Started: err == nil}
		if err != nil {
			results[index].Error = &api.ErrorResponse{
				Code:    "stale_lease",
				Message: err.Error(),
			}
		}
	}
	return api.StartAttemptsResponse{Results: results}, nil
}

func (p *fakeProtocol) CompleteAttempt(
	ctx context.Context,
	_ string,
	request api.CompleteAttemptRequest,
) (domain.CompletionReceipt, error) {
	select {
	case p.completions <- request.Completion:
	case <-ctx.Done():
		return domain.CompletionReceipt{}, ctx.Err()
	}
	return p.complete(request)
}

func (p *fakeProtocol) CompleteAttempts(
	ctx context.Context,
	_ string,
	request api.CompleteAttemptsRequest,
) (api.CompleteAttemptsResponse, error) {
	batch := append([]domain.Completion(nil), request.Completions...)
	select {
	case p.completionBatches <- batch:
	case <-ctx.Done():
		return api.CompleteAttemptsResponse{}, ctx.Err()
	}
	for _, completion := range request.Completions {
		select {
		case p.completions <- completion:
		case <-ctx.Done():
			return api.CompleteAttemptsResponse{}, ctx.Err()
		}
	}
	if p.completeBatch != nil {
		return p.completeBatch(request)
	}
	results := make([]api.CompletionResult, len(request.Completions))
	for index, completion := range request.Completions {
		receipt, err := p.complete(api.CompleteAttemptRequest{Completion: completion})
		results[index].JobID = completion.JobID
		if err != nil {
			results[index].Error = &api.ErrorResponse{
				Code:    "internal_error",
				Message: err.Error(),
			}
			continue
		}
		results[index].Receipt = &receipt
	}
	return api.CompleteAttemptsResponse{Results: results}, nil
}

type blockingExecutor struct {
	started  chan executor.Request
	results  chan executor.Result
	canceled chan string
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{
		started:  make(chan executor.Request, 16),
		results:  make(chan executor.Result, 16),
		canceled: make(chan string, 16),
	}
}

func (e *blockingExecutor) Execute(ctx context.Context, request executor.Request) executor.Result {
	select {
	case e.started <- request:
	case <-ctx.Done():
		return canceledResult(ctx.Err())
	}
	select {
	case result := <-e.results:
		return result
	case <-ctx.Done():
		e.canceled <- request.IdempotencyKey
		return canceledResult(ctx.Err())
	}
}

func canceledResult(err error) executor.Result {
	if err == nil {
		err = errors.New("canceled")
	}
	return executor.Result{
		Retryable: true,
		ExitCode:  -1,
		Failure: &domain.Failure{
			Class:   "canceled",
			Message: err.Error(),
		},
	}
}
