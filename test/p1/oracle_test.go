package p1_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	qa "github.com/rajeev-chaurasia/rail-yard/qa/p1"
)

func TestOracleDurableSubmitAcrossRestart(t *testing.T) {
	clock := qa.NewFakeClock(testEpoch())
	model := qa.NewModel(clock)
	submission := modelSubmission("durable")

	accepted, duplicate, err := model.Submit(submission)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if duplicate {
		t.Fatal("first submit reported duplicate")
	}

	repeated, duplicate, err := model.Submit(submission)
	if err != nil {
		t.Fatalf("repeat submit: %v", err)
	}
	if !duplicate || repeated.ID != accepted.ID {
		t.Fatalf("repeat submit = %#v duplicate=%t, want job %s", repeated, duplicate, accepted.ID)
	}

	conflict := submission
	conflict.RequestDigest = "different-digest"
	if _, _, err := model.Submit(conflict); !errors.Is(err, qa.ErrIdempotencyConflict) {
		t.Fatalf("conflicting submit error = %v, want %v", err, qa.ErrIdempotencyConflict)
	}

	restarted := model.Restart(clock)
	afterRestart, duplicate, err := restarted.Submit(submission)
	if err != nil {
		t.Fatalf("repeat submit after restart: %v", err)
	}
	if !duplicate || afterRestart.ID != accepted.ID {
		t.Fatalf("post-restart submit = %#v duplicate=%t, want job %s",
			afterRestart, duplicate, accepted.ID)
	}
	if err := restarted.Validate(); err != nil {
		t.Fatalf("restarted oracle invariants: %v", err)
	}
}

func TestOracleLeaseStartHeartbeatComplete(t *testing.T) {
	clock := qa.NewFakeClock(testEpoch())
	model := qa.NewModel(clock)
	job := mustSubmit(t, model, modelSubmission("lifecycle"))
	ttl := 3 * time.Second
	lease := mustAcquire(t, model, "worker-a", ttl)

	if lease.JobID != job.ID || lease.AttemptNo != 1 || lease.Generation != 1 {
		t.Fatalf("first lease = %#v, want first attempt and generation for %s", lease, job.ID)
	}
	if err := model.Start("worker-a", lease.ModelLeaseRef); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := model.Start("worker-a", lease.ModelLeaseRef); err != nil {
		t.Fatalf("duplicate start: %v", err)
	}

	clock.Advance(time.Second)
	heartbeat := model.Heartbeat("worker-a", []qa.ModelLeaseRef{lease.ModelLeaseRef}, ttl)
	if len(heartbeat) != 1 || !heartbeat[0].Accepted {
		t.Fatalf("heartbeat = %#v, want accepted", heartbeat)
	}
	if want := clock.Now().Add(ttl); !heartbeat[0].ExpiresAt.Equal(want) {
		t.Fatalf("heartbeat expiry = %s, want %s", heartbeat[0].ExpiresAt, want)
	}

	completion := successfulModelCompletion(lease)
	first, err := model.Complete(completion)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if first.State != qa.ModelSucceeded || first.Duplicate {
		t.Fatalf("first receipt = %#v", first)
	}
	second, err := model.Complete(completion)
	if err != nil {
		t.Fatalf("duplicate complete: %v", err)
	}
	if !second.Duplicate ||
		second.JobID != first.JobID ||
		second.StateVersion != first.StateVersion ||
		!second.CommittedAt.Equal(first.CommittedAt) {
		t.Fatalf("duplicate receipt = %#v, first = %#v", second, first)
	}

	conflict := completion
	conflict.Success = false
	if _, err := model.Complete(conflict); !errors.Is(err, qa.ErrTerminalConflict) {
		t.Fatalf("conflicting completion error = %v, want %v", err, qa.ErrTerminalConflict)
	}
	if err := model.Validate(); err != nil {
		t.Fatalf("oracle invariants: %v", err)
	}
}

func TestOracleReapReacquireFencesStaleLease(t *testing.T) {
	clock := qa.NewFakeClock(testEpoch())
	model := qa.NewModel(clock)
	job := mustSubmit(t, model, modelSubmission("reacquire"))
	ttl := 3 * time.Second
	first := mustAcquire(t, model, "worker-old", ttl)
	if err := model.Start("worker-old", first.ModelLeaseRef); err != nil {
		t.Fatalf("start first lease: %v", err)
	}

	clock.Advance(ttl)
	if _, err := model.Complete(successfulModelCompletion(first)); !errors.Is(err, qa.ErrStaleLease) {
		t.Fatalf("completion at expiry error = %v, want %v", err, qa.ErrStaleLease)
	}
	reaped := model.ReapExpired(10)
	if len(reaped) != 1 ||
		reaped[0].JobID != job.ID ||
		reaped[0].Generation != first.Generation {
		t.Fatalf("reaped = %#v, want first lease", reaped)
	}

	staleHeartbeat := model.Heartbeat(
		"worker-old",
		[]qa.ModelLeaseRef{first.ModelLeaseRef},
		ttl,
	)
	if len(staleHeartbeat) != 1 || staleHeartbeat[0].Accepted {
		t.Fatalf("stale heartbeat = %#v, want rejected", staleHeartbeat)
	}
	if err := model.Start("worker-old", first.ModelLeaseRef); !errors.Is(err, qa.ErrStaleLease) {
		t.Fatalf("stale start error = %v, want %v", err, qa.ErrStaleLease)
	}

	second := mustAcquire(t, model, "worker-new", ttl)
	if second.AttemptNo <= first.AttemptNo || second.Generation <= first.Generation {
		t.Fatalf("successor did not advance fence: first=%#v second=%#v", first, second)
	}
	if _, err := model.Complete(successfulModelCompletion(first)); !errors.Is(err, qa.ErrStaleLease) {
		t.Fatalf("stale completion error = %v, want %v", err, qa.ErrStaleLease)
	}
	if err := model.Start("worker-new", second.ModelLeaseRef); err != nil {
		t.Fatalf("start successor: %v", err)
	}
	if _, err := model.Complete(successfulModelCompletion(second)); err != nil {
		t.Fatalf("complete successor: %v", err)
	}

	snapshot := model.Snapshot()
	if len(snapshot.Jobs) != 1 ||
		snapshot.Jobs[0].CanonicalReceipt == nil ||
		snapshot.Jobs[0].CanonicalReceipt.State != qa.ModelSucceeded {
		t.Fatalf("canonical outcome = %#v", snapshot.Jobs)
	}
	if len(snapshot.Jobs[0].Attempts) != 2 {
		t.Fatalf("attempts = %#v, want expired predecessor and successful successor",
			snapshot.Jobs[0].Attempts)
	}
	if err := model.Validate(); err != nil {
		t.Fatalf("oracle invariants: %v", err)
	}
}

func TestOracleConcurrentCompletionCreatesOneCanonicalOutcome(t *testing.T) {
	clock := qa.NewFakeClock(testEpoch())
	model := qa.NewModel(clock)
	mustSubmit(t, model, modelSubmission("concurrent-completion"))
	lease := mustAcquire(t, model, "worker-a", 3*time.Second)
	if err := model.Start("worker-a", lease.ModelLeaseRef); err != nil {
		t.Fatalf("start: %v", err)
	}
	completion := successfulModelCompletion(lease)

	type result struct {
		receipt qa.ModelReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			receipt, err := model.Complete(completion)
			results <- result{receipt: receipt, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	duplicates := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("complete: %v", result.err)
		}
		if result.receipt.Duplicate {
			duplicates++
		}
	}
	if duplicates != 1 {
		t.Fatalf("duplicate receipts = %d, want 1", duplicates)
	}
	snapshot := model.Snapshot()
	if len(snapshot.Jobs) != 1 || snapshot.Jobs[0].CanonicalReceipt == nil {
		t.Fatalf("snapshot has no canonical outcome: %#v", snapshot)
	}
	if err := model.Validate(); err != nil {
		t.Fatalf("oracle invariants: %v", err)
	}
}

func TestFakeClockRejectsBackwardsTime(t *testing.T) {
	clock := qa.NewFakeClock(testEpoch())
	clock.Advance(time.Second)
	if err := clock.Set(testEpoch()); err == nil {
		t.Fatal("backwards clock set succeeded")
	}
}

func modelSubmission(key string) qa.ModelSubmission {
	return qa.ModelSubmission{
		TenantID:      "tenant-p1",
		SubmissionKey: key,
		RequestDigest: "digest-" + key,
		SlotCost:      1,
		MaxAttempts:   3,
		Retryable:     true,
	}
}

func mustSubmit(t *testing.T, model *qa.Model, submission qa.ModelSubmission) qa.ModelJob {
	t.Helper()
	job, duplicate, err := model.Submit(submission)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if duplicate {
		t.Fatal("first submit reported duplicate")
	}
	return job
}

func mustAcquire(
	t *testing.T,
	model *qa.Model,
	workerID string,
	ttl time.Duration,
) qa.ModelLease {
	t.Helper()
	leases := model.Acquire(workerID, 1, 1, ttl)
	if len(leases) != 1 {
		t.Fatalf("acquire = %#v, want one lease", leases)
	}
	return leases[0]
}

func successfulModelCompletion(lease qa.ModelLease) qa.ModelCompletion {
	return qa.ModelCompletion{
		ModelLeaseRef: lease.ModelLeaseRef,
		WorkerID:      lease.WorkerID,
		Success:       true,
		OutputDigest:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
}

func testEpoch() time.Time {
	return time.Date(2032, time.March, 14, 15, 9, 26, 0, time.UTC)
}
