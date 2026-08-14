package p1

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

type Fixture interface {
	Store() store.Store
	Reader() *sql.DB
	Crash(context.Context) error
	Reopen(context.Context) error
	Close() error
}

type FixtureFactory func(testing.TB, *FakeClock) (Fixture, error)

type ContractSuite struct {
	NewFixture   FixtureFactory
	LeaseTTL     time.Duration
	ReconcileSQL ReconcileSQL
}

func (suite ContractSuite) Run(t *testing.T) {
	t.Helper()
	if suite.NewFixture == nil {
		t.Fatal("p1 contract suite requires NewFixture")
	}

	t.Run("durable submit is idempotent across reopen", suite.testDurableSubmit)
	t.Run("lease start heartbeat and complete", suite.testLeaseLifecycle)
	t.Run("stale generation is fenced after reacquire", suite.testStaleFencing)
	t.Run("running lease survives crash then reaps", suite.testCrashReopen)
	t.Run("duplicate completion has one canonical row", suite.testCanonicalCompletion)
}

func (suite ContractSuite) testDurableSubmit(t *testing.T) {
	fixture, clock := suite.openFixture(t)
	ctx := context.Background()
	submission := newSubmission("durable-submit", "digest-durable-submit")

	job, duplicate, err := fixture.Store().SubmitJob(ctx, submission, clock.Now())
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	if duplicate {
		t.Fatal("first submit reported duplicate")
	}
	assertAcceptedJob(t, job)

	if err := fixture.Crash(ctx); err != nil {
		t.Fatalf("crash fixture: %v", err)
	}
	if err := fixture.Reopen(ctx); err != nil {
		t.Fatalf("reopen fixture: %v", err)
	}

	reopened, err := fixture.Store().GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get accepted job after reopen: %v", err)
	}
	if !reflect.DeepEqual(reopened, job) {
		t.Fatalf("durable job changed across reopen:\n got: %#v\nwant: %#v", reopened, job)
	}

	replayed, duplicate, err := fixture.Store().SubmitJob(ctx, submission, clock.Now())
	if err != nil {
		t.Fatalf("replay identical submit: %v", err)
	}
	if !duplicate {
		t.Fatal("identical submit did not report duplicate")
	}
	if !reflect.DeepEqual(replayed, job) {
		t.Fatalf("idempotent response changed:\n got: %#v\nwant: %#v", replayed, job)
	}

	conflicting := submission
	conflicting.RequestDigest = "digest-conflict"
	if _, _, err := fixture.Store().SubmitJob(ctx, conflicting, clock.Now()); !errors.Is(
		err,
		domain.ErrIdempotencyConflict,
	) {
		t.Fatalf("conflicting submit error = %v, want %v", err, domain.ErrIdempotencyConflict)
	}

	suite.assertReconciled(t, fixture, []string{job.ID}, false)
}

func (suite ContractSuite) testLeaseLifecycle(t *testing.T) {
	fixture, clock := suite.openFixture(t)
	ctx := context.Background()
	ttl := suite.leaseTTL()
	job := submitOne(t, ctx, fixture.Store(), clock, "lease-lifecycle")

	lease := acquireOne(t, ctx, fixture.Store(), clock, "worker-a", ttl)
	assertLeaseForJob(t, lease, job.ID, "worker-a", 1, 1)
	ref := leaseRef(lease)

	if err := fixture.Store().MarkRunning(ctx, "worker-a", ref, clock.Now()); err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	if err := fixture.Store().MarkRunning(ctx, "worker-a", ref, clock.Now()); err != nil {
		t.Fatalf("duplicate start was not idempotent: %v", err)
	}

	clock.Advance(ttl / 2)
	results, err := fixture.Store().Heartbeat(
		ctx,
		"worker-a",
		[]domain.LeaseRef{ref},
		clock.Now(),
		ttl,
	)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if len(results) != 1 || !results[0].Accepted {
		t.Fatalf("heartbeat results = %#v, want one accepted result", results)
	}
	wantExpiry := clock.Now().Add(ttl)
	if !results[0].ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("heartbeat expiry = %s, want %s", results[0].ExpiresAt, wantExpiry)
	}

	completion := successfulCompletion(lease)
	receipt, err := fixture.Store().Complete(ctx, completion, clock.Now())
	if err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	assertSuccessReceipt(t, receipt, job.ID, false)

	duplicate, err := fixture.Store().Complete(ctx, completion, clock.Now())
	if err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	assertStableDuplicateReceipt(t, receipt, duplicate)
	suite.assertReconciled(t, fixture, []string{job.ID}, true)
}

func (suite ContractSuite) testStaleFencing(t *testing.T) {
	fixture, clock := suite.openFixture(t)
	ctx := context.Background()
	ttl := suite.leaseTTL()
	job := submitOne(t, ctx, fixture.Store(), clock, "stale-fencing")

	first := acquireOne(t, ctx, fixture.Store(), clock, "worker-old", ttl)
	firstRef := leaseRef(first)
	if err := fixture.Store().MarkRunning(ctx, "worker-old", firstRef, clock.Now()); err != nil {
		t.Fatalf("start first attempt: %v", err)
	}

	clock.Advance(ttl)
	reaped, err := fixture.Store().ReapExpired(ctx, clock.Now(), 10)
	if err != nil {
		t.Fatalf("reap expired lease: %v", err)
	}
	if len(reaped) != 1 ||
		reaped[0].JobID != job.ID ||
		reaped[0].Generation != first.Generation ||
		reaped[0].OldWorkerID != "worker-old" {
		t.Fatalf("reaped leases = %#v, want expired first lease", reaped)
	}
	if _, err := fixture.Store().PromoteDue(ctx, clock.Now(), 10); err != nil {
		t.Fatalf("promote due reaped job: %v", err)
	}

	second := acquireOne(t, ctx, fixture.Store(), clock, "worker-new", ttl)
	if second.AttemptNo <= first.AttemptNo || second.Generation <= first.Generation {
		t.Fatalf("successor lease did not fence predecessor: first=%#v second=%#v", first, second)
	}

	heartbeat, err := fixture.Store().Heartbeat(
		ctx,
		"worker-old",
		[]domain.LeaseRef{firstRef},
		clock.Now(),
		ttl,
	)
	if err != nil {
		t.Fatalf("stale heartbeat returned transport error: %v", err)
	}
	if len(heartbeat) != 1 || heartbeat[0].Accepted {
		t.Fatalf("stale heartbeat results = %#v, want one rejected result", heartbeat)
	}
	if err := fixture.Store().MarkRunning(
		ctx,
		"worker-old",
		firstRef,
		clock.Now(),
	); !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("stale start error = %v, want %v", err, domain.ErrStaleLease)
	}
	if _, err := fixture.Store().Complete(
		ctx,
		successfulCompletion(first),
		clock.Now(),
	); !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("stale completion error = %v, want %v", err, domain.ErrStaleLease)
	}

	if err := fixture.Store().MarkRunning(
		ctx,
		"worker-new",
		leaseRef(second),
		clock.Now(),
	); err != nil {
		t.Fatalf("start successor attempt: %v", err)
	}
	receipt, err := fixture.Store().Complete(ctx, successfulCompletion(second), clock.Now())
	if err != nil {
		t.Fatalf("complete successor attempt: %v", err)
	}
	assertSuccessReceipt(t, receipt, job.ID, false)
	suite.assertReconciled(t, fixture, []string{job.ID}, true)
}

func (suite ContractSuite) testCrashReopen(t *testing.T) {
	fixture, clock := suite.openFixture(t)
	ctx := context.Background()
	ttl := suite.leaseTTL()
	job := submitOne(t, ctx, fixture.Store(), clock, "crash-reopen")
	first := acquireOne(t, ctx, fixture.Store(), clock, "worker-crashed", ttl)
	firstRef := leaseRef(first)

	if err := fixture.Store().MarkRunning(
		ctx,
		"worker-crashed",
		firstRef,
		clock.Now(),
	); err != nil {
		t.Fatalf("start pre-crash attempt: %v", err)
	}
	if err := fixture.Crash(ctx); err != nil {
		t.Fatalf("crash fixture: %v", err)
	}
	if err := fixture.Reopen(ctx); err != nil {
		t.Fatalf("reopen fixture: %v", err)
	}

	reopened, err := fixture.Store().GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get running job after reopen: %v", err)
	}
	if reopened.State != domain.StateRunning ||
		reopened.AttemptNo != first.AttemptNo ||
		reopened.LeaseGeneration != first.Generation {
		t.Fatalf("running lease changed across reopen: %#v", reopened)
	}
	early, err := fixture.Store().Acquire(ctx, "worker-new", 1, 1, clock.Now(), ttl)
	if err != nil {
		t.Fatalf("acquire before old lease expiry: %v", err)
	}
	if len(early) != 0 {
		t.Fatalf("acquired successor before expiry: %#v", early)
	}

	clock.Advance(ttl)
	reaped, err := fixture.Store().ReapExpired(ctx, clock.Now(), 10)
	if err != nil {
		t.Fatalf("reap after reopen: %v", err)
	}
	if len(reaped) != 1 || reaped[0].JobID != job.ID {
		t.Fatalf("reaped leases = %#v, want reopened job", reaped)
	}
	if _, err := fixture.Store().PromoteDue(ctx, clock.Now(), 10); err != nil {
		t.Fatalf("promote reaped job: %v", err)
	}
	second := acquireOne(t, ctx, fixture.Store(), clock, "worker-new", ttl)
	if second.Generation <= first.Generation {
		t.Fatalf("successor generation = %d, want greater than %d",
			second.Generation, first.Generation)
	}

	if _, err := fixture.Store().Complete(
		ctx,
		successfulCompletion(first),
		clock.Now(),
	); !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("pre-crash completion error = %v, want %v", err, domain.ErrStaleLease)
	}
	if err := fixture.Store().MarkRunning(
		ctx,
		"worker-new",
		leaseRef(second),
		clock.Now(),
	); err != nil {
		t.Fatalf("start successor after reopen: %v", err)
	}
	if _, err := fixture.Store().Complete(
		ctx,
		successfulCompletion(second),
		clock.Now(),
	); err != nil {
		t.Fatalf("complete successor after reopen: %v", err)
	}
	suite.assertReconciled(t, fixture, []string{job.ID}, true)
}

func (suite ContractSuite) testCanonicalCompletion(t *testing.T) {
	fixture, clock := suite.openFixture(t)
	ctx := context.Background()
	ttl := suite.leaseTTL()
	job := submitOne(t, ctx, fixture.Store(), clock, "canonical-completion")
	lease := acquireOne(t, ctx, fixture.Store(), clock, "worker-a", ttl)
	if err := fixture.Store().MarkRunning(
		ctx,
		"worker-a",
		leaseRef(lease),
		clock.Now(),
	); err != nil {
		t.Fatalf("start attempt: %v", err)
	}

	completion := successfulCompletion(lease)
	committedAt := clock.Now()
	type completionResult struct {
		receipt domain.CompletionReceipt
		err     error
	}
	start := make(chan struct{})
	results := make(chan completionResult, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			receipt, err := fixture.Store().Complete(ctx, completion, committedAt)
			results <- completionResult{receipt: receipt, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	var receipts []domain.CompletionReceipt
	duplicates := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent identical completion: %v", result.err)
		}
		receipts = append(receipts, result.receipt)
		if result.receipt.Duplicate {
			duplicates++
		}
	}
	if duplicates != 1 {
		t.Fatalf("duplicate receipts = %d, want 1; receipts=%#v", duplicates, receipts)
	}
	assertStableDuplicateReceipt(t, receipts[0], receipts[1])

	conflict := completion
	conflict.Success = false
	conflict.Retryable = false
	conflict.OutputDigest = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	conflict.Failure = &domain.Failure{Class: "conflicting_duplicate"}
	if _, err := fixture.Store().Complete(ctx, conflict, committedAt); err == nil {
		t.Fatal("conflicting duplicate completion succeeded")
	}
	suite.assertReconciled(t, fixture, []string{job.ID}, true)
}

func (suite ContractSuite) openFixture(t *testing.T) (Fixture, *FakeClock) {
	t.Helper()
	clock := NewFakeClock(time.Date(2032, time.March, 14, 15, 9, 26, 0, time.UTC))
	fixture, err := suite.NewFixture(t, clock)
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	if fixture == nil || fixture.Store() == nil || fixture.Reader() == nil {
		t.Fatal("fixture must expose a store and independent database reader")
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("close fixture: %v", err)
		}
	})
	return fixture, clock
}

func (suite ContractSuite) assertReconciled(
	t *testing.T,
	fixture Fixture,
	acceptedJobIDs []string,
	terminal bool,
) {
	t.Helper()
	expectation := ReconcileExpectation{
		AcceptedJobIDs:   acceptedJobIDs,
		RequireTerminal:  terminal,
		RequireSucceeded: terminal,
		RequireQuiescent: terminal,
	}
	queries := suite.ReconcileSQL
	if queries.Jobs == "" {
		queries = DefaultReconcileSQL
	}
	report, err := ReconcileWithSQL(context.Background(), fixture.Reader(), expectation, queries)
	if err != nil {
		t.Fatalf("reconcile database: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("database reconciliation failed: %s\nreport=%#v", report.Summary(), report)
	}
}

func (suite ContractSuite) leaseTTL() time.Duration {
	if suite.LeaseTTL <= 0 {
		return 3 * time.Second
	}
	return suite.LeaseTTL
}

func newSubmission(key, digest string) store.Submission {
	return store.Submission{
		Job: domain.JobSpec{
			Name:     key,
			TenantID: "tenant-p1",
			Queue:    "default",
			SlotCost: 1,
			Payload: domain.Payload{
				Type: domain.PayloadNoop,
			},
			Retry: domain.RetryPolicy{
				MaxAttempts: 3,
				Retryable:   true,
			},
		},
		IdempotencyKey: key,
		RequestDigest:  digest,
	}
}

func submitOne(
	t *testing.T,
	ctx context.Context,
	subject store.Store,
	clock *FakeClock,
	key string,
) domain.Job {
	t.Helper()
	submission := newSubmission(key, "digest-"+key)
	job, duplicate, err := subject.SubmitJob(ctx, submission, clock.Now())
	if err != nil {
		t.Fatalf("submit %s: %v", key, err)
	}
	if duplicate {
		t.Fatalf("first submit %s reported duplicate", key)
	}
	assertAcceptedJob(t, job)
	return job
}

func acquireOne(
	t *testing.T,
	ctx context.Context,
	subject store.Store,
	clock *FakeClock,
	workerID string,
	ttl time.Duration,
) domain.Lease {
	t.Helper()
	leases, err := subject.Acquire(ctx, workerID, 1, 1, clock.Now(), ttl)
	if err != nil {
		t.Fatalf("acquire lease for %s: %v", workerID, err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases for %s = %#v, want exactly one", workerID, leases)
	}
	return leases[0]
}

func assertAcceptedJob(t *testing.T, job domain.Job) {
	t.Helper()
	if job.ID == "" {
		t.Fatal("accepted job has empty ID")
	}
	if job.State != domain.StatePending {
		t.Fatalf("accepted job state = %s, want %s", job.State, domain.StatePending)
	}
	if job.StateVersion <= 0 {
		t.Fatalf("accepted job state version = %d, want positive", job.StateVersion)
	}
	if job.AttemptNo != 0 || job.LeaseGeneration != 0 {
		t.Fatalf("accepted job already attempted: %#v", job)
	}
}

func assertLeaseForJob(
	t *testing.T,
	lease domain.Lease,
	jobID, workerID string,
	attempt int,
	generation int64,
) {
	t.Helper()
	if lease.JobID != jobID ||
		lease.WorkerID != workerID ||
		lease.AttemptNo != attempt ||
		lease.Generation != generation ||
		lease.Token == "" {
		t.Fatalf("lease = %#v, want job=%s worker=%s attempt=%d generation=%d with token",
			lease, jobID, workerID, attempt, generation)
	}
}

func leaseRef(lease domain.Lease) domain.LeaseRef {
	return domain.LeaseRef{
		JobID:      lease.JobID,
		AttemptNo:  lease.AttemptNo,
		Generation: lease.Generation,
		Token:      lease.Token,
	}
}

func successfulCompletion(lease domain.Lease) domain.Completion {
	return domain.Completion{
		LeaseRef:     leaseRef(lease),
		WorkerID:     lease.WorkerID,
		Success:      true,
		OutputDigest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}
}

func assertSuccessReceipt(
	t *testing.T,
	receipt domain.CompletionReceipt,
	jobID string,
	duplicate bool,
) {
	t.Helper()
	if receipt.JobID != jobID ||
		receipt.State != domain.StateSucceeded ||
		receipt.StateVersion <= 0 ||
		receipt.CommittedAt.IsZero() ||
		receipt.Duplicate != duplicate {
		t.Fatalf("completion receipt = %#v, want succeeded job %s duplicate=%t",
			receipt, jobID, duplicate)
	}
}

func assertStableDuplicateReceipt(
	t *testing.T,
	first, second domain.CompletionReceipt,
) {
	t.Helper()
	firstDuplicate := first.Duplicate
	secondDuplicate := second.Duplicate
	first.Duplicate = false
	second.Duplicate = false
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("duplicate completion receipt changed:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if firstDuplicate == secondDuplicate {
		t.Fatalf("duplicate flags = %t and %t, want one duplicate", firstDuplicate, secondDuplicate)
	}
}
