package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func TestReopenDurability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rail-yard.db")
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)

	first := openTestStore(t, path)
	job := submitTestJob(t, first, "durable", "digest-durable", now)
	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	second := openTestStore(t, path)
	t.Cleanup(func() { _ = second.Close() })
	got, err := second.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("get job after reopen: %v", err)
	}
	if got.ID != job.ID || got.State != domain.StatePending || got.ReadySeq == 0 {
		t.Fatalf("unexpected durable job: %+v", got)
	}
}

func TestDuplicateSubmission(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "duplicate.db"))
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	submission := testSubmission("same-key", "same-digest")

	first, duplicate, err := store.SubmitJob(ctx, submission, now)
	if err != nil {
		t.Fatalf("submit first job: %v", err)
	}
	if duplicate {
		t.Fatal("first submission reported duplicate")
	}
	second, duplicate, err := store.SubmitJob(ctx, submission, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("submit duplicate job: %v", err)
	}
	if !duplicate || second.ID != first.ID || !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("duplicate response changed: first=%+v second=%+v", first, second)
	}

	conflict := submission
	conflict.RequestDigest = "different-digest"
	if _, _, err := store.SubmitJob(ctx, conflict, now); !errors.Is(
		err,
		domain.ErrIdempotencyConflict,
	) {
		t.Fatalf("conflicting submission error = %v, want idempotency conflict", err)
	}
}

func TestStaleLeaseFencing(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "fencing.db"))
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	job := submitTestJob(t, store, "fenced", "digest-fenced", now)
	lease := acquireOne(t, store, "worker-a", now, 3*time.Second)
	if lease.JobID != job.ID {
		t.Fatalf("leased job %q, want %q", lease.JobID, job.ID)
	}

	stale := domain.LeaseRef{
		JobID:      lease.JobID,
		AttemptNo:  lease.AttemptNo,
		Generation: lease.Generation,
		Token:      "not-the-token",
	}
	if err := store.MarkRunning(ctx, "worker-a", stale, now.Add(time.Second)); !errors.Is(
		err,
		domain.ErrStaleLease,
	) {
		t.Fatalf("stale start error = %v, want stale lease", err)
	}
	heartbeats, err := store.Heartbeat(
		ctx,
		"worker-a",
		[]domain.LeaseRef{stale},
		now.Add(time.Second),
		3*time.Second,
	)
	if err != nil {
		t.Fatalf("stale heartbeat: %v", err)
	}
	if len(heartbeats) != 1 || heartbeats[0].Accepted {
		t.Fatalf("stale heartbeat accepted: %+v", heartbeats)
	}
	_, err = store.Complete(ctx, domain.Completion{
		LeaseRef: stale,
		WorkerID: "worker-a",
		Success:  true,
	}, now.Add(time.Second))
	if !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("stale completion error = %v, want stale lease", err)
	}
}

func TestDuplicateCompletion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "completion.db"))
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	submitTestJob(t, store, "complete", "digest-complete", now)
	lease := acquireOne(t, store, "worker-a", now, 3*time.Second)
	ref := leaseRef(lease)
	if err := store.MarkRunning(ctx, "worker-a", ref, now.Add(time.Second)); err != nil {
		t.Fatalf("mark running: %v", err)
	}

	completion := domain.Completion{
		LeaseRef:     ref,
		WorkerID:     "worker-a",
		Success:      true,
		OutputDigest: "sha256:output",
	}
	first, err := store.Complete(ctx, completion, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("complete job: %v", err)
	}
	if first.Duplicate || first.State != domain.StateSucceeded {
		t.Fatalf("unexpected first completion: %+v", first)
	}
	second, err := store.Complete(ctx, completion, now.Add(10*time.Second))
	if err != nil {
		t.Fatalf("repeat completion: %v", err)
	}
	if !second.Duplicate ||
		second.StateVersion != first.StateVersion ||
		!second.CommittedAt.Equal(first.CommittedAt) {
		t.Fatalf("duplicate receipt changed: first=%+v second=%+v", first, second)
	}

	conflict := completion
	conflict.OutputDigest = "sha256:different"
	if _, err := store.Complete(ctx, conflict, now.Add(10*time.Second)); !errors.Is(
		err,
		domain.ErrStaleLease,
	) {
		t.Fatalf("conflicting completion error = %v, want stale lease", err)
	}
}

func TestAttemptStartBatchStartsAllValidLeases(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "start-batch.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 30, 0, 0, time.UTC)
	leases := prepareLeases(t, jobStore, 4, now)

	results, err := jobStore.MarkRunningBatch(
		ctx,
		"worker-a",
		leaseRefs(leases),
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d error = %v", index, result.Err)
		}
		job, getErr := jobStore.GetJob(ctx, leases[index].JobID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if job.State != domain.StateRunning {
			t.Fatalf("job %d state = %s, want RUNNING", index, job.State)
		}
	}
}

func TestAttemptStartBatchKeepsCurrentLeasesBesideStaleLease(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "mixed-start-batch.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 40, 0, 0, time.UTC)
	leases := prepareLeases(t, jobStore, 3, now)
	refs := leaseRefs(leases)
	refs[1].Token = "stale-token"

	results, err := jobStore.MarkRunningBatch(ctx, "worker-a", refs, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Err != nil || results[2].Err != nil {
		t.Fatalf("current lease errors = %v, %v", results[0].Err, results[2].Err)
	}
	if !errors.Is(results[1].Err, domain.ErrStaleLease) {
		t.Fatalf("stale lease error = %v", results[1].Err)
	}
	for _, index := range []int{0, 2} {
		job, getErr := jobStore.GetJob(ctx, leases[index].JobID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if job.State != domain.StateRunning {
			t.Fatalf("job %d state = %s, want RUNNING", index, job.State)
		}
	}
	staleJob, err := jobStore.GetJob(ctx, leases[1].JobID)
	if err != nil {
		t.Fatal(err)
	}
	if staleJob.State != domain.StateScheduled {
		t.Fatalf("stale job state = %s, want SCHEDULED", staleJob.State)
	}
	if err := jobStore.MarkRunning(
		ctx,
		"worker-a",
		leaseRef(leases[1]),
		now.Add(3*time.Second),
	); err != nil {
		t.Fatalf("start current lease after stale result: %v", err)
	}
}

func TestAttemptStartBatchAcceptsDuplicatesWithoutExtraEvents(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "duplicate-start-batch.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 50, 0, 0, time.UTC)
	leases := prepareLeases(t, jobStore, 2, now)
	refs := leaseRefs(leases)

	first, err := jobStore.MarkRunningBatch(ctx, "worker-a", refs, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	second, err := jobStore.MarkRunningBatch(ctx, "worker-a", refs, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for index := range refs {
		if first[index].Err != nil || second[index].Err != nil {
			t.Fatalf(
				"start errors for item %d = %v, %v",
				index,
				first[index].Err,
				second[index].Err,
			)
		}
	}
	var eventCount int
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM events WHERE event_type = 'attempt_started'",
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != len(refs) {
		t.Fatalf("attempt started events = %d, want %d", eventCount, len(refs))
	}
}

func TestAttemptStartBatchUsesOneTransaction(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "start-transactions.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 55, 0, 0, time.UTC)
	leases := prepareLeases(t, jobStore, 32, now)

	results, err := jobStore.MarkRunningBatch(
		ctx,
		"worker-a",
		leaseRefs(leases),
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if commits := jobStore.attemptStartCommits.Load(); commits != 1 {
		t.Fatalf("attempt start commits = %d, want 1 for 32 starts", commits)
	}
}

func TestReapAndReacquire(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "reap.db"))
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	submitTestJob(t, store, "reap", "digest-reap", now)
	first := acquireOne(t, store, "worker-a", now, time.Second)

	reaped, err := store.ReapExpired(ctx, now.Add(time.Second), 10)
	if err != nil {
		t.Fatalf("reap expired lease: %v", err)
	}
	if len(reaped) != 1 || reaped[0].JobID != first.JobID {
		t.Fatalf("unexpected reaped leases: %+v", reaped)
	}
	second := acquireOne(t, store, "worker-b", now.Add(time.Second), time.Second)
	if second.AttemptNo != first.AttemptNo+1 ||
		second.Generation != first.Generation+1 ||
		second.Token == first.Token {
		t.Fatalf("successor lease not fenced: first=%+v second=%+v", first, second)
	}

	_, err = store.Complete(ctx, domain.Completion{
		LeaseRef: leaseRef(first),
		WorkerID: first.WorkerID,
		Success:  true,
	}, now.Add(1500*time.Millisecond))
	if !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("old lease completion error = %v, want stale lease", err)
	}
}

func TestRetryPromotion(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "retry.db"))
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	submission := testSubmission("retry", "digest-retry")
	submission.Job.Retry = domain.RetryPolicy{MaxAttempts: 3, Retryable: true}
	if _, _, err := store.SubmitJob(ctx, submission, now); err != nil {
		t.Fatalf("submit retry job: %v", err)
	}
	first := acquireOne(t, store, "worker-a", now, time.Second)
	if err := store.MarkRunning(ctx, "worker-a", leaseRef(first), now.Add(100*time.Millisecond)); err != nil {
		t.Fatalf("mark retry attempt running: %v", err)
	}
	receipt, err := store.Complete(ctx, domain.Completion{
		LeaseRef:  leaseRef(first),
		WorkerID:  first.WorkerID,
		Retryable: true,
		Failure: &domain.Failure{
			Class:   "temporary",
			Message: "try again",
		},
	}, now.Add(200*time.Millisecond))
	if err != nil {
		t.Fatalf("complete retryable attempt: %v", err)
	}
	if receipt.State != domain.StateRetrying {
		t.Fatalf("completion state = %s, want RETRYING", receipt.State)
	}

	job, err := store.GetJob(ctx, first.JobID)
	if err != nil {
		t.Fatalf("get retrying job: %v", err)
	}
	if _, err := store.PromoteDue(ctx, job.AvailableAt.Add(-time.Nanosecond), 10); err != nil {
		t.Fatalf("early due promotion: %v", err)
	}
	leases, err := store.Acquire(
		ctx,
		"worker-b",
		1,
		1,
		job.AvailableAt.Add(-time.Nanosecond),
		time.Second,
	)
	if err != nil {
		t.Fatalf("acquire before retry due: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("retry leased before due: %+v", leases)
	}
	promoted, err := store.PromoteDue(ctx, job.AvailableAt, 10)
	if err != nil {
		t.Fatalf("promote retry: %v", err)
	}
	if promoted != 1 {
		t.Fatalf("promoted %d jobs, want 1", promoted)
	}
	second := acquireOne(t, store, "worker-b", job.AvailableAt, time.Second)
	if second.AttemptNo != 2 {
		t.Fatalf("retry attempt number = %d, want 2", second.AttemptNo)
	}
}

func TestCompletionBatchCommitsValidItemsBesideStaleItem(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "partial-stale-batch.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 9, 0, 0, 0, time.UTC)
	leases := prepareCompletionLeases(t, jobStore, 3, now)

	completions := successfulCompletions(leases)
	completions[1].Token = "stale-token"
	results, err := jobStore.CompleteBatch(ctx, completions, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(completions) {
		t.Fatalf("results = %d, want %d", len(results), len(completions))
	}
	if results[0].Err != nil || results[0].Receipt.State != domain.StateSucceeded {
		t.Fatalf("first result = %+v", results[0])
	}
	if !errors.Is(results[1].Err, domain.ErrStaleLease) {
		t.Fatalf("stale result error = %v", results[1].Err)
	}
	if results[2].Err != nil || results[2].Receipt.State != domain.StateSucceeded {
		t.Fatalf("third result = %+v", results[2])
	}

	for _, index := range []int{0, 2} {
		job, getErr := jobStore.GetJob(ctx, leases[index].JobID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if job.State != domain.StateSucceeded {
			t.Fatalf("job %d state = %s, want SUCCEEDED", index, job.State)
		}
	}
	staleJob, err := jobStore.GetJob(ctx, leases[1].JobID)
	if err != nil {
		t.Fatal(err)
	}
	if staleJob.State != domain.StateRunning {
		t.Fatalf("stale job state = %s, want RUNNING", staleJob.State)
	}

	completions[1].Token = leases[1].Token
	if _, err := jobStore.Complete(ctx, completions[1], now.Add(3*time.Second)); err != nil {
		t.Fatalf("complete previously stale item: %v", err)
	}
}

func TestCompletionBatchRejectsConflictBesideValidItem(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "conflicting-batch.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 9, 15, 0, 0, time.UTC)
	leases := prepareCompletionLeases(t, jobStore, 2, now)
	completions := successfulCompletions(leases)

	first, err := jobStore.Complete(ctx, completions[0], now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	conflict := completions[0]
	conflict.OutputDigest = "conflicting-output"
	results, err := jobStore.CompleteBatch(
		ctx,
		[]domain.Completion{conflict, completions[1]},
		now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(results[0].Err, domain.ErrStaleLease) {
		t.Fatalf("conflicting result error = %v, want stale lease", results[0].Err)
	}
	if results[1].Err != nil || results[1].Receipt.State != domain.StateSucceeded {
		t.Fatalf("valid result = %+v", results[1])
	}
	duplicate, err := jobStore.Complete(ctx, completions[0], now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate ||
		duplicate.StateVersion != first.StateVersion ||
		!duplicate.CommittedAt.Equal(first.CommittedAt) {
		t.Fatalf("duplicate receipt changed: first=%+v duplicate=%+v", first, duplicate)
	}
}

func TestCompletionBatchPreservesRetryTransition(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "retry-batch.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 9, 30, 0, 0, time.UTC)

	for index := 0; index < 2; index++ {
		submission := testSubmission(
			fmt.Sprintf("batch-retry-%d", index),
			fmt.Sprintf("batch-retry-digest-%d", index),
		)
		submission.Job.Retry = domain.RetryPolicy{MaxAttempts: 3, Retryable: true}
		if _, _, err := jobStore.SubmitJob(ctx, submission, now); err != nil {
			t.Fatal(err)
		}
	}
	leases, err := jobStore.Acquire(ctx, "worker-a", 2, 2, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 2 {
		t.Fatalf("leases = %d, want 2", len(leases))
	}
	for _, lease := range leases {
		if err := jobStore.MarkRunning(ctx, "worker-a", leaseRef(lease), now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	completions := successfulCompletions(leases)
	completions[0].Success = false
	completions[0].Retryable = true
	completions[0].Failure = &domain.Failure{Class: "temporary", Message: "retry"}
	results, err := jobStore.CompleteBatch(ctx, completions, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Err != nil || results[0].Receipt.State != domain.StateRetrying {
		t.Fatalf("retry result = %+v", results[0])
	}
	if results[1].Err != nil || results[1].Receipt.State != domain.StateSucceeded {
		t.Fatalf("success result = %+v", results[1])
	}
	retrying, err := jobStore.GetJob(ctx, leases[0].JobID)
	if err != nil {
		t.Fatal(err)
	}
	if retrying.State != domain.StateRetrying || retrying.TerminalAt != nil {
		t.Fatalf("retrying job = %+v", retrying)
	}
}

func TestConcurrentCompletionBatchesReturnStableReceipts(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "concurrent-batches.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 10, 0, 0, 0, time.UTC)
	leases := prepareCompletionLeases(t, jobStore, 4, now)
	completions := successfulCompletions(leases)

	const callers = 8
	start := make(chan struct{})
	resultSets := make(chan []storepkg.CompletionResult, callers)
	errs := make(chan error, callers)
	var workers sync.WaitGroup
	for range callers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			results, err := jobStore.CompleteBatch(ctx, completions, now.Add(2*time.Second))
			if err != nil {
				errs <- err
				return
			}
			resultSets <- results
		}()
	}
	close(start)
	workers.Wait()
	close(resultSets)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	firstReceipts := make(map[string]domain.CompletionReceipt)
	newReceipts := make(map[string]int)
	for results := range resultSets {
		for _, result := range results {
			if result.Err != nil {
				t.Fatalf("completion result error = %v", result.Err)
			}
			receipt := result.Receipt
			if !receipt.Duplicate {
				newReceipts[receipt.JobID]++
			}
			if first, ok := firstReceipts[receipt.JobID]; ok {
				if receipt.State != first.State ||
					receipt.StateVersion != first.StateVersion ||
					!receipt.CommittedAt.Equal(first.CommittedAt) {
					t.Fatalf("receipt changed: first=%+v current=%+v", first, receipt)
				}
			} else {
				firstReceipts[receipt.JobID] = receipt
			}
		}
	}
	for _, lease := range leases {
		if newReceipts[lease.JobID] != 1 {
			t.Fatalf("new receipts for %s = %d, want 1", lease.JobID, newReceipts[lease.JobID])
		}
	}
}

func TestCompletionBatchUsesOneTransaction(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "batch-transactions.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 10, 30, 0, 0, time.UTC)
	leases := prepareCompletionLeases(t, jobStore, 32, now)

	results, err := jobStore.CompleteBatch(
		ctx,
		successfulCompletions(leases),
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if commits := jobStore.completionCommits.Load(); commits != 1 {
		t.Fatalf("completion commits = %d, want 1 for 32 completions", commits)
	}
}

func TestWorkflowCycleRejected(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "cycle.db"))
	t.Cleanup(func() { _ = store.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	request := api.SubmitWorkflowRequest{
		TenantID: "tenant-a",
		Nodes: []api.WorkflowNode{
			{
				Key: "a",
				Job: testSubmission("", "").Job,
			},
			{
				Key: "b",
				Job: testSubmission("", "").Job,
			},
		},
	}
	request.Nodes[0].Job.DependsOn = []string{"b"}
	request.Nodes[1].Job.DependsOn = []string{"a"}
	_, _, err := store.SubmitWorkflow(ctx, storepkg.WorkflowSubmission{
		Request:        request,
		IdempotencyKey: "cycle",
		RequestDigest:  "digest-cycle",
	}, now)
	if !errors.Is(err, domain.ErrCycleDetected) {
		t.Fatalf("cycle submission error = %v, want cycle detected", err)
	}
}

func BenchmarkAttemptStartTransactions(b *testing.B) {
	const batchSize = 32
	for _, mode := range []string{"single", "batch"} {
		b.Run(mode, func(b *testing.B) {
			jobStore, err := Open(filepath.Join(b.TempDir(), mode+".db"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = jobStore.Close() })
			ctx := context.Background()
			base := time.Date(2026, time.August, 14, 10, 45, 0, 0, time.UTC)
			b.ResetTimer()

			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				iterationTime := base.Add(time.Duration(iteration) * time.Minute)
				leases := prepareBenchmarkLeases(
					b,
					jobStore,
					fmt.Sprintf("start-%s-%d", mode, iteration),
					batchSize,
					iterationTime,
				)
				refs := leaseRefs(leases)
				b.StartTimer()

				if mode == "single" {
					for _, ref := range refs {
						if err := jobStore.MarkRunning(
							ctx,
							"benchmark-worker",
							ref,
							iterationTime.Add(2*time.Second),
						); err != nil {
							b.Fatal(err)
						}
					}
					continue
				}
				if _, err := jobStore.MarkRunningBatch(
					ctx,
					"benchmark-worker",
					refs,
					iterationTime.Add(2*time.Second),
				); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(batchSize, "starts/op")
			transactions := batchSize
			if mode == "batch" {
				transactions = 1
			}
			b.ReportMetric(float64(transactions), "start_tx/op")
			b.ReportMetric(
				float64(batchSize*b.N)/b.Elapsed().Seconds(),
				"starts/s",
			)
		})
	}
}

func BenchmarkCompletionTransactions(b *testing.B) {
	const batchSize = 32
	for _, mode := range []string{"single", "batch"} {
		b.Run(mode, func(b *testing.B) {
			jobStore, err := Open(filepath.Join(b.TempDir(), mode+".db"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = jobStore.Close() })
			ctx := context.Background()
			base := time.Date(2026, time.August, 14, 11, 0, 0, 0, time.UTC)
			b.ResetTimer()

			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				iterationTime := base.Add(time.Duration(iteration) * time.Minute)
				completions := prepareBenchmarkCompletions(
					b,
					jobStore,
					fmt.Sprintf("%s-%d", mode, iteration),
					batchSize,
					iterationTime,
				)
				b.StartTimer()

				if mode == "single" {
					for _, completion := range completions {
						if _, err := jobStore.Complete(
							ctx,
							completion,
							iterationTime.Add(30*time.Second),
						); err != nil {
							b.Fatal(err)
						}
					}
					continue
				}
				if _, err := jobStore.CompleteBatch(
					ctx,
					completions,
					iterationTime.Add(30*time.Second),
				); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(batchSize, "completions/op")
			transactions := batchSize
			if mode == "batch" {
				transactions = 1
			}
			b.ReportMetric(float64(transactions), "completion_tx/op")
			b.ReportMetric(
				float64(batchSize*b.N)/b.Elapsed().Seconds(),
				"completions/s",
			)
		})
	}
}

func prepareCompletionLeases(
	t *testing.T,
	jobStore *Store,
	count int,
	now time.Time,
) []domain.Lease {
	t.Helper()
	leases := prepareLeases(t, jobStore, count, now)
	ctx := context.Background()
	for _, lease := range leases {
		if err := jobStore.MarkRunning(ctx, "worker-a", leaseRef(lease), now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	return leases
}

func prepareLeases(
	t *testing.T,
	jobStore *Store,
	count int,
	now time.Time,
) []domain.Lease {
	t.Helper()
	ctx := context.Background()
	for index := 0; index < count; index++ {
		if _, _, err := jobStore.SubmitJob(ctx, testSubmission(
			fmt.Sprintf("batch-%d", index),
			fmt.Sprintf("batch-digest-%d", index),
		), now); err != nil {
			t.Fatal(err)
		}
	}
	leases, err := jobStore.Acquire(ctx, "worker-a", count, count, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != count {
		t.Fatalf("leases = %d, want %d", len(leases), count)
	}
	return leases
}

func leaseRefs(leases []domain.Lease) []domain.LeaseRef {
	refs := make([]domain.LeaseRef, len(leases))
	for index, lease := range leases {
		refs[index] = leaseRef(lease)
	}
	return refs
}

func successfulCompletions(leases []domain.Lease) []domain.Completion {
	completions := make([]domain.Completion, len(leases))
	for index, lease := range leases {
		completions[index] = domain.Completion{
			LeaseRef:     leaseRef(lease),
			WorkerID:     lease.WorkerID,
			Success:      true,
			OutputDigest: fmt.Sprintf("output-%d", index),
		}
	}
	return completions
}

func prepareBenchmarkCompletions(
	b *testing.B,
	jobStore *Store,
	prefix string,
	count int,
	now time.Time,
) []domain.Completion {
	b.Helper()
	leases := prepareBenchmarkLeases(b, jobStore, prefix, count, now)
	ctx := context.Background()
	for _, lease := range leases {
		if err := jobStore.MarkRunning(
			ctx,
			"benchmark-worker",
			leaseRef(lease),
			now.Add(2*time.Second),
		); err != nil {
			b.Fatal(err)
		}
	}
	return successfulCompletions(leases)
}

func prepareBenchmarkLeases(
	b *testing.B,
	jobStore *Store,
	prefix string,
	count int,
	now time.Time,
) []domain.Lease {
	b.Helper()
	ctx := context.Background()
	for index := 0; index < count; index++ {
		submission := testSubmission(
			fmt.Sprintf("%s-%d", prefix, index),
			fmt.Sprintf("%s-digest-%d", prefix, index),
		)
		if _, _, err := jobStore.SubmitJob(ctx, submission, now); err != nil {
			b.Fatal(err)
		}
	}
	leases, err := jobStore.Acquire(ctx, "benchmark-worker", count, count, now.Add(time.Second), time.Minute)
	if err != nil {
		b.Fatal(err)
	}
	if len(leases) != count {
		b.Fatalf("leases = %d, want %d", len(leases), count)
	}
	return leases
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func submitTestJob(
	t *testing.T,
	store *Store,
	key string,
	digest string,
	now time.Time,
) domain.Job {
	t.Helper()
	job, duplicate, err := store.SubmitJob(
		context.Background(),
		testSubmission(key, digest),
		now,
	)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	if duplicate {
		t.Fatal("new test job reported duplicate")
	}
	return job
}

func testSubmission(key string, digest string) storepkg.Submission {
	return storepkg.Submission{
		IdempotencyKey: key,
		RequestDigest:  digest,
		Job: domain.JobSpec{
			TenantID: "tenant-a",
			Queue:    "default",
			SlotCost: 1,
			Payload: domain.Payload{
				Type: domain.PayloadNoop,
			},
			Retry: domain.RetryPolicy{
				MaxAttempts: 3,
			},
		},
	}
}

func acquireOne(
	t *testing.T,
	store *Store,
	workerID string,
	now time.Time,
	ttl time.Duration,
) domain.Lease {
	t.Helper()
	leases, err := store.Acquire(context.Background(), workerID, 1, 1, now, ttl)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("acquired %d leases, want 1", len(leases))
	}
	return leases[0]
}

func leaseRef(lease domain.Lease) domain.LeaseRef {
	return domain.LeaseRef{
		JobID:      lease.JobID,
		AttemptNo:  lease.AttemptNo,
		Generation: lease.Generation,
		Token:      lease.Token,
	}
}
