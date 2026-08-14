package sqlite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/decisionlog"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func TestAcquireIsFairAcrossQueuesAndRecordsDecision(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "scheduling.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)

	submitScheduledTestJob(t, jobStore, now, "tenant-a", "queue", "a-1", 100)
	submitScheduledTestJob(t, jobStore, now, "tenant-a", "queue", "a-2", 100)
	submitScheduledTestJob(t, jobStore, now, "tenant-b", "queue", "b-1", 0)

	leases, err := jobStore.Acquire(ctx, "worker", 3, 2, now, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 2 {
		t.Fatalf("leases = %d, want 2", len(leases))
	}
	tenants := map[string]bool{}
	for _, lease := range leases {
		job, err := jobStore.GetJob(ctx, lease.JobID)
		if err != nil {
			t.Fatal(err)
		}
		tenants[job.TenantID] = true
	}
	if !tenants["tenant-a"] || !tenants["tenant-b"] {
		t.Fatalf("granted tenants = %v, want both tenants", tenants)
	}

	var recordJSON string
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT record_json FROM decision_log WHERE sequence = 1",
	).Scan(&recordJSON); err != nil {
		t.Fatal(err)
	}
	records, err := decisionlog.ReadAll(bytes.NewBufferString(recordJSON + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || len(records[0].Decision.Grants) != 2 {
		t.Fatalf("decision records = %+v", records)
	}
}

func TestExhaustedRetryCreatesDeadLetter(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "dead-letter.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)

	job, _, err := jobStore.SubmitJob(ctx, storepkg.Submission{
		Job: domain.JobSpec{
			TenantID: "tenant",
			Queue:    "queue",
			SlotCost: 1,
			Payload:  domain.Payload{Type: domain.PayloadNoop},
			Retry:    domain.RetryPolicy{MaxAttempts: 1, Retryable: true},
		},
		IdempotencyKey: "dead-letter",
		RequestDigest:  "dead-letter-digest",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := jobStore.Acquire(ctx, "worker", 1, 1, now, 3*time.Second)
	if err != nil || len(leases) != 1 {
		t.Fatalf("acquire = %+v, error = %v", leases, err)
	}
	lease := leases[0]
	ref := domain.LeaseRef{
		JobID:      lease.JobID,
		AttemptNo:  lease.AttemptNo,
		Generation: lease.Generation,
		Token:      lease.Token,
	}
	receipt, err := jobStore.Complete(ctx, domain.Completion{
		LeaseRef:  ref,
		WorkerID:  lease.WorkerID,
		Retryable: true,
		Failure:   &domain.Failure{Class: "test_failure", Message: "failed"},
	}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.JobID != job.ID || receipt.State != domain.StateDeadLetter {
		t.Fatalf("receipt = %+v", receipt)
	}
	var count int
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM dead_letters WHERE job_id = ?",
		job.ID,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("dead letters = %d, want 1", count)
	}
	deadLetters, err := jobStore.ListDeadLetters(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(deadLetters) != 1 || deadLetters[0].JobID != job.ID {
		t.Fatalf("dead letters = %+v", deadLetters)
	}
	redriven, duplicate, err := jobStore.RedriveDeadLetter(
		ctx,
		job.ID,
		"redrive",
		"redrive-digest",
		now.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate || redriven.ID == job.ID || redriven.State != domain.StatePending {
		t.Fatalf("redriven job = %+v, duplicate = %t", redriven, duplicate)
	}
	if _, _, err := jobStore.RedriveDeadLetter(
		ctx,
		job.ID,
		"other-redrive",
		"other-redrive-digest",
		now.Add(3*time.Second),
	); !errors.Is(err, domain.ErrDeadLetterRedriven) {
		t.Fatalf("second redrive error = %v", err)
	}
}

func TestExpiredLeasePreemptsQueuedWork(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "recovery.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	submitScheduledTestJob(t, jobStore, now, "tenant", "queue", "first", 0)
	submitScheduledTestJob(t, jobStore, now, "tenant", "queue", "second", 100)

	first, err := jobStore.Acquire(ctx, "worker-old", 1, 1, now, 3*time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("first acquire = %+v, error = %v", first, err)
	}
	expiredJobID := first[0].JobID
	if _, err := jobStore.ReapExpired(ctx, now.Add(3*time.Second), 10); err != nil {
		t.Fatal(err)
	}
	successor, err := jobStore.Acquire(
		ctx,
		"worker-new",
		1,
		1,
		now.Add(3*time.Second),
		3*time.Second,
	)
	if err != nil || len(successor) != 1 {
		t.Fatalf("successor acquire = %+v, error = %v", successor, err)
	}
	if successor[0].JobID != expiredJobID || successor[0].Generation != 2 {
		t.Fatalf("successor = %+v, want recovered job %s", successor[0], expiredJobID)
	}
}

func TestRecoveryAwareAcquirePreservesOneSlot(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "reserve.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 0, 0, time.UTC)
	submitScheduledTestJob(t, jobStore, now, "tenant", "queue", "one", 0)
	submitScheduledTestJob(t, jobStore, now, "tenant", "queue", "two", 0)

	leases, err := jobStore.AcquireWithRecoveryReserve(
		ctx,
		"worker",
		2,
		2,
		now,
		2500*time.Millisecond,
		1,
	)
	if err != nil || len(leases) != 1 {
		t.Fatalf("initial leases = %+v, error = %v", leases, err)
	}
	empty, err := jobStore.AcquireWithRecoveryReserve(
		ctx,
		"worker",
		1,
		1,
		now,
		2500*time.Millisecond,
		1,
	)
	if err != nil || len(empty) != 0 {
		t.Fatalf("reserved slot leases = %+v, error = %v", empty, err)
	}
	if _, err := jobStore.ReapExpired(ctx, now.Add(2500*time.Millisecond), 10); err != nil {
		t.Fatal(err)
	}
	recovered, err := jobStore.AcquireWithRecoveryReserve(
		ctx,
		"worker",
		1,
		1,
		now.Add(2500*time.Millisecond),
		2500*time.Millisecond,
		1,
	)
	if err != nil || len(recovered) != 1 || recovered[0].JobID != leases[0].JobID {
		t.Fatalf("recovery leases = %+v, error = %v", recovered, err)
	}
}

func submitScheduledTestJob(
	t *testing.T,
	jobStore *Store,
	now time.Time,
	tenant string,
	queue string,
	key string,
	priority int,
) {
	t.Helper()
	_, duplicate, err := jobStore.SubmitJob(context.Background(), storepkg.Submission{
		Job: domain.JobSpec{
			TenantID: tenant,
			Queue:    queue,
			Priority: priority,
			SlotCost: 1,
			Payload:  domain.Payload{Type: domain.PayloadNoop},
			Retry:    domain.RetryPolicy{MaxAttempts: 3, Retryable: true},
		},
		IdempotencyKey: key,
		RequestDigest:  "digest-" + key,
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate {
		t.Fatal("first submission reported duplicate")
	}
}
