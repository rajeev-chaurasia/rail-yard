package telemetry

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
	sqlitestore "github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
)

type codedSQLiteError int

func (err codedSQLiteError) Error() string {
	return "sqlite error"
}

func (err codedSQLiteError) Code() int {
	return int(err)
}

func TestObservedStoreCountsBatchedTransactionsAndCompletions(t *testing.T) {
	ctx := context.Background()
	jobStore, err := sqlitestore.Open(filepath.Join(t.TempDir(), "telemetry-batch.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)

	for index := 0; index < 2; index++ {
		_, _, err := jobStore.SubmitJob(ctx, store.Submission{
			IdempotencyKey: fmt.Sprintf("telemetry-%d", index),
			RequestDigest:  fmt.Sprintf("telemetry-digest-%d", index),
			Job: domain.JobSpec{
				TenantID: "tenant-a",
				Queue:    "default",
				SlotCost: 1,
				Payload:  domain.Payload{Type: domain.PayloadNoop},
				Retry:    domain.RetryPolicy{MaxAttempts: 3},
			},
		}, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	leases, err := jobStore.Acquire(ctx, "worker-a", 2, 2, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	completions := make([]domain.Completion, len(leases))
	refs := make([]domain.LeaseRef, len(leases))
	for index, lease := range leases {
		reference := domain.LeaseRef{
			JobID:      lease.JobID,
			AttemptNo:  lease.AttemptNo,
			Generation: lease.Generation,
			Token:      lease.Token,
		}
		refs[index] = reference
		completions[index] = domain.Completion{
			LeaseRef:     reference,
			WorkerID:     "worker-a",
			Success:      true,
			OutputDigest: fmt.Sprintf("output-%d", index),
		}
	}

	metrics, err := New()
	if err != nil {
		t.Fatal(err)
	}
	observed := ObserveStore(jobStore, metrics)
	startResults, err := observed.MarkRunningBatch(ctx, "worker-a", refs, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range startResults {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	results, err := observed.CompleteBatch(ctx, completions, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if got := counterValue(
		t,
		metrics.Registry(),
		"railyard_completions_total",
		map[string]string{"outcome": "succeeded"},
	); got != 2 {
		t.Fatalf("successful completions = %v, want 2", got)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_sqlite_transaction_duration_seconds",
		map[string]string{"operation": "mark_running", "result": "success"},
	); got != 1 {
		t.Fatalf("attempt start transaction observations = %d, want 1", got)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_sqlite_transaction_duration_seconds",
		map[string]string{"operation": "complete", "result": "success"},
	); got != 1 {
		t.Fatalf("completion transaction observations = %d, want 1", got)
	}
}

func TestSQLiteResultClassifiesExtendedBusyAndLockedCodes(t *testing.T) {
	for _, code := range []int{5, 6, 261, 517, 773} {
		err := fmt.Errorf("wrapped: %w", codedSQLiteError(code))
		if got := sqliteResult(err); got != SQLiteBusy {
			t.Errorf("sqliteResult(code %d) = %v, want busy", code, got)
		}
	}
	if got := sqliteResult(errors.New("other")); got != SQLiteError {
		t.Errorf("sqliteResult(other) = %v, want error", got)
	}
	if got := sqliteResult(nil); got != SQLiteSuccess {
		t.Errorf("sqliteResult(nil) = %v, want success", got)
	}

	metrics, err := New()
	if err != nil {
		t.Fatal(err)
	}
	observed := &ObservedStore{metrics: metrics}
	observed.observeSQLite(SQLiteComplete, time.Now(), codedSQLiteError(773))
	if got := counterValue(
		t,
		metrics.Registry(),
		"railyard_sqlite_busy_total",
		map[string]string{"operation": "complete"},
	); got != 1 {
		t.Errorf("SQLite busy counter = %v, want 1", got)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_sqlite_transaction_duration_seconds",
		map[string]string{"operation": "complete", "result": "busy"},
	); got != 1 {
		t.Errorf("SQLite busy duration observations = %d, want 1", got)
	}
}
