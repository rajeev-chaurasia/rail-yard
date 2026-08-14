package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func TestForceReleaseFencesAttemptAndReleasesSlots(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "force-release.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)

	job, _, err := jobStore.SubmitJob(ctx, storepkg.Submission{
		Job: domain.JobSpec{
			TenantID: "tenant-a",
			Queue:    "batch",
			SlotCost: 1,
			Payload:  domain.Payload{Type: domain.PayloadNoop},
		},
		IdempotencyKey: "submit-release",
		RequestDigest:  "submit-release-digest",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	leases, err := jobStore.Acquire(ctx, "worker-a", 1, 1, now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}

	receipt, err := jobStore.ApplyJobControl(ctx, JobControlCommand{
		JobID:          job.ID,
		ReceiptAction:  "release",
		AuditAction:    "job.force.release",
		Actor:          "operator-a",
		Reason:         "worker unavailable",
		IdempotencyKey: "force-release",
		RequestDigest:  "force-release-digest",
		RequestedAt:    now.Add(2 * time.Second),
		NextState:      domain.StatePending,
		Release:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != domain.StatePending {
		t.Fatalf("state = %s, want PENDING", receipt.State)
	}
	depths, err := jobStore.TenantQueueDepth(ctx, "tenant-a", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(depths) != 1 || depths[0].ActiveSlots != 0 {
		t.Fatalf("queue depths = %#v, want zero active slots", depths)
	}
	results, err := jobStore.Heartbeat(
		ctx,
		"worker-a",
		[]domain.LeaseRef{{
			JobID:      leases[0].JobID,
			AttemptNo:  leases[0].AttemptNo,
			Generation: leases[0].Generation,
			Token:      leases[0].Token,
		}},
		now.Add(3*time.Second),
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Accepted {
		t.Fatalf("heartbeat results = %#v, want fenced lease", results)
	}
	_, err = jobStore.Complete(ctx, domain.Completion{
		LeaseRef: domain.LeaseRef{
			JobID:      leases[0].JobID,
			AttemptNo:  leases[0].AttemptNo,
			Generation: leases[0].Generation,
			Token:      leases[0].Token,
		},
		WorkerID:     "worker-a",
		Success:      true,
		OutputDigest: "late",
	}, now.Add(4*time.Second))
	if !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("late completion error = %v, want stale lease", err)
	}
}

func TestTerminalControlIsIdempotentWithOneLedgerRow(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "terminal-control.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 13, 0, 0, 0, time.UTC)

	job, _, err := jobStore.SubmitJob(ctx, storepkg.Submission{
		Job: domain.JobSpec{
			Payload: domain.Payload{Type: domain.PayloadNoop},
		},
		IdempotencyKey: "submit-terminal",
		RequestDigest:  "submit-terminal-digest",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	command := JobControlCommand{
		JobID:          job.ID,
		ReceiptAction:  "dead_letter",
		AuditAction:    "job.force.dead_letter",
		Actor:          "operator-a",
		Reason:         "acceptance drill",
		IdempotencyKey: "force-terminal",
		RequestDigest:  "force-terminal-digest",
		RequestedAt:    now.Add(time.Second),
		NextState:      domain.StateDeadLetter,
	}
	first, err := jobStore.ApplyJobControl(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	second, err := jobStore.ApplyJobControl(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || !second.Duplicate {
		t.Fatalf("duplicate flags = %t, %t", first.Duplicate, second.Duplicate)
	}
	var completions, audits int
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM job_completions WHERE job_id = ?",
		job.ID,
	).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM audit_events WHERE target_id = ? AND action = ?",
		job.ID,
		command.AuditAction,
	).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if completions != 1 || audits != 1 {
		t.Fatalf("completions = %d, audits = %d, want 1 each", completions, audits)
	}

	command.IdempotencyKey = "force-terminal-again"
	command.RequestDigest = "force-terminal-again-digest"
	_, err = jobStore.ApplyJobControl(ctx, command)
	if !errors.Is(err, domain.ErrTerminalJob) {
		t.Fatalf("second terminal action error = %v, want terminal job", err)
	}
}

func TestOperationSubmissionAndAuditRollBackTogether(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "submission-atomic.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	if _, err := jobStore.db.ExecContext(ctx, `
		CREATE TRIGGER reject_submit_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'job.submit'
		BEGIN
			SELECT RAISE(ABORT, 'injected audit failure');
		END
	`); err != nil {
		t.Fatal(err)
	}
	command := operations.SubmitJobCommand{
		Request: api.SubmitJobRequest{Job: domain.JobSpec{
			TenantID: "tenant-a",
			Payload:  domain.Payload{Type: domain.PayloadNoop},
		}},
		Actor:          "operator-a",
		IdempotencyKey: "atomic-submit",
		RequestDigest:  "atomic-submit-digest",
		RequestedAt:    time.Date(2026, time.August, 14, 15, 0, 0, 0, time.UTC),
	}
	if _, err := jobStore.SubmitJobOperation(ctx, command); err == nil {
		t.Fatal("submission succeeded despite injected audit failure")
	}
	var jobs, requests int
	if err := jobStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM operation_requests",
	).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if jobs != 0 || requests != 0 {
		t.Fatalf("jobs = %d, requests = %d, want zero", jobs, requests)
	}
	if _, err := jobStore.db.ExecContext(ctx, "DROP TRIGGER reject_submit_audit"); err != nil {
		t.Fatal(err)
	}
	if _, err := jobStore.SubmitJobOperation(ctx, command); err != nil {
		t.Fatalf("retry submission: %v", err)
	}
	if _, err := jobStore.db.ExecContext(ctx, `
		CREATE TRIGGER reject_dag_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'dag.submit'
		BEGIN
			SELECT RAISE(ABORT, 'injected DAG audit failure');
		END
	`); err != nil {
		t.Fatal(err)
	}
	_, err := jobStore.SubmitDAGOperation(ctx, "dag-atomic", operations.SubmitDAGCommand{
		Request: api.SubmitWorkflowRequest{
			TenantID: "tenant-a",
			Nodes: []api.WorkflowNode{{
				Key: "node",
				Job: domain.JobSpec{Payload: domain.Payload{Type: domain.PayloadNoop}},
			}},
		},
		Actor:          "operator-a",
		IdempotencyKey: "atomic-dag",
		RequestDigest:  "atomic-dag-digest",
		RequestedAt:    command.RequestedAt,
	})
	if err == nil {
		t.Fatal("DAG submission succeeded despite injected audit failure")
	}
	var dags int
	if err := jobStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dag_runs").Scan(&dags); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || dags != 0 {
		t.Fatalf("jobs = %d, DAGs = %d after DAG rollback", jobs, dags)
	}
}

func TestOperationIdempotencyIsScopedByTenantAndAction(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "operation-scope.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 16, 0, 0, 0, time.UTC)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		_, err := jobStore.SubmitJobOperation(ctx, operations.SubmitJobCommand{
			Request: api.SubmitJobRequest{Job: domain.JobSpec{
				TenantID: tenantID,
				Payload:  domain.Payload{Type: domain.PayloadNoop},
			}},
			Actor:          "operator-a",
			IdempotencyKey: "shared-key",
			RequestDigest:  "job-" + tenantID,
			RequestedAt:    now,
		})
		if err != nil {
			t.Fatalf("submit %s: %v", tenantID, err)
		}
	}
	_, err := jobStore.SubmitDAGOperation(ctx, "dag-a", operations.SubmitDAGCommand{
		Request: api.SubmitWorkflowRequest{
			TenantID: "tenant-a",
			Nodes: []api.WorkflowNode{{
				Key: "node",
				Job: domain.JobSpec{Payload: domain.Payload{Type: domain.PayloadNoop}},
			}},
		},
		Actor:          "operator-a",
		IdempotencyKey: "shared-key",
		RequestDigest:  "dag-tenant-a",
		RequestedAt:    now,
	})
	if err != nil {
		t.Fatalf("submit DAG with shared key: %v", err)
	}
	var requests int
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM operation_requests WHERE idempotency_key = 'shared-key'",
	).Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if requests != 3 {
		t.Fatalf("operation requests = %d, want 3", requests)
	}
}

func TestDeadLetterRedriveCrashWindowAndConcurrentKeys(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "redrive-atomic.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 17, 0, 0, 0, time.UTC)
	source := createDeadLetterForOperationTest(t, jobStore, now, "source")

	if _, err := jobStore.db.ExecContext(ctx, `
		CREATE TRIGGER reject_redrive_audit
		BEFORE INSERT ON audit_events
		WHEN NEW.action = 'dead_letter.redrive'
		BEGIN
			SELECT RAISE(ABORT, 'injected audit failure');
		END
	`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := jobStore.RedriveDeadLetter(
		ctx,
		source.ID,
		"crash-key",
		"crash-digest",
		now.Add(2*time.Second),
	); err == nil {
		t.Fatal("redrive succeeded despite injected audit failure")
	}
	var jobs int
	var linked *string
	if err := jobStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT redriven_job_id FROM dead_letters WHERE job_id = ?",
		source.ID,
	).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || linked != nil {
		t.Fatalf("jobs = %d, linked = %v after rollback", jobs, linked)
	}
	if _, err := jobStore.db.ExecContext(ctx, "DROP TRIGGER reject_redrive_audit"); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, _, err := jobStore.RedriveDeadLetter(
				ctx,
				source.ID,
				"concurrent-key-"+string(rune('a'+index)),
				"concurrent-digest-"+string(rune('a'+index)),
				now.Add(3*time.Second),
			)
			results <- err
		}(index)
	}
	wait.Wait()
	close(results)
	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrDeadLetterRedriven):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent redrive error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes = %d, conflicts = %d", successes, conflicts)
	}
	var audits, completions int
	if err := jobStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM audit_events WHERE action = 'dead_letter.redrive'",
	).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM job_completions WHERE job_id = ?",
		source.ID,
	).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if jobs != 2 || audits != 1 || completions != 1 {
		t.Fatalf("jobs = %d, audits = %d, completions = %d", jobs, audits, completions)
	}
}

func createDeadLetterForOperationTest(
	t *testing.T,
	jobStore *Store,
	now time.Time,
	key string,
) domain.Job {
	t.Helper()
	job, _, err := jobStore.SubmitJob(t.Context(), storepkg.Submission{
		Job: domain.JobSpec{
			TenantID: "tenant-a",
			Payload:  domain.Payload{Type: domain.PayloadNoop},
		},
		IdempotencyKey: key,
		RequestDigest:  key + "-digest",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobStore.ApplyJobControl(t.Context(), JobControlCommand{
		JobID:          job.ID,
		ReceiptAction:  "dead_letter",
		AuditAction:    "job.force.dead_letter",
		Actor:          "operator-a",
		Reason:         "test dead letter",
		IdempotencyKey: key + "-dead-letter",
		RequestDigest:  key + "-dead-letter-digest",
		RequestedAt:    now.Add(time.Second),
		NextState:      domain.StateDeadLetter,
	}); err != nil {
		t.Fatal(err)
	}
	return job
}
