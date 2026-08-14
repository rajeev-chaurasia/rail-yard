package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
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
