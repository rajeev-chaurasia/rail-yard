package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	telemetrymodel "github.com/rajeev-chaurasia/rail-yard/internal/telemetry/model"
)

func TestTelemetrySnapshotReportsDurableState(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "telemetry-snapshot.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 14, 0, 0, 0, time.UTC)

	submitTestJob(t, jobStore, "pending", "pending-digest", now)
	submitTestJob(t, jobStore, "running", "running-digest", now)
	lease := acquireOne(t, jobStore, "worker-a", now.Add(time.Second), time.Minute)
	if err := jobStore.MarkRunning(
		ctx,
		"worker-a",
		leaseRef(lease),
		now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	snapshot, err := jobStore.TelemetrySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Pending != 1 ||
		snapshot.Scheduled != 0 ||
		snapshot.Running != 1 ||
		snapshot.Retrying != 0 ||
		snapshot.DLQ != 0 {
		t.Fatalf("unexpected telemetry snapshot: %+v", snapshot)
	}
}

func TestTelemetryEventsUseDurableLifecycleOrigins(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "telemetry-events.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 14, 30, 0, 0, time.UTC)

	submitTestJob(t, jobStore, "timed", "timed-digest", now)
	lease := acquireOne(t, jobStore, "worker-a", now.Add(2*time.Second), time.Minute)
	if err := jobStore.MarkRunning(
		ctx,
		"worker-a",
		leaseRef(lease),
		now.Add(3*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := jobStore.Complete(ctx, domain.Completion{
		LeaseRef: leaseRef(lease),
		WorkerID: "worker-a",
		Success:  true,
	}, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}

	events, err := jobStore.TelemetryEvents(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	assertTimingEvent(t, events, "ready-to-lease", func(event eventDurationSet) *time.Duration {
		return event.ReadyToLease
	}, 2*time.Second)
	assertTimingEvent(t, events, "lease-to-completion", func(event eventDurationSet) *time.Duration {
		return event.LeaseToCompletion
	}, 3*time.Second)
	assertTimingEvent(t, events, "end-to-end", func(event eventDurationSet) *time.Duration {
		return event.EndToEnd
	}, 5*time.Second)
}

func TestTelemetryReportsRecoveryAndCurrentDLQ(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "telemetry-recovery.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 15, 0, 0, 0, time.UTC)

	submitTestJob(t, jobStore, "recovery", "recovery-digest", now)
	acquireOne(t, jobStore, "worker-a", now.Add(time.Second), time.Second)
	if _, err := jobStore.ReapExpired(ctx, now.Add(2*time.Second), 1); err != nil {
		t.Fatal(err)
	}
	acquireOne(t, jobStore, "worker-b", now.Add(4*time.Second), time.Minute)

	events, err := jobStore.TelemetryEvents(ctx, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	assertTimingEvent(t, events, "lease recovery", func(event eventDurationSet) *time.Duration {
		return event.LeaseRecovery
	}, 2*time.Second)

	submission := testSubmission("dead-letter", "dead-letter-digest")
	submission.Job.Retry.MaxAttempts = 1
	if _, _, err := jobStore.SubmitJob(ctx, submission, now.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	deadLease := acquireOne(t, jobStore, "worker-c", now.Add(11*time.Second), time.Second)
	if _, err := jobStore.ReapExpired(ctx, now.Add(12*time.Second), 1); err != nil {
		t.Fatal(err)
	}
	snapshot, err := jobStore.TelemetrySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.DLQ != 1 {
		t.Fatalf("DLQ depth = %d, want 1", snapshot.DLQ)
	}
	if _, _, err := jobStore.RedriveDeadLetter(
		ctx,
		deadLease.JobID,
		"redrive",
		"redrive-digest",
		now.Add(13*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	snapshot, err = jobStore.TelemetrySnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.DLQ != 0 {
		t.Fatalf("DLQ depth after redrive = %d, want 0", snapshot.DLQ)
	}
}

type eventDurationSet struct {
	ReadyToLease      *time.Duration
	LeaseToCompletion *time.Duration
	EndToEnd          *time.Duration
	LeaseRecovery     *time.Duration
}

func assertTimingEvent(
	t *testing.T,
	events []telemetrymodel.TimingEvent,
	name string,
	selectDuration func(eventDurationSet) *time.Duration,
	want time.Duration,
) {
	t.Helper()
	for _, event := range events {
		duration := selectDuration(eventDurationSet{
			ReadyToLease:      event.ReadyToLease,
			LeaseToCompletion: event.LeaseToCompletion,
			EndToEnd:          event.EndToEnd,
			LeaseRecovery:     event.LeaseRecovery,
		})
		if duration != nil {
			if *duration != want {
				t.Fatalf("%s = %s, want %s", name, *duration, want)
			}
			return
		}
	}
	t.Fatalf("%s observation not found", name)
}
