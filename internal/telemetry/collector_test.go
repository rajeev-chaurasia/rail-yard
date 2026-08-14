package telemetry

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
	sqlitestore "github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
	telemetrymodel "github.com/rajeev-chaurasia/rail-yard/internal/telemetry/model"
)

func TestCollectorRefreshesSnapshotAndConsumesEventsOnce(t *testing.T) {
	ready := 2 * time.Second
	source := &collectorSource{
		snapshot: telemetrymodel.Snapshot{
			Pending:   3,
			Scheduled: 2,
			Running:   1,
			Retrying:  4,
			DLQ:       5,
		},
		events: []telemetrymodel.TimingEvent{{
			Sequence:     7,
			ReadyToLease: &ready,
		}},
	}
	metrics, err := New()
	if err != nil {
		t.Fatal(err)
	}
	collector := NewCollector(source, metrics)

	if err := collector.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := collector.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}

	for state, want := range map[string]float64{
		"pending":   3,
		"scheduled": 2,
		"running":   1,
		"retrying":  4,
	} {
		if got := gaugeValue(
			t,
			metrics.Registry(),
			"railyard_queue_depth",
			map[string]string{"state": state},
		); got != want {
			t.Errorf("%s depth = %v, want %v", state, got, want)
		}
	}
	if got := gaugeValue(t, metrics.Registry(), "railyard_dlq_depth", nil); got != 5 {
		t.Fatalf("DLQ depth = %v, want 5", got)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_job_latency_seconds",
		map[string]string{"stage": "ready_to_lease"},
	); got != 1 {
		t.Fatalf("ready-to-lease observations = %d, want 1", got)
	}
}

func TestCollectorStartsAtCurrentDurableSequence(t *testing.T) {
	ready := time.Second
	source := &collectorSource{
		snapshot: telemetrymodel.Snapshot{Sequence: 4},
		events: []telemetrymodel.TimingEvent{{
			Sequence:     4,
			ReadyToLease: &ready,
		}},
	}
	metrics, err := New()
	if err != nil {
		t.Fatal(err)
	}
	collector := NewCollector(source, metrics)
	if err := collector.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_job_latency_seconds",
		map[string]string{"stage": "ready_to_lease"},
	); got != 0 {
		t.Fatalf("historical observations at startup = %d, want 0", got)
	}

	source.events = append(source.events, telemetrymodel.TimingEvent{
		Sequence:     5,
		ReadyToLease: &ready,
	})
	if err := collector.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_job_latency_seconds",
		map[string]string{"stage": "ready_to_lease"},
	); got != 1 {
		t.Fatalf("new observations after startup = %d, want 1", got)
	}
}

func TestCollectorUsesDurableOriginsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "telemetry-restart.db")
	now := time.Date(2026, time.August, 14, 16, 0, 0, 0, time.UTC)

	first, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.SubmitJob(ctx, telemetrySubmission("completion"), now); err != nil {
		t.Fatal(err)
	}
	leases, err := first.Acquire(ctx, "worker-a", 1, 1, now.Add(time.Second), time.Minute)
	if err != nil || len(leases) != 1 {
		t.Fatalf("acquire before restart: leases=%d err=%v", len(leases), err)
	}
	completionRef := domain.LeaseRef{
		JobID:      leases[0].JobID,
		AttemptNo:  leases[0].AttemptNo,
		Generation: leases[0].Generation,
		Token:      leases[0].Token,
	}
	if _, _, err := first.SubmitJob(
		ctx,
		telemetrySubmission("recovery"),
		now.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Acquire(
		ctx,
		"worker-b",
		1,
		1,
		now.Add(2*time.Second),
		time.Second,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ReapExpired(ctx, now.Add(3*time.Second), 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.SubmitJob(
		ctx,
		telemetrySubmission("ready"),
		now.Add(4*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := sqlitestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	metrics, err := New()
	if err != nil {
		t.Fatal(err)
	}
	collector := NewCollector(second, metrics)
	if err := collector.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	if _, err := second.Complete(ctx, domain.Completion{
		LeaseRef: completionRef,
		WorkerID: "worker-a",
		Success:  true,
	}, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	leases, err = second.Acquire(
		ctx,
		"worker-c",
		2,
		2,
		now.Add(6*time.Second),
		time.Minute,
	)
	if err != nil || len(leases) != 2 {
		t.Fatalf("acquire after restart: leases=%d err=%v", len(leases), err)
	}
	if err := collector.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	for _, stage := range []string{"ready_to_lease", "lease_to_completion", "end_to_end"} {
		want := uint64(1)
		if stage == "ready_to_lease" {
			want = 2
		}
		if got := histogramCount(
			t,
			metrics.Registry(),
			"railyard_job_latency_seconds",
			map[string]string{"stage": stage},
		); got != want {
			t.Errorf("%s observations after restart = %d, want %d", stage, got, want)
		}
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_lease_recovery_duration_seconds",
		nil,
	); got != 1 {
		t.Fatalf("recovery observations after restart = %d, want 1", got)
	}
}

type collectorSource struct {
	snapshot telemetrymodel.Snapshot
	events   []telemetrymodel.TimingEvent
}

func (s *collectorSource) TelemetrySnapshot(context.Context) (telemetrymodel.Snapshot, error) {
	return s.snapshot, nil
}

func (s *collectorSource) TelemetryEvents(
	_ context.Context,
	afterSequence int64,
	_ int,
) ([]telemetrymodel.TimingEvent, error) {
	for index, event := range s.events {
		if event.Sequence > afterSequence {
			return s.events[index:], nil
		}
	}
	return []telemetrymodel.TimingEvent{}, nil
}

func telemetrySubmission(key string) store.Submission {
	return store.Submission{
		IdempotencyKey: key,
		RequestDigest:  fmt.Sprintf("%s-digest", key),
		Job: domain.JobSpec{
			TenantID: "tenant-a",
			Queue:    "default",
			SlotCost: 1,
			Payload:  domain.Payload{Type: domain.PayloadNoop},
			Retry:    domain.RetryPolicy{MaxAttempts: 3},
		},
	}
}
