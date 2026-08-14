package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func TestWorkerRegistrationSurvivesRestartAndReportsIdleHealth(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workers.db")
	registeredAt := time.Date(2026, time.August, 14, 18, 0, 0, 0, time.UTC)

	first := openTestStore(t, path)
	if err := first.RegisterWorker(ctx, "worker-idle", 8, registeredAt); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := openTestStore(t, path)
	t.Cleanup(func() { _ = second.Close() })
	health := workerHealthAt(t, second, registeredAt.Add(time.Second))
	if health.WorkerID != "worker-idle" ||
		health.CapacitySlots != 8 ||
		health.ActiveSlots != 0 ||
		health.ActiveLeases != 0 ||
		health.Status != operations.WorkerHealthy ||
		!health.LastHeartbeatAt.Equal(registeredAt) {
		t.Fatalf("idle worker health = %+v", health)
	}

	if err := second.RegisterWorker(ctx, "worker-idle", 8, registeredAt.Add(time.Minute)); err != nil {
		t.Fatalf("idempotent registration: %v", err)
	}
	var storedRegistration int64
	if err := second.db.QueryRowContext(
		ctx,
		"SELECT registered_at FROM workers WHERE worker_id = ?",
		"worker-idle",
	).Scan(&storedRegistration); err != nil {
		t.Fatal(err)
	}
	if got := timeFromDB(storedRegistration); !got.Equal(registeredAt) {
		t.Fatalf("registration time = %s, want %s", got, registeredAt)
	}
	if err := second.RegisterWorker(
		ctx,
		"worker-idle",
		4,
		registeredAt.Add(2*time.Minute),
	); !errors.Is(err, storepkg.ErrWorkerCapacityConflict) {
		t.Fatalf("capacity conflict error = %v", err)
	}
}

func TestWorkerHeartbeatHealthTransitionsAndActiveCapacity(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "worker-health.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 19, 0, 0, 0, time.UTC)

	if err := jobStore.RegisterWorker(ctx, "worker-health", 6, now); err != nil {
		t.Fatal(err)
	}
	if status := workerHealthAt(t, jobStore, now.Add(31*time.Second)).Status; status != operations.WorkerStale {
		t.Fatalf("status after 31 seconds = %s, want stale", status)
	}
	if status := workerHealthAt(t, jobStore, now.Add(121*time.Second)).Status; status != operations.WorkerOffline {
		t.Fatalf("status after 121 seconds = %s, want offline", status)
	}

	heartbeatAt := now.Add(3 * time.Minute)
	if err := jobStore.HeartbeatWorker(ctx, "worker-health", heartbeatAt); err != nil {
		t.Fatal(err)
	}
	submitTestJob(t, jobStore, "worker-health-job", "worker-health-digest", heartbeatAt)
	lease := acquireOne(t, jobStore, "worker-health", heartbeatAt.Add(time.Second), time.Minute)
	health := workerHealthAt(t, jobStore, heartbeatAt.Add(2*time.Second))
	if health.Status != operations.WorkerHealthy ||
		health.CapacitySlots != 6 ||
		health.ActiveSlots != lease.SlotCost ||
		health.ActiveLeases != 1 ||
		!health.LastHeartbeatAt.Equal(heartbeatAt) {
		t.Fatalf("active worker health = %+v", health)
	}
}

func TestConcurrentWorkerRegistrationKeepsOneCapacity(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "worker-race.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 20, 0, 0, 0, time.UTC)

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		capacity := 4
		if index%2 == 1 {
			capacity = 8
		}
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			errs <- jobStore.RegisterWorker(ctx, "worker-race", capacity, now)
		}()
	}
	close(start)
	wait.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, storepkg.ErrWorkerCapacityConflict):
			conflicts++
		default:
			t.Fatalf("registration error = %v", err)
		}
	}
	if successes != callers/2 || conflicts != callers/2 {
		t.Fatalf("successes = %d, conflicts = %d", successes, conflicts)
	}
	health := workerHealthAt(t, jobStore, now)
	if health.CapacitySlots != 4 && health.CapacitySlots != 8 {
		t.Fatalf("durable capacity = %d", health.CapacitySlots)
	}
}

func workerHealthAt(t *testing.T, jobStore *Store, now time.Time) operations.WorkerHealth {
	t.Helper()
	values, err := jobStore.WorkerHealth(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 {
		t.Fatalf("worker health rows = %d, want 1", len(values))
	}
	return values[0]
}
