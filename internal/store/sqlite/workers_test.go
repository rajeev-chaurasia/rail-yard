package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func TestWorkerRegistrationSurvivesRestartAndReportsIdleHealth(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "workers.db")
	registeredAt := time.Date(2026, time.August, 14, 18, 0, 0, 0, time.UTC)

	first := openTestStore(t, path)
	if _, err := first.RegisterWorker(ctx, "worker-idle", 8, "", registeredAt); err != nil {
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

	if _, err := second.RegisterWorker(ctx, "worker-idle", 8, "", registeredAt.Add(time.Minute)); err != nil {
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
	if _, err := second.RegisterWorker(
		ctx,
		"worker-idle",
		4,
		"",
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

	if _, err := jobStore.RegisterWorker(ctx, "worker-health", 6, "", now); err != nil {
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
			_, err := jobStore.RegisterWorker(ctx, "worker-race", capacity, "", now)
			errs <- err
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

func TestNewWorkerIncarnationFencesPredecessorLeases(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "incarnation.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.November, 6, 9, 0, 0, 0, time.UTC)

	if _, err := jobStore.RegisterWorker(ctx, "worker-a", 4, "first", now); err != nil {
		t.Fatal(err)
	}
	submitTestJob(t, jobStore, "fence-1", "digest-fence-1", now)
	submitTestJob(t, jobStore, "fence-2", "digest-fence-2", now)
	killed := []domain.Lease{
		acquireOne(t, jobStore, "worker-a", now, time.Minute),
		acquireOne(t, jobStore, "worker-a", now, time.Minute),
	}

	fenced, err := jobStore.RegisterWorker(ctx, "worker-a", 4, "first", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(fenced) != 0 {
		t.Fatalf("same incarnation fenced %+v, want nothing", fenced)
	}

	restartedAt := now.Add(2 * time.Second)
	fenced, err = jobStore.RegisterWorker(ctx, "worker-a", 4, "second", restartedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(fenced) != len(killed) {
		t.Fatalf("fenced %d leases, want %d: %+v", len(fenced), len(killed), fenced)
	}
	for _, value := range fenced {
		if value.OldWorkerID != "worker-a" || !value.NextAvailableAt.Equal(restartedAt) {
			t.Fatalf("fenced lease = %+v", value)
		}
	}
	for _, lease := range killed {
		job, err := jobStore.GetJob(ctx, lease.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State != domain.StatePending {
			t.Fatalf("job %s state = %s, want PENDING", lease.JobID, job.State)
		}
		var attemptState, failureJSON string
		if err := jobStore.db.QueryRowContext(
			ctx,
			"SELECT state, failure_json FROM attempts WHERE job_id = ? AND lease_generation = ?",
			lease.JobID,
			lease.Generation,
		).Scan(&attemptState, &failureJSON); err != nil {
			t.Fatal(err)
		}
		if attemptState != "EXPIRED" || !strings.Contains(failureJSON, workerRestartedMessage) {
			t.Fatalf("killed attempt state=%s failure=%s", attemptState, failureJSON)
		}
	}

	successor := acquireOne(t, jobStore, "worker-a", restartedAt, time.Minute)
	if successor.Generation <= killed[0].Generation && successor.Generation <= killed[1].Generation {
		t.Fatalf("successor generation %d does not advance past killed leases", successor.Generation)
	}
	_, err = jobStore.Complete(ctx, domain.Completion{
		LeaseRef: leaseRef(killed[0]),
		WorkerID: "worker-a",
		Success:  true,
	}, restartedAt.Add(time.Millisecond))
	if !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("predecessor completion error = %v, want stale lease", err)
	}

	fenced, err = jobStore.RegisterWorker(ctx, "worker-a", 4, "second", restartedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(fenced) != 0 {
		t.Fatalf("re-registration by the restarted process fenced its own leases: %+v", fenced)
	}
}

func TestRegistrationWithoutIncarnationNeverFences(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "legacy-incarnation.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.November, 6, 10, 0, 0, 0, time.UTC)

	if _, err := jobStore.RegisterWorker(ctx, "worker-a", 2, "", now); err != nil {
		t.Fatal(err)
	}
	submitTestJob(t, jobStore, "legacy", "digest-legacy", now)
	acquireOne(t, jobStore, "worker-a", now, time.Minute)

	for index, incarnation := range []string{"upgraded", "", "upgraded"} {
		fenced, err := jobStore.RegisterWorker(
			ctx,
			"worker-a",
			2,
			incarnation,
			now.Add(time.Duration(index+1)*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(fenced) != 0 {
			t.Fatalf("registration %d with incarnation %q fenced %+v", index, incarnation, fenced)
		}
	}
}
