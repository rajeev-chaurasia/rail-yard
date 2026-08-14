package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
	"github.com/rajeev-chaurasia/rail-yard/internal/trigger"
)

func TestCronTriggerFiresOneDurableOccurrence(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "cron.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 8, 0, 30, 0, time.UTC)
	submission := storepkg.CronSubmission{
		Trigger: domain.CronTrigger{
			TenantID:   "tenant",
			Expression: "* * * * *",
			Job: domain.JobSpec{
				Queue:    "cron",
				SlotCost: 1,
				Payload:  domain.Payload{Type: domain.PayloadNoop},
			},
		},
		IdempotencyKey: "cron-trigger",
		RequestDigest:  "cron-trigger-digest",
	}
	created, duplicate, err := jobStore.CreateCronTrigger(ctx, submission, now)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate || !created.NextFireAt.Equal(time.Date(2026, time.August, 14, 8, 1, 0, 0, time.UTC)) {
		t.Fatalf("created trigger = %+v, duplicate = %t", created, duplicate)
	}
	repeated, duplicate, err := jobStore.CreateCronTrigger(ctx, submission, now)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate || repeated.ID != created.ID {
		t.Fatalf("duplicate trigger = %+v, duplicate = %t", repeated, duplicate)
	}

	jobIDs, err := jobStore.FireDueCron(ctx, created.NextFireAt, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobIDs) != 1 {
		t.Fatalf("fired jobs = %v", jobIDs)
	}
	job, err := jobStore.GetJob(ctx, jobIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if job.TenantID != "tenant" || job.Queue != "cron" {
		t.Fatalf("cron job = %+v", job)
	}
	var occurrences int
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM cron_occurrences WHERE trigger_id = ?",
		created.ID,
	).Scan(&occurrences); err != nil {
		t.Fatal(err)
	}
	if occurrences != 1 {
		t.Fatalf("occurrences = %d, want 1", occurrences)
	}
}

func TestRedisDeliveryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "redis.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	spec := domain.JobSpec{
		TenantID: "tenant",
		Queue:    "events",
		SlotCost: 1,
		Payload:  domain.Payload{Type: domain.PayloadNoop},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	delivery := trigger.RedisDelivery{
		TriggerID: "default",
		Stream:    "events",
		MessageID: "1-0",
		Values:    map[string]any{"job": string(encoded)},
	}
	if err := jobStore.DeliverRedis(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.DeliverRedis(ctx, delivery); err != nil {
		t.Fatal(err)
	}

	var jobs, deliveries int
	if err := jobStore.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs").Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := jobStore.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM redis_deliveries",
	).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || deliveries != 1 {
		t.Fatalf("jobs = %d, deliveries = %d, want 1 and 1", jobs, deliveries)
	}
}
