package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
	"github.com/rajeev-chaurasia/rail-yard/internal/trigger"
)

type triggerStoreStub struct {
	create func(context.Context, store.CronSubmission, time.Time) (domain.CronTrigger, bool, error)
}

func (stub *triggerStoreStub) CreateCronTrigger(
	ctx context.Context,
	submission store.CronSubmission,
	now time.Time,
) (domain.CronTrigger, bool, error) {
	return stub.create(ctx, submission, now)
}

func (*triggerStoreStub) FireDueCron(context.Context, time.Time, int) ([]string, error) {
	return nil, nil
}

func (*triggerStoreStub) DeliverRedis(context.Context, trigger.RedisDelivery) error {
	return nil
}

func TestCreateCronTrigger(t *testing.T) {
	var captured store.CronSubmission
	triggerStore := &triggerStoreStub{
		create: func(
			_ context.Context,
			submission store.CronSubmission,
			now time.Time,
		) (domain.CronTrigger, bool, error) {
			captured = submission
			value := submission.Trigger
			value.ID = "trigger-1"
			value.NextFireAt = now.Add(time.Minute)
			return value, false, nil
		},
	}
	app := newTestServer(t, &fakeStore{}, func(config *Config) {
		config.TriggerStore = triggerStore
		config.CronInterval = time.Hour
	})
	response := performRequest(
		app,
		http.MethodPost,
		"/v1/triggers/cron",
		`{
			"tenant_id":"tenant",
			"expression":"* * * * *",
			"job":{"queue":"cron","payload":{"type":"noop"}}
		}`,
		"cron-key",
	)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if captured.IdempotencyKey != "cron-key" ||
		captured.Actor != "test-actor" ||
		captured.Trigger.TenantID != "tenant" ||
		captured.Trigger.Job.Queue != "cron" {
		t.Fatalf("captured submission = %+v", captured)
	}
}
