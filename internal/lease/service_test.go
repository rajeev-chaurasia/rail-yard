package lease

import (
	"context"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/clock"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type repositoryStub struct {
	acquireAt time.Time
	ttl       time.Duration
	reapAt    time.Time
	reapLimit int
}

func (stub *repositoryStub) Acquire(
	_ context.Context,
	_ string,
	_, _ int,
	now time.Time,
	ttl time.Duration,
) ([]domain.Lease, error) {
	stub.acquireAt = now
	stub.ttl = ttl
	return []domain.Lease{{JobID: "job"}}, nil
}

func (stub *repositoryStub) MarkRunning(context.Context, string, domain.LeaseRef, time.Time) error {
	return nil
}

func (stub *repositoryStub) Heartbeat(
	context.Context,
	string,
	[]domain.LeaseRef,
	time.Time,
	time.Duration,
) ([]api.HeartbeatResult, error) {
	return nil, nil
}

func (stub *repositoryStub) Complete(
	context.Context,
	domain.Completion,
	time.Time,
) (domain.CompletionReceipt, error) {
	return domain.CompletionReceipt{}, nil
}

func (stub *repositoryStub) ReapExpired(
	_ context.Context,
	now time.Time,
	limit int,
) ([]domain.ReapedLease, error) {
	stub.reapAt = now
	stub.reapLimit = limit
	return nil, nil
}

func TestServiceUsesInjectedClockAndPolicy(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	jobClock := clock.NewFake(now)
	repository := &repositoryStub{}
	service, err := NewService(repository, jobClock, 3*time.Second, 100)
	if err != nil {
		t.Fatal(err)
	}

	leases, err := service.Acquire(context.Background(), "worker", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || repository.acquireAt != now || repository.ttl != 3*time.Second {
		t.Fatalf("unexpected acquire call: leases=%v repository=%+v", leases, repository)
	}

	jobClock.Advance(time.Second)
	if _, err := service.Reap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if repository.reapAt != now.Add(time.Second) || repository.reapLimit != 100 {
		t.Fatalf("unexpected reap call: %+v", repository)
	}
}
