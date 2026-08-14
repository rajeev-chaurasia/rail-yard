package lease

import (
	"context"
	"errors"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/clock"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type Repository interface {
	Acquire(context.Context, string, int, int, time.Time, time.Duration) ([]domain.Lease, error)
	MarkRunning(context.Context, string, domain.LeaseRef, time.Time) error
	Heartbeat(context.Context, string, []domain.LeaseRef, time.Time, time.Duration) ([]api.HeartbeatResult, error)
	Complete(context.Context, domain.Completion, time.Time) (domain.CompletionReceipt, error)
	ReapExpired(context.Context, time.Time, int) ([]domain.ReapedLease, error)
}

type Service struct {
	store     Repository
	clock     clock.Clock
	ttl       time.Duration
	reapLimit int
}

func NewService(jobStore Repository, jobClock clock.Clock, ttl time.Duration, reapLimit int) (*Service, error) {
	if jobStore == nil || jobClock == nil {
		return nil, errors.New("store and clock are required")
	}
	if ttl <= 0 || reapLimit < 1 {
		return nil, errors.New("lease TTL and reap limit must be positive")
	}
	return &Service{store: jobStore, clock: jobClock, ttl: ttl, reapLimit: reapLimit}, nil
}

func (service *Service) Acquire(ctx context.Context, workerID string, slots, limit int) ([]domain.Lease, error) {
	return service.store.Acquire(ctx, workerID, slots, limit, service.clock.Now(), service.ttl)
}

func (service *Service) Start(ctx context.Context, workerID string, ref domain.LeaseRef) error {
	return service.store.MarkRunning(ctx, workerID, ref, service.clock.Now())
}

func (service *Service) Heartbeat(
	ctx context.Context,
	workerID string,
	refs []domain.LeaseRef,
) ([]api.HeartbeatResult, error) {
	return service.store.Heartbeat(ctx, workerID, refs, service.clock.Now(), service.ttl)
}

func (service *Service) Complete(
	ctx context.Context,
	completion domain.Completion,
) (domain.CompletionReceipt, error) {
	return service.store.Complete(ctx, completion, service.clock.Now())
}

func (service *Service) Reap(ctx context.Context) ([]domain.ReapedLease, error) {
	return service.store.ReapExpired(ctx, service.clock.Now(), service.reapLimit)
}
