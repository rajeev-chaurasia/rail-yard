package server

import (
	"context"
	"sync"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

type fakeStore struct {
	submitJob      func(context.Context, store.Submission, time.Time) (domain.Job, bool, error)
	submitWorkflow func(
		context.Context,
		store.WorkflowSubmission,
		time.Time,
	) ([]domain.Job, bool, error)
	getJob           func(context.Context, string) (domain.Job, error)
	acquire          func(context.Context, string, int, int, time.Time, time.Duration) ([]domain.Lease, error)
	markRunning      func(context.Context, string, domain.LeaseRef, time.Time) error
	markRunningBatch func(
		context.Context,
		string,
		[]domain.LeaseRef,
		time.Time,
	) ([]store.AttemptStartResult, error)
	heartbeat func(
		context.Context,
		string,
		[]domain.LeaseRef,
		time.Time,
		time.Duration,
	) ([]api.HeartbeatResult, error)
	complete      func(context.Context, domain.Completion, time.Time) (domain.CompletionReceipt, error)
	completeBatch func(
		context.Context,
		[]domain.Completion,
		time.Time,
	) ([]store.CompletionResult, error)
	promoteDue func(context.Context, time.Time, int) (int, error)
	reap       func(context.Context, time.Time, int) ([]domain.ReapedLease, error)

	closeMu    sync.Mutex
	closeCalls int
	closeError error
}

func (f *fakeStore) SubmitJob(
	ctx context.Context,
	submission store.Submission,
	now time.Time,
) (domain.Job, bool, error) {
	if f.submitJob == nil {
		return domain.Job{}, false, nil
	}
	return f.submitJob(ctx, submission, now)
}

func (f *fakeStore) SubmitWorkflow(
	ctx context.Context,
	submission store.WorkflowSubmission,
	now time.Time,
) ([]domain.Job, bool, error) {
	if f.submitWorkflow == nil {
		return nil, false, nil
	}
	return f.submitWorkflow(ctx, submission, now)
}

func (f *fakeStore) GetJob(ctx context.Context, jobID string) (domain.Job, error) {
	if f.getJob == nil {
		return domain.Job{}, nil
	}
	return f.getJob(ctx, jobID)
}

func (f *fakeStore) Acquire(
	ctx context.Context,
	workerID string,
	availableSlots int,
	limit int,
	now time.Time,
	leaseTTL time.Duration,
) ([]domain.Lease, error) {
	if f.acquire == nil {
		return nil, nil
	}
	return f.acquire(ctx, workerID, availableSlots, limit, now, leaseTTL)
}

func (f *fakeStore) MarkRunning(
	ctx context.Context,
	workerID string,
	lease domain.LeaseRef,
	now time.Time,
) error {
	if f.markRunning == nil {
		return nil
	}
	return f.markRunning(ctx, workerID, lease, now)
}

func (f *fakeStore) MarkRunningBatch(
	ctx context.Context,
	workerID string,
	refs []domain.LeaseRef,
	now time.Time,
) ([]store.AttemptStartResult, error) {
	if f.markRunningBatch != nil {
		return f.markRunningBatch(ctx, workerID, refs, now)
	}
	results := make([]store.AttemptStartResult, len(refs))
	for index, ref := range refs {
		results[index].Err = f.MarkRunning(ctx, workerID, ref, now)
	}
	return results, nil
}

func (f *fakeStore) Heartbeat(
	ctx context.Context,
	workerID string,
	leases []domain.LeaseRef,
	now time.Time,
	leaseTTL time.Duration,
) ([]api.HeartbeatResult, error) {
	if f.heartbeat == nil {
		return nil, nil
	}
	return f.heartbeat(ctx, workerID, leases, now, leaseTTL)
}

func (f *fakeStore) Complete(
	ctx context.Context,
	completion domain.Completion,
	now time.Time,
) (domain.CompletionReceipt, error) {
	if f.complete == nil {
		return domain.CompletionReceipt{}, nil
	}
	return f.complete(ctx, completion, now)
}

func (f *fakeStore) CompleteBatch(
	ctx context.Context,
	completions []domain.Completion,
	now time.Time,
) ([]store.CompletionResult, error) {
	if f.completeBatch != nil {
		return f.completeBatch(ctx, completions, now)
	}
	results := make([]store.CompletionResult, len(completions))
	for index, completion := range completions {
		results[index].Receipt, results[index].Err = f.Complete(ctx, completion, now)
	}
	return results, nil
}

func (f *fakeStore) PromoteDue(ctx context.Context, now time.Time, limit int) (int, error) {
	if f.promoteDue == nil {
		return 0, nil
	}
	return f.promoteDue(ctx, now, limit)
}

func (f *fakeStore) ReapExpired(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]domain.ReapedLease, error) {
	if f.reap == nil {
		return nil, nil
	}
	return f.reap(ctx, now, limit)
}

func (f *fakeStore) Close() error {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	f.closeCalls++
	return f.closeError
}

func (f *fakeStore) numberOfCloseCalls() int {
	f.closeMu.Lock()
	defer f.closeMu.Unlock()
	return f.closeCalls
}
