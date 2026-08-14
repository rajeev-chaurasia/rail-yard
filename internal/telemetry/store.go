package telemetry

import (
	"context"
	"errors"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

type ObservedStore struct {
	next    store.Store
	metrics *Metrics
}

func ObserveStore(next store.Store, metrics *Metrics) *ObservedStore {
	return &ObservedStore{next: next, metrics: metrics}
}

func (s *ObservedStore) SubmitJob(
	ctx context.Context,
	submission store.Submission,
	now time.Time,
) (domain.Job, bool, error) {
	started := time.Now()
	job, duplicate, err := s.next.SubmitJob(ctx, submission, now)
	s.observeSQLite(SQLiteSubmitJob, started, err)
	s.recordAdmission(duplicate, err)
	return job, duplicate, err
}

func (s *ObservedStore) SubmitWorkflow(
	ctx context.Context,
	submission store.WorkflowSubmission,
	now time.Time,
) ([]domain.Job, bool, error) {
	started := time.Now()
	jobs, duplicate, err := s.next.SubmitWorkflow(ctx, submission, now)
	s.observeSQLite(SQLiteSubmitWorkflow, started, err)
	s.recordAdmission(duplicate, err)
	return jobs, duplicate, err
}

func (s *ObservedStore) GetJob(ctx context.Context, jobID string) (domain.Job, error) {
	return s.next.GetJob(ctx, jobID)
}

func (s *ObservedStore) Acquire(
	ctx context.Context,
	workerID string,
	slots int,
	limit int,
	now time.Time,
	ttl time.Duration,
) ([]domain.Lease, error) {
	started := time.Now()
	leases, err := s.next.Acquire(ctx, workerID, slots, limit, now, ttl)
	s.observeSQLite(SQLiteAcquireLease, started, err)
	switch {
	case err != nil:
		s.metrics.RecordSchedulerDecision(SchedulerFailed, 0)
	case len(leases) == 0:
		s.metrics.RecordSchedulerDecision(SchedulerQueueEmpty, 0)
	default:
		s.metrics.RecordSchedulerDecision(SchedulerGranted, len(leases))
	}
	s.recordLeaseLatency(leases, now)
	return leases, err
}

func (s *ObservedStore) AcquireWithRecoveryReserve(
	ctx context.Context,
	workerID string,
	slots int,
	limit int,
	now time.Time,
	ttl time.Duration,
	recoveryReserve int,
) ([]domain.Lease, error) {
	next, ok := s.next.(store.RecoveryAwareStore)
	if !ok {
		return s.Acquire(ctx, workerID, slots, limit, now, ttl)
	}
	started := time.Now()
	leases, err := next.AcquireWithRecoveryReserve(
		ctx,
		workerID,
		slots,
		limit,
		now,
		ttl,
		recoveryReserve,
	)
	s.observeSQLite(SQLiteAcquireLease, started, err)
	switch {
	case err != nil:
		s.metrics.RecordSchedulerDecision(SchedulerFailed, 0)
	case len(leases) == 0:
		s.metrics.RecordSchedulerDecision(SchedulerQueueEmpty, 0)
	default:
		s.metrics.RecordSchedulerDecision(SchedulerGranted, len(leases))
	}
	s.recordLeaseLatency(leases, now)
	return leases, err
}

func (s *ObservedStore) MarkRunning(
	ctx context.Context,
	workerID string,
	ref domain.LeaseRef,
	now time.Time,
) error {
	started := time.Now()
	err := s.next.MarkRunning(ctx, workerID, ref, now)
	s.observeSQLite(SQLiteMarkRunning, started, err)
	return err
}

func (s *ObservedStore) MarkRunningBatch(
	ctx context.Context,
	workerID string,
	refs []domain.LeaseRef,
	now time.Time,
) ([]store.AttemptStartResult, error) {
	next, ok := s.next.(store.BatchAttemptStartStore)
	if !ok {
		results := make([]store.AttemptStartResult, len(refs))
		for index, ref := range refs {
			results[index].Err = s.MarkRunning(ctx, workerID, ref, now)
		}
		return results, nil
	}

	started := time.Now()
	results, err := next.MarkRunningBatch(ctx, workerID, refs, now)
	s.observeSQLite(SQLiteMarkRunning, started, err)
	return results, err
}

func (s *ObservedStore) Heartbeat(
	ctx context.Context,
	workerID string,
	refs []domain.LeaseRef,
	now time.Time,
	ttl time.Duration,
) ([]api.HeartbeatResult, error) {
	started := time.Now()
	results, err := s.next.Heartbeat(ctx, workerID, refs, now, ttl)
	s.observeSQLite(SQLiteHeartbeat, started, err)
	return results, err
}

func (s *ObservedStore) Complete(
	ctx context.Context,
	completion domain.Completion,
	now time.Time,
) (domain.CompletionReceipt, error) {
	started := time.Now()
	receipt, err := s.next.Complete(ctx, completion, now)
	s.observeSQLite(SQLiteComplete, started, err)
	if err == nil {
		s.recordCompletionReceipt(ctx, receipt)
	}
	return receipt, err
}

func (s *ObservedStore) CompleteBatch(
	ctx context.Context,
	completions []domain.Completion,
	now time.Time,
) ([]store.CompletionResult, error) {
	next, ok := s.next.(store.BatchCompletionStore)
	if !ok {
		results := make([]store.CompletionResult, len(completions))
		for index, completion := range completions {
			results[index].Receipt, results[index].Err = s.Complete(ctx, completion, now)
		}
		return results, nil
	}

	started := time.Now()
	results, err := next.CompleteBatch(ctx, completions, now)
	s.observeSQLite(SQLiteComplete, started, err)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if result.Err == nil {
			s.recordCompletionReceipt(ctx, result.Receipt)
		}
	}
	return results, nil
}

func (s *ObservedStore) PromoteDue(ctx context.Context, now time.Time, limit int) (int, error) {
	started := time.Now()
	count, err := s.next.PromoteDue(ctx, now, limit)
	s.observeSQLite(SQLitePromoteDue, started, err)
	return count, err
}

func (s *ObservedStore) ReapExpired(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]domain.ReapedLease, error) {
	started := time.Now()
	values, err := s.next.ReapExpired(ctx, now, limit)
	s.observeSQLite(SQLiteReapExpired, started, err)
	if err == nil {
		deadLettered := false
		for _, value := range values {
			if value.NextAvailableAt.IsZero() {
				deadLettered = true
				s.metrics.RecordLeaseExpiration(LeaseDeadLettered)
				s.metrics.RecordDeadLetter(DeadLetterRetriesExhausted)
			} else {
				s.metrics.RecordLeaseExpiration(LeaseRequeued)
				s.metrics.RecordRetry(RetryLeaseExpired)
			}
		}
		if deadLettered {
			_ = s.RefreshDLQDepth(ctx)
		}
	}
	return values, err
}

func (s *ObservedStore) Close() error {
	return s.next.Close()
}

func (s *ObservedStore) ListDeadLetters(
	ctx context.Context,
	limit int,
) ([]domain.DeadLetter, error) {
	next, ok := s.next.(store.DeadLetterStore)
	if !ok {
		return nil, errors.New("dead-letter storage is not supported")
	}
	return next.ListDeadLetters(ctx, limit)
}

func (s *ObservedStore) RedriveDeadLetter(
	ctx context.Context,
	jobID string,
	key string,
	digest string,
	now time.Time,
) (domain.Job, bool, error) {
	next, ok := s.next.(store.DeadLetterStore)
	if !ok {
		return domain.Job{}, false, errors.New("dead-letter storage is not supported")
	}
	job, duplicate, err := next.RedriveDeadLetter(ctx, jobID, key, digest, now)
	if err == nil {
		_ = s.RefreshDLQDepth(ctx)
	}
	return job, duplicate, err
}

func (s *ObservedStore) RefreshDLQDepth(ctx context.Context) error {
	next, ok := s.next.(store.DeadLetterStore)
	if !ok {
		return errors.New("dead-letter storage is not supported")
	}
	values, err := next.ListDeadLetters(ctx, 100_000)
	if err != nil {
		return err
	}
	s.metrics.SetDLQDepth(len(values))
	return nil
}

func (s *ObservedStore) observeSQLite(operation SQLiteOperation, started time.Time, err error) {
	result := SQLiteSuccess
	if err != nil {
		result = SQLiteError
	}
	s.metrics.ObserveSQLiteTransaction(operation, result, time.Since(started))
}

func (s *ObservedStore) recordLeaseLatency(leases []domain.Lease, leasedAt time.Time) {
	for _, lease := range leases {
		if !lease.ReadyAt.IsZero() {
			s.metrics.ObserveJobLatency(JobReadyToLease, leasedAt.Sub(lease.ReadyAt))
		}
	}
}

func (s *ObservedStore) recordCompletionReceipt(
	ctx context.Context,
	receipt domain.CompletionReceipt,
) {
	if receipt.Duplicate {
		return
	}
	switch receipt.State {
	case domain.StateSucceeded:
		s.metrics.RecordCompletion(CompletionSucceeded)
	case domain.StateFailed:
		s.metrics.RecordCompletion(CompletionFailed)
	case domain.StateRetrying:
		s.metrics.RecordCompletion(CompletionRetrying)
		s.metrics.RecordRetry(RetryAttemptFailure)
	case domain.StateDeadLetter:
		s.metrics.RecordCompletion(CompletionDeadLetter)
		s.metrics.RecordDeadLetter(DeadLetterRetriesExhausted)
		_ = s.RefreshDLQDepth(ctx)
	}
}

func (s *ObservedStore) recordAdmission(duplicate bool, err error) {
	switch {
	case err != nil:
		s.recordRejection(err)
	case duplicate:
		s.metrics.RecordAdmission(AdmissionDuplicate)
	default:
		s.metrics.RecordAdmission(AdmissionAccepted)
	}
}

func (s *ObservedStore) recordRejection(err error) {
	switch {
	case errors.Is(err, domain.ErrQueueFull):
		s.metrics.RecordRejection(RejectionQueueFull)
	case errors.Is(err, domain.ErrCycleDetected):
		s.metrics.RecordRejection(RejectionCycleDetected)
	case errors.Is(err, domain.ErrIdempotencyConflict):
		s.metrics.RecordRejection(RejectionIdempotencyConflict)
	case errors.Is(err, domain.ErrStaleLease):
		s.metrics.RecordRejection(RejectionStaleLease)
	default:
		s.metrics.RecordRejection(RejectionInternal)
	}
}

var _ store.Store = (*ObservedStore)(nil)
var _ store.BatchAttemptStartStore = (*ObservedStore)(nil)
var _ store.BatchCompletionStore = (*ObservedStore)(nil)
var _ store.RecoveryAwareStore = (*ObservedStore)(nil)
var _ store.DeadLetterStore = (*ObservedStore)(nil)
