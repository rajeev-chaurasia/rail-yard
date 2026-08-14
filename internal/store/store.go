package store

import (
	"context"
	"errors"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/trigger"
)

var ErrWorkerCapacityConflict = errors.New("worker capacity conflicts with durable registration")

type Submission struct {
	Job            domain.JobSpec
	IdempotencyKey string
	RequestDigest  string
}

type WorkflowSubmission struct {
	Request        api.SubmitWorkflowRequest
	IdempotencyKey string
	RequestDigest  string
}

type Store interface {
	SubmitJob(context.Context, Submission, time.Time) (domain.Job, bool, error)
	SubmitWorkflow(context.Context, WorkflowSubmission, time.Time) ([]domain.Job, bool, error)
	GetJob(context.Context, string) (domain.Job, error)
	RegisterWorker(context.Context, string, int, time.Time) error
	HeartbeatWorker(context.Context, string, time.Time) error
	Acquire(context.Context, string, int, int, time.Time, time.Duration) ([]domain.Lease, error)
	MarkRunning(context.Context, string, domain.LeaseRef, time.Time) error
	Heartbeat(context.Context, string, []domain.LeaseRef, time.Time, time.Duration) ([]api.HeartbeatResult, error)
	Complete(context.Context, domain.Completion, time.Time) (domain.CompletionReceipt, error)
	PromoteDue(context.Context, time.Time, int) (int, error)
	ReapExpired(context.Context, time.Time, int) ([]domain.ReapedLease, error)
	Close() error
}

const MaxAttemptStartBatchSize = api.MaxAttemptStartBatchSize

type AttemptStartResult struct {
	Err error
}

type BatchAttemptStartStore interface {
	MarkRunningBatch(
		context.Context,
		string,
		[]domain.LeaseRef,
		time.Time,
	) ([]AttemptStartResult, error)
}

const MaxCompletionBatchSize = api.MaxCompletionBatchSize

type CompletionResult struct {
	Receipt domain.CompletionReceipt
	Err     error
}

type BatchCompletionStore interface {
	CompleteBatch(
		context.Context,
		[]domain.Completion,
		time.Time,
	) ([]CompletionResult, error)
}

type RecoveryAwareStore interface {
	AcquireWithRecoveryReserve(
		context.Context,
		string,
		int,
		int,
		time.Time,
		time.Duration,
		int,
	) ([]domain.Lease, error)
}

type CronSubmission struct {
	Trigger        domain.CronTrigger
	IdempotencyKey string
	RequestDigest  string
}

type TriggerStore interface {
	CreateCronTrigger(context.Context, CronSubmission, time.Time) (domain.CronTrigger, bool, error)
	FireDueCron(context.Context, time.Time, int) ([]string, error)
	trigger.RedisSink
}

type DeadLetterStore interface {
	ListDeadLetters(context.Context, int) ([]domain.DeadLetter, error)
	RedriveDeadLetter(
		context.Context,
		string,
		string,
		string,
		time.Time,
	) (domain.Job, bool, error)
}
