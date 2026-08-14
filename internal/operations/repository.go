package operations

import (
	"context"
	"errors"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

var ErrConflict = errors.New("operation conflicts with the current resource state")

type RequestError struct {
	Message string
}

func (e *RequestError) Error() string {
	return e.Message
}

type JobSubmitter interface {
	SubmitJob(context.Context, SubmitJobCommand) (api.SubmitJobResponse, error)
}

type DAGSubmitter interface {
	SubmitDAG(context.Context, SubmitDAGCommand) (SubmitDAGResponse, error)
}

type JobReader interface {
	GetJob(context.Context, string) (domain.Job, error)
}

type JobHistoryReader interface {
	GetJobHistory(context.Context, string, HistoryQuery) (JobHistoryPage, error)
}

type JobCanceller interface {
	CancelJob(context.Context, CancelJobCommand) (ActionReceipt, error)
}

type DeadLetterRedriver interface {
	RedriveDeadLetter(context.Context, RedriveCommand) (api.RedriveDeadLetterResponse, error)
}

type QueueDepthReader interface {
	ListTenantQueueDepth(context.Context, string) ([]QueueDepth, error)
}

type WorkerHealthReader interface {
	ListWorkerHealth(context.Context) ([]WorkerHealth, error)
}

type DAGReader interface {
	GetDAG(context.Context, string) (DAGDetail, error)
}

type ForceJobController interface {
	ForceJobAction(context.Context, ForceJobCommand) (ActionReceipt, error)
}

type Repositories struct {
	JobSubmitter       JobSubmitter
	DAGSubmitter       DAGSubmitter
	JobReader          JobReader
	JobHistoryReader   JobHistoryReader
	JobCanceller       JobCanceller
	DeadLetterRedriver DeadLetterRedriver
	QueueDepthReader   QueueDepthReader
	WorkerHealthReader WorkerHealthReader
	DAGReader          DAGReader
	ForceJobController ForceJobController
}
