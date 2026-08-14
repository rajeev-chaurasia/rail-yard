package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/dashboard"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
	"github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
)

type Adapter struct {
	store *sqlite.Store
	now   func() time.Time
}

func New(store *sqlite.Store) *Adapter {
	return &Adapter{store: store, now: time.Now}
}

func (a *Adapter) Repositories() operations.Repositories {
	return operations.Repositories{
		JobSubmitter:       a,
		DAGSubmitter:       a,
		JobReader:          a,
		JobHistoryReader:   a,
		JobCanceller:       a,
		DeadLetterRedriver: a,
		QueueDepthReader:   a,
		WorkerHealthReader: a,
		DAGReader:          a,
		ForceJobController: a,
	}
}

func (a *Adapter) SubmitJob(
	ctx context.Context,
	command operations.SubmitJobCommand,
) (api.SubmitJobResponse, error) {
	job, duplicate, err := a.store.SubmitJob(ctx, storepkg.Submission{
		Job:            command.Request.Job,
		IdempotencyKey: command.IdempotencyKey,
		RequestDigest:  command.RequestDigest,
	}, command.RequestedAt)
	if err != nil {
		return api.SubmitJobResponse{}, err
	}
	response := api.SubmitJobResponse{Job: job, Duplicate: duplicate}
	_, err = a.store.RecordControlAction(ctx, sqlite.ControlAction{
		IdempotencyKey: command.IdempotencyKey,
		Action:         "job.submit",
		Actor:          requestActor(ctx),
		RequestDigest:  command.RequestDigest,
		CommittedAt:    command.RequestedAt,
		TargetType:     "job",
		TargetID:       job.ID,
		TargetState:    job.State,
		TargetVersion:  job.StateVersion,
		Response:       response,
		Details:        map[string]string{"tenant_id": job.TenantID, "queue": job.Queue},
	})
	if err != nil {
		return api.SubmitJobResponse{}, err
	}
	return response, nil
}

func (a *Adapter) SubmitDAG(
	ctx context.Context,
	command operations.SubmitDAGCommand,
) (operations.SubmitDAGResponse, error) {
	jobs, duplicate, err := a.store.SubmitWorkflow(ctx, storepkg.WorkflowSubmission{
		Request:        command.Request,
		IdempotencyKey: command.IdempotencyKey,
		RequestDigest:  command.RequestDigest,
	}, command.RequestedAt)
	if err != nil {
		return operations.SubmitDAGResponse{}, err
	}
	dagID := deterministicDAGID(command.Request.TenantID, command.IdempotencyKey)
	nodes := make([]sqlite.DAGNode, len(jobs))
	for index, job := range jobs {
		nodes[index] = sqlite.DAGNode{
			JobID: job.ID,
			Key:   command.Request.Nodes[index].Key,
			Name:  command.Request.Nodes[index].Job.Name,
		}
	}
	if err := a.store.SaveDAG(
		ctx,
		dagID,
		command.Request.TenantID,
		command.IdempotencyKey,
		command.RequestDigest,
		nodes,
		command.RequestedAt,
	); err != nil {
		return operations.SubmitDAGResponse{}, err
	}
	response := operations.SubmitDAGResponse{
		DAGID:     dagID,
		Jobs:      jobs,
		Duplicate: duplicate,
	}
	_, err = a.store.RecordControlAction(ctx, sqlite.ControlAction{
		IdempotencyKey: command.IdempotencyKey,
		Action:         "dag.submit",
		Actor:          requestActor(ctx),
		RequestDigest:  command.RequestDigest,
		CommittedAt:    command.RequestedAt,
		TargetType:     "dag",
		TargetID:       dagID,
		Response:       response,
		Details:        map[string]string{"tenant_id": command.Request.TenantID},
	})
	if err != nil {
		return operations.SubmitDAGResponse{}, err
	}
	return response, nil
}

func (a *Adapter) GetJob(ctx context.Context, jobID string) (domain.Job, error) {
	return a.store.GetJob(ctx, jobID)
}

func (a *Adapter) GetJobHistory(
	ctx context.Context,
	jobID string,
	query operations.HistoryQuery,
) (operations.JobHistoryPage, error) {
	return a.store.JobHistory(ctx, jobID, query)
}

func (a *Adapter) CancelJob(
	ctx context.Context,
	command operations.CancelJobCommand,
) (operations.ActionReceipt, error) {
	return a.store.ApplyJobControl(ctx, sqlite.JobControlCommand{
		JobID:          command.JobID,
		ReceiptAction:  "cancel",
		AuditAction:    "job.cancel",
		Actor:          command.Actor,
		Reason:         command.Reason,
		IdempotencyKey: command.IdempotencyKey,
		RequestDigest:  command.RequestDigest,
		RequestedAt:    command.RequestedAt,
		NextState:      domain.StateFailed,
	})
}

func (a *Adapter) RedriveDeadLetter(
	ctx context.Context,
	command operations.RedriveCommand,
) (api.RedriveDeadLetterResponse, error) {
	return a.store.RedriveDeadLetterControl(ctx, sqlite.RedriveControlCommand{
		JobID:          command.JobID,
		Actor:          command.Actor,
		IdempotencyKey: command.IdempotencyKey,
		RequestDigest:  command.RequestDigest,
		RequestedAt:    command.RequestedAt,
	})
}

func (a *Adapter) ListTenantQueueDepth(
	ctx context.Context,
	tenantID string,
) ([]operations.QueueDepth, error) {
	return a.store.TenantQueueDepth(ctx, tenantID, a.now().UTC())
}

func (a *Adapter) ListWorkerHealth(ctx context.Context) ([]operations.WorkerHealth, error) {
	return a.store.WorkerHealth(ctx, a.now().UTC())
}

func (a *Adapter) GetDAG(ctx context.Context, dagID string) (operations.DAGDetail, error) {
	return a.store.DAGDetail(ctx, dagID)
}

func (a *Adapter) ForceJobAction(
	ctx context.Context,
	command operations.ForceJobCommand,
) (operations.ActionReceipt, error) {
	state := domain.StatePending
	release := command.Action == operations.ForceRelease
	switch command.Action {
	case operations.ForceFail:
		state = domain.StateFailed
	case operations.ForceDeadLetter:
		state = domain.StateDeadLetter
	}
	return a.store.ApplyJobControl(ctx, sqlite.JobControlCommand{
		JobID:          command.JobID,
		ReceiptAction:  string(command.Action),
		AuditAction:    "job.force." + string(command.Action),
		Actor:          command.Actor,
		Reason:         command.Reason,
		IdempotencyKey: command.IdempotencyKey,
		RequestDigest:  command.RequestDigest,
		RequestedAt:    command.RequestedAt,
		NextState:      state,
		Release:        release,
	})
}

func (a *Adapter) Snapshot(ctx context.Context) (dashboard.Snapshot, error) {
	value, err := a.store.DashboardSnapshot(ctx, a.now().UTC())
	return value, dashboardError(err)
}

func (a *Adapter) DeadLetters(
	ctx context.Context,
	limit int,
) ([]domain.DeadLetter, error) {
	values, err := a.store.ListDeadLetters(ctx, limit)
	return values, dashboardError(err)
}

func (a *Adapter) Run(ctx context.Context, runID string) (dashboard.Run, error) {
	value, err := a.store.DashboardRun(ctx, runID)
	return value, dashboardError(err)
}

func (a *Adapter) Operate(
	ctx context.Context,
	operation dashboard.Operation,
) (dashboard.OperationResult, error) {
	digest, err := digest(operation)
	if err != nil {
		return dashboard.OperationResult{}, dashboardError(err)
	}
	now := a.now().UTC()
	switch operation.Action {
	case dashboard.ActionCancel:
		_, err = a.CancelJob(ctx, operations.CancelJobCommand{
			JobID:          operation.JobID,
			Actor:          operation.Actor,
			Reason:         operation.Reason,
			IdempotencyKey: operation.RequestID,
			RequestDigest:  digest,
			RequestedAt:    now,
		})
	case dashboard.ActionForce:
		_, err = a.ForceJobAction(ctx, operations.ForceJobCommand{
			JobID:          operation.JobID,
			Action:         operations.ForceAction(operation.ForceAction),
			Actor:          operation.Actor,
			Reason:         operation.Reason,
			IdempotencyKey: operation.RequestID,
			RequestDigest:  digest,
			RequestedAt:    now,
		})
	case dashboard.ActionRedrive:
		var response api.RedriveDeadLetterResponse
		response, err = a.RedriveDeadLetter(ctx, operations.RedriveCommand{
			JobID:          operation.JobID,
			Actor:          operation.Actor,
			IdempotencyKey: operation.RequestID,
			RequestDigest:  digest,
			RequestedAt:    now,
		})
		if err == nil {
			return dashboard.OperationResult{
				Action:       operation.Action,
				JobID:        operation.JobID,
				CreatedJobID: response.Job.ID,
				Message:      "dead letter redriven",
			}, nil
		}
	case dashboard.ActionRetry:
		var created domain.Job
		created, _, err = a.store.RetryJobControl(ctx, sqlite.RetryControlCommand{
			JobID:          operation.JobID,
			Actor:          operation.Actor,
			IdempotencyKey: operation.RequestID,
			RequestDigest:  digest,
			RequestedAt:    now,
		})
		if err == nil {
			return dashboard.OperationResult{
				Action:       operation.Action,
				JobID:        operation.JobID,
				CreatedJobID: created.ID,
				Message:      "job retry created",
			}, nil
		}
	default:
		err = fmt.Errorf("unsupported dashboard action %q", operation.Action)
	}
	if err != nil {
		return dashboard.OperationResult{}, dashboardError(err)
	}
	return dashboard.OperationResult{
		Action:  operation.Action,
		JobID:   operation.JobID,
		Message: string(operation.Action) + " accepted",
	}, nil
}

func deterministicDAGID(tenantID, key string) string {
	sum := sha256.Sum256([]byte(tenantID + "\x00" + key))
	return "dag-" + hex.EncodeToString(sum[:12])
}

func digest(value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode control digest: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func dashboardError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return &dashboard.ClientError{Status: http.StatusNotFound, Code: "not_found", Message: "resource not found"}
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return &dashboard.ClientError{Status: http.StatusConflict, Code: "idempotency_conflict", Message: "request conflicts with an existing operation"}
	case errors.Is(err, domain.ErrTerminalJob), errors.Is(err, operations.ErrConflict):
		return &dashboard.ClientError{Status: http.StatusConflict, Code: "state_conflict", Message: "operation conflicts with the current job state"}
	default:
		return err
	}
}

var (
	_ operations.JobSubmitter       = (*Adapter)(nil)
	_ operations.DAGSubmitter       = (*Adapter)(nil)
	_ operations.JobReader          = (*Adapter)(nil)
	_ operations.JobHistoryReader   = (*Adapter)(nil)
	_ operations.JobCanceller       = (*Adapter)(nil)
	_ operations.DeadLetterRedriver = (*Adapter)(nil)
	_ operations.QueueDepthReader   = (*Adapter)(nil)
	_ operations.WorkerHealthReader = (*Adapter)(nil)
	_ operations.DAGReader          = (*Adapter)(nil)
	_ operations.ForceJobController = (*Adapter)(nil)
	_ dashboard.Client              = (*Adapter)(nil)
)
