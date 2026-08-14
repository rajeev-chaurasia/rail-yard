package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/dashboard"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

type ControlAction struct {
	TenantID       string
	IdempotencyKey string
	Action         string
	Actor          string
	Reason         string
	RequestDigest  string
	CommittedAt    time.Time
	TargetType     string
	TargetID       string
	TargetState    domain.JobState
	TargetVersion  int64
	Response       any
	Details        map[string]string
}

type AuditEvent = operations.AuditEvent

type JobControlCommand struct {
	JobID          string
	ReceiptAction  string
	AuditAction    string
	Actor          string
	Reason         string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
	NextState      domain.JobState
	Release        bool
}

type RedriveControlCommand struct {
	JobID          string
	Actor          string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type RetryControlCommand struct {
	JobID          string
	Actor          string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type DAGNode struct {
	JobID string
	Key   string
	Name  string
}

func (s *Store) SubmitJobOperation(
	ctx context.Context,
	command operations.SubmitJobCommand,
) (api.SubmitJobResponse, error) {
	var response api.SubmitJobResponse
	err := s.commitOperation(ctx, command.RequestedAt, "job submission", func(tx *sql.Tx, now time.Time) error {
		spec := command.Request.Job.Normalize()
		if err := validateIdempotency(command.IdempotencyKey, command.RequestDigest); err != nil {
			return err
		}
		stored, duplicate, err := readControlAction(
			ctx,
			tx,
			spec.TenantID,
			command.IdempotencyKey,
			"job.submit",
			command.RequestDigest,
		)
		if err != nil {
			return err
		}
		if duplicate {
			if err := json.Unmarshal([]byte(stored), &response); err != nil {
				return fmt.Errorf("decode job submission response: %w", err)
			}
			response.Duplicate = true
			return nil
		}
		if err := spec.Validate(s.maxSlotCost, s.allowShell); err != nil {
			return fmt.Errorf("validate job: %w", err)
		}
		dependencies, dependenciesSucceeded, dependencyFailed, err := validateExistingDependencies(
			ctx,
			tx,
			spec.TenantID,
			spec.DependsOn,
		)
		if err != nil {
			return err
		}
		if err := s.ensureAdmission(ctx, tx, spec.TenantID, 1); err != nil {
			return err
		}
		if err := ensureQueue(ctx, tx, spec.TenantID, spec.Queue, now); err != nil {
			return err
		}
		jobID, err := domain.NewID()
		if err != nil {
			return err
		}
		availableAt := spec.AvailableAt.UTC()
		if spec.AvailableAt.IsZero() {
			availableAt = now
		}
		readySeq := int64(0)
		if dependenciesSucceeded && !availableAt.After(now) {
			readySeq, err = nextReadySeq(ctx, tx)
			if err != nil {
				return err
			}
		}
		job := newJob(jobID, spec, availableAt, readySeq, now)
		if dependencyFailed {
			failure := domain.Failure{
				Class:   "upstream_failed",
				Message: "an upstream dependency did not succeed",
			}
			job.State = domain.StateDeadLetter
			job.TerminalAt = timePointer(now)
			job.Failure = &failure
			job.ReadySeq = 0
		}
		if err := insertJob(ctx, tx, job); err != nil {
			return err
		}
		for _, dependencyID := range dependencies {
			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (?, ?)",
				job.ID,
				dependencyID,
			); err != nil {
				return fmt.Errorf("insert job dependency: %w", err)
			}
		}
		if err := appendEvent(
			ctx,
			tx,
			job.ID,
			"job_admitted",
			job.State,
			job.StateVersion,
			now,
			map[string]any{"ready_seq": job.ReadySeq, "actor": command.Actor},
		); err != nil {
			return err
		}
		if dependencyFailed {
			encodedFailure, err := encodeOptionalFailure(job.Failure)
			if err != nil {
				return err
			}
			if err := insertCanonicalCompletion(
				ctx,
				tx,
				job.ID,
				job.State,
				job.StateVersion,
				0,
				"",
				encodedFailure,
				now,
			); err != nil {
				return err
			}
			if err := insertDeadLetter(ctx, tx, job.ID, encodedFailure, now); err != nil {
				return err
			}
		}
		response = api.SubmitJobResponse{Job: job}
		return insertControlAction(ctx, tx, ControlAction{
			TenantID:       job.TenantID,
			IdempotencyKey: command.IdempotencyKey,
			Action:         "job.submit",
			Actor:          command.Actor,
			RequestDigest:  command.RequestDigest,
			CommittedAt:    now,
			TargetType:     "job",
			TargetID:       job.ID,
			TargetState:    job.State,
			TargetVersion:  job.StateVersion,
			Response:       response,
			Details:        map[string]string{"tenant_id": job.TenantID, "queue": job.Queue},
		})
	})
	if err != nil {
		return api.SubmitJobResponse{}, err
	}
	return response, nil
}

func (s *Store) SubmitDAGOperation(
	ctx context.Context,
	dagID string,
	command operations.SubmitDAGCommand,
) (operations.SubmitDAGResponse, error) {
	var response operations.SubmitDAGResponse
	err := s.commitOperation(ctx, command.RequestedAt, "DAG submission", func(tx *sql.Tx, now time.Time) error {
		tenantID := command.Request.TenantID
		if tenantID == "" {
			tenantID = "default"
		}
		if err := validateIdempotency(command.IdempotencyKey, command.RequestDigest); err != nil {
			return err
		}
		stored, duplicate, err := readControlAction(
			ctx,
			tx,
			tenantID,
			command.IdempotencyKey,
			"dag.submit",
			command.RequestDigest,
		)
		if err != nil {
			return err
		}
		if duplicate {
			if err := json.Unmarshal([]byte(stored), &response); err != nil {
				return fmt.Errorf("decode DAG submission response: %w", err)
			}
			response.Duplicate = true
			return nil
		}
		specs, dependencyIndexes, err := validateWorkflow(
			storepkg.WorkflowSubmission{
				Request:        command.Request,
				IdempotencyKey: command.IdempotencyKey,
				RequestDigest:  command.RequestDigest,
			},
			tenantID,
			s.maxSlotCost,
			s.allowShell,
		)
		if err != nil {
			return err
		}
		if err := s.ensureAdmission(ctx, tx, tenantID, len(specs)); err != nil {
			return err
		}
		ids := make([]string, len(specs))
		for index := range ids {
			ids[index], err = domain.NewID()
			if err != nil {
				return err
			}
		}
		jobs := make([]domain.Job, len(specs))
		for index, spec := range specs {
			if err := ensureQueue(ctx, tx, tenantID, spec.Queue, now); err != nil {
				return err
			}
			availableAt := spec.AvailableAt.UTC()
			if spec.AvailableAt.IsZero() {
				availableAt = now
			}
			readySeq := int64(0)
			if len(dependencyIndexes[index]) == 0 && !availableAt.After(now) {
				readySeq, err = nextReadySeq(ctx, tx)
				if err != nil {
					return err
				}
			}
			jobs[index] = newJob(ids[index], spec, availableAt, readySeq, now)
			if err := insertJob(ctx, tx, jobs[index]); err != nil {
				return err
			}
		}
		for childIndex, parents := range dependencyIndexes {
			for _, parentIndex := range parents {
				if _, err := tx.ExecContext(
					ctx,
					"INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (?, ?)",
					ids[childIndex],
					ids[parentIndex],
				); err != nil {
					return fmt.Errorf("insert workflow dependency: %w", err)
				}
			}
		}
		for _, job := range jobs {
			if err := appendEvent(
				ctx,
				tx,
				job.ID,
				"job_admitted",
				job.State,
				job.StateVersion,
				now,
				map[string]any{"ready_seq": job.ReadySeq, "actor": command.Actor},
			); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO dag_runs
				(id, tenant_id, idempotency_key, request_digest, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			dagID,
			tenantID,
			scopedKey(tenantID, "dag.submit", command.IdempotencyKey),
			command.RequestDigest,
			timeToDB(now),
			timeToDB(now),
		); err != nil {
			return fmt.Errorf("insert DAG: %w", err)
		}
		for index, job := range jobs {
			node := command.Request.Nodes[index]
			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO dag_jobs (dag_id, job_id, node_key, name) VALUES (?, ?, ?, ?)",
				dagID,
				job.ID,
				node.Key,
				node.Job.Name,
			); err != nil {
				return fmt.Errorf("insert DAG node: %w", err)
			}
		}
		response = operations.SubmitDAGResponse{DAGID: dagID, Jobs: jobs}
		return insertControlAction(ctx, tx, ControlAction{
			TenantID:       tenantID,
			IdempotencyKey: command.IdempotencyKey,
			Action:         "dag.submit",
			Actor:          command.Actor,
			RequestDigest:  command.RequestDigest,
			CommittedAt:    now,
			TargetType:     "dag",
			TargetID:       dagID,
			Response:       response,
			Details:        map[string]string{"tenant_id": tenantID},
		})
	})
	if err != nil {
		return operations.SubmitDAGResponse{}, err
	}
	return response, nil
}

func (s *Store) commitOperation(
	ctx context.Context,
	requestedAt time.Time,
	name string,
	apply func(*sql.Tx, time.Time) error,
) error {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now := s.writeTime(requestedAt)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin %s: %w", name, err)
	}
	defer rollback(tx)
	if err := apply(tx, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}
	return nil
}

func (s *Store) RecordControlAction(ctx context.Context, action ControlAction) (bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	action.CommittedAt = s.writeTime(action.CommittedAt)
	if action.TenantID == "" {
		action.TenantID = "default"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin control audit: %w", err)
	}
	defer rollback(tx)
	_, duplicate, err := readControlAction(
		ctx,
		tx,
		action.TenantID,
		action.IdempotencyKey,
		action.Action,
		action.RequestDigest,
	)
	if err != nil {
		return false, err
	}
	if duplicate {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit duplicate control audit: %w", err)
		}
		return true, nil
	}
	if err := insertControlAction(ctx, tx, action); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit control audit: %w", err)
	}
	return false, nil
}

func (s *Store) RecordOperatorAction(
	ctx context.Context,
	action ControlAction,
) (AuditEvent, bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	action.CommittedAt = s.writeTime(action.CommittedAt)
	if action.TenantID == "" {
		action.TenantID = "default"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AuditEvent{}, false, fmt.Errorf("begin operator action: %w", err)
	}
	defer rollback(tx)
	response, duplicate, err := readControlAction(
		ctx,
		tx,
		action.TenantID,
		action.IdempotencyKey,
		action.Action,
		action.RequestDigest,
	)
	if err != nil {
		return AuditEvent{}, false, err
	}
	if duplicate {
		var envelope struct {
			Event AuditEvent `json:"event"`
		}
		if err := json.Unmarshal([]byte(response), &envelope); err != nil {
			return AuditEvent{}, false, fmt.Errorf("decode operator action response: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return AuditEvent{}, false, fmt.Errorf("commit duplicate operator action: %w", err)
		}
		return envelope.Event, true, nil
	}

	event := newAuditEvent(action)
	action.Response = struct {
		Event AuditEvent `json:"event"`
	}{Event: event}
	if err := insertControlActionWithEvent(ctx, tx, action, event); err != nil {
		return AuditEvent{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return AuditEvent{}, false, fmt.Errorf("commit operator action: %w", err)
	}
	return event, false, nil
}

func (s *Store) ApplyJobControl(
	ctx context.Context,
	command JobControlCommand,
) (operations.ActionReceipt, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now := s.writeTime(command.RequestedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return operations.ActionReceipt{}, fmt.Errorf("begin job control: %w", err)
	}
	defer rollback(tx)

	var state domain.JobState
	var version int64
	var attemptNo, slotCost int
	var tenantID, queue string
	err = tx.QueryRowContext(
		ctx,
		`SELECT state, state_version, attempt_no, slot_cost, tenant_id, queue_name
		 FROM jobs WHERE id = ?`,
		command.JobID,
	).Scan(&state, &version, &attemptNo, &slotCost, &tenantID, &queue)
	if errors.Is(err, sql.ErrNoRows) {
		return operations.ActionReceipt{}, domain.ErrNotFound
	}
	if err != nil {
		return operations.ActionReceipt{}, fmt.Errorf("read controlled job: %w", err)
	}
	response, duplicate, err := readControlAction(
		ctx,
		tx,
		tenantID,
		command.IdempotencyKey,
		command.AuditAction,
		command.RequestDigest,
	)
	if err != nil {
		return operations.ActionReceipt{}, err
	}
	if duplicate {
		var receipt operations.ActionReceipt
		if err := json.Unmarshal([]byte(response), &receipt); err != nil {
			return operations.ActionReceipt{}, fmt.Errorf("decode job control response: %w", err)
		}
		receipt.Duplicate = true
		if err := tx.Commit(); err != nil {
			return operations.ActionReceipt{}, fmt.Errorf("commit duplicate job control: %w", err)
		}
		return receipt, nil
	}
	if state.Terminal() {
		return operations.ActionReceipt{}, domain.ErrTerminalJob
	}

	active := state == domain.StateScheduled || state == domain.StateRunning
	if active {
		result, err := tx.ExecContext(
			ctx,
			`UPDATE attempts
			 SET state = 'EXPIRED', completed_at = ?,
			     failure_json = COALESCE(failure_json, ?)
			 WHERE job_id = ? AND attempt_no = ? AND state IN ('LEASED', 'RUNNING')`,
			timeToDB(now),
			failureJSON(command.ReceiptAction, command.Reason),
			command.JobID,
			attemptNo,
		)
		if err != nil {
			return operations.ActionReceipt{}, fmt.Errorf("fence controlled attempt: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return operations.ActionReceipt{}, fmt.Errorf("inspect controlled attempt fence: %w", err)
		}
		if affected != 1 {
			return operations.ActionReceipt{}, domain.ErrStaleLease
		}
		if err := releaseSlots(ctx, tx, tenantID, queue, slotCost, now); err != nil {
			return operations.ActionReceipt{}, err
		}
	}

	nextVersion := version + 1
	var readySeq int64
	var terminalAt any
	var encodedFailure any
	if command.Release {
		readySeq, err = nextReadySeq(ctx, tx)
		if err != nil {
			return operations.ActionReceipt{}, err
		}
	} else {
		terminalAt = timeToDB(now)
		encodedFailure = failureJSON(command.ReceiptAction, command.Reason)
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET state = ?, state_version = ?, available_at = ?, ready_seq = ?,
		     recovery_pending = ?, updated_at = ?, terminal_at = ?, failure_json = ?
		 WHERE id = ? AND state_version = ?
		   AND state NOT IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTER')`,
		command.NextState,
		nextVersion,
		timeToDB(now),
		readySeq,
		boolInt(command.Release && active),
		timeToDB(now),
		terminalAt,
		encodedFailure,
		command.JobID,
		version,
	)
	if err != nil {
		return operations.ActionReceipt{}, fmt.Errorf("update controlled job: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return operations.ActionReceipt{}, fmt.Errorf("inspect controlled job update: %w", err)
	}
	if affected != 1 {
		return operations.ActionReceipt{}, operations.ErrConflict
	}

	if !command.Release {
		if err := insertCanonicalCompletion(
			ctx,
			tx,
			command.JobID,
			command.NextState,
			nextVersion,
			attemptNo,
			"",
			encodedFailure,
			now,
		); err != nil {
			return operations.ActionReceipt{}, err
		}
		if command.NextState == domain.StateDeadLetter {
			if err := insertDeadLetter(ctx, tx, command.JobID, encodedFailure, now); err != nil {
				return operations.ActionReceipt{}, err
			}
		}
	}
	if err := appendEvent(
		ctx,
		tx,
		command.JobID,
		"operator_"+command.ReceiptAction,
		command.NextState,
		nextVersion,
		now,
		map[string]any{
			"actor":  command.Actor,
			"reason": command.Reason,
		},
	); err != nil {
		return operations.ActionReceipt{}, err
	}
	if !command.Release {
		if err := failDescendants(ctx, tx, command.JobID, now); err != nil {
			return operations.ActionReceipt{}, err
		}
	}

	receipt := operations.ActionReceipt{
		JobID:        command.JobID,
		Action:       command.ReceiptAction,
		State:        command.NextState,
		StateVersion: nextVersion,
		Actor:        command.Actor,
		CommittedAt:  now,
	}
	action := ControlAction{
		TenantID:       tenantID,
		IdempotencyKey: command.IdempotencyKey,
		Action:         command.AuditAction,
		Actor:          command.Actor,
		Reason:         command.Reason,
		RequestDigest:  command.RequestDigest,
		CommittedAt:    now,
		TargetType:     "job",
		TargetID:       command.JobID,
		TargetState:    command.NextState,
		TargetVersion:  nextVersion,
		Response:       receipt,
		Details: map[string]string{
			"reason": command.Reason,
			"state":  string(command.NextState),
		},
	}
	if err := insertControlAction(ctx, tx, action); err != nil {
		return operations.ActionReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return operations.ActionReceipt{}, fmt.Errorf("commit job control: %w", err)
	}
	return receipt, nil
}

func (s *Store) RedriveDeadLetterControl(
	ctx context.Context,
	command RedriveControlCommand,
) (api.RedriveDeadLetterResponse, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now := s.writeTime(command.RequestedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return api.RedriveDeadLetterResponse{}, fmt.Errorf("begin controlled redrive: %w", err)
	}
	defer rollback(tx)
	const actionName = "dead_letter.redrive"
	var redriven sql.NullString
	if err := tx.QueryRowContext(
		ctx,
		"SELECT redriven_job_id FROM dead_letters WHERE job_id = ?",
		command.JobID,
	).Scan(&redriven); errors.Is(err, sql.ErrNoRows) {
		return api.RedriveDeadLetterResponse{}, domain.ErrNotFound
	} else if err != nil {
		return api.RedriveDeadLetterResponse{}, fmt.Errorf("read controlled dead letter: %w", err)
	}
	original, err := scanJob(tx.QueryRowContext(
		ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = ?",
		command.JobID,
	))
	if err != nil {
		return api.RedriveDeadLetterResponse{}, fmt.Errorf("read redrive source: %w", err)
	}
	responseJSON, duplicate, err := readControlAction(
		ctx,
		tx,
		original.TenantID,
		command.IdempotencyKey,
		actionName,
		command.RequestDigest,
	)
	if err != nil {
		return api.RedriveDeadLetterResponse{}, err
	}
	if duplicate {
		var response api.RedriveDeadLetterResponse
		if err := json.Unmarshal([]byte(responseJSON), &response); err != nil {
			return api.RedriveDeadLetterResponse{}, fmt.Errorf("decode controlled redrive response: %w", err)
		}
		response.Duplicate = true
		if err := tx.Commit(); err != nil {
			return api.RedriveDeadLetterResponse{}, fmt.Errorf("commit duplicate controlled redrive: %w", err)
		}
		return response, nil
	}
	if redriven.Valid {
		return api.RedriveDeadLetterResponse{}, domain.ErrDeadLetterRedriven
	}
	created, err := cloneJob(ctx, tx, s, original, now)
	if err != nil {
		return api.RedriveDeadLetterResponse{}, err
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE dead_letters SET redriven_job_id = ?
		 WHERE job_id = ? AND redriven_job_id IS NULL`,
		created.ID,
		command.JobID,
	)
	if err != nil {
		return api.RedriveDeadLetterResponse{}, fmt.Errorf("link controlled redrive: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return api.RedriveDeadLetterResponse{}, fmt.Errorf("inspect controlled redrive link: %w", err)
	}
	if affected != 1 {
		return api.RedriveDeadLetterResponse{}, domain.ErrDeadLetterRedriven
	}
	if err := appendEvent(
		ctx,
		tx,
		command.JobID,
		"operator_redrive",
		domain.StateDeadLetter,
		original.StateVersion,
		now,
		map[string]any{"actor": command.Actor, "created_job_id": created.ID},
	); err != nil {
		return api.RedriveDeadLetterResponse{}, err
	}
	response := api.RedriveDeadLetterResponse{Job: created}
	if err := insertControlAction(ctx, tx, ControlAction{
		TenantID:       original.TenantID,
		IdempotencyKey: command.IdempotencyKey,
		Action:         actionName,
		Actor:          command.Actor,
		RequestDigest:  command.RequestDigest,
		CommittedAt:    now,
		TargetType:     "dead_letter",
		TargetID:       command.JobID,
		TargetState:    created.State,
		TargetVersion:  created.StateVersion,
		Response:       response,
		Details:        map[string]string{"created_job_id": created.ID},
	}); err != nil {
		return api.RedriveDeadLetterResponse{}, err
	}
	if err := tx.Commit(); err != nil {
		return api.RedriveDeadLetterResponse{}, fmt.Errorf("commit controlled redrive: %w", err)
	}
	return response, nil
}

func (s *Store) RetryJobControl(
	ctx context.Context,
	command RetryControlCommand,
) (domain.Job, bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now := s.writeTime(command.RequestedAt)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("begin controlled retry: %w", err)
	}
	defer rollback(tx)
	const actionName = "job.retry"
	original, err := scanJob(tx.QueryRowContext(
		ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = ?",
		command.JobID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, false, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("read retry source: %w", err)
	}
	responseJSON, duplicate, err := readControlAction(
		ctx,
		tx,
		original.TenantID,
		command.IdempotencyKey,
		actionName,
		command.RequestDigest,
	)
	if err != nil {
		return domain.Job{}, false, err
	}
	if duplicate {
		var job domain.Job
		if err := json.Unmarshal([]byte(responseJSON), &job); err != nil {
			return domain.Job{}, false, fmt.Errorf("decode controlled retry response: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.Job{}, false, fmt.Errorf("commit duplicate controlled retry: %w", err)
		}
		return job, true, nil
	}
	if !original.State.Terminal() {
		return domain.Job{}, false, operations.ErrConflict
	}
	created, err := cloneJob(ctx, tx, s, original, now)
	if err != nil {
		return domain.Job{}, false, err
	}
	if err := appendEvent(
		ctx,
		tx,
		command.JobID,
		"operator_retry",
		original.State,
		original.StateVersion,
		now,
		map[string]any{"actor": command.Actor, "created_job_id": created.ID},
	); err != nil {
		return domain.Job{}, false, err
	}
	if err := insertControlAction(ctx, tx, ControlAction{
		TenantID:       original.TenantID,
		IdempotencyKey: command.IdempotencyKey,
		Action:         actionName,
		Actor:          command.Actor,
		RequestDigest:  command.RequestDigest,
		CommittedAt:    now,
		TargetType:     "job",
		TargetID:       command.JobID,
		TargetState:    created.State,
		TargetVersion:  created.StateVersion,
		Response:       created,
		Details:        map[string]string{"created_job_id": created.ID},
	}); err != nil {
		return domain.Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Job{}, false, fmt.Errorf("commit controlled retry: %w", err)
	}
	return created, false, nil
}

func cloneJob(
	ctx context.Context,
	tx *sql.Tx,
	store *Store,
	original domain.Job,
	now time.Time,
) (domain.Job, error) {
	if err := store.ensureAdmission(ctx, tx, original.TenantID, 1); err != nil {
		return domain.Job{}, err
	}
	if err := ensureQueue(ctx, tx, original.TenantID, original.Queue, now); err != nil {
		return domain.Job{}, err
	}
	id, err := domain.NewID()
	if err != nil {
		return domain.Job{}, err
	}
	readySeq, err := nextReadySeq(ctx, tx)
	if err != nil {
		return domain.Job{}, err
	}
	job := newJob(id, domain.JobSpec{
		TenantID: original.TenantID,
		Queue:    original.Queue,
		Priority: original.Priority,
		SlotCost: original.SlotCost,
		Payload:  original.Payload,
		Retry:    original.Retry,
	}, now, readySeq, now)
	if err := insertJob(ctx, tx, job); err != nil {
		return domain.Job{}, err
	}
	if err := appendEvent(
		ctx,
		tx,
		job.ID,
		"job_admitted",
		job.State,
		job.StateVersion,
		now,
		map[string]any{"ready_seq": readySeq, "source_job_id": original.ID},
	); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func (s *Store) SaveDAG(
	ctx context.Context,
	id, tenantID, idempotencyKey, requestDigest string,
	nodes []DAGNode,
	now time.Time,
) error {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin DAG persistence: %w", err)
	}
	defer rollback(tx)
	var existingID, existingDigest string
	err = tx.QueryRowContext(
		ctx,
		"SELECT id, request_digest FROM dag_runs WHERE idempotency_key = ?",
		idempotencyKey,
	).Scan(&existingID, &existingDigest)
	if err == nil {
		if existingID != id || existingDigest != requestDigest {
			return domain.ErrIdempotencyConflict
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read persisted DAG: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO dag_runs
			(id, tenant_id, idempotency_key, request_digest, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id,
		tenantID,
		idempotencyKey,
		requestDigest,
		timeToDB(now),
		timeToDB(now),
	); err != nil {
		return fmt.Errorf("insert DAG: %w", err)
	}
	for _, node := range nodes {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO dag_jobs (dag_id, job_id, node_key, name)
			 VALUES (?, ?, ?, ?)`,
			id,
			node.JobID,
			node.Key,
			node.Name,
		); err != nil {
			return fmt.Errorf("insert DAG node: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DAG persistence: %w", err)
	}
	return nil
}

func (s *Store) JobHistory(
	ctx context.Context,
	jobID string,
	query operations.HistoryQuery,
) (operations.JobHistoryPage, error) {
	var exists int
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT 1 FROM jobs WHERE id = ?",
		jobID,
	).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return operations.JobHistoryPage{}, domain.ErrNotFound
	} else if err != nil {
		return operations.JobHistoryPage{}, fmt.Errorf("read history job: %w", err)
	}
	before := query.BeforeSeq
	if before == 0 {
		before = int64(^uint64(0) >> 1)
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT event_seq, event_type, state, state_version, occurred_at, payload_json
		 FROM events
		 WHERE job_id = ? AND event_seq < ?
		 ORDER BY event_seq DESC
		 LIMIT ?`,
		jobID,
		before,
		query.Limit+1,
	)
	if err != nil {
		return operations.JobHistoryPage{}, fmt.Errorf("query job history: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	events := make([]operations.JobEvent, 0, query.Limit+1)
	for rows.Next() {
		var event operations.JobEvent
		var occurredAt int64
		if err := rows.Scan(
			&event.Sequence,
			&event.Type,
			&event.State,
			&event.StateVersion,
			&occurredAt,
			&event.Payload,
		); err != nil {
			return operations.JobHistoryPage{}, fmt.Errorf("scan job history: %w", err)
		}
		event.OccurredAt = timeFromDB(occurredAt)
		event.Actor = "system"
		var payload struct {
			Actor string `json:"actor"`
		}
		if json.Unmarshal(event.Payload, &payload) == nil && payload.Actor != "" {
			event.Actor = payload.Actor
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return operations.JobHistoryPage{}, fmt.Errorf("iterate job history: %w", err)
	}
	page := operations.JobHistoryPage{Events: events}
	if len(events) > query.Limit {
		page.Events = events[:query.Limit]
		page.NextBeforeSeq = page.Events[len(page.Events)-1].Sequence
	}
	return page, nil
}

func (s *Store) TenantQueueDepth(
	ctx context.Context,
	tenantID string,
	now time.Time,
) ([]operations.QueueDepth, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
			q.tenant_id, q.queue_name,
			SUM(CASE WHEN j.state = 'PENDING' THEN 1 ELSE 0 END),
			SUM(CASE WHEN j.state = 'SCHEDULED' THEN 1 ELSE 0 END),
			SUM(CASE WHEN j.state = 'RUNNING' THEN 1 ELSE 0 END),
			SUM(CASE WHEN j.state = 'RETRYING' THEN 1 ELSE 0 END),
			SUM(CASE WHEN j.state NOT IN ('SUCCEEDED','FAILED','DEAD_LETTER') THEN 1 ELSE 0 END),
			t.max_depth, q.active_slots, t.max_slots,
			MIN(CASE WHEN j.state = 'PENDING' AND j.ready_seq > 0 THEN j.updated_at END)
		 FROM queue_state q
		 JOIN tenant_limits t ON t.tenant_id = q.tenant_id
		 LEFT JOIN jobs j
		   ON j.tenant_id = q.tenant_id AND j.queue_name = q.queue_name
		 WHERE q.tenant_id = ?
		 GROUP BY q.tenant_id, q.queue_name, t.max_depth, q.active_slots, t.max_slots
		 ORDER BY q.queue_name`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("query tenant queue depth: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	values := []operations.QueueDepth{}
	for rows.Next() {
		var value operations.QueueDepth
		var oldest sql.NullInt64
		if err := rows.Scan(
			&value.TenantID,
			&value.Queue,
			&value.Pending,
			&value.Scheduled,
			&value.Running,
			&value.Retrying,
			&value.Active,
			&value.MaxDepth,
			&value.ActiveSlots,
			&value.MaxSlots,
			&oldest,
		); err != nil {
			return nil, fmt.Errorf("scan tenant queue depth: %w", err)
		}
		if oldest.Valid {
			value.OldestReadyAgeMS = max(0, now.UTC().Sub(timeFromDB(oldest.Int64)).Milliseconds())
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant queue depth: %w", err)
	}
	return values, nil
}

func (s *Store) WorkerHealth(
	ctx context.Context,
	now time.Time,
) ([]operations.WorkerHealth, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
			w.worker_id,
			w.capacity_slots,
			w.last_heartbeat_at,
			COALESCE(SUM(CASE WHEN a.state IN ('LEASED','RUNNING')
				AND j.state IN ('SCHEDULED','RUNNING')
				AND j.attempt_no = a.attempt_no
				AND j.lease_generation = a.lease_generation THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN a.state IN ('LEASED','RUNNING')
				AND j.state IN ('SCHEDULED','RUNNING')
				AND j.attempt_no = a.attempt_no
				AND j.lease_generation = a.lease_generation THEN j.slot_cost ELSE 0 END), 0),
			MIN(CASE WHEN a.state IN ('LEASED','RUNNING')
				AND j.state IN ('SCHEDULED','RUNNING')
				AND j.attempt_no = a.attempt_no
				AND j.lease_generation = a.lease_generation THEN a.leased_at END),
			MIN(CASE WHEN a.state IN ('LEASED','RUNNING')
				AND j.state IN ('SCHEDULED','RUNNING')
				AND j.attempt_no = a.attempt_no
				AND j.lease_generation = a.lease_generation THEN a.expires_at END)
		 FROM workers w
		 LEFT JOIN attempts a ON a.worker_id = w.worker_id
		 LEFT JOIN jobs j ON j.id = a.job_id
		 GROUP BY w.worker_id, w.capacity_slots, w.last_heartbeat_at
		 ORDER BY w.worker_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("query worker health: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	values := []operations.WorkerHealth{}
	for rows.Next() {
		var value operations.WorkerHealth
		var heartbeat int64
		var oldestLease, oldestExpiry sql.NullInt64
		if err := rows.Scan(
			&value.WorkerID,
			&value.CapacitySlots,
			&heartbeat,
			&value.ActiveLeases,
			&value.ActiveSlots,
			&oldestLease,
			&oldestExpiry,
		); err != nil {
			return nil, fmt.Errorf("scan worker health: %w", err)
		}
		value.LastHeartbeatAt = timeFromDB(heartbeat)
		value.HeartbeatAgeMS = max(0, now.UTC().Sub(value.LastHeartbeatAt).Milliseconds())
		switch {
		case value.HeartbeatAgeMS <= int64((30 * time.Second).Milliseconds()):
			value.Status = operations.WorkerHealthy
		case value.HeartbeatAgeMS <= int64((2 * time.Minute).Milliseconds()):
			value.Status = operations.WorkerStale
		default:
			value.Status = operations.WorkerOffline
		}
		if oldestLease.Valid {
			value.OldestLeaseAgeMS = max(0, now.UTC().Sub(timeFromDB(oldestLease.Int64)).Milliseconds())
		}
		if oldestExpiry.Valid {
			expiry := timeFromDB(oldestExpiry.Int64)
			value.OldestLeaseExpiry = &expiry
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker health: %w", err)
	}
	return values, nil
}

func (s *Store) DAGDetail(ctx context.Context, dagID string) (operations.DAGDetail, error) {
	var detail operations.DAGDetail
	var createdAt, updatedAt int64
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, tenant_id, created_at, updated_at FROM dag_runs WHERE id = ?`,
		dagID,
	).Scan(&detail.ID, &detail.TenantID, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return operations.DAGDetail{}, domain.ErrNotFound
	}
	if err != nil {
		return operations.DAGDetail{}, fmt.Errorf("read DAG: %w", err)
	}
	detail.CreatedAt = timeFromDB(createdAt)
	detail.UpdatedAt = timeFromDB(updatedAt)
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT `+prefixedJobColumns("j")+`
		 FROM dag_jobs dj JOIN jobs j ON j.id = dj.job_id
		 WHERE dj.dag_id = ? ORDER BY dj.node_key`,
		dagID,
	)
	if err != nil {
		return operations.DAGDetail{}, fmt.Errorf("query DAG jobs: %w", err)
	}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			_ = rows.Close()
			return operations.DAGDetail{}, fmt.Errorf("scan DAG job: %w", err)
		}
		detail.Jobs = append(detail.Jobs, job)
	}
	if err := rows.Close(); err != nil {
		return operations.DAGDetail{}, fmt.Errorf("close DAG jobs: %w", err)
	}
	if err := rows.Err(); err != nil {
		return operations.DAGDetail{}, fmt.Errorf("iterate DAG jobs: %w", err)
	}
	edges, err := s.db.QueryContext(
		ctx,
		`SELECT d.depends_on_id, d.job_id
		 FROM job_dependencies d
		 JOIN dag_jobs child ON child.job_id = d.job_id AND child.dag_id = ?
		 JOIN dag_jobs parent ON parent.job_id = d.depends_on_id AND parent.dag_id = ?
		 ORDER BY d.depends_on_id, d.job_id`,
		dagID,
		dagID,
	)
	if err != nil {
		return operations.DAGDetail{}, fmt.Errorf("query DAG edges: %w", err)
	}
	defer func() {
		_ = edges.Close()
	}()
	for edges.Next() {
		var edge operations.DAGEdge
		if err := edges.Scan(&edge.FromJobID, &edge.ToJobID); err != nil {
			return operations.DAGDetail{}, fmt.Errorf("scan DAG edge: %w", err)
		}
		detail.Edges = append(detail.Edges, edge)
	}
	if err := edges.Err(); err != nil {
		return operations.DAGDetail{}, fmt.Errorf("iterate DAG edges: %w", err)
	}
	return detail, nil
}

func (s *Store) DashboardSnapshot(ctx context.Context, now time.Time) (dashboard.Snapshot, error) {
	snapshot := dashboard.Snapshot{
		GeneratedAt: now.UTC(),
		QueueDepths: []dashboard.QueueDepth{},
		RunningJobs: []dashboard.JobSummary{},
		FailedJobs:  []dashboard.JobSummary{},
		Workers:     []dashboard.WorkerSummary{},
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT tenant_id, queue_name, state, COUNT(*)
		 FROM jobs
		 WHERE state NOT IN ('SUCCEEDED','FAILED','DEAD_LETTER')
		 GROUP BY tenant_id, queue_name, state
		 ORDER BY tenant_id, queue_name, state`,
	)
	if err != nil {
		return dashboard.Snapshot{}, fmt.Errorf("query dashboard queue depths: %w", err)
	}
	for rows.Next() {
		var value dashboard.QueueDepth
		if err := rows.Scan(&value.TenantID, &value.Queue, &value.State, &value.Depth); err != nil {
			_ = rows.Close()
			return dashboard.Snapshot{}, fmt.Errorf("scan dashboard queue depth: %w", err)
		}
		snapshot.QueueDepths = append(snapshot.QueueDepths, value)
	}
	if err := rows.Close(); err != nil {
		return dashboard.Snapshot{}, fmt.Errorf("close dashboard queue depths: %w", err)
	}
	if err := rows.Err(); err != nil {
		return dashboard.Snapshot{}, fmt.Errorf("iterate dashboard queue depths: %w", err)
	}

	jobs, err := s.db.QueryContext(
		ctx,
		`SELECT j.id, COALESCE(dj.name, ''), j.tenant_id, j.queue_name,
		        j.state, j.attempt_no, j.updated_at, j.failure_json
		 FROM jobs j
		 LEFT JOIN dag_jobs dj ON dj.job_id = j.id
		 WHERE j.state IN ('SCHEDULED','RUNNING','FAILED','DEAD_LETTER')
		 ORDER BY j.updated_at DESC, j.id`,
	)
	if err != nil {
		return dashboard.Snapshot{}, fmt.Errorf("query dashboard jobs: %w", err)
	}
	for jobs.Next() {
		var value dashboard.JobSummary
		var updatedAt int64
		var encodedFailure sql.NullString
		if err := jobs.Scan(
			&value.ID,
			&value.Name,
			&value.TenantID,
			&value.Queue,
			&value.State,
			&value.AttemptNo,
			&updatedAt,
			&encodedFailure,
		); err != nil {
			_ = jobs.Close()
			return dashboard.Snapshot{}, fmt.Errorf("scan dashboard job: %w", err)
		}
		value.UpdatedAt = timeFromDB(updatedAt)
		if encodedFailure.Valid {
			var failure domain.Failure
			if err := json.Unmarshal([]byte(encodedFailure.String), &failure); err != nil {
				_ = jobs.Close()
				return dashboard.Snapshot{}, fmt.Errorf("decode dashboard failure: %w", err)
			}
			value.Failure = &failure
		}
		if value.State == domain.StateScheduled || value.State == domain.StateRunning {
			snapshot.RunningJobs = append(snapshot.RunningJobs, value)
		} else {
			snapshot.FailedJobs = append(snapshot.FailedJobs, value)
		}
	}
	if err := jobs.Close(); err != nil {
		return dashboard.Snapshot{}, fmt.Errorf("close dashboard jobs: %w", err)
	}
	if err := jobs.Err(); err != nil {
		return dashboard.Snapshot{}, fmt.Errorf("iterate dashboard jobs: %w", err)
	}
	workers, err := s.dashboardWorkers(ctx, now)
	if err != nil {
		return dashboard.Snapshot{}, err
	}
	snapshot.Workers = workers
	return snapshot, nil
}

func (s *Store) dashboardWorkers(
	ctx context.Context,
	now time.Time,
) ([]dashboard.WorkerSummary, error) {
	health, err := s.WorkerHealth(ctx, now)
	if err != nil {
		return nil, err
	}
	values := make([]dashboard.WorkerSummary, 0, len(health))
	for _, worker := range health {
		value := dashboard.WorkerSummary{
			ID:              worker.WorkerID,
			Healthy:         worker.Status == operations.WorkerHealthy,
			Capacity:        worker.CapacitySlots,
			UsedSlots:       worker.ActiveSlots,
			ActiveLeases:    worker.ActiveLeases,
			LastHeartbeatAt: worker.LastHeartbeatAt,
		}
		if worker.OldestLeaseAgeMS > 0 {
			value.OldestLeaseStartedAt = now.UTC().Add(-time.Duration(worker.OldestLeaseAgeMS) * time.Millisecond)
		}
		if worker.OldestLeaseExpiry != nil {
			value.NearestLeaseExpiry = *worker.OldestLeaseExpiry
		}
		values = append(values, value)
	}
	return values, nil
}

func (s *Store) DashboardRun(ctx context.Context, dagID string) (dashboard.Run, error) {
	var exists int
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT 1 FROM dag_runs WHERE id = ?",
		dagID,
	).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return dashboard.Run{}, domain.ErrNotFound
	} else if err != nil {
		return dashboard.Run{}, fmt.Errorf("read dashboard run: %w", err)
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT dj.job_id, dj.name, j.state, j.attempt_no, j.updated_at
		 FROM dag_jobs dj JOIN jobs j ON j.id = dj.job_id
		 WHERE dj.dag_id = ? ORDER BY dj.node_key`,
		dagID,
	)
	if err != nil {
		return dashboard.Run{}, fmt.Errorf("query dashboard run: %w", err)
	}
	run := dashboard.Run{ID: dagID, Nodes: []dashboard.RunNode{}}
	for rows.Next() {
		var node dashboard.RunNode
		var updatedAt int64
		if err := rows.Scan(
			&node.ID,
			&node.Name,
			&node.State,
			&node.AttemptNo,
			&updatedAt,
		); err != nil {
			return dashboard.Run{}, fmt.Errorf("scan dashboard run node: %w", err)
		}
		node.UpdatedAt = timeFromDB(updatedAt)
		run.Nodes = append(run.Nodes, node)
	}
	if err := rows.Close(); err != nil {
		return dashboard.Run{}, fmt.Errorf("close dashboard run: %w", err)
	}
	if err := rows.Err(); err != nil {
		return dashboard.Run{}, fmt.Errorf("iterate dashboard run: %w", err)
	}
	for index := range run.Nodes {
		node := &run.Nodes[index]
		dependencies, err := s.db.QueryContext(
			ctx,
			`SELECT depends_on_id FROM job_dependencies
			 WHERE job_id = ? ORDER BY depends_on_id`,
			node.ID,
		)
		if err != nil {
			return dashboard.Run{}, fmt.Errorf("query dashboard run dependencies: %w", err)
		}
		for dependencies.Next() {
			var dependency string
			if err := dependencies.Scan(&dependency); err != nil {
				_ = dependencies.Close()
				return dashboard.Run{}, fmt.Errorf("scan dashboard run dependency: %w", err)
			}
			node.DependsOn = append(node.DependsOn, dependency)
		}
		if err := dependencies.Close(); err != nil {
			return dashboard.Run{}, fmt.Errorf("close dashboard run dependencies: %w", err)
		}
		if err := dependencies.Err(); err != nil {
			return dashboard.Run{}, fmt.Errorf("iterate dashboard run dependencies: %w", err)
		}
		if node.DependsOn == nil {
			node.DependsOn = []string{}
		}
	}
	return run, nil
}

func (s *Store) ListAuditEvents(
	ctx context.Context,
	since time.Time,
	actor string,
) ([]AuditEvent, error) {
	query := `SELECT id, tenant_id, action, actor, occurred_at, target_type, target_id, details_json
		FROM audit_events WHERE occurred_at >= ?`
	args := []any{timeToDB(since)}
	if actor != "" {
		query += " AND actor = ?"
		args = append(args, actor)
	}
	query += " ORDER BY occurred_at, id"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	values := []AuditEvent{}
	for rows.Next() {
		var value AuditEvent
		var occurredAt int64
		var detailsJSON string
		if err := rows.Scan(
			&value.ID,
			&value.TenantID,
			&value.Action,
			&value.Actor,
			&occurredAt,
			&value.TargetType,
			&value.TargetID,
			&detailsJSON,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		value.OccurredAt = timeFromDB(occurredAt)
		if err := json.Unmarshal([]byte(detailsJSON), &value.Details); err != nil {
			return nil, fmt.Errorf("decode audit event details: %w", err)
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return values, nil
}

func readControlAction(
	ctx context.Context,
	tx *sql.Tx,
	tenantID, key, action, digest string,
) (string, bool, error) {
	var storedDigest, response string
	err := tx.QueryRowContext(
		ctx,
		`SELECT request_digest, response_json
		 FROM operation_requests
		 WHERE tenant_id = ? AND action = ? AND idempotency_key = ?`,
		tenantID,
		action,
		key,
	).Scan(&storedDigest, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read control idempotency: %w", err)
	}
	if storedDigest != digest {
		return "", false, domain.ErrIdempotencyConflict
	}
	return response, true, nil
}

func insertControlAction(ctx context.Context, tx *sql.Tx, action ControlAction) error {
	return insertControlActionWithEvent(ctx, tx, action, newAuditEvent(action))
}

func insertControlActionWithEvent(
	ctx context.Context,
	tx *sql.Tx,
	action ControlAction,
	event AuditEvent,
) error {
	responseJSON, err := json.Marshal(action.Response)
	if err != nil {
		return fmt.Errorf("encode control response: %w", err)
	}
	detailsJSON, err := json.Marshal(event.Details)
	if err != nil {
		return fmt.Errorf("encode control audit details: %w", err)
	}
	var targetState any
	if action.TargetState != "" {
		targetState = action.TargetState
	}
	var targetVersion any
	if action.TargetVersion > 0 {
		targetVersion = action.TargetVersion
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO operation_requests (
			tenant_id, idempotency_key, action, actor, reason, request_digest, committed_at,
			target_type, target_id, target_state, target_version, response_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		action.TenantID,
		action.IdempotencyKey,
		action.Action,
		action.Actor,
		action.Reason,
		action.RequestDigest,
		timeToDB(action.CommittedAt),
		action.TargetType,
		action.TargetID,
		targetState,
		targetVersion,
		string(responseJSON),
	); err != nil {
		return fmt.Errorf("insert control action: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO audit_events (
			id, tenant_id, idempotency_key, action, actor, occurred_at,
			target_type, target_id, details_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID,
		action.TenantID,
		action.IdempotencyKey,
		event.Action,
		event.Actor,
		timeToDB(event.OccurredAt),
		event.TargetType,
		event.TargetID,
		string(detailsJSON),
	); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func newAuditEvent(action ControlAction) AuditEvent {
	sum := sha256.Sum256([]byte(
		action.TenantID + "\x00" + action.Action + "\x00" + action.IdempotencyKey,
	))
	return AuditEvent{
		ID:         "audit-" + hex.EncodeToString(sum[:12]),
		TenantID:   action.TenantID,
		Action:     action.Action,
		Actor:      action.Actor,
		OccurredAt: action.CommittedAt.UTC(),
		TargetType: action.TargetType,
		TargetID:   action.TargetID,
		Details:    action.Details,
	}
}

func scopedKey(tenantID, action, key string) string {
	sum := sha256.Sum256([]byte(tenantID + "\x00" + action + "\x00" + key))
	return hex.EncodeToString(sum[:])
}

func failureJSON(action, reason string) string {
	failure := domain.Failure{
		Class:   "operator_" + action,
		Message: reason,
	}
	body, _ := json.Marshal(failure)
	return string(body)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func prefixedJobColumns(alias string) string {
	return alias + ".id," +
		alias + ".tenant_id," +
		alias + ".queue_name," +
		alias + ".priority," +
		alias + ".slot_cost," +
		alias + ".payload_json," +
		alias + ".retry_json," +
		alias + ".state," +
		alias + ".attempt_no," +
		alias + ".state_version," +
		alias + ".lease_generation," +
		alias + ".available_at," +
		alias + ".ready_seq," +
		alias + ".created_at," +
		alias + ".updated_at," +
		alias + ".terminal_at," +
		alias + ".failure_json"
}
