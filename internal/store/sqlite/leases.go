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
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/retry"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

type leaseCandidate struct {
	jobID        string
	tenantID     string
	queue        string
	priority     int
	slotCost     int
	payload      domain.Payload
	attemptNo    int
	stateVersion int64
	generation   int64
	executionKey string
	readyAt      time.Time
}

func (s *Store) Acquire(
	ctx context.Context,
	workerID string,
	availableSlots int,
	limit int,
	now time.Time,
	leaseTTL time.Duration,
) ([]domain.Lease, error) {
	return s.acquire(ctx, workerID, availableSlots, limit, now, leaseTTL, 0)
}

func (s *Store) AcquireWithRecoveryReserve(
	ctx context.Context,
	workerID string,
	availableSlots int,
	limit int,
	now time.Time,
	leaseTTL time.Duration,
	recoveryReserve int,
) ([]domain.Lease, error) {
	return s.acquire(
		ctx,
		workerID,
		availableSlots,
		limit,
		now,
		leaseTTL,
		recoveryReserve,
	)
}

func (s *Store) acquire(
	ctx context.Context,
	workerID string,
	availableSlots int,
	limit int,
	now time.Time,
	leaseTTL time.Duration,
	recoveryReserve int,
) ([]domain.Lease, error) {
	if workerID == "" {
		return nil, errors.New("worker ID must not be empty")
	}
	if availableSlots < 1 || limit < 1 {
		return []domain.Lease{}, nil
	}
	if leaseTTL <= 0 {
		return nil, errors.New("lease TTL must be positive")
	}
	if recoveryReserve < 0 {
		return nil, errors.New("recovery reserve must not be negative")
	}
	releaseWrite := s.beginWrite(writeDispatch)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin lease acquisition: %w", err)
	}
	defer rollback(tx)

	plan, err := buildSchedulePlan(
		ctx,
		tx,
		now,
		availableSlots,
		limit,
		recoveryReserve,
	)
	if err != nil {
		return nil, err
	}
	if len(plan.candidates) == 0 {
		return []domain.Lease{}, nil
	}

	leases := make([]domain.Lease, 0, len(plan.decision.Grants))
	for _, grant := range plan.decision.Grants {
		candidate, exists := plan.candidates[grant.JobID]
		if !exists {
			return nil, fmt.Errorf("scheduler selected unknown job %q", grant.JobID)
		}

		reserved, err := reserveSlots(
			ctx,
			tx,
			candidate.tenantID,
			candidate.queue,
			candidate.slotCost,
			now,
		)
		if err != nil {
			return nil, err
		}
		if !reserved {
			return nil, fmt.Errorf("scheduler capacity changed for job %q", candidate.jobID)
		}

		token, err := domain.NewID()
		if err != nil {
			return nil, fmt.Errorf("generate lease token: %w", err)
		}
		attemptNo := candidate.attemptNo + 1
		generation := candidate.generation + 1
		stateVersion := candidate.stateVersion + 1
		expiresAt := now.Add(leaseTTL).UTC()

		result, err := tx.ExecContext(
			ctx,
			`UPDATE jobs
			 SET state = 'SCHEDULED',
			     attempt_no = ?,
			     lease_generation = ?,
			     state_version = ?,
			     ready_seq = 0,
			     recovery_pending = 0,
			     updated_at = ?
			 WHERE id = ?
			   AND state = 'PENDING'
			   AND ready_seq > 0
			   AND state_version = ?`,
			attemptNo,
			generation,
			stateVersion,
			timeToDB(now),
			candidate.jobID,
			candidate.stateVersion,
		)
		if err != nil {
			return nil, fmt.Errorf("schedule job %q: %w", candidate.jobID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("inspect scheduled job %q: %w", candidate.jobID, err)
		}
		if affected != 1 {
			return nil, fmt.Errorf("lease candidate %q changed during acquisition", candidate.jobID)
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO attempts (
				job_id, attempt_no, worker_id, lease_generation, token_hash,
				state, leased_at, heartbeat_at, expires_at
			) VALUES (?, ?, ?, ?, ?, 'LEASED', ?, ?, ?)`,
			candidate.jobID,
			attemptNo,
			workerID,
			generation,
			domain.TokenHash(token),
			timeToDB(now),
			timeToDB(now),
			timeToDB(expiresAt),
		); err != nil {
			return nil, fmt.Errorf("insert attempt for job %q: %w", candidate.jobID, err)
		}
		if err := appendEvent(
			ctx,
			tx,
			candidate.jobID,
			"lease_acquired",
			domain.StateScheduled,
			stateVersion,
			now,
			struct {
				AttemptNo  int    `json:"attempt_no"`
				Generation int64  `json:"generation"`
				WorkerID   string `json:"worker_id"`
				ExpiresAt  int64  `json:"expires_at"`
			}{
				AttemptNo:  attemptNo,
				Generation: generation,
				WorkerID:   workerID,
				ExpiresAt:  timeToDB(expiresAt),
			},
		); err != nil {
			return nil, err
		}

		leases = append(leases, domain.Lease{
			JobID:          candidate.jobID,
			AttemptNo:      attemptNo,
			Generation:     generation,
			Token:          token,
			WorkerID:       workerID,
			ExpiresAt:      expiresAt,
			ReadyAt:        candidate.readyAt,
			IdempotencyKey: candidate.executionKey,
			SlotCost:       candidate.slotCost,
			Payload:        candidate.payload,
		})
	}
	if err := persistSchedulePlan(ctx, tx, plan, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lease acquisition: %w", err)
	}
	return leases, nil
}

func (s *Store) MarkRunning(
	ctx context.Context,
	workerID string,
	ref domain.LeaseRef,
	now time.Time,
) error {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin attempt start: %w", err)
	}
	defer rollback(tx)

	if err := markRunningTx(ctx, tx, workerID, ref, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit attempt start: %w", err)
	}
	s.attemptStartCommits.Add(1)
	return nil
}

func (s *Store) MarkRunningBatch(
	ctx context.Context,
	workerID string,
	refs []domain.LeaseRef,
	now time.Time,
) ([]store.AttemptStartResult, error) {
	if len(refs) == 0 || len(refs) > store.MaxAttemptStartBatchSize {
		return nil, fmt.Errorf(
			"attempt start batch must contain between 1 and %d items",
			store.MaxAttemptStartBatchSize,
		)
	}

	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin attempt start batch: %w", err)
	}
	defer rollback(tx)

	results := make([]store.AttemptStartResult, len(refs))
	for index, ref := range refs {
		startErr := markRunningTx(ctx, tx, workerID, ref, now)
		if errors.Is(startErr, domain.ErrStaleLease) {
			results[index].Err = startErr
			continue
		}
		if startErr != nil {
			return nil, fmt.Errorf("start batch item %d: %w", index, startErr)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit attempt start batch: %w", err)
	}
	s.attemptStartCommits.Add(1)
	return results, nil
}

func markRunningTx(
	ctx context.Context,
	tx *sql.Tx,
	workerID string,
	ref domain.LeaseRef,
	now time.Time,
) error {
	var attemptState, jobState string
	var expiresAt, stateVersion int64
	err := tx.QueryRowContext(
		ctx,
		`SELECT a.state, a.expires_at, j.state, j.state_version
		 FROM attempts a
		 JOIN jobs j ON j.id = a.job_id
		 WHERE a.job_id = ?
		   AND a.attempt_no = ?
		   AND a.worker_id = ?
		   AND a.lease_generation = ?
		   AND a.token_hash = ?
		   AND j.attempt_no = a.attempt_no
		   AND j.lease_generation = a.lease_generation`,
		ref.JobID,
		ref.AttemptNo,
		workerID,
		ref.Generation,
		domain.TokenHash(ref.Token),
	).Scan(&attemptState, &expiresAt, &jobState, &stateVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrStaleLease
	}
	if err != nil {
		return fmt.Errorf("read attempt start fence: %w", err)
	}
	if expiresAt <= timeToDB(now) {
		return domain.ErrStaleLease
	}
	if attemptState == "RUNNING" && jobState == string(domain.StateRunning) {
		return nil
	}
	if attemptState != "LEASED" || jobState != string(domain.StateScheduled) {
		return domain.ErrStaleLease
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE attempts
		 SET state = 'RUNNING', started_at = ?
		 WHERE job_id = ? AND attempt_no = ?`,
		timeToDB(now),
		ref.JobID,
		ref.AttemptNo,
	); err != nil {
		return fmt.Errorf("start attempt: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET state = 'RUNNING', state_version = state_version + 1, updated_at = ?
		 WHERE id = ? AND state_version = ?`,
		timeToDB(now),
		ref.JobID,
		stateVersion,
	); err != nil {
		return fmt.Errorf("mark job running: %w", err)
	}
	if err := appendEvent(
		ctx,
		tx,
		ref.JobID,
		"attempt_started",
		domain.StateRunning,
		stateVersion+1,
		now,
		struct {
			AttemptNo  int   `json:"attempt_no"`
			Generation int64 `json:"generation"`
		}{AttemptNo: ref.AttemptNo, Generation: ref.Generation},
	); err != nil {
		return err
	}
	return nil
}

func (s *Store) Heartbeat(
	ctx context.Context,
	workerID string,
	refs []domain.LeaseRef,
	now time.Time,
	leaseTTL time.Duration,
) ([]api.HeartbeatResult, error) {
	if leaseTTL <= 0 {
		return nil, errors.New("lease TTL must be positive")
	}
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin heartbeat: %w", err)
	}
	defer rollback(tx)

	results := make([]api.HeartbeatResult, len(refs))
	requestedExpiry := timeToDB(now.Add(leaseTTL))
	for index, ref := range refs {
		results[index].JobID = ref.JobID
		result, err := tx.ExecContext(
			ctx,
			`UPDATE attempts
			 SET heartbeat_at = ?,
			     expires_at = MAX(expires_at, ?)
			 WHERE job_id = ?
			   AND attempt_no = ?
			   AND worker_id = ?
			   AND lease_generation = ?
			   AND token_hash = ?
			   AND expires_at > ?
			   AND state IN ('LEASED', 'RUNNING')
			   AND EXISTS (
			       SELECT 1 FROM jobs j
			       WHERE j.id = attempts.job_id
			         AND j.attempt_no = attempts.attempt_no
			         AND j.lease_generation = attempts.lease_generation
			         AND j.state IN ('SCHEDULED', 'RUNNING')
			   )`,
			timeToDB(now),
			requestedExpiry,
			ref.JobID,
			ref.AttemptNo,
			workerID,
			ref.Generation,
			domain.TokenHash(ref.Token),
			timeToDB(now),
		)
		if err != nil {
			return nil, fmt.Errorf("heartbeat job %q: %w", ref.JobID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("inspect heartbeat job %q: %w", ref.JobID, err)
		}
		if affected != 1 {
			continue
		}

		var expiresAt int64
		if err := tx.QueryRowContext(
			ctx,
			`SELECT expires_at FROM attempts WHERE job_id = ? AND attempt_no = ?`,
			ref.JobID,
			ref.AttemptNo,
		).Scan(&expiresAt); err != nil {
			return nil, fmt.Errorf("read heartbeat expiry for job %q: %w", ref.JobID, err)
		}
		results[index].Accepted = true
		results[index].ExpiresAt = timeFromDB(expiresAt)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit heartbeat: %w", err)
	}
	return results, nil
}

type completionRecord struct {
	workerID          string
	generation        int64
	tokenHash         string
	attemptState      string
	expiresAt         int64
	completionDigest  sql.NullString
	receiptState      sql.NullString
	receiptVersion    sql.NullInt64
	receiptCommitted  sql.NullInt64
	jobState          string
	jobVersion        int64
	currentAttempt    int
	currentGeneration int64
	slotCost          int
	tenantID          string
	queue             string
	retry             domain.RetryPolicy
}

func (s *Store) Complete(
	ctx context.Context,
	completion domain.Completion,
	now time.Time,
) (domain.CompletionReceipt, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.CompletionReceipt{}, fmt.Errorf("begin completion: %w", err)
	}
	defer rollback(tx)

	receipt, err := applyCompletionTx(ctx, tx, completion, now)
	if err != nil {
		return domain.CompletionReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.CompletionReceipt{}, fmt.Errorf("commit completion: %w", err)
	}
	s.completionCommits.Add(1)
	return receipt, nil
}

func (s *Store) CompleteBatch(
	ctx context.Context,
	completions []domain.Completion,
	now time.Time,
) ([]store.CompletionResult, error) {
	if len(completions) == 0 || len(completions) > store.MaxCompletionBatchSize {
		return nil, fmt.Errorf(
			"completion batch must contain between 1 and %d items",
			store.MaxCompletionBatchSize,
		)
	}

	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin completion batch: %w", err)
	}
	defer rollback(tx)

	results := make([]store.CompletionResult, len(completions))
	for index, completion := range completions {
		receipt, completionErr := applyCompletionTx(ctx, tx, completion, now)
		if errors.Is(completionErr, domain.ErrStaleLease) {
			results[index].Err = completionErr
			continue
		}
		if completionErr != nil {
			return nil, fmt.Errorf("complete batch item %d: %w", index, completionErr)
		}
		results[index].Receipt = receipt
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit completion batch: %w", err)
	}
	s.completionCommits.Add(1)
	return results, nil
}

func applyCompletionTx(
	ctx context.Context,
	tx *sql.Tx,
	completion domain.Completion,
	now time.Time,
) (domain.CompletionReceipt, error) {
	requestDigest, err := completionDigest(completion)
	if err != nil {
		return domain.CompletionReceipt{}, err
	}

	record, err := readCompletionRecord(ctx, tx, completion.JobID, completion.AttemptNo)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.CompletionReceipt{}, domain.ErrStaleLease
	}
	if err != nil {
		return domain.CompletionReceipt{}, err
	}
	if record.workerID != completion.WorkerID ||
		record.generation != completion.Generation ||
		record.tokenHash != domain.TokenHash(completion.Token) {
		return domain.CompletionReceipt{}, domain.ErrStaleLease
	}
	if record.completionDigest.Valid {
		if record.completionDigest.String != requestDigest ||
			!record.receiptState.Valid ||
			!record.receiptVersion.Valid ||
			!record.receiptCommitted.Valid {
			return domain.CompletionReceipt{}, domain.ErrStaleLease
		}
		receipt := domain.CompletionReceipt{
			JobID:        completion.JobID,
			State:        domain.JobState(record.receiptState.String),
			StateVersion: record.receiptVersion.Int64,
			CommittedAt:  timeFromDB(record.receiptCommitted.Int64),
			Duplicate:    true,
		}
		return receipt, nil
	}
	if record.currentAttempt != completion.AttemptNo ||
		record.currentGeneration != completion.Generation ||
		(record.jobState != string(domain.StateScheduled) &&
			record.jobState != string(domain.StateRunning)) ||
		(record.attemptState != "LEASED" && record.attemptState != "RUNNING") ||
		record.expiresAt <= timeToDB(now) {
		return domain.CompletionReceipt{}, domain.ErrStaleLease
	}

	failure := completion.Failure
	if completion.Success {
		failure = nil
	}
	if !completion.Success && failure == nil {
		failure = &domain.Failure{
			Class:   "attempt_failed",
			Message: "worker reported an unsuccessful attempt",
		}
	}
	failureJSON, err := encodeOptionalFailure(failure)
	if err != nil {
		return domain.CompletionReceipt{}, err
	}

	nextState := domain.StateSucceeded
	attemptState := "SUCCEEDED"
	availableAt := now.UTC()
	terminal := true
	if !completion.Success {
		attemptState = "FAILED"
		switch {
		case completion.Retryable &&
			record.retry.Retryable &&
			completion.AttemptNo < record.retry.MaxAttempts:
			nextState = domain.StateRetrying
			availableAt, err = retry.ReleaseAt(now, completion.JobID, completion.AttemptNo)
			if err != nil {
				return domain.CompletionReceipt{}, err
			}
			terminal = false
		case completion.Retryable:
			nextState = domain.StateDeadLetter
		default:
			nextState = domain.StateFailed
		}
	}
	nextVersion := record.jobVersion + 1

	result, err := tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET state = ?,
		     state_version = ?,
		     available_at = ?,
		     ready_seq = 0,
		     updated_at = ?,
		     terminal_at = ?,
		     failure_json = ?
		 WHERE id = ?
		   AND attempt_no = ?
		   AND lease_generation = ?
		   AND state_version = ?
		   AND state IN ('SCHEDULED', 'RUNNING')`,
		nextState,
		nextVersion,
		timeToDB(availableAt),
		timeToDB(now),
		nullableTime(now, terminal),
		failureJSON,
		completion.JobID,
		completion.AttemptNo,
		completion.Generation,
		record.jobVersion,
	)
	if err != nil {
		return domain.CompletionReceipt{}, fmt.Errorf("update completed job: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return domain.CompletionReceipt{}, fmt.Errorf("inspect completed job: %w", err)
	}
	if affected != 1 {
		return domain.CompletionReceipt{}, domain.ErrStaleLease
	}
	if err := releaseSlots(
		ctx,
		tx,
		record.tenantID,
		record.queue,
		record.slotCost,
		now,
	); err != nil {
		return domain.CompletionReceipt{}, err
	}

	if terminal {
		if err := insertCanonicalCompletion(
			ctx,
			tx,
			completion.JobID,
			nextState,
			nextVersion,
			completion.AttemptNo,
			completion.OutputDigest,
			failureJSON,
			now,
		); err != nil {
			return domain.CompletionReceipt{}, err
		}
		if nextState == domain.StateDeadLetter {
			if err := insertDeadLetter(ctx, tx, completion.JobID, failureJSON, now); err != nil {
				return domain.CompletionReceipt{}, err
			}
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE attempts
		 SET state = ?,
		     completed_at = ?,
		     failure_json = ?,
		     completion_request_digest = ?,
		     receipt_state = ?,
		     receipt_state_version = ?,
		     receipt_committed_at = ?
		 WHERE job_id = ?
		   AND attempt_no = ?
		   AND completion_request_digest IS NULL`,
		attemptState,
		timeToDB(now),
		failureJSON,
		requestDigest,
		nextState,
		nextVersion,
		timeToDB(now),
		completion.JobID,
		completion.AttemptNo,
	); err != nil {
		return domain.CompletionReceipt{}, fmt.Errorf("close completed attempt: %w", err)
	}

	eventType := "job_completed"
	if !terminal {
		eventType = "retry_scheduled"
	}
	if err := appendEvent(
		ctx,
		tx,
		completion.JobID,
		eventType,
		nextState,
		nextVersion,
		now,
		struct {
			AttemptNo   int   `json:"attempt_no"`
			AvailableAt int64 `json:"available_at,omitempty"`
		}{
			AttemptNo:   completion.AttemptNo,
			AvailableAt: timeToDB(availableAt),
		},
	); err != nil {
		return domain.CompletionReceipt{}, err
	}

	if nextState == domain.StateSucceeded {
		if err := activateChildren(ctx, tx, completion.JobID, now); err != nil {
			return domain.CompletionReceipt{}, err
		}
	} else if terminal {
		if err := failDescendants(ctx, tx, completion.JobID, now); err != nil {
			return domain.CompletionReceipt{}, err
		}
	}

	receipt := domain.CompletionReceipt{
		JobID:        completion.JobID,
		State:        nextState,
		StateVersion: nextVersion,
		CommittedAt:  now.UTC(),
	}
	return receipt, nil
}

func readCompletionRecord(
	ctx context.Context,
	tx *sql.Tx,
	jobID string,
	attemptNo int,
) (completionRecord, error) {
	var record completionRecord
	var retryJSON string
	err := tx.QueryRowContext(
		ctx,
		`SELECT
			a.worker_id, a.lease_generation, a.token_hash, a.state, a.expires_at,
			a.completion_request_digest, a.receipt_state,
			a.receipt_state_version, a.receipt_committed_at,
			j.state, j.state_version, j.attempt_no, j.lease_generation,
			j.slot_cost, j.tenant_id, j.queue_name, j.retry_json
		 FROM attempts a
		 JOIN jobs j ON j.id = a.job_id
		 WHERE a.job_id = ? AND a.attempt_no = ?`,
		jobID,
		attemptNo,
	).Scan(
		&record.workerID,
		&record.generation,
		&record.tokenHash,
		&record.attemptState,
		&record.expiresAt,
		&record.completionDigest,
		&record.receiptState,
		&record.receiptVersion,
		&record.receiptCommitted,
		&record.jobState,
		&record.jobVersion,
		&record.currentAttempt,
		&record.currentGeneration,
		&record.slotCost,
		&record.tenantID,
		&record.queue,
		&retryJSON,
	)
	if err != nil {
		return completionRecord{}, err
	}
	if err := json.Unmarshal([]byte(retryJSON), &record.retry); err != nil {
		return completionRecord{}, fmt.Errorf("decode completion retry policy: %w", err)
	}
	return record, nil
}

func completionDigest(completion domain.Completion) (string, error) {
	body, err := json.Marshal(completion)
	if err != nil {
		return "", fmt.Errorf("encode completion request: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func nullableTime(value time.Time, present bool) any {
	if !present {
		return nil
	}
	return timeToDB(value)
}

func encodeOptionalFailure(failure *domain.Failure) (any, error) {
	if failure == nil {
		return nil, nil
	}
	body, err := json.Marshal(failure)
	if err != nil {
		return nil, fmt.Errorf("encode failure: %w", err)
	}
	return string(body), nil
}

func reserveSlots(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	queue string,
	slotCost int,
	now time.Time,
) (bool, error) {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE tenant_limits
		 SET active_slots = active_slots + ?
		 WHERE tenant_id = ?
		   AND (max_slots = 0 OR active_slots + ? <= max_slots)`,
		slotCost,
		tenantID,
		slotCost,
	)
	if err != nil {
		return false, fmt.Errorf("reserve tenant slots: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect tenant slot reservation: %w", err)
	}
	if affected != 1 {
		return false, nil
	}
	result, err = tx.ExecContext(
		ctx,
		`UPDATE queue_state
		 SET active_slots = active_slots + ?, updated_at = ?
		 WHERE tenant_id = ? AND queue_name = ?`,
		slotCost,
		timeToDB(now),
		tenantID,
		queue,
	)
	if err != nil {
		return false, fmt.Errorf("reserve queue slots: %w", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect queue slot reservation: %w", err)
	}
	if affected != 1 {
		return false, errors.New("queue state is missing")
	}
	return true, nil
}

func releaseSlots(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	queue string,
	slotCost int,
	now time.Time,
) error {
	result, err := tx.ExecContext(
		ctx,
		`UPDATE tenant_limits
		 SET active_slots = active_slots - ?
		 WHERE tenant_id = ? AND active_slots >= ?`,
		slotCost,
		tenantID,
		slotCost,
	)
	if err != nil {
		return fmt.Errorf("release tenant slots: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect tenant slot release: %w", err)
	}
	if affected != 1 {
		return errors.New("tenant slot reservation is inconsistent")
	}
	result, err = tx.ExecContext(
		ctx,
		`UPDATE queue_state
		 SET active_slots = active_slots - ?, updated_at = ?
		 WHERE tenant_id = ? AND queue_name = ? AND active_slots >= ?`,
		slotCost,
		timeToDB(now),
		tenantID,
		queue,
		slotCost,
	)
	if err != nil {
		return fmt.Errorf("release queue slots: %w", err)
	}
	affected, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect queue slot release: %w", err)
	}
	if affected != 1 {
		return errors.New("queue slot reservation is inconsistent")
	}
	return nil
}

func insertCanonicalCompletion(
	ctx context.Context,
	tx *sql.Tx,
	jobID string,
	state domain.JobState,
	stateVersion int64,
	attemptNo int,
	outputDigest string,
	failureJSON any,
	now time.Time,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO job_completions
			(job_id, state, state_version, attempt_no, output_digest, failure_json, committed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		jobID,
		state,
		stateVersion,
		attemptNo,
		outputDigest,
		failureJSON,
		timeToDB(now),
	); err != nil {
		return fmt.Errorf("insert canonical completion: %w", err)
	}
	return nil
}

func insertDeadLetter(
	ctx context.Context,
	tx *sql.Tx,
	jobID string,
	failureJSON any,
	now time.Time,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO dead_letters (job_id, failure_json, created_at)
		 VALUES (?, ?, ?)`,
		jobID,
		failureJSON,
		timeToDB(now),
	); err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}
	return nil
}
