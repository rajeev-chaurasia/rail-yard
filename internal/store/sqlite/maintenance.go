package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

func (s *Store) PromoteDue(
	ctx context.Context,
	now time.Time,
	limit int,
) (int, error) {
	if limit < 1 {
		return 0, nil
	}
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin due promotion: %w", err)
	}
	defer rollback(tx)

	rows, err := tx.QueryContext(
		ctx,
		`SELECT j.id, j.state, j.state_version
		 FROM jobs j
		 WHERE j.state IN ('PENDING', 'RETRYING')
		   AND j.ready_seq = 0
		   AND j.available_at <= ?
		   AND NOT EXISTS (
		       SELECT 1
		       FROM job_dependencies d
		       JOIN jobs parent ON parent.id = d.depends_on_id
		       WHERE d.job_id = j.id AND parent.state <> 'SUCCEEDED'
		   )
		 ORDER BY j.available_at ASC, j.id ASC
		 LIMIT ?`,
		timeToDB(now),
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("select due jobs: %w", err)
	}

	type dueJob struct {
		id      string
		state   domain.JobState
		version int64
	}
	jobs := make([]dueJob, 0, limit)
	for rows.Next() {
		var job dueJob
		if err := rows.Scan(&job.id, &job.state, &job.version); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan due job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close due jobs: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate due jobs: %w", err)
	}

	for _, job := range jobs {
		readySeq, err := nextReadySeq(ctx, tx)
		if err != nil {
			return 0, err
		}
		nextVersion := job.version + 1
		result, err := tx.ExecContext(
			ctx,
			`UPDATE jobs
			 SET state = 'PENDING',
			     state_version = ?,
			     ready_seq = ?,
			     updated_at = ?
			 WHERE id = ?
			   AND state = ?
			   AND state_version = ?
			   AND ready_seq = 0`,
			nextVersion,
			readySeq,
			timeToDB(now),
			job.id,
			job.state,
			job.version,
		)
		if err != nil {
			return 0, fmt.Errorf("promote due job %q: %w", job.id, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("inspect due job %q: %w", job.id, err)
		}
		if affected != 1 {
			return 0, fmt.Errorf("due job %q changed during promotion", job.id)
		}
		if err := appendEvent(
			ctx,
			tx,
			job.id,
			"job_promoted",
			domain.StatePending,
			nextVersion,
			now,
			struct {
				ReadySeq int64 `json:"ready_seq"`
			}{ReadySeq: readySeq},
		); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit due promotion: %w", err)
	}
	return len(jobs), nil
}

type expiredAttempt struct {
	jobID        string
	workerID     string
	attemptNo    int
	generation   int64
	expiresAt    int64
	jobState     domain.JobState
	stateVersion int64
	slotCost     int
	tenantID     string
	queue        string
	retry        domain.RetryPolicy
}

func (s *Store) ReapExpired(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]domain.ReapedLease, error) {
	if limit < 1 {
		return []domain.ReapedLease{}, nil
	}
	releaseWrite := s.beginWrite(writeMaintenance)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin expired lease reap: %w", err)
	}
	defer rollback(tx)

	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			a.job_id, a.worker_id, a.attempt_no, a.lease_generation, a.expires_at,
			j.state, j.state_version, j.slot_cost, j.tenant_id, j.queue_name, j.retry_json
		 FROM attempts a
		 JOIN jobs j
		   ON j.id = a.job_id
		  AND j.attempt_no = a.attempt_no
		  AND j.lease_generation = a.lease_generation
		 WHERE a.state IN ('LEASED', 'RUNNING')
		   AND a.expires_at <= ?
		   AND j.state IN ('SCHEDULED', 'RUNNING')
		 ORDER BY a.expires_at ASC, a.job_id ASC
		 LIMIT ?`,
		timeToDB(now),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("select expired leases: %w", err)
	}

	expired := make([]expiredAttempt, 0, limit)
	for rows.Next() {
		var attempt expiredAttempt
		var retryJSON string
		if err := rows.Scan(
			&attempt.jobID,
			&attempt.workerID,
			&attempt.attemptNo,
			&attempt.generation,
			&attempt.expiresAt,
			&attempt.jobState,
			&attempt.stateVersion,
			&attempt.slotCost,
			&attempt.tenantID,
			&attempt.queue,
			&retryJSON,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan expired lease: %w", err)
		}
		if err := json.Unmarshal([]byte(retryJSON), &attempt.retry); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("decode expired lease retry policy: %w", err)
		}
		expired = append(expired, attempt)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close expired leases: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired leases: %w", err)
	}

	reaped := make([]domain.ReapedLease, 0, len(expired))
	for _, attempt := range expired {
		if err := reapAttempt(ctx, tx, attempt, now); err != nil {
			return nil, err
		}
		nextAvailableAt := now.UTC()
		if attempt.attemptNo >= attempt.retry.MaxAttempts {
			nextAvailableAt = time.Time{}
		}
		reaped = append(reaped, domain.ReapedLease{
			JobID:           attempt.jobID,
			OldWorkerID:     attempt.workerID,
			Generation:      attempt.generation,
			ExpiredAt:       timeFromDB(attempt.expiresAt),
			NextAvailableAt: nextAvailableAt,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit expired lease reap: %w", err)
	}
	return reaped, nil
}

func reapAttempt(
	ctx context.Context,
	tx *sql.Tx,
	attempt expiredAttempt,
	now time.Time,
) error {
	failure := &domain.Failure{
		Class:   "lease_expired",
		Message: "worker lease expired before completion",
	}
	failureJSON, err := encodeOptionalFailure(failure)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE attempts
		 SET state = 'EXPIRED', completed_at = ?, failure_json = ?
		 WHERE job_id = ?
		   AND attempt_no = ?
		   AND lease_generation = ?
		   AND state IN ('LEASED', 'RUNNING')`,
		timeToDB(now),
		failureJSON,
		attempt.jobID,
		attempt.attemptNo,
		attempt.generation,
	); err != nil {
		return fmt.Errorf("expire attempt for job %q: %w", attempt.jobID, err)
	}
	if err := releaseSlots(
		ctx,
		tx,
		attempt.tenantID,
		attempt.queue,
		attempt.slotCost,
		now,
	); err != nil {
		return err
	}

	nextVersion := attempt.stateVersion + 1
	if attempt.attemptNo < attempt.retry.MaxAttempts {
		readySeq, err := nextReadySeq(ctx, tx)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(
			ctx,
			`UPDATE jobs
			 SET state = 'PENDING',
			     state_version = ?,
			     available_at = ?,
			     ready_seq = ?,
			     recovery_pending = 1,
			     updated_at = ?,
			     failure_json = ?
			 WHERE id = ?
			   AND attempt_no = ?
			   AND lease_generation = ?
			   AND state_version = ?
			   AND state IN ('SCHEDULED', 'RUNNING')`,
			nextVersion,
			timeToDB(now),
			readySeq,
			timeToDB(now),
			failureJSON,
			attempt.jobID,
			attempt.attemptNo,
			attempt.generation,
			attempt.stateVersion,
		)
		if err != nil {
			return fmt.Errorf("requeue expired job %q: %w", attempt.jobID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect expired job %q: %w", attempt.jobID, err)
		}
		if affected != 1 {
			return domain.ErrStaleLease
		}
		return appendEvent(
			ctx,
			tx,
			attempt.jobID,
			"lease_reaped",
			domain.StatePending,
			nextVersion,
			now,
			struct {
				AttemptNo  int   `json:"attempt_no"`
				Generation int64 `json:"generation"`
				ReadySeq   int64 `json:"ready_seq"`
			}{
				AttemptNo:  attempt.attemptNo,
				Generation: attempt.generation,
				ReadySeq:   readySeq,
			},
		)
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE jobs
		 SET state = 'DEAD_LETTER',
		     state_version = ?,
		     ready_seq = 0,
		     updated_at = ?,
		     terminal_at = ?,
		     failure_json = ?
		 WHERE id = ?
		   AND attempt_no = ?
		   AND lease_generation = ?
		   AND state_version = ?
		   AND state IN ('SCHEDULED', 'RUNNING')`,
		nextVersion,
		timeToDB(now),
		timeToDB(now),
		failureJSON,
		attempt.jobID,
		attempt.attemptNo,
		attempt.generation,
		attempt.stateVersion,
	)
	if err != nil {
		return fmt.Errorf("dead-letter expired job %q: %w", attempt.jobID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect dead-letter job %q: %w", attempt.jobID, err)
	}
	if affected != 1 {
		return domain.ErrStaleLease
	}
	if err := insertCanonicalCompletion(
		ctx,
		tx,
		attempt.jobID,
		domain.StateDeadLetter,
		nextVersion,
		attempt.attemptNo,
		"",
		failureJSON,
		now,
	); err != nil {
		return err
	}
	if err := insertDeadLetter(ctx, tx, attempt.jobID, failureJSON, now); err != nil {
		return err
	}
	if err := appendEvent(
		ctx,
		tx,
		attempt.jobID,
		"lease_reaped",
		domain.StateDeadLetter,
		nextVersion,
		now,
		struct {
			AttemptNo  int   `json:"attempt_no"`
			Generation int64 `json:"generation"`
		}{AttemptNo: attempt.attemptNo, Generation: attempt.generation},
	); err != nil {
		return err
	}
	return failDescendants(ctx, tx, attempt.jobID, now)
}

func activateChildren(
	ctx context.Context,
	tx *sql.Tx,
	parentID string,
	now time.Time,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT child.id, child.state_version
		 FROM job_dependencies edge
		 JOIN jobs child ON child.id = edge.job_id
		 WHERE edge.depends_on_id = ?
		   AND child.state = 'PENDING'
		   AND child.ready_seq = 0
		   AND child.available_at <= ?
		   AND NOT EXISTS (
		       SELECT 1
		       FROM job_dependencies sibling_edge
		       JOIN jobs parent ON parent.id = sibling_edge.depends_on_id
		       WHERE sibling_edge.job_id = child.id
		         AND parent.state <> 'SUCCEEDED'
		   )
		 ORDER BY child.id ASC`,
		parentID,
		timeToDB(now),
	)
	if err != nil {
		return fmt.Errorf("select newly ready children: %w", err)
	}
	type child struct {
		id      string
		version int64
	}
	var children []child
	for rows.Next() {
		var value child
		if err := rows.Scan(&value.id, &value.version); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan newly ready child: %w", err)
		}
		children = append(children, value)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close newly ready children: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate newly ready children: %w", err)
	}

	for _, child := range children {
		readySeq, err := nextReadySeq(ctx, tx)
		if err != nil {
			return err
		}
		nextVersion := child.version + 1
		result, err := tx.ExecContext(
			ctx,
			`UPDATE jobs
			 SET ready_seq = ?, state_version = ?, updated_at = ?
			 WHERE id = ? AND state = 'PENDING' AND ready_seq = 0 AND state_version = ?`,
			readySeq,
			nextVersion,
			timeToDB(now),
			child.id,
			child.version,
		)
		if err != nil {
			return fmt.Errorf("activate child job %q: %w", child.id, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect activated child %q: %w", child.id, err)
		}
		if affected != 1 {
			return fmt.Errorf("child job %q changed during activation", child.id)
		}
		if err := appendEvent(
			ctx,
			tx,
			child.id,
			"dependency_released",
			domain.StatePending,
			nextVersion,
			now,
			struct {
				ParentID string `json:"parent_id"`
				ReadySeq int64  `json:"ready_seq"`
			}{ParentID: parentID, ReadySeq: readySeq},
		); err != nil {
			return err
		}
	}
	return nil
}

func failDescendants(
	ctx context.Context,
	tx *sql.Tx,
	parentID string,
	now time.Time,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`WITH RECURSIVE descendants(id) AS (
		     SELECT job_id FROM job_dependencies WHERE depends_on_id = ?
		     UNION
		     SELECT edge.job_id
		     FROM job_dependencies edge
		     JOIN descendants parent ON parent.id = edge.depends_on_id
		 )
		 SELECT
			j.id, j.state, j.state_version, j.attempt_no,
			j.slot_cost, j.tenant_id, j.queue_name
		 FROM jobs j
		 JOIN descendants d ON d.id = j.id
		 WHERE j.state NOT IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTER')
		 ORDER BY j.id ASC`,
		parentID,
	)
	if err != nil {
		return fmt.Errorf("select failed descendants: %w", err)
	}
	type descendant struct {
		id        string
		state     domain.JobState
		version   int64
		attemptNo int
		slotCost  int
		tenantID  string
		queue     string
	}
	var descendants []descendant
	for rows.Next() {
		var value descendant
		if err := rows.Scan(
			&value.id,
			&value.state,
			&value.version,
			&value.attemptNo,
			&value.slotCost,
			&value.tenantID,
			&value.queue,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan failed descendant: %w", err)
		}
		descendants = append(descendants, value)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close failed descendants: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate failed descendants: %w", err)
	}

	failure := &domain.Failure{
		Class:   "upstream_failed",
		Message: "an upstream dependency did not succeed",
	}
	failureJSON, err := encodeOptionalFailure(failure)
	if err != nil {
		return err
	}
	for _, child := range descendants {
		if child.state == domain.StateScheduled || child.state == domain.StateRunning {
			if err := releaseSlots(
				ctx,
				tx,
				child.tenantID,
				child.queue,
				child.slotCost,
				now,
			); err != nil {
				return err
			}
		}
		nextVersion := child.version + 1
		result, err := tx.ExecContext(
			ctx,
			`UPDATE jobs
			 SET state = 'DEAD_LETTER',
			     state_version = ?,
			     ready_seq = 0,
			     updated_at = ?,
			     terminal_at = ?,
			     failure_json = ?
			 WHERE id = ?
			   AND state_version = ?
			   AND state NOT IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTER')`,
			nextVersion,
			timeToDB(now),
			timeToDB(now),
			failureJSON,
			child.id,
			child.version,
		)
		if err != nil {
			return fmt.Errorf("dead-letter descendant %q: %w", child.id, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect failed descendant %q: %w", child.id, err)
		}
		if affected != 1 {
			return fmt.Errorf("descendant %q changed during propagation", child.id)
		}
		if err := insertCanonicalCompletion(
			ctx,
			tx,
			child.id,
			domain.StateDeadLetter,
			nextVersion,
			child.attemptNo,
			"",
			failureJSON,
			now,
		); err != nil {
			return err
		}
		if err := insertDeadLetter(ctx, tx, child.id, failureJSON, now); err != nil {
			return err
		}
		if err := appendEvent(
			ctx,
			tx,
			child.id,
			"upstream_failed",
			domain.StateDeadLetter,
			nextVersion,
			now,
			struct {
				RootJobID string `json:"root_job_id"`
			}{RootJobID: parentID},
		); err != nil {
			return err
		}
	}
	return nil
}
