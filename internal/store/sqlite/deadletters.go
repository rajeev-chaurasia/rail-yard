package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func (s *Store) ListDeadLetters(ctx context.Context, limit int) ([]domain.DeadLetter, error) {
	if limit < 1 {
		return []domain.DeadLetter{}, nil
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT job_id, failure_json, created_at, redriven_job_id
		 FROM dead_letters
		 WHERE redriven_job_id IS NULL
		 ORDER BY created_at, job_id
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query dead letters: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	values := make([]domain.DeadLetter, 0, limit)
	for rows.Next() {
		var value domain.DeadLetter
		var failureJSON string
		var createdAt int64
		var redrivenJobID sql.NullString
		if err := rows.Scan(
			&value.JobID,
			&failureJSON,
			&createdAt,
			&redrivenJobID,
		); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		if err := json.Unmarshal([]byte(failureJSON), &value.Failure); err != nil {
			return nil, fmt.Errorf("decode dead letter failure: %w", err)
		}
		value.CreatedAt = timeFromDB(createdAt)
		if redrivenJobID.Valid {
			value.RedrivenJobID = redrivenJobID.String
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters: %w", err)
	}
	return values, nil
}

func (s *Store) RedriveDeadLetter(
	ctx context.Context,
	jobID string,
	idempotencyKey string,
	requestDigest string,
	now time.Time,
) (domain.Job, bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("begin dead-letter redrive: %w", err)
	}
	defer rollback(tx)

	original, err := scanJob(tx.QueryRowContext(
		ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = ?",
		jobID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, false, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("read redrive source: %w", err)
	}
	var redrivenJobID sql.NullString
	err = tx.QueryRowContext(
		ctx,
		"SELECT redriven_job_id FROM dead_letters WHERE job_id = ?",
		jobID,
	).Scan(&redrivenJobID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, false, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("read dead letter: %w", err)
	}
	const actionName = "dead_letter.redrive"
	stored, duplicate, err := readControlAction(
		ctx,
		tx,
		original.TenantID,
		idempotencyKey,
		actionName,
		requestDigest,
	)
	if err != nil {
		return domain.Job{}, false, err
	}
	if duplicate {
		var response api.RedriveDeadLetterResponse
		if err := json.Unmarshal([]byte(stored), &response); err != nil {
			return domain.Job{}, false, fmt.Errorf("decode redrive response: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.Job{}, false, fmt.Errorf("commit duplicate redrive: %w", err)
		}
		return response.Job, true, nil
	}
	if redrivenJobID.Valid {
		return domain.Job{}, false, domain.ErrDeadLetterRedriven
	}
	created, err := cloneJob(ctx, tx, s, original, now)
	if err != nil {
		return domain.Job{}, false, err
	}
	result, err := tx.ExecContext(
		ctx,
		`UPDATE dead_letters
		 SET redriven_job_id = ?
		 WHERE job_id = ? AND redriven_job_id IS NULL`,
		created.ID,
		jobID,
	)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("link redriven job: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("inspect redrive link: %w", err)
	}
	if affected != 1 {
		return domain.Job{}, false, domain.ErrDeadLetterRedriven
	}
	if err := appendEvent(
		ctx,
		tx,
		jobID,
		"operator_redrive",
		original.State,
		original.StateVersion,
		now,
		map[string]any{"actor": "api", "created_job_id": created.ID},
	); err != nil {
		return domain.Job{}, false, err
	}
	response := api.RedriveDeadLetterResponse{Job: created}
	if err := insertControlAction(ctx, tx, ControlAction{
		TenantID:       original.TenantID,
		IdempotencyKey: idempotencyKey,
		Action:         actionName,
		Actor:          "api",
		RequestDigest:  requestDigest,
		CommittedAt:    now,
		TargetType:     "dead_letter",
		TargetID:       jobID,
		TargetState:    created.State,
		TargetVersion:  created.StateVersion,
		Response:       response,
		Details:        map[string]string{"created_job_id": created.ID},
	}); err != nil {
		return domain.Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return domain.Job{}, false, fmt.Errorf("commit dead-letter redrive: %w", err)
	}
	return created, false, nil
}

var _ storepkg.DeadLetterStore = (*Store)(nil)
