package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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
	s.redriveMu.Lock()
	defer s.redriveMu.Unlock()

	var redrivenJobID sql.NullString
	err := s.db.QueryRowContext(
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
	if redrivenJobID.Valid {
		return domain.Job{}, false, domain.ErrDeadLetterRedriven
	}
	original, err := s.GetJob(ctx, jobID)
	if err != nil {
		return domain.Job{}, false, err
	}
	created, duplicate, err := s.SubmitJob(ctx, storepkg.Submission{
		Job: domain.JobSpec{
			TenantID: original.TenantID,
			Queue:    original.Queue,
			Priority: original.Priority,
			SlotCost: original.SlotCost,
			Payload:  original.Payload,
			Retry:    original.Retry,
		},
		IdempotencyKey: idempotencyKey,
		RequestDigest:  requestDigest,
	}, now)
	if err != nil {
		return domain.Job{}, false, err
	}
	result, err := s.db.ExecContext(
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
	return created, duplicate, nil
}

var _ storepkg.DeadLetterStore = (*Store)(nil)
