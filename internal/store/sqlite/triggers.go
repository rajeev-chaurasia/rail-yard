package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
	"github.com/rajeev-chaurasia/rail-yard/internal/trigger"
)

func (s *Store) CreateCronTrigger(
	ctx context.Context,
	submission storepkg.CronSubmission,
	now time.Time,
) (domain.CronTrigger, bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	value := submission.Trigger
	if value.TenantID == "" {
		value.TenantID = "default"
	}
	value.Job.TenantID = value.TenantID
	value.Job = value.Job.Normalize()
	if err := value.Job.Validate(s.maxSlotCost, s.allowShell); err != nil {
		return domain.CronTrigger{}, false, err
	}
	schedule, err := trigger.ParseCron(value.Expression)
	if err != nil {
		return domain.CronTrigger{}, false, err
	}
	if err := validateIdempotency(submission.IdempotencyKey, submission.RequestDigest); err != nil {
		return domain.CronTrigger{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.CronTrigger{}, false, fmt.Errorf("begin cron trigger creation: %w", err)
	}
	defer rollback(tx)
	response, found, err := readIdempotency(
		ctx,
		tx,
		value.TenantID,
		submission.IdempotencyKey,
		"cron",
		submission.RequestDigest,
	)
	if err != nil {
		return domain.CronTrigger{}, false, err
	}
	if found {
		var existing domain.CronTrigger
		if err := json.Unmarshal([]byte(response), &existing); err != nil {
			return domain.CronTrigger{}, false, fmt.Errorf("decode cron trigger response: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.CronTrigger{}, false, fmt.Errorf("commit duplicate cron trigger: %w", err)
		}
		return existing, true, nil
	}

	if value.ID == "" {
		value.ID, err = domain.NewID()
		if err != nil {
			return domain.CronTrigger{}, false, err
		}
	}
	value.Enabled = true
	value.CreatedAt = now.UTC()
	value.UpdatedAt = now.UTC()
	value.NextFireAt = schedule.Next(now.UTC())
	jobJSON, err := json.Marshal(value.Job)
	if err != nil {
		return domain.CronTrigger{}, false, fmt.Errorf("encode cron job: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO cron_triggers
			(id, tenant_id, expression, job_spec_json, next_fire_at, enabled, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, 1, ?, ?)`,
		value.ID,
		value.TenantID,
		value.Expression,
		string(jobJSON),
		timeToDB(value.NextFireAt),
		timeToDB(now),
		timeToDB(now),
	); err != nil {
		return domain.CronTrigger{}, false, fmt.Errorf("insert cron trigger: %w", err)
	}
	responseJSON, err := json.Marshal(value)
	if err != nil {
		return domain.CronTrigger{}, false, fmt.Errorf("encode cron trigger response: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO idempotency_requests
			(tenant_id, submission_key, request_kind, request_digest, response_json, created_at)
		 VALUES (?, ?, 'cron', ?, ?, ?)`,
		value.TenantID,
		submission.IdempotencyKey,
		submission.RequestDigest,
		string(responseJSON),
		timeToDB(now),
	); err != nil {
		return domain.CronTrigger{}, false, fmt.Errorf("record cron idempotency: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.CronTrigger{}, false, fmt.Errorf("commit cron trigger: %w", err)
	}
	return value, false, nil
}

func (s *Store) FireDueCron(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if limit < 1 {
		return []string{}, nil
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, tenant_id, expression, job_spec_json, next_fire_at
		 FROM cron_triggers
		 WHERE enabled = 1 AND next_fire_at <= ?
		 ORDER BY next_fire_at, id
		 LIMIT ?`,
		timeToDB(now),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query due cron triggers: %w", err)
	}
	type dueTrigger struct {
		id         string
		tenantID   string
		expression string
		job        domain.JobSpec
		nominal    time.Time
	}
	values := make([]dueTrigger, 0, limit)
	for rows.Next() {
		var value dueTrigger
		var jobJSON string
		var nominal int64
		if err := rows.Scan(&value.id, &value.tenantID, &value.expression, &jobJSON, &nominal); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan due cron trigger: %w", err)
		}
		if err := json.Unmarshal([]byte(jobJSON), &value.job); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("decode cron job: %w", err)
		}
		value.nominal = timeFromDB(nominal)
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close due cron triggers: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due cron triggers: %w", err)
	}

	jobIDs := make([]string, 0, len(values))
	for _, value := range values {
		key := trigger.CronOccurrenceKey(value.id, value.nominal)
		sum := sha256.Sum256([]byte(key))
		job, _, err := s.SubmitJob(ctx, storepkg.Submission{
			Job:            value.job,
			IdempotencyKey: key,
			RequestDigest:  hex.EncodeToString(sum[:]),
		}, now)
		if err != nil {
			return nil, err
		}
		schedule, err := trigger.ParseCron(value.expression)
		if err != nil {
			return nil, err
		}
		nextFireAt := schedule.Next(now.UTC())

		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("begin cron occurrence: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO cron_occurrences (trigger_id, nominal_at, job_id, created_at)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT (trigger_id, nominal_at) DO NOTHING`,
			value.id,
			timeToDB(value.nominal),
			job.ID,
			timeToDB(now),
		); err != nil {
			rollback(tx)
			return nil, fmt.Errorf("insert cron occurrence: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE cron_triggers
			 SET next_fire_at = ?, updated_at = ?
			 WHERE id = ? AND next_fire_at = ?`,
			timeToDB(nextFireAt),
			timeToDB(now),
			value.id,
			timeToDB(value.nominal),
		); err != nil {
			rollback(tx)
			return nil, fmt.Errorf("advance cron trigger: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit cron occurrence: %w", err)
		}
		jobIDs = append(jobIDs, job.ID)
	}
	return jobIDs, nil
}

func (s *Store) DeliverRedis(ctx context.Context, delivery trigger.RedisDelivery) error {
	rawJob, exists := delivery.Values["job"]
	if !exists {
		return errors.New("redis delivery requires a job field")
	}
	var encoded []byte
	switch value := rawJob.(type) {
	case string:
		encoded = []byte(value)
	case []byte:
		encoded = value
	default:
		return fmt.Errorf("redis job field has unsupported type %T", rawJob)
	}
	var spec domain.JobSpec
	if err := json.Unmarshal(encoded, &spec); err != nil {
		return fmt.Errorf("decode Redis job: %w", err)
	}
	canonical, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encode Redis job: %w", err)
	}
	sum := sha256.Sum256(canonical)
	now := time.Now().UTC()
	job, _, err := s.SubmitJob(ctx, storepkg.Submission{
		Job:            spec,
		IdempotencyKey: delivery.IdempotencyKey(),
		RequestDigest:  hex.EncodeToString(sum[:]),
	}, now)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Redis delivery: %w", err)
	}
	defer rollback(tx)
	result, err := tx.ExecContext(
		ctx,
		`INSERT INTO redis_deliveries
			(trigger_id, stream, message_id, job_id, created_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (trigger_id, stream, message_id) DO NOTHING`,
		delivery.TriggerID,
		delivery.Stream,
		delivery.MessageID,
		job.ID,
		timeToDB(now),
	)
	if err != nil {
		return fmt.Errorf("insert Redis delivery: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect Redis delivery: %w", err)
	}
	if affected == 0 {
		var existingJobID string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT job_id FROM redis_deliveries
			 WHERE trigger_id = ? AND stream = ? AND message_id = ?`,
			delivery.TriggerID,
			delivery.Stream,
			delivery.MessageID,
		).Scan(&existingJobID); err != nil {
			return fmt.Errorf("read Redis delivery: %w", err)
		}
		if existingJobID != job.ID {
			return domain.ErrIdempotencyConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Redis delivery: %w", err)
	}
	return nil
}

var _ storepkg.TriggerStore = (*Store)(nil)
