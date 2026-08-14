package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	telemetrymodel "github.com/rajeev-chaurasia/rail-yard/internal/telemetry/model"
)

func (s *Store) TelemetrySnapshot(ctx context.Context) (telemetrymodel.Snapshot, error) {
	var snapshot telemetrymodel.Snapshot
	err := s.db.QueryRowContext(ctx, `
		SELECT
			(SELECT COALESCE(MAX(event_seq), 0) FROM events),
			COALESCE(SUM(CASE WHEN state = 'PENDING' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'SCHEDULED' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'RUNNING' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'RETRYING' THEN 1 ELSE 0 END), 0),
			(SELECT COUNT(*) FROM dead_letters WHERE redriven_job_id IS NULL)
		FROM jobs
	`).Scan(
		&snapshot.Sequence,
		&snapshot.Pending,
		&snapshot.Scheduled,
		&snapshot.Running,
		&snapshot.Retrying,
		&snapshot.DLQ,
	)
	if err != nil {
		return telemetrymodel.Snapshot{}, fmt.Errorf("read telemetry snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Store) TelemetryEvents(
	ctx context.Context,
	afterSequence int64,
	limit int,
) ([]telemetrymodel.TimingEvent, error) {
	if limit < 1 {
		return []telemetrymodel.TimingEvent{}, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			e.event_seq,
			e.event_type,
			e.state,
			j.created_at,
			a.leased_at,
			a.completed_at,
			CASE WHEN e.event_type = 'lease_acquired' THEN (
				SELECT MAX(ready.occurred_at)
				FROM events ready
				WHERE ready.job_id = e.job_id
				  AND ready.event_seq < e.event_seq
				  AND ready.event_type IN (
				      'job_admitted',
				      'job_promoted',
				      'dependency_released',
				      'lease_reaped',
				      'operator_release'
				  )
			) END,
			CASE WHEN e.event_type = 'lease_acquired' AND EXISTS (
				SELECT 1
				FROM events loss
				WHERE loss.job_id = e.job_id
				  AND loss.event_type = 'lease_reaped'
				  AND loss.state = 'PENDING'
				  AND CAST(json_extract(loss.payload_json, '$.attempt_no') AS INTEGER) =
				      previous.attempt_no
				  AND loss.occurred_at = previous.completed_at
			) THEN previous.completed_at END,
			completion.committed_at
		FROM events e
		JOIN jobs j ON j.id = e.job_id
		LEFT JOIN attempts a
		  ON a.job_id = e.job_id
		 AND a.attempt_no = CAST(json_extract(e.payload_json, '$.attempt_no') AS INTEGER)
		LEFT JOIN attempts previous
		  ON previous.job_id = e.job_id
		 AND previous.attempt_no = a.attempt_no - 1
		 AND previous.state = 'EXPIRED'
		LEFT JOIN job_completions completion
		  ON completion.job_id = e.job_id
		 AND completion.committed_at = e.occurred_at
		WHERE e.event_seq > ?
		ORDER BY e.event_seq
		LIMIT ?
	`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("query telemetry events: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	events := make([]telemetrymodel.TimingEvent, 0, limit)
	for rows.Next() {
		var (
			event                       telemetrymodel.TimingEvent
			eventType, state            string
			createdAt                   int64
			leasedAt, completedAt       sql.NullInt64
			readyAt, recoveryOrigin     sql.NullInt64
			terminalCompletionCommitted sql.NullInt64
		)
		if err := rows.Scan(
			&event.Sequence,
			&eventType,
			&state,
			&createdAt,
			&leasedAt,
			&completedAt,
			&readyAt,
			&recoveryOrigin,
			&terminalCompletionCommitted,
		); err != nil {
			return nil, fmt.Errorf("scan telemetry event: %w", err)
		}

		if eventType == "lease_acquired" && leasedAt.Valid {
			event.ReadyToLease = durationBetween(readyAt, leasedAt.Int64)
			event.LeaseRecovery = durationBetween(recoveryOrigin, leasedAt.Int64)
		}
		if (eventType == "job_completed" || eventType == "retry_scheduled") &&
			leasedAt.Valid {
			event.LeaseToCompletion = durationBetween(leasedAt, completedAt.Int64)
		}
		if isTerminalState(state) && terminalCompletionCommitted.Valid {
			origin := sql.NullInt64{Int64: createdAt, Valid: true}
			event.EndToEnd = durationBetween(origin, terminalCompletionCommitted.Int64)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate telemetry events: %w", err)
	}
	return events, nil
}

func durationBetween(origin sql.NullInt64, end int64) *time.Duration {
	if !origin.Valid || end < origin.Int64 {
		return nil
	}
	value := time.Duration(end - origin.Int64)
	return &value
}

func isTerminalState(state string) bool {
	return state == "SUCCEEDED" || state == "FAILED" || state == "DEAD_LETTER"
}
