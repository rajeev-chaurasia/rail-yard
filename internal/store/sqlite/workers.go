package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func (s *Store) RegisterWorker(
	ctx context.Context,
	workerID string,
	capacitySlots int,
	now time.Time,
) error {
	if workerID == "" {
		return errors.New("worker ID must not be empty")
	}
	if capacitySlots < 1 {
		return errors.New("worker capacity must be positive")
	}

	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker registration: %w", err)
	}
	defer rollback(tx)

	var existingCapacity int
	err = tx.QueryRowContext(
		ctx,
		"SELECT capacity_slots FROM workers WHERE worker_id = ?",
		workerID,
	).Scan(&existingCapacity)
	switch {
	case err == nil && existingCapacity != capacitySlots:
		return storepkg.ErrWorkerCapacityConflict
	case err == nil:
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE workers
			 SET last_heartbeat_at = MAX(last_heartbeat_at, ?)
			 WHERE worker_id = ?`,
			timeToDB(now),
			workerID,
		); err != nil {
			return fmt.Errorf("refresh worker registration: %w", err)
		}
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO workers (
				worker_id, capacity_slots, registered_at, last_heartbeat_at
			) VALUES (?, ?, ?, ?)`,
			workerID,
			capacitySlots,
			timeToDB(now),
			timeToDB(now),
		); err != nil {
			return fmt.Errorf("insert worker registration: %w", err)
		}
	default:
		return fmt.Errorf("read worker registration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit worker registration: %w", err)
	}
	return nil
}

func (s *Store) HeartbeatWorker(ctx context.Context, workerID string, now time.Time) error {
	if workerID == "" {
		return errors.New("worker ID must not be empty")
	}

	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	result, err := s.db.ExecContext(
		ctx,
		`UPDATE workers
		 SET last_heartbeat_at = MAX(last_heartbeat_at, ?)
		 WHERE worker_id = ?`,
		timeToDB(now),
		workerID,
	)
	if err != nil {
		return fmt.Errorf("refresh worker heartbeat: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect worker heartbeat: %w", err)
	}
	if affected != 1 {
		return domain.ErrNotFound
	}
	return nil
}
