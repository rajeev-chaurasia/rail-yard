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

// RegisterWorker records a worker process. A registration that carries a
// different non-empty incarnation than the stored one comes from a new
// process under the same worker ID, which proves the previous process is
// gone, so its leases are requeued now instead of after lease expiry. A
// repeat registration from the same process, such as after a control-plane
// restart, keeps its leases.
func (s *Store) RegisterWorker(
	ctx context.Context,
	workerID string,
	capacitySlots int,
	incarnation string,
	now time.Time,
) ([]domain.ReapedLease, error) {
	if workerID == "" {
		return nil, errors.New("worker ID must not be empty")
	}
	if capacitySlots < 1 {
		return nil, errors.New("worker capacity must be positive")
	}

	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin worker registration: %w", err)
	}
	defer rollback(tx)

	var existingCapacity int
	var existingIncarnation string
	var fenced []domain.ReapedLease
	err = tx.QueryRowContext(
		ctx,
		"SELECT capacity_slots, incarnation FROM workers WHERE worker_id = ?",
		workerID,
	).Scan(&existingCapacity, &existingIncarnation)
	switch {
	case err == nil && existingCapacity != capacitySlots:
		return nil, storepkg.ErrWorkerCapacityConflict
	case err == nil:
		if incarnation != "" && existingIncarnation != "" && incarnation != existingIncarnation {
			orphaned, err := selectLiveAttempts(
				ctx,
				tx,
				`a.worker_id = ? ORDER BY a.job_id ASC`,
				workerID,
			)
			if err != nil {
				return nil, err
			}
			fenced, err = reapAttempts(ctx, tx, orphaned, now, workerRestartedMessage)
			if err != nil {
				return nil, err
			}
		}
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE workers
			 SET last_heartbeat_at = MAX(last_heartbeat_at, ?),
			     incarnation = CASE WHEN ? = '' THEN incarnation ELSE ? END
			 WHERE worker_id = ?`,
			timeToDB(now),
			incarnation,
			incarnation,
			workerID,
		); err != nil {
			return nil, fmt.Errorf("refresh worker registration: %w", err)
		}
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO workers (
				worker_id, capacity_slots, registered_at, last_heartbeat_at, incarnation
			) VALUES (?, ?, ?, ?, ?)`,
			workerID,
			capacitySlots,
			timeToDB(now),
			timeToDB(now),
			incarnation,
		); err != nil {
			return nil, fmt.Errorf("insert worker registration: %w", err)
		}
	default:
		return nil, fmt.Errorf("read worker registration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit worker registration: %w", err)
	}
	if fenced == nil {
		fenced = []domain.ReapedLease{}
	}
	return fenced, nil
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
