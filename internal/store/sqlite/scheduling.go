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

	"github.com/rajeev-chaurasia/rail-yard/internal/decisionlog"
	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

type schedulePlan struct {
	input      scheduler.Snapshot
	decision   scheduler.Decision
	candidates map[string]leaseCandidate
}

func buildSchedulePlan(
	ctx context.Context,
	tx *sql.Tx,
	now time.Time,
	workerSlots int,
	limit int,
	recoveryReserve int,
) (schedulePlan, error) {
	rows, err := tx.QueryContext(
		ctx,
		`WITH ranked AS (
			SELECT
				j.id, j.tenant_id, j.queue_name, j.priority, j.slot_cost,
				j.payload_json, j.attempt_no, j.state_version,
				j.lease_generation, j.execution_key,
				q.weight, q.deficit, j.ready_seq, j.recovery_pending, j.updated_at,
				ROW_NUMBER() OVER (
					PARTITION BY j.tenant_id, j.queue_name
					ORDER BY j.recovery_pending DESC, j.priority DESC, j.ready_seq ASC, j.id ASC
				) AS candidate_rank
			FROM jobs j
			JOIN tenant_limits t ON t.tenant_id = j.tenant_id
			JOIN queue_state q
			  ON q.tenant_id = j.tenant_id AND q.queue_name = j.queue_name
			WHERE j.state = 'PENDING'
			  AND j.ready_seq > 0
			  AND j.available_at <= ?
			  AND (t.max_slots = 0 OR t.active_slots + j.slot_cost <= t.max_slots)
		)
		SELECT
			id, tenant_id, queue_name, priority, slot_cost,
			payload_json, attempt_no, state_version, lease_generation,
			execution_key, weight, deficit, ready_seq, recovery_pending, updated_at
		FROM ranked
		WHERE candidate_rank <= ?
		ORDER BY tenant_id ASC, queue_name ASC, priority DESC, ready_seq ASC, id ASC`,
		timeToDB(now),
		limit,
	)
	if err != nil {
		return schedulePlan{}, fmt.Errorf("select scheduler candidates: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	type queueKey struct {
		tenant string
		name   string
	}
	queues := make(map[queueKey]*scheduler.Queue)
	candidates := make(map[string]leaseCandidate)
	hasRecovery := false
	for rows.Next() {
		var candidate leaseCandidate
		var payloadJSON string
		var weight, deficit int
		var readySeq int64
		var recovery int
		var readyAt int64
		if err := rows.Scan(
			&candidate.jobID,
			&candidate.tenantID,
			&candidate.queue,
			&candidate.priority,
			&candidate.slotCost,
			&payloadJSON,
			&candidate.attemptNo,
			&candidate.stateVersion,
			&candidate.generation,
			&candidate.executionKey,
			&weight,
			&deficit,
			&readySeq,
			&recovery,
			&readyAt,
		); err != nil {
			return schedulePlan{}, fmt.Errorf("scan scheduler candidate: %w", err)
		}
		candidate.readyAt = timeFromDB(readyAt)
		if err := json.Unmarshal([]byte(payloadJSON), &candidate.payload); err != nil {
			return schedulePlan{}, fmt.Errorf("decode scheduler payload: %w", err)
		}
		key := queueKey{tenant: candidate.tenantID, name: candidate.queue}
		queue := queues[key]
		if queue == nil {
			queue = &scheduler.Queue{
				TenantID: candidate.tenantID,
				Name:     candidate.queue,
				Weight:   weight,
				Deficit:  deficit,
			}
			queues[key] = queue
		}
		queue.Candidates = append(queue.Candidates, scheduler.Candidate{
			JobID:    candidate.jobID,
			TenantID: candidate.tenantID,
			Queue:    candidate.queue,
			Priority: candidate.priority,
			ReadySeq: readySeq,
			SlotCost: candidate.slotCost,
			Recovery: recovery == 1,
		})
		hasRecovery = hasRecovery || recovery == 1
		candidates[candidate.jobID] = candidate
	}
	if err := rows.Err(); err != nil {
		return schedulePlan{}, fmt.Errorf("iterate scheduler candidates: %w", err)
	}
	if len(candidates) == 0 {
		return schedulePlan{}, nil
	}
	if !hasRecovery {
		workerSlots -= min(workerSlots, recoveryReserve)
	}
	if workerSlots == 0 {
		return schedulePlan{}, nil
	}

	queueValues := make([]scheduler.Queue, 0, len(queues))
	for _, queue := range queues {
		queueValues = append(queueValues, *queue)
	}
	sequence, err := nextCounter(ctx, tx, "scheduler_seq")
	if err != nil {
		return schedulePlan{}, err
	}
	cursor, err := readCounter(ctx, tx, "scheduler_cursor")
	if err != nil {
		return schedulePlan{}, err
	}
	configSum := sha256.Sum256([]byte(scheduler.AlgorithmVersion))
	input := scheduler.Snapshot{
		Sequence:      sequence,
		LogicalTimeNS: now.UTC().UnixNano(),
		WorkerSlots:   workerSlots,
		BatchLimit:    limit,
		Cursor:        int(cursor),
		Queues:        queueValues,
		ConfigHash:    hex.EncodeToString(configSum[:]),
		Algorithm:     scheduler.AlgorithmVersion,
	}
	decision, err := scheduler.Decide(input)
	if err != nil {
		return schedulePlan{}, err
	}
	return schedulePlan{input: input, decision: decision, candidates: candidates}, nil
}

func persistSchedulePlan(ctx context.Context, tx *sql.Tx, plan schedulePlan, now time.Time) error {
	for _, queue := range plan.decision.Queues {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE queue_state
			 SET deficit = ?, updated_at = ?
			 WHERE tenant_id = ? AND queue_name = ?`,
			queue.Deficit,
			timeToDB(now),
			queue.TenantID,
			queue.Queue,
		); err != nil {
			return fmt.Errorf("update scheduler queue state: %w", err)
		}
	}
	if err := setCounter(ctx, tx, "scheduler_cursor", int64(plan.decision.NextCursor)); err != nil {
		return err
	}

	previousHash := ""
	err := tx.QueryRowContext(
		ctx,
		"SELECT hash FROM decision_log ORDER BY sequence DESC LIMIT 1",
	).Scan(&previousHash)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read decision hash head: %w", err)
	}
	record, err := decisionlog.NewRecord(previousHash, plan.input, plan.decision)
	if err != nil {
		return err
	}
	recordJSON, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode decision record: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO decision_log
			(sequence, previous_hash, hash, record_json, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		record.Sequence,
		record.PreviousHash,
		record.Hash,
		string(recordJSON),
		timeToDB(now),
	); err != nil {
		return fmt.Errorf("insert decision record: %w", err)
	}
	return nil
}

func nextCounter(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	if _, err := tx.ExecContext(
		ctx,
		"UPDATE counters SET value = value + 1 WHERE name = ?",
		name,
	); err != nil {
		return 0, fmt.Errorf("advance %s counter: %w", name, err)
	}
	return readCounter(ctx, tx, name)
}

func readCounter(ctx context.Context, tx *sql.Tx, name string) (int64, error) {
	var value int64
	if err := tx.QueryRowContext(
		ctx,
		"SELECT value FROM counters WHERE name = ?",
		name,
	).Scan(&value); err != nil {
		return 0, fmt.Errorf("read %s counter: %w", name, err)
	}
	return value, nil
}

func setCounter(ctx context.Context, tx *sql.Tx, name string, value int64) error {
	result, err := tx.ExecContext(
		ctx,
		"UPDATE counters SET value = ? WHERE name = ?",
		value,
		name,
	)
	if err != nil {
		return fmt.Errorf("set %s counter: %w", name, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s counter update: %w", name, err)
	}
	if affected != 1 {
		return fmt.Errorf("%s counter is missing", name)
	}
	return nil
}
