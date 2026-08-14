package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func (s *Store) SubmitJob(
	ctx context.Context,
	submission storepkg.Submission,
	now time.Time,
) (domain.Job, bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	spec := submission.Job.Normalize()
	if err := validateIdempotency(submission.IdempotencyKey, submission.RequestDigest); err != nil {
		return domain.Job{}, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("begin job submission: %w", err)
	}
	defer rollback(tx)

	response, found, err := readIdempotency(
		ctx,
		tx,
		spec.TenantID,
		submission.IdempotencyKey,
		"job",
		submission.RequestDigest,
	)
	if err != nil {
		return domain.Job{}, false, err
	}
	if found {
		var job domain.Job
		if err := json.Unmarshal([]byte(response), &job); err != nil {
			return domain.Job{}, false, fmt.Errorf("decode idempotent job response: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.Job{}, false, fmt.Errorf("commit duplicate job submission: %w", err)
		}
		return job, true, nil
	}

	if err := spec.Validate(s.maxSlotCost, s.allowShell); err != nil {
		return domain.Job{}, false, fmt.Errorf("validate job: %w", err)
	}
	dependencies, dependenciesSucceeded, dependencyFailed, err := validateExistingDependencies(
		ctx,
		tx,
		spec.TenantID,
		spec.DependsOn,
	)
	if err != nil {
		return domain.Job{}, false, err
	}
	if err := s.ensureAdmission(ctx, tx, spec.TenantID, 1); err != nil {
		return domain.Job{}, false, err
	}
	if err := ensureQueue(ctx, tx, spec.TenantID, spec.Queue, now); err != nil {
		return domain.Job{}, false, err
	}

	jobID, err := domain.NewID()
	if err != nil {
		return domain.Job{}, false, err
	}
	availableAt := spec.AvailableAt.UTC()
	if spec.AvailableAt.IsZero() {
		availableAt = now.UTC()
	}

	readySeq := int64(0)
	if dependenciesSucceeded && !availableAt.After(now) {
		readySeq, err = nextReadySeq(ctx, tx)
		if err != nil {
			return domain.Job{}, false, err
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
		return domain.Job{}, false, err
	}
	for _, dependencyID := range dependencies {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (?, ?)`,
			job.ID,
			dependencyID,
		); err != nil {
			return domain.Job{}, false, fmt.Errorf("insert job dependency: %w", err)
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
		struct {
			ReadySeq int64 `json:"ready_seq"`
		}{ReadySeq: job.ReadySeq},
	); err != nil {
		return domain.Job{}, false, err
	}
	if dependencyFailed {
		failureJSON, err := encodeOptionalFailure(job.Failure)
		if err != nil {
			return domain.Job{}, false, err
		}
		if err := insertCanonicalCompletion(
			ctx,
			tx,
			job.ID,
			job.State,
			job.StateVersion,
			0,
			"",
			failureJSON,
			now,
		); err != nil {
			return domain.Job{}, false, err
		}
		if err := insertDeadLetter(ctx, tx, job.ID, failureJSON, now); err != nil {
			return domain.Job{}, false, err
		}
	}

	encodedResponse, err := json.Marshal(job)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("encode job response: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO idempotency_requests
			(tenant_id, submission_key, request_kind, request_digest, job_id, response_json, created_at)
		 VALUES (?, ?, 'job', ?, ?, ?, ?)`,
		spec.TenantID,
		submission.IdempotencyKey,
		submission.RequestDigest,
		job.ID,
		string(encodedResponse),
		timeToDB(now),
	); err != nil {
		return domain.Job{}, false, fmt.Errorf("record job idempotency: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return domain.Job{}, false, fmt.Errorf("commit job submission: %w", err)
	}
	return job, false, nil
}

func (s *Store) SubmitWorkflow(
	ctx context.Context,
	submission storepkg.WorkflowSubmission,
	now time.Time,
) ([]domain.Job, bool, error) {
	releaseWrite := s.beginWrite(writeNormal)
	defer releaseWrite()
	now = s.writeTime(now)

	tenantID := submission.Request.TenantID
	if tenantID == "" {
		tenantID = "default"
	}
	if err := validateIdempotency(submission.IdempotencyKey, submission.RequestDigest); err != nil {
		return nil, false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin workflow submission: %w", err)
	}
	defer rollback(tx)

	response, found, err := readIdempotency(
		ctx,
		tx,
		tenantID,
		submission.IdempotencyKey,
		"workflow",
		submission.RequestDigest,
	)
	if err != nil {
		return nil, false, err
	}
	if found {
		var jobs []domain.Job
		if err := json.Unmarshal([]byte(response), &jobs); err != nil {
			return nil, false, fmt.Errorf("decode idempotent workflow response: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("commit duplicate workflow submission: %w", err)
		}
		return jobs, true, nil
	}

	specs, dependencyIndexes, err := validateWorkflow(
		submission,
		tenantID,
		s.maxSlotCost,
		s.allowShell,
	)
	if err != nil {
		return nil, false, err
	}
	if err := s.ensureAdmission(ctx, tx, tenantID, len(specs)); err != nil {
		return nil, false, err
	}

	ids := make([]string, len(specs))
	for index := range ids {
		ids[index], err = domain.NewID()
		if err != nil {
			return nil, false, err
		}
	}

	jobs := make([]domain.Job, len(specs))
	for index, spec := range specs {
		if err := ensureQueue(ctx, tx, tenantID, spec.Queue, now); err != nil {
			return nil, false, err
		}
		availableAt := spec.AvailableAt.UTC()
		if spec.AvailableAt.IsZero() {
			availableAt = now.UTC()
		}
		readySeq := int64(0)
		if len(dependencyIndexes[index]) == 0 && !availableAt.After(now) {
			readySeq, err = nextReadySeq(ctx, tx)
			if err != nil {
				return nil, false, err
			}
		}
		jobs[index] = newJob(ids[index], spec, availableAt, readySeq, now)
		if err := insertJob(ctx, tx, jobs[index]); err != nil {
			return nil, false, err
		}
	}

	for childIndex, parents := range dependencyIndexes {
		for _, parentIndex := range parents {
			if _, err := tx.ExecContext(
				ctx,
				`INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (?, ?)`,
				ids[childIndex],
				ids[parentIndex],
			); err != nil {
				return nil, false, fmt.Errorf("insert workflow dependency: %w", err)
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
			struct {
				ReadySeq int64 `json:"ready_seq"`
			}{ReadySeq: job.ReadySeq},
		); err != nil {
			return nil, false, err
		}
	}

	encodedResponse, err := json.Marshal(jobs)
	if err != nil {
		return nil, false, fmt.Errorf("encode workflow response: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO idempotency_requests
			(tenant_id, submission_key, request_kind, request_digest, response_json, created_at)
		 VALUES (?, ?, 'workflow', ?, ?, ?)`,
		tenantID,
		submission.IdempotencyKey,
		submission.RequestDigest,
		string(encodedResponse),
		timeToDB(now),
	); err != nil {
		return nil, false, fmt.Errorf("record workflow idempotency: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit workflow submission: %w", err)
	}
	return jobs, false, nil
}

func (s *Store) GetJob(ctx context.Context, jobID string) (domain.Job, error) {
	job, err := scanJob(s.db.QueryRowContext(
		ctx,
		"SELECT "+jobColumns+" FROM jobs WHERE id = ?",
		jobID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func validateIdempotency(key string, digest string) error {
	if strings.TrimSpace(key) == "" {
		return errors.New("idempotency key must not be empty")
	}
	if strings.TrimSpace(digest) == "" {
		return errors.New("request digest must not be empty")
	}
	return nil
}

func readIdempotency(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	key string,
	kind string,
	digest string,
) (string, bool, error) {
	var storedKind, storedDigest, response string
	err := tx.QueryRowContext(
		ctx,
		`SELECT request_kind, request_digest, response_json
		 FROM idempotency_requests
		 WHERE tenant_id = ? AND submission_key = ?`,
		tenantID,
		key,
	).Scan(&storedKind, &storedDigest, &response)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read idempotency request: %w", err)
	}
	if storedKind != kind || storedDigest != digest {
		return "", false, domain.ErrIdempotencyConflict
	}
	return response, true, nil
}

func validateExistingDependencies(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	dependencies []string,
) ([]string, bool, bool, error) {
	seen := make(map[string]struct{}, len(dependencies))
	result := make([]string, 0, len(dependencies))
	allSucceeded := true
	failed := false
	for _, dependencyID := range dependencies {
		if dependencyID == "" {
			return nil, false, false, errors.New("dependency ID must not be empty")
		}
		if _, duplicate := seen[dependencyID]; duplicate {
			return nil, false, false, fmt.Errorf("dependency %q is repeated", dependencyID)
		}
		seen[dependencyID] = struct{}{}

		var parentTenant string
		var parentState domain.JobState
		if err := tx.QueryRowContext(
			ctx,
			"SELECT tenant_id, state FROM jobs WHERE id = ?",
			dependencyID,
		).Scan(&parentTenant, &parentState); errors.Is(err, sql.ErrNoRows) {
			return nil, false, false, fmt.Errorf(
				"dependency %q: %w",
				dependencyID,
				domain.ErrNotFound,
			)
		} else if err != nil {
			return nil, false, false, fmt.Errorf("read dependency %q: %w", dependencyID, err)
		}
		if parentTenant != tenantID {
			return nil, false, false, fmt.Errorf(
				"dependency %q belongs to another tenant",
				dependencyID,
			)
		}
		allSucceeded = allSucceeded && parentState == domain.StateSucceeded
		failed = failed ||
			parentState == domain.StateFailed ||
			parentState == domain.StateDeadLetter
		result = append(result, dependencyID)
	}
	return result, allSucceeded, failed, nil
}

func validateWorkflow(
	submission storepkg.WorkflowSubmission,
	tenantID string,
	maxSlotCost int,
	allowShell bool,
) ([]domain.JobSpec, [][]int, error) {
	nodes := submission.Request.Nodes
	if len(nodes) == 0 {
		return nil, nil, errors.New("workflow must contain at least one node")
	}

	indexByKey := make(map[string]int, len(nodes))
	for index, node := range nodes {
		if strings.TrimSpace(node.Key) == "" {
			return nil, nil, errors.New("workflow node key must not be empty")
		}
		if _, exists := indexByKey[node.Key]; exists {
			return nil, nil, fmt.Errorf("workflow node key %q is repeated", node.Key)
		}
		indexByKey[node.Key] = index
	}

	specs := make([]domain.JobSpec, len(nodes))
	dependencies := make([][]int, len(nodes))
	children := make([][]int, len(nodes))
	indegree := make([]int, len(nodes))
	for childIndex, node := range nodes {
		spec := node.Job
		spec.TenantID = tenantID
		spec = spec.Normalize()
		if err := spec.Validate(maxSlotCost, allowShell); err != nil {
			return nil, nil, fmt.Errorf("validate workflow node %q: %w", node.Key, err)
		}
		specs[childIndex] = spec

		seen := make(map[int]struct{}, len(spec.DependsOn))
		for _, parentKey := range spec.DependsOn {
			parentIndex, exists := indexByKey[parentKey]
			if !exists {
				return nil, nil, fmt.Errorf(
					"workflow node %q depends on unknown node %q",
					node.Key,
					parentKey,
				)
			}
			if _, duplicate := seen[parentIndex]; duplicate {
				return nil, nil, fmt.Errorf(
					"workflow node %q repeats dependency %q",
					node.Key,
					parentKey,
				)
			}
			seen[parentIndex] = struct{}{}
			dependencies[childIndex] = append(dependencies[childIndex], parentIndex)
			children[parentIndex] = append(children[parentIndex], childIndex)
			indegree[childIndex]++
		}
	}

	queue := make([]int, 0, len(nodes))
	for index, degree := range indegree {
		if degree == 0 {
			queue = append(queue, index)
		}
	}
	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, child := range children[node] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if visited != len(nodes) {
		return nil, nil, domain.ErrCycleDetected
	}
	return specs, dependencies, nil
}

func (s *Store) ensureAdmission(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	incoming int,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO tenant_limits (tenant_id, max_depth, max_slots) VALUES (?, ?, ?)
		 ON CONFLICT (tenant_id) DO NOTHING`,
		tenantID,
		s.defaultTenantDepth,
		s.defaultTenantSlots,
	); err != nil {
		return fmt.Errorf("ensure tenant limits: %w", err)
	}

	var maxDepth, active int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT max_depth,
			(SELECT COUNT(*) FROM jobs
			 WHERE tenant_id = ?
			   AND state NOT IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTER'))
		 FROM tenant_limits
		 WHERE tenant_id = ?`,
		tenantID,
		tenantID,
	).Scan(&maxDepth, &active); err != nil {
		return fmt.Errorf("read tenant admission: %w", err)
	}
	if maxDepth > 0 && active+incoming > maxDepth {
		return domain.ErrQueueFull
	}
	return nil
}

func ensureQueue(
	ctx context.Context,
	tx *sql.Tx,
	tenantID string,
	queue string,
	now time.Time,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO queue_state (tenant_id, queue_name, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT (tenant_id, queue_name) DO NOTHING`,
		tenantID,
		queue,
		timeToDB(now),
	); err != nil {
		return fmt.Errorf("ensure queue state: %w", err)
	}
	return nil
}

func newJob(
	id string,
	spec domain.JobSpec,
	availableAt time.Time,
	readySeq int64,
	now time.Time,
) domain.Job {
	return domain.Job{
		ID:              id,
		TenantID:        spec.TenantID,
		Queue:           spec.Queue,
		Priority:        spec.Priority,
		SlotCost:        spec.SlotCost,
		Payload:         spec.Payload,
		Retry:           spec.Retry,
		State:           domain.StatePending,
		StateVersion:    1,
		AvailableAt:     availableAt.UTC(),
		ReadySeq:        readySeq,
		CreatedAt:       now.UTC(),
		UpdatedAt:       now.UTC(),
		AttemptNo:       0,
		LeaseGeneration: 0,
	}
}

func insertJob(ctx context.Context, tx *sql.Tx, job domain.Job) error {
	payloadJSON, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("encode job payload: %w", err)
	}
	retryJSON, err := json.Marshal(job.Retry)
	if err != nil {
		return fmt.Errorf("encode retry policy: %w", err)
	}
	failureJSON, err := encodeOptionalFailure(job.Failure)
	if err != nil {
		return err
	}
	var terminalAt any
	if job.TerminalAt != nil {
		terminalAt = timeToDB(*job.TerminalAt)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO jobs (
			id, tenant_id, queue_name, priority, slot_cost,
			payload_json, retry_json, state, attempt_no, state_version,
			lease_generation, available_at, ready_seq, execution_key,
			created_at, updated_at, terminal_at, failure_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID,
		job.TenantID,
		job.Queue,
		job.Priority,
		job.SlotCost,
		string(payloadJSON),
		string(retryJSON),
		job.State,
		job.AttemptNo,
		job.StateVersion,
		job.LeaseGeneration,
		timeToDB(job.AvailableAt),
		job.ReadySeq,
		job.ID,
		timeToDB(job.CreatedAt),
		timeToDB(job.UpdatedAt),
		terminalAt,
		failureJSON,
	); err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func timePointer(value time.Time) *time.Time {
	utc := value.UTC()
	return &utc
}
