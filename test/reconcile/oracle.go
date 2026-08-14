package reconcile

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const reportVersion = 1

var terminalStates = map[string]bool{
	"SUCCEEDED":   true,
	"FAILED":      true,
	"DEAD_LETTER": true,
}

var activeAttemptStates = map[string]bool{
	"LEASED":  true,
	"RUNNING": true,
}

// AcceptedRecord is one durable submission receipt from a chaos campaign.
type AcceptedRecord struct {
	Sequence      int       `json:"sequence"`
	SubmissionKey string    `json:"submission_key"`
	JobID         string    `json:"job_id"`
	TenantID      string    `json:"tenant_id"`
	AcceptedAt    time.Time `json:"accepted_at"`
	Duplicate     bool      `json:"duplicate"`
}

type Options struct {
	ExpectedJobs int
	MaxDetails   int
}

type Check struct {
	Name       string `json:"name"`
	Passed     bool   `json:"passed"`
	Violations int    `json:"violations"`
}

type Violation struct {
	Check   string `json:"check"`
	Message string `json:"message"`
	JobID   string `json:"job_id,omitempty"`
}

type Counts struct {
	Accepted         int            `json:"accepted"`
	Jobs             int            `json:"jobs"`
	Completions      int            `json:"completions"`
	Attempts         int            `json:"attempts"`
	AttemptRepeats   int            `json:"attempt_repeats"`
	Events           int            `json:"events"`
	IdempotencyRows  int            `json:"idempotency_rows"`
	ActiveJobs       int            `json:"active_jobs"`
	ActiveAttempts   int            `json:"active_attempts"`
	SlotReservations int            `json:"slot_reservations"`
	StateCounts      map[string]int `json:"state_counts"`
}

type Report struct {
	Version          int         `json:"version"`
	GeneratedAt      time.Time   `json:"generated_at"`
	Passed           bool        `json:"passed"`
	ExpectedJobs     int         `json:"expected_jobs"`
	Counts           Counts      `json:"counts"`
	Checks           []Check     `json:"checks"`
	ViolationCount   int         `json:"violation_count"`
	Violations       []Violation `json:"violations"`
	DetailsTruncated bool        `json:"details_truncated"`
	NotObservable    []string    `json:"not_observable"`
}

type snapshot struct {
	integrity     []string
	foreignKeys   []string
	journalMode   string
	synchronous   int
	foreignKeysOn int
	busyTimeout   int
	jobs          []jobRow
	completions   []completionRow
	attempts      []attemptRow
	events        []eventRow
	idempotency   []idempotencyRow
	dependencies  []dependencyRow
	tenants       []tenantRow
	queues        []queueRow
	counters      []counterRow
	migrations    []migrationRow
}

type jobRow struct {
	ID              string
	TenantID        string
	Queue           string
	SlotCost        int
	PayloadJSON     string
	RetryJSON       string
	State           string
	AttemptNo       int
	StateVersion    int64
	LeaseGeneration int64
	AvailableAt     int64
	ReadySeq        int64
	ExecutionKey    string
	CreatedAt       int64
	UpdatedAt       int64
	TerminalAt      sql.NullInt64
	FailureJSON     sql.NullString
}

type completionRow struct {
	JobID        string
	State        string
	StateVersion int64
	AttemptNo    int
	OutputDigest string
	FailureJSON  sql.NullString
	CommittedAt  int64
}

type attemptRow struct {
	JobID                   string
	AttemptNo               int
	WorkerID                string
	LeaseGeneration         int64
	TokenHash               string
	State                   string
	LeasedAt                int64
	HeartbeatAt             int64
	ExpiresAt               int64
	StartedAt               sql.NullInt64
	CompletedAt             sql.NullInt64
	FailureJSON             sql.NullString
	CompletionRequestDigest sql.NullString
	ReceiptState            sql.NullString
	ReceiptStateVersion     sql.NullInt64
	ReceiptCommittedAt      sql.NullInt64
}

type eventRow struct {
	Sequence     int64
	JobID        string
	Type         string
	State        string
	StateVersion int64
	OccurredAt   int64
	PayloadJSON  string
}

type idempotencyRow struct {
	TenantID      string
	SubmissionKey string
	RequestKind   string
	RequestDigest string
	JobID         sql.NullString
	ResponseJSON  string
	CreatedAt     int64
}

type dependencyRow struct {
	JobID     string
	DependsOn string
}

type tenantRow struct {
	TenantID    string
	MaxDepth    int
	MaxSlots    int
	ActiveSlots int
}

type queueRow struct {
	TenantID    string
	Queue       string
	Weight      int
	Deficit     int
	ActiveSlots int
}

type counterRow struct {
	Name  string
	Value int64
}

type migrationRow struct {
	Version   int
	Name      string
	Checksum  string
	AppliedAt int64
}

type retryPolicy struct {
	MaxAttempts int  `json:"max_attempts"`
	Retryable   bool `json:"retryable"`
}

type payload struct {
	Type       string   `json:"type"`
	DurationMS int64    `json:"duration_ms"`
	Args       []string `json:"args"`
}

type collector struct {
	maxDetails int
	total      int
	checks     map[string]int
	details    []Violation
}

func ReadManifest(reader io.Reader) ([]AcceptedRecord, error) {
	if reader == nil {
		return nil, errors.New("manifest reader is nil")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	records := make([]AcceptedRecord, 0)
	for line := 1; scanner.Scan(); line++ {
		body := strings.TrimSpace(scanner.Text())
		if body == "" {
			return nil, fmt.Errorf("manifest line %d is empty", line)
		}
		var record AcceptedRecord
		decoder := json.NewDecoder(strings.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("decode manifest line %d: %w", line, err)
		}
		if err := ensureJSONEOF(decoder); err != nil {
			return nil, fmt.Errorf("decode manifest line %d: %w", line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	return records, nil
}

func OpenReadOnly(path string) (*sql.DB, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	slashPath := filepath.ToSlash(absolute)
	if volume := filepath.VolumeName(absolute); volume != "" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	uri := url.URL{Scheme: "file", Path: slashPath}
	query := uri.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "busy_timeout(5000)")
	uri.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping SQLite database: %w", err)
	}
	return db, nil
}

func Reconcile(
	ctx context.Context,
	db *sql.DB,
	accepted []AcceptedRecord,
	options Options,
) (Report, error) {
	if db == nil {
		return Report{}, errors.New("database is nil")
	}
	if options.ExpectedJobs == 0 {
		options.ExpectedJobs = len(accepted)
	}
	if options.ExpectedJobs < 1 {
		return Report{}, errors.New("expected jobs must be positive")
	}
	if options.MaxDetails == 0 {
		options.MaxDetails = 1000
	}
	if options.MaxDetails < 1 {
		return Report{}, errors.New("maximum details must be positive")
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Report{}, fmt.Errorf("begin reconciliation snapshot: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	data, err := readSnapshot(ctx, tx)
	if err != nil {
		return Report{}, err
	}
	if err := tx.Commit(); err != nil {
		return Report{}, fmt.Errorf("commit reconciliation snapshot: %w", err)
	}
	return analyze(data, accepted, options), nil
}

func readSnapshot(ctx context.Context, tx *sql.Tx) (snapshot, error) {
	var result snapshot
	var err error
	if result.integrity, err = readSingleColumn(ctx, tx, "PRAGMA integrity_check"); err != nil {
		return result, fmt.Errorf("run integrity_check: %w", err)
	}
	result.integrity = removeValue(result.integrity, "ok")
	if result.foreignKeys, err = readForeignKeys(ctx, tx); err != nil {
		return result, fmt.Errorf("run foreign_key_check: %w", err)
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&result.journalMode); err != nil {
		return result, fmt.Errorf("read journal_mode: %w", err)
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&result.synchronous); err != nil {
		return result, fmt.Errorf("read synchronous: %w", err)
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&result.foreignKeysOn); err != nil {
		return result, fmt.Errorf("read foreign_keys: %w", err)
	}
	if err := tx.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&result.busyTimeout); err != nil {
		return result, fmt.Errorf("read busy_timeout: %w", err)
	}
	if result.jobs, err = readJobs(ctx, tx); err != nil {
		return result, err
	}
	if result.completions, err = readCompletions(ctx, tx); err != nil {
		return result, err
	}
	if result.attempts, err = readAttempts(ctx, tx); err != nil {
		return result, err
	}
	if result.events, err = readEvents(ctx, tx); err != nil {
		return result, err
	}
	if result.idempotency, err = readIdempotency(ctx, tx); err != nil {
		return result, err
	}
	if result.dependencies, err = readDependencies(ctx, tx); err != nil {
		return result, err
	}
	if result.tenants, err = readTenants(ctx, tx); err != nil {
		return result, err
	}
	if result.queues, err = readQueues(ctx, tx); err != nil {
		return result, err
	}
	if result.counters, err = readCounters(ctx, tx); err != nil {
		return result, err
	}
	if result.migrations, err = readMigrations(ctx, tx); err != nil {
		return result, err
	}
	return result, nil
}

func readSingleColumn(ctx context.Context, tx *sql.Tx, query string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func readForeignKeys(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	var failures []string
	for rows.Next() {
		var table, parent string
		var rowID, foreignKeyID sql.NullInt64
		if err := rows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			return nil, err
		}
		failures = append(failures, fmt.Sprintf(
			"table=%s rowid=%s parent=%s fkid=%s",
			table,
			nullIntString(rowID),
			parent,
			nullIntString(foreignKeyID),
		))
	}
	return failures, rows.Err()
}

func readJobs(ctx context.Context, tx *sql.Tx) ([]jobRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, tenant_id, queue_name, slot_cost, payload_json, retry_json,
		       state, attempt_no, state_version, lease_generation, available_at,
		       ready_seq, execution_key, created_at, updated_at, terminal_at, failure_json
		FROM jobs
		ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []jobRow
	for rows.Next() {
		var row jobRow
		if err := rows.Scan(
			&row.ID, &row.TenantID, &row.Queue, &row.SlotCost, &row.PayloadJSON,
			&row.RetryJSON, &row.State, &row.AttemptNo, &row.StateVersion,
			&row.LeaseGeneration, &row.AvailableAt, &row.ReadySeq, &row.ExecutionKey,
			&row.CreatedAt, &row.UpdatedAt, &row.TerminalAt, &row.FailureJSON,
		); err != nil {
			return nil, fmt.Errorf("scan jobs: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return result, nil
}

func readCompletions(ctx context.Context, tx *sql.Tx) ([]completionRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT job_id, state, state_version, attempt_no, output_digest, failure_json, committed_at
		FROM job_completions
		ORDER BY job_id`)
	if err != nil {
		return nil, fmt.Errorf("query job_completions: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []completionRow
	for rows.Next() {
		var row completionRow
		if err := rows.Scan(
			&row.JobID, &row.State, &row.StateVersion, &row.AttemptNo,
			&row.OutputDigest, &row.FailureJSON, &row.CommittedAt,
		); err != nil {
			return nil, fmt.Errorf("scan job_completions: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job_completions: %w", err)
	}
	return result, nil
}

func readAttempts(ctx context.Context, tx *sql.Tx) ([]attemptRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT job_id, attempt_no, worker_id, lease_generation, token_hash, state,
		       leased_at, heartbeat_at, expires_at, started_at, completed_at,
		       failure_json, completion_request_digest, receipt_state,
		       receipt_state_version, receipt_committed_at
		FROM attempts
		ORDER BY job_id, attempt_no`)
	if err != nil {
		return nil, fmt.Errorf("query attempts: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []attemptRow
	for rows.Next() {
		var row attemptRow
		if err := rows.Scan(
			&row.JobID, &row.AttemptNo, &row.WorkerID, &row.LeaseGeneration,
			&row.TokenHash, &row.State, &row.LeasedAt, &row.HeartbeatAt,
			&row.ExpiresAt, &row.StartedAt, &row.CompletedAt, &row.FailureJSON,
			&row.CompletionRequestDigest, &row.ReceiptState,
			&row.ReceiptStateVersion, &row.ReceiptCommittedAt,
		); err != nil {
			return nil, fmt.Errorf("scan attempts: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate attempts: %w", err)
	}
	return result, nil
}

func readEvents(ctx context.Context, tx *sql.Tx) ([]eventRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT event_seq, job_id, event_type, state, state_version, occurred_at, payload_json
		FROM events
		ORDER BY event_seq`)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []eventRow
	for rows.Next() {
		var row eventRow
		if err := rows.Scan(
			&row.Sequence, &row.JobID, &row.Type, &row.State,
			&row.StateVersion, &row.OccurredAt, &row.PayloadJSON,
		); err != nil {
			return nil, fmt.Errorf("scan events: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return result, nil
}

func readIdempotency(ctx context.Context, tx *sql.Tx) ([]idempotencyRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT tenant_id, submission_key, request_kind, request_digest,
		       job_id, response_json, created_at
		FROM idempotency_requests
		ORDER BY tenant_id, submission_key`)
	if err != nil {
		return nil, fmt.Errorf("query idempotency_requests: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []idempotencyRow
	for rows.Next() {
		var row idempotencyRow
		if err := rows.Scan(
			&row.TenantID, &row.SubmissionKey, &row.RequestKind, &row.RequestDigest,
			&row.JobID, &row.ResponseJSON, &row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan idempotency_requests: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate idempotency_requests: %w", err)
	}
	return result, nil
}

func readDependencies(ctx context.Context, tx *sql.Tx) ([]dependencyRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT job_id, depends_on_id
		FROM job_dependencies
		ORDER BY job_id, depends_on_id`)
	if err != nil {
		return nil, fmt.Errorf("query job_dependencies: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []dependencyRow
	for rows.Next() {
		var row dependencyRow
		if err := rows.Scan(&row.JobID, &row.DependsOn); err != nil {
			return nil, fmt.Errorf("scan job_dependencies: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job_dependencies: %w", err)
	}
	return result, nil
}

func readTenants(ctx context.Context, tx *sql.Tx) ([]tenantRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT tenant_id, max_depth, max_slots, active_slots
		FROM tenant_limits
		ORDER BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("query tenant_limits: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []tenantRow
	for rows.Next() {
		var row tenantRow
		if err := rows.Scan(&row.TenantID, &row.MaxDepth, &row.MaxSlots, &row.ActiveSlots); err != nil {
			return nil, fmt.Errorf("scan tenant_limits: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant_limits: %w", err)
	}
	return result, nil
}

func readQueues(ctx context.Context, tx *sql.Tx) ([]queueRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT tenant_id, queue_name, weight, deficit, active_slots
		FROM queue_state
		ORDER BY tenant_id, queue_name`)
	if err != nil {
		return nil, fmt.Errorf("query queue_state: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []queueRow
	for rows.Next() {
		var row queueRow
		if err := rows.Scan(
			&row.TenantID, &row.Queue, &row.Weight, &row.Deficit, &row.ActiveSlots,
		); err != nil {
			return nil, fmt.Errorf("scan queue_state: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queue_state: %w", err)
	}
	return result, nil
}

func readCounters(ctx context.Context, tx *sql.Tx) ([]counterRow, error) {
	rows, err := tx.QueryContext(ctx, "SELECT name, value FROM counters ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("query counters: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []counterRow
	for rows.Next() {
		var row counterRow
		if err := rows.Scan(&row.Name, &row.Value); err != nil {
			return nil, fmt.Errorf("scan counters: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate counters: %w", err)
	}
	return result, nil
}

func readMigrations(ctx context.Context, tx *sql.Tx) ([]migrationRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT version, name, checksum, applied_at
		FROM schema_migrations
		ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("query schema_migrations: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()
	var result []migrationRow
	for rows.Next() {
		var row migrationRow
		if err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.AppliedAt); err != nil {
			return nil, fmt.Errorf("scan schema_migrations: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema_migrations: %w", err)
	}
	return result, nil
}

func analyze(data snapshot, accepted []AcceptedRecord, options Options) Report {
	checkNames := []string{
		"sqlite_integrity",
		"sqlite_settings",
		"foreign_keys",
		"manifest",
		"canonical_ledger",
		"materialized_jobs",
		"idempotency",
		"attempts_and_leases",
		"event_log",
		"dag",
		"slot_accounting",
		"schema_migrations",
	}
	c := &collector{
		maxDetails: options.MaxDetails,
		checks:     make(map[string]int, len(checkNames)),
	}
	for _, name := range checkNames {
		c.checks[name] = 0
	}
	for _, failure := range data.integrity {
		c.add("sqlite_integrity", "", failure)
	}
	for _, failure := range data.foreignKeys {
		c.add("foreign_keys", "", failure)
	}
	if !strings.EqualFold(data.journalMode, "wal") {
		c.add("sqlite_settings", "", fmt.Sprintf(
			"journal_mode=%s want=WAL", data.journalMode,
		))
	}
	if data.synchronous != 2 {
		c.add("sqlite_settings", "", fmt.Sprintf(
			"synchronous=%d want=2 (FULL)", data.synchronous,
		))
	}
	if data.foreignKeysOn != 1 {
		c.add("sqlite_settings", "", "foreign_keys is not enabled")
	}
	if data.busyTimeout != 5000 {
		c.add("sqlite_settings", "", fmt.Sprintf(
			"busy_timeout=%d want=5000", data.busyTimeout,
		))
	}

	acceptedByID, acceptedByKey := analyzeManifest(c, accepted, options.ExpectedJobs)
	jobs := analyzeJobs(c, data.jobs, acceptedByID)
	completions := analyzeCompletions(c, data.completions, acceptedByID, jobs)
	analyzeIdempotency(c, data.idempotency, acceptedByKey, acceptedByID)
	attemptRepeats, activeAttempts := analyzeAttempts(c, data.attempts, jobs, completions)
	analyzeEvents(c, data.events, jobs)
	analyzeDAG(c, data.dependencies, jobs, data.events)
	slotReservations := analyzeSlots(c, data.tenants, data.queues, jobs)
	analyzeCounters(c, data.counters, jobs)
	analyzeMigrations(c, data.migrations)

	stateCounts := make(map[string]int)
	activeJobs := 0
	for _, job := range data.jobs {
		stateCounts[job.State]++
		if !terminalStates[job.State] {
			activeJobs++
		}
	}

	checks := make([]Check, 0, len(c.checks))
	for name, count := range c.checks {
		checks = append(checks, Check{Name: name, Passed: count == 0, Violations: count})
	}
	sort.Slice(checks, func(left, right int) bool {
		return checks[left].Name < checks[right].Name
	})
	sort.Slice(c.details, func(left, right int) bool {
		if c.details[left].Check != c.details[right].Check {
			return c.details[left].Check < c.details[right].Check
		}
		if c.details[left].JobID != c.details[right].JobID {
			return c.details[left].JobID < c.details[right].JobID
		}
		return c.details[left].Message < c.details[right].Message
	})

	return Report{
		Version:          reportVersion,
		GeneratedAt:      time.Now().UTC(),
		Passed:           c.total == 0,
		ExpectedJobs:     options.ExpectedJobs,
		Checks:           checks,
		ViolationCount:   c.total,
		Violations:       c.details,
		DetailsTruncated: c.total > len(c.details),
		Counts: Counts{
			Accepted:         len(acceptedByID),
			Jobs:             len(data.jobs),
			Completions:      len(data.completions),
			Attempts:         len(data.attempts),
			AttemptRepeats:   attemptRepeats,
			Events:           len(data.events),
			IdempotencyRows:  len(data.idempotency),
			ActiveJobs:       activeJobs,
			ActiveAttempts:   activeAttempts,
			SlotReservations: slotReservations,
			StateCounts:      stateCounts,
		},
		NotObservable: []string{
			"historical scheduler ordering and capacity peaks require decision records",
			"stale request rejection and duplicate HTTP receipt semantics require protocol traces",
			"graph immutability requires historical snapshots",
			"Redis acknowledgement ordering requires transport traces",
			"payload side effects require a cooperative idempotency target",
		},
	}
}

func analyzeManifest(
	c *collector,
	accepted []AcceptedRecord,
	expected int,
) (map[string]AcceptedRecord, map[string]AcceptedRecord) {
	if len(accepted) != expected {
		c.add("manifest", "", fmt.Sprintf("accepted records=%d want=%d", len(accepted), expected))
	}
	byID := make(map[string]AcceptedRecord, len(accepted))
	byKey := make(map[string]AcceptedRecord, len(accepted))
	for index, record := range accepted {
		if record.Sequence != index+1 {
			c.add("manifest", record.JobID, fmt.Sprintf(
				"sequence=%d want=%d", record.Sequence, index+1,
			))
		}
		if !validID(record.JobID) {
			c.add("manifest", record.JobID, "job ID is not 32 lowercase hexadecimal characters")
		}
		if record.TenantID == "" {
			c.add("manifest", record.JobID, "tenant ID is empty")
		}
		if record.SubmissionKey == "" {
			c.add("manifest", record.JobID, "submission key is empty")
		}
		if record.AcceptedAt.IsZero() {
			c.add("manifest", record.JobID, "accepted timestamp is zero")
		}
		if _, exists := byID[record.JobID]; exists {
			c.add("manifest", record.JobID, "duplicate accepted job ID")
		}
		if _, exists := byKey[manifestKey(record.TenantID, record.SubmissionKey)]; exists {
			c.add("manifest", record.JobID, "duplicate tenant and submission key")
		}
		byID[record.JobID] = record
		byKey[manifestKey(record.TenantID, record.SubmissionKey)] = record
	}
	return byID, byKey
}

func analyzeJobs(
	c *collector,
	rows []jobRow,
	accepted map[string]AcceptedRecord,
) map[string]jobRow {
	jobs := make(map[string]jobRow, len(rows))
	for _, job := range rows {
		if _, exists := jobs[job.ID]; exists {
			c.add("materialized_jobs", job.ID, "duplicate jobs row")
		}
		jobs[job.ID] = job
		if _, exists := accepted[job.ID]; !exists {
			c.add("canonical_ledger", job.ID, "database job is absent from accepted manifest")
		}
		if !validID(job.ID) {
			c.add("materialized_jobs", job.ID, "job ID is malformed")
		}
		if job.ExecutionKey == "" {
			c.add("materialized_jobs", job.ID, "execution key is empty")
		}
		if job.StateVersion < 1 || job.AttemptNo < 0 || job.LeaseGeneration < 0 {
			c.add("materialized_jobs", job.ID, "negative counter or nonpositive state version")
		}
		if job.CreatedAt <= 0 || job.UpdatedAt < job.CreatedAt {
			c.add("materialized_jobs", job.ID, "job timestamps are not ordered")
		}
		if terminalStates[job.State] {
			if !job.TerminalAt.Valid || job.TerminalAt.Int64 < job.CreatedAt ||
				job.TerminalAt.Int64 > job.UpdatedAt {
				c.add("materialized_jobs", job.ID, "terminal timestamp is missing or unordered")
			}
		} else if job.TerminalAt.Valid {
			c.add("materialized_jobs", job.ID, "nonterminal job has terminal timestamp")
		}
		if job.State != "PENDING" && job.ReadySeq != 0 {
			c.add("materialized_jobs", job.ID, "non-pending job has a ready sequence")
		}
		if job.SlotCost < 1 {
			c.add("materialized_jobs", job.ID, "slot cost is not positive")
		}
		var body payload
		if err := json.Unmarshal([]byte(job.PayloadJSON), &body); err != nil {
			c.add("materialized_jobs", job.ID, "payload JSON is invalid")
		} else if body.Type != "noop" && body.Type != "shell" {
			c.add("materialized_jobs", job.ID, "payload type is invalid")
		}
		var retry retryPolicy
		if err := json.Unmarshal([]byte(job.RetryJSON), &retry); err != nil {
			c.add("materialized_jobs", job.ID, "retry JSON is invalid")
		} else if retry.MaxAttempts < 1 {
			c.add("materialized_jobs", job.ID, "retry maximum is not positive")
		}
		if job.FailureJSON.Valid && !json.Valid([]byte(job.FailureJSON.String)) {
			c.add("materialized_jobs", job.ID, "failure JSON is invalid")
		}
	}
	for jobID := range accepted {
		if _, exists := jobs[jobID]; !exists {
			c.add("canonical_ledger", jobID, "accepted job has no jobs row")
		}
	}
	return jobs
}

func analyzeCompletions(
	c *collector,
	rows []completionRow,
	accepted map[string]AcceptedRecord,
	jobs map[string]jobRow,
) map[string]completionRow {
	completions := make(map[string]completionRow, len(rows))
	for _, completion := range rows {
		if _, exists := completions[completion.JobID]; exists {
			c.add("canonical_ledger", completion.JobID, "duplicate canonical completion")
		}
		completions[completion.JobID] = completion
		if _, exists := accepted[completion.JobID]; !exists {
			c.add("canonical_ledger", completion.JobID, "orphan canonical completion")
		}
		job, exists := jobs[completion.JobID]
		if !exists {
			c.add("canonical_ledger", completion.JobID, "completion has no jobs row")
			continue
		}
		if completion.State != job.State ||
			completion.StateVersion != job.StateVersion ||
			completion.AttemptNo != job.AttemptNo {
			c.add("canonical_ledger", completion.JobID, "completion disagrees with materialized job")
		}
		if completion.CommittedAt <= 0 ||
			(job.TerminalAt.Valid && completion.CommittedAt != job.TerminalAt.Int64) {
			c.add("canonical_ledger", completion.JobID, "completion commit timestamp is inconsistent")
		}
		if completion.State != "SUCCEEDED" {
			c.add("canonical_ledger", completion.JobID, "canonical outcome is "+completion.State)
		}
		if completion.AttemptNo > 0 && !validDigest(completion.OutputDigest) {
			c.add("canonical_ledger", completion.JobID, "attempt completion output digest is invalid")
		}
		if completion.FailureJSON.Valid && !json.Valid([]byte(completion.FailureJSON.String)) {
			c.add("canonical_ledger", completion.JobID, "completion failure JSON is invalid")
		}
	}
	for jobID := range accepted {
		if _, exists := completions[jobID]; !exists {
			c.add("canonical_ledger", jobID, "accepted job has no canonical completion")
		}
	}
	for jobID, job := range jobs {
		_, completed := completions[jobID]
		if terminalStates[job.State] != completed {
			c.add("canonical_ledger", jobID, "terminal materialization and completion presence disagree")
		}
	}
	return completions
}

func analyzeIdempotency(
	c *collector,
	rows []idempotencyRow,
	acceptedByKey map[string]AcceptedRecord,
	acceptedByID map[string]AcceptedRecord,
) {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := manifestKey(row.TenantID, row.SubmissionKey)
		if _, exists := seen[key]; exists {
			c.add("idempotency", valueOrEmpty(row.JobID), "duplicate idempotency row")
		}
		seen[key] = struct{}{}
		record, exists := acceptedByKey[key]
		if !exists {
			c.add("idempotency", valueOrEmpty(row.JobID), "idempotency row is absent from manifest")
		} else if !row.JobID.Valid || row.JobID.String != record.JobID {
			c.add("idempotency", record.JobID, "idempotency row points to another job")
		}
		if row.RequestKind != "job" || row.RequestDigest == "" || row.CreatedAt <= 0 {
			c.add("idempotency", valueOrEmpty(row.JobID), "idempotency row metadata is invalid")
		}
		var response struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(row.ResponseJSON), &response); err != nil {
			c.add("idempotency", valueOrEmpty(row.JobID), "stable response JSON is invalid")
		} else if !row.JobID.Valid || response.ID != row.JobID.String {
			c.add("idempotency", valueOrEmpty(row.JobID), "stable response job ID is inconsistent")
		}
		if row.JobID.Valid {
			if _, exists := acceptedByID[row.JobID.String]; !exists {
				c.add("idempotency", row.JobID.String, "idempotency job is absent from manifest")
			}
		}
	}
	for key, record := range acceptedByKey {
		if _, exists := seen[key]; !exists {
			c.add("idempotency", record.JobID, "accepted submission has no idempotency row")
		}
	}
}

func analyzeAttempts(
	c *collector,
	rows []attemptRow,
	jobs map[string]jobRow,
	completions map[string]completionRow,
) (int, int) {
	byJob := make(map[string][]attemptRow)
	activeCount := 0
	for _, attempt := range rows {
		byJob[attempt.JobID] = append(byJob[attempt.JobID], attempt)
		if activeAttemptStates[attempt.State] {
			activeCount++
		}
		if _, exists := jobs[attempt.JobID]; !exists {
			c.add("attempts_and_leases", attempt.JobID, "attempt has no jobs row")
		}
		if attempt.WorkerID == "" || !validDigest(attempt.TokenHash) {
			c.add("attempts_and_leases", attempt.JobID, "attempt worker or token hash is invalid")
		}
		if attempt.AttemptNo < 1 || attempt.LeaseGeneration < 1 {
			c.add("attempts_and_leases", attempt.JobID, "attempt number or generation is not positive")
		}
		if attempt.LeasedAt <= 0 ||
			attempt.HeartbeatAt < attempt.LeasedAt ||
			attempt.ExpiresAt < attempt.HeartbeatAt {
			c.add("attempts_and_leases", attempt.JobID, "lease timestamps are not ordered")
		}
		if attempt.StartedAt.Valid && attempt.StartedAt.Int64 < attempt.LeasedAt {
			c.add("attempts_and_leases", attempt.JobID, "attempt started before it was leased")
		}
		if activeAttemptStates[attempt.State] {
			if attempt.CompletedAt.Valid {
				c.add("attempts_and_leases", attempt.JobID, "active attempt has completion timestamp")
			}
		} else if !attempt.CompletedAt.Valid || attempt.CompletedAt.Int64 < attempt.LeasedAt {
			c.add("attempts_and_leases", attempt.JobID, "closed attempt has invalid completion timestamp")
		}
		receiptFields := 0
		if attempt.CompletionRequestDigest.Valid {
			receiptFields++
			if !validDigest(attempt.CompletionRequestDigest.String) {
				c.add("attempts_and_leases", attempt.JobID, "completion request digest is invalid")
			}
		}
		if attempt.ReceiptState.Valid {
			receiptFields++
		}
		if attempt.ReceiptStateVersion.Valid {
			receiptFields++
		}
		if attempt.ReceiptCommittedAt.Valid {
			receiptFields++
		}
		if receiptFields != 0 && receiptFields != 4 {
			c.add("attempts_and_leases", attempt.JobID, "completion receipt fields are partially populated")
		}
		if attempt.State == "EXPIRED" && receiptFields != 0 {
			c.add("attempts_and_leases", attempt.JobID, "expired attempt has a completion receipt")
		}
		if attempt.FailureJSON.Valid && !json.Valid([]byte(attempt.FailureJSON.String)) {
			c.add("attempts_and_leases", attempt.JobID, "attempt failure JSON is invalid")
		}
	}

	repeats := 0
	for jobID, attempts := range byJob {
		if len(attempts) > 1 {
			repeats += len(attempts) - 1
		}
		active := 0
		var previousGeneration int64
		for index, attempt := range attempts {
			if attempt.AttemptNo != index+1 {
				c.add("attempts_and_leases", jobID, fmt.Sprintf(
					"attempt number=%d want=%d", attempt.AttemptNo, index+1,
				))
			}
			if index > 0 && attempt.LeaseGeneration <= previousGeneration {
				c.add("attempts_and_leases", jobID, "lease generations are not strictly increasing")
			}
			previousGeneration = attempt.LeaseGeneration
			if activeAttemptStates[attempt.State] {
				active++
			}
		}
		if active > 1 {
			c.add("attempts_and_leases", jobID, "more than one active attempt exists")
		}
		job, exists := jobs[jobID]
		if !exists {
			continue
		}
		last := attempts[len(attempts)-1]
		if job.AttemptNo != last.AttemptNo || job.LeaseGeneration != last.LeaseGeneration {
			c.add("attempts_and_leases", jobID, "job fence does not match latest attempt")
		}
		if active > 0 &&
			(job.State != "SCHEDULED" && job.State != "RUNNING") {
			c.add("attempts_and_leases", jobID, "active attempt belongs to an inactive job")
		}
		if completion, ok := completions[jobID]; ok && completion.AttemptNo > 0 {
			final := attempts[len(attempts)-1]
			if final.AttemptNo != completion.AttemptNo ||
				!attemptMatchesCompletion(final.State, completion.State) ||
				!final.ReceiptCommittedAt.Valid ||
				final.ReceiptCommittedAt.Int64 != completion.CommittedAt {
				c.add("attempts_and_leases", jobID, "final attempt disagrees with canonical completion")
			}
		}
	}
	for jobID, job := range jobs {
		if len(byJob[jobID]) == 0 && (job.AttemptNo != 0 || job.LeaseGeneration != 0) {
			c.add("attempts_and_leases", jobID, "job fence is nonzero without attempts")
		}
	}
	if activeCount != 0 {
		c.add("attempts_and_leases", "", fmt.Sprintf("%d active attempts remain after drain", activeCount))
	}
	return repeats, activeCount
}

func attemptMatchesCompletion(attemptState, completionState string) bool {
	switch completionState {
	case "SUCCEEDED":
		return attemptState == "SUCCEEDED"
	case "FAILED":
		return attemptState == "FAILED"
	case "DEAD_LETTER":
		return attemptState == "FAILED" || attemptState == "EXPIRED"
	default:
		return false
	}
}

func analyzeEvents(c *collector, rows []eventRow, jobs map[string]jobRow) {
	byJob := make(map[string][]eventRow)
	for index, event := range rows {
		if event.Sequence != int64(index+1) {
			c.add("event_log", event.JobID, fmt.Sprintf(
				"global event sequence=%d want=%d", event.Sequence, index+1,
			))
		}
		if event.OccurredAt <= 0 || !json.Valid([]byte(event.PayloadJSON)) {
			c.add("event_log", event.JobID, "event timestamp or payload JSON is invalid")
		}
		byJob[event.JobID] = append(byJob[event.JobID], event)
	}
	for jobID, events := range byJob {
		job, exists := jobs[jobID]
		if !exists {
			c.add("event_log", jobID, "event has no jobs row")
			continue
		}
		admissions := 0
		previousState := ""
		for index, event := range events {
			if event.Type == "job_admitted" {
				admissions++
			}
			if event.StateVersion != int64(index+1) {
				c.add("event_log", jobID, fmt.Sprintf(
					"event state version=%d want=%d", event.StateVersion, index+1,
				))
			}
			if index == 0 {
				if event.Type != "job_admitted" {
					c.add("event_log", jobID, "first event is not job_admitted")
				}
			} else if !allowedTransition(previousState, event.State, event.Type) {
				c.add("event_log", jobID, fmt.Sprintf(
					"invalid transition %s to %s for %s",
					previousState, event.State, event.Type,
				))
			}
			previousState = event.State
		}
		if admissions != 1 {
			c.add("event_log", jobID, fmt.Sprintf("admission events=%d want=1", admissions))
		}
		last := events[len(events)-1]
		if last.State != job.State || last.StateVersion != job.StateVersion {
			c.add("event_log", jobID, "event fold disagrees with materialized job")
		}
	}
	for jobID := range jobs {
		if len(byJob[jobID]) == 0 {
			c.add("event_log", jobID, "job has no events")
		}
	}
}

func analyzeDAG(
	c *collector,
	edges []dependencyRow,
	jobs map[string]jobRow,
	events []eventRow,
) {
	indegree := make(map[string]int, len(jobs))
	children := make(map[string][]string)
	parents := make(map[string][]string)
	for jobID := range jobs {
		indegree[jobID] = 0
	}
	for _, edge := range edges {
		child, childExists := jobs[edge.JobID]
		parent, parentExists := jobs[edge.DependsOn]
		if !childExists || !parentExists {
			c.add("dag", edge.JobID, "dependency endpoint is missing")
			continue
		}
		if child.TenantID != parent.TenantID {
			c.add("dag", edge.JobID, "dependency crosses tenants")
		}
		indegree[edge.JobID]++
		children[edge.DependsOn] = append(children[edge.DependsOn], edge.JobID)
		parents[edge.JobID] = append(parents[edge.JobID], edge.DependsOn)
	}
	queue := make([]string, 0, len(jobs))
	for jobID, degree := range indegree {
		if degree == 0 {
			queue = append(queue, jobID)
		}
	}
	visited := 0
	for len(queue) > 0 {
		jobID := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		visited++
		for _, child := range children[jobID] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if visited != len(jobs) {
		c.add("dag", "", "dependency graph contains a cycle")
	}
	for jobID, job := range jobs {
		if job.ReadySeq > 0 || job.State == "SCHEDULED" || job.State == "RUNNING" || job.State == "SUCCEEDED" {
			for _, parentID := range parents[jobID] {
				if jobs[parentID].State != "SUCCEEDED" {
					c.add("dag", jobID, "released job has an unsuccessful parent")
				}
			}
		}
	}
	releases := make(map[string]int)
	for _, event := range events {
		if event.Type == "dependency_released" {
			releases[event.JobID]++
		}
	}
	for jobID, count := range releases {
		if count > 1 {
			c.add("dag", jobID, fmt.Sprintf("dependency release events=%d want at most 1", count))
		}
	}
	for rootID, root := range jobs {
		if root.State != "FAILED" && root.State != "DEAD_LETTER" {
			continue
		}
		seen := map[string]bool{rootID: true}
		stack := append([]string(nil), children[rootID]...)
		for len(stack) > 0 {
			childID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if seen[childID] {
				continue
			}
			seen[childID] = true
			if !terminalStates[jobs[childID].State] {
				c.add("dag", childID, "descendant of unsuccessful job is nonterminal")
			}
			stack = append(stack, children[childID]...)
		}
	}
}

func analyzeSlots(
	c *collector,
	tenants []tenantRow,
	queues []queueRow,
	jobs map[string]jobRow,
) int {
	tenantSlots := make(map[string]int)
	queueSlots := make(map[string]int)
	tenantDepth := make(map[string]int)
	total := 0
	for _, job := range jobs {
		if !terminalStates[job.State] {
			tenantDepth[job.TenantID]++
		}
		if job.State == "SCHEDULED" || job.State == "RUNNING" {
			tenantSlots[job.TenantID] += job.SlotCost
			queueSlots[manifestKey(job.TenantID, job.Queue)] += job.SlotCost
			total += job.SlotCost
		}
	}
	seenTenants := make(map[string]bool)
	for _, tenant := range tenants {
		seenTenants[tenant.TenantID] = true
		if tenant.ActiveSlots != tenantSlots[tenant.TenantID] {
			c.add("slot_accounting", "", fmt.Sprintf(
				"tenant %s active_slots=%d want=%d",
				tenant.TenantID, tenant.ActiveSlots, tenantSlots[tenant.TenantID],
			))
		}
		if tenant.MaxSlots > 0 && tenant.ActiveSlots > tenant.MaxSlots {
			c.add("slot_accounting", "", "tenant slot cap exceeded for "+tenant.TenantID)
		}
		if tenant.MaxDepth > 0 && tenantDepth[tenant.TenantID] > tenant.MaxDepth {
			c.add("slot_accounting", "", "tenant depth cap exceeded for "+tenant.TenantID)
		}
	}
	for tenantID := range tenantDepth {
		if !seenTenants[tenantID] {
			c.add("slot_accounting", "", "tenant limits missing for "+tenantID)
		}
	}
	seenQueues := make(map[string]bool)
	for _, queue := range queues {
		key := manifestKey(queue.TenantID, queue.Queue)
		seenQueues[key] = true
		if queue.Weight < 1 {
			c.add("slot_accounting", "", "queue weight is not positive for "+key)
		}
		if queue.ActiveSlots != queueSlots[key] {
			c.add("slot_accounting", "", fmt.Sprintf(
				"queue %s active_slots=%d want=%d", key, queue.ActiveSlots, queueSlots[key],
			))
		}
	}
	for key := range queueSlots {
		if !seenQueues[key] {
			c.add("slot_accounting", "", "queue state missing for "+key)
		}
	}
	if total != 0 {
		c.add("slot_accounting", "", fmt.Sprintf("%d slot reservations remain after drain", total))
	}
	return total
}

func analyzeCounters(c *collector, counters []counterRow, jobs map[string]jobRow) {
	values := make(map[string]int64, len(counters))
	for _, counter := range counters {
		if _, exists := values[counter.Name]; exists {
			c.add("materialized_jobs", "", "duplicate counter "+counter.Name)
		}
		values[counter.Name] = counter.Value
	}
	readyCounter, exists := values["ready_seq"]
	if !exists {
		c.add("materialized_jobs", "", "ready sequence counter is missing")
		return
	}
	var maximum int64
	seen := make(map[int64]string)
	for _, job := range jobs {
		if job.ReadySeq > maximum {
			maximum = job.ReadySeq
		}
		if job.ReadySeq > 0 {
			if other, exists := seen[job.ReadySeq]; exists {
				c.add("materialized_jobs", job.ID, fmt.Sprintf(
					"ready sequence is shared with %s", other,
				))
			}
			seen[job.ReadySeq] = job.ID
		}
	}
	if readyCounter < maximum {
		c.add("materialized_jobs", "", "ready sequence counter trails materialized jobs")
	}
}

func analyzeMigrations(c *collector, rows []migrationRow) {
	if len(rows) == 0 {
		c.add("schema_migrations", "", "no schema migrations are recorded")
		return
	}
	for index, row := range rows {
		if row.Version != index+1 {
			c.add("schema_migrations", "", fmt.Sprintf(
				"migration version=%d want=%d", row.Version, index+1,
			))
		}
		if row.Name == "" || !validDigest(row.Checksum) || row.AppliedAt <= 0 {
			c.add("schema_migrations", "", fmt.Sprintf(
				"migration %d metadata is invalid", row.Version,
			))
		}
	}
}

func (c *collector) add(check, jobID, message string) {
	c.total++
	c.checks[check]++
	if len(c.details) < c.maxDetails {
		c.details = append(c.details, Violation{Check: check, JobID: jobID, Message: message})
	}
}

func allowedTransition(from, to, eventType string) bool {
	if terminalStates[from] {
		return false
	}
	if from == to {
		return from == "PENDING" &&
			(eventType == "dependency_released" || eventType == "job_promoted")
	}
	switch from {
	case "PENDING":
		return to == "SCHEDULED" || to == "FAILED" || to == "DEAD_LETTER"
	case "SCHEDULED", "RUNNING":
		return to == "RUNNING" || to == "PENDING" || to == "RETRYING" ||
			to == "SUCCEEDED" || to == "FAILED" || to == "DEAD_LETTER"
	case "RETRYING":
		return to == "PENDING" || to == "SCHEDULED" || to == "DEAD_LETTER"
	default:
		return false
	}
}

func validID(value string) bool {
	if len(value) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16 && strings.ToLower(value) == value
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}

func manifestKey(tenant, key string) string {
	return tenant + "\x00" + key
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

func removeValue(values []string, ignored string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if !strings.EqualFold(value, ignored) {
			result = append(result, value)
		}
	}
	return result
}

func nullIntString(value sql.NullInt64) string {
	if !value.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%d", value.Int64)
}

func valueOrEmpty(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
