package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
	_ "modernc.org/sqlite"
)

const (
	busyTimeoutMS = 5000
	openTimeout   = 30 * time.Second
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Store struct {
	db                  *sql.DB
	defaultTenantDepth  int
	defaultTenantSlots  int
	maxSlotCost         int
	allowShell          bool
	writes              *writeGate
	attemptStartCommits atomic.Uint64
	completionCommits   atomic.Uint64
	clockOrigin         time.Time
	logicalOrigin       int64
	lastTime            int64
	closeOnce           sync.Once
	closeErr            error
}

var _ storepkg.Store = (*Store)(nil)
var _ storepkg.BatchAttemptStartStore = (*Store)(nil)
var _ storepkg.BatchCompletionStore = (*Store)(nil)

func Open(path string) (*Store, error) {
	return OpenWithOptions(path, DefaultOptions())
}

type Options struct {
	DefaultTenantDepth int
	DefaultTenantSlots int
	MaxSlotCost        int
	AllowShell         bool
}

func DefaultOptions() Options {
	return Options{
		DefaultTenantDepth: 100_000,
		MaxSlotCost:        64,
	}
}

func OpenWithOptions(path string, options Options) (*Store, error) {
	if options.DefaultTenantDepth < 1 {
		return nil, errors.New("default tenant depth must be positive")
	}
	if options.DefaultTenantSlots < 0 {
		return nil, errors.New("default tenant slots must not be negative")
	}
	if options.MaxSlotCost < 1 {
		return nil, errors.New("maximum slot cost must be positive")
	}
	dsn, err := dataSourceName(path)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	store := &Store{
		db:                 db,
		defaultTenantDepth: options.DefaultTenantDepth,
		defaultTenantSlots: options.DefaultTenantSlots,
		maxSlotCost:        options.MaxSlotCost,
		allowShell:         options.AllowShell,
		writes:             newWriteGate(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := store.validateSettings(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.applyMigrations(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.initializeLogicalClock(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) initializeLogicalClock(ctx context.Context) error {
	var persisted sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `
		SELECT MAX(value) FROM (
			SELECT MAX(updated_at) AS value FROM jobs
			UNION ALL SELECT MAX(heartbeat_at) FROM attempts
			UNION ALL SELECT MAX(completed_at) FROM attempts
			UNION ALL SELECT MAX(committed_at) FROM job_completions
			UNION ALL SELECT MAX(occurred_at) FROM events
			UNION ALL SELECT MAX(committed_at) FROM operation_requests
			UNION ALL SELECT MAX(occurred_at) FROM audit_events
			UNION ALL SELECT MAX(updated_at) FROM dag_runs
		)`).Scan(&persisted); err != nil {
		return fmt.Errorf("initialize logical clock: %w", err)
	}
	s.clockOrigin = time.Now()
	s.logicalOrigin = 0
	if persisted.Valid {
		s.logicalOrigin = persisted.Int64
	}
	s.lastTime = s.logicalOrigin
	return nil
}

func (s *Store) writeTime(requested time.Time) time.Time {
	candidate := requested.UTC().UnixNano()
	monotonic := s.logicalOrigin + time.Since(s.clockOrigin).Nanoseconds()
	if monotonic > candidate {
		candidate = monotonic
	}
	if candidate < s.lastTime {
		candidate = s.lastTime
	}
	s.lastTime = candidate
	return time.Unix(0, candidate).UTC()
}

type writeGate struct {
	mu        sync.Mutex
	condition *sync.Cond
	active    bool
	waiters   [3]int
}

const (
	writeNormal = iota
	writeDispatch
	writeMaintenance
)

func newWriteGate() *writeGate {
	gate := &writeGate{}
	gate.condition = sync.NewCond(&gate.mu)
	return gate
}

func (gate *writeGate) lock(priority int) {
	gate.mu.Lock()
	gate.waiters[priority]++
	for gate.active || gate.hasHigherWaiter(priority) {
		gate.condition.Wait()
	}
	gate.waiters[priority]--
	gate.active = true
	gate.mu.Unlock()
}

func (gate *writeGate) hasHigherWaiter(priority int) bool {
	for candidate := priority + 1; candidate < len(gate.waiters); candidate++ {
		if gate.waiters[candidate] > 0 {
			return true
		}
	}
	return false
}

func (gate *writeGate) unlock() {
	gate.mu.Lock()
	gate.active = false
	gate.condition.Broadcast()
	gate.mu.Unlock()
}

func (s *Store) beginWrite(priority int) func() {
	s.writes.lock(priority)
	return s.writes.unlock
}

func dataSourceName(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("sqlite path must not be empty")
	}
	if path == ":memory:" {
		return "", errors.New("in-memory sqlite cannot satisfy WAL durability")
	}

	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve sqlite path: %w", err)
	}
	slashPath := filepath.ToSlash(absolute)
	if volume := filepath.VolumeName(absolute); volume != "" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}

	uri := url.URL{Scheme: "file", Path: slashPath}
	query := uri.Query()
	query.Add("_pragma", "journal_mode(WAL)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	uri.RawQuery = query.Encode()
	return uri.String(), nil
}

func (s *Store) validateSettings(ctx context.Context) error {
	var journalMode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return fmt.Errorf("read sqlite journal_mode: %w", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf("sqlite journal_mode validation failed: got %q, want WAL", journalMode)
	}

	var synchronous int
	if err := s.db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		return fmt.Errorf("read sqlite synchronous: %w", err)
	}
	if synchronous != 2 {
		return fmt.Errorf("sqlite synchronous validation failed: got %d, want FULL", synchronous)
	}

	var foreignKeys int
	if err := s.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return fmt.Errorf("read sqlite foreign_keys: %w", err)
	}
	if foreignKeys != 1 {
		return fmt.Errorf("sqlite foreign_keys validation failed: got %d, want ON", foreignKeys)
	}

	var timeout int
	if err := s.db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		return fmt.Errorf("read sqlite busy_timeout: %w", err)
	}
	if timeout != busyTimeoutMS {
		return fmt.Errorf(
			"sqlite busy_timeout validation failed: got %d, want %d",
			timeout,
			busyTimeoutMS,
		)
	}
	return nil
}

func (s *Store) applyMigrations(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer rollback(tx)

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at INTEGER NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		versionText, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return fmt.Errorf("migration %q has no numeric prefix", entry.Name())
		}
		version, err := strconv.Atoi(versionText)
		if err != nil || version < 1 {
			return fmt.Errorf("migration %q has invalid version", entry.Name())
		}

		body, err := fs.ReadFile(migrationFiles, "migrations/"+entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])

		var storedChecksum string
		err = tx.QueryRowContext(
			ctx,
			"SELECT checksum FROM schema_migrations WHERE version = ?",
			version,
		).Scan(&storedChecksum)
		switch {
		case err == nil:
			if storedChecksum != checksum {
				return fmt.Errorf("migration %d checksum mismatch", version)
			}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read migration %d: %w", version, err)
		}

		if err := executeMigration(ctx, tx, string(body)); err != nil {
			return fmt.Errorf("apply migration %d: %w", version, err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO schema_migrations (version, name, checksum, applied_at)
			 VALUES (?, ?, ?, ?)`,
			version,
			entry.Name(),
			checksum,
			timeToDB(time.Now()),
		); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func executeMigration(ctx context.Context, tx *sql.Tx, body string) error {
	for _, statement := range strings.Split(body, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

const jobColumns = `
	id,
	tenant_id,
	queue_name,
	priority,
	slot_cost,
	payload_json,
	retry_json,
	state,
	attempt_no,
	state_version,
	lease_generation,
	available_at,
	ready_seq,
	created_at,
	updated_at,
	terminal_at,
	failure_json
`

type scanner interface {
	Scan(...any) error
}

func scanJob(row scanner) (domain.Job, error) {
	var (
		job         domain.Job
		payloadJSON string
		retryJSON   string
		state       string
		availableAt int64
		createdAt   int64
		updatedAt   int64
		terminalAt  sql.NullInt64
		failureJSON sql.NullString
	)
	if err := row.Scan(
		&job.ID,
		&job.TenantID,
		&job.Queue,
		&job.Priority,
		&job.SlotCost,
		&payloadJSON,
		&retryJSON,
		&state,
		&job.AttemptNo,
		&job.StateVersion,
		&job.LeaseGeneration,
		&availableAt,
		&job.ReadySeq,
		&createdAt,
		&updatedAt,
		&terminalAt,
		&failureJSON,
	); err != nil {
		return domain.Job{}, err
	}

	job.State = domain.JobState(state)
	job.AvailableAt = timeFromDB(availableAt)
	job.CreatedAt = timeFromDB(createdAt)
	job.UpdatedAt = timeFromDB(updatedAt)
	if terminalAt.Valid {
		value := timeFromDB(terminalAt.Int64)
		job.TerminalAt = &value
	}
	if err := json.Unmarshal([]byte(payloadJSON), &job.Payload); err != nil {
		return domain.Job{}, fmt.Errorf("decode job payload: %w", err)
	}
	if err := json.Unmarshal([]byte(retryJSON), &job.Retry); err != nil {
		return domain.Job{}, fmt.Errorf("decode retry policy: %w", err)
	}
	if failureJSON.Valid {
		var failure domain.Failure
		if err := json.Unmarshal([]byte(failureJSON.String), &failure); err != nil {
			return domain.Job{}, fmt.Errorf("decode job failure: %w", err)
		}
		job.Failure = &failure
	}
	return job, nil
}

func timeToDB(value time.Time) int64 {
	return value.UTC().UnixNano()
}

func timeFromDB(value int64) time.Time {
	return time.Unix(0, value).UTC()
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}

func nextReadySeq(ctx context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(
		ctx,
		"UPDATE counters SET value = value + 1 WHERE name = 'ready_seq'",
	)
	if err != nil {
		return 0, fmt.Errorf("advance ready sequence: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect ready sequence update: %w", err)
	}
	if affected != 1 {
		return 0, errors.New("ready sequence counter is missing")
	}

	var value int64
	if err := tx.QueryRowContext(
		ctx,
		"SELECT value FROM counters WHERE name = 'ready_seq'",
	).Scan(&value); err != nil {
		return 0, fmt.Errorf("read ready sequence: %w", err)
	}
	return value, nil
}

func appendEvent(
	ctx context.Context,
	tx *sql.Tx,
	jobID string,
	eventType string,
	state domain.JobState,
	stateVersion int64,
	now time.Time,
	payload any,
) error {
	body := []byte("{}")
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode %s event: %w", eventType, err)
		}
	}
	_, err := tx.ExecContext(
		ctx,
		`INSERT INTO events
			(job_id, event_type, state, state_version, occurred_at, payload_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		jobID,
		eventType,
		state,
		stateVersion,
		timeToDB(now),
		string(body),
	)
	if err != nil {
		return fmt.Errorf("append %s event: %w", eventType, err)
	}
	return nil
}
