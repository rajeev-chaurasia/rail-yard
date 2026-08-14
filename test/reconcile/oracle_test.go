package reconcile

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const fixtureJobID = "0123456789abcdef0123456789abcdef"
const fixtureDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestReconcileAcceptsConsistentSuccessfulLedger(t *testing.T) {
	databasePath := createFixtureDatabase(t)
	db, err := OpenReadOnly(databasePath)
	if err != nil {
		t.Fatalf("open read-only database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	report, err := Reconcile(context.Background(), db, []AcceptedRecord{fixtureRecord()}, Options{
		ExpectedJobs: 1,
		MaxDetails:   100,
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !report.Passed {
		body, _ := json.MarshalIndent(report, "", "  ")
		t.Fatalf("consistent ledger failed:\n%s", body)
	}
	if report.Counts.Accepted != 1 ||
		report.Counts.Completions != 1 ||
		report.Counts.Attempts != 1 ||
		report.Counts.Events != 4 {
		t.Fatalf("unexpected counts: %#v", report.Counts)
	}
}

func TestReconcileDetectsMissingCanonicalCompletion(t *testing.T) {
	databasePath := createFixtureDatabase(t)
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	if _, err := db.Exec("DELETE FROM job_completions"); err != nil {
		t.Fatalf("delete completion: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture database: %v", err)
	}

	readOnly, err := OpenReadOnly(databasePath)
	if err != nil {
		t.Fatalf("open read-only database: %v", err)
	}
	defer func() {
		_ = readOnly.Close()
	}()
	report, err := Reconcile(context.Background(), readOnly, []AcceptedRecord{fixtureRecord()}, Options{})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if report.Passed {
		t.Fatal("ledger without canonical completion passed")
	}
	if !hasViolation(report, "canonical_ledger", "no canonical completion") {
		t.Fatalf("missing completion violation not reported: %#v", report.Violations)
	}
}

func TestReadManifestRejectsUnknownFieldsAndMultipleValues(t *testing.T) {
	valid, err := json.Marshal(fixtureRecord())
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	inputs := []string{
		strings.TrimSuffix(string(valid), "}") + `,"unexpected":true}` + "\n",
		string(valid) + ` {}` + "\n",
		"\n",
	}
	for _, input := range inputs {
		if _, err := ReadManifest(strings.NewReader(input)); err == nil {
			t.Fatalf("invalid manifest %q was accepted", input)
		}
	}
}

func TestOpenReadOnlyRejectsWrites(t *testing.T) {
	databasePath := createFixtureDatabase(t)
	db, err := OpenReadOnly(databasePath)
	if err != nil {
		t.Fatalf("open read-only database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.Exec("DELETE FROM events"); err == nil {
		t.Fatal("write through read-only oracle connection succeeded")
	}
}

func createFixtureDatabase(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()
	schema := `
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=FULL;
		PRAGMA foreign_keys=ON;
		CREATE TABLE jobs (
			id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			queue_name TEXT NOT NULL,
			slot_cost INTEGER NOT NULL,
			payload_json TEXT NOT NULL,
			retry_json TEXT NOT NULL,
			state TEXT NOT NULL,
			attempt_no INTEGER NOT NULL,
			state_version INTEGER NOT NULL,
			lease_generation INTEGER NOT NULL,
			available_at INTEGER NOT NULL,
			ready_seq INTEGER NOT NULL,
			execution_key TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			terminal_at INTEGER,
			failure_json TEXT
		);
		CREATE TABLE job_completions (
			job_id TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			state_version INTEGER NOT NULL,
			attempt_no INTEGER NOT NULL,
			output_digest TEXT NOT NULL,
			failure_json TEXT,
			committed_at INTEGER NOT NULL
		);
		CREATE TABLE attempts (
			job_id TEXT NOT NULL,
			attempt_no INTEGER NOT NULL,
			worker_id TEXT NOT NULL,
			lease_generation INTEGER NOT NULL,
			token_hash TEXT NOT NULL,
			state TEXT NOT NULL,
			leased_at INTEGER NOT NULL,
			heartbeat_at INTEGER NOT NULL,
			expires_at INTEGER NOT NULL,
			started_at INTEGER,
			completed_at INTEGER,
			failure_json TEXT,
			completion_request_digest TEXT,
			receipt_state TEXT,
			receipt_state_version INTEGER,
			receipt_committed_at INTEGER,
			PRIMARY KEY (job_id, attempt_no)
		);
		CREATE TABLE events (
			event_seq INTEGER PRIMARY KEY,
			job_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			state TEXT NOT NULL,
			state_version INTEGER NOT NULL,
			occurred_at INTEGER NOT NULL,
			payload_json TEXT NOT NULL
		);
		CREATE TABLE idempotency_requests (
			tenant_id TEXT NOT NULL,
			submission_key TEXT NOT NULL,
			request_kind TEXT NOT NULL,
			request_digest TEXT NOT NULL,
			job_id TEXT,
			response_json TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (tenant_id, submission_key)
		);
		CREATE TABLE job_dependencies (
			job_id TEXT NOT NULL,
			depends_on_id TEXT NOT NULL,
			PRIMARY KEY (job_id, depends_on_id)
		);
		CREATE TABLE tenant_limits (
			tenant_id TEXT PRIMARY KEY,
			max_depth INTEGER NOT NULL,
			max_slots INTEGER NOT NULL,
			active_slots INTEGER NOT NULL
		);
		CREATE TABLE queue_state (
			tenant_id TEXT NOT NULL,
			queue_name TEXT NOT NULL,
			weight INTEGER NOT NULL,
			deficit INTEGER NOT NULL,
			active_slots INTEGER NOT NULL,
			PRIMARY KEY (tenant_id, queue_name)
		);
		CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
		CREATE TABLE schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL,
			applied_at INTEGER NOT NULL
		);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create fixture schema: %v", err)
	}
	response := `{"id":"` + fixtureJobID + `"}`
	inserts := []struct {
		query string
		args  []any
	}{
		{query: "INSERT INTO tenant_limits VALUES ('chaos', 0, 0, 0)"},
		{query: "INSERT INTO queue_state VALUES ('chaos', 'noop', 1, 0, 0)"},
		{query: "INSERT INTO counters VALUES ('ready_seq', 1)"},
		{
			query: "INSERT INTO schema_migrations VALUES (1, '001_initial.sql', ?, 1)",
			args:  []any{fixtureDigest},
		},
		{
			query: `INSERT INTO jobs VALUES (?, 'chaos', 'noop', 1, '{"type":"noop"}',
				'{"max_attempts":100,"retryable":true}', 'SUCCEEDED', 1, 4, 1,
				1, 0, ?, 1, 5, 5, NULL)`,
			args: []any{fixtureJobID, fixtureJobID},
		},
		{
			query: `INSERT INTO attempts VALUES (?, 1, 'worker-1', 1, ?, 'SUCCEEDED',
				2, 3, 6, 3, 5, NULL, ?, 'SUCCEEDED', 4, 5)`,
			args: []any{fixtureJobID, fixtureDigest, fixtureDigest},
		},
		{
			query: "INSERT INTO job_completions VALUES (?, 'SUCCEEDED', 4, 1, ?, NULL, 5)",
			args:  []any{fixtureJobID, fixtureDigest},
		},
		{
			query: `INSERT INTO idempotency_requests VALUES
				('chaos', 'stable-key', 'job', ?, ?, ?, 1)`,
			args: []any{fixtureDigest, fixtureJobID, response},
		},
		{
			query: "INSERT INTO events VALUES (1, ?, 'job_admitted', 'PENDING', 1, 1, '{}')",
			args:  []any{fixtureJobID},
		},
		{
			query: "INSERT INTO events VALUES (2, ?, 'lease_acquired', 'SCHEDULED', 2, 2, '{}')",
			args:  []any{fixtureJobID},
		},
		{
			query: "INSERT INTO events VALUES (3, ?, 'attempt_started', 'RUNNING', 3, 3, '{}')",
			args:  []any{fixtureJobID},
		},
		{
			query: "INSERT INTO events VALUES (4, ?, 'job_completed', 'SUCCEEDED', 4, 5, '{}')",
			args:  []any{fixtureJobID},
		},
	}
	for _, insert := range inserts {
		if _, err := db.Exec(insert.query, insert.args...); err != nil {
			t.Fatalf("insert fixture ledger: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture database: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat fixture database: %v", err)
	}
	return path
}

func fixtureRecord() AcceptedRecord {
	return AcceptedRecord{
		Sequence:      1,
		SubmissionKey: "stable-key",
		JobID:         fixtureJobID,
		TenantID:      "chaos",
		AcceptedAt:    time.Unix(1, 0).UTC(),
	}
}

func hasViolation(report Report, check, fragment string) bool {
	for _, violation := range report.Violations {
		if violation.Check == check && strings.Contains(violation.Message, fragment) {
			return true
		}
	}
	return false
}
