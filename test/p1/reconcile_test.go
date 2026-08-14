package p1_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	qa "github.com/rajeev-chaurasia/rail-yard/qa/p1"
)

func TestReconcileQueriesDatabaseSQLDirectly(t *testing.T) {
	queries := qa.DefaultReconcileSQL
	db := openScriptedDatabase(t, map[string]scriptedRows{
		normalizeSQL("PRAGMA integrity_check"): {
			columns: []string{"integrity_check"},
			values:  [][]driver.Value{{"ok"}},
		},
		normalizeSQL("PRAGMA foreign_key_check"): {
			columns: []string{"table", "rowid", "parent", "fkid"},
		},
		normalizeSQL(queries.Jobs): {
			columns: []string{"id", "state", "state_version"},
			values:  [][]driver.Value{{"job-1", "SUCCEEDED", int64(7)}},
		},
		normalizeSQL(queries.Completions): {
			columns: []string{"job_id", "state"},
			values:  [][]driver.Value{{"job-1", "SUCCEEDED"}},
		},
		normalizeSQL(queries.Attempts): {
			columns: []string{"job_id", "attempt_no", "lease_generation", "state"},
			values: [][]driver.Value{
				{"job-1", int64(1), int64(1), "EXPIRED"},
				{"job-1", int64(2), int64(2), "SUCCEEDED"},
			},
		},
		normalizeSQL(queries.Events): {
			columns: []string{"event_seq", "job_id", "from_state", "to_state", "state_version"},
			values: [][]driver.Value{
				{int64(1), "job-1", nil, "PENDING", int64(1)},
				{int64(2), "job-1", "PENDING", "SCHEDULED", int64(2)},
				{int64(3), "job-1", "SCHEDULED", "RUNNING", int64(3)},
				{int64(4), "job-1", "RUNNING", "RETRYING", int64(4)},
				{int64(5), "job-1", "RETRYING", "SCHEDULED", int64(5)},
				{int64(6), "job-1", "SCHEDULED", "RUNNING", int64(6)},
				{int64(7), "job-1", "RUNNING", "SUCCEEDED", int64(7)},
			},
		},
	})

	report, err := qa.Reconcile(context.Background(), db, qa.ReconcileExpectation{
		AcceptedJobIDs:   []string{"job-1"},
		RequireTerminal:  true,
		RequireSucceeded: true,
		RequireQuiescent: true,
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("reconciliation failed: %s\nreport=%#v", report.Summary(), report)
	}
	if report.AcceptedCount != 1 || report.TerminalCount != 1 {
		t.Fatalf("reconciliation counts = accepted %d terminal %d",
			report.AcceptedCount, report.TerminalCount)
	}
}

func TestReconcileDetectsLedgerAndTransitionViolations(t *testing.T) {
	snapshot := qa.DatabaseSnapshot{
		IntegrityFailures:  []string{"wrong # of entries in index"},
		ForeignKeyFailures: []string{"table=attempts rowid=2 parent=jobs fkid=0"},
		Jobs: []qa.ReconcileJob{
			{ID: "job-1", State: "RUNNING", StateVersion: 3},
			{ID: "job-orphan", State: "SUCCEEDED", StateVersion: 1},
		},
		Completions: []qa.ReconcileCompletion{
			{JobID: "job-orphan", State: "SUCCEEDED"},
			{JobID: "job-orphan", State: "SUCCEEDED"},
		},
		Attempts: []qa.ReconcileAttempt{
			{JobID: "job-1", AttemptNo: 1, LeaseGeneration: 1, State: "RUNNING"},
			{JobID: "job-1", AttemptNo: 1, LeaseGeneration: 1, State: "RUNNING"},
		},
		Events: []qa.ReconcileEvent{
			{
				Sequence:     1,
				JobID:        "job-1",
				ToState:      "PENDING",
				StateVersion: 1,
			},
			{
				Sequence:     3,
				JobID:        "job-1",
				FromState:    "PENDING",
				ToState:      "SCHEDULED",
				StateVersion: 2,
			},
		},
	}

	report := qa.AnalyzeDatabaseSnapshot(snapshot, qa.ReconcileExpectation{
		AcceptedJobIDs:   []string{"job-1", "job-1", "job-lost"},
		RequireTerminal:  true,
		RequireSucceeded: true,
		RequireQuiescent: true,
	})
	if report.Passed() {
		t.Fatalf("violating snapshot passed: %#v", report)
	}
	if len(report.Lost) != 2 ||
		len(report.Orphans) != 1 ||
		len(report.DuplicateAcceptedIDs) != 1 ||
		report.DuplicateCompletions["job-orphan"] != 1 ||
		len(report.ActiveJobs) != 1 ||
		len(report.ActiveAttempts) != 2 ||
		len(report.AttemptViolations) == 0 ||
		len(report.EventViolations) == 0 ||
		len(report.IntegrityFailures) != 1 ||
		len(report.ForeignKeyFailures) != 1 {
		t.Fatalf("reconciliation missed violations: %s\nreport=%#v", report.Summary(), report)
	}
}

type scriptedRows struct {
	columns []string
	values  [][]driver.Value
}

type scriptedDriver struct {
	queries map[string]scriptedRows
}

func (d *scriptedDriver) Open(string) (driver.Conn, error) {
	return &scriptedConnection{queries: d.queries}, nil
}

type scriptedConnection struct {
	queries map[string]scriptedRows
}

func (c *scriptedConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("scripted database does not prepare statements")
}

func (c *scriptedConnection) Close() error {
	return nil
}

func (c *scriptedConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("scripted database does not begin transactions")
}

func (c *scriptedConnection) QueryContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	rows, found := c.queries[normalizeSQL(query)]
	if !found {
		return nil, fmt.Errorf("unexpected query: %s", normalizeSQL(query))
	}
	return &scriptedResultRows{
		columns: append([]string(nil), rows.columns...),
		values:  cloneDriverValues(rows.values),
	}, nil
}

type scriptedResultRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *scriptedResultRows) Columns() []string {
	return r.columns
}

func (r *scriptedResultRows) Close() error {
	return nil
}

func (r *scriptedResultRows) Next(destination []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(destination, r.values[r.index])
	r.index++
	return nil
}

var scriptedDriverSequence atomic.Uint64

func openScriptedDatabase(t *testing.T, queries map[string]scriptedRows) *sql.DB {
	t.Helper()
	driverName := fmt.Sprintf("rail-yard-p1-script-%d", scriptedDriverSequence.Add(1))
	sql.Register(driverName, &scriptedDriver{queries: queries})
	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("open scripted database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close scripted database: %v", err)
		}
	})
	return db
}

func normalizeSQL(query string) string {
	return strings.Join(strings.Fields(query), " ")
}

func cloneDriverValues(values [][]driver.Value) [][]driver.Value {
	copyValues := make([][]driver.Value, len(values))
	for index := range values {
		copyValues[index] = append([]driver.Value(nil), values[index]...)
	}
	return copyValues
}

var _ driver.QueryerContext = (*scriptedConnection)(nil)
