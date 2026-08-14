package p1

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type ReconcileSQL struct {
	Jobs        string
	Completions string
	Attempts    string
	Events      string
}

var DefaultReconcileSQL = ReconcileSQL{
	Jobs: `
		SELECT id, state, state_version
		FROM jobs
		ORDER BY id`,
	Completions: `
		SELECT job_id, state
		FROM job_completions
		ORDER BY job_id`,
	Attempts: `
		SELECT job_id, attempt_no, lease_generation, state
		FROM attempts
		ORDER BY job_id, attempt_no`,
	Events: `
		SELECT event_seq, job_id, NULL, state, state_version
		FROM events
		ORDER BY event_seq`,
}

type ReconcileExpectation struct {
	AcceptedJobIDs   []string
	RequireTerminal  bool
	RequireSucceeded bool
	RequireQuiescent bool
}

type ReconcileJob struct {
	ID           string
	State        string
	StateVersion int64
}

type ReconcileCompletion struct {
	JobID string
	State string
}

type ReconcileAttempt struct {
	JobID           string
	AttemptNo       int
	LeaseGeneration int64
	State           string
}

type ReconcileEvent struct {
	Sequence     int64
	JobID        string
	FromState    string
	ToState      string
	StateVersion int64
}

type DatabaseSnapshot struct {
	IntegrityFailures  []string
	ForeignKeyFailures []string
	Jobs               []ReconcileJob
	Completions        []ReconcileCompletion
	Attempts           []ReconcileAttempt
	Events             []ReconcileEvent
}

type ReconcileReport struct {
	AcceptedCount             int
	TerminalCount             int
	Lost                      []string
	Orphans                   []string
	DuplicateAcceptedIDs      []string
	DuplicateCompletions      map[string]int
	Unsuccessful              map[string]string
	ActiveJobs                []string
	ActiveAttempts            []string
	MaterializationViolations []string
	AttemptViolations         []string
	EventViolations           []string
	IntegrityFailures         []string
	ForeignKeyFailures        []string
	RequireTerminal           bool
	RequireSucceeded          bool
	RequireQuiescent          bool
}

func Reconcile(
	ctx context.Context,
	db *sql.DB,
	expectation ReconcileExpectation,
) (ReconcileReport, error) {
	return ReconcileWithSQL(ctx, db, expectation, DefaultReconcileSQL)
}

func ReconcileWithSQL(
	ctx context.Context,
	db *sql.DB,
	expectation ReconcileExpectation,
	queries ReconcileSQL,
) (ReconcileReport, error) {
	if db == nil {
		return ReconcileReport{}, fmt.Errorf("reconcile: nil database")
	}
	snapshot, err := ReadDatabaseSnapshot(ctx, db, queries)
	if err != nil {
		return ReconcileReport{}, err
	}
	return AnalyzeDatabaseSnapshot(snapshot, expectation), nil
}

func ReadDatabaseSnapshot(
	ctx context.Context,
	db *sql.DB,
	queries ReconcileSQL,
) (DatabaseSnapshot, error) {
	var snapshot DatabaseSnapshot

	integrityRows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return snapshot, fmt.Errorf("reconcile integrity_check: %w", err)
	}
	for integrityRows.Next() {
		var result string
		if err := integrityRows.Scan(&result); err != nil {
			_ = integrityRows.Close()
			return snapshot, fmt.Errorf("scan integrity_check: %w", err)
		}
		if result != "ok" {
			snapshot.IntegrityFailures = append(snapshot.IntegrityFailures, result)
		}
	}
	if err := closeRows(integrityRows); err != nil {
		return snapshot, fmt.Errorf("finish integrity_check: %w", err)
	}

	foreignKeyRows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return snapshot, fmt.Errorf("reconcile foreign_key_check: %w", err)
	}
	for foreignKeyRows.Next() {
		var table, parent string
		var rowID, foreignKeyID any
		if err := foreignKeyRows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			_ = foreignKeyRows.Close()
			return snapshot, fmt.Errorf("scan foreign_key_check: %w", err)
		}
		snapshot.ForeignKeyFailures = append(snapshot.ForeignKeyFailures,
			fmt.Sprintf("table=%s rowid=%v parent=%s fkid=%v", table, rowID, parent, foreignKeyID))
	}
	if err := closeRows(foreignKeyRows); err != nil {
		return snapshot, fmt.Errorf("finish foreign_key_check: %w", err)
	}

	jobRows, err := db.QueryContext(ctx, queries.Jobs)
	if err != nil {
		return snapshot, fmt.Errorf("query jobs: %w", err)
	}
	for jobRows.Next() {
		var job ReconcileJob
		if err := jobRows.Scan(&job.ID, &job.State, &job.StateVersion); err != nil {
			_ = jobRows.Close()
			return snapshot, fmt.Errorf("scan jobs: %w", err)
		}
		snapshot.Jobs = append(snapshot.Jobs, job)
	}
	if err := closeRows(jobRows); err != nil {
		return snapshot, fmt.Errorf("finish jobs: %w", err)
	}

	completionRows, err := db.QueryContext(ctx, queries.Completions)
	if err != nil {
		return snapshot, fmt.Errorf("query job_completions: %w", err)
	}
	for completionRows.Next() {
		var completion ReconcileCompletion
		if err := completionRows.Scan(&completion.JobID, &completion.State); err != nil {
			_ = completionRows.Close()
			return snapshot, fmt.Errorf("scan job_completions: %w", err)
		}
		snapshot.Completions = append(snapshot.Completions, completion)
	}
	if err := closeRows(completionRows); err != nil {
		return snapshot, fmt.Errorf("finish job_completions: %w", err)
	}

	attemptRows, err := db.QueryContext(ctx, queries.Attempts)
	if err != nil {
		return snapshot, fmt.Errorf("query attempts: %w", err)
	}
	for attemptRows.Next() {
		var attempt ReconcileAttempt
		if err := attemptRows.Scan(
			&attempt.JobID,
			&attempt.AttemptNo,
			&attempt.LeaseGeneration,
			&attempt.State,
		); err != nil {
			_ = attemptRows.Close()
			return snapshot, fmt.Errorf("scan attempts: %w", err)
		}
		snapshot.Attempts = append(snapshot.Attempts, attempt)
	}
	if err := closeRows(attemptRows); err != nil {
		return snapshot, fmt.Errorf("finish attempts: %w", err)
	}

	eventRows, err := db.QueryContext(ctx, queries.Events)
	if err != nil {
		return snapshot, fmt.Errorf("query events: %w", err)
	}
	for eventRows.Next() {
		var event ReconcileEvent
		var fromState sql.NullString
		if err := eventRows.Scan(
			&event.Sequence,
			&event.JobID,
			&fromState,
			&event.ToState,
			&event.StateVersion,
		); err != nil {
			_ = eventRows.Close()
			return snapshot, fmt.Errorf("scan events: %w", err)
		}
		if fromState.Valid {
			event.FromState = fromState.String
		}
		snapshot.Events = append(snapshot.Events, event)
	}
	if err := closeRows(eventRows); err != nil {
		return snapshot, fmt.Errorf("finish events: %w", err)
	}

	return snapshot, nil
}

func closeRows(rows *sql.Rows) error {
	err := rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func AnalyzeDatabaseSnapshot(
	snapshot DatabaseSnapshot,
	expectation ReconcileExpectation,
) ReconcileReport {
	report := ReconcileReport{
		DuplicateCompletions: make(map[string]int),
		Unsuccessful:         make(map[string]string),
		IntegrityFailures:    append([]string(nil), snapshot.IntegrityFailures...),
		ForeignKeyFailures:   append([]string(nil), snapshot.ForeignKeyFailures...),
		RequireTerminal:      expectation.RequireTerminal,
		RequireSucceeded:     expectation.RequireSucceeded,
		RequireQuiescent:     expectation.RequireQuiescent,
	}

	accepted := make(map[string]struct{}, len(expectation.AcceptedJobIDs))
	for _, jobID := range expectation.AcceptedJobIDs {
		if _, duplicate := accepted[jobID]; duplicate {
			report.DuplicateAcceptedIDs = append(report.DuplicateAcceptedIDs, jobID)
		}
		accepted[jobID] = struct{}{}
	}
	report.AcceptedCount = len(accepted)

	jobs := make(map[string]ReconcileJob, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		if _, duplicate := jobs[job.ID]; duplicate {
			report.MaterializationViolations = append(report.MaterializationViolations,
				fmt.Sprintf("duplicate jobs row for %s", job.ID))
		}
		jobs[job.ID] = job
		if !isTerminalState(job.State) {
			report.ActiveJobs = append(report.ActiveJobs, job.ID)
		}
	}

	completionCounts := make(map[string]int)
	completionStates := make(map[string]string)
	for _, completion := range snapshot.Completions {
		completionCounts[completion.JobID]++
		completionStates[completion.JobID] = completion.State
	}
	report.TerminalCount = len(completionCounts)
	for jobID, count := range completionCounts {
		if count > 1 {
			report.DuplicateCompletions[jobID] = count - 1
		}
		if _, found := accepted[jobID]; !found {
			report.Orphans = append(report.Orphans, jobID)
		}
		job, found := jobs[jobID]
		if !found {
			report.MaterializationViolations = append(report.MaterializationViolations,
				fmt.Sprintf("completion %s has no jobs row", jobID))
			continue
		}
		if !isTerminalState(job.State) || job.State != completionStates[jobID] {
			report.MaterializationViolations = append(report.MaterializationViolations,
				fmt.Sprintf("completion %s state %s disagrees with jobs state %s",
					jobID, completionStates[jobID], job.State))
		}
	}

	for jobID := range accepted {
		job, materialized := jobs[jobID]
		if !materialized {
			report.MaterializationViolations = append(report.MaterializationViolations,
				fmt.Sprintf("accepted job %s has no jobs row", jobID))
		}
		if completionCounts[jobID] == 0 {
			report.Lost = append(report.Lost, jobID)
		}
		if expectation.RequireSucceeded && completionStates[jobID] != string(ModelSucceeded) {
			report.Unsuccessful[jobID] = completionStates[jobID]
		}
		if materialized && isTerminalState(job.State) && completionCounts[jobID] == 0 {
			report.MaterializationViolations = append(report.MaterializationViolations,
				fmt.Sprintf("terminal job %s has no canonical completion", jobID))
		}
	}

	analyzeAttempts(snapshot.Attempts, &report)
	analyzeEvents(snapshot.Events, jobs, &report)
	sortReport(&report)
	return report
}

func analyzeAttempts(attempts []ReconcileAttempt, report *ReconcileReport) {
	lastAttempt := make(map[string]int)
	lastGeneration := make(map[string]int64)
	seenAttempt := make(map[string]map[int]struct{})
	seenGeneration := make(map[string]map[int64]struct{})
	for _, attempt := range attempts {
		if seenAttempt[attempt.JobID] == nil {
			seenAttempt[attempt.JobID] = make(map[int]struct{})
			seenGeneration[attempt.JobID] = make(map[int64]struct{})
		}
		if _, duplicate := seenAttempt[attempt.JobID][attempt.AttemptNo]; duplicate {
			report.AttemptViolations = append(report.AttemptViolations,
				fmt.Sprintf("%s duplicate attempt %d", attempt.JobID, attempt.AttemptNo))
		}
		if _, duplicate := seenGeneration[attempt.JobID][attempt.LeaseGeneration]; duplicate {
			report.AttemptViolations = append(report.AttemptViolations,
				fmt.Sprintf("%s duplicate generation %d", attempt.JobID, attempt.LeaseGeneration))
		}
		if attempt.AttemptNo <= lastAttempt[attempt.JobID] ||
			attempt.LeaseGeneration <= lastGeneration[attempt.JobID] {
			report.AttemptViolations = append(report.AttemptViolations,
				fmt.Sprintf("%s attempts or generations are not strictly increasing", attempt.JobID))
		}
		seenAttempt[attempt.JobID][attempt.AttemptNo] = struct{}{}
		seenGeneration[attempt.JobID][attempt.LeaseGeneration] = struct{}{}
		lastAttempt[attempt.JobID] = attempt.AttemptNo
		lastGeneration[attempt.JobID] = attempt.LeaseGeneration
		if isActiveAttemptState(attempt.State) {
			report.ActiveAttempts = append(report.ActiveAttempts,
				fmt.Sprintf("%s/%d/%d", attempt.JobID, attempt.AttemptNo, attempt.LeaseGeneration))
		}
	}
}

func analyzeEvents(
	events []ReconcileEvent,
	jobs map[string]ReconcileJob,
	report *ReconcileReport,
) {
	if len(jobs) > 0 && len(events) == 0 {
		report.EventViolations = append(report.EventViolations, "jobs exist without job events")
		return
	}

	lastState := make(map[string]string)
	lastVersion := make(map[string]int64)
	for index, event := range events {
		wantSequence := int64(index + 1)
		if event.Sequence != wantSequence {
			report.EventViolations = append(report.EventViolations,
				fmt.Sprintf("event sequence got %d, want %d", event.Sequence, wantSequence))
		}
		if event.StateVersion != lastVersion[event.JobID]+1 {
			report.EventViolations = append(report.EventViolations,
				fmt.Sprintf("%s event version got %d after %d",
					event.JobID, event.StateVersion, lastVersion[event.JobID]))
		}
		if event.FromState != "" && event.FromState != lastState[event.JobID] {
			report.EventViolations = append(report.EventViolations,
				fmt.Sprintf("%s event from state %s, folded state %s",
					event.JobID, event.FromState, lastState[event.JobID]))
		}
		lastState[event.JobID] = event.ToState
		lastVersion[event.JobID] = event.StateVersion
	}
	for jobID, job := range jobs {
		if lastState[jobID] != job.State || lastVersion[jobID] != job.StateVersion {
			report.EventViolations = append(report.EventViolations,
				fmt.Sprintf("%s event fold is %s/%d, jobs row is %s/%d",
					jobID, lastState[jobID], lastVersion[jobID], job.State, job.StateVersion))
		}
	}
}

func isTerminalState(state string) bool {
	switch strings.ToUpper(state) {
	case string(ModelSucceeded), string(ModelFailed), string(ModelDeadLetter):
		return true
	default:
		return false
	}
}

func isActiveAttemptState(state string) bool {
	switch strings.ToUpper(state) {
	case "ACTIVE", "LEASED", string(ModelScheduled), string(ModelRunning):
		return true
	default:
		return false
	}
}

func sortReport(report *ReconcileReport) {
	sort.Strings(report.Lost)
	sort.Strings(report.Orphans)
	sort.Strings(report.DuplicateAcceptedIDs)
	sort.Strings(report.ActiveJobs)
	sort.Strings(report.ActiveAttempts)
	sort.Strings(report.MaterializationViolations)
	sort.Strings(report.AttemptViolations)
	sort.Strings(report.EventViolations)
	sort.Strings(report.IntegrityFailures)
	sort.Strings(report.ForeignKeyFailures)
}

func (r ReconcileReport) Passed() bool {
	if len(r.Orphans) > 0 ||
		len(r.DuplicateAcceptedIDs) > 0 ||
		len(r.DuplicateCompletions) > 0 ||
		len(r.MaterializationViolations) > 0 ||
		len(r.AttemptViolations) > 0 ||
		len(r.EventViolations) > 0 ||
		len(r.IntegrityFailures) > 0 ||
		len(r.ForeignKeyFailures) > 0 {
		return false
	}
	if r.RequireTerminal && len(r.Lost) > 0 {
		return false
	}
	if r.RequireSucceeded && len(r.Unsuccessful) > 0 {
		return false
	}
	if r.RequireQuiescent && (len(r.ActiveJobs) > 0 || len(r.ActiveAttempts) > 0) {
		return false
	}
	return true
}

func (r ReconcileReport) Summary() string {
	return fmt.Sprintf(
		"accepted=%d terminal=%d lost=%d orphan=%d duplicates=%d unsuccessful=%d active_jobs=%d active_attempts=%d materialization=%d attempts=%d events=%d integrity=%d foreign_keys=%d",
		r.AcceptedCount,
		r.TerminalCount,
		len(r.Lost),
		len(r.Orphans),
		len(r.DuplicateCompletions),
		len(r.Unsuccessful),
		len(r.ActiveJobs),
		len(r.ActiveAttempts),
		len(r.MaterializationViolations),
		len(r.AttemptViolations),
		len(r.EventViolations),
		len(r.IntegrityFailures),
		len(r.ForeignKeyFailures),
	)
}
