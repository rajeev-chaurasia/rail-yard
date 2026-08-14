package evidence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteTimestampSource = "quiesced_sqlite_snapshot"

type databaseJob struct {
	ID           string
	State        string
	CreatedAt    time.Time
	StateVersion int64
}

type databaseCompletion struct {
	JobID       string
	State       string
	AttemptNo   int
	CommittedAt time.Time
}

type databaseAttempt struct {
	JobID      string
	AttemptNo  int
	Generation int64
	State      string
	LeasedAt   time.Time
}

type databaseEvent struct {
	Sequence     int64
	JobID        string
	State        string
	StateVersion int64
}

type databaseSnapshot struct {
	Jobs                    []databaseJob
	Completions             []databaseCompletion
	Attempts                []databaseAttempt
	Events                  []databaseEvent
	IntegrityFailures       []string
	ForeignKeyFailures      []string
	SlotReservationFailures []string
	Pragmas                 map[string]string
}

func StableIdempotencyKey(runID string, seed int64, index int) string {
	input := fmt.Sprintf("railyard-benchmark-v1\x00%s\x00%d\x00%d", runID, seed, index)
	sum := sha256Bytes([]byte(input))
	return "benchmark-v1-" + hex.EncodeToString(sum[:24])
}

func ReconcileSnapshot(
	ctx context.Context,
	databasePath string,
	manifest RunManifest,
	submissions []SubmissionSample,
	drainSamples []DrainSample,
) (ReconciliationReport, []BenchmarkSample, BenchmarkSummary, error) {
	digests, err := SQLiteSnapshotDigests(databasePath)
	if err != nil {
		return ReconciliationReport{}, nil, BenchmarkSummary{}, fmt.Errorf("hash database snapshot: %w", err)
	}
	db, err := openReadOnlySQLite(databasePath)
	if err != nil {
		return ReconciliationReport{}, nil, BenchmarkSummary{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	snapshot, err := readDatabaseSnapshot(ctx, db)
	if err != nil {
		return ReconciliationReport{}, nil, BenchmarkSummary{}, err
	}

	report := analyzeSnapshot(manifest, submissions, drainSamples, snapshot)
	report.SchemaVersion = SchemaVersion
	report.RunID = manifest.RunID
	report.DatabaseSHA256 = digests["database"]
	report.DatabaseFilesSHA256 = digests
	report.SQLitePragmas = snapshot.Pragmas
	report.Passed = reconciliationPassed(report)

	samples := buildBenchmarkSamples(manifest.RunID, snapshot, &report)
	report.Passed = reconciliationPassed(report)
	summary := buildRunSummary(manifest, report, snapshot, samples)
	return report, samples, summary, nil
}

func openReadOnlySQLite(path string) (*sql.DB, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database snapshot: %w", err)
	}
	slashPath := filepath.ToSlash(absolute)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	uri := url.URL{Scheme: "file", Path: slashPath}
	query := uri.Query()
	query.Set("mode", "ro")
	query.Add("_pragma", "query_only(ON)")
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "busy_timeout(5000)")
	uri.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, fmt.Errorf("open database snapshot: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping database snapshot: %w", err)
	}
	return db, nil
}

func readDatabaseSnapshot(ctx context.Context, db *sql.DB) (databaseSnapshot, error) {
	snapshot := databaseSnapshot{Pragmas: make(map[string]string)}
	if err := readPragmas(ctx, db, &snapshot); err != nil {
		return snapshot, err
	}
	if err := readIntegrity(ctx, db, &snapshot); err != nil {
		return snapshot, err
	}

	rows, err := db.QueryContext(ctx, `
		SELECT id, state, created_at, state_version
		FROM jobs
		ORDER BY id`)
	if err != nil {
		return snapshot, fmt.Errorf("query jobs: %w", err)
	}
	for rows.Next() {
		var job databaseJob
		var createdAt int64
		if err := rows.Scan(&job.ID, &job.State, &createdAt, &job.StateVersion); err != nil {
			_ = rows.Close()
			return snapshot, fmt.Errorf("scan jobs: %w", err)
		}
		job.CreatedAt = time.Unix(0, createdAt).UTC()
		snapshot.Jobs = append(snapshot.Jobs, job)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, fmt.Errorf("finish jobs: %w", err)
	}

	rows, err = db.QueryContext(ctx, `
		SELECT job_id, state, attempt_no, committed_at
		FROM job_completions
		ORDER BY job_id`)
	if err != nil {
		return snapshot, fmt.Errorf("query completions: %w", err)
	}
	for rows.Next() {
		var completion databaseCompletion
		var committedAt int64
		if err := rows.Scan(
			&completion.JobID,
			&completion.State,
			&completion.AttemptNo,
			&committedAt,
		); err != nil {
			_ = rows.Close()
			return snapshot, fmt.Errorf("scan completions: %w", err)
		}
		completion.CommittedAt = time.Unix(0, committedAt).UTC()
		snapshot.Completions = append(snapshot.Completions, completion)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, fmt.Errorf("finish completions: %w", err)
	}

	rows, err = db.QueryContext(ctx, `
		SELECT job_id, attempt_no, lease_generation, state, leased_at
		FROM attempts
		ORDER BY job_id, attempt_no`)
	if err != nil {
		return snapshot, fmt.Errorf("query attempts: %w", err)
	}
	for rows.Next() {
		var attempt databaseAttempt
		var leasedAt int64
		if err := rows.Scan(
			&attempt.JobID,
			&attempt.AttemptNo,
			&attempt.Generation,
			&attempt.State,
			&leasedAt,
		); err != nil {
			_ = rows.Close()
			return snapshot, fmt.Errorf("scan attempts: %w", err)
		}
		attempt.LeasedAt = time.Unix(0, leasedAt).UTC()
		snapshot.Attempts = append(snapshot.Attempts, attempt)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, fmt.Errorf("finish attempts: %w", err)
	}

	rows, err = db.QueryContext(ctx, `
		SELECT event_seq, job_id, state, state_version
		FROM events
		ORDER BY event_seq`)
	if err != nil {
		return snapshot, fmt.Errorf("query events: %w", err)
	}
	for rows.Next() {
		var event databaseEvent
		if err := rows.Scan(
			&event.Sequence,
			&event.JobID,
			&event.State,
			&event.StateVersion,
		); err != nil {
			_ = rows.Close()
			return snapshot, fmt.Errorf("scan events: %w", err)
		}
		snapshot.Events = append(snapshot.Events, event)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, fmt.Errorf("finish events: %w", err)
	}

	if err := readSlotReservations(ctx, db, &snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func readPragmas(ctx context.Context, db *sql.DB, snapshot *databaseSnapshot) error {
	for _, name := range []string{"journal_mode", "synchronous", "foreign_keys", "busy_timeout"} {
		var value string
		if err := db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&value); err != nil {
			return fmt.Errorf("read SQLite pragma %s: %w", name, err)
		}
		snapshot.Pragmas[name] = value
	}
	return nil
}

func readIntegrity(ctx context.Context, db *sql.DB, snapshot *databaseSnapshot) error {
	rows, err := db.QueryContext(ctx, "PRAGMA integrity_check")
	if err != nil {
		return fmt.Errorf("run integrity_check: %w", err)
	}
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan integrity_check: %w", err)
		}
		if result != "ok" {
			snapshot.IntegrityFailures = append(snapshot.IntegrityFailures, result)
		}
	}
	if err := closeRows(rows); err != nil {
		return fmt.Errorf("finish integrity_check: %w", err)
	}

	rows, err = db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("run foreign_key_check: %w", err)
	}
	for rows.Next() {
		var table, parent string
		var rowID, foreignKeyID any
		if err := rows.Scan(&table, &rowID, &parent, &foreignKeyID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan foreign_key_check: %w", err)
		}
		snapshot.ForeignKeyFailures = append(
			snapshot.ForeignKeyFailures,
			fmt.Sprintf("table=%s rowid=%v parent=%s fkid=%v", table, rowID, parent, foreignKeyID),
		)
	}
	return closeRows(rows)
}

func readSlotReservations(ctx context.Context, db *sql.DB, snapshot *databaseSnapshot) error {
	rows, err := db.QueryContext(ctx, `
		SELECT 'tenant', tenant_id, active_slots
		FROM tenant_limits
		WHERE active_slots <> 0
		UNION ALL
		SELECT 'queue', tenant_id || '/' || queue_name, active_slots
		FROM queue_state
		WHERE active_slots <> 0
		ORDER BY 1, 2`)
	if err != nil {
		return fmt.Errorf("query slot reservations: %w", err)
	}
	for rows.Next() {
		var kind, owner string
		var slots int
		if err := rows.Scan(&kind, &owner, &slots); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan slot reservations: %w", err)
		}
		snapshot.SlotReservationFailures = append(
			snapshot.SlotReservationFailures,
			fmt.Sprintf("%s %s retains %d slots", kind, owner, slots),
		)
	}
	return closeRows(rows)
}

func closeRows(rows *sql.Rows) error {
	err := rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func analyzeSnapshot(
	manifest RunManifest,
	submissions []SubmissionSample,
	drainSamples []DrainSample,
	snapshot databaseSnapshot,
) ReconciliationReport {
	report := ReconciliationReport{
		AcceptedCount:             len(submissions),
		DatabaseJobCount:          len(snapshot.Jobs),
		DurableLeaseGrantCount:    len(snapshot.Attempts),
		IntegrityFailures:         slices.Clone(snapshot.IntegrityFailures),
		ForeignKeyFailures:        slices.Clone(snapshot.ForeignKeyFailures),
		SlotReservationViolations: slices.Clone(snapshot.SlotReservationFailures),
	}
	report.IntegrityFailures = append(report.IntegrityFailures, validateSnapshotPragmas(snapshot.Pragmas)...)
	report.ManifestViolations = validateManifest(manifest, submissions, drainSamples)

	accepted := make(map[string]struct{}, len(submissions))
	for _, submission := range submissions {
		if submission.JobID == "" {
			continue
		}
		if _, exists := accepted[submission.JobID]; exists {
			report.DuplicateAcceptedJobIDs = append(report.DuplicateAcceptedJobIDs, submission.JobID)
		}
		accepted[submission.JobID] = struct{}{}
	}
	report.AcceptedCount = len(accepted)

	jobs := make(map[string]databaseJob, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		jobs[job.ID] = job
		if _, ok := accepted[job.ID]; !ok {
			report.OrphanJobIDs = append(report.OrphanJobIDs, job.ID)
		}
		if !terminalState(job.State) {
			report.ActiveJobIDs = append(report.ActiveJobIDs, job.ID)
		}
	}
	report.ActiveJobCount = len(report.ActiveJobIDs)
	for jobID := range accepted {
		if _, ok := jobs[jobID]; !ok {
			report.LostJobIDs = append(report.LostJobIDs, jobID)
		}
	}

	completionCounts := make(map[string]int)
	completionByJob := make(map[string]databaseCompletion)
	for _, completion := range snapshot.Completions {
		report.TerminalCount++
		completionCounts[completion.JobID]++
		completionByJob[completion.JobID] = completion
		job, hasJob := jobs[completion.JobID]
		if !hasJob {
			report.MaterializationViolations = append(
				report.MaterializationViolations,
				fmt.Sprintf("completion %s has no jobs row", completion.JobID),
			)
		} else if job.State != completion.State {
			report.MaterializationViolations = append(
				report.MaterializationViolations,
				fmt.Sprintf(
					"completion %s state %s disagrees with jobs state %s",
					completion.JobID,
					completion.State,
					job.State,
				),
			)
		}
		switch completion.State {
		case "SUCCEEDED":
			report.SucceededCount++
		case "FAILED":
			report.FailedCount++
		case "DEAD_LETTER":
			report.DeadLetterCount++
		}
	}
	for jobID, count := range completionCounts {
		if count > 1 {
			report.DuplicateCompletionJobIDs = append(report.DuplicateCompletionJobIDs, jobID)
		}
	}
	for jobID := range accepted {
		completion, ok := completionByJob[jobID]
		if !ok {
			if !slices.Contains(report.LostJobIDs, jobID) {
				report.LostJobIDs = append(report.LostJobIDs, jobID)
			}
			continue
		}
		if completion.State != "SUCCEEDED" {
			report.UnsuccessfulJobIDs = append(report.UnsuccessfulJobIDs, jobID)
		}
	}

	analyzeAttempts(snapshot.Attempts, &report)
	analyzeEvents(snapshot.Events, jobs, &report)
	sortReconciliation(&report)
	return report
}

func validateSnapshotPragmas(pragmas map[string]string) []string {
	expected := map[string][]string{
		"journal_mode": {"wal"},
		"synchronous":  {"2"},
		"foreign_keys": {"1"},
		"busy_timeout": {"5000"},
	}
	var violations []string
	for name, acceptedValues := range expected {
		value := strings.ToLower(strings.TrimSpace(pragmas[name]))
		if !slices.Contains(acceptedValues, value) {
			violations = append(
				violations,
				fmt.Sprintf("SQLite pragma %s got %q", name, value),
			)
		}
	}
	slices.Sort(violations)
	return violations
}

func validateManifest(
	manifest RunManifest,
	submissions []SubmissionSample,
	drainSamples []DrainSample,
) []string {
	var violations []string
	if manifest.SchemaVersion != SchemaVersion {
		violations = append(violations, "unsupported manifest schema version")
	}
	if manifest.RunID == "" {
		violations = append(violations, "run_id is empty")
	}
	if manifest.Phase != PhaseWarmup && manifest.Phase != PhaseMeasured {
		violations = append(violations, "phase must be warmup or measured")
	}
	if manifest.Scored != (manifest.Phase == PhaseMeasured) {
		violations = append(violations, "scored must be true only for measured runs")
	}
	if manifest.Config.JobCount != len(submissions) {
		violations = append(violations, fmt.Sprintf(
			"manifest job_count=%d, submission records=%d",
			manifest.Config.JobCount,
			len(submissions),
		))
	}
	if !manifest.DatabaseQuiesced {
		violations = append(violations, "database snapshot was not confirmed quiescent")
	}

	indexes := make(map[int]struct{}, len(submissions))
	keys := make(map[string]struct{}, len(submissions))
	for _, submission := range submissions {
		if submission.RunID != manifest.RunID {
			violations = append(violations, fmt.Sprintf("submission %d has a different run_id", submission.Index))
		}
		if submission.Error != "" {
			violations = append(violations, fmt.Sprintf("submission %d failed: %s", submission.Index, submission.Error))
		}
		if submission.Duplicate && !submission.AmbiguousRetry {
			violations = append(violations, fmt.Sprintf("submission %d reused an existing admission", submission.Index))
		}
		if submission.AttemptCount < 1 {
			violations = append(violations, fmt.Sprintf("submission %d has no HTTP attempts", submission.Index))
		}
		if _, duplicate := indexes[submission.Index]; duplicate {
			violations = append(violations, fmt.Sprintf("duplicate submission index %d", submission.Index))
		}
		indexes[submission.Index] = struct{}{}
		if _, duplicate := keys[submission.IdempotencyKey]; duplicate {
			violations = append(violations, fmt.Sprintf("duplicate idempotency key at index %d", submission.Index))
		}
		keys[submission.IdempotencyKey] = struct{}{}
		wantKey := StableIdempotencyKey(manifest.RunID, manifest.Config.Seed, submission.Index)
		if submission.IdempotencyKey != wantKey {
			violations = append(violations, fmt.Sprintf("submission %d has an unstable idempotency key", submission.Index))
		}
	}
	submittedJobs := make(map[string]struct{}, len(submissions))
	for _, submission := range submissions {
		if submission.JobID != "" {
			submittedJobs[submission.JobID] = struct{}{}
		}
	}
	drainedJobs := make(map[string]struct{}, len(drainSamples))
	for _, sample := range drainSamples {
		if sample.RunID != manifest.RunID {
			violations = append(violations, fmt.Sprintf("drain sample %s has a different run_id", sample.JobID))
		}
		if _, duplicate := drainedJobs[sample.JobID]; duplicate {
			violations = append(violations, fmt.Sprintf("duplicate drain sample for %s", sample.JobID))
		}
		drainedJobs[sample.JobID] = struct{}{}
		if _, submitted := submittedJobs[sample.JobID]; !submitted {
			violations = append(violations, fmt.Sprintf("drain sample contains unsubmitted job %s", sample.JobID))
		}
		if !terminalState(sample.State) || sample.TerminalAt == nil {
			violations = append(violations, fmt.Sprintf("drain sample %s is not terminal", sample.JobID))
		}
	}
	for jobID := range submittedJobs {
		if _, drained := drainedJobs[jobID]; !drained {
			violations = append(violations, fmt.Sprintf("submitted job %s lacks a drain sample", jobID))
		}
	}
	if manifest.Config.Qualification {
		violations = append(violations, validateQualificationEnvironment(manifest)...)
	}
	slices.Sort(violations)
	return slices.Compact(violations)
}

func validateQualificationEnvironment(manifest RunManifest) []string {
	environment := manifest.Environment
	requiredText := map[string]string{
		"git_commit":      environment.GitCommit,
		"go_version":      environment.GoVersion,
		"docker_version":  environment.DockerVersion,
		"compose_version": environment.ComposeVersion,
		"hostname":        environment.Hostname,
		"os":              environment.OS,
		"architecture":    environment.Architecture,
		"filesystem":      environment.Filesystem,
		"timezone":        environment.Timezone,
	}
	var violations []string
	for name, value := range requiredText {
		if strings.TrimSpace(value) == "" {
			violations = append(violations, "qualification environment missing "+name)
		}
	}
	if environment.GitDirty == nil {
		violations = append(violations, "qualification environment missing git_dirty")
	} else if *environment.GitDirty {
		violations = append(violations, "qualification requires a clean git worktree")
	}
	if environment.CPUCount < 1 {
		violations = append(violations, "qualification environment missing cpu_count")
	}
	if len(environment.BinaryDigests) < 2 {
		violations = append(violations, "qualification environment missing binary_digests")
	}
	if len(environment.ImageDigests) < 10 {
		violations = append(violations, "qualification environment missing image_digests")
	}
	if len(environment.SQLitePragmas) < 4 {
		violations = append(violations, "qualification environment missing sqlite_pragmas")
	}
	if manifest.Config.ConfigurationSHA256 == "" {
		violations = append(violations, "qualification run missing configuration_sha256")
	}
	if manifest.Config.JobCount != 5_000 {
		violations = append(violations, "qualification job_count must be 5000")
	}
	if manifest.Config.WorkerCount != 8 {
		violations = append(violations, "qualification worker_count must be 8")
	}
	if manifest.Config.WorkerSlots != 256 {
		violations = append(violations, "qualification worker_slots must be 256")
	}
	if strings.TrimSpace(environment.OperatorDetails["worker_count_evidence"]) == "" {
		violations = append(violations, "qualification environment missing worker_count_evidence")
	}
	expectedPragmas := map[string][]string{
		"journal_mode": {"wal"},
		"synchronous":  {"full", "2"},
		"foreign_keys": {"on", "1"},
		"busy_timeout": {"5000"},
	}
	for name, acceptedValues := range expectedPragmas {
		value := strings.ToLower(strings.TrimSpace(environment.SQLitePragmas[name]))
		if !slices.Contains(acceptedValues, value) {
			violations = append(
				violations,
				fmt.Sprintf("qualification sqlite_pragmas.%s has unsupported value %q", name, value),
			)
		}
	}
	return violations
}

func analyzeAttempts(attempts []databaseAttempt, report *ReconciliationReport) {
	lastAttempt := make(map[string]int)
	lastGeneration := make(map[string]int64)
	counts := make(map[string]int)
	for _, attempt := range attempts {
		counts[attempt.JobID]++
		if attempt.AttemptNo <= lastAttempt[attempt.JobID] {
			report.AttemptViolations = append(
				report.AttemptViolations,
				fmt.Sprintf("%s attempt numbers are not strictly increasing", attempt.JobID),
			)
		}
		if attempt.Generation <= lastGeneration[attempt.JobID] {
			report.AttemptViolations = append(
				report.AttemptViolations,
				fmt.Sprintf("%s lease generations are not strictly increasing", attempt.JobID),
			)
		}
		lastAttempt[attempt.JobID] = attempt.AttemptNo
		lastGeneration[attempt.JobID] = attempt.Generation
		if attempt.State == "LEASED" || attempt.State == "RUNNING" {
			report.ActiveAttemptIDs = append(
				report.ActiveAttemptIDs,
				fmt.Sprintf("%s/%d/%d", attempt.JobID, attempt.AttemptNo, attempt.Generation),
			)
		}
	}
	for _, count := range counts {
		if count > 1 {
			report.RepeatedAttemptCount += count - 1
		}
	}
	report.ActiveAttemptCount = len(report.ActiveAttemptIDs)
}

func analyzeEvents(events []databaseEvent, jobs map[string]databaseJob, report *ReconciliationReport) {
	lastState := make(map[string]string)
	lastVersion := make(map[string]int64)
	for index, event := range events {
		wantSequence := int64(index + 1)
		if event.Sequence != wantSequence {
			report.EventViolations = append(
				report.EventViolations,
				fmt.Sprintf("event sequence got %d, want %d", event.Sequence, wantSequence),
			)
		}
		if event.StateVersion != lastVersion[event.JobID]+1 {
			report.EventViolations = append(
				report.EventViolations,
				fmt.Sprintf(
					"%s event version got %d after %d",
					event.JobID,
					event.StateVersion,
					lastVersion[event.JobID],
				),
			)
		}
		lastState[event.JobID] = event.State
		lastVersion[event.JobID] = event.StateVersion
	}
	for jobID, job := range jobs {
		if lastState[jobID] != job.State || lastVersion[jobID] != job.StateVersion {
			report.EventViolations = append(
				report.EventViolations,
				fmt.Sprintf(
					"%s event fold is %s/%d, jobs row is %s/%d",
					jobID,
					lastState[jobID],
					lastVersion[jobID],
					job.State,
					job.StateVersion,
				),
			)
		}
	}
}

func buildBenchmarkSamples(
	runID string,
	snapshot databaseSnapshot,
	report *ReconciliationReport,
) []BenchmarkSample {
	jobs := make(map[string]databaseJob, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		jobs[job.ID] = job
	}
	attempts := make(map[string][]databaseAttempt)
	attemptByNumber := make(map[string]map[int]databaseAttempt)
	for _, attempt := range snapshot.Attempts {
		attempts[attempt.JobID] = append(attempts[attempt.JobID], attempt)
		if attemptByNumber[attempt.JobID] == nil {
			attemptByNumber[attempt.JobID] = make(map[int]databaseAttempt)
		}
		attemptByNumber[attempt.JobID][attempt.AttemptNo] = attempt
	}

	var samples []BenchmarkSample
	for _, completion := range snapshot.Completions {
		job, hasJob := jobs[completion.JobID]
		jobAttempts := attempts[completion.JobID]
		completionAttempt, hasCompletionAttempt := attemptByNumber[completion.JobID][completion.AttemptNo]
		if !hasJob || len(jobAttempts) == 0 || !hasCompletionAttempt {
			report.AttemptViolations = append(
				report.AttemptViolations,
				fmt.Sprintf("%s lacks a complete admission, lease, and completion timeline", completion.JobID),
			)
			continue
		}
		firstLease := jobAttempts[0].LeasedAt
		admissionToLease := firstLease.Sub(job.CreatedAt)
		leaseToCompletion := completion.CommittedAt.Sub(completionAttempt.LeasedAt)
		endToEnd := completion.CommittedAt.Sub(job.CreatedAt)
		if admissionToLease < 0 || leaseToCompletion < 0 || endToEnd < 0 {
			report.AttemptViolations = append(
				report.AttemptViolations,
				fmt.Sprintf("%s has a negative lifecycle duration", completion.JobID),
			)
			continue
		}
		samples = append(samples, BenchmarkSample{
			SchemaVersion:               SchemaVersion,
			RunID:                       runID,
			JobID:                       completion.JobID,
			AdmissionCommittedAt:        job.CreatedAt,
			FirstLeaseCommittedAt:       firstLease,
			CompletionLeaseCommittedAt:  completionAttempt.LeasedAt,
			CompletionCommittedAt:       completion.CommittedAt,
			AdmissionToFirstLease:       admissionToLease,
			CompletionLeaseToCompletion: leaseToCompletion,
			EndToEnd:                    endToEnd,
			AttemptCount:                len(jobAttempts),
			CompletionState:             completion.State,
			TimestampSource:             sqliteTimestampSource,
		})
	}
	slices.SortFunc(samples, func(left, right BenchmarkSample) int {
		return strings.Compare(left.JobID, right.JobID)
	})
	sortReconciliation(report)
	return samples
}

func buildRunSummary(
	manifest RunManifest,
	report ReconciliationReport,
	snapshot databaseSnapshot,
	samples []BenchmarkSample,
) BenchmarkSummary {
	admissionTimes := make([]time.Time, 0, len(snapshot.Jobs))
	for _, job := range snapshot.Jobs {
		admissionTimes = append(admissionTimes, job.CreatedAt)
	}
	leaseTimes := make([]time.Time, 0, len(snapshot.Attempts))
	for _, attempt := range snapshot.Attempts {
		leaseTimes = append(leaseTimes, attempt.LeasedAt)
	}
	completionTimes := make([]time.Time, 0, len(snapshot.Completions))
	for _, completion := range snapshot.Completions {
		if completion.State == "SUCCEEDED" {
			completionTimes = append(completionTimes, completion.CommittedAt)
		}
	}

	admissionToLease := make([]time.Duration, 0, len(samples))
	leaseToCompletion := make([]time.Duration, 0, len(samples))
	endToEnd := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		admissionToLease = append(admissionToLease, sample.AdmissionToFirstLease)
		leaseToCompletion = append(leaseToCompletion, sample.CompletionLeaseToCompletion)
		endToEnd = append(endToEnd, sample.EndToEnd)
	}

	return BenchmarkSummary{
		SchemaVersion:               SchemaVersion,
		RunID:                       manifest.RunID,
		Phase:                       manifest.Phase,
		Valid:                       report.Passed,
		InvalidReasons:              reconciliationReasons(report),
		Admissions:                  summarizeRate(admissionTimes),
		DurableLeaseGrants:          summarizeRate(leaseTimes),
		SuccessfulCompletions:       summarizeRate(completionTimes),
		AdmissionToFirstLease:       summarizeLatency(admissionToLease),
		CompletionLeaseToCompletion: summarizeLatency(leaseToCompletion),
		EndToEnd:                    summarizeLatency(endToEnd),
		CanonicalJobCount:           report.SucceededCount,
		DurableLeaseGrantCount:      report.DurableLeaseGrantCount,
		RepeatedAttemptCount:        report.RepeatedAttemptCount,
	}
}

func summarizeRate(timestamps []time.Time) RateSample {
	if len(timestamps) < 2 {
		return RateSample{
			Available:         false,
			Count:             len(timestamps),
			UnavailableReason: "at least two durable timestamps are required",
		}
	}
	slices.SortFunc(timestamps, func(left, right time.Time) int {
		return left.Compare(right)
	})
	rate, err := RatePerMinute(len(timestamps), timestamps[0], timestamps[len(timestamps)-1])
	if err != nil {
		return RateSample{
			Available:         false,
			Count:             len(timestamps),
			UnavailableReason: err.Error(),
		}
	}
	first := timestamps[0]
	last := timestamps[len(timestamps)-1]
	return RateSample{
		Available:        true,
		Count:            len(timestamps),
		FirstCommittedAt: &first,
		LastCommittedAt:  &last,
		Interval:         last.Sub(first),
		PerMinute:        &rate,
		Source:           sqliteTimestampSource,
	}
}

func summarizeLatency(durations []time.Duration) LatencySummary {
	distribution, err := SummarizeDurations(durations)
	if err != nil {
		return LatencySummary{Available: false, UnavailableReason: err.Error()}
	}
	return LatencySummary{
		Available:    true,
		Distribution: &distribution,
		Source:       sqliteTimestampSource,
	}
}

func reconciliationPassed(report ReconciliationReport) bool {
	return report.OperationalError == "" &&
		report.AcceptedCount > 0 &&
		report.AcceptedCount == report.DatabaseJobCount &&
		report.AcceptedCount == report.TerminalCount &&
		report.AcceptedCount == report.SucceededCount &&
		report.FailedCount == 0 &&
		report.DeadLetterCount == 0 &&
		report.ActiveJobCount == 0 &&
		report.ActiveAttemptCount == 0 &&
		len(report.LostJobIDs) == 0 &&
		len(report.OrphanJobIDs) == 0 &&
		len(report.DuplicateAcceptedJobIDs) == 0 &&
		len(report.DuplicateCompletionJobIDs) == 0 &&
		len(report.UnsuccessfulJobIDs) == 0 &&
		len(report.MaterializationViolations) == 0 &&
		len(report.AttemptViolations) == 0 &&
		len(report.EventViolations) == 0 &&
		len(report.SlotReservationViolations) == 0 &&
		len(report.IntegrityFailures) == 0 &&
		len(report.ForeignKeyFailures) == 0 &&
		len(report.ManifestViolations) == 0
}

func reconciliationReasons(report ReconciliationReport) []string {
	if report.Passed {
		return nil
	}
	counts := []struct {
		name  string
		count int
	}{
		{name: "lost jobs", count: len(report.LostJobIDs)},
		{name: "orphan jobs", count: len(report.OrphanJobIDs)},
		{name: "duplicate accepted IDs", count: len(report.DuplicateAcceptedJobIDs)},
		{name: "duplicate completions", count: len(report.DuplicateCompletionJobIDs)},
		{name: "unsuccessful jobs", count: len(report.UnsuccessfulJobIDs)},
		{name: "active jobs", count: report.ActiveJobCount},
		{name: "active attempts", count: report.ActiveAttemptCount},
		{name: "materialization violations", count: len(report.MaterializationViolations)},
		{name: "attempt violations", count: len(report.AttemptViolations)},
		{name: "event violations", count: len(report.EventViolations)},
		{name: "slot reservation violations", count: len(report.SlotReservationViolations)},
		{name: "integrity failures", count: len(report.IntegrityFailures)},
		{name: "foreign key failures", count: len(report.ForeignKeyFailures)},
		{name: "manifest violations", count: len(report.ManifestViolations)},
	}
	var reasons []string
	if report.AcceptedCount != report.DatabaseJobCount {
		reasons = append(reasons, fmt.Sprintf(
			"accepted count %d does not match database job count %d",
			report.AcceptedCount,
			report.DatabaseJobCount,
		))
	}
	if report.AcceptedCount != report.TerminalCount {
		reasons = append(reasons, fmt.Sprintf(
			"accepted count %d does not match terminal count %d",
			report.AcceptedCount,
			report.TerminalCount,
		))
	}
	if report.AcceptedCount != report.SucceededCount {
		reasons = append(reasons, fmt.Sprintf(
			"accepted count %d does not match succeeded count %d",
			report.AcceptedCount,
			report.SucceededCount,
		))
	}
	for _, count := range counts {
		if count.count > 0 {
			reasons = append(reasons, fmt.Sprintf("%s: %d", count.name, count.count))
		}
	}
	slices.Sort(reasons)
	return reasons
}

func sortReconciliation(report *ReconciliationReport) {
	slices.Sort(report.LostJobIDs)
	report.LostJobIDs = slices.Compact(report.LostJobIDs)
	slices.Sort(report.OrphanJobIDs)
	report.OrphanJobIDs = slices.Compact(report.OrphanJobIDs)
	slices.Sort(report.DuplicateAcceptedJobIDs)
	report.DuplicateAcceptedJobIDs = slices.Compact(report.DuplicateAcceptedJobIDs)
	slices.Sort(report.DuplicateCompletionJobIDs)
	report.DuplicateCompletionJobIDs = slices.Compact(report.DuplicateCompletionJobIDs)
	slices.Sort(report.UnsuccessfulJobIDs)
	report.UnsuccessfulJobIDs = slices.Compact(report.UnsuccessfulJobIDs)
	slices.Sort(report.ActiveJobIDs)
	slices.Sort(report.ActiveAttemptIDs)
	slices.Sort(report.MaterializationViolations)
	report.MaterializationViolations = slices.Compact(report.MaterializationViolations)
	slices.Sort(report.AttemptViolations)
	report.AttemptViolations = slices.Compact(report.AttemptViolations)
	slices.Sort(report.EventViolations)
	report.EventViolations = slices.Compact(report.EventViolations)
	slices.Sort(report.SlotReservationViolations)
	slices.Sort(report.IntegrityFailures)
	slices.Sort(report.ForeignKeyFailures)
	slices.Sort(report.ManifestViolations)
}

func terminalState(state string) bool {
	return state == "SUCCEEDED" || state == "FAILED" || state == "DEAD_LETTER"
}

func sha256Bytes(body []byte) [32]byte {
	return sha256.Sum256(body)
}
