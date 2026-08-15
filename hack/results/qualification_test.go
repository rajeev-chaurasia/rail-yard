package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

func TestNormalizeSuiteArtifactPathsPreservesPublishedReferences(t *testing.T) {
	expected := evidence.SuiteSummary{
		Warmup: evidence.SuiteRun{ArtifactPath: "runs/warmup"},
		MeasuredRuns: []evidence.SuiteRun{
			{ArtifactPath: "runs/measured-01"},
			{ArtifactPath: "runs/measured-02"},
		},
	}
	recomputed := evidence.SuiteSummary{
		Warmup: evidence.SuiteRun{ArtifactPath: `C:\absolute\warmup`},
		MeasuredRuns: []evidence.SuiteRun{
			{ArtifactPath: `C:\absolute\measured-01`},
			{ArtifactPath: `C:\absolute\measured-02`},
		},
	}

	normalizeSuiteArtifactPaths(&recomputed, expected)

	if recomputed.Warmup.ArtifactPath != expected.Warmup.ArtifactPath {
		t.Fatalf("warmup path = %q", recomputed.Warmup.ArtifactPath)
	}
	for index := range expected.MeasuredRuns {
		if recomputed.MeasuredRuns[index].ArtifactPath != expected.MeasuredRuns[index].ArtifactPath {
			t.Fatalf("measured path %d = %q", index, recomputed.MeasuredRuns[index].ArtifactPath)
		}
	}
}

func TestValidateReplayDoesNotRoundMissIntoCompliance(t *testing.T) {
	t.Parallel()

	input := validReplaySummary()
	input.ByteMatchPercent = 99.999999
	input.Passed = false
	path := writeCheckedJSON(t, input)

	result, digest, reasons := validateReplay(path)
	if len(reasons) != 0 {
		t.Fatalf("validateReplay reasons = %v", reasons)
	}
	if !result.EvidenceValid || result.Qualified {
		t.Fatalf("validateReplay result = %#v", result)
	}
	if digest == "" {
		t.Fatal("validateReplay returned an empty input digest")
	}
}

func TestValidateReplayRequiresStatusToMatchMeasurements(t *testing.T) {
	t.Parallel()

	input := validReplaySummary()
	input.Passed = false
	path := writeCheckedJSON(t, input)

	result, _, reasons := validateReplay(path)
	if result.EvidenceValid {
		t.Fatalf("validateReplay accepted inconsistent status: %#v", result)
	}
	if !containsReason(reasons, "passed status") {
		t.Fatalf("validateReplay reasons = %v", reasons)
	}
}

func TestValidateReplayRejectsNonexactDecisionCount(t *testing.T) {
	t.Parallel()

	input := validReplaySummary()
	input.Decisions++
	path := writeCheckedJSON(t, input)

	result, _, reasons := validateReplay(path)
	if result.EvidenceValid || !containsReason(reasons, "exactly 50000 decisions") {
		t.Fatalf("validateReplay result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateSLOAcceptsExactRuleEvidence(t *testing.T) {
	t.Parallel()

	input := validSLOSummary()
	path := writeCheckedJSON(t, input)

	result, digest, reasons := validateSLO(path)
	if len(reasons) != 0 {
		t.Fatalf("validateSLO reasons = %v", reasons)
	}
	if !result.EvidenceValid || !result.Qualified || digest == "" {
		t.Fatalf("validateSLO result = %#v, digest = %q", result, digest)
	}
}

func TestValidateSLORejectsMissingPromtoolLogs(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "slo-summary.json")
	writeJSONFixture(t, path, validSLOSummary())
	if err := evidence.GenerateChecksums(directory); err != nil {
		t.Fatalf("generate checksums: %v", err)
	}

	result, _, reasons := validateSLO(path)
	if result.EvidenceValid || !containsReason(reasons, "promtool-check.log") {
		t.Fatalf("validateSLO result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateP5AcceptsLifecycleWithDeterministicRules(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	input := p5Summary{
		RunID:                  "qualification-1",
		Actor:                  "qualification",
		StartedAt:              now,
		CompletedAt:            now.Add(time.Minute),
		WorkflowJobIDs:         []string{"root", "middle", "leaf"},
		ReassignedJobID:        "reassigned",
		ReassignmentObservedIn: time.Second,
		DeadLetterJobID:        "dead-letter",
		RedrivenJobID:          "redriven",
		AuditEventCount:        6,
		LiveAlertWaitsSkipped:  true,
		SLORuleEvidence:        "slo-summary.json",
		Passed:                 true,
	}
	path, sloPath := writeCheckedP5Evidence(t, input)

	result, _, reasons := validateP5(path, sloPath, true)
	if len(reasons) != 0 {
		t.Fatalf("validateP5 reasons = %v", reasons)
	}
	if !result.EvidenceValid || !result.Qualified {
		t.Fatalf("validateP5 result = %#v", result)
	}
	if !result.DeterministicRulesValidated {
		t.Fatal("validateP5 did not link deterministic rule evidence")
	}
}

func TestVerifyInputRejectsTampering(t *testing.T) {
	t.Parallel()

	path := writeCheckedJSON(t, validReplaySummary())
	if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("tamper input: %v", err)
	}

	if _, err := verifyInput(path); err == nil {
		t.Fatal("verifyInput accepted a modified summary")
	}
}

func TestWriteNewAtomicPublishesOnce(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "nested", "qualification.json")
	body := []byte("{\"status\":\"qualified\"}\n")
	if err := writeNewAtomic(path, body); err != nil {
		t.Fatalf("writeNewAtomic: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("output = %q, want %q", got, body)
	}
	if err := writeNewAtomic(path, []byte("replacement")); err == nil {
		t.Fatal("writeNewAtomic replaced an existing output")
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("read retained output: %v", err)
	}
	if string(got) != string(body) {
		t.Fatalf("existing output changed to %q", got)
	}
}

func TestActivationTextPreservesMeasuredValues(t *testing.T) {
	t.Parallel()

	summary := qualificationSummary{
		Throughput: throughputResult{LeaseGrantsPerMinute: 10000.0000001},
		Chaos: chaosResult{
			ReconciledRuns:  1,
			RequiredRuns:    1,
			RecoverySamples: 7,
			RecoveryP99NS:   int64(4999999999 * time.Nanosecond),
		},
		Replay: replayResult{ByteMatchPercent: 100},
	}
	got := activationText(summary)
	for _, want := range []string{"10000.0000001", "1/1", "4999.999999ms", "n=7", "100%"} {
		if !strings.Contains(got, want) {
			t.Fatalf("activationText = %q, missing %q", got, want)
		}
	}
}

func TestValidateChaosAcceptsV3DurableRecoveryEvidence(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{})
	result, digest, reasons := validateChaos(path)
	if len(reasons) != 0 {
		t.Fatalf("validateChaos reasons = %v", reasons)
	}
	if !result.EvidenceValid ||
		!result.CorrectnessQualified ||
		!result.RecoveryQualified ||
		result.Runs != requiredChaosRuns ||
		result.ReconciledRuns != requiredChaosRuns ||
		result.RecoverySamples != 1 ||
		result.RecoveryP99NS != int64(time.Second) {
		t.Fatalf("validateChaos result = %#v", result)
	}
	if digest == "" {
		t.Fatal("validateChaos returned an empty input digest")
	}
}

func TestValidateChaosRejectsV2Manifest(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{manifestVersion: 2})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "manifest version=2, want 3") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsMixedManifestFormats(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{v2ManifestRun: 1})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "run 1: manifest version=2, want 3") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsMissingAffectedLeaseSample(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{missingSampleRun: 1})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "affected leases") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsExcessiveClockUncertainty(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{
		clockUncertainty: maxClockUncertainty + time.Nanosecond,
	})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "uncertainty") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsChecksumCorruption(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{corruptRun: 1})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "checksums") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsMixedConfigurationHashes(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{
		campaignRuns:          2,
		mixedConfigurationRun: 2,
	})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "configuration_hash") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsOldPortfolioShape(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{
		campaignRuns: 10,
		jobs:         50_000,
	})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid ||
		!containsReason(reasons, "campaign has 10 runs, want 1") ||
		!containsReason(reasons, "exact chaos qualification workload") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsExtraWorkerKill(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{workerKills: requiredWorkerKills + 1})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "exact chaos qualification workload") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosRejectsHostObservedRecoveryTiming(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{
		successorObservedAfter: 4 * time.Second,
		reportedRecovery:       4 * time.Second,
	})
	result, _, reasons := validateChaos(path)
	if result.EvidenceValid || !containsReason(reasons, "durable successor timing") {
		t.Fatalf("validateChaos result = %#v, reasons = %v", result, reasons)
	}
}

func TestValidateChaosPreservesDurableRecoveryMiss(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{
		durableRecovery:  6 * time.Second,
		reportedRecovery: 6 * time.Second,
	})
	result, _, reasons := validateChaos(path)
	if len(reasons) != 0 {
		t.Fatalf("validateChaos reasons = %v", reasons)
	}
	if !result.EvidenceValid || !result.CorrectnessQualified || result.RecoveryQualified {
		t.Fatalf("validateChaos result = %#v", result)
	}
}

func TestValidateChaosPreservesReconciliationMiss(t *testing.T) {
	t.Parallel()

	path := writeChaosCampaign(t, chaosFixtureOptions{reconciliationMissRun: 1})
	result, _, reasons := validateChaos(path)
	if len(reasons) != 0 {
		t.Fatalf("validateChaos reasons = %v", reasons)
	}
	if !result.EvidenceValid ||
		result.CorrectnessQualified ||
		!result.RecoveryQualified ||
		result.ReconciledRuns != requiredChaosRuns-1 {
		t.Fatalf("validateChaos result = %#v", result)
	}
}

func TestRunRequiresEveryInput(t *testing.T) {
	t.Parallel()

	err := run(nil, &strings.Builder{}, &strings.Builder{})
	if err == nil || !strings.Contains(err.Error(), "--benchmark-suite is required") {
		t.Fatalf("run error = %v", err)
	}

	err = run([]string{"-help"}, &strings.Builder{}, &strings.Builder{})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("run help error = %v", err)
	}
}

type chaosFixtureOptions struct {
	manifestVersion        int
	v2ManifestRun          int
	missingSampleRun       int
	clockUncertainty       time.Duration
	corruptRun             int
	campaignRuns           int
	jobs                   int
	workerKills            int
	mixedConfigurationRun  int
	reconciliationMissRun  int
	durableRecovery        time.Duration
	successorObservedAfter time.Duration
	reportedRecovery       time.Duration
}

func writeChaosCampaign(t *testing.T, options chaosFixtureOptions) string {
	t.Helper()

	root := t.TempDir()
	version := options.manifestVersion
	if version == 0 {
		version = chaosManifestVersion
	}
	uncertainty := options.clockUncertainty
	if uncertainty == 0 {
		uncertainty = 5 * time.Millisecond
	}
	durableRecovery := options.durableRecovery
	if durableRecovery == 0 {
		durableRecovery = time.Second
	}
	observedAfter := options.successorObservedAfter
	if observedAfter == 0 {
		observedAfter = durableRecovery + 5*time.Millisecond
	}
	reportedRecovery := options.reportedRecovery
	if reportedRecovery == 0 {
		reportedRecovery = durableRecovery
	}
	runCount := options.campaignRuns
	if runCount == 0 {
		runCount = requiredChaosRuns
	}
	jobs := options.jobs
	if jobs == 0 {
		jobs = requiredJobsPerRun
	}
	workerKills := options.workerKills
	if workerKills == 0 {
		workerKills = requiredWorkerKills
	}

	started := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	campaign := campaignSummary{
		Version:     schemaVersion,
		StartedAt:   started,
		CompletedAt: started.Add(2 * time.Hour),
		Passed: durableRecovery < recoveryTarget &&
			options.reconciliationMissRun == 0 &&
			runCount == requiredChaosRuns &&
			jobs == requiredJobsPerRun &&
			workerKills == requiredWorkerKills,
		RecoverySamples:  runCount,
		RecoveryP99MS:    durationMilliseconds(reportedRecovery),
		RecoveryTargetMS: durationMilliseconds(recoveryTarget),
		Runs:             make([]runSummary, 0, runCount),
	}
	for run := 1; run <= runCount; run++ {
		runStarted := started.Add(time.Duration(run) * time.Minute)
		directory := filepath.Join(root, fmt.Sprintf("run-%02d", run))
		if err := os.MkdirAll(filepath.Join(directory, "database"), 0o755); err != nil {
			t.Fatalf("create chaos database directory: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(directory, "logs"), 0o755); err != nil {
			t.Fatalf("create chaos logs directory: %v", err)
		}
		configurationHash := strings.Repeat("a", 64)
		if run == options.mixedConfigurationRun {
			configurationHash = strings.Repeat("b", 64)
		}
		manifest := chaosManifest{
			Version:              version,
			Run:                  run,
			Seed:                 int64(1000 + run),
			Project:              fmt.Sprintf("chaos-%02d", run),
			TenantID:             fmt.Sprintf("tenant-%02d", run),
			Queue:                "noop",
			Jobs:                 jobs,
			Workers:              chaosWorkers(),
			WorkerKills:          workerKills,
			SubmitConcurrency:    16,
			JobDuration:          250 * time.Millisecond,
			ServerKillTarget:     0.5,
			ServerKillTargetJobs: jobs / 2,
			MaxRecovery:          recoveryTarget,
			ComposeFile:          "deploy/compose.yaml",
			ConfigurationHash:    configurationHash,
			StartedAt:            runStarted,
			CompletedAt:          runStarted.Add(time.Minute),
		}
		if run == options.v2ManifestRun {
			manifest.Version = 2
		}
		writeJSONFixture(t, filepath.Join(directory, "manifest.json"), manifest)

		report := reconcile.Report{
			Version:        schemaVersion,
			GeneratedAt:    runStarted.Add(time.Minute),
			Passed:         true,
			ExpectedJobs:   jobs,
			ViolationCount: 0,
			Counts: reconcile.Counts{
				Accepted:         jobs,
				Jobs:             jobs,
				Completions:      jobs,
				Attempts:         jobs + 1,
				AttemptRepeats:   1,
				Events:           jobs * 4,
				IdempotencyRows:  jobs,
				StateCounts:      map[string]int{"SUCCEEDED": jobs},
				ActiveJobs:       0,
				ActiveAttempts:   0,
				SlotReservations: 0,
			},
		}
		for _, name := range requiredReconciliationChecks {
			check := reconcile.Check{
				Name:   name,
				Passed: true,
			}
			if run == options.reconciliationMissRun && name == "canonical_ledger" {
				check.Passed = false
				check.Violations = 1
				report.Passed = false
				report.ViolationCount = 1
				report.Violations = []reconcile.Violation{{
					Check:   name,
					Message: "accepted job has no canonical completion",
					JobID:   fmt.Sprintf("job-%02d", run),
				}}
				report.Counts.Jobs--
				report.Counts.Completions--
				report.Counts.StateCounts["SUCCEEDED"]--
			}
			report.Checks = append(report.Checks, check)
		}
		writeJSONFixture(t, filepath.Join(directory, "reconciliation.json"), report)

		killHostAt := runStarted.Add(10 * time.Second)
		mapping := fixtureClockMapping(killHostAt, uncertainty)
		lease := activeLease{
			JobID:      fmt.Sprintf("job-%02d", run),
			AttemptNo:  1,
			Generation: 1,
			LeasedAt:   killHostAt.Add(-time.Second),
		}
		events := make([]chaosEvent, 0, workerKills+1)
		for sequence := 1; sequence <= workerKills; sequence++ {
			details := workerKillDetails{
				KillSequence:    sequence,
				VictimContainer: fmt.Sprintf("container-%02d", sequence),
				KillConfirmedAt: killHostAt,
				ClockMapping:    mapping,
				PreKillLeases:   []activeLease{},
				AffectedLeases:  []activeLease{},
			}
			if sequence == 1 {
				details.PreKillLeases = []activeLease{lease}
				details.AffectedLeases = []activeLease{lease}
			}
			events = append(events, chaosEvent{
				Sequence:   sequence,
				Type:       "worker_killed",
				ObservedAt: killHostAt,
				Service:    fmt.Sprintf("worker-%d", (sequence-1)%requiredWorkers+1),
				Details:    mustJSON(t, details),
			})
		}
		events = append(events, chaosEvent{
			Sequence:   workerKills + 1,
			Type:       "server_killed",
			ObservedAt: killHostAt,
		})
		writeJSONLinesFixture(t, filepath.Join(directory, "events.jsonl"), events)

		samples := []recoverySample{}
		if run != options.missingSampleRun {
			successorLeasedAt := killHostAt.Add(durableRecovery)
			samples = append(samples, recoverySample{
				KillSequence:        1,
				Worker:              "worker-1",
				VictimContainerID:   "container-01",
				JobID:               lease.JobID,
				KilledAttempt:       lease.AttemptNo,
				KilledGeneration:    lease.Generation,
				KillConfirmedHostAt: killHostAt,
				KillConfirmedAt:     killHostAt,
				ClockMapping:        mapping,
				SuccessorAttempt:    2,
				SuccessorGeneration: 2,
				SuccessorLeasedAt:   successorLeasedAt,
				SuccessorObservedAt: killHostAt.Add(observedAfter),
				CompletionAt:        successorLeasedAt.Add(time.Second),
				RecoveryMS:          durationMilliseconds(reportedRecovery),
			})
		}
		writeJSONLinesFixture(t, filepath.Join(directory, "recovery-samples.jsonl"), samples)
		if err := os.WriteFile(
			filepath.Join(directory, "submitted.jsonl"),
			[]byte("checked by reconciliation\n"),
			0o644,
		); err != nil {
			t.Fatalf("write submitted fixture: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(directory, "logs", "compose.log"),
			[]byte("checked logs\n"),
			0o644,
		); err != nil {
			t.Fatalf("write log fixture: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(directory, "database", "railyard.db"),
			[]byte("checked snapshot\n"),
			0o644,
		); err != nil {
			t.Fatalf("write database fixture: %v", err)
		}
		if err := evidence.GenerateChecksums(directory); err != nil {
			t.Fatalf("generate chaos checksums: %v", err)
		}
		if run == options.corruptRun {
			file, err := os.OpenFile(
				filepath.Join(directory, "events.jsonl"),
				os.O_APPEND|os.O_WRONLY,
				0,
			)
			if err != nil {
				t.Fatalf("open corrupt chaos artifact: %v", err)
			}
			if _, err := file.WriteString("{}\n"); err != nil {
				_ = file.Close()
				t.Fatalf("corrupt chaos artifact: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close corrupt chaos artifact: %v", err)
			}
		}
		campaign.Runs = append(campaign.Runs, runSummary{
			Run:                run,
			Seed:               manifest.Seed,
			Project:            manifest.Project,
			TenantID:           manifest.TenantID,
			StartedAt:          manifest.StartedAt,
			CompletedAt:        manifest.CompletedAt,
			Accepted:           jobs,
			WorkerKills:        workerKills,
			ServerKills:        1,
			RecoverySamples:    1,
			RecoveryP99MS:      durationMilliseconds(reportedRecovery),
			RecoveryTargetMS:   durationMilliseconds(recoveryTarget),
			ReconciliationPass: run != options.reconciliationMissRun,
			ArtifactDirectory:  directory,
		})
	}
	path := filepath.Join(root, "summary.json")
	writeJSONFixture(t, path, campaign)
	return path
}

func fixtureClockMapping(hostTime time.Time, uncertainty time.Duration) clockMapping {
	return clockMapping{
		HostLowerBound: hostTime.Add(-uncertainty),
		HostUpperBound: hostTime.Add(uncertainty),
		ServerTime:     hostTime,
		Offset:         0,
		Uncertainty:    uncertainty,
	}
}

func chaosWorkers() []string {
	workers := make([]string, requiredWorkers)
	for index := range workers {
		workers[index] = fmt.Sprintf("worker-%d", index+1)
	}
	return workers
}

func writeJSONFixture(t *testing.T, path string, value any) {
	t.Helper()
	body := mustJSON(t, value)
	body = append(body, '\n')
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write JSON fixture: %v", err)
	}
}

func writeJSONLinesFixture[T any](t *testing.T, path string, values []T) {
	t.Helper()
	var body []byte
	for _, value := range values {
		body = append(body, mustJSON(t, value)...)
		body = append(body, '\n')
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write JSON Lines fixture: %v", err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return body
}

func validReplaySummary() replaySummary {
	return replaySummary{
		SchemaVersion:       schemaVersion,
		GeneratedAt:         time.Date(2026, 8, 14, 10, 28, 0, 0, time.UTC),
		GoVersion:           "go1.26.6",
		OS:                  "linux",
		Architecture:        "amd64",
		Decisions:           requiredReplayDecisions,
		CleanProcessReplays: requiredReplays,
		ByteMatchPercent:    replayMatchTarget,
		SHA256:              strings.Repeat("a", 64),
		Command:             "go test ./internal/replay",
		Passed:              true,
	}
}

func validSLOSummary() sloSummaryInput {
	return sloSummaryInput{
		SchemaVersion:        schemaVersion,
		GeneratedAt:          time.Date(2026, 8, 14, 10, 45, 0, 0, time.UTC),
		RulesFile:            "deploy/prometheus/alerts.yml",
		TestsFile:            "deploy/prometheus/slo-tests.yml",
		RecordingRules:       3,
		Alerts:               requiredSLOAlerts,
		FireAndRecoveryCases: requiredSLORecoveryCases,
		Command:              "promtool test rules deploy/prometheus/slo-tests.yml",
		Passed:               true,
	}
}

func writeCheckedJSON(t *testing.T, value any) string {
	t.Helper()

	directory := t.TempDir()
	path := filepath.Join(directory, "summary.json")
	if _, isSLO := value.(sloSummaryInput); isSLO {
		writePromtoolLogs(t, directory)
	}
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	body = append(body, '\n')
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	if err := evidence.GenerateChecksums(directory); err != nil {
		t.Fatalf("generate checksums: %v", err)
	}
	return path
}

func writeCheckedP5Evidence(t *testing.T, walkthrough p5Summary) (string, string) {
	t.Helper()

	directory := t.TempDir()
	writePromtoolLogs(t, directory)
	sloPath := filepath.Join(directory, "slo-summary.json")
	walkthroughPath := filepath.Join(directory, "walkthrough.json")
	writeJSONFixture(t, sloPath, validSLOSummary())
	writeJSONFixture(t, walkthroughPath, walkthrough)
	if err := evidence.GenerateChecksums(directory); err != nil {
		t.Fatalf("generate P5 checksums: %v", err)
	}
	return walkthroughPath, sloPath
}

func writePromtoolLogs(t *testing.T, directory string) {
	t.Helper()
	for _, name := range []string{"promtool-check.log", "promtool-test.log"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("SUCCESS\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func containsReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}
