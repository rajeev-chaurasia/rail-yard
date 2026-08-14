package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

const (
	schemaVersion            = 1
	requiredBenchmarkRuns    = 3
	requiredJobsPerRun       = 50_000
	requiredWorkers          = 8
	requiredWorkerSlots      = 256
	requiredChaosRuns        = 10
	requiredWorkerKills      = 20
	requiredReplayDecisions  = 50_000
	requiredReplays          = 3
	throughputTarget         = 10_000.0
	recoveryTarget           = 5 * time.Second
	replayMatchTarget        = 100.0
	requiredSLOAlerts        = 2
	requiredSLORecoveryCases = 2
)

type inputPaths struct {
	benchmark string
	chaos     string
	replay    string
	slo       string
	p5        string
}

type inputDigests struct {
	BenchmarkSuite string `json:"benchmark_suite_sha256,omitempty"`
	ChaosCampaign  string `json:"chaos_campaign_sha256,omitempty"`
	ReplaySummary  string `json:"replay_summary_sha256,omitempty"`
	SLOSummary     string `json:"slo_summary_sha256,omitempty"`
	P5Walkthrough  string `json:"p5_walkthrough_sha256,omitempty"`
}

type throughputResult struct {
	EvidenceValid        bool    `json:"evidence_valid"`
	Qualified            bool    `json:"qualified"`
	MeasuredRuns         int     `json:"measured_runs"`
	LeaseGrantsPerMinute float64 `json:"durable_lease_grants_per_minute"`
	TargetPerMinute      float64 `json:"target_per_minute"`
}

type chaosResult struct {
	EvidenceValid           bool  `json:"evidence_valid"`
	CorrectnessQualified    bool  `json:"correctness_qualified"`
	RecoveryQualified       bool  `json:"recovery_qualified"`
	Runs                    int   `json:"runs"`
	ReconciledRuns          int   `json:"reconciled_runs"`
	RequiredRuns            int   `json:"required_runs"`
	RecoverySamples         int   `json:"recovery_samples"`
	RecoveryP99NS           int64 `json:"recovery_p99_ns"`
	RecoveryTargetExclusive int64 `json:"recovery_target_exclusive_ns"`
}

type replayResult struct {
	EvidenceValid    bool    `json:"evidence_valid"`
	Qualified        bool    `json:"qualified"`
	Decisions        int     `json:"decisions"`
	CleanProcesses   int     `json:"clean_process_replays"`
	ByteMatchPercent float64 `json:"byte_match_percent"`
}

type operationsResult struct {
	EvidenceValid       bool `json:"evidence_valid"`
	Qualified           bool `json:"qualified"`
	WorkflowJobs        int  `json:"workflow_jobs"`
	AuditEvents         int  `json:"audit_events"`
	LiveAlertsValidated bool `json:"live_alerts_validated"`
}

type sloResult struct {
	EvidenceValid        bool `json:"evidence_valid"`
	Qualified            bool `json:"qualified"`
	Alerts               int  `json:"alerts"`
	FireAndRecoveryCases int  `json:"fire_and_recovery_cases"`
	IntegrationValidated bool `json:"integration_validated"`
}

type qualificationSummary struct {
	SchemaVersion  int              `json:"schema_version"`
	Status         string           `json:"status"`
	EvidenceValid  bool             `json:"evidence_valid"`
	Qualified      bool             `json:"qualified"`
	Inputs         inputDigests     `json:"inputs"`
	Throughput     throughputResult `json:"throughput"`
	Chaos          chaosResult      `json:"chaos"`
	Replay         replayResult     `json:"replay"`
	Operations     operationsResult `json:"operations"`
	SLO            sloResult        `json:"slo"`
	InvalidReasons []string         `json:"invalid_reasons,omitempty"`
	MeasuredMisses []string         `json:"measured_misses,omitempty"`
	ActivationText string           `json:"activation_text,omitempty"`
}

type campaignSummary struct {
	Version          int          `json:"version"`
	StartedAt        time.Time    `json:"started_at"`
	CompletedAt      time.Time    `json:"completed_at"`
	Passed           bool         `json:"passed"`
	RecoverySamples  int          `json:"recovery_samples"`
	RecoveryP99MS    float64      `json:"recovery_p99_ms"`
	RecoveryTargetMS float64      `json:"recovery_target_ms"`
	Runs             []runSummary `json:"runs"`
}

type runSummary struct {
	Run                int       `json:"run"`
	Seed               int64     `json:"seed"`
	Project            string    `json:"project"`
	TenantID           string    `json:"tenant_id"`
	StartedAt          time.Time `json:"started_at"`
	CompletedAt        time.Time `json:"completed_at"`
	Accepted           int       `json:"accepted"`
	WorkerKills        int       `json:"worker_kills"`
	ServerKills        int       `json:"server_kills"`
	RecoverySamples    int       `json:"recovery_samples"`
	RecoveryP99MS      float64   `json:"recovery_p99_ms"`
	RecoveryTargetMS   float64   `json:"recovery_target_ms"`
	ReconciliationPass bool      `json:"reconciliation_pass"`
	ArtifactDirectory  string    `json:"artifact_directory"`
}

type chaosManifest struct {
	Version              int           `json:"version"`
	Run                  int           `json:"run"`
	Seed                 int64         `json:"seed"`
	Project              string        `json:"project"`
	TenantID             string        `json:"tenant_id"`
	Queue                string        `json:"queue"`
	Jobs                 int           `json:"jobs"`
	Workers              []string      `json:"workers"`
	WorkerKills          int           `json:"worker_kills"`
	SubmitConcurrency    int           `json:"submit_concurrency"`
	JobDuration          time.Duration `json:"job_duration"`
	ServerKillTarget     float64       `json:"server_kill_target"`
	ServerKillTargetJobs int           `json:"server_kill_target_jobs"`
	MaxRecovery          time.Duration `json:"max_recovery"`
	ComposeFile          string        `json:"compose_file"`
	ConfigurationHash    string        `json:"configuration_hash"`
	StartedAt            time.Time     `json:"started_at"`
	CompletedAt          time.Time     `json:"completed_at"`
}

type recoverySample struct {
	KillSequence        int       `json:"kill_sequence"`
	Worker              string    `json:"worker"`
	VictimContainerID   string    `json:"victim_container_id"`
	JobID               string    `json:"job_id"`
	KilledAttempt       int       `json:"killed_attempt"`
	KilledGeneration    int64     `json:"killed_generation"`
	KillConfirmedAt     time.Time `json:"kill_confirmed_at"`
	SuccessorAttempt    int       `json:"successor_attempt"`
	SuccessorGeneration int64     `json:"successor_generation"`
	SuccessorLeasedAt   time.Time `json:"successor_leased_at"`
	SuccessorObservedAt time.Time `json:"successor_observed_at"`
	CompletionAt        time.Time `json:"completion_at"`
	RecoveryMS          float64   `json:"recovery_ms"`
}

type chaosEvent struct {
	Sequence   int            `json:"sequence"`
	Type       string         `json:"type"`
	PlannedAt  time.Time      `json:"planned_at,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
	Service    string         `json:"service,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

type replaySummary struct {
	SchemaVersion       int       `json:"schema_version"`
	GeneratedAt         time.Time `json:"generated_at"`
	GoVersion           string    `json:"go_version"`
	OS                  string    `json:"os"`
	Architecture        string    `json:"architecture"`
	Decisions           int       `json:"decisions"`
	CleanProcessReplays int       `json:"clean_process_replays"`
	ByteMatchPercent    float64   `json:"byte_match_percent"`
	SHA256              string    `json:"sha256"`
	Command             string    `json:"command"`
	Passed              bool      `json:"passed"`
}

type sloSummaryInput struct {
	SchemaVersion        int       `json:"schema_version"`
	GeneratedAt          time.Time `json:"generated_at"`
	RulesFile            string    `json:"rules_file"`
	TestsFile            string    `json:"tests_file"`
	RecordingRules       int       `json:"recording_rules"`
	Alerts               int       `json:"alerts"`
	FireAndRecoveryCases int       `json:"fire_and_recovery_cases"`
	Command              string    `json:"command"`
	Passed               bool      `json:"passed"`
}

type p5Summary struct {
	RunID                    string        `json:"run_id"`
	Actor                    string        `json:"actor"`
	StartedAt                time.Time     `json:"started_at"`
	CompletedAt              time.Time     `json:"completed_at"`
	WorkflowJobIDs           []string      `json:"workflow_job_ids"`
	ReassignedJobID          string        `json:"reassigned_job_id"`
	ReassignmentObservedIn   time.Duration `json:"reassignment_observed_ns"`
	DeadLetterJobID          string        `json:"dead_letter_job_id"`
	RedrivenJobID            string        `json:"redriven_job_id"`
	RecoveryAlertFiredAt     time.Time     `json:"recovery_alert_fired_at"`
	RecoveryAlertRecoveredAt time.Time     `json:"recovery_alert_recovered_at"`
	QueueAlertFiredAt        time.Time     `json:"queue_alert_fired_at"`
	QueueAlertRecoveredAt    time.Time     `json:"queue_alert_recovered_at"`
	AuditEventCount          int           `json:"audit_event_count"`
	LiveAlertWaitsSkipped    bool          `json:"live_alert_waits_skipped"`
	SLORuleEvidence          string        `json:"slo_rule_evidence"`
	Passed                   bool          `json:"passed"`
}

func evaluate(paths inputPaths) qualificationSummary {
	summary := qualificationSummary{
		SchemaVersion: schemaVersion,
		Status:        "invalid",
		Throughput: throughputResult{
			TargetPerMinute: throughputTarget,
		},
		Chaos: chaosResult{
			RequiredRuns:            requiredChaosRuns,
			RecoveryTargetExclusive: int64(recoveryTarget),
		},
	}

	throughput, digest, reasons := validateBenchmark(paths.benchmark)
	summary.Throughput = throughput
	summary.Inputs.BenchmarkSuite = digest
	appendReasons(&summary, "benchmark", reasons)

	chaos, digest, reasons := validateChaos(paths.chaos)
	summary.Chaos = chaos
	summary.Inputs.ChaosCampaign = digest
	appendReasons(&summary, "chaos", reasons)

	replay, digest, reasons := validateReplay(paths.replay)
	summary.Replay = replay
	summary.Inputs.ReplaySummary = digest
	appendReasons(&summary, "replay", reasons)

	slo, digest, reasons := validateSLO(paths.slo)
	summary.SLO = slo
	summary.Inputs.SLOSummary = digest
	appendReasons(&summary, "slo", reasons)

	operations, digest, reasons := validateP5(paths.p5, paths.slo, slo.Qualified)
	summary.Operations = operations
	summary.Inputs.P5Walkthrough = digest
	summary.SLO.IntegrationValidated = operations.LiveAlertsValidated
	summary.SLO.Qualified = summary.SLO.Qualified && operations.LiveAlertsValidated
	appendReasons(&summary, "p5", reasons)

	slices.Sort(summary.InvalidReasons)
	summary.InvalidReasons = slices.Compact(summary.InvalidReasons)
	summary.EvidenceValid = len(summary.InvalidReasons) == 0
	if !summary.EvidenceValid {
		return summary
	}

	if !summary.Throughput.Qualified {
		summary.MeasuredMisses = append(summary.MeasuredMisses, fmt.Sprintf(
			"throughput %.17g is below %.17g durable lease grants per minute",
			summary.Throughput.LeaseGrantsPerMinute,
			summary.Throughput.TargetPerMinute,
		))
	}
	if !summary.Chaos.CorrectnessQualified {
		summary.MeasuredMisses = append(summary.MeasuredMisses, fmt.Sprintf(
			"chaos reconciliation passed %d of %d runs",
			summary.Chaos.ReconciledRuns,
			summary.Chaos.RequiredRuns,
		))
	}
	if !summary.Chaos.RecoveryQualified {
		summary.MeasuredMisses = append(summary.MeasuredMisses, fmt.Sprintf(
			"worker reassignment p99 %sns is not below %dns",
			strconv.FormatInt(summary.Chaos.RecoveryP99NS, 10),
			summary.Chaos.RecoveryTargetExclusive,
		))
	}
	if !summary.Replay.Qualified {
		summary.MeasuredMisses = append(summary.MeasuredMisses, fmt.Sprintf(
			"replay matched %.17g%% across %d clean processes and %d decisions",
			summary.Replay.ByteMatchPercent,
			summary.Replay.CleanProcesses,
			summary.Replay.Decisions,
		))
	}
	if !summary.Operations.Qualified {
		summary.MeasuredMisses = append(summary.MeasuredMisses, "operations lifecycle did not pass")
	}
	if !summary.SLO.Qualified {
		summary.MeasuredMisses = append(
			summary.MeasuredMisses,
			"SLO alerts did not fire and recover in both rule tests and the integration walkthrough",
		)
	}
	slices.Sort(summary.MeasuredMisses)
	summary.Qualified = len(summary.MeasuredMisses) == 0
	if !summary.Qualified {
		summary.Status = "miss"
		return summary
	}

	summary.Status = "qualified"
	summary.ActivationText = activationText(summary)
	return summary
}

func validateBenchmark(path string) (throughputResult, string, []string) {
	result := throughputResult{
		TargetPerMinute: throughputTarget,
		MeasuredRuns:    requiredBenchmarkRuns,
	}
	digest, err := verifyInput(path)
	if err != nil {
		return result, "", []string{err.Error()}
	}
	var suite evidence.SuiteSummary
	if err := readStrictJSON(path, &suite); err != nil {
		return result, digest, []string{err.Error()}
	}
	var reasons []string
	if suite.SchemaVersion != evidence.SchemaVersion {
		reasons = append(reasons, "unsupported schema_version")
	}
	if suite.GeneratedAt.IsZero() {
		reasons = append(reasons, "generated_at is empty")
	}
	if !suite.Valid || len(suite.InvalidReasons) != 0 {
		reasons = append(reasons, "suite status is not valid")
	}
	if suite.Warmup.Phase != evidence.PhaseWarmup || strings.TrimSpace(suite.Warmup.RunID) == "" {
		reasons = append(reasons, "suite requires one identified warmup run")
	}
	if len(suite.MeasuredRuns) != requiredBenchmarkRuns {
		reasons = append(reasons, fmt.Sprintf(
			"suite has %d measured runs, want %d",
			len(suite.MeasuredRuns),
			requiredBenchmarkRuns,
		))
	}

	references := append([]evidence.SuiteRun{suite.Warmup}, suite.MeasuredRuns...)
	runDirectories := make([]string, 0, len(references))
	seenIDs := make(map[string]struct{}, len(references))
	for index, reference := range references {
		if _, duplicate := seenIDs[reference.RunID]; duplicate || reference.RunID == "" {
			reasons = append(reasons, fmt.Sprintf("run reference %d has an empty or duplicate run_id", index+1))
		}
		seenIDs[reference.RunID] = struct{}{}
		if index > 0 && reference.Phase != evidence.PhaseMeasured {
			reasons = append(reasons, fmt.Sprintf("run reference %d is not measured", index+1))
		}
		directory, resolveErr := resolveEvidenceDirectory(reference.ArtifactPath, filepath.Dir(path))
		if resolveErr != nil {
			reasons = append(reasons, fmt.Sprintf("run %s: %v", reference.RunID, resolveErr))
			continue
		}
		runDirectories = append(runDirectories, directory)
		if runReasons := validateBenchmarkRun(directory, reference); len(runReasons) != 0 {
			for _, reason := range runReasons {
				reasons = append(reasons, fmt.Sprintf("run %s: %s", reference.RunID, reason))
			}
		}
	}
	if len(runDirectories) == len(references) {
		recomputed := evidence.SummarizeSuite(runDirectories, suite.GeneratedAt)
		if !reflect.DeepEqual(recomputed, suite) {
			reasons = append(reasons, "suite does not match recomputed checked run evidence")
		}
	}
	if !suite.DurableLeaseGrants.Available ||
		suite.DurableLeaseGrants.MedianPerMinute == nil {
		reasons = append(reasons, "durable lease grant median is unavailable")
	} else {
		result.LeaseGrantsPerMinute = *suite.DurableLeaseGrants.MedianPerMinute
		if !finiteNonnegative(result.LeaseGrantsPerMinute) {
			reasons = append(reasons, "durable lease grant median is not finite and nonnegative")
		}
	}
	result.MeasuredRuns = len(suite.MeasuredRuns)
	result.EvidenceValid = len(reasons) == 0
	result.Qualified = result.EvidenceValid && result.LeaseGrantsPerMinute >= throughputTarget
	return result, digest, reasons
}

func validateBenchmarkRun(directory string, reference evidence.SuiteRun) []string {
	var reasons []string
	if err := evidence.VerifyChecksums(directory); err != nil {
		return []string{"checksums: " + err.Error()}
	}
	var manifest evidence.RunManifest
	if err := readStrictJSON(filepath.Join(directory, "manifest.json"), &manifest); err != nil {
		return []string{err.Error()}
	}
	var summary evidence.BenchmarkSummary
	summaryPath := filepath.Join(directory, "benchmark-summary.json")
	if err := readStrictJSON(summaryPath, &summary); err != nil {
		return []string{err.Error()}
	}
	actualDigest, err := fileSHA256(summaryPath)
	if err != nil {
		reasons = append(reasons, err.Error())
	} else if actualDigest != strings.ToLower(reference.SHA256) || !validSHA256(reference.SHA256) {
		reasons = append(reasons, "summary_sha256 does not match benchmark-summary.json")
	}
	if manifest.SchemaVersion != evidence.SchemaVersion ||
		manifest.RunID != reference.RunID ||
		manifest.Phase != reference.Phase ||
		manifest.Status != evidence.StatusValid ||
		len(manifest.InvalidReasons) != 0 {
		reasons = append(reasons, "manifest identity or status is invalid")
	}
	if manifest.Scored != (manifest.Phase == evidence.PhaseMeasured) {
		reasons = append(reasons, "manifest scored flag does not match phase")
	}
	if !manifest.Config.Qualification ||
		manifest.Config.JobCount != requiredJobsPerRun ||
		manifest.Config.WorkerCount != requiredWorkers ||
		manifest.Config.WorkerSlots != requiredWorkerSlots {
		reasons = append(reasons, "manifest does not use the exact qualification workload")
	}
	if summary.SchemaVersion != evidence.SchemaVersion ||
		summary.RunID != reference.RunID ||
		summary.Phase != reference.Phase ||
		!summary.Valid ||
		len(summary.InvalidReasons) != 0 {
		reasons = append(reasons, "benchmark summary identity or status is invalid")
	}
	if summary.CanonicalJobCount != requiredJobsPerRun ||
		summary.DurableLeaseGrantCount != requiredJobsPerRun ||
		summary.RepeatedAttemptCount != 0 {
		reasons = append(reasons, "benchmark summary counts do not match the no-op qualification workload")
	}
	return reasons
}

func validateChaos(path string) (chaosResult, string, []string) {
	result := chaosResult{
		RequiredRuns:            requiredChaosRuns,
		RecoveryTargetExclusive: int64(recoveryTarget),
	}
	digest, err := verifyInput(path)
	if err != nil {
		return result, "", []string{err.Error()}
	}
	var campaign campaignSummary
	if err := readStrictJSON(path, &campaign); err != nil {
		return result, digest, []string{err.Error()}
	}
	var reasons []string
	if campaign.Version != schemaVersion {
		reasons = append(reasons, "unsupported version")
	}
	if campaign.StartedAt.IsZero() || campaign.CompletedAt.Before(campaign.StartedAt) {
		reasons = append(reasons, "campaign timestamps are incomplete")
	}
	if campaign.RecoveryTargetMS != float64(recoveryTarget)/float64(time.Millisecond) {
		reasons = append(reasons, "campaign recovery target is not exactly 5000ms")
	}
	if len(campaign.Runs) != requiredChaosRuns {
		reasons = append(reasons, fmt.Sprintf(
			"campaign has %d runs, want %d",
			len(campaign.Runs),
			requiredChaosRuns,
		))
	}

	seenRuns := make(map[int]struct{}, len(campaign.Runs))
	seenSeeds := make(map[int64]struct{}, len(campaign.Runs))
	var observedRecovery []float64
	var committedRecovery []time.Duration
	reconciled := 0
	for _, run := range campaign.Runs {
		if run.Run < 1 || run.Run > requiredChaosRuns {
			reasons = append(reasons, fmt.Sprintf("run number %d is outside 1..%d", run.Run, requiredChaosRuns))
		}
		if _, duplicate := seenRuns[run.Run]; duplicate {
			reasons = append(reasons, fmt.Sprintf("run number %d is duplicated", run.Run))
		}
		seenRuns[run.Run] = struct{}{}
		if _, duplicate := seenSeeds[run.Seed]; duplicate {
			reasons = append(reasons, fmt.Sprintf("seed %d is duplicated", run.Seed))
		}
		seenSeeds[run.Seed] = struct{}{}
		directory, resolveErr := resolveEvidenceDirectory(run.ArtifactDirectory, filepath.Dir(path))
		if resolveErr != nil {
			reasons = append(reasons, fmt.Sprintf("run %d: %v", run.Run, resolveErr))
			continue
		}
		runObserved, runCommitted, runReasons := validateChaosRun(directory, run)
		observedRecovery = append(observedRecovery, runObserved...)
		committedRecovery = append(committedRecovery, runCommitted...)
		for _, reason := range runReasons {
			reasons = append(reasons, fmt.Sprintf("run %d: %s", run.Run, reason))
		}
		if run.ReconciliationPass {
			reconciled++
		}
	}
	if campaign.RecoverySamples != len(observedRecovery) {
		reasons = append(reasons, fmt.Sprintf(
			"campaign recovery_samples=%d, checked samples=%d",
			campaign.RecoverySamples,
			len(observedRecovery),
		))
	}
	if len(observedRecovery) == 0 {
		reasons = append(reasons, "campaign has no recovery samples")
	} else if !equalFloat(campaign.RecoveryP99MS, nearestRankFloat(observedRecovery, 99)) {
		reasons = append(reasons, "campaign recovery_p99_ms does not match checked samples")
	}
	expectedPassed := reconciled == requiredChaosRuns &&
		len(campaign.Runs) == requiredChaosRuns &&
		campaign.RecoveryP99MS < campaign.RecoveryTargetMS
	if campaign.Passed != expectedPassed {
		reasons = append(reasons, "campaign passed status does not match measured results")
	}

	result.Runs = len(campaign.Runs)
	result.ReconciledRuns = reconciled
	result.RecoverySamples = len(committedRecovery)
	if len(committedRecovery) > 0 {
		result.RecoveryP99NS = int64(nearestRankDuration(committedRecovery, 99))
	}
	result.EvidenceValid = len(reasons) == 0
	result.CorrectnessQualified = result.EvidenceValid &&
		result.Runs == requiredChaosRuns &&
		result.ReconciledRuns == requiredChaosRuns
	result.RecoveryQualified = result.EvidenceValid &&
		result.RecoverySamples > 0 &&
		time.Duration(result.RecoveryP99NS) < recoveryTarget
	return result, digest, reasons
}

func validateChaosRun(
	directory string,
	run runSummary,
) ([]float64, []time.Duration, []string) {
	if err := evidence.VerifyChecksums(directory); err != nil {
		return nil, nil, []string{"checksums: " + err.Error()}
	}
	var reasons []string
	var manifest chaosManifest
	if err := readStrictJSON(filepath.Join(directory, "manifest.json"), &manifest); err != nil {
		return nil, nil, []string{err.Error()}
	}
	if manifest.Version != 2 ||
		manifest.Run != run.Run ||
		manifest.Seed != run.Seed ||
		manifest.Project != run.Project ||
		manifest.TenantID != run.TenantID {
		reasons = append(reasons, "manifest identity does not match campaign summary")
	}
	if manifest.Jobs != requiredJobsPerRun ||
		len(manifest.Workers) != requiredWorkers ||
		manifest.WorkerKills < requiredWorkerKills ||
		manifest.MaxRecovery != recoveryTarget {
		reasons = append(reasons, "manifest does not use the exact chaos qualification workload")
	}
	if manifest.CompletedAt.IsZero() || manifest.CompletedAt.Before(manifest.StartedAt) ||
		run.CompletedAt.IsZero() || run.CompletedAt.Before(run.StartedAt) {
		reasons = append(reasons, "run timestamps are incomplete")
	}
	if run.Accepted != requiredJobsPerRun ||
		run.WorkerKills < requiredWorkerKills ||
		run.ServerKills != 1 ||
		run.RecoveryTargetMS != float64(recoveryTarget)/float64(time.Millisecond) {
		reasons = append(reasons, "run counts or target do not match the exact chaos qualifier")
	}

	var report reconcile.Report
	if err := readStrictJSON(filepath.Join(directory, "reconciliation.json"), &report); err != nil {
		reasons = append(reasons, err.Error())
	} else {
		if report.Version != schemaVersion ||
			report.ExpectedJobs != requiredJobsPerRun ||
			report.Counts.Accepted != requiredJobsPerRun {
			reasons = append(reasons, "reconciliation identity or accepted count is invalid")
		}
		if report.Passed != run.ReconciliationPass {
			reasons = append(reasons, "reconciliation status does not match campaign summary")
		}
		if report.Passed &&
			(report.ViolationCount != 0 ||
				report.Counts.Jobs != requiredJobsPerRun ||
				report.Counts.Completions != requiredJobsPerRun) {
			reasons = append(reasons, "passing reconciliation report has incompatible counts")
		}
	}

	workerKills, serverKills, eventErr := countChaosEvents(filepath.Join(directory, "events.jsonl"))
	if eventErr != nil {
		reasons = append(reasons, eventErr.Error())
	} else if workerKills != run.WorkerKills || serverKills != run.ServerKills {
		reasons = append(reasons, "action trace kill counts do not match campaign summary")
	}

	samples, sampleErr := readStrictJSONLines[recoverySample](
		filepath.Join(directory, "recovery-samples.jsonl"),
	)
	if sampleErr != nil {
		reasons = append(reasons, sampleErr.Error())
		return nil, nil, reasons
	}
	var observed []float64
	var committed []time.Duration
	for index, sample := range samples {
		if sample.JobID == "" ||
			sample.Worker == "" ||
			sample.VictimContainerID == "" ||
			sample.KillConfirmedAt.IsZero() ||
			sample.SuccessorLeasedAt.IsZero() ||
			sample.SuccessorObservedAt.IsZero() ||
			sample.SuccessorGeneration <= sample.KilledGeneration ||
			sample.SuccessorAttempt <= sample.KilledAttempt {
			reasons = append(reasons, fmt.Sprintf("recovery sample %d is incomplete", index+1))
			continue
		}
		observedDuration := sample.SuccessorObservedAt.Sub(sample.KillConfirmedAt)
		committedDuration := sample.SuccessorLeasedAt.Sub(sample.KillConfirmedAt)
		if observedDuration < 0 || committedDuration < 0 ||
			!equalFloat(sample.RecoveryMS, float64(observedDuration)/float64(time.Millisecond)) {
			reasons = append(reasons, fmt.Sprintf("recovery sample %d has inconsistent timing", index+1))
			continue
		}
		observed = append(observed, sample.RecoveryMS)
		committed = append(committed, committedDuration)
	}
	if run.RecoverySamples != len(samples) {
		reasons = append(reasons, fmt.Sprintf(
			"recovery_samples=%d, checked samples=%d",
			run.RecoverySamples,
			len(samples),
		))
	}
	if len(observed) == 0 {
		reasons = append(reasons, "recovery samples are empty")
	} else if !equalFloat(run.RecoveryP99MS, nearestRankFloat(observed, 99)) {
		reasons = append(reasons, "recovery_p99_ms does not match checked samples")
	}
	return observed, committed, reasons
}

func validateReplay(path string) (replayResult, string, []string) {
	var result replayResult
	digest, err := verifyInput(path)
	if err != nil {
		return result, "", []string{err.Error()}
	}
	var input replaySummary
	if err := readStrictJSON(path, &input); err != nil {
		return result, digest, []string{err.Error()}
	}
	var reasons []string
	if input.SchemaVersion != schemaVersion {
		reasons = append(reasons, "unsupported schema_version")
	}
	if input.GeneratedAt.IsZero() ||
		strings.TrimSpace(input.GoVersion) == "" ||
		strings.TrimSpace(input.OS) == "" ||
		strings.TrimSpace(input.Architecture) == "" ||
		strings.TrimSpace(input.Command) == "" {
		reasons = append(reasons, "replay provenance is incomplete")
	}
	if input.Decisions < 1 || input.CleanProcessReplays < 1 ||
		!finiteInRange(input.ByteMatchPercent, 0, 100) ||
		!validSHA256(input.SHA256) {
		reasons = append(reasons, "replay measurements are invalid")
	}
	qualified := input.Decisions >= requiredReplayDecisions &&
		input.CleanProcessReplays == requiredReplays &&
		input.ByteMatchPercent == replayMatchTarget
	if input.Passed != qualified {
		reasons = append(reasons, "passed status does not match exact replay qualifiers")
	}
	result = replayResult{
		EvidenceValid:    len(reasons) == 0,
		Qualified:        len(reasons) == 0 && qualified,
		Decisions:        input.Decisions,
		CleanProcesses:   input.CleanProcessReplays,
		ByteMatchPercent: input.ByteMatchPercent,
	}
	return result, digest, reasons
}

func validateSLO(path string) (sloResult, string, []string) {
	var result sloResult
	digest, err := verifyInput(path)
	if err != nil {
		return result, "", []string{err.Error()}
	}
	var input sloSummaryInput
	if err := readStrictJSON(path, &input); err != nil {
		return result, digest, []string{err.Error()}
	}
	var reasons []string
	if input.SchemaVersion != schemaVersion {
		reasons = append(reasons, "unsupported schema_version")
	}
	if input.GeneratedAt.IsZero() ||
		strings.TrimSpace(input.RulesFile) == "" ||
		strings.TrimSpace(input.TestsFile) == "" ||
		strings.TrimSpace(input.Command) == "" ||
		input.RecordingRules < 1 {
		reasons = append(reasons, "SLO provenance is incomplete")
	}
	qualified := input.Alerts == requiredSLOAlerts &&
		input.FireAndRecoveryCases == requiredSLORecoveryCases
	if input.Passed != qualified {
		reasons = append(reasons, "passed status does not match exact SLO rule qualifiers")
	}
	result = sloResult{
		EvidenceValid:        len(reasons) == 0,
		Qualified:            len(reasons) == 0 && qualified,
		Alerts:               input.Alerts,
		FireAndRecoveryCases: input.FireAndRecoveryCases,
	}
	return result, digest, reasons
}

func validateP5(path, sloPath string, sloQualified bool) (operationsResult, string, []string) {
	var result operationsResult
	digest, err := verifyInput(path)
	if err != nil {
		return result, "", []string{err.Error()}
	}
	var input p5Summary
	if err := readStrictJSON(path, &input); err != nil {
		return result, digest, []string{err.Error()}
	}
	var reasons []string
	if input.RunID == "" ||
		input.Actor == "" ||
		input.StartedAt.IsZero() ||
		input.CompletedAt.Before(input.StartedAt) {
		reasons = append(reasons, "walkthrough identity or timestamps are incomplete")
	}
	if len(input.WorkflowJobIDs) != 3 || !uniqueNonempty(input.WorkflowJobIDs) ||
		input.ReassignedJobID == "" ||
		input.ReassignmentObservedIn <= 0 ||
		input.DeadLetterJobID == "" ||
		input.RedrivenJobID == "" ||
		input.DeadLetterJobID == input.RedrivenJobID ||
		input.AuditEventCount < 6 {
		reasons = append(reasons, "operations lifecycle evidence is incomplete")
	}
	lifecycleComplete := len(reasons) == 0
	if input.Passed != lifecycleComplete {
		reasons = append(reasons, "passed status does not match the operations lifecycle")
	}
	if input.SLORuleEvidence == "" {
		reasons = append(reasons, "slo_rule_evidence is empty")
	} else {
		referenced, resolveErr := resolveEvidenceFile(input.SLORuleEvidence, filepath.Dir(path))
		if resolveErr != nil {
			reasons = append(reasons, "slo_rule_evidence: "+resolveErr.Error())
		} else if same, sameErr := sameFile(referenced, sloPath); sameErr != nil || !same {
			reasons = append(reasons, "slo_rule_evidence does not identify the supplied SLO summary")
		}
	}
	liveAlerts := !input.LiveAlertWaitsSkipped &&
		orderedTimes(input.RecoveryAlertFiredAt, input.RecoveryAlertRecoveredAt) &&
		orderedTimes(input.QueueAlertFiredAt, input.QueueAlertRecoveredAt)
	if !input.LiveAlertWaitsSkipped && !liveAlerts {
		reasons = append(reasons, "live alert timestamps are incomplete or unordered")
	}
	result = operationsResult{
		EvidenceValid:       len(reasons) == 0,
		Qualified:           len(reasons) == 0 && input.Passed,
		WorkflowJobs:        len(input.WorkflowJobIDs),
		AuditEvents:         input.AuditEventCount,
		LiveAlertsValidated: len(reasons) == 0 && liveAlerts && sloQualified,
	}
	return result, digest, reasons
}

func appendReasons(summary *qualificationSummary, source string, reasons []string) {
	for _, reason := range reasons {
		summary.InvalidReasons = append(summary.InvalidReasons, source+": "+reason)
	}
}

func verifyInput(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path is empty")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", path)
	}
	if err := evidence.VerifyChecksums(filepath.Dir(path)); err != nil {
		return "", fmt.Errorf("verify checksums for %s: %w", path, err)
	}
	return fileSHA256(path)
}

func resolveEvidenceDirectory(path, relativeTo string) (string, error) {
	resolved, err := resolveEvidencePath(path, relativeTo)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", resolved)
	}
	return resolved, nil
}

func resolveEvidenceFile(path, relativeTo string) (string, error) {
	resolved, err := resolveEvidencePath(path, relativeTo)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s is not a regular file", resolved)
	}
	return resolved, nil
}

func resolveEvidencePath(path, relativeTo string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("artifact path is empty")
	}
	candidates := []string{filepath.Clean(filepath.FromSlash(path))}
	if !filepath.IsAbs(candidates[0]) {
		candidates = append(candidates, filepath.Join(relativeTo, candidates[0]))
	}
	var matches []string
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			absolute, absErr := filepath.Abs(candidate)
			if absErr != nil {
				return "", absErr
			}
			if !slices.Contains(matches, absolute) {
				matches = append(matches, absolute)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("artifact path %q does not exist", path)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("artifact path %q is ambiguous", path)
	}
	return matches[0], nil
}

func readStrictJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := json.NewDecoder(io.LimitReader(file, 16*1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func readStrictJSONLines[T any](path string) ([]T, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = file.Close()
	}()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var records []T
	for line := 1; scanner.Scan(); line++ {
		body := strings.TrimSpace(scanner.Text())
		if body == "" {
			return nil, fmt.Errorf("%s line %d is empty", path, line)
		}
		var record T
		decoder := json.NewDecoder(strings.NewReader(body))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", path, line, err)
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			if err == nil {
				err = errors.New("multiple JSON values")
			}
			return nil, fmt.Errorf("%s line %d: %w", path, line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return records, nil
}

func countChaosEvents(path string) (int, int, error) {
	events, err := readStrictJSONLines[chaosEvent](path)
	if err != nil {
		return 0, 0, err
	}
	workerKills := 0
	serverKills := 0
	for _, event := range events {
		switch event.Type {
		case "worker_killed":
			workerKills++
		case "server_killed":
			serverKills++
		}
	}
	return workerKills, serverKills, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return "", fmt.Errorf("hash %s: %w", path, copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close %s: %w", path, closeErr)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func nearestRankFloat(values []float64, percentile int) float64 {
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	rank := (percentile*len(ordered) + 99) / 100
	return ordered[rank-1]
}

func nearestRankDuration(values []time.Duration, percentile int) time.Duration {
	ordered := slices.Clone(values)
	slices.Sort(ordered)
	rank := (percentile*len(ordered) + 99) / 100
	return ordered[rank-1]
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func finiteNonnegative(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func finiteInRange(value, minimum, maximum float64) bool {
	return finiteNonnegative(value) && value >= minimum && value <= maximum
}

func equalFloat(left, right float64) bool {
	return math.Float64bits(left) == math.Float64bits(right)
}

func uniqueNonempty(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func orderedTimes(first, second time.Time) bool {
	return !first.IsZero() && second.After(first)
}

func sameFile(left, right string) (bool, error) {
	leftInfo, err := os.Stat(left)
	if err != nil {
		return false, err
	}
	rightInfo, err := os.Stat(right)
	if err != nil {
		return false, err
	}
	return os.SameFile(leftInfo, rightInfo), nil
}

func activationText(summary qualificationSummary) string {
	return fmt.Sprintf(
		"built: railyard | no-op scheduling rate=%s durable lease grants/min | "+
			"chaos reconciliation=%d/%d runs | worker reassignment p99=%sms | replay match=%s%%",
		formatFloat(summary.Throughput.LeaseGrantsPerMinute),
		summary.Chaos.ReconciledRuns,
		summary.Chaos.RequiredRuns,
		formatMilliseconds(time.Duration(summary.Chaos.RecoveryP99NS)),
		formatFloat(summary.Replay.ByteMatchPercent),
	)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func formatMilliseconds(value time.Duration) string {
	negative := value < 0
	if negative {
		value = -value
	}
	whole := value / time.Millisecond
	fraction := value % time.Millisecond
	text := strconv.FormatInt(int64(whole), 10)
	if fraction != 0 {
		text += "." + strings.TrimRight(fmt.Sprintf("%06d", fraction), "0")
	}
	if negative {
		return "-" + text
	}
	return text
}
