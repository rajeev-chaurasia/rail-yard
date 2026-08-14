package main

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

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

func TestValidateP5KeepsSkippedLiveAlertsOutOfSLOQualification(t *testing.T) {
	t.Parallel()

	sloPath := writeCheckedJSON(t, validSLOSummary())
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
		SLORuleEvidence:        sloPath,
		Passed:                 true,
	}
	path := writeCheckedJSON(t, input)

	result, _, reasons := validateP5(path, sloPath, true)
	if len(reasons) != 0 {
		t.Fatalf("validateP5 reasons = %v", reasons)
	}
	if !result.EvidenceValid || !result.Qualified {
		t.Fatalf("validateP5 result = %#v", result)
	}
	if result.LiveAlertsValidated {
		t.Fatal("validateP5 treated skipped live alerts as integration evidence")
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
			ReconciledRuns: 10,
			RequiredRuns:   10,
			RecoveryP99NS:  int64(4999999999 * time.Nanosecond),
		},
		Replay: replayResult{ByteMatchPercent: 100},
	}
	got := activationText(summary)
	for _, want := range []string{"10000.0000001", "10/10", "4999.999999ms", "100%"} {
		if !strings.Contains(got, want) {
			t.Fatalf("activationText = %q, missing %q", got, want)
		}
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

func containsReason(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}
