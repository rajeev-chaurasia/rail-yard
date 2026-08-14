package p5

import (
	"testing"
	"time"
)

func TestConfiguredAlertNames(t *testing.T) {
	t.Parallel()
	if readyStartSLOAlert != "RailYardReadyStartSLOBreach" {
		t.Fatalf("ready-start alert = %q", readyStartSLOAlert)
	}
	if dlqDepthHighAlert != "RailYardDLQDepthHigh" {
		t.Fatalf("DLQ alert = %q", dlqDepthHighAlert)
	}
}

func TestReadyStartRecoveryPopulationClearsBreachRatio(t *testing.T) {
	t.Parallel()
	ratio := float64(readyStartRecoveryJobs) /
		float64(readyStartBreachJobs+readyStartRecoveryJobs)
	if ratio < 0.99 {
		t.Fatalf("recovery ratio = %v, want at least 0.99", ratio)
	}
	if readyStartRecoveryJobs%readyStartBatchSize != 0 {
		t.Fatalf(
			"recovery jobs %d are not divisible by batch size %d",
			readyStartRecoveryJobs,
			readyStartBatchSize,
		)
	}
}

func TestRequiredAuditCountsCoverAlertOperations(t *testing.T) {
	t.Parallel()
	counts := requiredAuditCounts()
	want := map[string]int{
		"dag.submit":             26,
		"job.submit":             12,
		"worker.kill":            1,
		"job.force.dead_letter":  11,
		"dead_letter.redrive":    11,
		"alert.exercise.start":   2,
		"alert.exercise.recover": 2,
	}
	for action, count := range want {
		if counts[action] != count {
			t.Errorf("audit count for %q = %d, want %d", action, counts[action], count)
		}
	}
}

func TestSLORuleSummaryMatchesReleaseAudit(t *testing.T) {
	t.Parallel()
	generatedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	summary := newSLORuleSummary(generatedAt)
	if !summary.GeneratedAt.Equal(generatedAt) {
		t.Fatalf("generated_at = %s, want %s", summary.GeneratedAt, generatedAt)
	}
	if summary.SchemaVersion != 1 ||
		summary.RecordingRules != 3 ||
		summary.Alerts != 2 ||
		summary.FireAndRecoveryCases != 2 ||
		!summary.Passed {
		t.Fatalf("SLO summary = %#v", summary)
	}
}
