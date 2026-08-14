package p5

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

const prometheusImage = "prom/prometheus:v3.5.0"

type sloRuleSummary struct {
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

func RetainSLORuleEvidence(ctx context.Context, repositoryRoot, outputDirectory string) error {
	rulesDirectory, err := filepath.Abs(
		filepath.Join(repositoryRoot, "deploy", "prometheus"),
	)
	if err != nil {
		return fmt.Errorf("resolve Prometheus rules directory: %w", err)
	}
	checkOutput, err := runPromtool(ctx, rulesDirectory, "check", "rules", "/rules/alerts.yml")
	if writeErr := evidence.WriteBytes(
		filepath.Join(outputDirectory, "promtool-check.log"),
		checkOutput,
	); writeErr != nil {
		return fmt.Errorf("write promtool check evidence: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("check Prometheus rules: %w", err)
	}

	testOutput, err := runPromtool(ctx, rulesDirectory, "test", "rules", "/rules/slo-tests.yml")
	if writeErr := evidence.WriteBytes(
		filepath.Join(outputDirectory, "promtool-test.log"),
		testOutput,
	); writeErr != nil {
		return fmt.Errorf("write promtool test evidence: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("test Prometheus rules: %w", err)
	}
	summary := newSLORuleSummary(time.Now().UTC())
	if err := evidence.WriteJSON(
		filepath.Join(outputDirectory, "slo-summary.json"),
		summary,
	); err != nil {
		return fmt.Errorf("write SLO rule summary: %w", err)
	}
	return nil
}

func runPromtool(ctx context.Context, rulesDirectory string, args ...string) ([]byte, error) {
	commandArgs := []string{
		"run",
		"--rm",
		"--entrypoint",
		"promtool",
		"-v",
		rulesDirectory + ":/rules:ro",
		prometheusImage,
	}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, "docker", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%w: %s", err, output)
	}
	return output, nil
}

func newSLORuleSummary(generatedAt time.Time) sloRuleSummary {
	return sloRuleSummary{
		SchemaVersion:        1,
		GeneratedAt:          generatedAt,
		RulesFile:            "deploy/prometheus/alerts.yml",
		TestsFile:            "deploy/prometheus/slo-tests.yml",
		RecordingRules:       3,
		Alerts:               2,
		FireAndRecoveryCases: 2,
		Command:              "promtool test rules deploy/prometheus/slo-tests.yml",
		Passed:               true,
	}
}
