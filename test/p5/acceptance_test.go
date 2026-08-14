package p5

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOperationsWalkthrough(t *testing.T) {
	if os.Getenv("RAILYARD_P5_ACCEPTANCE") != "1" {
		t.Skip("set RAILYARD_P5_ACCEPTANCE=1 to run the live P5 acceptance suite")
	}
	config := DefaultConfig()
	config.BaseURL = envOrDefault("RAILYARD_P5_BASE_URL", config.BaseURL)
	config.PrometheusURL = envOrDefault("RAILYARD_P5_PROMETHEUS_URL", config.PrometheusURL)
	config.Actor = envOrDefault("RAILYARD_P5_ACTOR", "p5-acceptance")
	config.RunID = envOrDefault(
		"RAILYARD_P5_RUN_ID",
		time.Now().UTC().Format("20060102T150405Z"),
	)
	config.RepositoryRoot = envOrDefault(
		"RAILYARD_P5_REPOSITORY_ROOT",
		filepath.Clean(filepath.Join("..", "..")),
	)
	config.ComposeFile = envOrDefault("RAILYARD_P5_COMPOSE_FILE", config.ComposeFile)
	config.ComposeProject = envOrDefault("RAILYARD_P5_COMPOSE_PROJECT", config.ComposeProject)
	config.SkipLiveAlerts = os.Getenv("RAILYARD_P5_SKIP_LIVE_ALERTS") == "1"

	runner, err := NewRunner(config, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()
	report, err := runner.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.CompletedAt.IsZero() {
		t.Fatal("walkthrough returned without a completion timestamp")
	}
	if report.AuditEventCount < 11 {
		t.Fatalf("audit event count = %d, want at least 11", report.AuditEventCount)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
