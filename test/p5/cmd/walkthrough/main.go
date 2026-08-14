package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/test/p5"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	config := p5.DefaultConfig()
	var output string
	var totalTimeout time.Duration
	flag.StringVar(&config.BaseURL, "base-url", config.BaseURL, "Rail Yard API URL")
	flag.StringVar(
		&config.PrometheusURL,
		"prometheus-url",
		config.PrometheusURL,
		"Prometheus URL",
	)
	flag.StringVar(&config.Actor, "actor", config.Actor, "operator audit actor")
	flag.StringVar(&config.RunID, "run-id", config.RunID, "unique walkthrough run ID")
	flag.StringVar(&config.RepositoryRoot, "repo-root", config.RepositoryRoot, "repository root")
	flag.StringVar(&config.ComposeFile, "compose-file", config.ComposeFile, "Compose file")
	flag.StringVar(&config.ComposeProject, "compose-project", config.ComposeProject, "Compose project")
	flag.DurationVar(
		&config.AlertFireTimeout,
		"alert-fire-timeout",
		config.AlertFireTimeout,
		"maximum wait for each alert to fire",
	)
	flag.DurationVar(
		&config.AlertClearTimeout,
		"alert-clear-timeout",
		config.AlertClearTimeout,
		"maximum wait for each alert to clear",
	)
	flag.DurationVar(
		&config.RecoveryHold,
		"recovery-hold",
		config.RecoveryHold,
		"worker outage used to breach the recovery SLO",
	)
	flag.BoolVar(
		&config.SkipLiveAlerts,
		"skip-live-alerts",
		false,
		"skip live alert waits when promtool evidence is validated separately",
	)
	flag.DurationVar(&totalTimeout, "timeout", 35*time.Minute, "whole walkthrough timeout")
	flag.StringVar(
		&output,
		"output",
		"",
		"completed report path, defaults under results/_work/p5",
	)
	flag.Parse()

	if output == "" {
		output = filepath.Join(
			config.RepositoryRoot,
			"results",
			"_work",
			"p5",
			config.RunID,
			"walkthrough.json",
		)
	}
	runner, err := p5.NewRunner(config, func(format string, args ...any) {
		fmt.Printf(format+"\n", args...)
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()
	report, err := runner.Run(ctx)
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode completed walkthrough report: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o750); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o640); err != nil {
		return fmt.Errorf("write completed walkthrough report: %w", err)
	}
	fmt.Printf("completed report: %s\n", output)
	return nil
}
