package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	defaultWorkers := "worker-1,worker-2,worker-3,worker-4,worker-5,worker-6,worker-7,worker-8"
	flags := flag.NewFlagSet("railyard-chaos", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cfg := config{}
	var workerList string
	flags.StringVar(&cfg.ComposeFile, "compose-file", "deploy/compose.yaml", "Docker Compose file")
	flags.StringVar(&cfg.ProjectPrefix, "project-prefix", "rail-yard-chaos", "isolated Compose project prefix")
	flags.StringVar(&cfg.OutputDirectory, "output", "artifacts/chaos", "artifact output directory")
	flags.StringVar(&cfg.ServerURL, "server-url", "http://127.0.0.1:8080", "host Rail Yard server URL")
	flags.StringVar(
		&cfg.DatabasePath,
		"database-path",
		"/var/lib/railyard/railyard.db",
		"SQLite path inside the server container",
	)
	flags.StringVar(&cfg.DockerExecutable, "docker", "docker", "Docker CLI executable")
	flags.IntVar(&cfg.Runs, "runs", 1, "number of fresh-volume runs")
	flags.IntVar(&cfg.Jobs, "jobs", 1000, "exact accepted jobs per run")
	flags.Int64Var(&cfg.BaseSeed, "seed", 1, "deterministic base seed")
	flags.IntVar(&cfg.SubmitConcurrency, "submit-concurrency", 16, "concurrent idempotent submitters")
	flags.IntVar(&cfg.WorkerKills, "worker-kills", defaultWorkerKills, "SIGKILL actions per run")
	flags.StringVar(&workerList, "workers", defaultWorkers, "comma-separated Compose worker services")
	flags.DurationVar(&cfg.JobDuration, "job-duration", 250*time.Millisecond, "no-op duration")
	flags.DurationVar(&cfg.ActionMinimum, "action-min", 100*time.Millisecond, "minimum seeded worker kill interval")
	flags.DurationVar(&cfg.ActionMaximum, "action-max", 500*time.Millisecond, "maximum seeded worker kill interval")
	flags.DurationVar(&cfg.PollInterval, "poll-interval", 100*time.Millisecond, "predicate poll interval")
	flags.DurationVar(&cfg.StartupTimeout, "startup-timeout", 3*time.Minute, "startup and recovery predicate deadline")
	flags.DurationVar(&cfg.DrainTimeout, "drain-timeout", 30*time.Minute, "drain predicate deadline")
	flags.DurationVar(&cfg.RequestTimeout, "request-timeout", 10*time.Second, "individual HTTP request timeout")
	flags.DurationVar(&cfg.MaxRecovery, "max-recovery", 5*time.Second, "strict worker recovery p99 ceiling")
	flags.BoolVar(&cfg.KeepStack, "keep-stack", false, "keep a successful stopped stack and volume")
	flags.BoolVar(&cfg.Resume, "resume", false, "reuse verified completed runs")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	cfg.Workers = splitWorkers(workerList)
	if err := cfg.validate(); err != nil {
		return err
	}

	summary, err := runCampaign(ctx, cfg, processRunner{})
	encodeErr := json.NewEncoder(stdout).Encode(summary)
	if err != nil {
		return err
	}
	if encodeErr != nil {
		return fmt.Errorf("write campaign summary: %w", encodeErr)
	}
	if !summary.Passed {
		return fmt.Errorf("chaos campaign did not pass")
	}
	return nil
}

func splitWorkers(value string) []string {
	raw := strings.Split(value, ",")
	result := make([]string, 0, len(raw))
	for _, worker := range raw {
		if worker = strings.TrimSpace(worker); worker != "" {
			result = append(result, worker)
		}
	}
	return result
}
