package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "railyard benchmark:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	options := orchestrationOptions{}
	flags := flag.NewFlagSet("railyard-benchmark-compose", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.composeFile, "compose-file", "deploy/compose.yaml", "Docker Compose file")
	flags.StringVar(&options.projectPrefix, "project-prefix", "rail-yard-benchmark", "isolated Compose project prefix")
	flags.StringVar(&options.output, "output", "artifacts/benchmark", "new suite artifact directory")
	flags.BoolVar(&options.resume, "resume", false, "resume a suite from finalized run checkpoints")
	flags.StringVar(&options.dockerExecutable, "docker", "docker", "Docker CLI executable")
	flags.StringVar(&options.goExecutable, "go", "go", "Go executable used for benchmark commands")
	flags.StringVar(&options.databasePath, "database-path", "/var/lib/railyard/railyard.db", "SQLite path in the server container")
	flags.IntVar(&options.runs, "runs", 3, "number of measured fresh-volume runs")
	flags.IntVar(&options.jobs, "jobs", 50_000, "accepted jobs per run")
	flags.IntVar(&options.workers, "workers", 8, "worker service count")
	flags.IntVar(&options.workerSlots, "worker-slots", 16, "slots per worker")
	flags.IntVar(&options.hostPort, "host-port", 0, "server host port, or zero to allocate one for the suite")
	flags.IntVar(&options.submitConcurrency, "submit-concurrency", 64, "maximum concurrent submissions")
	flags.IntVar(&options.pollConcurrency, "poll-concurrency", 128, "maximum concurrent job inspections")
	flags.Int64Var(&options.seed, "seed", 1, "deterministic base seed")
	flags.DurationVar(&options.startupTimeout, "startup-timeout", 3*time.Minute, "Compose startup predicate deadline")
	flags.DurationVar(&options.drainTimeout, "drain-timeout", 30*time.Minute, "workload drain predicate deadline")
	flags.DurationVar(&options.requestTimeout, "request-timeout", 10*time.Second, "individual HTTP request timeout")
	flags.DurationVar(&options.pollInterval, "poll-interval", 250*time.Millisecond, "predicate polling interval")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if err := options.validate(); err != nil {
		return err
	}
	return orchestrate(ctx, options, processRunner{}, stdout)
}
