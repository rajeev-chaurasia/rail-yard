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
	"path/filepath"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}

func runMain(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("railyard-reconcile", flag.ContinueOnError)
	flags.SetOutput(stderr)
	databasePath := flags.String("db", "", "path to the stopped SQLite database snapshot")
	manifestPath := flags.String("manifest", "", "path to submitted.jsonl")
	outputPath := flags.String("output", "", "optional JSON report path")
	expectedJobs := flags.Int("expected-jobs", 0, "required accepted job count, defaults to manifest size")
	maxDetails := flags.Int("max-details", 1000, "maximum violation details in the report")
	timeout := flags.Duration("timeout", 5*time.Minute, "reconciliation deadline")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		reportError(stderr, "unexpected arguments: %v", flags.Args())
		return 2
	}
	if *databasePath == "" || *manifestPath == "" {
		reportError(stderr, "--db and --manifest are required")
		return 2
	}
	if *expectedJobs < 0 || *maxDetails < 1 || *timeout <= 0 {
		reportError(
			stderr,
			"--expected-jobs must be nonnegative, --max-details and --timeout must be positive",
		)
		return 2
	}

	manifest, err := os.Open(*manifestPath)
	if err != nil {
		reportError(stderr, "open manifest: %v", err)
		return 2
	}
	accepted, readErr := reconcile.ReadManifest(manifest)
	closeErr := manifest.Close()
	if readErr != nil {
		reportError(stderr, "%v", readErr)
		return 2
	}
	if closeErr != nil {
		reportError(stderr, "close manifest: %v", closeErr)
		return 2
	}

	db, err := reconcile.OpenReadOnly(*databasePath)
	if err != nil {
		reportError(stderr, "%v", err)
		return 2
	}
	defer func() {
		_ = db.Close()
	}()

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(signalContext, *timeout)
	defer cancel()
	report, err := reconcile.Reconcile(ctx, db, accepted, reconcile.Options{
		ExpectedJobs: *expectedJobs,
		MaxDetails:   *maxDetails,
	})
	if err != nil {
		reportError(stderr, "%v", err)
		return 2
	}

	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		reportError(stderr, "encode report: %v", err)
		return 2
	}
	body = append(body, '\n')
	if _, err := stdout.Write(body); err != nil {
		reportError(stderr, "write report: %v", err)
		return 2
	}
	if *outputPath != "" {
		if err := writeAtomic(*outputPath, body); err != nil {
			reportError(stderr, "%v", err)
			return 2
		}
	}
	if !report.Passed {
		return 1
	}
	return 0
}

func writeAtomic(path string, body []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create report directory: %w", err)
	}
	file, err := os.CreateTemp(directory, ".reconciliation-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary report: %w", err)
	}
	temporary := file.Name()
	defer func() {
		_ = os.Remove(temporary)
	}()
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temporary report: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync temporary report: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary report: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("replace report: remove old file: %w", removeErr)
		}
		if retryErr := os.Rename(temporary, path); retryErr != nil {
			return fmt.Errorf("replace report: %w", retryErr)
		}
	}
	return nil
}

func reportError(writer io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(writer, format+"\n", args...)
}
