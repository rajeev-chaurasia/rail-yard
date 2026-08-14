package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

func reconcileRun(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	var runDirectory string
	var databaseSnapshot string
	var quiesced bool
	flags.StringVar(&runDirectory, "run-dir", "", "drained workload artifact directory")
	flags.StringVar(&databaseSnapshot, "db-snapshot", "", "quiesced SQLite snapshot")
	flags.BoolVar(&quiesced, "quiesced", false, "confirm writers stopped and WAL data was captured")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("reconcile does not accept positional arguments")
	}
	if runDirectory == "" || databaseSnapshot == "" {
		return errors.New("-run-dir and -db-snapshot are required")
	}
	if !quiesced {
		return errors.New("-quiesced is required; stop writers and capture a consistent SQLite/WAL snapshot")
	}
	inside, err := pathWithin(runDirectory, databaseSnapshot)
	if err != nil {
		return err
	}
	if inside {
		return errors.New("-db-snapshot must be outside the run artifact directory")
	}
	if err := evidence.VerifyChecksums(runDirectory); err != nil {
		return fmt.Errorf("verify preliminary artifacts: %w", err)
	}

	var manifest evidence.RunManifest
	if err := evidence.ReadJSON(filepath.Join(runDirectory, "manifest.json"), &manifest); err != nil {
		return fmt.Errorf("read run manifest: %w", err)
	}
	if manifest.Status != evidence.StatusAwaitingReconciliation {
		return fmt.Errorf("run status is %q, want %q", manifest.Status, evidence.StatusAwaitingReconciliation)
	}
	submissions, err := evidence.ReadJSONLines[evidence.SubmissionSample](
		filepath.Join(runDirectory, "submitted.jsonl"),
	)
	if err != nil {
		return fmt.Errorf("read submission samples: %w", err)
	}
	drainSamples, err := evidence.ReadJSONLines[evidence.DrainSample](
		filepath.Join(runDirectory, "drain-samples.jsonl"),
	)
	if err != nil {
		return fmt.Errorf("read drain samples: %w", err)
	}

	manifest.DatabaseQuiesced = true
	report, samples, summary, err := evidence.ReconcileSnapshot(
		ctx,
		databaseSnapshot,
		manifest,
		submissions,
		drainSamples,
	)
	if err != nil {
		reconcileErr := fmt.Errorf("reconcile database snapshot: %w", err)
		finalizeErr := finalizeReconciliationError(
			runDirectory,
			databaseSnapshot,
			&manifest,
			len(submissions),
			reconcileErr,
		)
		return errors.Join(reconcileErr, finalizeErr)
	}
	finalizedAt := time.Now().UTC()
	manifest.DatabaseSHA256 = report.DatabaseSHA256
	manifest.DatabaseFilesSHA256 = report.DatabaseFilesSHA256
	manifest.FinalizedAt = &finalizedAt
	manifest.InvalidReasons = summary.InvalidReasons
	if report.Passed {
		manifest.Status = evidence.StatusValid
	} else {
		manifest.Status = evidence.StatusInvalid
	}

	if err := evidence.WriteJSON(filepath.Join(runDirectory, "reconciliation.json"), report); err != nil {
		return fmt.Errorf("write reconciliation report: %w", err)
	}
	if err := evidence.WriteJSONLines(
		filepath.Join(runDirectory, "benchmark-samples.jsonl"),
		samples,
	); err != nil {
		return fmt.Errorf("write benchmark samples: %w", err)
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "benchmark-summary.json"), summary); err != nil {
		return fmt.Errorf("write benchmark summary: %w", err)
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		return fmt.Errorf("write finalized manifest: %w", err)
	}
	if err := evidence.GenerateChecksums(runDirectory); err != nil {
		return fmt.Errorf("write final checksums: %w", err)
	}
	if err := evidence.VerifyChecksums(runDirectory); err != nil {
		return fmt.Errorf("verify final artifacts: %w", err)
	}
	if !report.Passed {
		return fmt.Errorf("reconciliation invalidated run %s: %v", manifest.RunID, summary.InvalidReasons)
	}

	fmt.Printf("reconciliation passed for %s\n", manifest.RunID)
	printRate("admissions", summary.Admissions)
	printRate("durable lease grants", summary.DurableLeaseGrants)
	printRate("successful completions", summary.SuccessfulCompletions)
	return nil
}

func finalizeReconciliationError(
	runDirectory string,
	databaseSnapshot string,
	manifest *evidence.RunManifest,
	acceptedCount int,
	reconcileErr error,
) error {
	digests, _ := evidence.SQLiteSnapshotDigests(databaseSnapshot)
	digest := digests["database"]
	finalizedAt := time.Now().UTC()
	reason := reconcileErr.Error()
	manifest.DatabaseSHA256 = digest
	manifest.DatabaseFilesSHA256 = digests
	manifest.FinalizedAt = &finalizedAt
	manifest.Status = evidence.StatusInvalid
	manifest.InvalidReasons = []string{reason}
	report := evidence.ReconciliationReport{
		SchemaVersion:       evidence.SchemaVersion,
		RunID:               manifest.RunID,
		Passed:              false,
		OperationalError:    reason,
		DatabaseSHA256:      digest,
		DatabaseFilesSHA256: digests,
		AcceptedCount:       acceptedCount,
	}
	unavailableRate := evidence.RateSample{
		Available:         false,
		UnavailableReason: "reconciliation failed",
	}
	unavailableLatency := evidence.LatencySummary{
		Available:         false,
		UnavailableReason: "reconciliation failed",
	}
	summary := evidence.BenchmarkSummary{
		SchemaVersion:               evidence.SchemaVersion,
		RunID:                       manifest.RunID,
		Phase:                       manifest.Phase,
		Valid:                       false,
		InvalidReasons:              []string{reason},
		Admissions:                  unavailableRate,
		DurableLeaseGrants:          unavailableRate,
		SuccessfulCompletions:       unavailableRate,
		AdmissionToFirstLease:       unavailableLatency,
		CompletionLeaseToCompletion: unavailableLatency,
		EndToEnd:                    unavailableLatency,
	}
	var writeErrors []error
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "reconciliation.json"), report); err != nil {
		writeErrors = append(writeErrors, err)
	}
	if err := evidence.WriteJSONLines(
		filepath.Join(runDirectory, "benchmark-samples.jsonl"),
		[]evidence.BenchmarkSample{},
	); err != nil {
		writeErrors = append(writeErrors, err)
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "benchmark-summary.json"), summary); err != nil {
		writeErrors = append(writeErrors, err)
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		writeErrors = append(writeErrors, err)
	}
	if err := evidence.GenerateChecksums(runDirectory); err != nil {
		writeErrors = append(writeErrors, err)
	}
	return errors.Join(writeErrors...)
}

func pathWithin(parent, child string) (bool, error) {
	parentAbsolute, err := filepath.Abs(parent)
	if err != nil {
		return false, err
	}
	childAbsolute, err := filepath.Abs(child)
	if err != nil {
		return false, err
	}
	relative, err := filepath.Rel(parentAbsolute, childAbsolute)
	if err != nil {
		return false, err
	}
	return relative == "." ||
		(relative != ".." &&
			!filepath.IsAbs(relative) &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator))), nil
}

func printRate(name string, rate evidence.RateSample) {
	if !rate.Available || rate.PerMinute == nil {
		fmt.Printf("%s: unavailable (%s)\n", name, rate.UnavailableReason)
		return
	}
	fmt.Printf("%s: %.2f/min\n", name, *rate.PerMinute)
}
