package evidence

import (
	"fmt"
	"path/filepath"
	"slices"
	"time"
)

var requiredRunArtifacts = []string{
	"manifest.json",
	"submitted.jsonl",
	"drain-samples.jsonl",
	"reconciliation.json",
	"benchmark-samples.jsonl",
	"benchmark-summary.json",
	ChecksumsFile,
}

type finalizedRun struct {
	directory string
	manifest  RunManifest
	summary   BenchmarkSummary
	samples   []BenchmarkSample
	reference SuiteRun
}

func SummarizeSuite(runDirectories []string, now time.Time) SuiteSummary {
	suite := SuiteSummary{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   now.UTC(),
	}
	if len(runDirectories) != 4 {
		suite.InvalidReasons = append(
			suite.InvalidReasons,
			fmt.Sprintf("suite requires exactly four run directories, got %d", len(runDirectories)),
		)
	}

	var warmups []finalizedRun
	var measured []finalizedRun
	seenDirectories := make(map[string]struct{}, len(runDirectories))
	seenRunIDs := make(map[string]struct{}, len(runDirectories))
	for _, directory := range runDirectories {
		absolute, err := filepath.Abs(directory)
		if err != nil {
			suite.InvalidReasons = append(suite.InvalidReasons, fmt.Sprintf("%s: %v", directory, err))
			continue
		}
		if _, duplicate := seenDirectories[absolute]; duplicate {
			suite.InvalidReasons = append(suite.InvalidReasons, fmt.Sprintf("%s: duplicate run directory", directory))
			continue
		}
		seenDirectories[absolute] = struct{}{}
		run, err := loadFinalizedRun(directory)
		if err != nil {
			suite.InvalidReasons = append(suite.InvalidReasons, fmt.Sprintf("%s: %v", directory, err))
			continue
		}
		if _, duplicate := seenRunIDs[run.manifest.RunID]; duplicate {
			suite.InvalidReasons = append(
				suite.InvalidReasons,
				fmt.Sprintf("%s: duplicate run ID %s", directory, run.manifest.RunID),
			)
			continue
		}
		seenRunIDs[run.manifest.RunID] = struct{}{}
		switch run.manifest.Phase {
		case PhaseWarmup:
			warmups = append(warmups, run)
		case PhaseMeasured:
			measured = append(measured, run)
		default:
			suite.InvalidReasons = append(
				suite.InvalidReasons,
				fmt.Sprintf("%s: unsupported phase %q", directory, run.manifest.Phase),
			)
		}
	}
	if len(warmups) != 1 {
		suite.InvalidReasons = append(
			suite.InvalidReasons,
			fmt.Sprintf("suite requires one warmup, got %d", len(warmups)),
		)
	}
	if len(measured) != 3 {
		suite.InvalidReasons = append(
			suite.InvalidReasons,
			fmt.Sprintf("suite requires three measured runs, got %d", len(measured)),
		)
	}
	if len(warmups) == 1 {
		suite.Warmup = warmups[0].reference
	}
	slices.SortFunc(measured, func(left, right finalizedRun) int {
		return left.manifest.StartedAt.Compare(right.manifest.StartedAt)
	})
	for _, run := range measured {
		suite.MeasuredRuns = append(suite.MeasuredRuns, run.reference)
	}

	allRuns := append(slices.Clone(warmups), measured...)
	if len(allRuns) > 1 {
		baseline := allRuns[0].manifest.Config
		baseline.Seed = 0
		for _, run := range allRuns[1:] {
			config := run.manifest.Config
			config.Seed = 0
			if config != baseline {
				suite.InvalidReasons = append(
					suite.InvalidReasons,
					fmt.Sprintf("%s: workload configuration differs from the suite baseline", run.directory),
				)
			}
		}
	}

	suite.Admissions = medianRateForRuns(measured, func(summary BenchmarkSummary) RateSample {
		return summary.Admissions
	})
	suite.DurableLeaseGrants = medianRateForRuns(measured, func(summary BenchmarkSummary) RateSample {
		return summary.DurableLeaseGrants
	})
	suite.SuccessfulCompletions = medianRateForRuns(measured, func(summary BenchmarkSummary) RateSample {
		return summary.SuccessfulCompletions
	})

	var admissionToLease, leaseToCompletion, endToEnd []time.Duration
	for _, run := range measured {
		for _, sample := range run.samples {
			admissionToLease = append(admissionToLease, sample.AdmissionToFirstLease)
			leaseToCompletion = append(leaseToCompletion, sample.CompletionLeaseToCompletion)
			endToEnd = append(endToEnd, sample.EndToEnd)
		}
	}
	suite.AdmissionToFirstLease = summarizeLatency(admissionToLease)
	suite.CompletionLeaseToCompletion = summarizeLatency(leaseToCompletion)
	suite.EndToEnd = summarizeLatency(endToEnd)

	if !suite.Admissions.Available {
		suite.InvalidReasons = append(suite.InvalidReasons, "admission rate median is unavailable")
	}
	if !suite.DurableLeaseGrants.Available {
		suite.InvalidReasons = append(suite.InvalidReasons, "durable lease grant rate median is unavailable")
	}
	if !suite.SuccessfulCompletions.Available {
		suite.InvalidReasons = append(suite.InvalidReasons, "completion rate median is unavailable")
	}
	if !suite.AdmissionToFirstLease.Available ||
		!suite.CompletionLeaseToCompletion.Available ||
		!suite.EndToEnd.Available {
		suite.InvalidReasons = append(suite.InvalidReasons, "one or more latency distributions are unavailable")
	}
	slices.Sort(suite.InvalidReasons)
	suite.InvalidReasons = slices.Compact(suite.InvalidReasons)
	suite.Valid = len(suite.InvalidReasons) == 0
	return suite
}

func loadFinalizedRun(directory string) (finalizedRun, error) {
	var run finalizedRun
	run.directory = directory
	for _, name := range requiredRunArtifacts {
		if _, err := filepath.Abs(filepath.Join(directory, name)); err != nil {
			return run, fmt.Errorf("resolve required artifact %s: %w", name, err)
		}
		if name == ChecksumsFile {
			continue
		}
		if _, err := FileSHA256(filepath.Join(directory, name)); err != nil {
			return run, fmt.Errorf("required artifact %s: %w", name, err)
		}
	}
	if err := VerifyChecksums(directory); err != nil {
		return run, fmt.Errorf("artifact integrity: %w", err)
	}
	if err := ReadJSON(filepath.Join(directory, "manifest.json"), &run.manifest); err != nil {
		return run, fmt.Errorf("read manifest: %w", err)
	}
	if err := ReadJSON(filepath.Join(directory, "benchmark-summary.json"), &run.summary); err != nil {
		return run, fmt.Errorf("read benchmark summary: %w", err)
	}
	samples, err := ReadJSONLines[BenchmarkSample](filepath.Join(directory, "benchmark-samples.jsonl"))
	if err != nil {
		return run, fmt.Errorf("read benchmark samples: %w", err)
	}
	run.samples = samples
	if run.manifest.Status != StatusValid || !run.summary.Valid {
		return run, fmt.Errorf("run is not valid")
	}
	if run.manifest.SchemaVersion != SchemaVersion || run.summary.SchemaVersion != SchemaVersion {
		return run, fmt.Errorf("unsupported artifact schema version")
	}
	if run.manifest.RunID != run.summary.RunID || run.manifest.Phase != run.summary.Phase {
		return run, fmt.Errorf("manifest and summary identity mismatch")
	}
	if len(run.samples) != run.manifest.Config.JobCount ||
		len(run.samples) != run.summary.CanonicalJobCount {
		return run, fmt.Errorf(
			"benchmark sample count %d does not match manifest %d and summary %d",
			len(run.samples),
			run.manifest.Config.JobCount,
			run.summary.CanonicalJobCount,
		)
	}
	sampleJobs := make(map[string]struct{}, len(run.samples))
	for _, sample := range run.samples {
		if sample.SchemaVersion != SchemaVersion ||
			sample.RunID != run.manifest.RunID ||
			sample.TimestampSource != sqliteTimestampSource ||
			sample.CompletionState != "SUCCEEDED" {
			return run, fmt.Errorf("benchmark samples contain incompatible identity or timestamp source")
		}
		if _, duplicate := sampleJobs[sample.JobID]; duplicate {
			return run, fmt.Errorf("benchmark samples repeat job %s", sample.JobID)
		}
		sampleJobs[sample.JobID] = struct{}{}
	}
	summaryDigest, err := FileSHA256(filepath.Join(directory, "benchmark-summary.json"))
	if err != nil {
		return run, err
	}
	run.reference = SuiteRun{
		RunID:        run.manifest.RunID,
		Phase:        run.manifest.Phase,
		ArtifactPath: filepath.ToSlash(directory),
		SHA256:       summaryDigest,
	}
	return run, nil
}

func medianRateForRuns(
	runs []finalizedRun,
	selectRate func(BenchmarkSummary) RateSample,
) MedianRate {
	if len(runs) != 3 {
		return MedianRate{
			Available:         false,
			UnavailableReason: "exactly three measured runs are required",
		}
	}
	values := make([]float64, 0, len(runs))
	for _, run := range runs {
		rate := selectRate(run.summary)
		if !rate.Available || rate.PerMinute == nil {
			return MedianRate{
				Available:         false,
				UnavailableReason: fmt.Sprintf("rate is unavailable for run %s", run.manifest.RunID),
			}
		}
		values = append(values, *rate.PerMinute)
	}
	median, err := Median(values)
	if err != nil {
		return MedianRate{Available: false, UnavailableReason: err.Error()}
	}
	return MedianRate{
		Available:        true,
		SamplesPerMinute: values,
		MedianPerMinute:  &median,
	}
}
