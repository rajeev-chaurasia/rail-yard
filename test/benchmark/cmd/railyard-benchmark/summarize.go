package main

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

type stringList []string

func (values *stringList) String() string {
	return fmt.Sprint([]string(*values))
}

func (values *stringList) Set(value string) error {
	if value == "" {
		return errors.New("run directory must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func summarizeRuns(arguments []string) error {
	flags := flag.NewFlagSet("summarize", flag.ContinueOnError)
	var output string
	var runDirectories stringList
	flags.StringVar(&output, "output", "", "new suite summary artifact directory")
	flags.Var(&runDirectories, "run-dir", "finalized run directory; repeat four times")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("summarize does not accept positional arguments")
	}
	if output == "" {
		return errors.New("-output is required")
	}
	for _, runDirectory := range runDirectories {
		outputInsideRun, err := pathWithin(runDirectory, output)
		if err != nil {
			return err
		}
		runInsideOutput, err := pathWithin(output, runDirectory)
		if err != nil {
			return err
		}
		if outputInsideRun || runInsideOutput {
			return fmt.Errorf("suite output and run directory must not contain each other: %s", runDirectory)
		}
	}

	summary := evidence.SummarizeSuite(runDirectories, time.Now().UTC())
	if err := createArtifactDirectory(output); err != nil {
		return err
	}
	if err := evidence.WriteJSON(filepath.Join(output, "benchmark-summary.json"), summary); err != nil {
		return fmt.Errorf("write suite summary: %w", err)
	}
	if err := evidence.GenerateChecksums(output); err != nil {
		return fmt.Errorf("write suite checksums: %w", err)
	}
	if err := evidence.VerifyChecksums(output); err != nil {
		return fmt.Errorf("verify suite artifacts: %w", err)
	}
	if !summary.Valid {
		return fmt.Errorf("suite is invalid: %v", summary.InvalidReasons)
	}

	fmt.Println("suite summary is valid")
	printMedianRate("admissions", summary.Admissions)
	printMedianRate("durable lease grants", summary.DurableLeaseGrants)
	printMedianRate("successful completions", summary.SuccessfulCompletions)
	return nil
}

func printMedianRate(name string, rate evidence.MedianRate) {
	if !rate.Available || rate.MedianPerMinute == nil {
		fmt.Printf("%s median: unavailable (%s)\n", name, rate.UnavailableReason)
		return
	}
	fmt.Printf("%s median: %.2f/min\n", name, *rate.MedianPerMinute)
}
