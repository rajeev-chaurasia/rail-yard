package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type qualificationError struct {
	status string
}

func (e qualificationError) Error() string {
	return "qualification status: " + e.status
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("railyard-results", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var paths inputPaths
	var output string
	flags.StringVar(&paths.benchmark, "benchmark-suite", "", "benchmark suite summary path")
	flags.StringVar(&paths.chaos, "chaos-campaign", "", "chaos campaign summary path")
	flags.StringVar(&paths.replay, "replay-summary", "", "deterministic replay summary path")
	flags.StringVar(&paths.slo, "slo-summary", "", "SLO rule summary path")
	flags.StringVar(&paths.p5, "p5-walkthrough", "", "P5 walkthrough summary path")
	flags.StringVar(&output, "output", "", "new qualification summary JSON path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	required := []struct {
		name  string
		value string
	}{
		{name: "benchmark-suite", value: paths.benchmark},
		{name: "chaos-campaign", value: paths.chaos},
		{name: "replay-summary", value: paths.replay},
		{name: "slo-summary", value: paths.slo},
		{name: "p5-walkthrough", value: paths.p5},
		{name: "output", value: output},
	}
	for _, input := range required {
		if strings.TrimSpace(input.value) == "" {
			return fmt.Errorf("--%s is required", input.name)
		}
	}
	if err := requireNewOutput(output); err != nil {
		return err
	}

	summary := evaluate(paths)
	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("encode qualification summary: %w", err)
	}
	body = append(body, '\n')
	if err := writeNewAtomic(output, body); err != nil {
		return fmt.Errorf("write qualification summary: %w", err)
	}
	if summary.Qualified {
		if _, err := fmt.Fprintln(stdout, summary.ActivationText); err != nil {
			return fmt.Errorf("write activation text: %w", err)
		}
		return nil
	}
	return qualificationError{status: summary.Status}
}

func requireNewOutput(path string) error {
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		return fmt.Errorf("output path already exists: %s (%s)", path, info.Mode())
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("inspect output path %s: %w", path, err)
	}
}

func writeNewAtomic(path string, body []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary output: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary output: %w", err)
	}
	if err := os.Chmod(temporaryPath, 0o644); err != nil {
		return fmt.Errorf("set temporary output permissions: %w", err)
	}
	if err := os.Link(temporaryPath, path); err != nil {
		return fmt.Errorf("publish new output atomically: %w", err)
	}
	return nil
}
