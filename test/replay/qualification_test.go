package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRunReplayProcessesUsesDistinctProcesses(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	inputPath := filepath.Join(directory, "input.jsonl")
	body := []byte("{\"sequence\":1}\n{\"sequence\":2}\n{\"sequence\":3}\n")
	if err := os.WriteFile(inputPath, body, 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	invocation := replayInvocation{
		Path:       os.Args[0],
		PrefixArgs: []string{"-test.run=^TestReplayHelperProcess$", "--"},
		Env:        append(os.Environ(), "RAILYARD_REPLAY_HELPER=1"),
	}
	runs, err := runReplayProcesses(invocation, inputPath, inputPath, directory, 3)
	if err != nil {
		t.Fatalf("runReplayProcesses: %v", err)
	}
	if len(runs) != requiredReplayCount {
		t.Fatalf("runs = %d, want %d", len(runs), requiredReplayCount)
	}
	seen := make(map[int]struct{}, len(runs))
	for _, run := range runs {
		if run.ProcessID == os.Getpid() {
			t.Fatalf("run %d reused qualification process %d", run.Run, run.ProcessID)
		}
		if _, duplicate := seen[run.ProcessID]; duplicate {
			t.Fatalf("process ID %d was reused", run.ProcessID)
		}
		seen[run.ProcessID] = struct{}{}
		if run.OutputSHA256 != bytesSHA256(body) || run.ReportedSHA256 != run.OutputSHA256 {
			t.Fatalf("run %d has inconsistent digests: %#v", run.Run, run)
		}
	}
}

func TestPrepareInputCreatesCheckedCanonicalLog(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	inputPath := filepath.Join(directory, "input.jsonl")
	canonicalPath := filepath.Join(directory, "canonical.jsonl")
	if err := prepareInput("", inputPath, canonicalPath, 3); err != nil {
		t.Fatalf("prepareInput: %v", err)
	}
	records, err := readRecords(inputPath)
	if err != nil {
		t.Fatalf("readRecords: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3", len(records))
	}
	canonical, err := os.ReadFile(canonicalPath)
	if err != nil {
		t.Fatalf("read canonical decisions: %v", err)
	}
	if lines := strings.Count(string(canonical), "\n"); lines != 3 {
		t.Fatalf("canonical decision lines = %d, want 3", lines)
	}
}

func TestCompareFilesRejectsMismatch(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	left := filepath.Join(directory, "left")
	right := filepath.Join(directory, "right")
	if err := os.WriteFile(left, []byte("matching-prefix-left"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(right, []byte("matching-prefix-right"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := compareFiles(left, right); err == nil {
		t.Fatal("compareFiles accepted different files")
	}
}

func TestQualificationCommandRecordsInputAndOutput(t *testing.T) {
	t.Parallel()

	got := qualificationCommand(qualificationConfig{
		InputPath:  "captured.jsonl",
		OutputPath: "results/replay/run",
	})
	want := []string{
		"go", "run", "./test/replay",
		"--input", "captured.jsonl",
		"--output", "results/replay/run",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("qualificationCommand = %v, want %v", got, want)
	}
}

func TestReplayHelperProcess(t *testing.T) {
	if os.Getenv("RAILYARD_REPLAY_HELPER") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing helper argument separator")
	}
	flags := flag.NewFlagSet("replay-helper", flag.ContinueOnError)
	inputPath := flags.String("input", "", "")
	outputPath := flags.String("output", "", "")
	if err := flags.Parse(os.Args[separator+1:]); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(*inputPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*outputPath, body, 0o644); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	records := 0
	for scanner.Scan() {
		records++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	summary, err := json.Marshal(replayCLIResult{
		Records: records,
		Digest:  bytesSHA256(body),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintln(os.Stderr, string(summary)); err != nil {
		t.Fatal(err)
	}
}

func bytesSHA256(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
