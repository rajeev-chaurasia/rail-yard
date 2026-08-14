package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/decisionlog"
	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

const (
	qualificationSchemaVersion = 2
	requiredDecisionCount      = 50_000
	requiredReplayCount        = 3
)

type qualificationConfig struct {
	InputPath  string
	OutputPath string
}

type qualificationResult struct {
	OutputPath      string
	Decisions       int
	CanonicalSHA256 string
	Runs            []replayRun
}

type replayManifest struct {
	SchemaVersion            int         `json:"schema_version"`
	GeneratedAt              time.Time   `json:"generated_at"`
	GoVersion                string      `json:"go_version"`
	OS                       string      `json:"os"`
	Architecture             string      `json:"architecture"`
	QualificationCommand     []string    `json:"qualification_command"`
	BuildCommand             []string    `json:"build_command"`
	ReplayBinarySHA256       string      `json:"replay_binary_sha256"`
	Decisions                int         `json:"decisions"`
	Input                    string      `json:"input"`
	InputSHA256              string      `json:"input_sha256"`
	CanonicalDecisions       string      `json:"canonical_decisions"`
	CanonicalDecisionsSHA256 string      `json:"canonical_decisions_sha256"`
	Runs                     []replayRun `json:"runs"`
	Passed                   bool        `json:"passed"`
}

type replayRun struct {
	Run            int      `json:"run"`
	ProcessID      int      `json:"process_id"`
	Command        []string `json:"command"`
	Output         string   `json:"output"`
	Records        int      `json:"records"`
	ReportedSHA256 string   `json:"reported_sha256"`
	OutputSHA256   string   `json:"output_sha256"`
}

type replaySummary struct {
	SchemaVersion       int       `json:"schema_version"`
	GeneratedAt         time.Time `json:"generated_at"`
	GoVersion           string    `json:"go_version"`
	OS                  string    `json:"os"`
	Architecture        string    `json:"architecture"`
	Decisions           int       `json:"decisions"`
	CleanProcessReplays int       `json:"clean_process_replays"`
	ByteMatchPercent    float64   `json:"byte_match_percent"`
	SHA256              string    `json:"sha256"`
	Command             string    `json:"command"`
	Passed              bool      `json:"passed"`
}

type replayInvocation struct {
	Path       string
	PrefixArgs []string
	Env        []string
}

type replayProcess struct {
	run     replayRun
	command *exec.Cmd
	stderr  bytes.Buffer
}

type replayCLIResult struct {
	Records int    `json:"records"`
	Digest  string `json:"digest"`
}

func qualify(config qualificationConfig) (qualificationResult, error) {
	var result qualificationResult
	outputPath, err := filepath.Abs(config.OutputPath)
	if err != nil {
		return result, fmt.Errorf("resolve output path: %w", err)
	}
	if err := requireAbsent(outputPath); err != nil {
		return result, err
	}
	parent := filepath.Dir(outputPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return result, fmt.Errorf("create output parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".replay-evidence-*")
	if err != nil {
		return result, fmt.Errorf("create evidence staging directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(staging)
	}()

	inputPath := filepath.Join(staging, "replay-input.jsonl")
	canonicalPath := filepath.Join(staging, "canonical-decisions.jsonl")
	if err := prepareInput(config.InputPath, inputPath, canonicalPath, requiredDecisionCount); err != nil {
		return result, err
	}
	inputDigest, err := evidence.FileSHA256(inputPath)
	if err != nil {
		return result, fmt.Errorf("hash replay input: %w", err)
	}
	canonicalDigest, err := evidence.FileSHA256(canonicalPath)
	if err != nil {
		return result, fmt.Errorf("hash canonical decisions: %w", err)
	}

	repositoryRoot, err := findRepositoryRoot()
	if err != nil {
		return result, err
	}
	invocation, buildCommand, binaryDigest, cleanupBinary, err := buildReplayBinary(repositoryRoot, parent)
	if err != nil {
		return result, err
	}
	defer cleanupBinary()

	runs, err := runReplayProcesses(
		invocation,
		inputPath,
		canonicalPath,
		staging,
		requiredDecisionCount,
	)
	if err != nil {
		return result, err
	}

	generatedAt := time.Now().UTC()
	qualificationCommand := qualificationCommand(config)
	manifest := replayManifest{
		SchemaVersion:            qualificationSchemaVersion,
		GeneratedAt:              generatedAt,
		GoVersion:                runtime.Version(),
		OS:                       runtime.GOOS,
		Architecture:             runtime.GOARCH,
		QualificationCommand:     qualificationCommand,
		BuildCommand:             buildCommand,
		ReplayBinarySHA256:       binaryDigest,
		Decisions:                requiredDecisionCount,
		Input:                    filepath.Base(inputPath),
		InputSHA256:              inputDigest,
		CanonicalDecisions:       filepath.Base(canonicalPath),
		CanonicalDecisionsSHA256: canonicalDigest,
		Runs:                     runs,
		Passed:                   true,
	}
	if err := evidence.WriteJSON(filepath.Join(staging, "manifest.json"), manifest); err != nil {
		return result, fmt.Errorf("write replay manifest: %w", err)
	}
	summary := replaySummary{
		SchemaVersion:       1,
		GeneratedAt:         generatedAt,
		GoVersion:           runtime.Version(),
		OS:                  runtime.GOOS,
		Architecture:        runtime.GOARCH,
		Decisions:           requiredDecisionCount,
		CleanProcessReplays: len(runs),
		ByteMatchPercent:    100,
		SHA256:              canonicalDigest,
		Command:             strings.Join(qualificationCommand, " "),
		Passed:              true,
	}
	if err := evidence.WriteJSON(filepath.Join(staging, "summary.json"), summary); err != nil {
		return result, fmt.Errorf("write replay summary: %w", err)
	}
	if err := evidence.GenerateChecksums(staging); err != nil {
		return result, fmt.Errorf("write replay checksums: %w", err)
	}
	if err := evidence.VerifyChecksums(staging); err != nil {
		return result, fmt.Errorf("verify replay checksums: %w", err)
	}
	if err := requireAbsent(outputPath); err != nil {
		return result, err
	}
	if err := os.Rename(staging, outputPath); err != nil {
		return result, fmt.Errorf("publish replay evidence: %w", err)
	}

	result = qualificationResult{
		OutputPath:      outputPath,
		Decisions:       requiredDecisionCount,
		CanonicalSHA256: canonicalDigest,
		Runs:            runs,
	}
	return result, nil
}

func prepareInput(sourcePath, inputPath, canonicalPath string, decisions int) error {
	var records []decisionlog.Record
	var err error
	if sourcePath == "" {
		records, err = generateRecords(decisions)
	} else {
		records, err = readRecords(sourcePath)
	}
	if err != nil {
		return err
	}
	if len(records) != decisions {
		return fmt.Errorf("decision log has %d records, want %d", len(records), decisions)
	}
	if err := evidence.WriteJSONLines(inputPath, records); err != nil {
		return fmt.Errorf("write canonical replay input: %w", err)
	}
	if err := writeCanonicalDecisions(canonicalPath, records); err != nil {
		return err
	}
	checked, err := readRecords(inputPath)
	if err != nil {
		return fmt.Errorf("check canonical replay input: %w", err)
	}
	if len(checked) != decisions {
		return fmt.Errorf("checked decision log has %d records, want %d", len(checked), decisions)
	}
	return nil
}

func generateRecords(decisions int) ([]decisionlog.Record, error) {
	records := make([]decisionlog.Record, 0, decisions)
	previousHash := ""
	for sequence := int64(1); sequence <= int64(decisions); sequence++ {
		jobID := fmt.Sprintf("%032x", sequence)
		snapshot := scheduler.Snapshot{
			Sequence:      sequence,
			LogicalTimeNS: sequence * 1_000_000,
			WorkerSlots:   1,
			BatchLimit:    1,
			Queues: []scheduler.Queue{{
				TenantID: "qualification",
				Name:     "replay",
				Weight:   1,
				Candidates: []scheduler.Candidate{{
					JobID:    jobID,
					TenantID: "qualification",
					Queue:    "replay",
					ReadySeq: sequence,
					SlotCost: 1,
				}},
			}},
		}
		decision, err := scheduler.Decide(snapshot)
		if err != nil {
			return nil, fmt.Errorf("generate decision %d: %w", sequence, err)
		}
		record, err := decisionlog.NewRecord(previousHash, snapshot, decision)
		if err != nil {
			return nil, fmt.Errorf("generate record %d: %w", sequence, err)
		}
		records = append(records, record)
		previousHash = record.Hash
	}
	return records, nil
}

func readRecords(path string) ([]decisionlog.Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open decision log: %w", err)
	}
	records, readErr := decisionlog.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close decision log: %w", closeErr)
	}
	return records, nil
}

func writeCanonicalDecisions(path string, records []decisionlog.Record) error {
	return writeAtomic(path, func(writer io.Writer) error {
		for _, record := range records {
			canonical, err := decisionlog.CanonicalDecision(record.Decision)
			if err != nil {
				return fmt.Errorf("canonicalize decision %d: %w", record.Sequence, err)
			}
			if _, err := writer.Write(canonical); err != nil {
				return fmt.Errorf("write decision %d: %w", record.Sequence, err)
			}
		}
		return nil
	})
}

func buildReplayBinary(
	repositoryRoot string,
	parent string,
) (replayInvocation, []string, string, func(), error) {
	var invocation replayInvocation
	goBinary, err := exec.LookPath("go")
	if err != nil {
		return invocation, nil, "", func() {}, fmt.Errorf("locate go: %w", err)
	}
	buildDirectory, err := os.MkdirTemp(parent, ".replay-binary-*")
	if err != nil {
		return invocation, nil, "", func() {}, fmt.Errorf("create binary staging directory: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(buildDirectory)
	}
	name := "railyard-replay"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binaryPath := filepath.Join(buildDirectory, name)
	command := []string{goBinary, "build", "-trimpath", "-o", binaryPath, "./cmd/railyard-replay"}
	build := exec.Command(command[0], command[1:]...)
	build.Dir = repositoryRoot
	output, err := build.CombinedOutput()
	if err != nil {
		cleanup()
		return invocation, command, "", func() {}, fmt.Errorf(
			"build railyard-replay: %w: %s",
			err,
			strings.TrimSpace(string(output)),
		)
	}
	digest, err := evidence.FileSHA256(binaryPath)
	if err != nil {
		cleanup()
		return invocation, command, "", func() {}, fmt.Errorf("hash railyard-replay: %w", err)
	}
	return replayInvocation{Path: binaryPath}, command, digest, cleanup, nil
}

func runReplayProcesses(
	invocation replayInvocation,
	inputPath string,
	canonicalPath string,
	outputDirectory string,
	expectedRecords int,
) ([]replayRun, error) {
	processes := make([]*replayProcess, 0, requiredReplayCount)
	for run := 1; run <= requiredReplayCount; run++ {
		outputName := fmt.Sprintf("replay-output-%d.jsonl", run)
		outputPath := filepath.Join(outputDirectory, outputName)
		args := append(slices.Clone(invocation.PrefixArgs), "--input", inputPath, "--output", outputPath)
		command := exec.Command(invocation.Path, args...)
		if invocation.Env != nil {
			command.Env = invocation.Env
		}
		process := &replayProcess{
			run: replayRun{
				Run:     run,
				Command: append([]string{invocation.Path}, args...),
				Output:  outputName,
			},
			command: command,
		}
		command.Stderr = &process.stderr
		if err := command.Start(); err != nil {
			stopReplayProcesses(processes)
			return nil, fmt.Errorf("start replay %d: %w", run, err)
		}
		process.run.ProcessID = command.Process.Pid
		processes = append(processes, process)
	}
	if err := requireDistinctProcessIDs(processes); err != nil {
		stopReplayProcesses(processes)
		return nil, err
	}

	var waitErrors []error
	for _, process := range processes {
		if err := process.command.Wait(); err != nil {
			waitErrors = append(waitErrors, fmt.Errorf(
				"replay %d process %d: %w: %s",
				process.run.Run,
				process.run.ProcessID,
				err,
				strings.TrimSpace(process.stderr.String()),
			))
		}
	}
	if err := errors.Join(waitErrors...); err != nil {
		return nil, err
	}

	runs := make([]replayRun, 0, len(processes))
	for _, process := range processes {
		var cliResult replayCLIResult
		decoder := json.NewDecoder(bytes.NewReader(process.stderr.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cliResult); err != nil {
			return nil, fmt.Errorf("decode replay %d summary: %w", process.run.Run, err)
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("replay %d summary contains extra data", process.run.Run)
		}
		if cliResult.Records != expectedRecords {
			return nil, fmt.Errorf(
				"replay %d reported %d records, want %d",
				process.run.Run,
				cliResult.Records,
				expectedRecords,
			)
		}
		outputPath := filepath.Join(outputDirectory, process.run.Output)
		if err := compareFiles(canonicalPath, outputPath); err != nil {
			return nil, fmt.Errorf("replay %d differs from captured decisions: %w", process.run.Run, err)
		}
		digest, err := evidence.FileSHA256(outputPath)
		if err != nil {
			return nil, fmt.Errorf("hash replay %d output: %w", process.run.Run, err)
		}
		if cliResult.Digest != digest {
			return nil, fmt.Errorf(
				"replay %d reported digest %s, actual %s",
				process.run.Run,
				cliResult.Digest,
				digest,
			)
		}
		process.run.Records = cliResult.Records
		process.run.ReportedSHA256 = cliResult.Digest
		process.run.OutputSHA256 = digest
		runs = append(runs, process.run)
	}
	for left := 0; left < len(runs); left++ {
		for right := left + 1; right < len(runs); right++ {
			leftPath := filepath.Join(outputDirectory, runs[left].Output)
			rightPath := filepath.Join(outputDirectory, runs[right].Output)
			if err := compareFiles(leftPath, rightPath); err != nil {
				return nil, fmt.Errorf(
					"replay outputs %d and %d differ: %w",
					runs[left].Run,
					runs[right].Run,
					err,
				)
			}
		}
	}
	return runs, nil
}

func requireDistinctProcessIDs(processes []*replayProcess) error {
	seen := make(map[int]struct{}, len(processes))
	for _, process := range processes {
		pid := process.run.ProcessID
		if pid <= 0 || pid == os.Getpid() {
			return fmt.Errorf("replay %d has invalid process ID %d", process.run.Run, pid)
		}
		if _, duplicate := seen[pid]; duplicate {
			return fmt.Errorf("replay process ID %d is not unique", pid)
		}
		seen[pid] = struct{}{}
	}
	return nil
}

func stopReplayProcesses(processes []*replayProcess) {
	for _, process := range processes {
		if process.command.Process != nil {
			_ = process.command.Process.Kill()
		}
	}
	for _, process := range processes {
		_ = process.command.Wait()
	}
}

func compareFiles(leftPath, rightPath string) error {
	left, err := os.Open(leftPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", leftPath, err)
	}
	defer func() {
		_ = left.Close()
	}()
	right, err := os.Open(rightPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", rightPath, err)
	}
	defer func() {
		_ = right.Close()
	}()

	leftReader := bufio.NewReader(left)
	rightReader := bufio.NewReader(right)
	var offset int64
	for {
		leftChunk := make([]byte, 64*1024)
		rightChunk := make([]byte, 64*1024)
		leftCount, leftErr := io.ReadFull(leftReader, leftChunk)
		rightCount, rightErr := io.ReadFull(rightReader, rightChunk)
		limit := min(leftCount, rightCount)
		for index := 0; index < limit; index++ {
			if leftChunk[index] != rightChunk[index] {
				return fmt.Errorf("byte mismatch at offset %d", offset+int64(index))
			}
		}
		if leftCount != rightCount {
			return fmt.Errorf("size mismatch at offset %d", offset+int64(limit))
		}
		offset += int64(leftCount)
		leftDone := errors.Is(leftErr, io.EOF) || errors.Is(leftErr, io.ErrUnexpectedEOF)
		rightDone := errors.Is(rightErr, io.EOF) || errors.Is(rightErr, io.ErrUnexpectedEOF)
		if leftDone || rightDone {
			if leftDone && rightDone {
				return nil
			}
			return fmt.Errorf("size mismatch at offset %d", offset)
		}
		if leftErr != nil {
			return fmt.Errorf("read %s: %w", leftPath, leftErr)
		}
		if rightErr != nil {
			return fmt.Errorf("read %s: %w", rightPath, rightErr)
		}
	}
}

func findRepositoryRoot() (string, error) {
	current, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for {
		info, statErr := os.Stat(filepath.Join(current, "go.mod"))
		if statErr == nil && info.Mode().IsRegular() {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("repository root containing go.mod not found")
		}
		current = parent
	}
}

func qualificationCommand(config qualificationConfig) []string {
	command := []string{"go", "run", "./test/replay"}
	if config.InputPath != "" {
		command = append(command, "--input", config.InputPath)
	}
	return append(command, "--output", config.OutputPath)
}

func requireAbsent(path string) error {
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		return fmt.Errorf("output path already exists: %s (%s)", path, info.Mode())
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("inspect output path: %w", err)
	}
}

func writeAtomic(path string, write func(io.Writer) error) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	if err := write(temporary); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporaryPath, 0o644); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
