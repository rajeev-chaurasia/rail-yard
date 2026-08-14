package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

type recordedCommand struct {
	environment []string
	name        string
	arguments   []string
}

type recordingRunner struct {
	mu       sync.Mutex
	commands []recordedCommand
}

func (r *recordingRunner) Run(
	_ context.Context,
	environment []string,
	name string,
	arguments ...string,
) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commands = append(r.commands, recordedCommand{
		environment: append([]string(nil), environment...),
		name:        name,
		arguments:   append([]string(nil), arguments...),
	})
	return nil, nil
}

func TestComposeStartSelectsExactWorkers(t *testing.T) {
	runner := &recordingRunner{}
	compose := composeClient{
		runner:      runner,
		executable:  "docker",
		file:        "deploy/compose.yaml",
		project:     "benchmark-measured-01",
		environment: []string{"RAILYARD_HTTP_PORT=18081"},
	}
	if err := compose.start(context.Background(), []string{"worker-1", "worker-2"}); err != nil {
		t.Fatalf("start Compose stack: %v", err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("command count=%d want=1", len(runner.commands))
	}
	want := []string{
		"compose",
		"--file", "deploy/compose.yaml",
		"--project-name", "benchmark-measured-01",
		"up", "--detach", "--build",
		"redis", "server", "worker-1", "worker-2",
	}
	if !reflect.DeepEqual(runner.commands[0].arguments, want) {
		t.Fatalf("arguments=%q want=%q", runner.commands[0].arguments, want)
	}
	if !reflect.DeepEqual(runner.commands[0].environment, compose.environment) {
		t.Fatalf("environment=%q want=%q", runner.commands[0].environment, compose.environment)
	}
}

func TestQualificationRequiresCanonicalShape(t *testing.T) {
	options := validOptions(t)
	options.runs = 3
	options.jobs = 5_000
	options.workers = 8
	options.workerSlots = 256
	if !options.qualification() {
		t.Fatal("canonical benchmark shape was not treated as qualification")
	}
	options.jobs--
	if options.qualification() {
		t.Fatal("noncanonical benchmark shape was treated as qualification")
	}
	options.jobs++
	options.workerSlots--
	if options.qualification() {
		t.Fatal("noncanonical worker slots were treated as qualification")
	}
}

func TestOptionsRejectUnavailableWorkerService(t *testing.T) {
	options := validOptions(t)
	options.workers = maxWorkers + 1
	if err := options.validate(); err == nil {
		t.Fatal("worker count beyond the Compose topology passed validation")
	}
}

func TestAutomaticHostPortIsAvailable(t *testing.T) {
	port, err := selectHostPort(0)
	if err != nil {
		t.Fatalf("select host port: %v", err)
	}
	if port < 1 || port > 65535 {
		t.Fatalf("selected invalid port %d", port)
	}
}

func TestRunCleansProjectAfterStartupFailure(t *testing.T) {
	options := validOptions(t)
	options.output = filepath.Join(t.TempDir(), "artifacts")
	options.startupTimeout = 5 * time.Millisecond
	options.pollInterval = time.Millisecond
	runner := &recordingRunner{}

	err := orchestrate(context.Background(), options, runner, io.Discard)
	if err == nil {
		t.Fatal("orchestration unexpectedly succeeded")
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	var downCount int
	for _, command := range runner.commands {
		joined := strings.Join(command.arguments, " ")
		if strings.Contains(joined, " down --volumes --remove-orphans") {
			downCount++
		}
	}
	if downCount < 2 {
		t.Fatalf("Compose down count=%d want at least 2", downCount)
	}
}

func TestResumeReusesCleanFinalizedRun(t *testing.T) {
	options, checkpoint := resumableOptions(t)
	specification := runSpecifications(options)[0]
	writeFinalizedRun(t, options, specification, checkpoint.SelectedHostPort, checkpoint.ConfigurationSHA256)
	options.hostPort = checkpoint.SelectedHostPort

	reusable, configurationHash, err := inspectResumableRun(
		options,
		specification,
		filepath.Join(options.output, "runs", specification.name),
		checkpoint.ConfigurationSHA256,
	)
	if err != nil {
		t.Fatalf("inspect resumable run: %v", err)
	}
	if !reusable {
		t.Fatal("finalized run was not reusable")
	}
	if configurationHash != checkpoint.ConfigurationSHA256 {
		t.Fatalf("configuration hash=%q want=%q", configurationHash, checkpoint.ConfigurationSHA256)
	}
}

func TestResumeRejectsConfigurationMismatch(t *testing.T) {
	options, _ := resumableOptions(t)
	options.jobs++

	if _, err := prepareOrchestration(options); err == nil {
		t.Fatal("resume accepted changed job count")
	}
}

func TestResumeRejectsCorruptedChecksums(t *testing.T) {
	options, checkpoint := resumableOptions(t)
	specification := runSpecifications(options)[0]
	runDirectory := writeFinalizedRun(
		t,
		options,
		specification,
		checkpoint.SelectedHostPort,
		checkpoint.ConfigurationSHA256,
	)
	options.hostPort = checkpoint.SelectedHostPort
	if err := os.WriteFile(
		filepath.Join(runDirectory, "benchmark-summary.json"),
		[]byte("{}\n"),
		0o644,
	); err != nil {
		t.Fatalf("corrupt summary: %v", err)
	}

	_, _, err := inspectResumableRun(
		options,
		specification,
		runDirectory,
		checkpoint.ConfigurationSHA256,
	)
	if err == nil || !strings.Contains(err.Error(), "checksum verification failed") {
		t.Fatalf("corrupted run error=%v", err)
	}
}

func TestResumeQuarantinesIncompleteRun(t *testing.T) {
	options, checkpoint := resumableOptions(t)
	specification := runSpecifications(options)[0]
	options.hostPort = checkpoint.SelectedHostPort
	runDirectory := filepath.Join(options.output, "runs", specification.name)
	if err := os.MkdirAll(runDirectory, 0o755); err != nil {
		t.Fatalf("create incomplete run: %v", err)
	}
	manifest := evidence.RunManifest{
		SchemaVersion: evidence.SchemaVersion,
		Status:        evidence.StatusRunning,
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		t.Fatalf("write incomplete manifest: %v", err)
	}
	for _, directory := range []string{
		filepath.Join(options.output, "captures", specification.name),
		filepath.Join(options.output, "snapshots", specification.name),
	} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatalf("create incomplete companion artifacts: %v", err)
		}
	}

	reusable, _, err := inspectResumableRun(
		options,
		specification,
		runDirectory,
		checkpoint.ConfigurationSHA256,
	)
	if err != nil {
		t.Fatalf("inspect incomplete run: %v", err)
	}
	if reusable {
		t.Fatal("incomplete run was reusable")
	}
	if _, err := os.Stat(runDirectory); !os.IsNotExist(err) {
		t.Fatalf("incomplete run still exists: %v", err)
	}
	discarded, err := os.ReadDir(options.output + "-discarded")
	if err != nil {
		t.Fatalf("read discarded runs: %v", err)
	}
	if len(discarded) != 1 {
		t.Fatalf("discarded run count=%d want=1", len(discarded))
	}
}

func TestResumeRegeneratesAlreadyCompleteSuite(t *testing.T) {
	options, checkpoint := resumableOptions(t)
	for _, specification := range runSpecifications(options) {
		writeFinalizedRun(
			t,
			options,
			specification,
			checkpoint.SelectedHostPort,
			checkpoint.ConfigurationSHA256,
		)
	}
	suiteDirectory := filepath.Join(options.output, "suite")
	if err := os.MkdirAll(suiteDirectory, 0o755); err != nil {
		t.Fatalf("create old suite: %v", err)
	}
	oldMarker := filepath.Join(suiteDirectory, "old-summary")
	if err := os.WriteFile(oldMarker, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write old suite marker: %v", err)
	}
	runner := &recordingRunner{}
	var output strings.Builder

	if err := orchestrate(context.Background(), options, runner, &output); err != nil {
		t.Fatalf("resume complete suite: %v", err)
	}
	if _, err := os.Stat(oldMarker); !os.IsNotExist(err) {
		t.Fatalf("old suite marker still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(suiteDirectory, "orchestration-summary.json")); err != nil {
		t.Fatalf("regenerated suite summary: %v", err)
	}
	if strings.Count(output.String(), "resuming after completed") != len(runSpecifications(options)) {
		t.Fatalf("resume output=%q", output.String())
	}
	for _, command := range runner.commands {
		if strings.Contains(strings.Join(command.arguments, " "), " up ") {
			t.Fatalf("already complete suite started Compose: %#v", command)
		}
	}
}

func resumableOptions(t *testing.T) (orchestrationOptions, orchestrationCheckpoint) {
	t.Helper()
	options := validOptions(t)
	options.output = filepath.Join(t.TempDir(), "artifacts")
	checkpoint, err := prepareOrchestration(options)
	if err != nil {
		t.Fatalf("prepare initial orchestration: %v", err)
	}
	checkpoint.ConfigurationSHA256 = strings.Repeat("a", 64)
	if err := evidence.WriteJSON(
		filepath.Join(options.output, orchestrationCheckpointFile),
		checkpoint,
	); err != nil {
		t.Fatalf("write resume checkpoint: %v", err)
	}
	options.resume = true
	return options, checkpoint
}

func writeFinalizedRun(
	t *testing.T,
	options orchestrationOptions,
	specification runSpec,
	hostPort int,
	configurationHash string,
) string {
	t.Helper()
	runDirectory := filepath.Join(options.output, "runs", specification.name)
	if err := os.MkdirAll(runDirectory, 0o755); err != nil {
		t.Fatalf("create finalized run: %v", err)
	}
	now := time.Now().UTC()
	manifest := evidence.RunManifest{
		SchemaVersion:      evidence.SchemaVersion,
		RunID:              specification.name + "-run",
		Phase:              specification.phase,
		Scored:             specification.phase == evidence.PhaseMeasured,
		Status:             evidence.StatusValid,
		StartedAt:          now.Add(-time.Minute),
		WorkloadFinishedAt: &now,
		FinalizedAt:        &now,
		Config: evidence.RunConfig{
			ServerURL:           "http://127.0.0.1:" + strconv.Itoa(hostPort),
			JobCount:            options.jobs,
			WorkerCount:         options.workers,
			WorkerSlots:         options.workerSlots,
			SubmitConcurrency:   options.submitConcurrency,
			PollConcurrency:     options.pollConcurrency,
			SubmissionAttempts:  3,
			RequestTimeout:      options.requestTimeout,
			HealthTimeout:       options.startupTimeout,
			DrainTimeout:        options.drainTimeout,
			PollInterval:        options.pollInterval,
			TenantID:            "benchmark",
			Queue:               "benchmark",
			PayloadBytes:        1,
			PayloadSHA256:       "payload",
			ConfigurationSHA256: configurationHash,
			Seed:                specification.seed,
			Qualification:       options.qualification(),
		},
		DatabaseSHA256: "database",
		DatabaseFilesSHA256: map[string]string{
			"database": "database",
		},
		DatabaseQuiesced: true,
	}
	summary := evidence.BenchmarkSummary{
		SchemaVersion:     evidence.SchemaVersion,
		RunID:             manifest.RunID,
		Phase:             manifest.Phase,
		Valid:             true,
		CanonicalJobCount: options.jobs,
	}
	reconciliation := evidence.ReconciliationReport{
		SchemaVersion: evidence.SchemaVersion,
		RunID:         manifest.RunID,
		Passed:        true,
	}
	samples := make([]evidence.BenchmarkSample, options.jobs)
	for index := range samples {
		samples[index] = evidence.BenchmarkSample{
			SchemaVersion:   evidence.SchemaVersion,
			RunID:           manifest.RunID,
			JobID:           strconv.Itoa(index),
			CompletionState: "SUCCEEDED",
			TimestampSource: "quiesced_sqlite_snapshot",
		}
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		t.Fatalf("write finalized manifest: %v", err)
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "benchmark-summary.json"), summary); err != nil {
		t.Fatalf("write finalized summary: %v", err)
	}
	if err := evidence.WriteJSON(filepath.Join(runDirectory, "reconciliation.json"), reconciliation); err != nil {
		t.Fatalf("write reconciliation: %v", err)
	}
	if err := evidence.WriteJSONLines(filepath.Join(runDirectory, "benchmark-samples.jsonl"), samples); err != nil {
		t.Fatalf("write benchmark samples: %v", err)
	}
	if err := evidence.WriteJSONLines(
		filepath.Join(runDirectory, "submitted.jsonl"),
		[]evidence.SubmissionSample{},
	); err != nil {
		t.Fatalf("write submissions: %v", err)
	}
	if err := evidence.WriteJSONLines(
		filepath.Join(runDirectory, "drain-samples.jsonl"),
		[]evidence.DrainSample{},
	); err != nil {
		t.Fatalf("write drain samples: %v", err)
	}
	if err := evidence.GenerateChecksums(runDirectory); err != nil {
		t.Fatalf("write run checksums: %v", err)
	}
	return runDirectory
}

func validOptions(t *testing.T) orchestrationOptions {
	t.Helper()
	composeFile := filepath.Join(t.TempDir(), "compose.yaml")
	if err := os.WriteFile(composeFile, []byte("services: {}\n"), 0o644); err != nil {
		t.Fatalf("write Compose fixture: %v", err)
	}
	return orchestrationOptions{
		composeFile:       composeFile,
		projectPrefix:     "rail-yard-benchmark",
		output:            filepath.Join(t.TempDir(), "artifacts"),
		dockerExecutable:  "docker",
		goExecutable:      "go",
		databasePath:      "/var/lib/railyard/railyard.db",
		runs:              1,
		jobs:              100,
		workers:           1,
		workerSlots:       4,
		submitConcurrency: 4,
		pollConcurrency:   8,
		startupTimeout:    time.Second,
		drainTimeout:      time.Second,
		requestTimeout:    time.Millisecond,
		pollInterval:      time.Millisecond,
	}
}
