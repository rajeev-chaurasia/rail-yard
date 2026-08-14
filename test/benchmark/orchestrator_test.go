package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
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
	options.jobs = 50_000
	options.workers = 8
	if !options.qualification() {
		t.Fatal("canonical benchmark shape was not treated as qualification")
	}
	options.jobs--
	if options.qualification() {
		t.Fatal("noncanonical benchmark shape was treated as qualification")
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
