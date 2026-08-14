package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

const (
	benchmarkCommandPath = "./test/benchmark/cmd/railyard-benchmark"
	serverService        = "server"
	redisService         = "redis"
	maxWorkers           = 8
)

var composeProjectPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

type orchestrationOptions struct {
	composeFile       string
	projectPrefix     string
	output            string
	resume            bool
	dockerExecutable  string
	goExecutable      string
	databasePath      string
	runs              int
	jobs              int
	workers           int
	workerSlots       int
	hostPort          int
	submitConcurrency int
	pollConcurrency   int
	seed              int64
	startupTimeout    time.Duration
	drainTimeout      time.Duration
	requestTimeout    time.Duration
	pollInterval      time.Duration
}

type runSpec struct {
	name  string
	phase evidence.RunPhase
	seed  int64
}

type orchestrationCheckpoint struct {
	SchemaVersion       int                          `json:"schema_version"`
	Config              immutableOrchestrationConfig `json:"config"`
	SelectedHostPort    int                          `json:"selected_host_port"`
	ConfigurationSHA256 string                       `json:"configuration_sha256,omitempty"`
}

type immutableOrchestrationConfig struct {
	ComposeFileSHA256 string        `json:"compose_file_sha256"`
	ComposeFile       string        `json:"compose_file"`
	ProjectPrefix     string        `json:"project_prefix"`
	DockerExecutable  string        `json:"docker_executable"`
	GoExecutable      string        `json:"go_executable"`
	DatabasePath      string        `json:"database_path"`
	Runs              int           `json:"runs"`
	Jobs              int           `json:"jobs"`
	Workers           int           `json:"workers"`
	WorkerSlots       int           `json:"worker_slots"`
	RequestedHostPort int           `json:"requested_host_port"`
	SubmitConcurrency int           `json:"submit_concurrency"`
	PollConcurrency   int           `json:"poll_concurrency"`
	Seed              int64         `json:"seed"`
	StartupTimeout    time.Duration `json:"startup_timeout_ns"`
	DrainTimeout      time.Duration `json:"drain_timeout_ns"`
	RequestTimeout    time.Duration `json:"request_timeout_ns"`
	PollInterval      time.Duration `json:"poll_interval_ns"`
}

const orchestrationCheckpointFile = "orchestration-checkpoint.json"

type commandRunner interface {
	Run(context.Context, []string, string, ...string) ([]byte, error)
}

type processRunner struct{}

func (processRunner) Run(
	ctx context.Context,
	environment []string,
	name string,
	arguments ...string,
) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Env = append(os.Environ(), environment...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf(
			"%s %s: %w: %s",
			name,
			strings.Join(arguments, " "),
			err,
			strings.TrimSpace(string(output)),
		)
	}
	return output, nil
}

type composeClient struct {
	runner      commandRunner
	executable  string
	file        string
	project     string
	environment []string
}

func (c composeClient) run(ctx context.Context, arguments ...string) ([]byte, error) {
	base := []string{
		"compose",
		"--file", c.file,
		"--project-name", c.project,
	}
	return c.runner.Run(ctx, c.environment, c.executable, append(base, arguments...)...)
}

func (c composeClient) down(ctx context.Context) error {
	_, err := c.run(ctx, "down", "--volumes", "--remove-orphans")
	return err
}

func (c composeClient) start(ctx context.Context, workers []string) error {
	services := append([]string{redisService, serverService}, workers...)
	arguments := append([]string{"up", "--detach", "--build"}, services...)
	_, err := c.run(ctx, arguments...)
	return err
}

func (c composeClient) stop(ctx context.Context, services ...string) error {
	arguments := append([]string{"stop", "--timeout", "45"}, services...)
	_, err := c.run(ctx, arguments...)
	return err
}

func (c composeClient) runningServices(ctx context.Context) ([]string, error) {
	output, err := c.run(ctx, "ps", "--status", "running", "--services")
	if err != nil {
		return nil, err
	}
	return nonemptyLines(output), nil
}

func (c composeClient) containerID(ctx context.Context, service string) (string, error) {
	output, err := c.run(ctx, "ps", "--all", "--quiet", service)
	if err != nil {
		return "", err
	}
	ids := nonemptyLines(output)
	if len(ids) != 1 {
		return "", fmt.Errorf("service %s has %d containers, want 1", service, len(ids))
	}
	return ids[0], nil
}

func (c composeClient) serviceHealth(ctx context.Context, service string) (string, error) {
	containerID, err := c.containerID(ctx, service)
	if err != nil {
		return "", err
	}
	output, err := c.runner.Run(
		ctx,
		c.environment,
		c.executable,
		"inspect",
		"--format={{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
		containerID,
	)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func (c composeClient) exec(ctx context.Context, service string, arguments ...string) ([]byte, error) {
	command := append([]string{"exec", "--no-TTY", service}, arguments...)
	return c.run(ctx, command...)
}

type orchestrationSummary struct {
	SchemaVersion        int                         `json:"schema_version"`
	Qualification        bool                        `json:"qualification"`
	Valid                bool                        `json:"valid"`
	AggregationAvailable bool                        `json:"aggregation_available"`
	UnavailableReason    string                      `json:"unavailable_reason,omitempty"`
	GeneratedAt          time.Time                   `json:"generated_at"`
	Runs                 []evidence.BenchmarkSummary `json:"runs"`
}

func (o orchestrationOptions) validate() error {
	switch {
	case strings.TrimSpace(o.composeFile) == "":
		return errors.New("compose-file is required")
	case strings.TrimSpace(o.projectPrefix) == "" ||
		!composeProjectPattern.MatchString(o.projectPrefix):
		return fmt.Errorf("project-prefix %q must match %s", o.projectPrefix, composeProjectPattern)
	case strings.TrimSpace(o.output) == "":
		return errors.New("output is required")
	case strings.TrimSpace(o.dockerExecutable) == "":
		return errors.New("docker executable is required")
	case strings.TrimSpace(o.goExecutable) == "":
		return errors.New("go executable is required")
	case strings.TrimSpace(o.databasePath) == "" || !strings.HasPrefix(o.databasePath, "/"):
		return errors.New("database-path must be an absolute container path")
	case o.runs < 1:
		return errors.New("runs must be positive")
	case o.jobs < 2:
		return errors.New("jobs must be at least 2")
	case o.workers < 1 || o.workers > maxWorkers:
		return fmt.Errorf("workers must be between 1 and %d", maxWorkers)
	case o.workerSlots < 1:
		return errors.New("worker-slots must be positive")
	case o.hostPort < 0 || o.hostPort > 65535:
		return errors.New("host-port must be between 0 and 65535")
	case o.submitConcurrency < 1 || o.pollConcurrency < 1:
		return errors.New("submission and polling concurrency must be positive")
	case o.startupTimeout <= 0 || o.drainTimeout <= 0 ||
		o.requestTimeout <= 0 || o.pollInterval <= 0:
		return errors.New("timeouts and poll interval must be positive")
	}
	info, err := os.Stat(o.composeFile)
	if err != nil {
		return fmt.Errorf("inspect compose file: %w", err)
	}
	if info.IsDir() {
		return errors.New("compose-file must not be a directory")
	}
	return nil
}

func (o orchestrationOptions) qualification() bool {
	return o.runs == 3 && o.jobs == 50_000 && o.workers == 8
}

func orchestrate(
	ctx context.Context,
	options orchestrationOptions,
	runner commandRunner,
	stdout io.Writer,
) (orchestrationErr error) {
	if err := options.validate(); err != nil {
		return err
	}
	if runner == nil {
		return errors.New("command runner is nil")
	}
	checkpoint, err := prepareOrchestration(options)
	if err != nil {
		return err
	}
	options.hostPort = checkpoint.SelectedHostPort

	suiteCompose := composeClient{
		runner:     runner,
		executable: options.dockerExecutable,
		file:       options.composeFile,
		project:    options.projectPrefix,
		environment: []string{
			"RAILYARD_HTTP_PORT=" + strconv.Itoa(options.hostPort),
			"RAILYARD_WORKER_SLOTS=" + strconv.Itoa(options.workerSlots),
			"RAILYARD_LEASE_TTL=10s",
		},
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := suiteCompose.down(cleanupContext); err != nil {
			orchestrationErr = errors.Join(orchestrationErr, fmt.Errorf("remove suite Compose resources: %w", err))
		}
	}()

	specifications := runSpecifications(options)
	runDirectories := make([]string, 0, len(specifications))
	for _, specification := range specifications {
		directory := filepath.Join(options.output, "runs", specification.name)
		if options.resume {
			reusable, configurationHash, err := inspectResumableRun(
				options,
				specification,
				directory,
				checkpoint.ConfigurationSHA256,
			)
			if err != nil {
				return err
			}
			if reusable {
				if err := bindConfigurationHash(options.output, &checkpoint, configurationHash); err != nil {
					return err
				}
				runDirectories = append(runDirectories, directory)
				_, _ = fmt.Fprintf(stdout, "resuming after completed %s run\n", specification.name)
				continue
			}
		}
		_, _ = fmt.Fprintf(stdout, "starting %s run\n", specification.name)
		err := runComposeBenchmark(
			ctx,
			options,
			runner,
			specification.name,
			specification.phase,
			specification.seed,
			directory,
		)
		if err != nil {
			_ = evidence.GenerateChecksums(options.output)
			return fmt.Errorf("%s run: %w", specification.name, err)
		}
		_, configurationHash, err := inspectFinalizedRun(
			options,
			specification,
			directory,
			checkpoint.ConfigurationSHA256,
		)
		if err != nil {
			_ = evidence.GenerateChecksums(options.output)
			return fmt.Errorf("%s finalized evidence: %w", specification.name, err)
		}
		if err := bindConfigurationHash(options.output, &checkpoint, configurationHash); err != nil {
			return err
		}
		runDirectories = append(runDirectories, directory)
	}

	if err := os.RemoveAll(filepath.Join(options.output, "suite")); err != nil {
		return fmt.Errorf("remove previous suite summary: %w", err)
	}
	if err := summarizeOrchestration(
		ctx,
		options,
		runner,
		runDirectories,
	); err != nil {
		_ = evidence.GenerateChecksums(options.output)
		return err
	}
	if err := evidence.GenerateChecksums(options.output); err != nil {
		return fmt.Errorf("generate suite checksums: %w", err)
	}
	if err := evidence.VerifyChecksums(options.output); err != nil {
		return fmt.Errorf("verify suite checksums: %w", err)
	}
	_, _ = fmt.Fprintf(stdout, "benchmark artifacts written to %s\n", options.output)
	return nil
}

func prepareOrchestration(options orchestrationOptions) (orchestrationCheckpoint, error) {
	config, err := immutableConfig(options)
	if err != nil {
		return orchestrationCheckpoint{}, err
	}
	checkpointPath := filepath.Join(options.output, orchestrationCheckpointFile)
	if options.resume {
		var checkpoint orchestrationCheckpoint
		if err := evidence.ReadJSON(checkpointPath, &checkpoint); err != nil {
			return checkpoint, fmt.Errorf("read resume checkpoint: %w", err)
		}
		if checkpoint.SchemaVersion != evidence.SchemaVersion {
			return checkpoint, fmt.Errorf("resume checkpoint schema version is %d, want %d",
				checkpoint.SchemaVersion, evidence.SchemaVersion)
		}
		if checkpoint.Config != config {
			return checkpoint, errors.New("resume configuration does not match the checkpoint")
		}
		if checkpoint.SelectedHostPort < 1 || checkpoint.SelectedHostPort > 65535 {
			return checkpoint, errors.New("resume checkpoint has an invalid selected host port")
		}
		return checkpoint, nil
	}

	if err := createNewDirectory(options.output); err != nil {
		return orchestrationCheckpoint{}, err
	}
	hostPort, err := selectHostPort(options.hostPort)
	if err != nil {
		return orchestrationCheckpoint{}, err
	}
	checkpoint := orchestrationCheckpoint{
		SchemaVersion:    evidence.SchemaVersion,
		Config:           config,
		SelectedHostPort: hostPort,
	}
	if err := evidence.WriteJSON(checkpointPath, checkpoint); err != nil {
		return orchestrationCheckpoint{}, fmt.Errorf("write orchestration checkpoint: %w", err)
	}
	return checkpoint, nil
}

func immutableConfig(options orchestrationOptions) (immutableOrchestrationConfig, error) {
	composeFile, err := filepath.Abs(options.composeFile)
	if err != nil {
		return immutableOrchestrationConfig{}, fmt.Errorf("resolve compose file: %w", err)
	}
	composeDigest, err := evidence.FileSHA256(composeFile)
	if err != nil {
		return immutableOrchestrationConfig{}, fmt.Errorf("hash compose file: %w", err)
	}
	return immutableOrchestrationConfig{
		ComposeFileSHA256: composeDigest,
		ComposeFile:       filepath.Clean(composeFile),
		ProjectPrefix:     options.projectPrefix,
		DockerExecutable:  options.dockerExecutable,
		GoExecutable:      options.goExecutable,
		DatabasePath:      options.databasePath,
		Runs:              options.runs,
		Jobs:              options.jobs,
		Workers:           options.workers,
		WorkerSlots:       options.workerSlots,
		RequestedHostPort: options.hostPort,
		SubmitConcurrency: options.submitConcurrency,
		PollConcurrency:   options.pollConcurrency,
		Seed:              options.seed,
		StartupTimeout:    options.startupTimeout,
		DrainTimeout:      options.drainTimeout,
		RequestTimeout:    options.requestTimeout,
		PollInterval:      options.pollInterval,
	}, nil
}

func runSpecifications(options orchestrationOptions) []runSpec {
	specifications := []runSpec{{
		name:  "warmup",
		phase: evidence.PhaseWarmup,
		seed:  options.seed,
	}}
	for index := 1; index <= options.runs; index++ {
		specifications = append(specifications, runSpec{
			name:  fmt.Sprintf("measured-%02d", index),
			phase: evidence.PhaseMeasured,
			seed:  options.seed + int64(index),
		})
	}
	return specifications
}

func inspectResumableRun(
	options orchestrationOptions,
	specification runSpec,
	directory string,
	configurationHash string,
) (bool, string, error) {
	info, err := os.Stat(directory)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := quarantineIncompleteRun(options.output, specification.name); err != nil {
			return false, "", err
		}
		return false, "", nil
	case err != nil:
		return false, "", fmt.Errorf("inspect %s run directory: %w", specification.name, err)
	case !info.IsDir():
		return false, "", fmt.Errorf("%s run path is not a directory", specification.name)
	}

	var manifest evidence.RunManifest
	manifestErr := evidence.ReadJSON(filepath.Join(directory, "manifest.json"), &manifest)
	if manifestErr == nil && manifest.Status == evidence.StatusValid {
		return inspectFinalizedRun(options, specification, directory, configurationHash)
	}
	if manifestErr != nil && evidence.VerifyChecksums(directory) == nil {
		return false, "", fmt.Errorf("%s run has checksummed but unreadable manifest: %w",
			specification.name, manifestErr)
	}
	if err := quarantineIncompleteRun(options.output, specification.name); err != nil {
		return false, "", err
	}
	return false, "", nil
}

func inspectFinalizedRun(
	options orchestrationOptions,
	specification runSpec,
	directory string,
	configurationHash string,
) (bool, string, error) {
	for _, name := range []string{
		"manifest.json",
		"submitted.jsonl",
		"drain-samples.jsonl",
		"reconciliation.json",
		"benchmark-samples.jsonl",
		"benchmark-summary.json",
	} {
		if _, err := evidence.FileSHA256(filepath.Join(directory, name)); err != nil {
			return false, "", fmt.Errorf("%s required artifact %s: %w", specification.name, name, err)
		}
	}
	if err := evidence.VerifyChecksums(directory); err != nil {
		return false, "", fmt.Errorf("%s run checksum verification failed: %w", specification.name, err)
	}
	var manifest evidence.RunManifest
	if err := evidence.ReadJSON(filepath.Join(directory, "manifest.json"), &manifest); err != nil {
		return false, "", fmt.Errorf("read %s manifest: %w", specification.name, err)
	}
	var summary evidence.BenchmarkSummary
	if err := evidence.ReadJSON(filepath.Join(directory, "benchmark-summary.json"), &summary); err != nil {
		return false, "", fmt.Errorf("read %s summary: %w", specification.name, err)
	}
	var reconciliation evidence.ReconciliationReport
	if err := evidence.ReadJSON(filepath.Join(directory, "reconciliation.json"), &reconciliation); err != nil {
		return false, "", fmt.Errorf("read %s reconciliation: %w", specification.name, err)
	}
	samples, err := evidence.ReadJSONLines[evidence.BenchmarkSample](
		filepath.Join(directory, "benchmark-samples.jsonl"),
	)
	if err != nil {
		return false, "", fmt.Errorf("read %s benchmark samples: %w", specification.name, err)
	}

	if manifest.SchemaVersion != evidence.SchemaVersion ||
		manifest.Status != evidence.StatusValid ||
		manifest.FinalizedAt == nil ||
		manifest.WorkloadFinishedAt == nil ||
		!manifest.DatabaseQuiesced ||
		manifest.DatabaseSHA256 == "" ||
		manifest.DatabaseFilesSHA256["database"] != manifest.DatabaseSHA256 {
		return false, "", fmt.Errorf("%s manifest is not finalized and valid", specification.name)
	}
	if manifest.Phase != specification.phase ||
		manifest.Scored != (specification.phase == evidence.PhaseMeasured) {
		return false, "", fmt.Errorf("%s manifest phase does not match the requested run", specification.name)
	}
	if err := validateRunConfig(options, specification, manifest.Config, configurationHash); err != nil {
		return false, "", fmt.Errorf("%s configuration mismatch: %w", specification.name, err)
	}
	if !summary.Valid ||
		summary.SchemaVersion != evidence.SchemaVersion ||
		summary.RunID != manifest.RunID ||
		summary.Phase != manifest.Phase ||
		summary.CanonicalJobCount != options.jobs ||
		len(samples) != options.jobs {
		return false, "", fmt.Errorf("%s benchmark summary is not finalized and valid", specification.name)
	}
	if !reconciliation.Passed ||
		reconciliation.SchemaVersion != evidence.SchemaVersion ||
		reconciliation.RunID != manifest.RunID {
		return false, "", fmt.Errorf("%s reconciliation is not finalized and valid", specification.name)
	}
	sampleJobs := make(map[string]struct{}, len(samples))
	for _, sample := range samples {
		if sample.SchemaVersion != evidence.SchemaVersion ||
			sample.RunID != manifest.RunID ||
			sample.CompletionState != "SUCCEEDED" ||
			sample.TimestampSource != "quiesced_sqlite_snapshot" {
			return false, "", fmt.Errorf("%s benchmark samples have incompatible identity", specification.name)
		}
		if _, duplicate := sampleJobs[sample.JobID]; duplicate {
			return false, "", fmt.Errorf("%s benchmark samples repeat job %s", specification.name, sample.JobID)
		}
		sampleJobs[sample.JobID] = struct{}{}
	}
	return true, manifest.Config.ConfigurationSHA256, nil
}

func validateRunConfig(
	options orchestrationOptions,
	specification runSpec,
	config evidence.RunConfig,
	configurationHash string,
) error {
	expectedServerURL := "http://127.0.0.1:" + strconv.Itoa(options.hostPort)
	switch {
	case config.ServerURL != expectedServerURL:
		return errors.New("server URL differs")
	case config.JobCount != options.jobs:
		return errors.New("job count differs")
	case config.WorkerCount != options.workers:
		return errors.New("worker count differs")
	case config.WorkerSlots != options.workerSlots:
		return errors.New("worker slots differ")
	case config.SubmitConcurrency != options.submitConcurrency:
		return errors.New("submission concurrency differs")
	case config.PollConcurrency != options.pollConcurrency:
		return errors.New("polling concurrency differs")
	case config.SubmissionAttempts != 3:
		return errors.New("submission attempts differ")
	case config.RequestTimeout != options.requestTimeout:
		return errors.New("request timeout differs")
	case config.HealthTimeout != options.startupTimeout:
		return errors.New("health timeout differs")
	case config.DrainTimeout != options.drainTimeout:
		return errors.New("drain timeout differs")
	case config.PollInterval != options.pollInterval:
		return errors.New("poll interval differs")
	case config.TenantID != "benchmark" || config.Queue != "benchmark":
		return errors.New("tenant or queue differs")
	case config.PayloadBytes < 1 || config.PayloadSHA256 == "":
		return errors.New("fixed workload identity is missing")
	case config.ConfigurationSHA256 == "":
		return errors.New("rendered Compose configuration digest is missing")
	case configurationHash != "" && config.ConfigurationSHA256 != configurationHash:
		return errors.New("rendered Compose configuration digest differs")
	case config.Seed != specification.seed:
		return errors.New("seed differs")
	case config.Qualification != options.qualification():
		return errors.New("qualification mode differs")
	}
	return nil
}

func bindConfigurationHash(
	output string,
	checkpoint *orchestrationCheckpoint,
	configurationHash string,
) error {
	if checkpoint.ConfigurationSHA256 != "" {
		if checkpoint.ConfigurationSHA256 != configurationHash {
			return errors.New("rendered Compose configuration changed between runs")
		}
		return nil
	}
	checkpoint.ConfigurationSHA256 = configurationHash
	if err := evidence.WriteJSON(filepath.Join(output, orchestrationCheckpointFile), checkpoint); err != nil {
		return fmt.Errorf("update orchestration checkpoint: %w", err)
	}
	return nil
}

func quarantineIncompleteRun(output, name string) error {
	sources := []struct {
		kind string
		path string
	}{
		{kind: "runs", path: filepath.Join(output, "runs", name)},
		{kind: "captures", path: filepath.Join(output, "captures", name)},
		{kind: "snapshots", path: filepath.Join(output, "snapshots", name)},
	}
	var existing []struct {
		kind string
		path string
	}
	for _, source := range sources {
		_, err := os.Lstat(source.path)
		switch {
		case err == nil:
			existing = append(existing, source)
		case errors.Is(err, os.ErrNotExist):
		default:
			return fmt.Errorf("inspect incomplete %s artifacts: %w", name, err)
		}
	}
	if len(existing) == 0 {
		return nil
	}

	quarantine := filepath.Join(
		filepath.Clean(output)+"-discarded",
		name+"-"+time.Now().UTC().Format("20060102T150405.000000000Z"),
	)
	for _, source := range existing {
		destination := filepath.Join(quarantine, source.kind)
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return fmt.Errorf("create incomplete run quarantine: %w", err)
		}
		if err := os.Rename(source.path, destination); err != nil {
			return fmt.Errorf("quarantine incomplete %s artifacts: %w", name, err)
		}
	}
	return nil
}

func runComposeBenchmark(
	ctx context.Context,
	options orchestrationOptions,
	runner commandRunner,
	name string,
	phase evidence.RunPhase,
	seed int64,
	runDirectory string,
) (runErr error) {
	port, err := selectHostPort(options.hostPort)
	if err != nil {
		return err
	}
	workers := workerServices(options.workers)
	project := options.projectPrefix
	compose := composeClient{
		runner:     runner,
		executable: options.dockerExecutable,
		file:       options.composeFile,
		project:    project,
		environment: []string{
			"RAILYARD_HTTP_PORT=" + strconv.Itoa(port),
			"RAILYARD_WORKER_SLOTS=" + strconv.Itoa(options.workerSlots),
			"RAILYARD_LEASE_TTL=10s",
		},
	}
	captureDirectory := filepath.Join(options.output, "captures", name)
	snapshotDirectory := filepath.Join(options.output, "snapshots", name)
	if err := os.MkdirAll(captureDirectory, 0o755); err != nil {
		return fmt.Errorf("create capture directory: %w", err)
	}

	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := compose.down(cleanupContext); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("remove Compose resources: %w", err))
		}
	}()
	cleanupContext, cancelCleanup := context.WithTimeout(ctx, 2*time.Minute)
	err = compose.down(cleanupContext)
	cancelCleanup()
	if err != nil {
		return fmt.Errorf("remove previous isolated stack: %w", err)
	}
	if err := compose.start(ctx, workers); err != nil {
		return fmt.Errorf("build and start Compose stack: %w", err)
	}

	serverURL := "http://127.0.0.1:" + strconv.Itoa(port)
	if err := waitForStack(ctx, options, compose, serverURL, workers); err != nil {
		_ = captureCompose(ctx, compose, captureDirectory)
		return err
	}
	configuration, err := compose.run(ctx, "config")
	if err != nil {
		return fmt.Errorf("render Compose configuration: %w", err)
	}
	if err := os.WriteFile(filepath.Join(captureDirectory, "compose-config.yaml"), configuration, 0o644); err != nil {
		return fmt.Errorf("write Compose configuration: %w", err)
	}
	configurationSum := sha256.Sum256(configuration)
	configurationHash := hex.EncodeToString(configurationSum[:])

	environment, err := captureEnvironment(ctx, options, runner, compose, workers)
	if err != nil {
		return err
	}
	if options.qualification() {
		if err := requireQualificationEnvironment(environment); err != nil {
			return err
		}
	}
	environmentPath := filepath.Join(captureDirectory, "environment.json")
	if err := evidence.WriteJSON(environmentPath, environment); err != nil {
		return fmt.Errorf("write environment manifest: %w", err)
	}

	runID := time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + name
	arguments := []string{
		"run",
		"--server-url=" + serverURL,
		"--metrics-url=" + serverURL + "/metrics",
		"--output=" + runDirectory,
		"--run-id=" + runID,
		"--phase=" + string(phase),
		"--environment=" + environmentPath,
		"--configuration-sha256=" + configurationHash,
		"--jobs=" + strconv.Itoa(options.jobs),
		"--workers=" + strconv.Itoa(options.workers),
		"--worker-slots=" + strconv.Itoa(options.workerSlots),
		"--submit-concurrency=" + strconv.Itoa(options.submitConcurrency),
		"--poll-concurrency=" + strconv.Itoa(options.pollConcurrency),
		"--seed=" + strconv.FormatInt(seed, 10),
		"--request-timeout=" + options.requestTimeout.String(),
		"--health-timeout=" + options.startupTimeout.String(),
		"--drain-timeout=" + options.drainTimeout.String(),
		"--poll-interval=" + options.pollInterval.String(),
		"--qualification=" + strconv.FormatBool(options.qualification()),
	}
	if _, err := runner.Run(ctx, nil, options.goExecutable,
		append([]string{"run", "-mod=readonly", benchmarkCommandPath}, arguments...)...,
	); err != nil {
		_ = captureCompose(ctx, compose, captureDirectory)
		_ = copyDirectory(captureDirectory, filepath.Join(runDirectory, "orchestration"))
		_ = evidence.GenerateChecksums(runDirectory)
		return fmt.Errorf("execute benchmark workload: %w", err)
	}
	if err := captureCompose(ctx, compose, captureDirectory); err != nil {
		return err
	}
	if err := compose.stop(ctx, workers...); err != nil {
		return fmt.Errorf("stop worker writers: %w", err)
	}
	if err := compose.stop(ctx, serverService); err != nil {
		return fmt.Errorf("stop server writer: %w", err)
	}
	if err := captureStoppedState(ctx, compose, captureDirectory); err != nil {
		return err
	}
	if err := copySQLiteSnapshot(
		ctx,
		options,
		runner,
		compose,
		snapshotDirectory,
	); err != nil {
		return err
	}
	if err := copyDirectory(captureDirectory, filepath.Join(runDirectory, "orchestration")); err != nil {
		return fmt.Errorf("attach orchestration evidence: %w", err)
	}
	if err := evidence.GenerateChecksums(runDirectory); err != nil {
		return fmt.Errorf("checksum preliminary run evidence: %w", err)
	}

	databaseSnapshot := filepath.Join(snapshotDirectory, path.Base(options.databasePath))
	reconcileArguments := []string{
		"run", "-mod=readonly", benchmarkCommandPath,
		"reconcile",
		"--run-dir=" + runDirectory,
		"--db-snapshot=" + databaseSnapshot,
		"--quiesced=true",
	}
	if _, err := runner.Run(ctx, nil, options.goExecutable, reconcileArguments...); err != nil {
		return fmt.Errorf("reconcile benchmark run: %w", err)
	}
	return nil
}

func summarizeOrchestration(
	ctx context.Context,
	options orchestrationOptions,
	runner commandRunner,
	runDirectories []string,
) error {
	suiteDirectory := filepath.Join(options.output, "suite")
	if options.qualification() {
		arguments := []string{
			"run", "-mod=readonly", benchmarkCommandPath,
			"summarize",
			"--output=" + suiteDirectory,
		}
		for _, directory := range runDirectories {
			arguments = append(arguments, "--run-dir="+directory)
		}
		if _, err := runner.Run(ctx, nil, options.goExecutable, arguments...); err != nil {
			return fmt.Errorf("summarize qualification suite: %w", err)
		}
		return nil
	}

	summaries := make([]evidence.BenchmarkSummary, 0, len(runDirectories))
	valid := true
	for _, directory := range runDirectories {
		var summary evidence.BenchmarkSummary
		if err := evidence.ReadJSON(filepath.Join(directory, "benchmark-summary.json"), &summary); err != nil {
			return fmt.Errorf("read run summary %s: %w", directory, err)
		}
		summaries = append(summaries, summary)
		valid = valid && summary.Valid
	}
	if err := createNewDirectory(suiteDirectory); err != nil {
		return err
	}
	summary := orchestrationSummary{
		SchemaVersion:        evidence.SchemaVersion,
		Qualification:        false,
		Valid:                valid,
		AggregationAvailable: false,
		UnavailableReason:    "canonical aggregate statistics require three measured qualification runs",
		GeneratedAt:          time.Now().UTC(),
		Runs:                 summaries,
	}
	if err := evidence.WriteJSON(filepath.Join(suiteDirectory, "orchestration-summary.json"), summary); err != nil {
		return fmt.Errorf("write bounded suite summary: %w", err)
	}
	if err := evidence.GenerateChecksums(suiteDirectory); err != nil {
		return fmt.Errorf("checksum bounded suite summary: %w", err)
	}
	if !valid {
		return errors.New("one or more bounded benchmark runs are invalid")
	}
	return nil
}

func waitForStack(
	ctx context.Context,
	options orchestrationOptions,
	compose composeClient,
	serverURL string,
	workers []string,
) error {
	deadlineContext, cancel := context.WithTimeout(ctx, options.startupTimeout)
	defer cancel()
	client := &http.Client{Timeout: options.requestTimeout}
	expected := append([]string{redisService, serverService}, workers...)
	slices.Sort(expected)

	return poll(deadlineContext, options.pollInterval, func() (bool, error) {
		request, err := http.NewRequestWithContext(
			deadlineContext,
			http.MethodGet,
			serverURL+"/health/ready",
			nil,
		)
		if err != nil {
			return false, err
		}
		response, err := client.Do(request)
		if err != nil {
			return false, nil
		}
		var health struct {
			Status string `json:"status"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&health)
		closeErr := response.Body.Close()
		if response.StatusCode != http.StatusOK || decodeErr != nil || closeErr != nil ||
			health.Status != "ready" {
			return false, nil
		}
		running, err := compose.runningServices(deadlineContext)
		if err != nil {
			return false, nil
		}
		slices.Sort(running)
		if !slices.Equal(running, expected) {
			return false, nil
		}
		for _, service := range expected {
			health, err := compose.serviceHealth(deadlineContext, service)
			if err != nil || health != "healthy" {
				return false, nil
			}
		}
		return true, nil
	}, "server readiness and exact worker topology")
}

func captureCompose(ctx context.Context, compose composeClient, directory string) error {
	captures := []struct {
		name      string
		arguments []string
	}{
		{name: "compose.log", arguments: []string{"logs", "--no-color", "--timestamps"}},
		{name: "compose-ps.json", arguments: []string{"ps", "--all", "--format", "json"}},
		{name: "compose-images.json", arguments: []string{"images", "--format", "json"}},
	}
	for _, capture := range captures {
		output, err := compose.run(ctx, capture.arguments...)
		if err != nil {
			return fmt.Errorf("capture %s: %w", capture.name, err)
		}
		if err := os.WriteFile(filepath.Join(directory, capture.name), output, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", capture.name, err)
		}
	}
	return nil
}

func captureStoppedState(ctx context.Context, compose composeClient, directory string) error {
	output, err := compose.run(ctx, "ps", "--all", "--format", "json")
	if err != nil {
		return fmt.Errorf("capture stopped Compose state: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "compose-ps-stopped.json"), output, 0o644); err != nil {
		return fmt.Errorf("write stopped Compose state: %w", err)
	}
	return nil
}

func copySQLiteSnapshot(
	ctx context.Context,
	options orchestrationOptions,
	runner commandRunner,
	compose composeClient,
	destination string,
) error {
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	containerID, err := compose.containerID(ctx, serverService)
	if err != nil {
		return fmt.Errorf("find stopped server container: %w", err)
	}
	source := containerID + ":" + path.Dir(options.databasePath) + "/."
	if _, err := runner.Run(
		ctx,
		compose.environment,
		options.dockerExecutable,
		"cp",
		source,
		destination,
	); err != nil {
		return fmt.Errorf("copy stopped SQLite database and WAL: %w", err)
	}
	database := filepath.Join(destination, path.Base(options.databasePath))
	info, err := os.Stat(database)
	if err != nil {
		return fmt.Errorf("verify SQLite snapshot: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("SQLite snapshot is not a regular file")
	}
	return nil
}

func captureEnvironment(
	ctx context.Context,
	options orchestrationOptions,
	runner commandRunner,
	compose composeClient,
	workers []string,
) (evidence.EnvironmentManifest, error) {
	environment := evidence.EnvironmentManifest{
		GoVersion:       runtime.Version(),
		OS:              runtime.GOOS,
		Architecture:    runtime.GOARCH,
		CPUCount:        runtime.NumCPU(),
		BinaryDigests:   make(map[string]string),
		ImageDigests:    make(map[string]string),
		SQLitePragmas:   make(map[string]string),
		OperatorDetails: make(map[string]string),
	}
	environment.Hostname, _ = os.Hostname()
	zone, offset := time.Now().Zone()
	environment.Timezone = zone + " UTC" + formatOffset(offset)

	if output, err := runner.Run(ctx, nil, "git", "rev-parse", "HEAD"); err == nil {
		environment.GitCommit = strings.TrimSpace(string(output))
	}
	if output, err := runner.Run(ctx, nil, "git", "status", "--porcelain"); err == nil {
		dirty := strings.TrimSpace(string(output)) != ""
		environment.GitDirty = &dirty
	}
	if output, err := runner.Run(ctx, nil, options.dockerExecutable, "version", "--format",
		"{{.Client.Version}} client, {{.Server.Version}} server"); err == nil {
		environment.DockerVersion = strings.TrimSpace(string(output))
	}
	if output, err := runner.Run(ctx, nil, options.dockerExecutable, "compose", "version", "--short"); err == nil {
		environment.ComposeVersion = strings.TrimSpace(string(output))
	}
	if output, err := runner.Run(ctx, nil, "uname", "-sr"); err == nil {
		environment.Kernel = strings.TrimSpace(string(output))
	}
	environment.CPUModel = linuxCPUModel()
	environment.MemoryBytes = linuxMemoryBytes()
	environment.CgroupLimits = linuxCgroupLimits()

	if output, err := compose.exec(ctx, serverService, "df", "-T", path.Dir(options.databasePath)); err == nil {
		lines := nonemptyLines(output)
		if len(lines) > 1 {
			fields := strings.Fields(lines[len(lines)-1])
			if len(fields) > 1 {
				environment.Filesystem = fields[1]
			}
		}
	}
	for name, servicePath := range map[string]struct {
		service string
		path    string
	}{
		"server": {service: serverService, path: "/usr/local/bin/railyard-server"},
		"worker": {service: workers[0], path: "/usr/local/bin/railyard-worker"},
	} {
		output, err := compose.exec(ctx, servicePath.service, "sha256sum", servicePath.path)
		if err == nil {
			fields := strings.Fields(string(output))
			if len(fields) > 0 {
				environment.BinaryDigests[name] = fields[0]
			}
		}
	}
	services := append([]string{redisService, serverService}, workers...)
	for _, service := range services {
		containerID, err := compose.containerID(ctx, service)
		if err != nil {
			continue
		}
		output, err := runner.Run(ctx, nil, options.dockerExecutable, "inspect", "--format={{.Image}}", containerID)
		if err == nil {
			environment.ImageDigests[service] = strings.TrimSpace(string(output))
		}
	}
	pragmaOutput, err := compose.exec(
		ctx,
		serverService,
		"sqlite3",
		"-readonly",
		"-cmd",
		".timeout 5000",
		options.databasePath,
		"PRAGMA foreign_keys=ON; PRAGMA journal_mode; PRAGMA synchronous; PRAGMA foreign_keys; PRAGMA busy_timeout;",
	)
	if err == nil {
		values := nonemptyLines(pragmaOutput)
		if len(values) == 4 {
			for index, name := range []string{"journal_mode", "synchronous", "foreign_keys", "busy_timeout"} {
				environment.SQLitePragmas[name] = values[index]
			}
		}
	}
	running, err := compose.runningServices(ctx)
	if err == nil {
		slices.Sort(running)
		environment.OperatorDetails["worker_count_evidence"] = strings.Join(running, ",")
	}
	environment.Unavailable = unavailableEnvironment(environment)
	return environment, nil
}

func requireQualificationEnvironment(environment evidence.EnvironmentManifest) error {
	if environment.GitDirty == nil || *environment.GitDirty {
		return errors.New("qualification requires a clean Git worktree")
	}
	for name, value := range map[string]string{
		"git_commit":      environment.GitCommit,
		"docker_version":  environment.DockerVersion,
		"compose_version": environment.ComposeVersion,
		"filesystem":      environment.Filesystem,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("qualification requires %s evidence", name)
		}
	}
	if len(environment.BinaryDigests) < 2 {
		return errors.New("qualification requires server and worker binary digests")
	}
	if len(environment.ImageDigests) < 10 {
		return errors.New("qualification requires image digests for Redis, server, and eight workers")
	}
	if len(environment.SQLitePragmas) != 4 {
		return errors.New("qualification requires all SQLite pragma evidence")
	}
	return nil
}

func unavailableEnvironment(environment evidence.EnvironmentManifest) []string {
	text := map[string]string{
		"git_commit":      environment.GitCommit,
		"docker_version":  environment.DockerVersion,
		"compose_version": environment.ComposeVersion,
		"kernel":          environment.Kernel,
		"cpu_model":       environment.CPUModel,
		"filesystem":      environment.Filesystem,
		"cgroup_limits":   environment.CgroupLimits,
	}
	var unavailable []string
	for name, value := range text {
		if strings.TrimSpace(value) == "" {
			unavailable = append(unavailable, name)
		}
	}
	if environment.GitDirty == nil {
		unavailable = append(unavailable, "git_dirty")
	}
	if environment.MemoryBytes < 1 {
		unavailable = append(unavailable, "memory_bytes")
	}
	if len(environment.BinaryDigests) == 0 {
		unavailable = append(unavailable, "binary_digests")
	}
	if len(environment.ImageDigests) == 0 {
		unavailable = append(unavailable, "image_digests")
	}
	if len(environment.SQLitePragmas) == 0 {
		unavailable = append(unavailable, "sqlite_pragmas")
	}
	slices.Sort(unavailable)
	return unavailable
}

func linuxCPUModel() string {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	defer func() {
		_ = file.Close()
	}()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), ":")
		if found && strings.TrimSpace(key) == "model name" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func linuxMemoryBytes() int64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer func() {
		_ = file.Close()
	}()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kibibytes, err := strconv.ParseInt(fields[1], 10, 64)
			if err == nil {
				return kibibytes * 1024
			}
		}
	}
	return 0
}

func linuxCgroupLimits() string {
	parts := make([]string, 0, 2)
	for _, item := range []struct {
		name string
		path string
	}{
		{name: "cpu.max", path: "/sys/fs/cgroup/cpu.max"},
		{name: "memory.max", path: "/sys/fs/cgroup/memory.max"},
	} {
		body, err := os.ReadFile(item.path)
		if err == nil {
			parts = append(parts, item.name+"="+strings.TrimSpace(string(body)))
		}
	}
	return strings.Join(parts, ",")
}

func selectHostPort(requested int) (int, error) {
	address := "127.0.0.1:0"
	if requested != 0 {
		address = "127.0.0.1:" + strconv.Itoa(requested)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return 0, fmt.Errorf("reserve benchmark host port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, fmt.Errorf("release benchmark host port reservation: %w", err)
	}
	return port, nil
}

func workerServices(count int) []string {
	workers := make([]string, count)
	for index := range count {
		workers[index] = "worker-" + strconv.Itoa(index+1)
	}
	return workers
}

func createNewDirectory(path string) error {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return fmt.Errorf("output directory already exists: %s", path)
	case !errors.Is(err, os.ErrNotExist):
		return err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	return nil
}

func copyDirectory(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("capture is not a regular file: %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() {
			_ = input.Close()
		}()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		return errors.Join(copyErr, closeErr)
	})
}

func poll(
	ctx context.Context,
	interval time.Duration,
	predicate func() (bool, error),
	description string,
) error {
	for {
		matched, err := predicate()
		if err != nil {
			return fmt.Errorf("%s predicate: %w", description, err)
		}
		if matched {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for %s: %w", description, ctx.Err())
		case <-timer.C:
		}
	}
}

func nonemptyLines(output []byte) []string {
	lines := strings.Split(strings.ReplaceAll(string(output), "\r\n", "\n"), "\n")
	result := make([]string, 0, len(lines))
	for _, line := range lines {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	return result
}

func formatOffset(offsetSeconds int) string {
	sign := "+"
	if offsetSeconds < 0 {
		sign = "-"
		offsetSeconds = -offsetSeconds
	}
	return fmt.Sprintf("%s%02d:%02d", sign, offsetSeconds/3600, (offsetSeconds%3600)/60)
}
