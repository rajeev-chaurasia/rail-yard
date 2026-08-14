package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

const (
	minimumWorkerKills         = 1
	defaultRuns                = 1
	defaultJobs                = 5_000
	defaultWorkerKills         = 20
	databaseService            = "server"
	maxClockMappingUncertainty = 250 * time.Millisecond
)

var composeNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

type config struct {
	ComposeFile       string
	ProjectPrefix     string
	OutputDirectory   string
	ServerURL         string
	DatabasePath      string
	DockerExecutable  string
	Runs              int
	Jobs              int
	BaseSeed          int64
	SubmitConcurrency int
	WorkerKills       int
	Workers           []string
	JobDuration       time.Duration
	ActionMinimum     time.Duration
	ActionMaximum     time.Duration
	PollInterval      time.Duration
	StartupTimeout    time.Duration
	DrainTimeout      time.Duration
	RequestTimeout    time.Duration
	MaxRecovery       time.Duration
	KeepStack         bool
	Resume            bool
}

type campaignSummary struct {
	Version          int          `json:"version"`
	StartedAt        time.Time    `json:"started_at"`
	CompletedAt      time.Time    `json:"completed_at"`
	Passed           bool         `json:"passed"`
	RecoverySamples  int          `json:"recovery_samples"`
	RecoveryP99MS    float64      `json:"recovery_p99_ms"`
	RecoveryTargetMS float64      `json:"recovery_target_ms"`
	Runs             []runSummary `json:"runs"`
}

type runSummary struct {
	Run                int       `json:"run"`
	Seed               int64     `json:"seed"`
	Project            string    `json:"project"`
	TenantID           string    `json:"tenant_id"`
	StartedAt          time.Time `json:"started_at"`
	CompletedAt        time.Time `json:"completed_at"`
	Accepted           int       `json:"accepted"`
	WorkerKills        int       `json:"worker_kills"`
	ServerKills        int       `json:"server_kills"`
	RecoverySamples    int       `json:"recovery_samples"`
	RecoveryP99MS      float64   `json:"recovery_p99_ms"`
	RecoveryTargetMS   float64   `json:"recovery_target_ms"`
	ReconciliationPass bool      `json:"reconciliation_pass"`
	ArtifactDirectory  string    `json:"artifact_directory"`
	recoveryValues     []float64
}

type runManifest struct {
	Version              int           `json:"version"`
	Run                  int           `json:"run"`
	Seed                 int64         `json:"seed"`
	Project              string        `json:"project"`
	TenantID             string        `json:"tenant_id"`
	Queue                string        `json:"queue"`
	Jobs                 int           `json:"jobs"`
	Workers              []string      `json:"workers"`
	WorkerKills          int           `json:"worker_kills"`
	SubmitConcurrency    int           `json:"submit_concurrency"`
	JobDuration          time.Duration `json:"job_duration"`
	ServerKillTarget     float64       `json:"server_kill_target"`
	ServerKillTargetJobs int           `json:"server_kill_target_jobs"`
	MaxRecovery          time.Duration `json:"max_recovery"`
	ComposeFile          string        `json:"compose_file"`
	ConfigurationHash    string        `json:"configuration_hash"`
	StartedAt            time.Time     `json:"started_at"`
	CompletedAt          time.Time     `json:"completed_at,omitempty"`
}

type progress struct {
	Total      int
	Succeeded  int
	Failed     int
	DeadLetter int
	Active     int
}

type activeLease struct {
	JobID      string
	AttemptNo  int
	Generation int64
	LeasedAt   time.Time
}

type workerKill struct {
	Sequence        int
	Worker          string
	ContainerID     string
	ConfirmedHostAt time.Time
	ConfirmedAt     time.Time
	ClockMapping    clockMapping
	Leases          []activeLease
	RecoveredAt     map[string]time.Time
}

type clockMapping struct {
	HostLowerBound time.Time     `json:"host_lower_bound"`
	HostUpperBound time.Time     `json:"host_upper_bound"`
	ServerTime     time.Time     `json:"server_time"`
	Offset         time.Duration `json:"offset"`
	Uncertainty    time.Duration `json:"uncertainty"`
}

func (m clockMapping) serverTime(hostTime time.Time) time.Time {
	return hostTime.Add(m.Offset).UTC()
}

type clockCalibrator struct {
	mu      sync.RWMutex
	mapping clockMapping
}

func (c *clockCalibrator) record(mapping clockMapping) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.mapping.ServerTime.IsZero() && c.mapping.Uncertainty <= mapping.Uncertainty {
		return
	}
	c.mapping = mapping
}

func (c *clockCalibrator) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapping = clockMapping{}
}

func (c *clockCalibrator) current() (clockMapping, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mapping, !c.mapping.ServerTime.IsZero()
}

type recoverySample struct {
	KillSequence        int          `json:"kill_sequence"`
	Worker              string       `json:"worker"`
	VictimContainerID   string       `json:"victim_container_id"`
	JobID               string       `json:"job_id"`
	KilledAttempt       int          `json:"killed_attempt"`
	KilledGeneration    int64        `json:"killed_generation"`
	KillConfirmedHostAt time.Time    `json:"kill_confirmed_host_at"`
	KillConfirmedAt     time.Time    `json:"kill_confirmed_at"`
	ClockMapping        clockMapping `json:"clock_mapping"`
	SuccessorAttempt    int          `json:"successor_attempt"`
	SuccessorGeneration int64        `json:"successor_generation"`
	SuccessorLeasedAt   time.Time    `json:"successor_leased_at"`
	SuccessorObservedAt time.Time    `json:"successor_observed_at"`
	CompletionAt        time.Time    `json:"completion_at"`
	RecoveryMS          float64      `json:"recovery_ms"`
}

type databaseAttempt struct {
	JobID        string
	AttemptNo    int
	Generation   int64
	LeasedAt     time.Time
	CompletionAt time.Time
}

type leaseBoundaryState struct {
	JobID       string
	AttemptNo   int
	Generation  int64
	State       string
	CompletedAt time.Time
}

type actionEvent struct {
	Sequence   int            `json:"sequence"`
	Type       string         `json:"type"`
	PlannedAt  time.Time      `json:"planned_at,omitempty"`
	ObservedAt time.Time      `json:"observed_at"`
	Service    string         `json:"service,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type processRunner struct{}

func (processRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w: %s",
			name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

type composeClient struct {
	runner     commandRunner
	executable string
	file       string
	project    string
}

func (c composeClient) run(ctx context.Context, args ...string) ([]byte, error) {
	base := []string{
		"compose",
		"--file", c.file,
		"--project-name", c.project,
	}
	return c.runner.Run(ctx, c.executable, append(base, args...)...)
}

func (c composeClient) start(ctx context.Context, services []string) error {
	args := []string{"up", "--detach", "--build"}
	args = append(args, services...)
	_, err := c.run(ctx, args...)
	return err
}

func (c composeClient) stop(ctx context.Context, services []string) error {
	args := append([]string{"stop", "--timeout", "30"}, services...)
	_, err := c.run(ctx, args...)
	return err
}

func (c composeClient) down(ctx context.Context) error {
	_, err := c.run(ctx, "down", "--volumes", "--remove-orphans")
	return err
}

func (c composeClient) kill(ctx context.Context, service string) error {
	_, err := c.run(ctx, "kill", "--signal", "SIGKILL", service)
	return err
}

func (c composeClient) ensureStarted(ctx context.Context, service string) error {
	_, err := c.run(ctx, "up", "--detach", "--no-deps", service)
	return err
}

func (c composeClient) restartWorkers(ctx context.Context, workers []string) error {
	args := append([]string{"restart", "--no-deps"}, workers...)
	_, err := c.run(ctx, args...)
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

func (c composeClient) querySQLite(ctx context.Context, databasePath, query string) ([]byte, error) {
	return c.run(
		ctx,
		"exec", "--no-TTY",
		databaseService,
		"sqlite3", "-readonly", "-cmd", ".timeout 5000",
		databasePath,
		query,
	)
}

type jsonLines struct {
	mu       sync.Mutex
	file     *os.File
	buffer   *bufio.Writer
	sequence int
}

func createJSONLines(path string) (*jsonLines, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &jsonLines{file: file, buffer: bufio.NewWriterSize(file, 64*1024)}, nil
}

func (w *jsonLines) write(value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("JSON Lines writer is closed")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := w.buffer.Write(body); err != nil {
		return err
	}
	if err := w.buffer.WriteByte('\n'); err != nil {
		return err
	}
	return w.buffer.Flush()
}

func (w *jsonLines) nextSequence() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sequence++
	return w.sequence
}

func (w *jsonLines) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	flushErr := w.buffer.Flush()
	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	w.file = nil
	return errors.Join(flushErr, syncErr, closeErr)
}

type receiptRecorder struct {
	mu       sync.Mutex
	writer   *jsonLines
	accepted *atomic.Int64
	next     int
	ids      map[string]struct{}
}

func (r *receiptRecorder) record(record reconcile.AcceptedRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, duplicate := r.ids[record.JobID]; duplicate {
		return fmt.Errorf("server returned duplicate job ID %s for distinct submissions", record.JobID)
	}
	r.next++
	record.Sequence = r.next
	if err := r.writer.write(record); err != nil {
		return fmt.Errorf("write accepted receipt: %w", err)
	}
	r.ids[record.JobID] = struct{}{}
	r.accepted.Store(int64(r.next))
	return nil
}

type submitResponse struct {
	Job struct {
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_at"`
	} `json:"job"`
	Duplicate bool `json:"duplicate"`
	mapping   clockMapping
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type submissionClient struct {
	baseURL      string
	client       *http.Client
	pollInterval time.Duration
}

func (c submissionClient) submit(
	ctx context.Context,
	index int,
	key, tenant, queue string,
	duration time.Duration,
) (submitResponse, error) {
	requestBody := struct {
		Job struct {
			Name     string `json:"name"`
			TenantID string `json:"tenant_id"`
			Queue    string `json:"queue"`
			Priority int    `json:"priority"`
			SlotCost int    `json:"slot_cost"`
			Payload  struct {
				Type       string `json:"type"`
				DurationMS int64  `json:"duration_ms"`
			} `json:"payload"`
			Retry struct {
				MaxAttempts int  `json:"max_attempts"`
				Retryable   bool `json:"retryable"`
			} `json:"retry"`
		} `json:"job"`
	}{}
	requestBody.Job.Name = fmt.Sprintf("chaos-%08d", index)
	requestBody.Job.TenantID = tenant
	requestBody.Job.Queue = queue
	requestBody.Job.SlotCost = 1
	requestBody.Job.Payload.Type = "noop"
	requestBody.Job.Payload.DurationMS = duration.Milliseconds()
	requestBody.Job.Retry.MaxAttempts = 100
	requestBody.Job.Retry.Retryable = true
	body, err := json.Marshal(requestBody)
	if err != nil {
		return submitResponse{}, fmt.Errorf("encode submission %d: %w", index, err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return submitResponse{}, err
		}
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			c.baseURL+"/v1/jobs",
			bytes.NewReader(body),
		)
		if err != nil {
			return submitResponse{}, fmt.Errorf("build submission %d: %w", index, err)
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", key)
		request.Header.Set("X-Rail-Yard-Actor", "chaos-controller")
		hostBefore := time.Now().UTC()
		response, err := c.client.Do(request)
		hostAfter := time.Now().UTC()
		if err != nil {
			if waitErr := wait(ctx, c.pollInterval); waitErr != nil {
				return submitResponse{}, waitErr
			}
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || len(responseBody) > 1024*1024 {
			if waitErr := wait(ctx, c.pollInterval); waitErr != nil {
				return submitResponse{}, waitErr
			}
			continue
		}
		if response.StatusCode == http.StatusCreated || response.StatusCode == http.StatusOK {
			var result submitResponse
			if err := json.Unmarshal(responseBody, &result); err != nil {
				return submitResponse{}, fmt.Errorf("decode submission %d response: %w", index, err)
			}
			if result.Job.ID == "" {
				return submitResponse{}, fmt.Errorf("submission %d response has empty job ID", index)
			}
			if result.Job.CreatedAt.IsZero() {
				return submitResponse{}, fmt.Errorf("submission %d response has no durable creation time", index)
			}
			if !result.Duplicate {
				result.mapping, err = newClockMapping(result.Job.CreatedAt, hostBefore, hostAfter)
				if err != nil {
					return submitResponse{}, fmt.Errorf("submission %d clock mapping: %w", index, err)
				}
			}
			return result, nil
		}
		var payload apiError
		_ = json.Unmarshal(responseBody, &payload)
		if response.StatusCode == http.StatusTooManyRequests ||
			response.StatusCode == http.StatusRequestTimeout ||
			response.StatusCode >= http.StatusInternalServerError {
			if waitErr := wait(ctx, c.pollInterval); waitErr != nil {
				return submitResponse{}, waitErr
			}
			continue
		}
		return submitResponse{}, fmt.Errorf(
			"submission %d returned HTTP %d (%s): %s",
			index, response.StatusCode, payload.Code, payload.Message,
		)
	}
}

func runCampaign(ctx context.Context, cfg config, runner commandRunner) (campaignSummary, error) {
	return runCampaignWithExecutor(ctx, cfg, runner, runOnce)
}

type runExecutor func(context.Context, config, commandRunner, int, int64) (runSummary, error)

func runCampaignWithExecutor(
	ctx context.Context,
	cfg config,
	runner commandRunner,
	execute runExecutor,
) (campaignSummary, error) {
	if err := cfg.validate(); err != nil {
		return campaignSummary{}, err
	}
	if runner == nil {
		return campaignSummary{}, errors.New("command runner is nil")
	}
	if execute == nil {
		return campaignSummary{}, errors.New("run executor is nil")
	}
	if err := os.MkdirAll(cfg.OutputDirectory, 0o755); err != nil {
		return campaignSummary{}, fmt.Errorf("create output directory: %w", err)
	}
	completed := make(map[int]runSummary)
	if cfg.Resume {
		var err error
		completed, err = loadResumedRuns(cfg)
		if err != nil {
			return campaignSummary{}, err
		}
	}
	summary := campaignSummary{
		Version:          1,
		StartedAt:        time.Now().UTC(),
		Passed:           true,
		RecoveryTargetMS: float64(cfg.MaxRecovery) / float64(time.Millisecond),
		Runs:             make([]runSummary, 0, cfg.Runs),
	}
	var recoveryValues []float64
	for run := 1; run <= cfg.Runs; run++ {
		runResult, found := completed[run]
		var err error
		if !found {
			runResult, err = execute(ctx, cfg, runner, run, cfg.BaseSeed+int64(run-1))
		}
		summary.Runs = append(summary.Runs, runResult)
		recoveryValues = append(recoveryValues, runResult.recoveryValues...)
		if err != nil {
			summary.Passed = false
			summary.CompletedAt = time.Now().UTC()
			_ = writeJSONAtomic(filepath.Join(cfg.OutputDirectory, "summary.json"), summary)
			return summary, fmt.Errorf("chaos run %d: %w", run, err)
		}
	}
	summary.RecoverySamples = len(recoveryValues)
	summary.RecoveryP99MS = nearestRank(recoveryValues, 0.99)
	if summary.RecoveryP99MS >= summary.RecoveryTargetMS {
		summary.Passed = false
		summary.CompletedAt = time.Now().UTC()
		_ = writeJSONAtomic(filepath.Join(cfg.OutputDirectory, "summary.json"), summary)
		return summary, fmt.Errorf(
			"campaign worker recovery p99 %.3fms did not meet strict target below %.3fms",
			summary.RecoveryP99MS,
			summary.RecoveryTargetMS,
		)
	}
	summary.CompletedAt = time.Now().UTC()
	if err := writeJSONAtomic(filepath.Join(cfg.OutputDirectory, "summary.json"), summary); err != nil {
		return summary, err
	}
	return summary, nil
}

func runOnce(
	ctx context.Context,
	cfg config,
	runner commandRunner,
	run int,
	seed int64,
) (result runSummary, runErr error) {
	project := projectName(cfg.ProjectPrefix, run, seed)
	tenant := fmt.Sprintf("chaos-r%02d-s%s", run, seedToken(seed))
	queue := "noop"
	runDirectory := filepath.Join(
		cfg.OutputDirectory,
		fmt.Sprintf("%s-r%02d-s%s", time.Now().UTC().Format("20060102T150405.000000000Z"), run, seedToken(seed)),
	)
	if err := os.MkdirAll(runDirectory, 0o755); err != nil {
		return result, fmt.Errorf("create run directory: %w", err)
	}
	result = runSummary{
		Run:               run,
		Seed:              seed,
		Project:           project,
		TenantID:          tenant,
		StartedAt:         time.Now().UTC(),
		ArtifactDirectory: runDirectory,
	}
	compose := composeClient{
		runner:     runner,
		executable: cfg.DockerExecutable,
		file:       cfg.ComposeFile,
		project:    project,
	}
	stackStarted := false
	defer func() {
		result.CompletedAt = time.Now().UTC()
		if stackStarted && !cfg.KeepStack {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := compose.down(cleanupCtx); err != nil && runErr == nil {
				runErr = fmt.Errorf("remove run stack: %w", err)
			}
		}
	}()

	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, 2*time.Minute)
	err := compose.down(cleanupCtx)
	cancelCleanup()
	if err != nil {
		return result, fmt.Errorf("remove previous isolated stack: %w", err)
	}
	services := append([]string{databaseService}, cfg.Workers...)
	if err := compose.start(ctx, services); err != nil {
		return result, fmt.Errorf("start Compose topology: %w", err)
	}
	stackStarted = true
	if err := waitForReady(ctx, cfg, compose); err != nil {
		return result, err
	}
	for _, worker := range cfg.Workers {
		if err := waitForService(ctx, cfg, compose, worker); err != nil {
			return result, err
		}
	}

	random := rand.New(rand.NewSource(seed))
	targetFraction := 0.25 + random.Float64()*0.40
	targetJobs := max(1, int(math.Ceil(targetFraction*float64(cfg.Jobs))))
	configurationHash, err := cfg.configurationHash()
	if err != nil {
		return result, err
	}
	manifest := runManifest{
		Version:              3,
		Run:                  run,
		Seed:                 seed,
		Project:              project,
		TenantID:             tenant,
		Queue:                queue,
		Jobs:                 cfg.Jobs,
		Workers:              append([]string(nil), cfg.Workers...),
		WorkerKills:          cfg.WorkerKills,
		SubmitConcurrency:    cfg.SubmitConcurrency,
		JobDuration:          cfg.JobDuration,
		ServerKillTarget:     targetFraction,
		ServerKillTargetJobs: targetJobs,
		MaxRecovery:          cfg.MaxRecovery,
		ComposeFile:          cfg.ComposeFile,
		ConfigurationHash:    configurationHash,
		StartedAt:            result.StartedAt,
	}
	if err := writeJSONAtomic(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		return result, err
	}

	receipts, err := createJSONLines(filepath.Join(runDirectory, "submitted.jsonl"))
	if err != nil {
		return result, fmt.Errorf("create submitted manifest: %w", err)
	}
	events, err := createJSONLines(filepath.Join(runDirectory, "events.jsonl"))
	if err != nil {
		_ = receipts.close()
		return result, fmt.Errorf("create event trace: %w", err)
	}
	eventsClosed := false
	defer func() {
		if eventsClosed {
			return
		}
		if err := events.close(); err != nil && runErr == nil {
			runErr = fmt.Errorf("close event trace: %w", err)
		}
	}()
	var acceptedCount atomic.Int64
	calibrator := &clockCalibrator{}
	recorder := &receiptRecorder{
		writer:   receipts,
		accepted: &acceptedCount,
		ids:      make(map[string]struct{}, cfg.Jobs),
	}

	runContext, cancelRun := context.WithCancel(ctx)
	submissionResults := make(chan error, 1)
	actionResults := make(chan actionResult, 1)
	serverRestarted := make(chan struct{})
	go func() {
		submissionResults <- submitAll(
			runContext,
			cfg,
			run,
			seed,
			tenant,
			queue,
			targetJobs,
			serverRestarted,
			recorder,
			calibrator,
		)
	}()
	go func() {
		actionResults <- runActions(
			runContext,
			cfg,
			compose,
			random,
			tenant,
			targetJobs,
			&acceptedCount,
			events,
			serverRestarted,
			calibrator,
		)
	}()

	var (
		submissionErr  error
		actionOutcome  actionResult
		submissionDone bool
		actionsDone    bool
	)
	for !submissionDone || !actionsDone {
		select {
		case submissionErr = <-submissionResults:
			submissionDone = true
			if submissionErr != nil {
				cancelRun()
			}
		case actionOutcome = <-actionResults:
			actionsDone = true
			if actionOutcome.err != nil {
				cancelRun()
			}
		case <-ctx.Done():
			cancelRun()
		}
	}
	cancelRun()
	closeErr := receipts.close()
	if submissionErr != nil {
		return result, fmt.Errorf("submit jobs: %w", submissionErr)
	}
	if actionOutcome.err != nil {
		return result, fmt.Errorf("execute chaos actions: %w", actionOutcome.err)
	}
	if closeErr != nil {
		return result, fmt.Errorf("close run traces: %w", closeErr)
	}
	result.Accepted = int(acceptedCount.Load())
	result.WorkerKills = actionOutcome.workerKills
	result.ServerKills = actionOutcome.serverKills
	if result.Accepted != cfg.Jobs {
		return result, fmt.Errorf("accepted jobs=%d want=%d", result.Accepted, cfg.Jobs)
	}

	if err := writeEvent(events, actionEvent{
		Type:       "drain_started",
		ObservedAt: time.Now().UTC(),
		Details:    map[string]any{"accepted_jobs": result.Accepted},
	}); err != nil {
		return result, err
	}
	drained, err := waitForDrain(ctx, cfg, compose, tenant)
	if err != nil {
		return result, err
	}
	if err := writeEvent(events, actionEvent{
		Type:       "drain_completed",
		ObservedAt: time.Now().UTC(),
		Details: map[string]any{
			"total":       drained.Total,
			"succeeded":   drained.Succeeded,
			"failed":      drained.Failed,
			"dead_letter": drained.DeadLetter,
			"active":      drained.Active,
		},
	}); err != nil {
		return result, err
	}
	samples, err := buildRecoverySamples(ctx, compose, cfg.DatabasePath, actionOutcome.kills)
	if err != nil {
		return result, err
	}
	if len(samples) == 0 {
		return result, errors.New("worker kills captured no active leases, recovery evidence is empty")
	}
	result.RecoverySamples = len(samples)
	result.RecoveryP99MS = nearestRankP99(samples)
	result.RecoveryTargetMS = float64(cfg.MaxRecovery) / float64(time.Millisecond)
	result.recoveryValues = make([]float64, len(samples))
	for index, sample := range samples {
		result.recoveryValues[index] = sample.RecoveryMS
	}
	if err := writeRecoverySamples(filepath.Join(runDirectory, "recovery-samples.jsonl"), samples); err != nil {
		return result, err
	}

	if err := captureLogs(ctx, compose, filepath.Join(runDirectory, "logs", "compose.log")); err != nil {
		return result, err
	}
	if err := compose.stop(ctx, cfg.Workers); err != nil {
		return result, fmt.Errorf("stop worker writers: %w", err)
	}
	if err := compose.stop(ctx, []string{databaseService}); err != nil {
		return result, fmt.Errorf("stop server writer: %w", err)
	}
	if err := writeEvent(events, actionEvent{
		Type:       "writers_stopped",
		ObservedAt: time.Now().UTC(),
		Details:    map[string]any{"workers": cfg.Workers, "server": databaseService},
	}); err != nil {
		return result, err
	}
	if err := events.close(); err != nil {
		return result, fmt.Errorf("close event trace: %w", err)
	}
	eventsClosed = true
	databaseDirectory := filepath.Join(runDirectory, "database")
	if err := copyDatabase(ctx, cfg, compose, databaseDirectory, runner); err != nil {
		return result, err
	}
	databaseSnapshot := filepath.Join(databaseDirectory, filepath.Base(cfg.DatabasePath))
	report, err := reconcileSnapshot(
		ctx,
		databaseSnapshot,
		filepath.Join(runDirectory, "submitted.jsonl"),
		cfg.Jobs,
		filepath.Join(runDirectory, "reconciliation.json"),
	)
	if err != nil {
		return result, err
	}
	result.ReconciliationPass = report.Passed
	if !report.Passed {
		return result, fmt.Errorf("reconciliation failed with %d violations", report.ViolationCount)
	}

	manifest.CompletedAt = time.Now().UTC()
	if err := writeJSONAtomic(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		return result, err
	}
	if err := finalizeRunArtifacts(runDirectory, cfg); err != nil {
		return result, err
	}
	return result, nil
}

func submitAll(
	ctx context.Context,
	cfg config,
	run int,
	seed int64,
	tenant, queue string,
	serverReleaseAt int,
	serverRestarted <-chan struct{},
	recorder *receiptRecorder,
	calibrator *clockCalibrator,
) error {
	client := submissionClient{
		baseURL: strings.TrimRight(cfg.ServerURL, "/"),
		client: &http.Client{
			Timeout: cfg.RequestTimeout,
		},
		pollInterval: cfg.PollInterval,
	}
	indexes := make(chan int)
	errorsChannel := make(chan error, cfg.SubmitConcurrency)
	var workers sync.WaitGroup
	for range cfg.SubmitConcurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range indexes {
				key := stableSubmissionKey(run, seed, index)
				response, err := client.submit(ctx, index, key, tenant, queue, cfg.JobDuration)
				if err != nil {
					errorsChannel <- err
					return
				}
				if err := recorder.record(reconcile.AcceptedRecord{
					SubmissionKey: key,
					JobID:         response.Job.ID,
					TenantID:      tenant,
					AcceptedAt:    time.Now().UTC(),
					Duplicate:     response.Duplicate,
				}); err != nil {
					errorsChannel <- err
					return
				}
				if !response.mapping.ServerTime.IsZero() {
					calibrator.record(response.mapping)
				}
			}
		}()
	}
sendLoop:
	for index := 1; index <= cfg.Jobs; index++ {
		if index == serverReleaseAt+1 {
			if err := waitForSubmissionRelease(ctx, serverRestarted, errorsChannel); err != nil {
				close(indexes)
				workers.Wait()
				return err
			}
		}
		select {
		case indexes <- index:
		case err := <-errorsChannel:
			close(indexes)
			workers.Wait()
			return err
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(indexes)
	workers.Wait()
	select {
	case err := <-errorsChannel:
		return err
	default:
	}
	return ctx.Err()
}

type actionResult struct {
	workerKills int
	serverKills int
	kills       []workerKill
	err         error
}

func runActions(
	ctx context.Context,
	cfg config,
	compose composeClient,
	random *rand.Rand,
	tenant string,
	serverTarget int,
	accepted *atomic.Int64,
	events *jsonLines,
	serverRestarted chan<- struct{},
	calibrator *clockCalibrator,
) actionResult {
	result := actionResult{kills: make([]workerKill, 0, cfg.WorkerKills)}
	serverKilled := false
	for result.workerKills < cfg.WorkerKills || !serverKilled {
		currentAccepted := int(accepted.Load())
		if !serverKilled && currentAccepted >= serverTarget {
			fraction := float64(currentAccepted) / float64(cfg.Jobs)
			if fraction < 0.20 || fraction > 0.80 {
				result.err = fmt.Errorf(
					"server kill progress %.4f is outside [0.20, 0.80]",
					fraction,
				)
				return result
			}
			if err := killAndRestartServer(
				ctx, cfg, compose, events, currentAccepted, fraction, tenant,
			); err != nil {
				result.err = err
				return result
			}
			calibrator.reset()
			serverKilled = true
			result.serverKills = 1
			close(serverRestarted)
			continue
		}
		if result.workerKills >= cfg.WorkerKills {
			if err := wait(ctx, cfg.PollInterval); err != nil {
				result.err = err
				return result
			}
			continue
		}

		activeWorkers, err := readActiveWorkers(ctx, compose, cfg.DatabasePath)
		if err != nil {
			result.err = err
			return result
		}
		candidates := intersection(activeWorkers, cfg.Workers)
		if len(candidates) == 0 {
			state, progressErr := readProgress(ctx, compose, cfg.DatabasePath, tenant)
			if progressErr != nil {
				result.err = progressErr
				return result
			}
			if state.Active == 0 && int(accepted.Load()) >= cfg.Jobs {
				result.err = fmt.Errorf(
					"work drained after %d worker kills, want at least %d",
					result.workerKills,
					cfg.WorkerKills,
				)
				return result
			}
			if err := wait(ctx, cfg.PollInterval); err != nil {
				result.err = err
				return result
			}
			continue
		}
		victim := candidates[random.Intn(len(candidates))]
		delay := randomDuration(random, cfg.ActionMinimum, cfg.ActionMaximum)
		plannedAt := time.Now().UTC().Add(delay)
		if err := writeEvent(events, actionEvent{
			Type:       "worker_kill_planned",
			PlannedAt:  plannedAt,
			ObservedAt: time.Now().UTC(),
			Service:    victim,
			Details: map[string]any{
				"kill_sequence": result.workerKills + 1,
				"delay_ms":      delay.Milliseconds(),
			},
		}); err != nil {
			result.err = err
			return result
		}
		if err := wait(ctx, delay); err != nil {
			result.err = err
			return result
		}
		state, err := readProgress(ctx, compose, cfg.DatabasePath, tenant)
		if err != nil {
			result.err = err
			return result
		}
		if state.Active == 0 && int(accepted.Load()) >= cfg.Jobs {
			result.err = fmt.Errorf(
				"work drained after %d worker kills, want at least %d",
				result.workerKills, cfg.WorkerKills,
			)
			return result
		}
		kill, err := killAndRestartWorker(
			ctx, cfg, compose, events, victim, result.workerKills+1, calibrator,
		)
		if err != nil {
			result.err = err
			return result
		}
		result.kills = append(result.kills, kill)
		result.workerKills++
	}
	return result
}

func killAndRestartWorker(
	ctx context.Context,
	cfg config,
	compose composeClient,
	events *jsonLines,
	worker string,
	sequence int,
	calibrator *clockCalibrator,
) (workerKill, error) {
	containerID, err := compose.containerID(ctx, worker)
	if err != nil {
		return workerKill{}, fmt.Errorf("find victim container for %s: %w", worker, err)
	}
	mapping, err := waitForClockMapping(ctx, cfg, calibrator)
	if err != nil {
		return workerKill{}, fmt.Errorf("calibrate server clock before killing %s: %w", worker, err)
	}
	snapshot, err := readCurrentFencedLeases(ctx, compose, cfg.DatabasePath, worker)
	if err != nil {
		return workerKill{}, err
	}
	if err := compose.kill(ctx, worker); err != nil {
		return workerKill{}, fmt.Errorf("SIGKILL worker %s: %w", worker, err)
	}
	confirmedHostAt := time.Now().UTC()
	confirmedAt := mapping.serverTime(confirmedHostAt)
	leases, err := reconcileKilledLeases(
		ctx,
		compose,
		cfg.DatabasePath,
		snapshot,
		confirmedAt,
		mapping.Uncertainty,
	)
	if err != nil {
		return workerKill{}, err
	}
	if err := writeEvent(events, actionEvent{
		Type:       "worker_killed",
		ObservedAt: confirmedHostAt,
		Service:    worker,
		Details: map[string]any{
			"kill_sequence":       sequence,
			"victim_container_id": containerID,
			"kill_confirmed_at":   confirmedAt,
			"clock_mapping":       mapping,
			"pre_kill_leases":     snapshot,
			"active_leases":       leases,
		},
	}); err != nil {
		return workerKill{}, err
	}
	if err := compose.ensureStarted(ctx, worker); err != nil {
		return workerKill{}, fmt.Errorf("restart worker %s: %w", worker, err)
	}
	if err := waitForService(ctx, cfg, compose, worker); err != nil {
		return workerKill{}, err
	}
	recoveredAt, err := waitForLeaseRecovery(ctx, cfg, compose, leases, mapping)
	if err != nil {
		return workerKill{}, err
	}
	if err := writeEvent(events, actionEvent{
		Type:       "worker_restarted",
		ObservedAt: time.Now().UTC(),
		Service:    worker,
		Details:    map[string]any{"kill_sequence": sequence},
	}); err != nil {
		return workerKill{}, err
	}
	return workerKill{
		Sequence:        sequence,
		Worker:          worker,
		ContainerID:     containerID,
		ConfirmedHostAt: confirmedHostAt,
		ConfirmedAt:     confirmedAt,
		ClockMapping:    mapping,
		Leases:          leases,
		RecoveredAt:     recoveredAt,
	}, nil
}

func killAndRestartServer(
	ctx context.Context,
	cfg config,
	compose composeClient,
	events *jsonLines,
	accepted int,
	fraction float64,
	tenant string,
) error {
	containerID, err := compose.containerID(ctx, databaseService)
	if err != nil {
		return fmt.Errorf("find server victim container: %w", err)
	}
	plannedAt := time.Now().UTC()
	if err := writeEvent(events, actionEvent{
		Type:       "server_kill_planned",
		PlannedAt:  plannedAt,
		ObservedAt: plannedAt,
		Service:    databaseService,
		Details: map[string]any{
			"accepted_jobs":       accepted,
			"progress":            fraction,
			"victim_container_id": containerID,
		},
	}); err != nil {
		return err
	}
	if err := compose.kill(ctx, databaseService); err != nil {
		return fmt.Errorf("SIGKILL server: %w", err)
	}
	confirmedAt := time.Now().UTC()
	if err := writeEvent(events, actionEvent{
		Type:       "server_killed",
		ObservedAt: confirmedAt,
		Service:    databaseService,
		Details: map[string]any{
			"accepted_jobs":       accepted,
			"progress":            fraction,
			"victim_container_id": containerID,
		},
	}); err != nil {
		return err
	}
	if err := compose.ensureStarted(ctx, databaseService); err != nil {
		return fmt.Errorf("restart server: %w", err)
	}
	if err := waitForReady(ctx, cfg, compose); err != nil {
		return err
	}
	readyAt := time.Now().UTC()
	if err := compose.restartWorkers(ctx, cfg.Workers); err != nil {
		return fmt.Errorf("re-register workers after server restart: %w", err)
	}
	for _, worker := range cfg.Workers {
		if err := waitForService(ctx, cfg, compose, worker); err != nil {
			return err
		}
	}
	firstLease, err := waitForPostRestartLease(
		ctx, cfg, compose, tenant, confirmedAt,
	)
	if err != nil {
		return err
	}
	recoveredAt := readyAt
	if firstLease.After(recoveredAt) {
		recoveredAt = firstLease
	}
	return writeEvent(events, actionEvent{
		Type:       "server_recovered",
		ObservedAt: recoveredAt,
		Service:    databaseService,
		Details: map[string]any{
			"kill_confirmed_at":           confirmedAt,
			"readiness_restored_at":       readyAt,
			"first_post_restart_lease_at": firstLease,
			"recovery_ms":                 float64(recoveredAt.Sub(confirmedAt)) / float64(time.Millisecond),
		},
	})
}

func waitForReady(ctx context.Context, cfg config, compose composeClient) error {
	readyURL, err := url.JoinPath(strings.TrimRight(cfg.ServerURL, "/"), "/health/ready")
	if err != nil {
		return fmt.Errorf("build readiness URL: %w", err)
	}
	client := &http.Client{Timeout: cfg.RequestTimeout}
	deadlineContext, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	return poll(deadlineContext, cfg.PollInterval, func() (bool, error) {
		request, err := http.NewRequestWithContext(deadlineContext, http.MethodGet, readyURL, nil)
		if err != nil {
			return false, err
		}
		response, err := client.Do(request)
		if err != nil {
			return false, nil
		}
		if response.StatusCode != http.StatusOK {
			_ = response.Body.Close()
			return false, nil
		}
		var body struct {
			Status string `json:"status"`
		}
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&body)
		closeErr := response.Body.Close()
		if decodeErr != nil || closeErr != nil {
			return false, nil
		}
		return body.Status == "ready", nil
	}, "server readiness")
}

func waitForService(
	ctx context.Context,
	cfg config,
	compose composeClient,
	service string,
) error {
	deadlineContext, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	return poll(deadlineContext, cfg.PollInterval, func() (bool, error) {
		services, err := compose.runningServices(deadlineContext)
		if err != nil {
			return false, nil
		}
		return contains(services, service), nil
	}, "running service "+service)
}

func waitForDrain(
	ctx context.Context,
	cfg config,
	compose composeClient,
	tenant string,
) (progress, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, cfg.DrainTimeout)
	defer cancel()
	var latest progress
	err := poll(deadlineContext, cfg.PollInterval, func() (bool, error) {
		value, err := readProgress(deadlineContext, compose, cfg.DatabasePath, tenant)
		if err != nil {
			return false, nil
		}
		latest = value
		if value.Total != cfg.Jobs {
			return false, fmt.Errorf("database jobs=%d want=%d", value.Total, cfg.Jobs)
		}
		return value.Active == 0 &&
			value.Succeeded+value.Failed+value.DeadLetter == cfg.Jobs, nil
	}, "job drain")
	if err != nil {
		return latest, err
	}
	return latest, nil
}

func waitForPostRestartLease(
	ctx context.Context,
	cfg config,
	compose composeClient,
	tenant string,
	after time.Time,
) (time.Time, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	var result time.Time
	err := poll(deadlineContext, cfg.PollInterval, func() (bool, error) {
		query := fmt.Sprintf(`
			SELECT COALESCE(MIN(a.leased_at), 0)
			FROM attempts a
			JOIN jobs j ON j.id = a.job_id
			WHERE j.tenant_id = %s AND a.leased_at > %d;`,
			sqlLiteral(tenant),
			after.UnixNano(),
		)
		output, err := compose.querySQLite(deadlineContext, cfg.DatabasePath, query)
		if err != nil {
			return false, nil
		}
		nanoseconds, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
		if err != nil {
			return false, fmt.Errorf("parse post-restart lease time: %w", err)
		}
		if nanoseconds == 0 {
			return false, nil
		}
		result = time.Unix(0, nanoseconds).UTC()
		return true, nil
	}, "post-restart durable lease")
	return result, err
}

func newClockMapping(serverTime, hostLower, hostUpper time.Time) (clockMapping, error) {
	if serverTime.IsZero() || hostLower.IsZero() || hostUpper.IsZero() {
		return clockMapping{}, errors.New("clock mapping contains a zero timestamp")
	}
	if hostUpper.Before(hostLower) {
		return clockMapping{}, errors.New("clock mapping host bounds are reversed")
	}
	uncertainty := hostUpper.Sub(hostLower) / 2
	hostMidpoint := hostLower.Add(hostUpper.Sub(hostLower) / 2)
	return clockMapping{
		HostLowerBound: hostLower,
		HostUpperBound: hostUpper,
		ServerTime:     serverTime,
		Offset:         serverTime.Sub(hostMidpoint),
		Uncertainty:    uncertainty,
	}, nil
}

func waitForClockMapping(
	ctx context.Context,
	cfg config,
	calibrator *clockCalibrator,
) (clockMapping, error) {
	if calibrator == nil {
		return clockMapping{}, errors.New("clock calibrator is nil")
	}
	deadlineContext, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	var result clockMapping
	err := poll(deadlineContext, cfg.PollInterval, func() (bool, error) {
		mapping, exists := calibrator.current()
		if !exists {
			return false, nil
		}
		if mapping.Uncertainty < 0 || mapping.Uncertainty > maxClockMappingUncertainty {
			return false, fmt.Errorf("persisted clock mapping uncertainty is invalid")
		}
		result = mapping
		return true, nil
	}, "durable server clock mapping")
	return result, err
}

func waitForLeaseRecovery(
	ctx context.Context,
	cfg config,
	compose composeClient,
	leases []activeLease,
	mapping clockMapping,
) (map[string]time.Time, error) {
	recovered := make(map[string]time.Time, len(leases))
	if len(leases) == 0 {
		return recovered, nil
	}
	conditions := make([]string, len(leases))
	for index, lease := range leases {
		conditions[index] = fmt.Sprintf(
			"(job_id = %s AND lease_generation > %d)",
			sqlLiteral(lease.JobID),
			lease.Generation,
		)
	}
	query := fmt.Sprintf(
		"SELECT DISTINCT job_id FROM attempts WHERE %s ORDER BY job_id;",
		strings.Join(conditions, " OR "),
	)
	deadlineContext, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	err := poll(deadlineContext, cfg.PollInterval, func() (bool, error) {
		output, err := compose.querySQLite(deadlineContext, cfg.DatabasePath, query)
		if err != nil {
			return false, nil
		}
		observedAt := mapping.serverTime(time.Now().UTC())
		for _, jobID := range nonemptyLines(output) {
			if _, exists := recovered[jobID]; !exists {
				recovered[jobID] = observedAt
			}
		}
		return len(recovered) == len(leases), nil
	}, "durable successor leases")
	if err != nil {
		return nil, err
	}
	return recovered, nil
}

func readProgress(
	ctx context.Context,
	compose composeClient,
	databasePath, tenant string,
) (progress, error) {
	query := fmt.Sprintf(`
		SELECT
			COUNT(*),
			COALESCE(SUM(state = 'SUCCEEDED'), 0),
			COALESCE(SUM(state = 'FAILED'), 0),
			COALESCE(SUM(state = 'DEAD_LETTER'), 0),
			COALESCE(SUM(state NOT IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTER')), 0)
		FROM jobs
		WHERE tenant_id = %s;`,
		sqlLiteral(tenant),
	)
	output, err := compose.querySQLite(ctx, databasePath, query)
	if err != nil {
		return progress{}, fmt.Errorf("query campaign progress: %w", err)
	}
	parts := strings.Split(strings.TrimSpace(string(output)), "|")
	if len(parts) != 5 {
		return progress{}, fmt.Errorf("campaign progress returned %q", strings.TrimSpace(string(output)))
	}
	values := make([]int, len(parts))
	for index, part := range parts {
		values[index], err = strconv.Atoi(part)
		if err != nil {
			return progress{}, fmt.Errorf("parse campaign progress %q: %w", part, err)
		}
	}
	return progress{
		Total:      values[0],
		Succeeded:  values[1],
		Failed:     values[2],
		DeadLetter: values[3],
		Active:     values[4],
	}, nil
}

func readCurrentFencedLeases(
	ctx context.Context,
	compose composeClient,
	databasePath, worker string,
) ([]activeLease, error) {
	query := fmt.Sprintf(`
		SELECT a.job_id, a.attempt_no, a.lease_generation, a.leased_at
		FROM attempts a
		JOIN jobs j
		  ON j.id = a.job_id
		 AND j.attempt_no = a.attempt_no
		 AND j.lease_generation = a.lease_generation
		WHERE a.worker_id = %s
		  AND a.state IN ('LEASED', 'RUNNING')
		  AND j.state IN ('SCHEDULED', 'RUNNING')
		ORDER BY a.job_id;`,
		sqlLiteral(worker),
	)
	output, err := compose.querySQLite(ctx, databasePath, query)
	if err != nil {
		return nil, fmt.Errorf("query active leases for %s: %w", worker, err)
	}
	lines := nonemptyLines(output)
	result := make([]activeLease, 0, len(lines))
	for _, line := range lines {
		parts := strings.Split(line, "|")
		if len(parts) != 4 {
			return nil, fmt.Errorf("active lease row has %d columns: %q", len(parts), line)
		}
		attemptNo, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("parse active lease attempt: %w", err)
		}
		generation, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse active lease generation: %w", err)
		}
		leasedAt, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse active lease timestamp: %w", err)
		}
		result = append(result, activeLease{
			JobID:      parts[0],
			AttemptNo:  attemptNo,
			Generation: generation,
			LeasedAt:   time.Unix(0, leasedAt).UTC(),
		})
	}
	return result, nil
}

func reconcileKilledLeases(
	ctx context.Context,
	compose composeClient,
	databasePath string,
	snapshot []activeLease,
	confirmedAt time.Time,
	uncertainty time.Duration,
) ([]activeLease, error) {
	if len(snapshot) == 0 {
		return nil, nil
	}
	conditions := make([]string, len(snapshot))
	for index, lease := range snapshot {
		conditions[index] = fmt.Sprintf(
			"(job_id = %s AND attempt_no = %d AND lease_generation = %d)",
			sqlLiteral(lease.JobID),
			lease.AttemptNo,
			lease.Generation,
		)
	}
	query := fmt.Sprintf(`
		SELECT job_id, attempt_no, lease_generation, state, COALESCE(completed_at, 0)
		FROM attempts
		WHERE %s
		ORDER BY job_id;`,
		strings.Join(conditions, " OR "),
	)
	output, err := compose.querySQLite(ctx, databasePath, query)
	if err != nil {
		return nil, fmt.Errorf("reconcile killed lease snapshot: %w", err)
	}
	states := make([]leaseBoundaryState, 0, len(snapshot))
	for _, line := range nonemptyLines(output) {
		parts := strings.Split(line, "|")
		if len(parts) != 5 {
			return nil, fmt.Errorf("lease boundary row has %d columns: %q", len(parts), line)
		}
		attemptNo, parseErr := strconv.Atoi(parts[1])
		if parseErr != nil {
			return nil, fmt.Errorf("parse lease boundary attempt: %w", parseErr)
		}
		generation, parseErr := strconv.ParseInt(parts[2], 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse lease boundary generation: %w", parseErr)
		}
		completedAt, parseErr := strconv.ParseInt(parts[4], 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse lease boundary completion: %w", parseErr)
		}
		state := leaseBoundaryState{
			JobID:      parts[0],
			AttemptNo:  attemptNo,
			Generation: generation,
			State:      parts[3],
		}
		if completedAt != 0 {
			state.CompletedAt = time.Unix(0, completedAt).UTC()
		}
		states = append(states, state)
	}
	return selectKilledLeases(snapshot, states, confirmedAt, uncertainty)
}

func selectKilledLeases(
	snapshot []activeLease,
	states []leaseBoundaryState,
	confirmedAt time.Time,
	uncertainty time.Duration,
) ([]activeLease, error) {
	if confirmedAt.IsZero() {
		return nil, errors.New("confirmed kill timestamp is zero")
	}
	if uncertainty < 0 || uncertainty > maxClockMappingUncertainty {
		return nil, fmt.Errorf("kill boundary uncertainty %s is invalid", uncertainty)
	}
	byFence := make(map[string]leaseBoundaryState, len(states))
	for _, state := range states {
		key := leaseFenceKey(state.JobID, state.AttemptNo, state.Generation)
		if _, duplicate := byFence[key]; duplicate {
			return nil, fmt.Errorf("lease boundary state %s is duplicated", key)
		}
		byFence[key] = state
	}
	lowerBound := confirmedAt.Add(-uncertainty)
	upperBound := confirmedAt.Add(uncertainty)
	affected := make([]activeLease, 0, len(snapshot))
	for _, lease := range snapshot {
		key := leaseFenceKey(lease.JobID, lease.AttemptNo, lease.Generation)
		state, exists := byFence[key]
		if !exists {
			return nil, fmt.Errorf("snapshot lease %s disappeared before boundary reconciliation", key)
		}
		if state.CompletedAt.IsZero() {
			if state.State != "LEASED" && state.State != "RUNNING" {
				return nil, fmt.Errorf("closed snapshot lease %s has no completion timestamp", key)
			}
			affected = append(affected, lease)
			continue
		}
		if state.CompletedAt.Before(lowerBound) {
			continue
		}
		if state.CompletedAt.After(upperBound) {
			affected = append(affected, lease)
			continue
		}
		return nil, fmt.Errorf(
			"snapshot lease %s completed within uncertain kill boundary [%s,%s]",
			key,
			lowerBound.Format(time.RFC3339Nano),
			upperBound.Format(time.RFC3339Nano),
		)
	}
	if len(byFence) != len(snapshot) {
		return nil, errors.New("lease boundary query returned an unrecognized fence")
	}
	return affected, nil
}

func leaseFenceKey(jobID string, attemptNo int, generation int64) string {
	return fmt.Sprintf("%s/%d/%d", jobID, attemptNo, generation)
}

func readActiveWorkers(
	ctx context.Context,
	compose composeClient,
	databasePath string,
) ([]string, error) {
	output, err := compose.querySQLite(ctx, databasePath, `
		SELECT DISTINCT a.worker_id
		FROM attempts a
		JOIN jobs j
		  ON j.id = a.job_id
		 AND j.attempt_no = a.attempt_no
		 AND j.lease_generation = a.lease_generation
		WHERE a.state IN ('LEASED', 'RUNNING')
		  AND j.state IN ('SCHEDULED', 'RUNNING')
		ORDER BY a.worker_id;`)
	if err != nil {
		return nil, fmt.Errorf("query active workers: %w", err)
	}
	return nonemptyLines(output), nil
}

func buildRecoverySamples(
	ctx context.Context,
	compose composeClient,
	databasePath string,
	kills []workerKill,
) ([]recoverySample, error) {
	jobSet := make(map[string]struct{})
	for _, kill := range kills {
		for _, lease := range kill.Leases {
			jobSet[lease.JobID] = struct{}{}
		}
	}
	if len(jobSet) == 0 {
		return nil, nil
	}
	jobIDs := make([]string, 0, len(jobSet))
	for jobID := range jobSet {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	literals := make([]string, len(jobIDs))
	for index, jobID := range jobIDs {
		literals[index] = sqlLiteral(jobID)
	}
	query := fmt.Sprintf(`
		SELECT a.job_id, a.attempt_no, a.lease_generation, a.leased_at,
		       COALESCE(c.committed_at, 0)
		FROM attempts a
		LEFT JOIN job_completions c ON c.job_id = a.job_id
		WHERE a.job_id IN (%s)
		ORDER BY a.job_id, a.lease_generation;`,
		strings.Join(literals, ","),
	)
	output, err := compose.querySQLite(ctx, databasePath, query)
	if err != nil {
		return nil, fmt.Errorf("query recovery attempts: %w", err)
	}
	attempts := make(map[string][]databaseAttempt)
	for _, line := range nonemptyLines(output) {
		parts := strings.Split(line, "|")
		if len(parts) != 5 {
			return nil, fmt.Errorf("recovery attempt row has %d columns: %q", len(parts), line)
		}
		attemptNo, parseErr := strconv.Atoi(parts[1])
		if parseErr != nil {
			return nil, fmt.Errorf("parse recovery attempt: %w", parseErr)
		}
		generation, parseErr := strconv.ParseInt(parts[2], 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse recovery generation: %w", parseErr)
		}
		leasedAt, parseErr := strconv.ParseInt(parts[3], 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse recovery lease time: %w", parseErr)
		}
		completedAt, parseErr := strconv.ParseInt(parts[4], 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse recovery completion time: %w", parseErr)
		}
		attempt := databaseAttempt{
			JobID:      parts[0],
			AttemptNo:  attemptNo,
			Generation: generation,
			LeasedAt:   time.Unix(0, leasedAt).UTC(),
		}
		if completedAt != 0 {
			attempt.CompletionAt = time.Unix(0, completedAt).UTC()
		}
		attempts[parts[0]] = append(attempts[parts[0]], attempt)
	}
	return scoreRecoverySamples(kills, attempts)
}

func scoreRecoverySamples(
	kills []workerKill,
	attempts map[string][]databaseAttempt,
) ([]recoverySample, error) {
	var samples []recoverySample
	for _, kill := range kills {
		if kill.ConfirmedHostAt.IsZero() || kill.ConfirmedAt.IsZero() {
			return nil, fmt.Errorf("worker kill %d has no confirmed timestamp", kill.Sequence)
		}
		if kill.ClockMapping.Uncertainty < 0 ||
			kill.ClockMapping.Uncertainty > maxClockMappingUncertainty {
			return nil, fmt.Errorf("worker kill %d has invalid clock uncertainty", kill.Sequence)
		}
		if mapped := kill.ClockMapping.serverTime(kill.ConfirmedHostAt); !mapped.Equal(kill.ConfirmedAt) {
			return nil, fmt.Errorf("worker kill %d clock mapping is inconsistent", kill.Sequence)
		}
		for _, killed := range kill.Leases {
			var successor databaseAttempt
			for _, attempt := range attempts[killed.JobID] {
				if attempt.Generation > killed.Generation {
					successor = attempt
					break
				}
			}
			if successor.JobID == "" {
				return nil, fmt.Errorf(
					"job %s has no successor after killed generation %d",
					killed.JobID, killed.Generation,
				)
			}
			if successor.LeasedAt.IsZero() {
				return nil, fmt.Errorf("job %s successor has no durable lease timestamp", killed.JobID)
			}
			observedAt, exists := kill.RecoveredAt[killed.JobID]
			if !exists {
				return nil, fmt.Errorf("job %s has no observed successor timestamp", killed.JobID)
			}
			if observedAt.Add(kill.ClockMapping.Uncertainty).Before(successor.LeasedAt) {
				return nil, fmt.Errorf("job %s successor observation predates its durable lease", killed.JobID)
			}
			if successor.CompletionAt.IsZero() || successor.CompletionAt.Before(successor.LeasedAt) {
				return nil, fmt.Errorf("job %s successor has no ordered durable completion", killed.JobID)
			}
			killUpperBound := kill.ConfirmedAt.Add(kill.ClockMapping.Uncertainty)
			if !successor.LeasedAt.After(killUpperBound) {
				return nil, fmt.Errorf(
					"job %s successor lease is not after the uncertain kill boundary",
					killed.JobID,
				)
			}
			recovery := successor.LeasedAt.Sub(kill.ConfirmedAt)
			if recovery < 0 {
				return nil, fmt.Errorf(
					"job %s successor lease predates confirmed worker kill",
					killed.JobID,
				)
			}
			samples = append(samples, recoverySample{
				KillSequence:        kill.Sequence,
				Worker:              kill.Worker,
				VictimContainerID:   kill.ContainerID,
				JobID:               killed.JobID,
				KilledAttempt:       killed.AttemptNo,
				KilledGeneration:    killed.Generation,
				KillConfirmedHostAt: kill.ConfirmedHostAt,
				KillConfirmedAt:     kill.ConfirmedAt,
				ClockMapping:        kill.ClockMapping,
				SuccessorAttempt:    successor.AttemptNo,
				SuccessorGeneration: successor.Generation,
				SuccessorLeasedAt:   successor.LeasedAt,
				SuccessorObservedAt: observedAt,
				CompletionAt:        successor.CompletionAt,
				RecoveryMS:          float64(recovery) / float64(time.Millisecond),
			})
		}
	}
	sort.Slice(samples, func(left, right int) bool {
		if samples[left].KillSequence != samples[right].KillSequence {
			return samples[left].KillSequence < samples[right].KillSequence
		}
		return samples[left].JobID < samples[right].JobID
	})
	return samples, nil
}

func captureLogs(ctx context.Context, compose composeClient, path string) error {
	output, err := compose.run(ctx, "logs", "--no-color", "--timestamps")
	if err != nil {
		return fmt.Errorf("capture Compose logs: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	if err := os.WriteFile(path, output, 0o644); err != nil {
		return fmt.Errorf("write Compose logs: %w", err)
	}
	return nil
}

func copyDatabase(
	ctx context.Context,
	cfg config,
	compose composeClient,
	destination string,
	runner commandRunner,
) error {
	if err := os.MkdirAll(destination, 0o755); err != nil {
		return fmt.Errorf("create database snapshot directory: %w", err)
	}
	containerID, err := compose.containerID(ctx, databaseService)
	if err != nil {
		return fmt.Errorf("find stopped server container: %w", err)
	}
	sourceDirectory := filepath.ToSlash(filepath.Dir(cfg.DatabasePath)) + "/."
	if _, err := runner.Run(
		ctx,
		cfg.DockerExecutable,
		"cp",
		containerID+":"+sourceDirectory,
		destination,
	); err != nil {
		return fmt.Errorf("copy stopped SQLite database and WAL: %w", err)
	}
	return nil
}

func reconcileSnapshot(
	ctx context.Context,
	databasePath, manifestPath string,
	expectedJobs int,
	reportPath string,
) (reconcile.Report, error) {
	manifest, err := os.Open(manifestPath)
	if err != nil {
		return reconcile.Report{}, fmt.Errorf("open submitted manifest: %w", err)
	}
	accepted, readErr := reconcile.ReadManifest(manifest)
	closeErr := manifest.Close()
	if readErr != nil {
		return reconcile.Report{}, readErr
	}
	if closeErr != nil {
		return reconcile.Report{}, fmt.Errorf("close submitted manifest: %w", closeErr)
	}
	db, err := reconcile.OpenReadOnly(databasePath)
	if err != nil {
		return reconcile.Report{}, err
	}
	report, reconcileErr := reconcile.Reconcile(ctx, db, accepted, reconcile.Options{
		ExpectedJobs: expectedJobs,
		MaxDetails:   1000,
	})
	closeErr = db.Close()
	if reconcileErr != nil {
		return reconcile.Report{}, reconcileErr
	}
	if closeErr != nil {
		return reconcile.Report{}, fmt.Errorf("close reconciliation database: %w", closeErr)
	}
	if err := writeJSONAtomic(reportPath, report); err != nil {
		return reconcile.Report{}, err
	}
	return report, nil
}

func writeRecoverySamples(path string, samples []recoverySample) error {
	writer, err := createJSONLines(path)
	if err != nil {
		return fmt.Errorf("create recovery samples: %w", err)
	}
	for _, sample := range samples {
		if err := writer.write(sample); err != nil {
			_ = writer.close()
			return fmt.Errorf("write recovery sample: %w", err)
		}
	}
	if err := writer.close(); err != nil {
		return fmt.Errorf("close recovery samples: %w", err)
	}
	return nil
}

func nearestRankP99(samples []recoverySample) float64 {
	values := make([]float64, len(samples))
	for index, sample := range samples {
		values[index] = sample.RecoveryMS
	}
	return nearestRank(values, 0.99)
}

func nearestRank(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	values = append([]float64(nil), values...)
	sort.Float64s(values)
	rank := int(math.Ceil(percentile * float64(len(values))))
	return values[max(1, rank)-1]
}

func writeEvent(writer *jsonLines, event actionEvent) error {
	event.Sequence = writer.nextSequence()
	if err := writer.write(event); err != nil {
		return fmt.Errorf("write action event: %w", err)
	}
	return nil
}

func writeJSONAtomic(path string, value any) error {
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	body = append(body, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create JSON directory: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".json-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary JSON file: %w", err)
	}
	temporary := file.Name()
	defer func() {
		_ = os.Remove(temporary)
	}()
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temporary JSON file: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync temporary JSON file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary JSON file: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("replace JSON file: %w", removeErr)
		}
		if retryErr := os.Rename(temporary, path); retryErr != nil {
			return fmt.Errorf("replace JSON file: %w", retryErr)
		}
	}
	return nil
}

func poll(
	ctx context.Context,
	interval time.Duration,
	predicate func() (bool, error),
	description string,
) error {
	for {
		satisfied, err := predicate()
		if err != nil {
			return fmt.Errorf("%s predicate: %w", description, err)
		}
		if satisfied {
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

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func waitForSubmissionRelease(
	ctx context.Context,
	released <-chan struct{},
	submissionErrors <-chan error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-released:
		return nil
	case err := <-submissionErrors:
		return err
	}
}

func randomDuration(random *rand.Rand, minimum, maximum time.Duration) time.Duration {
	if maximum == minimum {
		return minimum
	}
	return minimum + time.Duration(random.Int63n(int64(maximum-minimum)+1))
}

func stableSubmissionKey(run int, seed int64, index int) string {
	return fmt.Sprintf("chaos-v1-r%02d-s%s-j%08d", run, seedToken(seed), index)
}

func projectName(prefix string, run int, seed int64) string {
	return fmt.Sprintf("%s-r%02d-s%s", prefix, run, seedToken(seed))
}

func seedToken(seed int64) string {
	if seed < 0 {
		return "n" + strconv.FormatUint(uint64(-(seed+1))+1, 16)
	}
	return strconv.FormatInt(seed, 16)
}

func sqlLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func nonemptyLines(output []byte) []string {
	raw := strings.Split(strings.ReplaceAll(string(output), "\r\n", "\n"), "\n")
	result := make([]string, 0, len(raw))
	for _, line := range raw {
		if value := strings.TrimSpace(line); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func intersection(values, allowed []string) []string {
	set := make(map[string]bool, len(allowed))
	for _, value := range allowed {
		set[value] = true
	}
	var result []string
	for _, value := range values {
		if set[value] {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (c config) validate() error {
	switch {
	case strings.TrimSpace(c.ComposeFile) == "":
		return errors.New("compose file is required")
	case strings.TrimSpace(c.OutputDirectory) == "":
		return errors.New("output directory is required")
	case !composeNamePattern.MatchString(c.ProjectPrefix):
		return fmt.Errorf("project prefix %q must match %s", c.ProjectPrefix, composeNamePattern)
	case c.Runs < 1:
		return errors.New("runs must be positive")
	case c.Jobs < 3:
		return errors.New("jobs must be at least 3")
	case c.SubmitConcurrency < 1:
		return errors.New("submit concurrency must be positive")
	case c.WorkerKills < minimumWorkerKills:
		return fmt.Errorf("worker kills must be at least %d", minimumWorkerKills)
	case len(c.Workers) == 0:
		return errors.New("at least one worker is required")
	case c.JobDuration < 0:
		return errors.New("job duration must not be negative")
	case c.JobDuration > time.Duration(math.MaxInt64/2):
		return errors.New("job duration is too large")
	case c.ActionMinimum < 0 || c.ActionMaximum < c.ActionMinimum:
		return errors.New("action interval range is invalid")
	case c.PollInterval <= 0:
		return errors.New("poll interval must be positive")
	case c.StartupTimeout <= 0 || c.DrainTimeout <= 0 ||
		c.RequestTimeout <= 0 || c.MaxRecovery <= 0:
		return errors.New("timeouts must be positive")
	case strings.TrimSpace(c.DatabasePath) == "":
		return errors.New("container database path is required")
	case strings.TrimSpace(c.DockerExecutable) == "":
		return errors.New("docker executable is required")
	}
	parsed, err := url.Parse(c.ServerURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("server URL must be an absolute HTTP URL")
	}
	seen := make(map[string]bool, len(c.Workers))
	for _, worker := range c.Workers {
		if worker == "" || seen[worker] {
			return errors.New("worker service names must be nonempty and unique")
		}
		seen[worker] = true
	}
	return nil
}
