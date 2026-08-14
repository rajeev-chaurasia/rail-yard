package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/evidence"
)

const maxResponseBytes = 4 << 20

type runOptions struct {
	serverURL          string
	metricsURL         string
	output             string
	runID              string
	phase              string
	environmentPath    string
	configurationHash  string
	jobCount           int
	workerCount        int
	workerSlots        int
	submitConcurrency  int
	pollConcurrency    int
	submissionAttempts int
	seed               int64
	requestTimeout     time.Duration
	healthTimeout      time.Duration
	drainTimeout       time.Duration
	pollInterval       time.Duration
	tenantID           string
	queue              string
	qualification      bool
}

type benchmarkClient struct {
	baseURL        *url.URL
	client         *http.Client
	requestTimeout time.Duration
}

type jobPollResult struct {
	job        domain.Job
	observedAt time.Time
	err        error
}

func runWorkload(ctx context.Context, arguments []string) error {
	options, err := parseRunOptions(arguments)
	if err != nil {
		return err
	}
	requestBody, payloadHash, err := workloadRequest(options)
	if err != nil {
		return err
	}
	environment, err := loadEnvironment(options.environmentPath)
	if err != nil {
		return err
	}
	if err := createArtifactDirectory(options.output); err != nil {
		return err
	}
	startedAt := time.Now().UTC()
	manifest := evidence.RunManifest{
		SchemaVersion: evidence.SchemaVersion,
		RunID:         options.runID,
		Phase:         evidence.RunPhase(options.phase),
		Scored:        options.phase == string(evidence.PhaseMeasured),
		Status:        evidence.StatusRunning,
		StartedAt:     startedAt,
		Config: evidence.RunConfig{
			ServerURL:           options.serverURL,
			JobCount:            options.jobCount,
			WorkerCount:         options.workerCount,
			WorkerSlots:         options.workerSlots,
			SubmitConcurrency:   options.submitConcurrency,
			PollConcurrency:     options.pollConcurrency,
			SubmissionAttempts:  options.submissionAttempts,
			RequestTimeout:      options.requestTimeout,
			HealthTimeout:       options.healthTimeout,
			DrainTimeout:        options.drainTimeout,
			PollInterval:        options.pollInterval,
			TenantID:            options.tenantID,
			Queue:               options.queue,
			PayloadBytes:        len(requestBody),
			PayloadSHA256:       payloadHash,
			ConfigurationSHA256: strings.ToLower(options.configurationHash),
			Seed:                options.seed,
			Qualification:       options.qualification,
		},
		Environment: environment,
	}
	if err := evidence.WriteJSON(filepath.Join(options.output, "manifest.json"), manifest); err != nil {
		return fmt.Errorf("write initial manifest: %w", err)
	}

	client, err := newBenchmarkClient(options.serverURL, max(options.submitConcurrency, options.pollConcurrency), options.requestTimeout)
	if err != nil {
		return finalizeFailedRun(options.output, &manifest, nil, nil, err)
	}

	fmt.Printf("waiting for liveness at %s\n", options.serverURL)
	if err := client.waitForHealth(ctx, "/health/live", "ok", options.healthTimeout, options.pollInterval); err != nil {
		return finalizeFailedRun(options.output, &manifest, nil, nil, err)
	}
	fmt.Printf("waiting for readiness at %s\n", options.serverURL)
	if err := client.waitForHealth(ctx, "/health/ready", "ready", options.healthTimeout, options.pollInterval); err != nil {
		return finalizeFailedRun(options.output, &manifest, nil, nil, err)
	}

	fmt.Printf("submitting %d no-op jobs with concurrency %d\n", options.jobCount, options.submitConcurrency)
	submissions, submitErr := client.submitJobs(
		ctx,
		manifest,
		requestBody,
		options.submitConcurrency,
		options.submissionAttempts,
	)
	if submitErr != nil {
		return finalizeFailedRun(options.output, &manifest, submissions, nil, submitErr)
	}

	fmt.Printf("polling explicit drain predicate for %d jobs\n", len(submissions))
	drainSamples, drainErr := client.waitForDrain(
		ctx,
		manifest.RunID,
		submissions,
		options.pollConcurrency,
		options.drainTimeout,
		options.pollInterval,
	)
	if drainErr != nil {
		return finalizeFailedRun(options.output, &manifest, submissions, drainSamples, drainErr)
	}

	if options.metricsURL != "" {
		metrics, err := fetchMetrics(ctx, options.metricsURL, options.requestTimeout)
		if err != nil {
			return finalizeFailedRun(options.output, &manifest, submissions, drainSamples, err)
		}
		if err := evidence.WriteBytes(filepath.Join(options.output, "metrics.prom"), metrics); err != nil {
			return finalizeFailedRun(options.output, &manifest, submissions, drainSamples, err)
		}
	}

	finishedAt := time.Now().UTC()
	manifest.WorkloadFinishedAt = &finishedAt
	manifest.Status = evidence.StatusAwaitingReconciliation
	if err := writeRunArtifacts(options.output, manifest, submissions, drainSamples); err != nil {
		return err
	}
	fmt.Printf("workload drained; snapshot the quiesced database and run reconcile for %s\n", options.runID)
	return nil
}

func parseRunOptions(arguments []string) (runOptions, error) {
	var options runOptions
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.StringVar(&options.serverURL, "server-url", "http://127.0.0.1:8080", "Rail Yard server base URL")
	flags.StringVar(&options.metricsURL, "metrics-url", "", "optional Prometheus exposition URL")
	flags.StringVar(&options.output, "output", "", "new run artifact directory")
	flags.StringVar(&options.runID, "run-id", "", "stable run identifier")
	flags.StringVar(&options.phase, "phase", "", "warmup or measured")
	flags.StringVar(&options.environmentPath, "environment", "", "environment manifest JSON")
	flags.StringVar(&options.configurationHash, "configuration-sha256", "", "deployment configuration SHA-256")
	flags.IntVar(&options.jobCount, "jobs", 5_000, "number of independent no-op jobs")
	flags.IntVar(&options.workerCount, "workers", 8, "expected worker process count")
	flags.IntVar(&options.workerSlots, "worker-slots", 256, "slots per worker")
	flags.IntVar(&options.submitConcurrency, "submit-concurrency", 64, "maximum concurrent submissions")
	flags.IntVar(&options.pollConcurrency, "poll-concurrency", 128, "maximum concurrent job inspections")
	flags.IntVar(&options.submissionAttempts, "submission-attempts", 3, "maximum attempts per idempotent submission")
	flags.Int64Var(&options.seed, "seed", 1, "recorded deterministic workload seed")
	flags.DurationVar(&options.requestTimeout, "request-timeout", 10*time.Second, "per-request timeout")
	flags.DurationVar(&options.healthTimeout, "health-timeout", 2*time.Minute, "health polling deadline")
	flags.DurationVar(&options.drainTimeout, "drain-timeout", 30*time.Minute, "drain polling deadline")
	flags.DurationVar(&options.pollInterval, "poll-interval", 250*time.Millisecond, "predicate polling interval")
	flags.StringVar(&options.tenantID, "tenant", "benchmark", "workload tenant")
	flags.StringVar(&options.queue, "queue", "benchmark", "workload queue")
	flags.BoolVar(&options.qualification, "qualification", false, "require the canonical qualification manifest")
	if err := flags.Parse(arguments); err != nil {
		return options, err
	}
	if flags.NArg() != 0 {
		return options, fmt.Errorf("run does not accept positional arguments")
	}
	if options.output == "" {
		return options, errors.New("-output is required")
	}
	if !validRunID(options.runID) {
		return options, errors.New("-run-id must contain 1 to 96 letters, digits, dots, underscores, or hyphens")
	}
	if options.phase != string(evidence.PhaseWarmup) && options.phase != string(evidence.PhaseMeasured) {
		return options, errors.New("-phase must be warmup or measured")
	}
	if options.jobCount < 2 {
		return options, errors.New("-jobs must be at least 2")
	}
	for name, value := range map[string]int{
		"workers":             options.workerCount,
		"worker-slots":        options.workerSlots,
		"submit-concurrency":  options.submitConcurrency,
		"poll-concurrency":    options.pollConcurrency,
		"submission-attempts": options.submissionAttempts,
	} {
		if value < 1 {
			return options, fmt.Errorf("-%s must be positive", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"request-timeout": options.requestTimeout,
		"health-timeout":  options.healthTimeout,
		"drain-timeout":   options.drainTimeout,
		"poll-interval":   options.pollInterval,
	} {
		if value <= 0 {
			return options, fmt.Errorf("-%s must be positive", name)
		}
	}
	if options.tenantID == "" || options.queue == "" {
		return options, errors.New("-tenant and -queue must not be empty")
	}
	if _, err := normalizeBaseURL(options.serverURL); err != nil {
		return options, fmt.Errorf("-server-url: %w", err)
	}
	if options.metricsURL != "" {
		if _, err := normalizeBaseURL(options.metricsURL); err != nil {
			return options, fmt.Errorf("-metrics-url: %w", err)
		}
	}
	if options.configurationHash != "" {
		decoded, err := hex.DecodeString(options.configurationHash)
		if err != nil || len(decoded) != sha256.Size {
			return options, errors.New("-configuration-sha256 must be a 64-character hexadecimal digest")
		}
	}
	if options.qualification && options.configurationHash == "" {
		return options, errors.New("-configuration-sha256 is required for qualification runs")
	}
	return options, nil
}

func workloadRequest(options runOptions) ([]byte, string, error) {
	request := api.SubmitJobRequest{
		Job: domain.JobSpec{
			TenantID: options.tenantID,
			Queue:    options.queue,
			Priority: 0,
			SlotCost: 1,
			Payload: domain.Payload{
				Type:       domain.PayloadNoop,
				DurationMS: 0,
			},
			Retry: domain.RetryPolicy{
				MaxAttempts: 1,
				Retryable:   false,
			},
		},
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, "", fmt.Errorf("encode fixed workload: %w", err)
	}
	sum := sha256.Sum256(body)
	return body, hex.EncodeToString(sum[:]), nil
}

func createArtifactDirectory(path string) error {
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

func newBenchmarkClient(rawURL string, concurrency int, timeout time.Duration) (*benchmarkClient, error) {
	baseURL, err := normalizeBaseURL(rawURL)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = concurrency
	transport.MaxIdleConnsPerHost = concurrency
	transport.MaxConnsPerHost = concurrency
	return &benchmarkClient{
		baseURL: baseURL,
		client: &http.Client{
			Transport: transport,
		},
		requestTimeout: timeout,
	}, nil
}

func normalizeBaseURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("URL scheme must be http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("URL must contain only scheme, host, and optional base path")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	return parsed, nil
}

func (c *benchmarkClient) endpoint(path string) string {
	result := *c.baseURL
	result.Path = strings.TrimSuffix(c.baseURL.Path, "/") + path
	return result.String()
}

func (c *benchmarkClient) waitForHealth(
	ctx context.Context,
	path string,
	expectedStatus string,
	timeout time.Duration,
	interval time.Duration,
) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastError error
	for {
		requestCtx, requestCancel := context.WithTimeout(deadlineCtx, c.requestTimeout)
		request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, c.endpoint(path), nil)
		if err == nil {
			var response *http.Response
			response, err = c.client.Do(request)
			if err == nil {
				var health struct {
					Status string `json:"status"`
				}
				decodeErr := decodeResponseJSON(response, &health)
				switch {
				case decodeErr != nil:
					err = decodeErr
				case response.StatusCode != http.StatusOK:
					err = fmt.Errorf("%s returned HTTP %d", path, response.StatusCode)
				case health.Status != expectedStatus:
					err = fmt.Errorf("%s returned status %q", path, health.Status)
				default:
					requestCancel()
					return nil
				}
			}
		}
		requestCancel()
		lastError = err

		timer := time.NewTimer(interval)
		select {
		case <-deadlineCtx.Done():
			timer.Stop()
			return fmt.Errorf("health predicate %s=%s not met: %w: last error: %w",
				path, expectedStatus, deadlineCtx.Err(), lastError)
		case <-timer.C:
		}
	}
}

func (c *benchmarkClient) submitJobs(
	ctx context.Context,
	manifest evidence.RunManifest,
	body []byte,
	concurrency int,
	maxAttempts int,
) ([]evidence.SubmissionSample, error) {
	records := make([]evidence.SubmissionSample, manifest.Config.JobCount)
	indexes := make(chan int)
	var workers sync.WaitGroup
	for range concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range indexes {
				records[index] = c.submitJob(ctx, manifest, body, index, maxAttempts)
			}
		}()
	}
	for index := 0; index < manifest.Config.JobCount; index++ {
		select {
		case indexes <- index:
		case <-ctx.Done():
			close(indexes)
			workers.Wait()
			return records[:index], ctx.Err()
		}
	}
	close(indexes)
	workers.Wait()

	var failed int
	for _, record := range records {
		if record.Error != "" {
			failed++
		}
	}
	if failed > 0 {
		return records, fmt.Errorf("%d submissions failed", failed)
	}
	return records, nil
}

func (c *benchmarkClient) submitJob(
	ctx context.Context,
	manifest evidence.RunManifest,
	body []byte,
	index int,
	maxAttempts int,
) evidence.SubmissionSample {
	record := evidence.SubmissionSample{
		SchemaVersion:    evidence.SchemaVersion,
		RunID:            manifest.RunID,
		Index:            index,
		IdempotencyKey:   evidence.StableIdempotencyKey(manifest.RunID, manifest.Config.Seed, index),
		RequestStartedAt: time.Now().UTC(),
	}
	var lastError error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		record.AttemptCount = attempt
		requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		request, err := http.NewRequestWithContext(
			requestCtx,
			http.MethodPost,
			c.endpoint("/v1/jobs"),
			bytes.NewReader(body),
		)
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", record.IdempotencyKey)
			request.Header.Set("X-Rail-Yard-Actor", "benchmark")
			var response *http.Response
			response, err = c.client.Do(request)
			if err == nil {
				record.StatusCode = response.StatusCode
				if response.StatusCode == http.StatusCreated || response.StatusCode == http.StatusOK {
					var submitted api.SubmitJobResponse
					err = decodeResponseJSON(response, &submitted)
					if err == nil {
						record.JobID = submitted.Job.ID
						record.AdmittedAt = submitted.Job.CreatedAt.UTC()
						record.Duplicate = submitted.Duplicate
						record.ResponseReceivedAt = time.Now().UTC()
						cancel()
						if record.JobID == "" || record.AdmittedAt.IsZero() {
							record.Error = "successful response omitted durable job identity or admission time"
						}
						return record
					}
				} else {
					var apiError api.ErrorResponse
					decodeErr := decodeResponseJSON(response, &apiError)
					if decodeErr != nil {
						err = fmt.Errorf("HTTP %d: %w", response.StatusCode, decodeErr)
					} else {
						err = fmt.Errorf("HTTP %d %s: %s", response.StatusCode, apiError.Code, apiError.Message)
					}
					if response.StatusCode != http.StatusTooManyRequests && response.StatusCode < 500 {
						cancel()
						record.ResponseReceivedAt = time.Now().UTC()
						record.Error = err.Error()
						return record
					}
				}
			} else {
				record.AmbiguousRetry = true
			}
		}
		cancel()
		lastError = err
		if attempt < maxAttempts {
			if err := waitContext(ctx, time.Duration(attempt)*100*time.Millisecond); err != nil {
				lastError = err
				break
			}
		}
	}
	record.ResponseReceivedAt = time.Now().UTC()
	if lastError == nil {
		lastError = errors.New("submission failed without an error")
	}
	record.Error = lastError.Error()
	return record
}

func (c *benchmarkClient) waitForDrain(
	ctx context.Context,
	runID string,
	submissions []evidence.SubmissionSample,
	concurrency int,
	timeout time.Duration,
	interval time.Duration,
) ([]evidence.DrainSample, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	unresolved := make(map[string]struct{}, len(submissions))
	for _, submission := range submissions {
		if submission.JobID == "" {
			return nil, fmt.Errorf("submission %d has no job ID", submission.Index)
		}
		if _, duplicate := unresolved[submission.JobID]; duplicate {
			return nil, fmt.Errorf("duplicate submitted job ID %s", submission.JobID)
		}
		unresolved[submission.JobID] = struct{}{}
	}
	samples := make(map[string]evidence.DrainSample, len(unresolved))
	var lastErrors int

	for len(unresolved) > 0 {
		jobIDs := make([]string, 0, len(unresolved))
		for jobID := range unresolved {
			jobIDs = append(jobIDs, jobID)
		}
		slices.Sort(jobIDs)
		results := c.pollJobs(deadlineCtx, jobIDs, concurrency)
		lastErrors = 0
		for jobID, result := range results {
			if result.err != nil {
				lastErrors++
				continue
			}
			if result.job.State.Terminal() {
				terminalAt := result.job.TerminalAt
				samples[jobID] = evidence.DrainSample{
					SchemaVersion: evidence.SchemaVersion,
					RunID:         runID,
					JobID:         jobID,
					State:         string(result.job.State),
					ObservedAt:    result.observedAt,
					TerminalAt:    terminalAt,
				}
				delete(unresolved, jobID)
			}
		}
		if len(unresolved) == 0 {
			break
		}
		if err := waitContext(deadlineCtx, interval); err != nil {
			break
		}
	}

	ordered := make([]evidence.DrainSample, 0, len(samples))
	for _, sample := range samples {
		ordered = append(ordered, sample)
	}
	slices.SortFunc(ordered, func(left, right evidence.DrainSample) int {
		return strings.Compare(left.JobID, right.JobID)
	})
	if len(unresolved) > 0 {
		return ordered, fmt.Errorf(
			"drain deadline reached with %d unresolved jobs and %d errors in the last poll: %w",
			len(unresolved),
			lastErrors,
			deadlineCtx.Err(),
		)
	}
	return ordered, nil
}

func (c *benchmarkClient) pollJobs(
	ctx context.Context,
	jobIDs []string,
	concurrency int,
) map[string]jobPollResult {
	results := make(map[string]jobPollResult, len(jobIDs))
	var resultsMu sync.Mutex
	work := make(chan string)
	var workers sync.WaitGroup
	for range min(concurrency, len(jobIDs)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for jobID := range work {
				result := c.getJob(ctx, jobID)
				resultsMu.Lock()
				results[jobID] = result
				resultsMu.Unlock()
			}
		}()
	}
	for _, jobID := range jobIDs {
		select {
		case work <- jobID:
		case <-ctx.Done():
			close(work)
			workers.Wait()
			return results
		}
	}
	close(work)
	workers.Wait()
	return results
}

func (c *benchmarkClient) getJob(ctx context.Context, jobID string) jobPollResult {
	requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodGet,
		c.endpoint("/v1/jobs/"+url.PathEscape(jobID)),
		nil,
	)
	if err != nil {
		return jobPollResult{err: err}
	}
	response, err := c.client.Do(request)
	if err != nil {
		return jobPollResult{err: err}
	}
	if response.StatusCode != http.StatusOK {
		var apiError api.ErrorResponse
		decodeErr := decodeResponseJSON(response, &apiError)
		if decodeErr != nil {
			return jobPollResult{err: fmt.Errorf("HTTP %d: %w", response.StatusCode, decodeErr)}
		}
		return jobPollResult{err: fmt.Errorf("HTTP %d %s: %s", response.StatusCode, apiError.Code, apiError.Message)}
	}
	var job domain.Job
	if err := decodeResponseJSON(response, &job); err != nil {
		return jobPollResult{err: err}
	}
	return jobPollResult{job: job, observedAt: time.Now().UTC()}
}

func decodeResponseJSON(response *http.Response, target any) error {
	defer func() {
		_ = response.Body.Close()
	}()
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func fetchMetrics(ctx context.Context, rawURL string, timeout time.Duration) ([]byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch metrics: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch metrics: HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	return body, nil
}

func finalizeFailedRun(
	output string,
	manifest *evidence.RunManifest,
	submissions []evidence.SubmissionSample,
	drainSamples []evidence.DrainSample,
	runErr error,
) error {
	finishedAt := time.Now().UTC()
	manifest.WorkloadFinishedAt = &finishedAt
	manifest.FinalizedAt = &finishedAt
	manifest.Status = evidence.StatusInvalid
	manifest.InvalidReasons = append(manifest.InvalidReasons, runErr.Error())
	if err := writeRunArtifacts(output, *manifest, submissions, drainSamples); err != nil {
		return errors.Join(runErr, err)
	}
	return runErr
}

func writeRunArtifacts(
	output string,
	manifest evidence.RunManifest,
	submissions []evidence.SubmissionSample,
	drainSamples []evidence.DrainSample,
) error {
	if submissions == nil {
		submissions = []evidence.SubmissionSample{}
	}
	if drainSamples == nil {
		drainSamples = []evidence.DrainSample{}
	}
	if err := evidence.WriteJSONLines(filepath.Join(output, "submitted.jsonl"), submissions); err != nil {
		return fmt.Errorf("write submitted samples: %w", err)
	}
	if err := evidence.WriteJSONLines(filepath.Join(output, "drain-samples.jsonl"), drainSamples); err != nil {
		return fmt.Errorf("write drain samples: %w", err)
	}
	if err := evidence.WriteJSON(filepath.Join(output, "manifest.json"), manifest); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := evidence.GenerateChecksums(output); err != nil {
		return fmt.Errorf("write checksums: %w", err)
	}
	return nil
}

func loadEnvironment(path string) (evidence.EnvironmentManifest, error) {
	var environment evidence.EnvironmentManifest
	if path != "" {
		if err := evidence.ReadJSON(path, &environment); err != nil {
			return environment, fmt.Errorf("read environment manifest: %w", err)
		}
	}
	if environment.GoVersion == "" {
		environment.GoVersion = runtime.Version()
	}
	if environment.OS == "" {
		environment.OS = runtime.GOOS
	}
	if environment.Architecture == "" {
		environment.Architecture = runtime.GOARCH
	}
	if environment.CPUCount == 0 {
		environment.CPUCount = runtime.NumCPU()
	}
	if environment.Hostname == "" {
		environment.Hostname, _ = os.Hostname()
	}
	if environment.Timezone == "" {
		zone, offset := time.Now().Zone()
		environment.Timezone = zone + " UTC" + formatOffset(offset)
	}
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range buildInfo.Settings {
			switch setting.Key {
			case "vcs.revision":
				if environment.GitCommit == "" {
					environment.GitCommit = setting.Value
				}
			case "vcs.modified":
				if environment.GitDirty == nil {
					value, err := strconv.ParseBool(setting.Value)
					if err == nil {
						environment.GitDirty = &value
					}
				}
			}
		}
	}
	environment.Unavailable = unavailableEnvironmentFields(environment)
	return environment, nil
}

func unavailableEnvironmentFields(environment evidence.EnvironmentManifest) []string {
	var unavailable []string
	text := map[string]string{
		"git_commit":      environment.GitCommit,
		"docker_version":  environment.DockerVersion,
		"compose_version": environment.ComposeVersion,
		"kernel":          environment.Kernel,
		"cpu_model":       environment.CPUModel,
		"filesystem":      environment.Filesystem,
		"cgroup_limits":   environment.CgroupLimits,
	}
	for name, value := range text {
		if value == "" {
			unavailable = append(unavailable, name)
		}
	}
	if environment.GitDirty == nil {
		unavailable = append(unavailable, "git_dirty")
	}
	if environment.MemoryBytes == 0 {
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
	unavailable = append(unavailable, environment.Unavailable...)
	slices.Sort(unavailable)
	return slices.Compact(unavailable)
}

func validRunID(value string) bool {
	if len(value) < 1 || len(value) > 96 {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '.' ||
			character == '_' ||
			character == '-' {
			continue
		}
		return false
	}
	return true
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func formatOffset(offsetSeconds int) string {
	sign := "+"
	if offsetSeconds < 0 {
		sign = "-"
		offsetSeconds = -offsetSeconds
	}
	hours := offsetSeconds / 3600
	minutes := (offsetSeconds % 3600) / 60
	return fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
}
