package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type config struct {
	composeFile string
	project     string
	output      string
	baseURL     string
}

type result struct {
	JobID          string          `json:"job_id"`
	TerminalState  domain.JobState `json:"terminal_state"`
	Recovery       time.Duration   `json:"recovery_ns"`
	RecoveryPassed bool            `json:"recovery_passed"`
	RedisJobID     string          `json:"redis_job_id"`
	CronJobID      string          `json:"cron_job_id"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var value config
	flag.StringVar(&value.composeFile, "compose-file", "deploy/compose.yaml", "Compose file")
	flag.StringVar(&value.project, "project", "railyard-integration", "Compose project")
	flag.StringVar(&value.output, "output", "results/_work/integration", "artifact directory")
	flag.StringVar(&value.baseURL, "base-url", "http://127.0.0.1:8080", "server URL")
	flag.Parse()

	if err := os.MkdirAll(value.output, 0o750); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	defer collectAndStop(value)

	if err := compose(ctx, value, "up", "-d", "--build", "redis", "server", "worker-1"); err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if err := waitFor(ctx, 2*time.Minute, func() (bool, error) {
		response, err := client.Get(value.baseURL + "/health/ready")
		if err != nil {
			return false, nil
		}
		defer func() {
			_ = response.Body.Close()
		}()
		return response.StatusCode == http.StatusOK, nil
	}); err != nil {
		return fmt.Errorf("wait for readiness: %w", err)
	}

	redisJobID, err := verifyRedisTrigger(ctx, client, value)
	if err != nil {
		return err
	}
	cronJobID, err := verifyCronTrigger(ctx, client, value)
	if err != nil {
		return err
	}
	job, err := submitJob(ctx, client, value.baseURL, "integration-kill", 4*time.Second)
	if err != nil {
		return err
	}
	if err := waitFor(ctx, 30*time.Second, func() (bool, error) {
		current, err := getJob(ctx, client, value.baseURL, job.ID)
		return err == nil && current.State == domain.StateRunning && current.AttemptNo == 1, nil
	}); err != nil {
		return fmt.Errorf("wait for first attempt: %w", err)
	}

	killedAt := time.Now()
	if err := compose(ctx, value, "kill", "-s", "SIGKILL", "worker-1"); err != nil {
		return err
	}
	if err := compose(ctx, value, "up", "-d", "worker-1"); err != nil {
		return err
	}
	var recovery time.Duration
	if err := waitFor(ctx, 15*time.Second, func() (bool, error) {
		current, err := getJob(ctx, client, value.baseURL, job.ID)
		if err != nil {
			return false, nil
		}
		if current.AttemptNo >= 2 &&
			(current.State == domain.StateScheduled ||
				current.State == domain.StateRunning ||
				current.State == domain.StateSucceeded) {
			recovery = time.Since(killedAt)
			return true, nil
		}
		return false, nil
	}); err != nil {
		return fmt.Errorf("wait for successor lease: %w", err)
	}
	var completed domain.Job
	if err := waitFor(ctx, 30*time.Second, func() (bool, error) {
		current, err := getJob(ctx, client, value.baseURL, job.ID)
		if err != nil {
			return false, nil
		}
		completed = current
		return current.State.Terminal(), nil
	}); err != nil {
		return fmt.Errorf("wait for terminal job: %w", err)
	}
	if completed.State != domain.StateSucceeded {
		return fmt.Errorf("job completed as %s", completed.State)
	}

	summary := result{
		JobID:          job.ID,
		TerminalState:  completed.State,
		Recovery:       recovery,
		RecoveryPassed: recovery < 5*time.Second,
		RedisJobID:     redisJobID,
		CronJobID:      cronJobID,
	}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(value.output, "summary.json"), encoded, 0o640); err != nil {
		return err
	}
	if !summary.RecoveryPassed {
		return fmt.Errorf("recovery %s exceeded five seconds", recovery)
	}
	return nil
}

func verifyRedisTrigger(
	ctx context.Context,
	client *http.Client,
	value config,
) (string, error) {
	spec := domain.JobSpec{
		TenantID: "integration",
		Queue:    "redis",
		SlotCost: 1,
		Payload:  domain.Payload{Type: domain.PayloadNoop},
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	messageID, err := composeOutput(
		ctx,
		value,
		"exec",
		"-T",
		"redis",
		"redis-cli",
		"--raw",
		"XADD",
		"railyard:events",
		"*",
		"job",
		string(encoded),
	)
	if err != nil {
		return "", err
	}
	messageID = strings.TrimSpace(messageID)
	var jobID string
	query := fmt.Sprintf(
		"SELECT job_id FROM redis_deliveries WHERE message_id = '%s';",
		messageID,
	)
	if err := waitFor(ctx, 20*time.Second, func() (bool, error) {
		output, queryErr := composeOutput(
			ctx,
			value,
			"exec",
			"-T",
			"server",
			"sqlite3",
			"/var/lib/railyard/railyard.db",
			query,
		)
		if queryErr != nil {
			return false, nil
		}
		jobID = strings.TrimSpace(output)
		return jobID != "", nil
	}); err != nil {
		return "", fmt.Errorf("wait for Redis delivery: %w", err)
	}
	if err := waitForSuccess(ctx, client, value.baseURL, jobID); err != nil {
		return "", fmt.Errorf("wait for Redis job: %w", err)
	}
	return jobID, nil
}

func verifyCronTrigger(
	ctx context.Context,
	client *http.Client,
	value config,
) (string, error) {
	requestBody, err := json.Marshal(api.CreateCronTriggerRequest{
		TenantID:   "integration",
		Expression: "@every 1s",
		Job: domain.JobSpec{
			Queue:    "cron",
			SlotCost: 1,
			Payload:  domain.Payload{Type: domain.PayloadNoop},
		},
	})
	if err != nil {
		return "", err
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		value.baseURL+"/v1/triggers/cron",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "integration-cron")
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return "", fmt.Errorf("create cron trigger returned %d: %s", response.StatusCode, body)
	}
	var created api.CreateCronTriggerResponse
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		return "", err
	}
	query := fmt.Sprintf(
		"SELECT job_id FROM cron_occurrences WHERE trigger_id = '%s' ORDER BY nominal_at LIMIT 1;",
		created.Trigger.ID,
	)
	var jobID string
	if err := waitFor(ctx, 20*time.Second, func() (bool, error) {
		output, queryErr := composeOutput(
			ctx,
			value,
			"exec",
			"-T",
			"server",
			"sqlite3",
			"/var/lib/railyard/railyard.db",
			query,
		)
		if queryErr != nil {
			return false, nil
		}
		jobID = strings.TrimSpace(output)
		return jobID != "", nil
	}); err != nil {
		return "", fmt.Errorf("wait for cron occurrence: %w", err)
	}
	if err := waitForSuccess(ctx, client, value.baseURL, jobID); err != nil {
		return "", fmt.Errorf("wait for cron job: %w", err)
	}
	return jobID, nil
}

func waitForSuccess(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	jobID string,
) error {
	return waitFor(ctx, 20*time.Second, func() (bool, error) {
		job, err := getJob(ctx, client, baseURL, jobID)
		if err != nil {
			return false, nil
		}
		if job.State.Terminal() && job.State != domain.StateSucceeded {
			return false, fmt.Errorf("job %s completed as %s", job.ID, job.State)
		}
		return job.State == domain.StateSucceeded, nil
	})
}

func submitJob(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	key string,
	duration time.Duration,
) (domain.Job, error) {
	request := api.SubmitJobRequest{Job: domain.JobSpec{
		TenantID: "integration",
		Queue:    "default",
		SlotCost: 1,
		Payload: domain.Payload{
			Type:       domain.PayloadNoop,
			DurationMS: duration.Milliseconds(),
		},
	}}
	body, err := json.Marshal(request)
	if err != nil {
		return domain.Job{}, err
	}
	httpRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/v1/jobs",
		bytes.NewReader(body),
	)
	if err != nil {
		return domain.Job{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Idempotency-Key", key)
	response, err := client.Do(httpRequest)
	if err != nil {
		return domain.Job{}, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return domain.Job{}, fmt.Errorf("submit returned %d: %s", response.StatusCode, message)
	}
	var payload api.SubmitJobResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return domain.Job{}, err
	}
	return payload.Job, nil
}

func getJob(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	jobID string,
) (domain.Job, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		baseURL+"/v1/jobs/"+jobID,
		nil,
	)
	if err != nil {
		return domain.Job{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return domain.Job{}, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return domain.Job{}, fmt.Errorf("get job returned %d", response.StatusCode)
	}
	var job domain.Job
	if err := json.NewDecoder(response.Body).Decode(&job); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func waitFor(ctx context.Context, timeout time.Duration, predicate func() (bool, error)) error {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		matched, err := predicate()
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
		select {
		case <-waitContext.Done():
			return waitContext.Err()
		case <-ticker.C:
		}
	}
}

func compose(ctx context.Context, value config, args ...string) error {
	commandArgs := []string{"compose", "-f", value.composeFile, "-p", value.project}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, "docker", commandArgs...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("docker compose %v: %w", args, err)
	}
	return nil
}

func composeOutput(ctx context.Context, value config, args ...string) (string, error) {
	commandArgs := []string{"compose", "-f", value.composeFile, "-p", value.project}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, "docker", commandArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker compose %v: %w: %s", args, err, output)
	}
	return string(output), nil
}

func collectAndStop(value config) {
	logContext, cancelLogs := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelLogs()
	logPath := filepath.Join(value.output, "compose.log")
	if file, err := os.Create(logPath); err == nil {
		command := exec.CommandContext(
			logContext,
			"docker",
			"compose",
			"-f",
			value.composeFile,
			"-p",
			value.project,
			"logs",
			"--no-color",
		)
		command.Stdout = file
		command.Stderr = file
		_ = command.Run()
		_ = file.Close()
	}
	stopContext, cancelStop := context.WithTimeout(context.Background(), time.Minute)
	defer cancelStop()
	command := exec.CommandContext(
		stopContext,
		"docker",
		"compose",
		"-f",
		value.composeFile,
		"-p",
		value.project,
		"down",
		"-v",
		"--remove-orphans",
	)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	_ = command.Run()
}
