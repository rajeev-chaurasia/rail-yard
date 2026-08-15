package p5

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

const (
	statePending    = "PENDING"
	stateScheduled  = "SCHEDULED"
	stateRunning    = "RUNNING"
	stateSucceeded  = "SUCCEEDED"
	stateDeadLetter = "DEAD_LETTER"

	readyStartSLOAlert = "RailYardReadyStartSLOBreach"
	dlqDepthHighAlert  = "RailYardDLQDepthHigh"

	readyStartBreachJobs   = 20
	readyStartRecoveryJobs = 2400
	readyStartBatchSize    = 100
	dlqDepthBreachJobs     = 10
)

type Config struct {
	BaseURL            string
	PrometheusURL      string
	Actor              string
	RunID              string
	RepositoryRoot     string
	ComposeFile        string
	ComposeProject     string
	RequestTimeout     time.Duration
	PollInterval       time.Duration
	OperationTimeout   time.Duration
	AlertFireTimeout   time.Duration
	AlertClearTimeout  time.Duration
	RecoveryHold       time.Duration
	ReadyBreachHold    time.Duration
	SkipLiveAlertWaits bool
	SLORuleEvidence    string
}

func DefaultConfig() Config {
	return Config{
		BaseURL:           "http://127.0.0.1:8080",
		PrometheusURL:     "http://127.0.0.1:9090",
		Actor:             "p5-qa",
		RunID:             time.Now().UTC().Format("20060102T150405Z"),
		RepositoryRoot:    ".",
		ComposeFile:       "deploy/compose.yaml",
		ComposeProject:    "railyard-p5",
		RequestTimeout:    15 * time.Second,
		PollInterval:      250 * time.Millisecond,
		OperationTimeout:  2 * time.Minute,
		AlertFireTimeout:  12 * time.Minute,
		AlertClearTimeout: 3 * time.Minute,
		RecoveryHold:      9 * time.Second,
		ReadyBreachHold:   6 * time.Second,
		SLORuleEvidence:   "slo-summary.json",
	}
}

type Report struct {
	RunID                    string        `json:"run_id"`
	Actor                    string        `json:"actor"`
	StartedAt                time.Time     `json:"started_at"`
	CompletedAt              time.Time     `json:"completed_at"`
	WorkflowJobIDs           []string      `json:"workflow_job_ids"`
	ReassignedJobID          string        `json:"reassigned_job_id"`
	ReassignmentObservedIn   time.Duration `json:"reassignment_observed_ns"`
	DeadLetterJobID          string        `json:"dead_letter_job_id"`
	RedrivenJobID            string        `json:"redriven_job_id"`
	RecoveryAlertFiredAt     time.Time     `json:"recovery_alert_fired_at"`
	RecoveryAlertRecoveredAt time.Time     `json:"recovery_alert_recovered_at"`
	QueueAlertFiredAt        time.Time     `json:"queue_alert_fired_at"`
	QueueAlertRecoveredAt    time.Time     `json:"queue_alert_recovered_at"`
	AuditEventCount          int           `json:"audit_event_count"`
	LiveAlertWaitsSkipped    bool          `json:"live_alert_waits_skipped"`
	SLORuleEvidence          string        `json:"slo_rule_evidence"`
	Passed                   bool          `json:"passed"`
}

type Runner struct {
	config  Config
	client  *Client
	compose Compose
	logf    func(string, ...any)
	keySeq  int
}

func NewRunner(config Config, logf func(string, ...any)) (*Runner, error) {
	if config.RunID == "" {
		return nil, fmt.Errorf("run ID is required")
	}
	if config.PollInterval <= 0 ||
		config.OperationTimeout <= 0 ||
		config.AlertFireTimeout <= 0 ||
		config.AlertClearTimeout <= 0 ||
		config.RecoveryHold <= 0 ||
		config.ReadyBreachHold <= 0 {
		return nil, fmt.Errorf("all walkthrough durations must be positive")
	}
	if config.SLORuleEvidence == "" {
		return nil, fmt.Errorf("SLO rule evidence path is required")
	}
	client, err := NewClient(
		config.BaseURL,
		config.PrometheusURL,
		config.Actor,
		config.RequestTimeout,
	)
	if err != nil {
		return nil, err
	}
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Runner{
		config: config,
		client: client,
		compose: Compose{
			WorkingDirectory: config.RepositoryRoot,
			File:             config.ComposeFile,
			Project:          config.ComposeProject,
		},
		logf: logf,
	}, nil
}

func (r *Runner) Run(ctx context.Context) (report Report, runErr error) {
	report = Report{
		RunID:                 r.config.RunID,
		Actor:                 r.config.Actor,
		StartedAt:             time.Now().UTC(),
		LiveAlertWaitsSkipped: r.config.SkipLiveAlertWaits,
		SLORuleEvidence:       r.config.SLORuleEvidence,
	}
	defer func() {
		restoreContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := r.compose.StartAllWorkers(restoreContext); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("restore worker pool: %w", err))
		}
	}()

	r.logf("preflight: Rail Yard, Prometheus, and P5 audit feed")
	if err := r.client.Ready(ctx); err != nil {
		return report, fmt.Errorf("rail yard readiness preflight: %w", err)
	}
	alertRules, err := r.client.AlertRules(ctx)
	if err != nil {
		return report, fmt.Errorf("prometheus alert rules preflight: %w", err)
	}
	for _, alertName := range []string{readyStartSLOAlert, dlqDepthHighAlert} {
		if !alertRules[alertName] {
			return report, fmt.Errorf("prometheus did not load required alert %s", alertName)
		}
		state, stateErr := r.client.AlertState(ctx, alertName)
		if stateErr != nil {
			return report, fmt.Errorf("read initial state for %s: %w", alertName, stateErr)
		}
		if state != "inactive" {
			return report, fmt.Errorf(
				"required alert %s starts in %s state, want inactive",
				alertName,
				state,
			)
		}
	}
	if _, err := r.client.Workers(ctx); err != nil {
		return report, err
	}
	if _, err := r.client.ListDeadLetters(ctx); err != nil {
		return report, err
	}
	if _, err := r.client.AuditEvents(ctx, report.StartedAt); err != nil {
		return report, err
	}
	if err := r.compose.StartAllWorkers(ctx); err != nil {
		return report, fmt.Errorf("start worker pool: %w", err)
	}

	r.logf("DAG: submit and observe dependency execution")
	workflowIDs, err := r.exerciseDAG(ctx)
	if err != nil {
		return report, err
	}
	report.WorkflowJobIDs = workflowIDs

	r.logf("reassignment: kill worker-1 and observe successor lease")
	reassignedID, recovery, err := r.exerciseReassignment(ctx)
	if err != nil {
		return report, err
	}
	report.ReassignedJobID = reassignedID
	report.ReassignmentObservedIn = recovery

	r.logf("DLQ: force failure, inspect dead letter, and redrive")
	deadLetterID, redrivenID, err := r.exerciseDeadLetter(ctx)
	if err != nil {
		return report, err
	}
	report.DeadLetterJobID = deadLetterID
	report.RedrivenJobID = redrivenID

	if r.config.SkipLiveAlertWaits {
		r.logf(
			"alerts: live waits skipped; deterministic rule evidence: %s",
			r.config.SLORuleEvidence,
		)
	} else {
		r.logf("alerts: exercise ready-start and DLQ breach lifecycles")
		report.RecoveryAlertFiredAt,
			report.RecoveryAlertRecoveredAt,
			report.QueueAlertFiredAt,
			report.QueueAlertRecoveredAt,
			err = r.exerciseSLOAlerts(ctx)
		if err != nil {
			return report, err
		}
	}

	auditEvents, err := r.verifyAudit(ctx, report.StartedAt)
	if err != nil {
		return report, err
	}
	report.AuditEventCount = auditEvents
	report.CompletedAt = time.Now().UTC()
	report.Passed = true
	return report, nil
}

func (r *Runner) exerciseDAG(ctx context.Context) ([]string, error) {
	key := r.nextKey("workflow")
	response, err := r.client.SubmitWorkflow(ctx, key, WorkflowRequest{
		TenantID: "p5",
		Nodes: []WorkflowNode{
			{
				Key: "00-root",
				Job: r.noopJob("p5-dag-root", 2*time.Second, nil, 3),
			},
			{
				Key: "10-middle",
				Job: r.noopJob("p5-dag-middle", time.Second, []string{"00-root"}, 3),
			},
			{
				Key: "20-leaf",
				Job: r.noopJob("p5-dag-leaf", time.Second, []string{"10-middle"}, 3),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("submit DAG: %w", err)
	}
	if response.Duplicate || response.DAGID == "" || len(response.Jobs) != 3 {
		return nil, fmt.Errorf(
			"submit DAG returned dag_id=%q duplicate=%t jobs=%d, want an ID, false, and 3",
			response.DAGID,
			response.Duplicate,
			len(response.Jobs),
		)
	}

	jobIDs := make([]string, len(response.Jobs))
	observedRunning := make(map[string]bool, len(response.Jobs))
	terminal := make(map[string]Job, len(response.Jobs))
	for index, job := range response.Jobs {
		jobIDs[index] = job.ID
	}
	err = r.wait(ctx, r.config.OperationTimeout, "DAG completion", func(ctx context.Context) (bool, error) {
		for _, jobID := range jobIDs {
			job, getErr := r.client.GetJob(ctx, jobID)
			if getErr != nil {
				return false, getErr
			}
			if job.State == stateRunning {
				observedRunning[jobID] = true
			}
			if terminalState(job.State) {
				if job.State != stateSucceeded {
					return false, fmt.Errorf("DAG job %s ended as %s", job.ID, job.State)
				}
				terminal[jobID] = job
			}
		}
		return len(terminal) == len(jobIDs), nil
	})
	if err != nil {
		return nil, err
	}
	for _, jobID := range jobIDs {
		if !observedRunning[jobID] {
			return nil, fmt.Errorf("DAG job %s reached terminal state without RUNNING being observable", jobID)
		}
	}
	rootDone := terminal[jobIDs[0]].TerminalAt
	middleDone := terminal[jobIDs[1]].TerminalAt
	leafDone := terminal[jobIDs[2]].TerminalAt
	if rootDone == nil || middleDone == nil || leafDone == nil ||
		middleDone.Before(*rootDone) || leafDone.Before(*middleDone) {
		return nil, fmt.Errorf("DAG terminal timestamps do not preserve dependency order")
	}
	return jobIDs, nil
}

func (r *Runner) exerciseReassignment(
	ctx context.Context,
) (string, time.Duration, error) {
	if err := r.compose.KeepOnlyWorker(ctx, "worker-1"); err != nil {
		return "", 0, fmt.Errorf("isolate worker-1: %w", err)
	}
	response, err := r.client.SubmitJob(ctx, r.nextKey("reassign-job"), SubmitJobRequest{
		Job: r.noopJob("p5-reassignment", 30*time.Second, nil, 3),
	})
	if err != nil {
		return "", 0, fmt.Errorf("submit reassignment job: %w", err)
	}
	jobID := response.Job.ID
	if err := r.waitForJob(ctx, jobID, r.config.OperationTimeout, func(job Job) bool {
		return job.State == stateRunning && job.AttemptNo == 1
	}); err != nil {
		return "", 0, fmt.Errorf("wait for worker-1 attempt: %w", err)
	}

	if err := r.compose.Kill(ctx, "worker-1"); err != nil {
		return "", 0, fmt.Errorf("SIGKILL worker-1: %w", err)
	}
	killedAt := time.Now().UTC()
	if err := r.compose.Stop(ctx, "worker-1"); err != nil {
		return "", 0, fmt.Errorf("disable worker-1 restart: %w", err)
	}
	if _, err := r.client.RecordOperatorAction(
		ctx,
		r.nextKey("worker-kill"),
		OperatorActionRequest{
			Action:     "worker.kill",
			TargetType: "worker",
			TargetID:   "worker-1",
			Details: map[string]string{
				"signal":       "SIGKILL",
				"confirmed_at": killedAt.Format(time.RFC3339Nano),
			},
		},
	); err != nil {
		return "", 0, err
	}
	if err := waitContext(ctx, r.config.RecoveryHold); err != nil {
		return "", 0, err
	}
	if err := r.compose.Start(ctx, "worker-2"); err != nil {
		return "", 0, fmt.Errorf("start successor worker: %w", err)
	}

	var successorObservedAt time.Time
	if err := r.waitForJob(ctx, jobID, r.config.OperationTimeout, func(job Job) bool {
		if job.AttemptNo >= 2 &&
			slices.Contains([]string{stateScheduled, stateRunning, stateSucceeded}, job.State) {
			successorObservedAt = time.Now().UTC()
			return true
		}
		return false
	}); err != nil {
		return "", 0, fmt.Errorf("wait for successor lease: %w", err)
	}
	if err := r.waitForJob(ctx, jobID, r.config.OperationTimeout, func(job Job) bool {
		return job.State == stateSucceeded
	}); err != nil {
		return "", 0, fmt.Errorf("wait for reassigned job completion: %w", err)
	}
	return jobID, successorObservedAt.Sub(killedAt), nil
}

func (r *Runner) exerciseDeadLetter(ctx context.Context) (string, string, error) {
	if err := r.compose.StopAllWorkers(ctx); err != nil {
		return "", "", fmt.Errorf("stop workers for forced DLQ action: %w", err)
	}
	response, err := r.client.SubmitJob(ctx, r.nextKey("dlq-job"), SubmitJobRequest{
		Job: r.noopJob("p5-forced-dead-letter", 0, nil, 3),
	})
	if err != nil {
		return "", "", fmt.Errorf("submit DLQ job: %w", err)
	}
	receipt, err := r.client.ForceDeadLetter(
		ctx,
		r.nextKey("force-dlq"),
		response.Job.ID,
		"P5 acceptance forced dead-letter drill",
	)
	if err != nil {
		return "", "", fmt.Errorf("force DLQ action: %w", err)
	}
	if receipt.State != stateDeadLetter ||
		receipt.Actor != r.config.Actor ||
		receipt.CommittedAt.IsZero() ||
		receipt.Duplicate {
		return "", "", fmt.Errorf("invalid forced DLQ receipt: %#v", receipt)
	}
	deadLetters, err := r.client.ListDeadLetters(ctx)
	if err != nil {
		return "", "", fmt.Errorf("list dead letters: %w", err)
	}
	found := false
	for _, deadLetter := range deadLetters.DeadLetters {
		if deadLetter.JobID == response.Job.ID {
			found = true
			if deadLetter.CreatedAt.IsZero() || deadLetter.Failure.Class == "" {
				return "", "", fmt.Errorf("dead letter %s lacks timestamp or failure context", response.Job.ID)
			}
		}
	}
	if !found {
		return "", "", fmt.Errorf(
			"dead letter %s is absent from GET /ops/api/dead-letters",
			response.Job.ID,
		)
	}
	redriven, err := r.client.Redrive(ctx, r.nextKey("redrive"), response.Job.ID)
	if err != nil {
		return "", "", fmt.Errorf("redrive dead letter: %w", err)
	}
	if redriven.Duplicate || redriven.Job.ID == "" || redriven.Job.ID == response.Job.ID {
		return "", "", fmt.Errorf("redrive response did not create a distinct job")
	}
	if err := r.compose.Start(ctx, "worker-1"); err != nil {
		return "", "", fmt.Errorf("start worker for redrive: %w", err)
	}
	if err := r.waitForJob(ctx, redriven.Job.ID, r.config.OperationTimeout, func(job Job) bool {
		return job.State == stateSucceeded
	}); err != nil {
		return "", "", fmt.Errorf("wait for redriven job: %w", err)
	}
	return response.Job.ID, redriven.Job.ID, nil
}

func (r *Runner) exerciseSLOAlerts(
	ctx context.Context,
) (time.Time, time.Time, time.Time, time.Time, error) {
	if err := r.compose.StopAllWorkers(ctx); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("stop workers for SLO alerts: %w", err)
	}

	dlqJobIDs, err := r.createDLQBreach(ctx)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}
	if err := r.recordAlertAction(ctx, "start", dlqDepthHighAlert, map[string]string{
		"unredriven_entries": fmt.Sprint(dlqDepthBreachJobs),
	}); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}

	readyJobIDs, err := r.submitIndependentJobs(
		ctx,
		"ready-breach",
		"p5-ready-start-breach",
		readyStartBreachJobs,
	)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("submit ready-start breach jobs: %w", err)
	}
	if err := r.recordAlertAction(ctx, "start", readyStartSLOAlert, map[string]string{
		"delayed_jobs": fmt.Sprint(readyStartBreachJobs),
		"hold":         r.config.ReadyBreachHold.String(),
	}); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}
	if err := waitContext(ctx, r.config.ReadyBreachHold); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}
	if err := r.compose.StartAllWorkers(ctx); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("start workers for ready-start observations: %w", err)
	}
	if err := r.waitForJobsSucceeded(ctx, readyJobIDs); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("complete ready-start breach jobs: %w", err)
	}

	if err := r.waitForAlert(
		ctx,
		readyStartSLOAlert,
		"firing",
		r.config.AlertFireTimeout,
	); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("%s did not fire: %w", readyStartSLOAlert, err)
	}
	readyFiredAt := time.Now().UTC()

	if err := r.createReadyStartRecovery(ctx); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}
	if err := r.waitForAlert(
		ctx,
		readyStartSLOAlert,
		"inactive",
		r.config.AlertClearTimeout,
	); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("%s did not recover: %w", readyStartSLOAlert, err)
	}
	readyRecoveredAt := time.Now().UTC()
	if err := r.recordAlertAction(ctx, "recover", readyStartSLOAlert, map[string]string{
		"good_jobs": fmt.Sprint(readyStartRecoveryJobs),
	}); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}

	if err := r.waitForAlert(
		ctx,
		dlqDepthHighAlert,
		"firing",
		r.config.AlertFireTimeout,
	); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("%s did not fire: %w", dlqDepthHighAlert, err)
	}
	dlqFiredAt := time.Now().UTC()
	if err := r.redriveDLQBreach(ctx, dlqJobIDs); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}
	if err := r.waitForAlert(
		ctx,
		dlqDepthHighAlert,
		"inactive",
		r.config.AlertClearTimeout,
	); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{},
			fmt.Errorf("%s did not recover: %w", dlqDepthHighAlert, err)
	}
	dlqRecoveredAt := time.Now().UTC()
	if err := r.recordAlertAction(ctx, "recover", dlqDepthHighAlert, nil); err != nil {
		return time.Time{}, time.Time{}, time.Time{}, time.Time{}, err
	}
	return readyFiredAt, readyRecoveredAt, dlqFiredAt, dlqRecoveredAt, nil
}

func (r *Runner) createDLQBreach(ctx context.Context) ([]string, error) {
	current, err := r.client.ListDeadLetters(ctx)
	if err != nil {
		return nil, err
	}
	if len(current.DeadLetters) != 0 {
		return nil, fmt.Errorf(
			"DLQ alert drill requires zero current dead letters, found %d",
			len(current.DeadLetters),
		)
	}
	jobIDs := make([]string, 0, dlqDepthBreachJobs)
	for index := 0; index < dlqDepthBreachJobs; index++ {
		response, submitErr := r.client.SubmitJob(
			ctx,
			r.nextKey("dlq-breach-job"),
			SubmitJobRequest{
				Job: r.noopJob(fmt.Sprintf("p5-dlq-breach-%02d", index), 0, nil, 3),
			},
		)
		if submitErr != nil {
			return nil, fmt.Errorf("submit DLQ breach job %d: %w", index, submitErr)
		}
		receipt, forceErr := r.client.ForceDeadLetter(
			ctx,
			r.nextKey("dlq-breach-force"),
			response.Job.ID,
			"P5 DLQ depth alert drill",
		)
		if forceErr != nil {
			return nil, fmt.Errorf("force DLQ breach job %d: %w", index, forceErr)
		}
		if receipt.State != stateDeadLetter || receipt.Duplicate {
			return nil, fmt.Errorf("invalid DLQ breach receipt for job %s", response.Job.ID)
		}
		jobIDs = append(jobIDs, response.Job.ID)
	}
	current, err = r.client.ListDeadLetters(ctx)
	if err != nil {
		return nil, err
	}
	if len(current.DeadLetters) != dlqDepthBreachJobs {
		return nil, fmt.Errorf(
			"current DLQ depth = %d, want %d",
			len(current.DeadLetters),
			dlqDepthBreachJobs,
		)
	}
	return jobIDs, nil
}

func (r *Runner) createReadyStartRecovery(ctx context.Context) error {
	remaining := readyStartRecoveryJobs
	for batch := 0; remaining > 0; batch++ {
		count := min(remaining, readyStartBatchSize)
		jobIDs, err := r.submitIndependentJobs(
			ctx,
			fmt.Sprintf("ready-recovery-%02d", batch),
			fmt.Sprintf("p5-ready-start-recovery-%02d", batch),
			count,
		)
		if err != nil {
			return fmt.Errorf("submit ready-start recovery batch %d: %w", batch, err)
		}
		if err := r.waitForJobsSucceeded(ctx, jobIDs); err != nil {
			return fmt.Errorf("complete ready-start recovery batch %d: %w", batch, err)
		}
		remaining -= count
	}
	return nil
}

func (r *Runner) redriveDLQBreach(ctx context.Context, jobIDs []string) error {
	redrivenIDs := make([]string, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		response, err := r.client.Redrive(ctx, r.nextKey("dlq-breach-redrive"), jobID)
		if err != nil {
			return fmt.Errorf("redrive DLQ breach job %s: %w", jobID, err)
		}
		if response.Duplicate || response.Job.ID == "" || response.Job.ID == jobID {
			return fmt.Errorf("invalid redrive response for DLQ breach job %s", jobID)
		}
		redrivenIDs = append(redrivenIDs, response.Job.ID)
	}
	if err := r.waitForJobsSucceeded(ctx, redrivenIDs); err != nil {
		return fmt.Errorf("complete redriven DLQ breach jobs: %w", err)
	}
	current, err := r.client.ListDeadLetters(ctx)
	if err != nil {
		return err
	}
	if len(current.DeadLetters) != 0 {
		return fmt.Errorf("current DLQ depth after redrive = %d, want 0", len(current.DeadLetters))
	}
	return nil
}

func (r *Runner) recordAlertAction(
	ctx context.Context,
	phase string,
	alertName string,
	details map[string]string,
) error {
	_, err := r.client.RecordOperatorAction(
		ctx,
		r.nextKey("alert-"+phase),
		OperatorActionRequest{
			Action:     "alert.exercise." + phase,
			TargetType: "prometheus_alert",
			TargetID:   alertName,
			Details:    details,
		},
	)
	return err
}

func (r *Runner) submitIndependentJobs(
	ctx context.Context,
	key string,
	namePrefix string,
	count int,
) ([]string, error) {
	nodes := make([]WorkflowNode, count)
	for index := range nodes {
		nodes[index] = WorkflowNode{
			Key: fmt.Sprintf("%04d", index),
			Job: r.noopJob(
				fmt.Sprintf("%s-%04d", namePrefix, index),
				0,
				nil,
				3,
			),
		}
	}
	response, err := r.client.SubmitWorkflow(
		ctx,
		r.nextKey(key),
		WorkflowRequest{TenantID: "p5", Nodes: nodes},
	)
	if err != nil {
		return nil, err
	}
	if response.Duplicate || len(response.Jobs) != count {
		return nil, fmt.Errorf(
			"workflow returned duplicate=%t jobs=%d, want false and %d",
			response.Duplicate,
			len(response.Jobs),
			count,
		)
	}
	jobIDs := make([]string, count)
	for index, job := range response.Jobs {
		if job.ID == "" {
			return nil, fmt.Errorf("workflow job %d has an empty ID", index)
		}
		jobIDs[index] = job.ID
	}
	return jobIDs, nil
}

func (r *Runner) waitForJobsSucceeded(ctx context.Context, jobIDs []string) error {
	return r.wait(ctx, r.config.OperationTimeout, "job batch completion", func(ctx context.Context) (bool, error) {
		complete := 0
		for _, jobID := range jobIDs {
			job, err := r.client.GetJob(ctx, jobID)
			if err != nil {
				return false, err
			}
			if terminalState(job.State) && job.State != stateSucceeded {
				return false, fmt.Errorf("job %s ended as %s", jobID, job.State)
			}
			if job.State == stateSucceeded {
				complete++
			}
		}
		return complete == len(jobIDs), nil
	})
}

func (r *Runner) verifyAudit(ctx context.Context, since time.Time) (int, error) {
	response, err := r.client.AuditEvents(ctx, since)
	if err != nil {
		return 0, err
	}
	requiredCounts := requiredAuditCounts(!r.config.SkipLiveAlertWaits)
	actualCounts := make(map[string]int)
	now := time.Now().UTC().Add(r.config.RequestTimeout)
	for _, event := range response.Events {
		if event.Actor != r.config.Actor {
			return 0, fmt.Errorf("audit event %s actor = %q, want %q", event.ID, event.Actor, r.config.Actor)
		}
		if event.OccurredAt.IsZero() ||
			event.OccurredAt.Before(since) ||
			event.OccurredAt.After(now) {
			return 0, fmt.Errorf("audit event %s has invalid occurred_at %s", event.ID, event.OccurredAt)
		}
		actualCounts[event.Action]++
	}
	for action, count := range requiredCounts {
		if actualCounts[action] < count {
			return 0, fmt.Errorf(
				"audit action %q count = %d, want at least %d",
				action,
				actualCounts[action],
				count,
			)
		}
	}
	return len(response.Events), nil
}

func requiredAuditCounts(includeLiveAlerts bool) map[string]int {
	counts := map[string]int{
		"dag.submit":            1,
		"job.submit":            2,
		"worker.kill":           1,
		"job.force.dead_letter": 1,
		"dead_letter.redrive":   1,
	}
	if includeLiveAlerts {
		counts["dag.submit"] += 1 + readyStartRecoveryJobs/readyStartBatchSize
		counts["job.submit"] += dlqDepthBreachJobs
		counts["job.force.dead_letter"] += dlqDepthBreachJobs
		counts["dead_letter.redrive"] += dlqDepthBreachJobs
		counts["alert.exercise.start"] = 2
		counts["alert.exercise.recover"] = 2
	}
	return counts
}

func (r *Runner) waitForJob(
	ctx context.Context,
	jobID string,
	timeout time.Duration,
	matches func(Job) bool,
) error {
	return r.wait(ctx, timeout, "job "+jobID, func(ctx context.Context) (bool, error) {
		job, err := r.client.GetJob(ctx, jobID)
		if err != nil {
			return false, err
		}
		if terminalState(job.State) && !matches(job) {
			return false, fmt.Errorf("job %s reached unexpected terminal state %s", jobID, job.State)
		}
		return matches(job), nil
	})
}

func (r *Runner) waitForAlert(
	ctx context.Context,
	alertName string,
	state string,
	timeout time.Duration,
) error {
	return r.wait(ctx, timeout, alertName+"="+state, func(ctx context.Context) (bool, error) {
		current, err := r.client.AlertState(ctx, alertName)
		if err != nil {
			return false, err
		}
		return current == state, nil
	})
}

func (r *Runner) wait(
	ctx context.Context,
	timeout time.Duration,
	description string,
	predicate func(context.Context) (bool, error),
) error {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()
	for {
		matched, err := predicate(waitContext)
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
		select {
		case <-waitContext.Done():
			return fmt.Errorf("wait for %s: %w", description, waitContext.Err())
		case <-ticker.C:
		}
	}
}

func (r *Runner) noopJob(
	name string,
	duration time.Duration,
	dependsOn []string,
	maxAttempts int,
) JobSpec {
	return JobSpec{
		Name:      name,
		TenantID:  "p5",
		Queue:     "p5",
		SlotCost:  1,
		Payload:   Payload{Type: "noop", DurationMS: duration.Milliseconds()},
		Retry:     RetryPolicy{MaxAttempts: maxAttempts, Retryable: true},
		DependsOn: dependsOn,
	}
}

func (r *Runner) nextKey(kind string) string {
	r.keySeq++
	return fmt.Sprintf("p5-%s-%s-%03d", r.config.RunID, kind, r.keySeq)
}

func terminalState(state string) bool {
	return state == stateSucceeded || state == "FAILED" || state == stateDeadLetter
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
