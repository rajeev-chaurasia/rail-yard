package p5

import "time"

type Payload struct {
	Type       string   `json:"type"`
	DurationMS int64    `json:"duration_ms,omitempty"`
	Args       []string `json:"args,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts int  `json:"max_attempts"`
	Retryable   bool `json:"retryable"`
}

type JobSpec struct {
	Name      string      `json:"name,omitempty"`
	TenantID  string      `json:"tenant_id"`
	Queue     string      `json:"queue"`
	Priority  int         `json:"priority"`
	SlotCost  int         `json:"slot_cost"`
	Payload   Payload     `json:"payload"`
	Retry     RetryPolicy `json:"retry"`
	DependsOn []string    `json:"depends_on,omitempty"`
}

type WorkflowNode struct {
	Key string  `json:"key"`
	Job JobSpec `json:"job"`
}

type WorkflowRequest struct {
	TenantID string         `json:"tenant_id"`
	Nodes    []WorkflowNode `json:"nodes"`
}

type WorkflowResponse struct {
	DAGID     string `json:"dag_id"`
	Jobs      []Job  `json:"jobs"`
	Duplicate bool   `json:"duplicate"`
}

type SubmitJobRequest struct {
	Job JobSpec `json:"job"`
}

type SubmitJobResponse struct {
	Job       Job  `json:"job"`
	Duplicate bool `json:"duplicate"`
}

type Job struct {
	ID              string      `json:"id"`
	State           string      `json:"state"`
	AttemptNo       int         `json:"attempt_no"`
	StateVersion    int64       `json:"state_version"`
	LeaseGeneration int64       `json:"lease_generation"`
	CreatedAt       time.Time   `json:"created_at"`
	UpdatedAt       time.Time   `json:"updated_at"`
	TerminalAt      *time.Time  `json:"terminal_at,omitempty"`
	Failure         *JobFailure `json:"failure,omitempty"`
}

type JobFailure struct {
	Class        string `json:"class"`
	Message      string `json:"message"`
	ExitCode     int    `json:"exit_code,omitempty"`
	OutputDigest string `json:"output_digest,omitempty"`
	Stderr       string `json:"stderr,omitempty"`
}

type ActionReceipt struct {
	JobID        string    `json:"job_id"`
	Action       string    `json:"action"`
	State        string    `json:"state"`
	StateVersion int64     `json:"state_version"`
	Actor        string    `json:"actor"`
	CommittedAt  time.Time `json:"committed_at"`
	Duplicate    bool      `json:"duplicate"`
}

type DeadLetter struct {
	JobID         string     `json:"job_id"`
	Failure       JobFailure `json:"failure"`
	CreatedAt     time.Time  `json:"created_at"`
	RedrivenJobID string     `json:"redriven_job_id,omitempty"`
}

type DeadLetterList struct {
	DeadLetters []DeadLetter `json:"dead_letters"`
}

type RedriveResponse struct {
	Job       Job  `json:"job"`
	Duplicate bool `json:"duplicate"`
}

type ForceJobRequest struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type WorkerHealth struct {
	WorkerID        string    `json:"worker_id"`
	Status          string    `json:"status"`
	ActiveLeases    int       `json:"active_leases"`
	LastHeartbeatAt time.Time `json:"last_heartbeat_at"`
}

type WorkerHealthResponse struct {
	Workers []WorkerHealth `json:"workers"`
}

type OperatorActionRequest struct {
	Action     string            `json:"action"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Details    map[string]string `json:"details,omitempty"`
}

type AuditEvent struct {
	ID         string            `json:"id"`
	Action     string            `json:"action"`
	Actor      string            `json:"actor"`
	OccurredAt time.Time         `json:"occurred_at"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Details    map[string]string `json:"details,omitempty"`
}

type OperatorActionResponse struct {
	Event AuditEvent `json:"event"`
}

type AuditEventList struct {
	Events []AuditEvent `json:"events"`
}

type prometheusAlertsResponse struct {
	Status string `json:"status"`
	Data   struct {
		Alerts []prometheusAlert `json:"alerts"`
	} `json:"data"`
}

type prometheusAlert struct {
	Labels map[string]string `json:"labels"`
	State  string            `json:"state"`
}

type prometheusRulesResponse struct {
	Status string `json:"status"`
	Data   struct {
		Groups []struct {
			Rules []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"rules"`
		} `json:"groups"`
	} `json:"data"`
}
