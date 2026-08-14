package dashboard

import (
	"context"
	"net/http"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type Client interface {
	Snapshot(context.Context) (Snapshot, error)
	DeadLetters(context.Context, int) ([]domain.DeadLetter, error)
	Run(context.Context, string) (Run, error)
	Operate(context.Context, Operation) (OperationResult, error)
}

type Snapshot struct {
	GeneratedAt time.Time       `json:"generated_at"`
	QueueDepths []QueueDepth    `json:"queue_depths"`
	RunningJobs []JobSummary    `json:"running_jobs"`
	FailedJobs  []JobSummary    `json:"failed_jobs"`
	Workers     []WorkerSummary `json:"workers"`
}

type QueueDepth struct {
	TenantID string          `json:"tenant_id"`
	Queue    string          `json:"queue"`
	State    domain.JobState `json:"state"`
	Depth    int             `json:"depth"`
}

type JobSummary struct {
	ID        string          `json:"id"`
	Name      string          `json:"name,omitempty"`
	TenantID  string          `json:"tenant_id"`
	Queue     string          `json:"queue"`
	State     domain.JobState `json:"state"`
	AttemptNo int             `json:"attempt_no"`
	UpdatedAt time.Time       `json:"updated_at"`
	Failure   *domain.Failure `json:"failure,omitempty"`
}

type WorkerSummary struct {
	ID                   string    `json:"id"`
	Healthy              bool      `json:"healthy"`
	Capacity             int       `json:"capacity"`
	UsedSlots            int       `json:"used_slots"`
	ActiveLeases         int       `json:"active_leases"`
	LastHeartbeatAt      time.Time `json:"last_heartbeat_at"`
	OldestLeaseStartedAt time.Time `json:"oldest_lease_started_at,omitempty"`
	NearestLeaseExpiry   time.Time `json:"nearest_lease_expiry,omitempty"`
}

type Run struct {
	ID    string    `json:"id"`
	Nodes []RunNode `json:"nodes"`
}

type RunNode struct {
	ID        string          `json:"id"`
	Name      string          `json:"name,omitempty"`
	State     domain.JobState `json:"state"`
	AttemptNo int             `json:"attempt_no"`
	DependsOn []string        `json:"depends_on"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type Action string

const (
	ActionCancel  Action = "cancel"
	ActionRetry   Action = "retry"
	ActionForce   Action = "force"
	ActionRedrive Action = "redrive"
)

type ForceAction string

const (
	ForceRelease    ForceAction = "release"
	ForceFail       ForceAction = "fail"
	ForceDeadLetter ForceAction = "dead_letter"
)

type Operation struct {
	Action      Action      `json:"action"`
	ForceAction ForceAction `json:"force_action,omitempty"`
	JobID       string      `json:"job_id"`
	Actor       string      `json:"actor"`
	Reason      string      `json:"reason,omitempty"`
	RequestID   string      `json:"request_id"`
}

type OperationResult struct {
	Action       Action `json:"action"`
	JobID        string `json:"job_id"`
	CreatedJobID string `json:"created_job_id,omitempty"`
	Message      string `json:"message"`
}

type ClientError struct {
	Status  int
	Code    string
	Message string
}

func (e *ClientError) Error() string {
	return e.Message
}

func (e *ClientError) normalized() *ClientError {
	result := *e
	if result.Status < http.StatusBadRequest || result.Status > 599 {
		result.Status = http.StatusServiceUnavailable
	}
	if result.Code == "" {
		result.Code = "backend_error"
	}
	if result.Message == "" {
		result.Message = "dashboard data is unavailable"
	}
	return &result
}
