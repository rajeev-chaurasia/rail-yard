package operations

import (
	"encoding/json"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type SubmitJobCommand struct {
	Request        api.SubmitJobRequest
	Actor          string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type SubmitDAGCommand struct {
	Request        api.SubmitWorkflowRequest
	Actor          string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type SubmitDAGResponse struct {
	DAGID     string       `json:"dag_id"`
	Jobs      []domain.Job `json:"jobs"`
	Duplicate bool         `json:"duplicate"`
}

type HistoryQuery struct {
	Limit     int
	BeforeSeq int64
}

type JobEvent struct {
	Sequence     int64           `json:"sequence"`
	Type         string          `json:"type"`
	State        domain.JobState `json:"state"`
	StateVersion int64           `json:"state_version"`
	OccurredAt   time.Time       `json:"occurred_at"`
	Actor        string          `json:"actor"`
	Payload      json.RawMessage `json:"payload"`
}

type JobHistoryPage struct {
	Events        []JobEvent `json:"events"`
	NextBeforeSeq int64      `json:"next_before_seq,omitempty"`
}

type CancelJobCommand struct {
	JobID          string
	Actor          string
	Reason         string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type RedriveCommand struct {
	JobID          string
	Actor          string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type ForceAction string

const (
	ForceRelease    ForceAction = "release"
	ForceFail       ForceAction = "fail"
	ForceDeadLetter ForceAction = "dead_letter"
)

func (a ForceAction) Valid() bool {
	return a == ForceRelease || a == ForceFail || a == ForceDeadLetter
}

type ForceJobCommand struct {
	JobID          string
	Action         ForceAction
	Actor          string
	Reason         string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type ActionReceipt struct {
	JobID        string          `json:"job_id"`
	Action       string          `json:"action"`
	State        domain.JobState `json:"state"`
	StateVersion int64           `json:"state_version"`
	Actor        string          `json:"actor"`
	CommittedAt  time.Time       `json:"committed_at"`
	Duplicate    bool            `json:"duplicate"`
}

type QueueDepth struct {
	TenantID         string `json:"tenant_id"`
	Queue            string `json:"queue"`
	Pending          int64  `json:"pending"`
	Scheduled        int64  `json:"scheduled"`
	Running          int64  `json:"running"`
	Retrying         int64  `json:"retrying"`
	Active           int64  `json:"active"`
	MaxDepth         int64  `json:"max_depth"`
	ActiveSlots      int64  `json:"active_slots"`
	MaxSlots         int64  `json:"max_slots"`
	OldestReadyAgeMS int64  `json:"oldest_ready_age_ms"`
}

type QueueDepthResponse struct {
	Queues []QueueDepth `json:"queues"`
}

type WorkerStatus string

const (
	WorkerHealthy WorkerStatus = "healthy"
	WorkerStale   WorkerStatus = "stale"
	WorkerOffline WorkerStatus = "offline"
)

type WorkerHealth struct {
	WorkerID          string       `json:"worker_id"`
	Status            WorkerStatus `json:"status"`
	CapacitySlots     int          `json:"capacity_slots"`
	ActiveSlots       int          `json:"active_slots"`
	ActiveLeases      int          `json:"active_leases"`
	LastHeartbeatAt   time.Time    `json:"last_heartbeat_at"`
	HeartbeatAgeMS    int64        `json:"heartbeat_age_ms"`
	OldestLeaseAgeMS  int64        `json:"oldest_lease_age_ms"`
	OldestLeaseExpiry *time.Time   `json:"oldest_lease_expiry,omitempty"`
}

type WorkerHealthResponse struct {
	Workers []WorkerHealth `json:"workers"`
}

type DAGEdge struct {
	FromJobID string `json:"from_job_id"`
	ToJobID   string `json:"to_job_id"`
}

type DAGDetail struct {
	ID        string       `json:"id"`
	TenantID  string       `json:"tenant_id"`
	Jobs      []domain.Job `json:"jobs"`
	Edges     []DAGEdge    `json:"edges"`
	CreatedAt time.Time    `json:"created_at"`
	UpdatedAt time.Time    `json:"updated_at"`
}

type CancelJobRequest struct {
	Reason string `json:"reason"`
}

type ForceJobRequest struct {
	Action ForceAction `json:"action"`
	Reason string      `json:"reason"`
}

type OperatorActionRequest struct {
	TenantID   string            `json:"tenant_id,omitempty"`
	Action     string            `json:"action"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Details    map[string]string `json:"details,omitempty"`
}

type OperatorActionCommand struct {
	Request        OperatorActionRequest
	Actor          string
	IdempotencyKey string
	RequestDigest  string
	RequestedAt    time.Time
}

type AuditEvent struct {
	ID         string            `json:"id"`
	TenantID   string            `json:"tenant_id"`
	Action     string            `json:"action"`
	Actor      string            `json:"actor"`
	OccurredAt time.Time         `json:"occurred_at"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Details    map[string]string `json:"details,omitempty"`
}

type OperatorActionResponse struct {
	Event     AuditEvent `json:"event"`
	Duplicate bool       `json:"duplicate"`
}

type AuditEventQuery struct {
	Since time.Time
	Actor string
}

type AuditEventResponse struct {
	Events []AuditEvent `json:"events"`
}
