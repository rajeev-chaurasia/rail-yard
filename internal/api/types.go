package api

import (
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

const (
	MaxAttemptStartBatchSize = 128
	MaxCompletionBatchSize   = 128
)

type SubmitJobRequest struct {
	Job domain.JobSpec `json:"job"`
}

type SubmitJobResponse struct {
	Job       domain.Job `json:"job"`
	Duplicate bool       `json:"duplicate"`
}

type WorkflowNode struct {
	Key string         `json:"key"`
	Job domain.JobSpec `json:"job"`
}

type SubmitWorkflowRequest struct {
	TenantID string         `json:"tenant_id"`
	Nodes    []WorkflowNode `json:"nodes"`
}

type SubmitWorkflowResponse struct {
	Jobs      []domain.Job `json:"jobs"`
	Duplicate bool         `json:"duplicate"`
}

type CreateCronTriggerRequest struct {
	TenantID   string         `json:"tenant_id"`
	Expression string         `json:"expression"`
	Job        domain.JobSpec `json:"job"`
}

type CreateCronTriggerResponse struct {
	Trigger   domain.CronTrigger `json:"trigger"`
	Duplicate bool               `json:"duplicate"`
}

type DeadLetterListResponse struct {
	DeadLetters []domain.DeadLetter `json:"dead_letters"`
}

type RedriveDeadLetterResponse struct {
	Job       domain.Job `json:"job"`
	Duplicate bool       `json:"duplicate"`
}

type RegisterWorkerRequest struct {
	WorkerID string `json:"worker_id"`
	Slots    int    `json:"slots"`
}

type RegisterWorkerResponse struct {
	WorkerID       string        `json:"worker_id"`
	HeartbeatEvery time.Duration `json:"heartbeat_every"`
	LeaseTTL       time.Duration `json:"lease_ttl"`
	ServerTime     time.Time     `json:"server_time"`
}

type AcquireLeasesRequest struct {
	AvailableSlots int `json:"available_slots"`
	Limit          int `json:"limit"`
}

type AcquireLeasesResponse struct {
	Leases []domain.Lease `json:"leases"`
}

type HeartbeatRequest struct {
	Leases []domain.LeaseRef `json:"leases"`
}

type HeartbeatResult struct {
	JobID     string    `json:"job_id"`
	Accepted  bool      `json:"accepted"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

type HeartbeatResponse struct {
	Results []HeartbeatResult `json:"results"`
}

type StartAttemptRequest struct {
	domain.LeaseRef
}

type StartAttemptsRequest struct {
	Leases []domain.LeaseRef `json:"leases"`
}

type StartResult struct {
	JobID   string         `json:"job_id"`
	Started bool           `json:"started"`
	Error   *ErrorResponse `json:"error,omitempty"`
}

type StartAttemptsResponse struct {
	Results []StartResult `json:"results"`
}

type CompleteAttemptRequest struct {
	domain.Completion
}

type CompleteAttemptsRequest struct {
	Completions []domain.Completion `json:"completions"`
}

type CompletionResult struct {
	JobID   string                    `json:"job_id"`
	Receipt *domain.CompletionReceipt `json:"receipt,omitempty"`
	Error   *ErrorResponse            `json:"error,omitempty"`
}

type CompleteAttemptsResponse struct {
	Results []CompletionResult `json:"results"`
}

type ErrorResponse struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	RetryAfter int    `json:"retry_after_seconds,omitempty"`
}
