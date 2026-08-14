package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type JobState string

const (
	StatePending    JobState = "PENDING"
	StateScheduled  JobState = "SCHEDULED"
	StateRunning    JobState = "RUNNING"
	StateRetrying   JobState = "RETRYING"
	StateSucceeded  JobState = "SUCCEEDED"
	StateFailed     JobState = "FAILED"
	StateDeadLetter JobState = "DEAD_LETTER"
)

func (s JobState) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateDeadLetter
}

type PayloadType string

const (
	PayloadNoop  PayloadType = "noop"
	PayloadShell PayloadType = "shell"
)

type Payload struct {
	Type       PayloadType `json:"type"`
	DurationMS int64       `json:"duration_ms,omitempty"`
	Args       []string    `json:"args,omitempty"`
}

func (p Payload) Validate(allowShell bool) error {
	switch p.Type {
	case PayloadNoop:
		if p.DurationMS < 0 {
			return errors.New("duration_ms must not be negative")
		}
	case PayloadShell:
		if !allowShell {
			return errors.New("shell jobs are disabled")
		}
		if len(p.Args) == 0 || p.Args[0] == "" {
			return errors.New("shell payload requires a non-empty argv")
		}
	default:
		return fmt.Errorf("unsupported payload type %q", p.Type)
	}
	return nil
}

type RetryPolicy struct {
	MaxAttempts int  `json:"max_attempts"`
	Retryable   bool `json:"retryable"`
}

func (p RetryPolicy) Normalized() RetryPolicy {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
		p.Retryable = true
	}
	return p
}

type JobSpec struct {
	Name        string      `json:"name,omitempty"`
	TenantID    string      `json:"tenant_id"`
	Queue       string      `json:"queue"`
	Priority    int         `json:"priority"`
	SlotCost    int         `json:"slot_cost"`
	Payload     Payload     `json:"payload"`
	Retry       RetryPolicy `json:"retry"`
	DependsOn   []string    `json:"depends_on,omitempty"`
	AvailableAt time.Time   `json:"available_at,omitempty"`
}

func (s JobSpec) Normalize() JobSpec {
	if s.TenantID == "" {
		s.TenantID = "default"
	}
	if s.Queue == "" {
		s.Queue = "default"
	}
	if s.SlotCost == 0 {
		s.SlotCost = 1
	}
	s.Retry = s.Retry.Normalized()
	return s
}

func (s JobSpec) Validate(maxSlotCost int, allowShell bool) error {
	s = s.Normalize()
	if s.SlotCost < 1 {
		return errors.New("slot_cost must be positive")
	}
	if s.SlotCost > maxSlotCost {
		return fmt.Errorf("slot_cost %d exceeds cluster maximum %d", s.SlotCost, maxSlotCost)
	}
	if s.Retry.MaxAttempts < 1 || s.Retry.MaxAttempts > 100 {
		return errors.New("max_attempts must be between 1 and 100")
	}
	return s.Payload.Validate(allowShell)
}

type Job struct {
	ID              string      `json:"id"`
	TenantID        string      `json:"tenant_id"`
	Queue           string      `json:"queue"`
	Priority        int         `json:"priority"`
	SlotCost        int         `json:"slot_cost"`
	Payload         Payload     `json:"payload"`
	Retry           RetryPolicy `json:"retry"`
	State           JobState    `json:"state"`
	AttemptNo       int         `json:"attempt_no"`
	StateVersion    int64       `json:"state_version"`
	LeaseGeneration int64       `json:"lease_generation"`
	AvailableAt     time.Time   `json:"available_at"`
	ReadySeq        int64       `json:"ready_seq"`
	CreatedAt       time.Time   `json:"created_at"`
	UpdatedAt       time.Time   `json:"updated_at"`
	TerminalAt      *time.Time  `json:"terminal_at,omitempty"`
	Failure         *Failure    `json:"failure,omitempty"`
}

type Failure struct {
	Class        string `json:"class"`
	Message      string `json:"message"`
	ExitCode     int    `json:"exit_code,omitempty"`
	OutputDigest string `json:"output_digest,omitempty"`
	Stderr       string `json:"stderr,omitempty"`
}

type Lease struct {
	JobID          string    `json:"job_id"`
	AttemptNo      int       `json:"attempt_no"`
	Generation     int64     `json:"generation"`
	Token          string    `json:"token"`
	WorkerID       string    `json:"worker_id"`
	ExpiresAt      time.Time `json:"expires_at"`
	ReadyAt        time.Time `json:"ready_at"`
	IdempotencyKey string    `json:"idempotency_key"`
	SlotCost       int       `json:"slot_cost"`
	Payload        Payload   `json:"payload"`
}

type LeaseRef struct {
	JobID      string `json:"job_id"`
	AttemptNo  int    `json:"attempt_no"`
	Generation int64  `json:"generation"`
	Token      string `json:"token"`
}

type Completion struct {
	LeaseRef
	WorkerID     string   `json:"worker_id"`
	Success      bool     `json:"success"`
	Retryable    bool     `json:"retryable"`
	OutputDigest string   `json:"output_digest"`
	Failure      *Failure `json:"failure,omitempty"`
}

type CompletionReceipt struct {
	JobID        string    `json:"job_id"`
	State        JobState  `json:"state"`
	StateVersion int64     `json:"state_version"`
	CommittedAt  time.Time `json:"committed_at"`
	Duplicate    bool      `json:"duplicate"`
}

type ReapedLease struct {
	JobID           string    `json:"job_id"`
	OldWorkerID     string    `json:"old_worker_id"`
	Generation      int64     `json:"generation"`
	ExpiredAt       time.Time `json:"expired_at"`
	NextAvailableAt time.Time `json:"next_available_at"`
}

type DeadLetter struct {
	JobID         string    `json:"job_id"`
	Failure       Failure   `json:"failure"`
	CreatedAt     time.Time `json:"created_at"`
	RedrivenJobID string    `json:"redriven_job_id,omitempty"`
}

type TriggerKind string

const (
	TriggerCron  TriggerKind = "cron"
	TriggerRedis TriggerKind = "redis"
)

func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
