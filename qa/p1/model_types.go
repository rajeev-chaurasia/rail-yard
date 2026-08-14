package p1

import (
	"errors"
	"time"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrStaleLease          = errors.New("lease is stale")
	ErrTerminalConflict    = errors.New("terminal completion conflicts with canonical outcome")
)

type Clock interface {
	Now() time.Time
}

type ModelState string

const (
	ModelPending    ModelState = "PENDING"
	ModelScheduled  ModelState = "SCHEDULED"
	ModelRunning    ModelState = "RUNNING"
	ModelRetrying   ModelState = "RETRYING"
	ModelSucceeded  ModelState = "SUCCEEDED"
	ModelFailed     ModelState = "FAILED"
	ModelDeadLetter ModelState = "DEAD_LETTER"
)

func (s ModelState) Terminal() bool {
	return s == ModelSucceeded || s == ModelFailed || s == ModelDeadLetter
}

type ModelSubmission struct {
	TenantID      string
	SubmissionKey string
	RequestDigest string
	SlotCost      int
	MaxAttempts   int
	Retryable     bool
	AvailableAt   time.Time
}

type ModelLeaseRef struct {
	JobID      string
	AttemptNo  int
	Generation int64
	Token      string
}

type ModelLease struct {
	ModelLeaseRef
	WorkerID  string
	SlotCost  int
	ExpiresAt time.Time
}

type ModelCompletion struct {
	ModelLeaseRef
	WorkerID     string
	Success      bool
	Retryable    bool
	OutputDigest string
}

type ModelReceipt struct {
	JobID        string
	State        ModelState
	StateVersion int64
	CommittedAt  time.Time
	Duplicate    bool
}

type ModelHeartbeat struct {
	JobID     string
	Accepted  bool
	ExpiresAt time.Time
}

type ModelReapedLease struct {
	JobID           string
	OldWorkerID     string
	Generation      int64
	ExpiredAt       time.Time
	NextAvailableAt time.Time
}

type ModelEvent struct {
	Sequence     int64
	JobID        string
	Kind         string
	From         ModelState
	To           ModelState
	StateVersion int64
	AttemptNo    int
	Generation   int64
	At           time.Time
}

type ModelAttempt struct {
	AttemptNo  int
	Generation int64
	WorkerID   string
	State      string
	ExpiresAt  time.Time
}

type ModelJob struct {
	ID               string
	TenantID         string
	SubmissionKey    string
	RequestDigest    string
	State            ModelState
	SlotCost         int
	MaxAttempts      int
	Retryable        bool
	AttemptNo        int
	LeaseGeneration  int64
	StateVersion     int64
	ReadySequence    int64
	AvailableAt      time.Time
	ActiveLease      *ModelLease
	Attempts         []ModelAttempt
	CanonicalReceipt *ModelReceipt
}

type ModelSnapshot struct {
	Jobs   []ModelJob
	Events []ModelEvent
}
