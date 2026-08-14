package domain

import "errors"

var (
	ErrNotFound            = errors.New("not found")
	ErrQueueFull           = errors.New("tenant queue is full")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrCycleDetected       = errors.New("workflow contains a dependency cycle")
	ErrStaleLease          = errors.New("lease is stale")
	ErrTerminalJob         = errors.New("job is already terminal")
	ErrDeadLetterRedriven  = errors.New("dead letter was already redriven")
)
