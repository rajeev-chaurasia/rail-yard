package model

import "time"

type Snapshot struct {
	Sequence  int64
	Pending   int
	Scheduled int
	Running   int
	Retrying  int
	DLQ       int
}

type TimingEvent struct {
	Sequence          int64
	ReadyToLease      *time.Duration
	LeaseToCompletion *time.Duration
	EndToEnd          *time.Duration
	LeaseRecovery     *time.Duration
}
