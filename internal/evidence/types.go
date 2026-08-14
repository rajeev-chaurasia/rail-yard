package evidence

import "time"

const SchemaVersion = 1

type RunPhase string

const (
	PhaseWarmup   RunPhase = "warmup"
	PhaseMeasured RunPhase = "measured"
)

type RunStatus string

const (
	StatusRunning                RunStatus = "running"
	StatusAwaitingReconciliation RunStatus = "awaiting_reconciliation"
	StatusValid                  RunStatus = "valid"
	StatusInvalid                RunStatus = "invalid"
)

type RunConfig struct {
	ServerURL           string        `json:"server_url"`
	JobCount            int           `json:"job_count"`
	WorkerCount         int           `json:"worker_count"`
	WorkerSlots         int           `json:"worker_slots"`
	SubmitConcurrency   int           `json:"submit_concurrency"`
	PollConcurrency     int           `json:"poll_concurrency"`
	SubmissionAttempts  int           `json:"submission_attempts"`
	RequestTimeout      time.Duration `json:"request_timeout_ns"`
	HealthTimeout       time.Duration `json:"health_timeout_ns"`
	DrainTimeout        time.Duration `json:"drain_timeout_ns"`
	PollInterval        time.Duration `json:"poll_interval_ns"`
	TenantID            string        `json:"tenant_id"`
	Queue               string        `json:"queue"`
	PayloadBytes        int           `json:"payload_bytes"`
	PayloadSHA256       string        `json:"payload_sha256"`
	ConfigurationSHA256 string        `json:"configuration_sha256"`
	Seed                int64         `json:"seed"`
	Qualification       bool          `json:"qualification"`
}

type EnvironmentManifest struct {
	GitCommit       string            `json:"git_commit,omitempty"`
	GitDirty        *bool             `json:"git_dirty,omitempty"`
	BinaryDigests   map[string]string `json:"binary_digests,omitempty"`
	ImageDigests    map[string]string `json:"image_digests,omitempty"`
	GoVersion       string            `json:"go_version,omitempty"`
	DockerVersion   string            `json:"docker_version,omitempty"`
	ComposeVersion  string            `json:"compose_version,omitempty"`
	Hostname        string            `json:"hostname,omitempty"`
	OS              string            `json:"os,omitempty"`
	Architecture    string            `json:"architecture,omitempty"`
	Kernel          string            `json:"kernel,omitempty"`
	CPUModel        string            `json:"cpu_model,omitempty"`
	CPUCount        int               `json:"cpu_count,omitempty"`
	MemoryBytes     int64             `json:"memory_bytes,omitempty"`
	Filesystem      string            `json:"filesystem,omitempty"`
	CgroupLimits    string            `json:"cgroup_limits,omitempty"`
	SQLitePragmas   map[string]string `json:"sqlite_pragmas,omitempty"`
	Timezone        string            `json:"timezone,omitempty"`
	Unavailable     []string          `json:"unavailable,omitempty"`
	OperatorDetails map[string]string `json:"operator_details,omitempty"`
}

type RunManifest struct {
	SchemaVersion       int                 `json:"schema_version"`
	RunID               string              `json:"run_id"`
	Phase               RunPhase            `json:"phase"`
	Scored              bool                `json:"scored"`
	Status              RunStatus           `json:"status"`
	InvalidReasons      []string            `json:"invalid_reasons,omitempty"`
	StartedAt           time.Time           `json:"started_at"`
	WorkloadFinishedAt  *time.Time          `json:"workload_finished_at,omitempty"`
	FinalizedAt         *time.Time          `json:"finalized_at,omitempty"`
	Config              RunConfig           `json:"config"`
	Environment         EnvironmentManifest `json:"environment"`
	DatabaseSHA256      string              `json:"database_sha256,omitempty"`
	DatabaseFilesSHA256 map[string]string   `json:"database_files_sha256,omitempty"`
	DatabaseQuiesced    bool                `json:"database_quiesced"`
}

type SubmissionSample struct {
	SchemaVersion      int       `json:"schema_version"`
	RunID              string    `json:"run_id"`
	Index              int       `json:"index"`
	IdempotencyKey     string    `json:"idempotency_key"`
	JobID              string    `json:"job_id,omitempty"`
	RequestStartedAt   time.Time `json:"request_started_at"`
	ResponseReceivedAt time.Time `json:"response_received_at"`
	AdmittedAt         time.Time `json:"admitted_at,omitempty"`
	StatusCode         int       `json:"status_code,omitempty"`
	AttemptCount       int       `json:"attempt_count"`
	AmbiguousRetry     bool      `json:"ambiguous_retry"`
	Duplicate          bool      `json:"duplicate"`
	Error              string    `json:"error,omitempty"`
}

type DrainSample struct {
	SchemaVersion int        `json:"schema_version"`
	RunID         string     `json:"run_id"`
	JobID         string     `json:"job_id"`
	State         string     `json:"state"`
	ObservedAt    time.Time  `json:"observed_at"`
	TerminalAt    *time.Time `json:"terminal_at,omitempty"`
}

type BenchmarkSample struct {
	SchemaVersion               int           `json:"schema_version"`
	RunID                       string        `json:"run_id"`
	JobID                       string        `json:"job_id"`
	AdmissionCommittedAt        time.Time     `json:"admission_committed_at"`
	FirstLeaseCommittedAt       time.Time     `json:"first_lease_committed_at"`
	CompletionLeaseCommittedAt  time.Time     `json:"completion_lease_committed_at"`
	CompletionCommittedAt       time.Time     `json:"completion_committed_at"`
	AdmissionToFirstLease       time.Duration `json:"admission_to_first_lease_ns"`
	CompletionLeaseToCompletion time.Duration `json:"completion_lease_to_completion_ns"`
	EndToEnd                    time.Duration `json:"end_to_end_ns"`
	AttemptCount                int           `json:"attempt_count"`
	CompletionState             string        `json:"completion_state"`
	TimestampSource             string        `json:"timestamp_source"`
}

type RateSample struct {
	Available         bool          `json:"available"`
	Count             int           `json:"count"`
	FirstCommittedAt  *time.Time    `json:"first_committed_at,omitempty"`
	LastCommittedAt   *time.Time    `json:"last_committed_at,omitempty"`
	Interval          time.Duration `json:"interval_ns,omitempty"`
	PerMinute         *float64      `json:"per_minute,omitempty"`
	Source            string        `json:"source,omitempty"`
	UnavailableReason string        `json:"unavailable_reason,omitempty"`
}

type LatencySummary struct {
	Available         bool                  `json:"available"`
	Distribution      *DurationDistribution `json:"distribution,omitempty"`
	Source            string                `json:"source,omitempty"`
	UnavailableReason string                `json:"unavailable_reason,omitempty"`
}

type BenchmarkSummary struct {
	SchemaVersion               int            `json:"schema_version"`
	RunID                       string         `json:"run_id"`
	Phase                       RunPhase       `json:"phase"`
	Valid                       bool           `json:"valid"`
	InvalidReasons              []string       `json:"invalid_reasons,omitempty"`
	Admissions                  RateSample     `json:"admissions"`
	DurableLeaseGrants          RateSample     `json:"durable_lease_grants"`
	SuccessfulCompletions       RateSample     `json:"successful_completions"`
	AdmissionToFirstLease       LatencySummary `json:"admission_to_first_lease"`
	CompletionLeaseToCompletion LatencySummary `json:"completion_lease_to_completion"`
	EndToEnd                    LatencySummary `json:"end_to_end"`
	CanonicalJobCount           int            `json:"canonical_job_count"`
	DurableLeaseGrantCount      int            `json:"durable_lease_grant_count"`
	RepeatedAttemptCount        int            `json:"repeated_attempt_count"`
}

type ReconciliationReport struct {
	SchemaVersion             int               `json:"schema_version"`
	RunID                     string            `json:"run_id"`
	Passed                    bool              `json:"passed"`
	OperationalError          string            `json:"operational_error,omitempty"`
	DatabaseSHA256            string            `json:"database_sha256"`
	DatabaseFilesSHA256       map[string]string `json:"database_files_sha256,omitempty"`
	AcceptedCount             int               `json:"accepted_count"`
	DatabaseJobCount          int               `json:"database_job_count"`
	TerminalCount             int               `json:"terminal_count"`
	SucceededCount            int               `json:"succeeded_count"`
	FailedCount               int               `json:"failed_count"`
	DeadLetterCount           int               `json:"dead_letter_count"`
	ActiveJobCount            int               `json:"active_job_count"`
	ActiveAttemptCount        int               `json:"active_attempt_count"`
	DurableLeaseGrantCount    int               `json:"durable_lease_grant_count"`
	RepeatedAttemptCount      int               `json:"repeated_attempt_count"`
	LostJobIDs                []string          `json:"lost_job_ids,omitempty"`
	OrphanJobIDs              []string          `json:"orphan_job_ids,omitempty"`
	DuplicateAcceptedJobIDs   []string          `json:"duplicate_accepted_job_ids,omitempty"`
	DuplicateCompletionJobIDs []string          `json:"duplicate_completion_job_ids,omitempty"`
	UnsuccessfulJobIDs        []string          `json:"unsuccessful_job_ids,omitempty"`
	ActiveJobIDs              []string          `json:"active_job_ids,omitempty"`
	ActiveAttemptIDs          []string          `json:"active_attempt_ids,omitempty"`
	MaterializationViolations []string          `json:"materialization_violations,omitempty"`
	AttemptViolations         []string          `json:"attempt_violations,omitempty"`
	EventViolations           []string          `json:"event_violations,omitempty"`
	SlotReservationViolations []string          `json:"slot_reservation_violations,omitempty"`
	IntegrityFailures         []string          `json:"integrity_failures,omitempty"`
	ForeignKeyFailures        []string          `json:"foreign_key_failures,omitempty"`
	ManifestViolations        []string          `json:"manifest_violations,omitempty"`
	SQLitePragmas             map[string]string `json:"sqlite_pragmas"`
}

type SuiteRun struct {
	RunID        string   `json:"run_id"`
	Phase        RunPhase `json:"phase"`
	ArtifactPath string   `json:"artifact_path"`
	SHA256       string   `json:"summary_sha256"`
}

type MedianRate struct {
	Available         bool      `json:"available"`
	SamplesPerMinute  []float64 `json:"samples_per_minute,omitempty"`
	MedianPerMinute   *float64  `json:"median_per_minute,omitempty"`
	UnavailableReason string    `json:"unavailable_reason,omitempty"`
}

type SuiteSummary struct {
	SchemaVersion               int            `json:"schema_version"`
	Valid                       bool           `json:"valid"`
	InvalidReasons              []string       `json:"invalid_reasons,omitempty"`
	GeneratedAt                 time.Time      `json:"generated_at"`
	Warmup                      SuiteRun       `json:"warmup"`
	MeasuredRuns                []SuiteRun     `json:"measured_runs"`
	Admissions                  MedianRate     `json:"admissions"`
	DurableLeaseGrants          MedianRate     `json:"durable_lease_grants"`
	SuccessfulCompletions       MedianRate     `json:"successful_completions"`
	AdmissionToFirstLease       LatencySummary `json:"admission_to_first_lease"`
	CompletionLeaseToCompletion LatencySummary `json:"completion_lease_to_completion"`
	EndToEnd                    LatencySummary `json:"end_to_end"`
}
