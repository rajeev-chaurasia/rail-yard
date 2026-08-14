package telemetry

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "railyard"

type AdmissionResult uint8

const (
	AdmissionUnknown AdmissionResult = iota
	AdmissionAccepted
	AdmissionDuplicate
)

type SchedulerOutcome uint8

const (
	SchedulerUnknown SchedulerOutcome = iota
	SchedulerGranted
	SchedulerNoGrant
	SchedulerFailed
)

type CompletionOutcome uint8

const (
	CompletionUnknown CompletionOutcome = iota
	CompletionSucceeded
	CompletionFailed
	CompletionRetrying
	CompletionDeadLetter
)

type QueueState uint8

const (
	QueueStateUnknown QueueState = iota
	QueuePending
	QueueScheduled
	QueueRunning
	QueueRetrying
)

type RejectionReason uint8

const (
	RejectionUnknown RejectionReason = iota
	RejectionQueueFull
	RejectionCycleDetected
	RejectionIdempotencyConflict
	RejectionStaleLease
	RejectionInternal
)

type LeaseDisposition uint8

const (
	LeaseDispositionUnknown LeaseDisposition = iota
	LeaseRequeued
	LeaseDeadLettered
)

type RetryReason uint8

const (
	RetryUnknown RetryReason = iota
	RetryAttemptFailure
	RetryLeaseExpired
)

type DeadLetterReason uint8

const (
	DeadLetterUnknown DeadLetterReason = iota
	DeadLetterRetriesExhausted
)

type SQLiteOperation uint8

const (
	SQLiteOperationUnknown SQLiteOperation = iota
	SQLiteSubmitJob
	SQLiteSubmitWorkflow
	SQLiteAcquireLease
	SQLiteMarkRunning
	SQLiteHeartbeat
	SQLiteComplete
	SQLitePromoteDue
	SQLiteReapExpired
	SQLiteRedisIngest
)

type SQLiteResult uint8

const (
	SQLiteResultUnknown SQLiteResult = iota
	SQLiteSuccess
	SQLiteError
	SQLiteBusy
)

type JobLatencyStage uint8

const (
	JobLatencyUnknown JobLatencyStage = iota
	JobReadyToLease
	JobLeaseToCompletion
	JobEndToEnd
)

type Metrics struct {
	registry *prometheus.Registry

	admissions            *prometheus.CounterVec
	schedulerDecisions    *prometheus.CounterVec
	leaseGrants           prometheus.Counter
	completions           *prometheus.CounterVec
	queueDepth            *prometheus.GaugeVec
	rejections            *prometheus.CounterVec
	leaseExpirations      *prometheus.CounterVec
	leaseRecoveryDuration prometheus.Histogram
	retries               *prometheus.CounterVec
	deadLetters           *prometheus.CounterVec
	dlqDepth              prometheus.Gauge
	sqliteDuration        *prometheus.HistogramVec
	sqliteBusy            *prometheus.CounterVec
	jobLatency            *prometheus.HistogramVec
	redisStreamLag        *prometheus.GaugeVec
	redisPendingEntries   *prometheus.GaugeVec
}

func New() (*Metrics, error) {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		admissions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "admissions_total",
			Help:      "Total durable job admissions and idempotent duplicates.",
		}, []string{"result"}),
		schedulerDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "scheduler_decisions_total",
			Help:      "Total scheduler decisions by bounded outcome.",
		}, []string{"outcome"}),
		leaseGrants: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "lease_grants_total",
			Help:      "Total durable lease grants.",
		}),
		completions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "completions_total",
			Help:      "Total durable completion transitions by outcome.",
		}, []string{"outcome"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_depth",
			Help:      "Current aggregate nonterminal job depth by state.",
		}, []string{"state"}),
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rejections_total",
			Help:      "Total rejected operations by bounded reason.",
		}, []string{"reason"}),
		leaseExpirations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "lease_expirations_total",
			Help:      "Total expired leases by durable disposition.",
		}, []string{"disposition"}),
		leaseRecoveryDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "lease_recovery_duration_seconds",
			Help:      "Time from confirmed lease loss to durable successor lease.",
			Buckets:   []float64{0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 7.5, 10, 15, 30},
		}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "retries_total",
			Help:      "Total durable retry transitions by bounded reason.",
		}, []string{"reason"}),
		deadLetters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "dead_letters_total",
			Help:      "Total durable dead-letter transitions by bounded reason.",
		}, []string{"reason"}),
		dlqDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "dlq_depth",
			Help:      "Current unredriven dead-letter count.",
		}),
		sqliteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "sqlite_transaction_duration_seconds",
			Help:      "SQLite-backed store call latency by bounded operation and result.",
			Buckets: []float64{
				0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
				0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
			},
		}, []string{"operation", "result"}),
		sqliteBusy: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sqlite_busy_total",
			Help:      "Total SQLite busy events by bounded operation.",
		}, []string{"operation"}),
		jobLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "job_latency_seconds",
			Help:      "Job latency by bounded lifecycle stage.",
			Buckets: []float64{
				0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
				2.5, 5, 10, 30, 60, 120, 300,
			},
		}, []string{"stage"}),
		redisStreamLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "redis_stream_lag",
			Help:      "Current aggregate Redis consumer-group stream lag.",
		}, nil),
		redisPendingEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "redis_pending_entries",
			Help:      "Current aggregate pending Redis stream entries.",
		}, nil),
	}

	metricCollectors := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		metrics.admissions,
		metrics.schedulerDecisions,
		metrics.leaseGrants,
		metrics.completions,
		metrics.queueDepth,
		metrics.rejections,
		metrics.leaseExpirations,
		metrics.leaseRecoveryDuration,
		metrics.retries,
		metrics.deadLetters,
		metrics.dlqDepth,
		metrics.sqliteDuration,
		metrics.sqliteBusy,
		metrics.jobLatency,
		metrics.redisStreamLag,
		metrics.redisPendingEntries,
	}
	for _, collector := range metricCollectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register telemetry collector: %w", err)
		}
	}

	metrics.initializeBoundedSeries()
	return metrics, nil
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

func (m *Metrics) RecordAdmission(result AdmissionResult) {
	m.admissions.WithLabelValues(admissionLabel(result)).Inc()
}

func (m *Metrics) RecordSchedulerDecision(outcome SchedulerOutcome, grantCount int) {
	m.schedulerDecisions.WithLabelValues(schedulerLabel(outcome)).Inc()
	if grantCount > 0 {
		m.leaseGrants.Add(float64(grantCount))
	}
}

func (m *Metrics) RecordCompletion(outcome CompletionOutcome) {
	m.completions.WithLabelValues(completionLabel(outcome)).Inc()
}

func (m *Metrics) SetQueueDepth(state QueueState, depth int) {
	if depth < 0 {
		depth = 0
	}
	m.queueDepth.WithLabelValues(queueStateLabel(state)).Set(float64(depth))
}

func (m *Metrics) RecordRejection(reason RejectionReason) {
	m.rejections.WithLabelValues(rejectionLabel(reason)).Inc()
}

func (m *Metrics) RecordLeaseExpiration(disposition LeaseDisposition) {
	m.leaseExpirations.WithLabelValues(leaseDispositionLabel(disposition)).Inc()
}

func (m *Metrics) ObserveLeaseRecovery(duration time.Duration) {
	m.leaseRecoveryDuration.Observe(durationSeconds(duration))
}

func (m *Metrics) RecordRetry(reason RetryReason) {
	m.retries.WithLabelValues(retryLabel(reason)).Inc()
}

func (m *Metrics) RecordDeadLetter(reason DeadLetterReason) {
	m.deadLetters.WithLabelValues(deadLetterLabel(reason)).Inc()
}

func (m *Metrics) SetDLQDepth(depth int) {
	if depth < 0 {
		depth = 0
	}
	m.dlqDepth.Set(float64(depth))
}

func (m *Metrics) ObserveSQLiteTransaction(
	operation SQLiteOperation,
	result SQLiteResult,
	duration time.Duration,
) {
	m.sqliteDuration.WithLabelValues(
		sqliteOperationLabel(operation),
		sqliteResultLabel(result),
	).Observe(durationSeconds(duration))
}

func (m *Metrics) RecordSQLiteBusy(operation SQLiteOperation) {
	m.sqliteBusy.WithLabelValues(sqliteOperationLabel(operation)).Inc()
}

func (m *Metrics) ObserveJobLatency(stage JobLatencyStage, duration time.Duration) {
	m.jobLatency.WithLabelValues(jobLatencyLabel(stage)).Observe(durationSeconds(duration))
}

func (m *Metrics) SetRedisStreamState(lag, pendingEntries int64) {
	if lag >= 0 {
		m.redisStreamLag.WithLabelValues().Set(float64(lag))
	} else {
		m.redisStreamLag.DeleteLabelValues()
	}
	if pendingEntries >= 0 {
		m.redisPendingEntries.WithLabelValues().Set(float64(pendingEntries))
	} else {
		m.redisPendingEntries.DeleteLabelValues()
	}
}

func (m *Metrics) ClearRedisStreamState() {
	m.redisStreamLag.DeleteLabelValues()
	m.redisPendingEntries.DeleteLabelValues()
}

func (m *Metrics) initializeBoundedSeries() {
	for _, label := range []string{"accepted", "duplicate", "other"} {
		m.admissions.WithLabelValues(label)
	}
	for _, label := range []string{"granted", "no_grant", "failed", "other"} {
		m.schedulerDecisions.WithLabelValues(label)
	}
	for _, label := range []string{"succeeded", "failed", "retrying", "dead_letter", "other"} {
		m.completions.WithLabelValues(label)
	}
	for _, label := range []string{"pending", "scheduled", "running", "retrying", "other"} {
		m.queueDepth.WithLabelValues(label).Set(0)
	}
	for _, label := range []string{
		"queue_full",
		"cycle_detected",
		"idempotency_conflict",
		"stale_lease",
		"internal",
		"other",
	} {
		m.rejections.WithLabelValues(label)
	}
	for _, label := range []string{"requeued", "dead_lettered", "other"} {
		m.leaseExpirations.WithLabelValues(label)
	}
	for _, label := range []string{"attempt_failure", "lease_expired", "other"} {
		m.retries.WithLabelValues(label)
	}
	for _, label := range []string{"retries_exhausted", "other"} {
		m.deadLetters.WithLabelValues(label)
	}

	operations := []string{
		"submit_job",
		"submit_workflow",
		"acquire_lease",
		"mark_running",
		"heartbeat",
		"complete",
		"promote_due",
		"reap_expired",
		"redis_ingest",
		"other",
	}
	for _, operation := range operations {
		m.sqliteBusy.WithLabelValues(operation)
		for _, result := range []string{"success", "error", "busy", "other"} {
			m.sqliteDuration.WithLabelValues(operation, result)
		}
	}
	for _, label := range []string{
		"ready_to_lease",
		"lease_to_completion",
		"end_to_end",
		"other",
	} {
		m.jobLatency.WithLabelValues(label)
	}
}

func admissionLabel(result AdmissionResult) string {
	switch result {
	case AdmissionAccepted:
		return "accepted"
	case AdmissionDuplicate:
		return "duplicate"
	default:
		return "other"
	}
}

func schedulerLabel(outcome SchedulerOutcome) string {
	switch outcome {
	case SchedulerGranted:
		return "granted"
	case SchedulerNoGrant:
		return "no_grant"
	case SchedulerFailed:
		return "failed"
	default:
		return "other"
	}
}

func completionLabel(outcome CompletionOutcome) string {
	switch outcome {
	case CompletionSucceeded:
		return "succeeded"
	case CompletionFailed:
		return "failed"
	case CompletionRetrying:
		return "retrying"
	case CompletionDeadLetter:
		return "dead_letter"
	default:
		return "other"
	}
}

func queueStateLabel(state QueueState) string {
	switch state {
	case QueuePending:
		return "pending"
	case QueueScheduled:
		return "scheduled"
	case QueueRunning:
		return "running"
	case QueueRetrying:
		return "retrying"
	default:
		return "other"
	}
}

func rejectionLabel(reason RejectionReason) string {
	switch reason {
	case RejectionQueueFull:
		return "queue_full"
	case RejectionCycleDetected:
		return "cycle_detected"
	case RejectionIdempotencyConflict:
		return "idempotency_conflict"
	case RejectionStaleLease:
		return "stale_lease"
	case RejectionInternal:
		return "internal"
	default:
		return "other"
	}
}

func leaseDispositionLabel(disposition LeaseDisposition) string {
	switch disposition {
	case LeaseRequeued:
		return "requeued"
	case LeaseDeadLettered:
		return "dead_lettered"
	default:
		return "other"
	}
}

func retryLabel(reason RetryReason) string {
	switch reason {
	case RetryAttemptFailure:
		return "attempt_failure"
	case RetryLeaseExpired:
		return "lease_expired"
	default:
		return "other"
	}
}

func deadLetterLabel(reason DeadLetterReason) string {
	switch reason {
	case DeadLetterRetriesExhausted:
		return "retries_exhausted"
	default:
		return "other"
	}
}

func sqliteOperationLabel(operation SQLiteOperation) string {
	switch operation {
	case SQLiteSubmitJob:
		return "submit_job"
	case SQLiteSubmitWorkflow:
		return "submit_workflow"
	case SQLiteAcquireLease:
		return "acquire_lease"
	case SQLiteMarkRunning:
		return "mark_running"
	case SQLiteHeartbeat:
		return "heartbeat"
	case SQLiteComplete:
		return "complete"
	case SQLitePromoteDue:
		return "promote_due"
	case SQLiteReapExpired:
		return "reap_expired"
	case SQLiteRedisIngest:
		return "redis_ingest"
	default:
		return "other"
	}
}

func sqliteResultLabel(result SQLiteResult) string {
	switch result {
	case SQLiteSuccess:
		return "success"
	case SQLiteError:
		return "error"
	case SQLiteBusy:
		return "busy"
	default:
		return "other"
	}
}

func jobLatencyLabel(stage JobLatencyStage) string {
	switch stage {
	case JobReadyToLease:
		return "ready_to_lease"
	case JobLeaseToCompletion:
		return "lease_to_completion"
	case JobEndToEnd:
		return "end_to_end"
	default:
		return "other"
	}
}

func durationSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}
