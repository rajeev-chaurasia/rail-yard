package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNewRegistersCustomCollectors(t *testing.T) {
	metrics, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	names := make(map[string]bool, len(families))
	for _, family := range families {
		names[family.GetName()] = true
	}
	for _, name := range []string{
		"go_goroutines",
		"process_cpu_seconds_total",
		"railyard_admissions_total",
		"railyard_scheduler_decisions_total",
		"railyard_lease_grants_total",
		"railyard_completions_total",
		"railyard_queue_depth",
		"railyard_rejections_total",
		"railyard_lease_expirations_total",
		"railyard_lease_recovery_duration_seconds",
		"railyard_retries_total",
		"railyard_dead_letters_total",
		"railyard_dlq_depth",
		"railyard_sqlite_transaction_duration_seconds",
		"railyard_sqlite_busy_total",
		"railyard_job_latency_seconds",
		"railyard_redis_stream_lag",
		"railyard_redis_pending_entries",
	} {
		if !names[name] {
			t.Errorf("Gather() did not contain %q", name)
		}
	}
}

func TestRecordersUseBoundedLabels(t *testing.T) {
	metrics, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	metrics.RecordAdmission(AdmissionResult(255))
	metrics.RecordSchedulerDecision(SchedulerGranted, 3)
	metrics.RecordCompletion(CompletionSucceeded)
	metrics.SetQueueDepth(QueuePending, 17)
	metrics.SetQueueDepth(QueueRunning, -2)
	metrics.RecordRejection(RejectionQueueFull)
	metrics.RecordLeaseExpiration(LeaseRequeued)
	metrics.ObserveLeaseRecovery(4 * time.Second)
	metrics.RecordRetry(RetryLeaseExpired)
	metrics.RecordDeadLetter(DeadLetterRetriesExhausted)
	metrics.SetDLQDepth(9)
	metrics.ObserveSQLiteTransaction(SQLiteComplete, SQLiteSuccess, 5*time.Millisecond)
	metrics.RecordSQLiteBusy(SQLiteComplete)
	metrics.ObserveJobLatency(JobEndToEnd, 2*time.Second)
	metrics.SetRedisStreamState(11, -1)

	if got := gaugeValue(t, metrics.Registry(), "railyard_dlq_depth", nil); got != 9 {
		t.Fatalf("dlq depth = %v, want 9", got)
	}

	if got := counterValue(
		t,
		metrics.Registry(),
		"railyard_admissions_total",
		map[string]string{"result": "other"},
	); got != 1 {
		t.Errorf("unknown admission counter = %v, want 1", got)
	}
	if got := counterValue(
		t,
		metrics.Registry(),
		"railyard_lease_grants_total",
		nil,
	); got != 3 {
		t.Errorf("lease grants counter = %v, want 3", got)
	}
	if got := gaugeValue(
		t,
		metrics.Registry(),
		"railyard_queue_depth",
		map[string]string{"state": "pending"},
	); got != 17 {
		t.Errorf("pending queue depth = %v, want 17", got)
	}
	if got := gaugeValue(
		t,
		metrics.Registry(),
		"railyard_queue_depth",
		map[string]string{"state": "running"},
	); got != 0 {
		t.Errorf("negative running queue depth = %v, want 0", got)
	}
	if got := histogramCount(
		t,
		metrics.Registry(),
		"railyard_job_latency_seconds",
		map[string]string{"stage": "end_to_end"},
	); got != 1 {
		t.Errorf("end-to-end histogram count = %d, want 1", got)
	}
	if got := gaugeValue(
		t,
		metrics.Registry(),
		"railyard_redis_stream_lag",
		nil,
	); got != 11 {
		t.Errorf("Redis stream lag = %v, want 11", got)
	}
	if got := gaugeValue(
		t,
		metrics.Registry(),
		"railyard_redis_pending_entries",
		nil,
	); got != 0 {
		t.Errorf("negative Redis pending entries = %v, want 0", got)
	}
	if got := familyMetricCount(t, metrics.Registry(), "railyard_admissions_total"); got != 3 {
		t.Errorf("admission series count = %d, want fixed count 3", got)
	}
}

func TestHandlerServesOnlyCustomRegistry(t *testing.T) {
	metrics, err := New()
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	metrics.RecordAdmission(AdmissionAccepted)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.Code, http.StatusOK)
	}
	body := response.Body.String()
	if !strings.Contains(body, "railyard_admissions_total") {
		t.Error("metrics body did not contain Rail Yard metrics")
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Error("metrics body did not contain Go collector metrics")
	}
}

func counterValue(
	t *testing.T,
	gatherer prometheus.Gatherer,
	name string,
	labels map[string]string,
) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if labelsMatch(metric.GetLabel(), labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("counter %q with labels %v not found", name, labels)
	return 0
}

func gaugeValue(
	t *testing.T,
	gatherer prometheus.Gatherer,
	name string,
	labels map[string]string,
) float64 {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if labelsMatch(metric.GetLabel(), labels) {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("gauge %q with labels %v not found", name, labels)
	return 0
}

func histogramCount(
	t *testing.T,
	gatherer prometheus.Gatherer,
	name string,
	labels map[string]string,
) uint64 {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if labelsMatch(metric.GetLabel(), labels) {
				return metric.GetHistogram().GetSampleCount()
			}
		}
	}
	t.Fatalf("histogram %q with labels %v not found", name, labels)
	return 0
}

func familyMetricCount(t *testing.T, gatherer prometheus.Gatherer, name string) int {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return len(family.GetMetric())
		}
	}
	t.Fatalf("metric family %q not found", name)
	return 0
}

func labelsMatch(pairs []*dto.LabelPair, labels map[string]string) bool {
	if len(pairs) != len(labels) {
		return false
	}
	for _, pair := range pairs {
		if labels[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}
