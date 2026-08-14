package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

func TestConfigRequiresQualificationChaosMinimum(t *testing.T) {
	cfg := validConfig()
	cfg.WorkerKills = minimumWorkerKills - 1
	if err := cfg.validate(); err == nil {
		t.Fatal("configuration with fewer than 20 worker kills passed validation")
	}
}

func TestStableSubmissionKeysAreDeterministicAndUnique(t *testing.T) {
	first := stableSubmissionKey(2, -17, 1)
	if first != stableSubmissionKey(2, -17, 1) {
		t.Fatal("stable key changed for identical inputs")
	}
	if first == stableSubmissionKey(2, -17, 2) {
		t.Fatal("different job indexes produced the same stable key")
	}
	if strings.ContainsAny(first, " \t\r\n") {
		t.Fatalf("stable key contains whitespace: %q", first)
	}
}

func TestSubmitAllRecordsExactlyOneReceiptPerJob(t *testing.T) {
	var (
		mu            sync.Mutex
		requestsByKey = make(map[string]int)
	)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		key := request.Header.Get("Idempotency-Key")
		if key == "" {
			http.Error(response, "missing idempotency key", http.StatusBadRequest)
			return
		}
		var body struct {
			Job struct {
				Name string `json:"name"`
			} `json:"job"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		index, err := strconv.Atoi(strings.TrimPrefix(body.Job.Name, "chaos-"))
		if err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		requestsByKey[key]++
		attempt := requestsByKey[key]
		mu.Unlock()
		if attempt == 1 {
			http.Error(response, `{"code":"temporary"}`, http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(
			response,
			`{"job":{"id":"%032x","created_at":"%s"},"duplicate":false}`,
			index,
			time.Now().UTC().Format(time.RFC3339Nano),
		)
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "submitted.jsonl")
	writer, err := createJSONLines(path)
	if err != nil {
		t.Fatalf("create receipt writer: %v", err)
	}
	var accepted atomic.Int64
	recorder := &receiptRecorder{
		writer:   writer,
		accepted: &accepted,
		ids:      make(map[string]struct{}),
	}
	cfg := validConfig()
	cfg.ServerURL = server.URL
	cfg.Jobs = 12
	cfg.SubmitConcurrency = 4
	cfg.PollInterval = time.Millisecond
	cfg.RequestTimeout = time.Second
	released := make(chan struct{})
	close(released)
	calibrator := &clockCalibrator{}
	if err := submitAll(
		context.Background(),
		cfg,
		1,
		23,
		"chaos",
		"noop",
		6,
		released,
		recorder,
		calibrator,
	); err != nil {
		t.Fatalf("submit all: %v", err)
	}
	if err := writer.close(); err != nil {
		t.Fatalf("close receipt writer: %v", err)
	}
	if accepted.Load() != int64(cfg.Jobs) {
		t.Fatalf("accepted=%d want=%d", accepted.Load(), cfg.Jobs)
	}
	if _, exists := calibrator.current(); !exists {
		t.Fatal("submission did not persist a server clock mapping")
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open receipts: %v", err)
	}
	records, err := reconcile.ReadManifest(file)
	closeErr := file.Close()
	if err != nil {
		t.Fatalf("read receipts: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("close receipts: %v", closeErr)
	}
	if len(records) != cfg.Jobs {
		t.Fatalf("receipt count=%d want=%d", len(records), cfg.Jobs)
	}
	for index, record := range records {
		if record.Sequence != index+1 {
			t.Fatalf("receipt sequence=%d want=%d", record.Sequence, index+1)
		}
		mu.Lock()
		attempts := requestsByKey[record.SubmissionKey]
		mu.Unlock()
		if attempts != 2 {
			t.Fatalf("key %s attempts=%d want=2", record.SubmissionKey, attempts)
		}
	}
}

func TestComposeKillUsesExplicitSIGKILL(t *testing.T) {
	runner := &recordingRunner{}
	compose := composeClient{
		runner:     runner,
		executable: "docker",
		file:       "deploy/compose.yaml",
		project:    "chaos",
	}
	if err := compose.kill(context.Background(), "worker-3"); err != nil {
		t.Fatalf("kill: %v", err)
	}
	want := []string{
		"compose", "--file", "deploy/compose.yaml", "--project-name", "chaos",
		"kill", "--signal", "SIGKILL", "worker-3",
	}
	if !reflect.DeepEqual(runner.args, want) {
		t.Fatalf("command args=%q want=%q", runner.args, want)
	}
}

func TestNearestRankP99(t *testing.T) {
	samples := make([]recoverySample, 100)
	for index := range samples {
		samples[index].RecoveryMS = float64(index + 1)
	}
	if got := nearestRankP99(samples); got != 99 {
		t.Fatalf("p99=%v want=99", got)
	}
}

func TestKilledLeaseSelectionSurvivesReaperAndCompletionRace(t *testing.T) {
	killTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	snapshot := []activeLease{
		{JobID: "reaped", AttemptNo: 1, Generation: 11},
		{JobID: "finished", AttemptNo: 1, Generation: 12},
		{JobID: "active", AttemptNo: 2, Generation: 13},
	}
	states := []leaseBoundaryState{
		{
			JobID:       "reaped",
			AttemptNo:   1,
			Generation:  11,
			State:       "EXPIRED",
			CompletedAt: killTime.Add(6 * time.Millisecond),
		},
		{
			JobID:       "finished",
			AttemptNo:   1,
			Generation:  12,
			State:       "SUCCEEDED",
			CompletedAt: killTime.Add(-6 * time.Millisecond),
		},
		{
			JobID:      "active",
			AttemptNo:  2,
			Generation: 13,
			State:      "RUNNING",
		},
	}
	affected, err := selectKilledLeases(snapshot, states, killTime, 5*time.Millisecond)
	if err != nil {
		t.Fatalf("select killed leases: %v", err)
	}
	want := []activeLease{snapshot[0], snapshot[2]}
	if !reflect.DeepEqual(affected, want) {
		t.Fatalf("affected leases=%#v want=%#v", affected, want)
	}
}

func TestKilledLeaseSelectionFailsOnBoundaryUncertainty(t *testing.T) {
	killTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	snapshot := []activeLease{{JobID: "ambiguous", AttemptNo: 1, Generation: 1}}
	states := []leaseBoundaryState{{
		JobID:       "ambiguous",
		AttemptNo:   1,
		Generation:  1,
		State:       "SUCCEEDED",
		CompletedAt: killTime.Add(2 * time.Millisecond),
	}}
	if _, err := selectKilledLeases(snapshot, states, killTime, 5*time.Millisecond); err == nil {
		t.Fatal("completion inside the uncertain kill boundary was scored")
	}
}

func TestClockMappingFailsOnExcessiveUncertainty(t *testing.T) {
	hostTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	mapping, err := newClockMapping(
		hostTime.Add(time.Second),
		hostTime,
		hostTime.Add(2*maxClockMappingUncertainty+time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create clock mapping: %v", err)
	}
	calibrator := &clockCalibrator{}
	calibrator.record(mapping)
	cfg := validConfig()
	if _, err := waitForClockMapping(context.Background(), cfg, calibrator); err == nil {
		t.Fatal("clock mapping with excessive uncertainty was accepted")
	}
}

func TestRecoveryScoringUsesDurableSuccessorLease(t *testing.T) {
	killTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	mapping, err := newClockMapping(
		killTime.Add(-time.Second+5*time.Millisecond),
		killTime.Add(-time.Second),
		killTime.Add(-time.Second+10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create clock mapping: %v", err)
	}
	kill := workerKill{
		Sequence:        1,
		Worker:          "worker-1",
		ContainerID:     "container-1",
		ConfirmedHostAt: killTime,
		ConfirmedAt:     killTime,
		ClockMapping:    mapping,
		Leases: []activeLease{{
			JobID:      "job-1",
			AttemptNo:  1,
			Generation: 1,
		}},
		RecoveredAt: map[string]time.Time{"job-1": killTime.Add(9 * time.Second)},
	}
	attempts := map[string][]databaseAttempt{
		"job-1": {
			{
				JobID:        "job-1",
				AttemptNo:    2,
				Generation:   2,
				LeasedAt:     killTime.Add(2 * time.Second),
				CompletionAt: killTime.Add(3 * time.Second),
			},
		},
	}
	samples, err := scoreRecoverySamples([]workerKill{kill}, attempts)
	if err != nil {
		t.Fatalf("score recovery: %v", err)
	}
	if len(samples) != 1 || samples[0].RecoveryMS != 2000 {
		t.Fatalf("recovery samples=%#v", samples)
	}
	if samples[0].SuccessorObservedAt.Equal(samples[0].SuccessorLeasedAt) {
		t.Fatal("test did not distinguish durable lease time from host observation")
	}
}

func TestRecoveryScoringFailsWithoutSuccessor(t *testing.T) {
	killTime := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	mapping, err := newClockMapping(killTime, killTime, killTime)
	if err != nil {
		t.Fatalf("create clock mapping: %v", err)
	}
	kill := workerKill{
		Sequence:        1,
		ConfirmedHostAt: killTime,
		ConfirmedAt:     killTime,
		ClockMapping:    mapping,
		Leases:          []activeLease{{JobID: "job-1", AttemptNo: 1, Generation: 1}},
		RecoveredAt:     map[string]time.Time{"job-1": killTime.Add(time.Second)},
	}
	if _, err := scoreRecoverySamples([]workerKill{kill}, nil); err == nil {
		t.Fatal("affected lease without a durable successor was scored")
	}
}

func TestRandomDurationIsSeededAndBounded(t *testing.T) {
	left := seededDurations(42)
	right := seededDurations(42)
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("seeded durations differ: %v != %v", left, right)
	}
	for _, value := range left {
		if value < 10*time.Millisecond || value > 20*time.Millisecond {
			t.Fatalf("duration %s is outside configured range", value)
		}
	}
}

type recordingRunner struct {
	name string
	args []string
}

func (r *recordingRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.name = name
	r.args = append([]string(nil), args...)
	return nil, nil
}

func validConfig() config {
	return config{
		ComposeFile:       "deploy/compose.yaml",
		ProjectPrefix:     "rail-yard-chaos",
		OutputDirectory:   "artifacts/chaos",
		ServerURL:         "http://127.0.0.1:8080",
		DatabasePath:      "/var/lib/railyard/railyard.db",
		DockerExecutable:  "docker",
		Runs:              1,
		Jobs:              100,
		BaseSeed:          1,
		SubmitConcurrency: 4,
		WorkerKills:       minimumWorkerKills,
		Workers:           []string{"worker-1"},
		JobDuration:       time.Millisecond,
		ActionMinimum:     time.Millisecond,
		ActionMaximum:     2 * time.Millisecond,
		PollInterval:      time.Millisecond,
		StartupTimeout:    time.Second,
		DrainTimeout:      time.Second,
		RequestTimeout:    time.Second,
		MaxRecovery:       5 * time.Second,
	}
}

func seededDurations(seed int64) []time.Duration {
	random := randSource(seed)
	values := make([]time.Duration, 10)
	for index := range values {
		values[index] = randomDuration(random, 10*time.Millisecond, 20*time.Millisecond)
	}
	return values
}

func randSource(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}
