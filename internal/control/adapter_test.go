package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	"github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
)

func TestMountedOperationsRecordActorAwareAudit(t *testing.T) {
	jobStore, err := sqlite.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 14, 0, 0, 0, time.UTC)
	adapter := New(jobStore)
	adapter.now = func() time.Time { return now }
	config := operations.DefaultConfig()
	config.Now = adapter.now
	handler, err := NewHandler(adapter, config)
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/operations/jobs",
		strings.NewReader(`{"job":{"payload":{"type":"noop"}}}`),
	)
	request.Header.Set("Idempotency-Key", "submit-audit")
	request.Header.Set(actorHeader, "operator-a")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("submit status = %d: %s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(
		http.MethodGet,
		"/v1/operations/audit-events?since="+now.Add(-time.Second).Format(time.RFC3339Nano)+
			"&actor=operator-a",
		nil,
	)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("audit status = %d: %s", response.Code, response.Body.String())
	}
	var audit struct {
		Events []sqlite.AuditEvent `json:"events"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &audit); err != nil {
		t.Fatal(err)
	}
	if len(audit.Events) != 1 ||
		audit.Events[0].Action != "job.submit" ||
		audit.Events[0].Actor != "operator-a" {
		t.Fatalf("audit events = %#v", audit.Events)
	}
}

func TestOperatorActionHookIsIdempotent(t *testing.T) {
	jobStore, err := sqlite.Open(filepath.Join(t.TempDir(), "operator-action.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobStore.Close() })
	adapter := New(jobStore)
	handler, err := NewHandler(adapter, operations.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	body := `{"action":"worker.kill","target_type":"worker","target_id":"worker-1"}`
	for index, wantStatus := range []int{http.StatusCreated, http.StatusOK} {
		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/operations/operator-actions",
			strings.NewReader(body),
		)
		request.Header.Set("Idempotency-Key", "worker-kill")
		request.Header.Set(actorHeader, "operator-a")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("request %d status = %d: %s", index, response.Code, response.Body.String())
		}
	}
	events, err := jobStore.ListAuditEvents(t.Context(), time.Unix(0, 0), "operator-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Action != "worker.kill" {
		t.Fatalf("events = %#v", events)
	}
}
