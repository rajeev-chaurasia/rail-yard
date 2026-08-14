package p5

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSubmitWorkflowCarriesOperatorIdentity(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/operations/dags" {
			http.NotFound(w, request)
			return
		}
		if got := request.Header.Get(actorHeader); got != "qa-operator" {
			t.Errorf("%s = %q, want qa-operator", actorHeader, got)
		}
		if got := request.Header.Get("Idempotency-Key"); got != "workflow-key" {
			t.Errorf("Idempotency-Key = %q, want workflow-key", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(WorkflowResponse{
			Jobs: []Job{{ID: "job-1", State: statePending}},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.URL, "qa-operator", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.SubmitWorkflow(
		context.Background(),
		"workflow-key",
		WorkflowRequest{TenantID: "p5"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Jobs) != 1 || response.Jobs[0].ID != "job-1" {
		t.Fatalf("response = %#v", response)
	}
}

func TestAuditFeedFailureNamesRequiredHook(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	client, err := NewClient(server.URL, server.URL, "qa-operator", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AuditEvents(context.Background(), time.Now())
	if err == nil ||
		!strings.Contains(err.Error(), "required P5 hook GET /v1/operations/audit-events unavailable") {
		t.Fatalf("error = %v, want required hook diagnostic", err)
	}
}

func TestForceDeadLetterUsesOperationsContract(t *testing.T) {
	t.Parallel()
	committedAt := time.Date(2032, time.March, 14, 15, 9, 26, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/operations/jobs/job-1/force" {
			http.NotFound(w, request)
			return
		}
		if got := request.Header.Get(actorHeader); got != "qa-operator" {
			t.Errorf("%s = %q, want qa-operator", actorHeader, got)
		}
		var body ForceJobRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body.Action != "dead_letter" || body.Reason != "acceptance drill" {
			t.Errorf("request = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ActionReceipt{
			JobID:       "job-1",
			Action:      "dead_letter",
			State:       stateDeadLetter,
			Actor:       "qa-operator",
			CommittedAt: committedAt,
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.URL, "qa-operator", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.ForceDeadLetter(
		context.Background(),
		"force-key",
		"job-1",
		"acceptance drill",
	)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != stateDeadLetter ||
		receipt.Actor != "qa-operator" ||
		!receipt.CommittedAt.Equal(committedAt) {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestAlertStateReadsPrometheusEnvelope(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/alerts" {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"alerts":[{
				"labels":{"alertname":"RailYardQueueStalled"},
				"state":"firing"
			}]}
		}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, server.URL, "qa-operator", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	state, err := client.AlertState(context.Background(), queueStalledAlert)
	if err != nil {
		t.Fatal(err)
	}
	if state != "firing" {
		t.Fatalf("state = %q, want firing", state)
	}
	inactive, err := client.AlertState(context.Background(), recoverySLOAlert)
	if err != nil {
		t.Fatal(err)
	}
	if inactive != "inactive" {
		t.Fatalf("missing alert state = %q, want inactive", inactive)
	}
}
