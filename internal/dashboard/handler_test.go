package dashboard

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

var fixedNow = time.Date(2026, time.August, 14, 9, 0, 0, 0, time.UTC)

type fakeClient struct {
	snapshot        Snapshot
	snapshotErr     error
	deadLetters     []domain.DeadLetter
	deadLettersErr  error
	deadLetterLimit int
	run             Run
	runErr          error
	runID           string
	operationResult OperationResult
	operationErr    error
	operations      []Operation
}

func (f *fakeClient) Snapshot(context.Context) (Snapshot, error) {
	return f.snapshot, f.snapshotErr
}

func (f *fakeClient) DeadLetters(_ context.Context, limit int) ([]domain.DeadLetter, error) {
	f.deadLetterLimit = limit
	return f.deadLetters, f.deadLettersErr
}

func (f *fakeClient) Run(_ context.Context, runID string) (Run, error) {
	f.runID = runID
	return f.run, f.runErr
}

func (f *fakeClient) Operate(_ context.Context, operation Operation) (OperationResult, error) {
	f.operations = append(f.operations, operation)
	return f.operationResult, f.operationErr
}

func newTestHandler(t *testing.T, client *fakeClient) http.Handler {
	t.Helper()
	value, err := New(client, Config{})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return value
}

func perform(handler http.Handler, method, path string, body io.Reader) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func csrfCookie(t *testing.T, handler http.Handler) *http.Cookie {
	t.Helper()
	response := perform(handler, http.MethodGet, "/ops/", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("page status = %d, want %d", response.Code, http.StatusOK)
	}
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == csrfCookieName {
			return cookie
		}
	}
	t.Fatal("page did not set CSRF cookie")
	return nil
}

func TestPageAndEmbeddedAssets(t *testing.T) {
	handler := newTestHandler(t, &fakeClient{})

	redirect := perform(handler, http.MethodGet, "/ops", nil)
	if redirect.Code != http.StatusPermanentRedirect {
		t.Fatalf("redirect status = %d, want %d", redirect.Code, http.StatusPermanentRedirect)
	}
	if location := redirect.Header().Get("Location"); location != "/ops/" {
		t.Fatalf("redirect location = %q, want /ops/", location)
	}

	page := perform(handler, http.MethodGet, "/ops/", nil)
	if page.Code != http.StatusOK {
		t.Fatalf("page status = %d, want %d", page.Code, http.StatusOK)
	}
	for _, text := range []string{
		"Operations dashboard",
		"Aggregate queue depth",
		"Worker health and leases",
		"Dead-letter queue",
		"One-run DAG",
		"Operator identity",
		"Change reason",
		"Force action",
		`src="/ops/assets/app.js"`,
	} {
		if !strings.Contains(page.Body.String(), text) {
			t.Errorf("page does not contain %q", text)
		}
	}
	if !strings.Contains(page.Header().Get("Content-Security-Policy"), "script-src 'self'") {
		t.Error("page does not set a restrictive content security policy")
	}
	cookies := page.Result().Cookies()
	if len(cookies) != 1 || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("CSRF cookie is missing SameSite=Strict: %#v", cookies)
	}

	stylesheet := perform(handler, http.MethodGet, "/ops/assets/app.css", nil)
	if stylesheet.Code != http.StatusOK || !strings.HasPrefix(stylesheet.Header().Get("Content-Type"), "text/css") {
		t.Fatalf("stylesheet response = %d %q", stylesheet.Code, stylesheet.Header().Get("Content-Type"))
	}
	script := perform(handler, http.MethodGet, "/ops/assets/app.js", nil)
	if script.Code != http.StatusOK || !strings.HasPrefix(script.Header().Get("Content-Type"), "text/javascript") {
		t.Fatalf("script response = %d %q", script.Code, script.Header().Get("Content-Type"))
	}
	for _, required := range []string{"maxConsecutiveFailures = 5", "Promise.allSettled", "textContent"} {
		if !strings.Contains(script.Body.String(), required) {
			t.Errorf("script does not contain %q", required)
		}
	}
	if strings.Contains(script.Body.String(), "innerHTML") {
		t.Error("script must not insert backend data through innerHTML")
	}
}

func TestReadAPIs(t *testing.T) {
	client := &fakeClient{
		snapshot: Snapshot{
			GeneratedAt: fixedNow,
			QueueDepths: []QueueDepth{{
				TenantID: "tenant-a",
				Queue:    "batch",
				State:    domain.StatePending,
				Depth:    7,
			}},
			FailedJobs: []JobSummary{{
				ID:    "job-failed",
				State: domain.StateFailed,
				Failure: &domain.Failure{
					Message: `<script>alert("unsafe")</script>`,
				},
			}},
		},
		deadLetters: []domain.DeadLetter{{
			JobID:     "job-dlq",
			Failure:   domain.Failure{Class: "exit"},
			CreatedAt: fixedNow,
		}},
		run: Run{
			ID: "run-1",
			Nodes: []RunNode{{
				ID:        "job-1",
				State:     domain.StateRunning,
				DependsOn: []string{},
			}},
		},
	}
	handler := newTestHandler(t, client)

	snapshot := perform(handler, http.MethodGet, "/ops/api/snapshot", nil)
	if snapshot.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want %d: %s", snapshot.Code, http.StatusOK, snapshot.Body.String())
	}
	if !strings.Contains(snapshot.Body.String(), `"depth":7`) {
		t.Errorf("snapshot body does not contain queue depth: %s", snapshot.Body.String())
	}
	if strings.Contains(snapshot.Body.String(), "<script>") {
		t.Errorf("snapshot body contains unescaped script markup: %s", snapshot.Body.String())
	}

	deadLetters := perform(handler, http.MethodGet, "/ops/api/dead-letters", nil)
	if deadLetters.Code != http.StatusOK || !strings.Contains(deadLetters.Body.String(), `"job_id":"job-dlq"`) {
		t.Fatalf("dead-letter response = %d: %s", deadLetters.Code, deadLetters.Body.String())
	}
	if client.deadLetterLimit != deadLetterLimit {
		t.Fatalf("dead-letter limit = %d, want %d", client.deadLetterLimit, deadLetterLimit)
	}

	run := perform(handler, http.MethodGet, "/ops/api/runs/run-1", nil)
	if run.Code != http.StatusOK || !strings.Contains(run.Body.String(), `"id":"run-1"`) {
		t.Fatalf("run response = %d: %s", run.Code, run.Body.String())
	}
	if client.runID != "run-1" {
		t.Fatalf("client received run ID %q, want run-1", client.runID)
	}
}

func TestMutationRequiresSameOriginAndCSRF(t *testing.T) {
	client := &fakeClient{
		operationResult: OperationResult{
			Action:  ActionCancel,
			JobID:   "job-1",
			Message: "job cancellation accepted",
		},
	}
	handler := newTestHandler(t, client)
	cookie := csrfCookie(t, handler)
	body := `{"action":"cancel","job_id":"job-1","actor":"oncall@example.com","reason":"runbook step 4"}`

	missingToken := httptest.NewRequest(http.MethodPost, "/ops/api/actions", strings.NewReader(body))
	missingToken.Header.Set("Content-Type", "application/json")
	missingToken.Header.Set("Origin", "http://example.com")
	missingTokenResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingTokenResponse, missingToken)
	if missingTokenResponse.Code != http.StatusForbidden {
		t.Fatalf("missing-token status = %d, want %d", missingTokenResponse.Code, http.StatusForbidden)
	}

	crossOrigin := httptest.NewRequest(http.MethodPost, "/ops/api/actions", strings.NewReader(body))
	crossOrigin.Header.Set("Content-Type", "application/json")
	crossOrigin.Header.Set("Origin", "https://attacker.example")
	crossOrigin.Header.Set("X-CSRF-Token", cookie.Value)
	crossOrigin.AddCookie(cookie)
	crossOriginResponse := httptest.NewRecorder()
	handler.ServeHTTP(crossOriginResponse, crossOrigin)
	if crossOriginResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-origin status = %d, want %d", crossOriginResponse.Code, http.StatusForbidden)
	}

	valid := httptest.NewRequest(http.MethodPost, "/ops/api/actions", strings.NewReader(body))
	valid.Header.Set("Content-Type", "application/json")
	valid.Header.Set("Origin", "http://example.com")
	valid.Header.Set("X-CSRF-Token", cookie.Value)
	valid.AddCookie(cookie)
	validResponse := httptest.NewRecorder()
	handler.ServeHTTP(validResponse, valid)
	if validResponse.Code != http.StatusOK {
		t.Fatalf("valid mutation status = %d, want %d: %s", validResponse.Code, http.StatusOK, validResponse.Body.String())
	}
	if len(client.operations) != 1 {
		t.Fatalf("operation calls = %d, want 1", len(client.operations))
	}
	operation := client.operations[0]
	if operation.Action != ActionCancel ||
		operation.JobID != "job-1" ||
		operation.Actor != "oncall@example.com" ||
		operation.Reason != "runbook step 4" {
		t.Fatalf("unexpected operation: %#v", operation)
	}
	if !validToken(operation.RequestID) {
		t.Fatalf("operation request ID is not a random token: %q", operation.RequestID)
	}
}

func TestMutationValidation(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{
			name:        "content type",
			contentType: "text/plain",
			body:        `{}`,
			wantStatus:  http.StatusUnsupportedMediaType,
		},
		{
			name:        "unknown field",
			contentType: "application/json",
			body:        `{"action":"retry","job_id":"job-1","actor":"operator","extra":true}`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "unknown action",
			contentType: "application/json",
			body:        `{"action":"delete","job_id":"job-1","actor":"operator"}`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing actor",
			contentType: "application/json",
			body:        `{"action":"force","force_action":"fail","job_id":"job-1","actor":"","reason":"operator request"}`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing force action",
			contentType: "application/json",
			body:        `{"action":"force","job_id":"job-1","actor":"operator","reason":"operator request"}`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing reason",
			contentType: "application/json",
			body:        `{"action":"cancel","job_id":"job-1","actor":"operator"}`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid job ID",
			contentType: "application/json",
			body:        `{"action":"cancel","job_id":"../job-1","actor":"operator"}`,
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			handler := newTestHandler(t, client)
			cookie := csrfCookie(t, handler)
			request := httptest.NewRequest(
				http.MethodPost,
				"/ops/api/actions",
				strings.NewReader(test.body),
			)
			request.Header.Set("Content-Type", test.contentType)
			request.Header.Set("Origin", "http://example.com")
			request.Header.Set("X-CSRF-Token", cookie.Value)
			request.AddCookie(cookie)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if len(client.operations) != 0 {
				t.Fatalf("invalid request invoked %d operations", len(client.operations))
			}
		})
	}
}

func TestClientErrorsAreClearAndBounded(t *testing.T) {
	handler := newTestHandler(t, &fakeClient{
		snapshotErr: &ClientError{
			Status:  http.StatusConflict,
			Code:    "snapshot_busy",
			Message: "snapshot is being rebuilt",
		},
		deadLettersErr: context.DeadlineExceeded,
	})

	snapshot := perform(handler, http.MethodGet, "/ops/api/snapshot", nil)
	if snapshot.Code != http.StatusConflict ||
		!strings.Contains(snapshot.Body.String(), `"code":"snapshot_busy"`) {
		t.Fatalf("snapshot error = %d: %s", snapshot.Code, snapshot.Body.String())
	}
	deadLetters := perform(handler, http.MethodGet, "/ops/api/dead-letters", nil)
	if deadLetters.Code != http.StatusServiceUnavailable ||
		!strings.Contains(deadLetters.Body.String(), `"code":"backend_unavailable"`) {
		t.Fatalf("dead-letter error = %d: %s", deadLetters.Code, deadLetters.Body.String())
	}
	if strings.Contains(deadLetters.Body.String(), context.DeadlineExceeded.Error()) {
		t.Error("backend error details leaked to the response")
	}
}

func TestRoutingAndConfiguration(t *testing.T) {
	client := &fakeClient{}
	if _, err := New(nil, Config{}); err == nil {
		t.Error("New accepted a nil client")
	}
	if _, err := New(client, Config{BasePath: "/unsafe/../"}); err == nil {
		t.Error("New accepted an unsafe base path")
	}
	if _, err := New(client, Config{PollInterval: time.Second}); err == nil {
		t.Error("New accepted an unbounded poll interval")
	}

	custom, err := New(client, Config{
		BasePath:       "/internal/ops/",
		PollInterval:   10 * time.Second,
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("New with custom configuration returned error: %v", err)
	}
	page := perform(custom, http.MethodGet, "/internal/ops/", nil)
	if page.Code != http.StatusOK || !strings.Contains(page.Body.String(), "/internal/ops/assets/app.css") {
		t.Fatalf("custom mount response = %d: %s", page.Code, page.Body.String())
	}

	wrongMethod := perform(custom, http.MethodPost, "/internal/ops/api/snapshot", nil)
	if wrongMethod.Code != http.StatusMethodNotAllowed ||
		wrongMethod.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("wrong method response = %d Allow=%q", wrongMethod.Code, wrongMethod.Header().Get("Allow"))
	}
	notFound := perform(custom, http.MethodGet, "/ops/", nil)
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("outside mount status = %d, want %d", notFound.Code, http.StatusNotFound)
	}
}
