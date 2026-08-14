package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

func TestHTTPClientRegister(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/workers/register" {
			t.Errorf("request = %s %s, want POST /v1/workers/register", request.Method, request.URL.Path)
		}
		var payload api.RegisterWorkerRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if payload.WorkerID != "worker-1" || payload.Slots != 4 {
			t.Errorf("payload = %+v, want worker-1 with four slots", payload)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(api.RegisterWorkerResponse{
			WorkerID:       payload.WorkerID,
			HeartbeatEvery: time.Second,
		})
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Register(context.Background(), api.RegisterWorkerRequest{
		WorkerID: "worker-1",
		Slots:    4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.WorkerID != "worker-1" || response.HeartbeatEvery != time.Second {
		t.Fatalf("response = %+v", response)
	}
}

func TestHTTPClientStartsBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/workers/worker-1/attempts/start-batch" {
			t.Errorf("path = %q", request.URL.Path)
		}
		var payload api.StartAttemptsRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if len(payload.Leases) != 1 || payload.Leases[0].JobID != "job-1" {
			t.Errorf("payload = %+v", payload)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(
			writer,
			`{"results":[{"job_id":"job-1","started":true}]}`,
		)
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.StartAttempts(
		context.Background(),
		"worker-1",
		api.StartAttemptsRequest{Leases: []domain.LeaseRef{{
			JobID:      "job-1",
			AttemptNo:  1,
			Generation: 1,
			Token:      "token",
		}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || !response.Results[0].Started {
		t.Fatalf("response = %+v", response)
	}
}

func TestHTTPClientFallsBackWhenStartBatchRouteIsUnavailable(t *testing.T) {
	batchCalls := 0
	singleCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/workers/worker-1/attempts/start-batch":
			batchCalls++
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, `{"code":"not_found","message":"route not found"}`)
		case "/v1/workers/worker-1/attempts/start":
			singleCalls++
			writer.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.StartAttempts(
		context.Background(),
		"worker-1",
		api.StartAttemptsRequest{Leases: []domain.LeaseRef{
			{JobID: "job-1"},
			{JobID: "job-2"},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if batchCalls != 1 || singleCalls != 2 || len(response.Results) != 2 {
		t.Fatalf(
			"batch calls = %d, single calls = %d, results = %d",
			batchCalls,
			singleCalls,
			len(response.Results),
		)
	}
}

func TestHTTPClientCompletesBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/workers/worker-1/attempts/complete-batch" {
			t.Errorf("path = %q", request.URL.Path)
		}
		var payload api.CompleteAttemptsRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if len(payload.Completions) != 1 || payload.Completions[0].JobID != "job-1" {
			t.Errorf("payload = %+v", payload)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(
			writer,
			`{"results":[{"job_id":"job-1","receipt":{"job_id":"job-1",`+
				`"state":"SUCCEEDED","state_version":3,`+
				`"committed_at":"2026-08-14T12:00:00Z","duplicate":false}}]}`,
		)
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.CompleteAttempts(
		context.Background(),
		"worker-1",
		api.CompleteAttemptsRequest{Completions: []domain.Completion{{
			LeaseRef:     domain.LeaseRef{JobID: "job-1", AttemptNo: 1, Generation: 1, Token: "token"},
			WorkerID:     "worker-1",
			Success:      true,
			OutputDigest: "digest",
		}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 ||
		response.Results[0].Receipt == nil ||
		response.Results[0].Receipt.State != domain.StateSucceeded {
		t.Fatalf("response = %+v", response)
	}
}

func TestHTTPClientFallsBackWhenBatchRouteIsUnavailable(t *testing.T) {
	batchCalls := 0
	singleCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/workers/worker-1/attempts/complete-batch":
			batchCalls++
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(writer, `{"code":"not_found","message":"route not found"}`)
		case "/v1/workers/worker-1/attempts/complete":
			singleCalls++
			var payload api.CompleteAttemptRequest
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Errorf("decode request: %v", err)
			}
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(domain.CompletionReceipt{
				JobID: payload.JobID,
				State: domain.StateSucceeded,
			})
		default:
			t.Errorf("unexpected path %q", request.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	request := api.CompleteAttemptsRequest{Completions: []domain.Completion{
		{LeaseRef: domain.LeaseRef{JobID: "job-1"}},
		{LeaseRef: domain.LeaseRef{JobID: "job-2"}},
	}}
	response, err := client.CompleteAttempts(context.Background(), "worker-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if batchCalls != 1 || singleCalls != 2 || len(response.Results) != 2 {
		t.Fatalf(
			"batch calls = %d, single calls = %d, results = %d",
			batchCalls,
			singleCalls,
			len(response.Results),
		)
	}
}

func TestHTTPClientEscapesWorkerID(t *testing.T) {
	var escapedPath string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		escapedPath = request.URL.EscapedPath()
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"leases":[]}`)
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AcquireLeases(context.Background(), "worker/one", api.AcquireLeasesRequest{
		AvailableSlots: 1,
		Limit:          1,
	}); err != nil {
		t.Fatal(err)
	}
	if escapedPath != "/v1/workers/worker%2Fone/leases/acquire" {
		t.Fatalf("escaped path = %q", escapedPath)
	}
}

func TestHTTPClientMapsStaleLease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(writer, `{"code":"stale_lease","message":"lease was reassigned"}`)
	}))
	defer server.Close()

	client, err := NewHTTPClient(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	err = client.StartAttempt(context.Background(), "worker", api.StartAttemptRequest{})
	if !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("error = %v, want stale lease", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		t.Fatalf("error = %#v, want HTTP 409 API error", err)
	}
}

func TestHTTPClientRetriesOnlyTransportFailures(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil, errors.New("network unavailable")
	})
	var waits []time.Duration
	client, err := NewHTTPClient(
		"http://worker.example",
		&http.Client{Transport: transport},
		WithTransportRetry(
			TransportRetryPolicy{MaxAttempts: 3, InitialBackoff: 2 * time.Millisecond, MaxBackoff: 3 * time.Millisecond},
			func(_ context.Context, duration time.Duration) error {
				waits = append(waits, duration)
				return nil
			},
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Register(context.Background(), api.RegisterWorkerRequest{WorkerID: "worker", Slots: 1})
	if err == nil || !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("error = %v, want bounded transport failure", err)
	}
	if calls != 3 {
		t.Fatalf("transport calls = %d, want 3", calls)
	}
	if want := []time.Duration{2 * time.Millisecond, 3 * time.Millisecond}; !reflect.DeepEqual(waits, want) {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
}

func TestHTTPClientRetriesResponseReadFailure(t *testing.T) {
	calls := 0
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		body := io.ReadCloser(errorReader{})
		if calls == 2 {
			body = io.NopCloser(strings.NewReader(`{"worker_id":"worker"}`))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	})
	client, err := NewHTTPClient(
		"http://worker.example",
		&http.Client{Transport: transport},
		WithTransportRetry(
			TransportRetryPolicy{MaxAttempts: 2, InitialBackoff: 0, MaxBackoff: 0},
			func(context.Context, time.Duration) error { return nil },
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	response, err := client.Register(context.Background(), api.RegisterWorkerRequest{WorkerID: "worker", Slots: 1})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || response.WorkerID != "worker" {
		t.Fatalf("calls = %d, response = %+v", calls, response)
	}
}

func TestHTTPClientDoesNotRetryHTTPFailure(t *testing.T) {
	calls := 0
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"code":"unavailable","message":"try later"}`)),
		}, nil
	})
	client, err := NewHTTPClient(
		"http://worker.example",
		&http.Client{Transport: transport},
		WithTransportRetry(
			TransportRetryPolicy{MaxAttempts: 3, InitialBackoff: 0, MaxBackoff: 0},
			func(context.Context, time.Duration) error {
				t.Fatal("unexpected retry wait")
				return nil
			},
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Register(context.Background(), api.RegisterWorkerRequest{WorkerID: "worker", Slots: 1})
	if err == nil {
		t.Fatal("expected HTTP error")
	}
	if calls != 1 {
		t.Fatalf("transport calls = %d, want 1", calls)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("response stream failed")
}

func (errorReader) Close() error {
	return nil
}
