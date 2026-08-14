package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/store"
	"github.com/rajeev-chaurasia/rail-yard/internal/trigger"
)

type healthResponse struct {
	Status string `json:"status"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.beginRequest() {
		writeHTTPError(w, &httpError{
			status:  http.StatusServiceUnavailable,
			code:    "shutting_down",
			message: "server is shutting down",
		})
		return
	}
	defer s.endRequest()

	timeout := s.config.RequestTimeout
	if isAcquirePath(r.URL.Path) {
		timeout += s.config.LongPollTimeout
	}
	requestContext, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	s.route(w, r.WithContext(requestContext))
}

func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	segments, validPath := pathSegments(r.URL.Path)
	if !validPath {
		writeHTTPError(w, &httpError{
			status:  http.StatusNotFound,
			code:    "not_found",
			message: "route not found",
		})
		return
	}

	switch {
	case len(segments) == 2 && segments[0] == "health" && segments[1] == "live":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleLive(w)
	case len(segments) == 2 && segments[0] == "health" && segments[1] == "ready":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleReady(w)
	case len(segments) == 2 && segments[0] == "v1" && segments[1] == "jobs":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleSubmitJob(w, r)
	case len(segments) == 3 && segments[0] == "v1" && segments[1] == "jobs":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleGetJob(w, r, segments[2])
	case len(segments) == 2 && segments[0] == "v1" && segments[1] == "workflows":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleSubmitWorkflow(w, r)
	case len(segments) == 3 &&
		segments[0] == "v1" &&
		segments[1] == "triggers" &&
		segments[2] == "cron":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleCreateCronTrigger(w, r)
	case len(segments) == 2 && segments[0] == "v1" && segments[1] == "dead-letters":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		s.handleListDeadLetters(w, r)
	case len(segments) == 4 &&
		segments[0] == "v1" &&
		segments[1] == "dead-letters" &&
		segments[3] == "redrive":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleRedriveDeadLetter(w, r, segments[2])
	case len(segments) == 3 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[2] == "register":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleRegisterWorker(w, r)
	case len(segments) == 5 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "leases" &&
		segments[4] == "acquire":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleAcquire(w, r, segments[2])
	case len(segments) == 4 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "heartbeats":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleHeartbeat(w, r, segments[2])
	case len(segments) == 5 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "attempts" &&
		segments[4] == "start-batch":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleStartAttempts(w, r, segments[2])
	case len(segments) == 5 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "attempts" &&
		segments[4] == "start":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleStartAttempt(w, r, segments[2])
	case len(segments) == 5 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "attempts" &&
		segments[4] == "complete-batch":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleCompleteAttempts(w, r, segments[2])
	case len(segments) == 5 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "attempts" &&
		segments[4] == "complete":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		s.handleCompleteAttempt(w, r, segments[2])
	default:
		writeHTTPError(w, &httpError{
			status:  http.StatusNotFound,
			code:    "not_found",
			message: "route not found",
		})
	}
}

func (s *Server) handleLive(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter) {
	if !s.ready.Load() {
		writeHTTPError(w, &httpError{
			status:  http.StatusServiceUnavailable,
			code:    "not_ready",
			message: "server is not ready",
		})
		return
	}
	writeJSON(w, http.StatusOK, healthResponse{Status: "ready"})
}

func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	key, requestError := idempotencyKey(r)
	if requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.SubmitJobRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	job, err := s.normalizeJob(request.Job)
	if err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}
	request.Job = job

	digest, err := stableDigest(request)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	created, duplicate, err := s.store.SubmitJob(r.Context(), store.Submission{
		Job:            request.Job,
		IdempotencyKey: key,
		RequestDigest:  digest,
	}, s.config.Now().UTC())
	if err != nil {
		writeStoreError(w, err)
		return
	}

	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, api.SubmitJobResponse{
		Job:       created,
		Duplicate: duplicate,
	})
}

func (s *Server) handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
	key, requestError := idempotencyKey(r)
	if requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.SubmitWorkflowRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if err := s.normalizeWorkflow(&request); err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}

	digest, err := stableDigest(request)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	jobs, duplicate, err := s.store.SubmitWorkflow(r.Context(), store.WorkflowSubmission{
		Request:        request,
		IdempotencyKey: key,
		RequestDigest:  digest,
	}, s.config.Now().UTC())
	if err != nil {
		writeStoreError(w, err)
		return
	}

	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, api.SubmitWorkflowResponse{
		Jobs:      jobs,
		Duplicate: duplicate,
	})
}

func (s *Server) handleCreateCronTrigger(w http.ResponseWriter, r *http.Request) {
	if s.config.TriggerStore == nil {
		writeHTTPError(w, &httpError{
			status:  http.StatusServiceUnavailable,
			code:    "triggers_disabled",
			message: "trigger storage is not configured",
		})
		return
	}
	key, requestError := idempotencyKey(r)
	if requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	var request api.CreateCronTriggerRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if request.TenantID == "" {
		request.TenantID = "default"
	}
	request.Job.TenantID = request.TenantID
	job, err := s.normalizeJob(request.Job)
	if err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}
	request.Job = job
	if _, err := trigger.ParseCron(request.Expression); err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}
	digest, err := stableDigest(request)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	created, duplicate, err := s.config.TriggerStore.CreateCronTrigger(
		r.Context(),
		store.CronSubmission{
			Trigger: domain.CronTrigger{
				TenantID:   request.TenantID,
				Expression: request.Expression,
				Job:        request.Job,
			},
			IdempotencyKey: key,
			RequestDigest:  digest,
		},
		s.config.Now().UTC(),
	)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, api.CreateCronTriggerResponse{
		Trigger:   created,
		Duplicate: duplicate,
	})
}

func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	deadLetters, ok := s.store.(store.DeadLetterStore)
	if !ok {
		writeHTTPError(w, &httpError{
			status:  http.StatusNotImplemented,
			code:    "not_supported",
			message: "dead-letter storage is not configured",
		})
		return
	}
	values, err := deadLetters.ListDeadLetters(r.Context(), 100)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.DeadLetterListResponse{DeadLetters: values})
}

func (s *Server) handleRedriveDeadLetter(
	w http.ResponseWriter,
	r *http.Request,
	jobID string,
) {
	deadLetters, ok := s.store.(store.DeadLetterStore)
	if !ok {
		writeHTTPError(w, &httpError{
			status:  http.StatusNotImplemented,
			code:    "not_supported",
			message: "dead-letter storage is not configured",
		})
		return
	}
	if err := validateIdentifier("job_id", jobID, 128); err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}
	key, requestError := idempotencyKey(r)
	if requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	digest, err := stableDigest(struct {
		JobID string `json:"job_id"`
	}{JobID: jobID})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	job, duplicate, err := deadLetters.RedriveDeadLetter(
		r.Context(),
		jobID,
		key,
		digest,
		s.config.Now().UTC(),
	)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, api.RedriveDeadLetterResponse{
		Job:       job,
		Duplicate: duplicate,
	})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request, jobID string) {
	if err := validateIdentifier("job_id", jobID, 128); err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}

	job, err := s.store.GetJob(r.Context(), jobID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var request api.RegisterWorkerRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if err := validateWorkerID(request.WorkerID); err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}
	if request.Slots < 1 || request.Slots > s.config.MaxWorkerSlots {
		writeHTTPError(w, invalidRequest(fmt.Sprintf(
			"slots must be between 1 and %d",
			s.config.MaxWorkerSlots,
		)))
		return
	}

	s.workersMu.Lock()
	existingSlots, registered := s.workers[request.WorkerID]
	if !registered {
		s.workers[request.WorkerID] = request.Slots
	}
	s.workersMu.Unlock()
	if registered && existingSlots != request.Slots {
		writeHTTPError(w, &httpError{
			status:  http.StatusConflict,
			code:    "worker_conflict",
			message: "worker is already registered with a different slot capacity",
		})
		return
	}

	writeJSON(w, http.StatusOK, api.RegisterWorkerResponse{
		WorkerID:       request.WorkerID,
		HeartbeatEvery: s.config.HeartbeatEvery,
		LeaseTTL:       s.config.LeaseTTL,
		ServerTime:     s.config.Now().UTC(),
	})
}

func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request, workerID string) {
	slots, requestError := s.registeredWorker(workerID)
	if requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.AcquireLeasesRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if request.AvailableSlots < 1 || request.AvailableSlots > slots {
		writeHTTPError(w, invalidRequest(fmt.Sprintf(
			"available_slots must be between 1 and the registered capacity of %d",
			slots,
		)))
		return
	}
	if request.Limit < 1 || request.Limit > s.config.MaxLeaseBatch {
		writeHTTPError(w, invalidRequest(fmt.Sprintf(
			"limit must be between 1 and %d",
			s.config.MaxLeaseBatch,
		)))
		return
	}

	longPoll := time.NewTimer(s.config.LongPollTimeout)
	defer longPoll.Stop()
	poll := time.NewTicker(s.config.AcquirePollInterval)
	defer poll.Stop()

	for {
		if s.isDraining() {
			writeHTTPError(w, &httpError{
				status:  http.StatusServiceUnavailable,
				code:    "shutting_down",
				message: "server is shutting down",
			})
			return
		}

		var leases []domain.Lease
		var err error
		if recoveryAware, ok := s.store.(store.RecoveryAwareStore); ok {
			leases, err = recoveryAware.AcquireWithRecoveryReserve(
				r.Context(),
				workerID,
				request.AvailableSlots,
				request.Limit,
				s.config.Now().UTC(),
				s.config.LeaseTTL,
				recoveryReserve(slots),
			)
		} else {
			leases, err = s.store.Acquire(
				r.Context(),
				workerID,
				request.AvailableSlots,
				request.Limit,
				s.config.Now().UTC(),
				s.config.LeaseTTL,
			)
		}
		if err != nil {
			writeStoreError(w, err)
			return
		}
		if len(leases) > 0 {
			writeJSON(w, http.StatusOK, api.AcquireLeasesResponse{Leases: leases})
			return
		}

		select {
		case <-longPoll.C:
			writeJSON(w, http.StatusOK, api.AcquireLeasesResponse{Leases: []domain.Lease{}})
			return
		case <-poll.C:
		case <-s.backgroundContext.Done():
			writeHTTPError(w, &httpError{
				status:  http.StatusServiceUnavailable,
				code:    "shutting_down",
				message: "server is shutting down",
			})
			return
		case <-r.Context().Done():
			writeStoreError(w, r.Context().Err())
			return
		}
	}
}

func recoveryReserve(workerSlots int) int {
	if workerSlots <= 1 {
		return 0
	}
	return (workerSlots + 3) / 4
}

func (s *Server) handleStartAttempt(w http.ResponseWriter, r *http.Request, workerID string) {
	if _, requestError := s.registeredWorker(workerID); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.StartAttemptRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if err := validateLeaseRef(request.LeaseRef); err != nil {
		writeHTTPError(w, invalidRequest(err.Error()))
		return
	}

	if err := s.store.MarkRunning(
		r.Context(),
		workerID,
		request.LeaseRef,
		s.config.Now().UTC(),
	); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStartAttempts(w http.ResponseWriter, r *http.Request, workerID string) {
	if _, requestError := s.registeredWorker(workerID); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.StartAttemptsRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if len(request.Leases) == 0 || len(request.Leases) > s.config.MaxAttemptStartBatch {
		writeHTTPError(w, invalidRequest(fmt.Sprintf(
			"leases must contain between 1 and %d items",
			s.config.MaxAttemptStartBatch,
		)))
		return
	}
	for index, ref := range request.Leases {
		if err := validateLeaseRef(ref); err != nil {
			writeHTTPError(w, invalidRequest(fmt.Sprintf(
				"lease %d: %s",
				index,
				err,
			)))
			return
		}
	}

	now := s.config.Now().UTC()
	var (
		results []store.AttemptStartResult
		err     error
	)
	if batchStore, ok := s.store.(store.BatchAttemptStartStore); ok {
		results, err = batchStore.MarkRunningBatch(r.Context(), workerID, request.Leases, now)
	} else {
		results = make([]store.AttemptStartResult, len(request.Leases))
		for index, ref := range request.Leases {
			results[index].Err = s.store.MarkRunning(r.Context(), workerID, ref, now)
		}
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if len(results) != len(request.Leases) {
		writeStoreError(w, errors.New("attempt start batch result count does not match request"))
		return
	}

	response := api.StartAttemptsResponse{Results: make([]api.StartResult, len(results))}
	for index, result := range results {
		response.Results[index].JobID = request.Leases[index].JobID
		if result.Err == nil {
			response.Results[index].Started = true
			continue
		}
		mapped := mapStoreError(result.Err)
		response.Results[index].Error = &api.ErrorResponse{
			Code:       mapped.code,
			Message:    mapped.message,
			RetryAfter: mapped.retryAfter,
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request, workerID string) {
	if _, requestError := s.registeredWorker(workerID); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.HeartbeatRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if len(request.Leases) == 0 || len(request.Leases) > s.config.MaxHeartbeatBatch {
		writeHTTPError(w, invalidRequest(fmt.Sprintf(
			"leases must contain between 1 and %d items",
			s.config.MaxHeartbeatBatch,
		)))
		return
	}
	for _, lease := range request.Leases {
		if err := validateLeaseRef(lease); err != nil {
			writeHTTPError(w, invalidRequest(err.Error()))
			return
		}
	}

	results, err := s.store.Heartbeat(
		r.Context(),
		workerID,
		request.Leases,
		s.config.Now().UTC(),
		s.config.LeaseTTL,
	)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if results == nil {
		results = []api.HeartbeatResult{}
	}
	writeJSON(w, http.StatusOK, api.HeartbeatResponse{Results: results})
}

func (s *Server) handleCompleteAttempt(w http.ResponseWriter, r *http.Request, workerID string) {
	if _, requestError := s.registeredWorker(workerID); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.CompleteAttemptRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if requestError := normalizeCompletion(workerID, &request.Completion); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	receipt, err := s.store.Complete(
		r.Context(),
		request.Completion,
		s.config.Now().UTC(),
	)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) handleCompleteAttempts(w http.ResponseWriter, r *http.Request, workerID string) {
	if _, requestError := s.registeredWorker(workerID); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}

	var request api.CompleteAttemptsRequest
	if requestError := s.decodeJSON(w, r, &request); requestError != nil {
		writeHTTPError(w, requestError)
		return
	}
	if len(request.Completions) == 0 || len(request.Completions) > s.config.MaxCompletionBatch {
		writeHTTPError(w, invalidRequest(fmt.Sprintf(
			"completions must contain between 1 and %d items",
			s.config.MaxCompletionBatch,
		)))
		return
	}
	for index := range request.Completions {
		if requestError := normalizeCompletion(workerID, &request.Completions[index]); requestError != nil {
			writeHTTPError(w, invalidRequest(fmt.Sprintf(
				"completion %d: %s",
				index,
				requestError.message,
			)))
			return
		}
	}

	now := s.config.Now().UTC()
	var (
		results []store.CompletionResult
		err     error
	)
	if batchStore, ok := s.store.(store.BatchCompletionStore); ok {
		results, err = batchStore.CompleteBatch(r.Context(), request.Completions, now)
	} else {
		results = make([]store.CompletionResult, len(request.Completions))
		for index, completion := range request.Completions {
			results[index].Receipt, results[index].Err = s.store.Complete(
				r.Context(),
				completion,
				now,
			)
		}
	}
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if len(results) != len(request.Completions) {
		writeStoreError(w, errors.New("completion batch result count does not match request"))
		return
	}

	response := api.CompleteAttemptsResponse{
		Results: make([]api.CompletionResult, len(results)),
	}
	for index, result := range results {
		response.Results[index].JobID = request.Completions[index].JobID
		if result.Err == nil {
			receipt := result.Receipt
			response.Results[index].Receipt = &receipt
			continue
		}
		mapped := mapStoreError(result.Err)
		response.Results[index].Error = &api.ErrorResponse{
			Code:       mapped.code,
			Message:    mapped.message,
			RetryAfter: mapped.retryAfter,
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func normalizeCompletion(workerID string, completion *domain.Completion) *httpError {
	if err := validateLeaseRef(completion.LeaseRef); err != nil {
		return invalidRequest(err.Error())
	}
	if completion.WorkerID != "" && completion.WorkerID != workerID {
		return invalidRequest("worker_id must match the worker in the route")
	}
	if completion.OutputDigest == "" {
		return invalidRequest("output_digest is required")
	}
	completion.WorkerID = workerID
	return nil
}

func (s *Server) normalizeJob(spec domain.JobSpec) (domain.JobSpec, error) {
	spec = spec.Normalize()
	if !spec.AvailableAt.IsZero() {
		spec.AvailableAt = spec.AvailableAt.UTC()
	}
	if err := validateIdentifier("tenant_id", spec.TenantID, 128); err != nil {
		return domain.JobSpec{}, err
	}
	if err := validateIdentifier("queue", spec.Queue, 128); err != nil {
		return domain.JobSpec{}, err
	}
	if err := spec.Validate(s.config.MaxSlotCost, s.config.AllowShell); err != nil {
		return domain.JobSpec{}, err
	}

	sort.Strings(spec.DependsOn)
	for index, dependency := range spec.DependsOn {
		if err := validateIdentifier("depends_on", dependency, 128); err != nil {
			return domain.JobSpec{}, err
		}
		if index > 0 && dependency == spec.DependsOn[index-1] {
			return domain.JobSpec{}, fmt.Errorf("depends_on contains duplicate %q", dependency)
		}
	}
	return spec, nil
}

func (s *Server) normalizeWorkflow(request *api.SubmitWorkflowRequest) error {
	if request.TenantID == "" {
		request.TenantID = "default"
	}
	if err := validateIdentifier("tenant_id", request.TenantID, 128); err != nil {
		return err
	}
	if len(request.Nodes) == 0 || len(request.Nodes) > s.config.MaxWorkflowNodes {
		return fmt.Errorf(
			"nodes must contain between 1 and %d items",
			s.config.MaxWorkflowNodes,
		)
	}

	nodeKeys := make(map[string]struct{}, len(request.Nodes))
	for index := range request.Nodes {
		node := &request.Nodes[index]
		if err := validateIdentifier("node key", node.Key, 128); err != nil {
			return err
		}
		if _, exists := nodeKeys[node.Key]; exists {
			return fmt.Errorf("nodes contains duplicate key %q", node.Key)
		}
		nodeKeys[node.Key] = struct{}{}

		if node.Job.TenantID == "" {
			node.Job.TenantID = request.TenantID
		}
		if node.Job.TenantID != request.TenantID {
			return fmt.Errorf("node %q tenant_id must match the workflow tenant_id", node.Key)
		}

		normalized, err := s.normalizeJob(node.Job)
		if err != nil {
			return fmt.Errorf("node %q: %w", node.Key, err)
		}
		node.Job = normalized
	}

	for _, node := range request.Nodes {
		for _, dependency := range node.Job.DependsOn {
			if _, exists := nodeKeys[dependency]; !exists {
				return fmt.Errorf(
					"node %q depends on unknown node %q",
					node.Key,
					dependency,
				)
			}
		}
	}
	sort.Slice(request.Nodes, func(left, right int) bool {
		return request.Nodes[left].Key < request.Nodes[right].Key
	})
	return nil
}

func (s *Server) registeredWorker(workerID string) (int, *httpError) {
	if err := validateWorkerID(workerID); err != nil {
		return 0, invalidRequest(err.Error())
	}

	s.workersMu.RLock()
	slots, registered := s.workers[workerID]
	s.workersMu.RUnlock()
	if !registered {
		return 0, &httpError{
			status:  http.StatusNotFound,
			code:    "not_found",
			message: "worker is not registered",
		}
	}
	return slots, nil
}

func validateLeaseRef(lease domain.LeaseRef) error {
	if err := validateIdentifier("job_id", lease.JobID, 128); err != nil {
		return err
	}
	if lease.AttemptNo < 1 {
		return fmt.Errorf("attempt_no must be positive")
	}
	if lease.Generation < 1 {
		return fmt.Errorf("generation must be positive")
	}
	if lease.Token == "" {
		return fmt.Errorf("token is required")
	}
	return nil
}

func validateWorkerID(workerID string) error {
	if err := validateIdentifier("worker_id", workerID, 128); err != nil {
		return err
	}
	for _, character := range workerID {
		if !isWorkerIDCharacter(character) {
			return fmt.Errorf("worker_id contains an unsupported character")
		}
	}
	return nil
}

func isWorkerIDCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' ||
		character == '-' ||
		character == '_' ||
		character == '.'
}

func validateIdentifier(name, value string, maxBytes int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not have leading or trailing whitespace", name)
	}
	if len(value) > maxBytes {
		return fmt.Errorf("%s exceeds %d bytes", name, maxBytes)
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("%s contains a control character", name)
		}
	}
	return nil
}

func pathSegments(path string) ([]string, bool) {
	if path == "" || path[0] != '/' || path == "/" || strings.HasSuffix(path, "/") {
		return nil, false
	}
	return strings.Split(strings.TrimPrefix(path, "/"), "/"), true
}

func isAcquirePath(path string) bool {
	segments, valid := pathSegments(path)
	return valid &&
		len(segments) == 5 &&
		segments[0] == "v1" &&
		segments[1] == "workers" &&
		segments[3] == "leases" &&
		segments[4] == "acquire"
}
