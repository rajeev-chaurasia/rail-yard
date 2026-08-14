package p1

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type canonicalCompletion struct {
	command ModelCompletion
	receipt ModelReceipt
}

type Model struct {
	mu sync.Mutex

	clock       Clock
	jobs        map[string]*ModelJob
	submissions map[string]string
	completions map[string]canonicalCompletion
	events      []ModelEvent
	nextJob     int64
	nextEvent   int64
	nextReady   int64
}

func NewModel(clock Clock) *Model {
	if clock == nil {
		panic("p1 model requires a clock")
	}
	return &Model{
		clock:       clock,
		jobs:        make(map[string]*ModelJob),
		submissions: make(map[string]string),
		completions: make(map[string]canonicalCompletion),
	}
}

func (m *Model) Submit(submission ModelSubmission) (ModelJob, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	submission = normalizeSubmission(submission, m.clock.Now())
	key := submission.TenantID + "\x00" + submission.SubmissionKey
	if jobID, found := m.submissions[key]; found {
		job := m.jobs[jobID]
		if job.RequestDigest != submission.RequestDigest {
			return ModelJob{}, false, ErrIdempotencyConflict
		}
		return cloneJob(job), true, nil
	}

	m.nextJob++
	m.nextReady++
	now := m.clock.Now()
	job := &ModelJob{
		ID:              fmt.Sprintf("job-%06d", m.nextJob),
		TenantID:        submission.TenantID,
		SubmissionKey:   submission.SubmissionKey,
		RequestDigest:   submission.RequestDigest,
		State:           ModelPending,
		SlotCost:        submission.SlotCost,
		MaxAttempts:     submission.MaxAttempts,
		Retryable:       submission.Retryable,
		ReadySequence:   m.nextReady,
		AvailableAt:     submission.AvailableAt,
		StateVersion:    0,
		AttemptNo:       0,
		LeaseGeneration: 0,
	}
	m.jobs[job.ID] = job
	m.submissions[key] = job.ID
	m.transitionLocked(job, "submitted", "", ModelPending, now)
	return cloneJob(job), false, nil
}

func normalizeSubmission(submission ModelSubmission, now time.Time) ModelSubmission {
	if submission.TenantID == "" {
		submission.TenantID = "default"
	}
	if submission.SlotCost == 0 {
		submission.SlotCost = 1
	}
	if submission.MaxAttempts == 0 {
		submission.MaxAttempts = 5
	}
	if submission.AvailableAt.IsZero() {
		submission.AvailableAt = now
	} else {
		submission.AvailableAt = submission.AvailableAt.UTC()
	}
	return submission
}

func (m *Model) Acquire(workerID string, availableSlots, limit int, ttl time.Duration) []ModelLease {
	m.mu.Lock()
	defer m.mu.Unlock()

	if workerID == "" || availableSlots <= 0 || limit <= 0 || ttl <= 0 {
		return nil
	}

	now := m.clock.Now()
	candidates := make([]*ModelJob, 0, len(m.jobs))
	for _, job := range m.jobs {
		if (job.State == ModelPending || job.State == ModelRetrying) &&
			!job.AvailableAt.After(now) && job.SlotCost <= availableSlots {
			candidates = append(candidates, job)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ReadySequence != candidates[j].ReadySequence {
			return candidates[i].ReadySequence < candidates[j].ReadySequence
		}
		return candidates[i].ID < candidates[j].ID
	})

	leases := make([]ModelLease, 0, limit)
	for _, job := range candidates {
		if len(leases) == limit || job.SlotCost > availableSlots {
			continue
		}

		from := job.State
		job.AttemptNo++
		job.LeaseGeneration++
		lease := ModelLease{
			ModelLeaseRef: ModelLeaseRef{
				JobID:      job.ID,
				AttemptNo:  job.AttemptNo,
				Generation: job.LeaseGeneration,
				Token:      fmt.Sprintf("token-%s-%d", job.ID, job.LeaseGeneration),
			},
			WorkerID:  workerID,
			SlotCost:  job.SlotCost,
			ExpiresAt: now.Add(ttl),
		}
		job.ActiveLease = &lease
		job.Attempts = append(job.Attempts, ModelAttempt{
			AttemptNo:  job.AttemptNo,
			Generation: job.LeaseGeneration,
			WorkerID:   workerID,
			State:      "LEASED",
			ExpiresAt:  lease.ExpiresAt,
		})
		m.transitionLocked(job, "lease_acquired", from, ModelScheduled, now)
		leases = append(leases, lease)
		availableSlots -= job.SlotCost
	}
	return leases
}

func (m *Model) Start(workerID string, ref ModelLeaseRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, err := m.currentLeaseLocked(workerID, ref, m.clock.Now())
	if err != nil {
		return err
	}
	if job.State == ModelRunning {
		return nil
	}
	if job.State != ModelScheduled {
		return ErrInvalidTransition
	}

	m.updateAttemptLocked(job, "RUNNING", job.ActiveLease.ExpiresAt)
	m.transitionLocked(job, "attempt_started", ModelScheduled, ModelRunning, m.clock.Now())
	return nil
}

func (m *Model) Heartbeat(workerID string, refs []ModelLeaseRef, ttl time.Duration) []ModelHeartbeat {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.clock.Now()
	results := make([]ModelHeartbeat, 0, len(refs))
	for _, ref := range refs {
		job, err := m.currentLeaseLocked(workerID, ref, now)
		if err != nil || ttl <= 0 {
			results = append(results, ModelHeartbeat{JobID: ref.JobID})
			continue
		}

		expiresAt := now.Add(ttl)
		job.ActiveLease.ExpiresAt = expiresAt
		m.updateAttemptLocked(job, job.Attempts[len(job.Attempts)-1].State, expiresAt)
		m.transitionLocked(job, "lease_heartbeat", job.State, job.State, now)
		results = append(results, ModelHeartbeat{
			JobID:     ref.JobID,
			Accepted:  true,
			ExpiresAt: expiresAt,
		})
	}
	return results
}

func (m *Model) Complete(completion ModelCompletion) (ModelReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if canonical, found := m.completions[completion.JobID]; found {
		if canonical.command == completion {
			receipt := canonical.receipt
			receipt.Duplicate = true
			return receipt, nil
		}
		if sameLease(canonical.command, completion) {
			return ModelReceipt{}, ErrTerminalConflict
		}
		return ModelReceipt{}, ErrStaleLease
	}

	now := m.clock.Now()
	job, err := m.currentLeaseLocked(completion.WorkerID, completion.ModelLeaseRef, now)
	if err != nil {
		return ModelReceipt{}, err
	}
	if job.State != ModelRunning {
		return ModelReceipt{}, ErrInvalidTransition
	}

	from := job.State
	next := ModelFailed
	attemptState := "FAILED"
	if completion.Success {
		next = ModelSucceeded
		attemptState = "SUCCEEDED"
	} else if completion.Retryable && job.Retryable && job.AttemptNo < job.MaxAttempts {
		next = ModelRetrying
		m.nextReady++
		job.ReadySequence = m.nextReady
		job.AvailableAt = now
	} else if completion.Retryable && job.Retryable {
		next = ModelDeadLetter
	}

	m.updateAttemptLocked(job, attemptState, job.ActiveLease.ExpiresAt)
	job.ActiveLease = nil
	m.transitionLocked(job, "attempt_completed", from, next, now)
	receipt := ModelReceipt{
		JobID:        job.ID,
		State:        job.State,
		StateVersion: job.StateVersion,
		CommittedAt:  now,
	}
	if next.Terminal() {
		job.CanonicalReceipt = &receipt
		m.completions[job.ID] = canonicalCompletion{
			command: completion,
			receipt: receipt,
		}
	}
	return receipt, nil
}

func sameLease(left, right ModelCompletion) bool {
	return left.ModelLeaseRef == right.ModelLeaseRef && left.WorkerID == right.WorkerID
}

func (m *Model) ReapExpired(limit int) []ModelReapedLease {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		return nil
	}
	now := m.clock.Now()
	expired := make([]*ModelJob, 0)
	for _, job := range m.jobs {
		if job.ActiveLease != nil && !now.Before(job.ActiveLease.ExpiresAt) {
			expired = append(expired, job)
		}
	}
	sort.Slice(expired, func(i, j int) bool {
		left := expired[i].ActiveLease
		right := expired[j].ActiveLease
		if !left.ExpiresAt.Equal(right.ExpiresAt) {
			return left.ExpiresAt.Before(right.ExpiresAt)
		}
		return expired[i].ID < expired[j].ID
	})

	if len(expired) > limit {
		expired = expired[:limit]
	}
	reaped := make([]ModelReapedLease, 0, len(expired))
	for _, job := range expired {
		lease := *job.ActiveLease
		next := ModelRetrying
		if job.AttemptNo >= job.MaxAttempts {
			next = ModelDeadLetter
		} else {
			m.nextReady++
			job.ReadySequence = m.nextReady
			job.AvailableAt = now
		}

		m.updateAttemptLocked(job, "EXPIRED", lease.ExpiresAt)
		job.ActiveLease = nil
		m.transitionLocked(job, "lease_reaped", job.State, next, now)
		if next.Terminal() {
			receipt := ModelReceipt{
				JobID:        job.ID,
				State:        job.State,
				StateVersion: job.StateVersion,
				CommittedAt:  now,
			}
			job.CanonicalReceipt = &receipt
			m.completions[job.ID] = canonicalCompletion{receipt: receipt}
		}
		reaped = append(reaped, ModelReapedLease{
			JobID:           job.ID,
			OldWorkerID:     lease.WorkerID,
			Generation:      lease.Generation,
			ExpiredAt:       lease.ExpiresAt,
			NextAvailableAt: job.AvailableAt,
		})
	}
	return reaped
}

func (m *Model) Job(jobID string) (ModelJob, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, found := m.jobs[jobID]
	if !found {
		return ModelJob{}, false
	}
	return cloneJob(job), true
}

func (m *Model) Snapshot() ModelSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *Model) Restart(clock Clock) *Model {
	m.mu.Lock()
	defer m.mu.Unlock()

	restarted := NewModel(clock)
	restarted.nextJob = m.nextJob
	restarted.nextEvent = m.nextEvent
	restarted.nextReady = m.nextReady
	restarted.events = append([]ModelEvent(nil), m.events...)
	for key, jobID := range m.submissions {
		restarted.submissions[key] = jobID
	}
	for jobID, completion := range m.completions {
		restarted.completions[jobID] = completion
	}
	for jobID, job := range m.jobs {
		copy := cloneJob(job)
		restarted.jobs[jobID] = &copy
	}
	return restarted
}

func (m *Model) Validate() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for index, event := range m.events {
		want := int64(index + 1)
		if event.Sequence != want {
			return fmt.Errorf("event sequence at index %d: got %d, want %d", index, event.Sequence, want)
		}
	}

	for jobID, job := range m.jobs {
		if err := validateJob(job, m.events); err != nil {
			return fmt.Errorf("job %s: %w", jobID, err)
		}
		key := job.TenantID + "\x00" + job.SubmissionKey
		if m.submissions[key] != jobID {
			return fmt.Errorf("submission index points away from job")
		}
		_, canonical := m.completions[jobID]
		if job.State.Terminal() != canonical {
			return fmt.Errorf("terminal state and canonical completion disagree")
		}
	}
	return nil
}

func validateJob(job *ModelJob, events []ModelEvent) error {
	var (
		state   ModelState
		version int64
	)
	for _, event := range events {
		if event.JobID != job.ID {
			continue
		}
		if event.StateVersion != version+1 {
			return fmt.Errorf("state version jumped from %d to %d", version, event.StateVersion)
		}
		if event.From != state {
			return fmt.Errorf("event fold expected from %q, got %q", state, event.From)
		}
		state = event.To
		version = event.StateVersion
	}
	if state != job.State || version != job.StateVersion {
		return fmt.Errorf("event fold produced state %s version %d, materialized state is %s version %d",
			state, version, job.State, job.StateVersion)
	}

	var lastAttempt int
	var lastGeneration int64
	for _, attempt := range job.Attempts {
		if attempt.AttemptNo <= lastAttempt || attempt.Generation <= lastGeneration {
			return fmt.Errorf("attempt or generation did not strictly increase")
		}
		lastAttempt = attempt.AttemptNo
		lastGeneration = attempt.Generation
	}
	if job.State.Terminal() && job.ActiveLease != nil {
		return fmt.Errorf("terminal job retains active lease")
	}
	if (job.State == ModelScheduled || job.State == ModelRunning) != (job.ActiveLease != nil) {
		return fmt.Errorf("active state and lease presence disagree")
	}
	return nil
}

func (m *Model) currentLeaseLocked(workerID string, ref ModelLeaseRef, now time.Time) (*ModelJob, error) {
	job, found := m.jobs[ref.JobID]
	if !found || job.ActiveLease == nil {
		return nil, ErrStaleLease
	}
	lease := job.ActiveLease
	if lease.WorkerID != workerID ||
		lease.ModelLeaseRef != ref ||
		!now.Before(lease.ExpiresAt) {
		return nil, ErrStaleLease
	}
	return job, nil
}

func (m *Model) updateAttemptLocked(job *ModelJob, state string, expiresAt time.Time) {
	index := len(job.Attempts) - 1
	job.Attempts[index].State = state
	job.Attempts[index].ExpiresAt = expiresAt
}

func (m *Model) transitionLocked(
	job *ModelJob,
	kind string,
	from, to ModelState,
	at time.Time,
) {
	job.State = to
	job.StateVersion++
	m.nextEvent++
	m.events = append(m.events, ModelEvent{
		Sequence:     m.nextEvent,
		JobID:        job.ID,
		Kind:         kind,
		From:         from,
		To:           to,
		StateVersion: job.StateVersion,
		AttemptNo:    job.AttemptNo,
		Generation:   job.LeaseGeneration,
		At:           at.UTC(),
	})
}

func (m *Model) snapshotLocked() ModelSnapshot {
	snapshot := ModelSnapshot{
		Jobs:   make([]ModelJob, 0, len(m.jobs)),
		Events: append([]ModelEvent(nil), m.events...),
	}
	for _, job := range m.jobs {
		snapshot.Jobs = append(snapshot.Jobs, cloneJob(job))
	}
	sort.Slice(snapshot.Jobs, func(i, j int) bool {
		return snapshot.Jobs[i].ID < snapshot.Jobs[j].ID
	})
	return snapshot
}

func cloneJob(job *ModelJob) ModelJob {
	copy := *job
	if job.ActiveLease != nil {
		lease := *job.ActiveLease
		copy.ActiveLease = &lease
	}
	copy.Attempts = append([]ModelAttempt(nil), job.Attempts...)
	if job.CanonicalReceipt != nil {
		receipt := *job.CanonicalReceipt
		copy.CanonicalReceipt = &receipt
	}
	return copy
}
