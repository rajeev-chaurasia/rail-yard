package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/executor"
)

const (
	DefaultHeartbeatInterval     = time.Second
	DefaultShutdownTimeout       = 2 * time.Second
	DefaultCompletionAttempts    = 3
	DefaultAttemptStartBatchSize = 64
	DefaultCompletionBatchSize   = 64
	DefaultCompletionBatchWait   = 2 * time.Millisecond
	defaultWorkerChannelPadding  = 4
)

type Config struct {
	WorkerID              string
	Slots                 int
	LeaseBatch            int
	HeartbeatInterval     time.Duration
	ShutdownTimeout       time.Duration
	MaxCompletionAttempts int
	AttemptStartBatchSize int
	CompletionBatchSize   int
	CompletionBatchWait   time.Duration
}

func (c Config) normalize() (Config, error) {
	if c.WorkerID == "" {
		return Config{}, errors.New("worker ID must not be empty")
	}
	if c.Slots < 1 {
		return Config{}, errors.New("worker slots must be positive")
	}
	if c.LeaseBatch == 0 {
		c.LeaseBatch = c.Slots
	}
	if c.LeaseBatch < 1 {
		return Config{}, errors.New("lease batch must be positive")
	}
	if c.HeartbeatInterval < 0 {
		return Config{}, errors.New("heartbeat interval must not be negative")
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	if c.ShutdownTimeout < 0 {
		return Config{}, errors.New("shutdown timeout must not be negative")
	}
	if c.MaxCompletionAttempts == 0 {
		c.MaxCompletionAttempts = DefaultCompletionAttempts
	}
	if c.MaxCompletionAttempts < 1 {
		return Config{}, errors.New("maximum completion attempts must be positive")
	}
	if c.AttemptStartBatchSize == 0 {
		c.AttemptStartBatchSize = DefaultAttemptStartBatchSize
	}
	if c.AttemptStartBatchSize < 1 {
		return Config{}, errors.New("attempt start batch size must be positive")
	}
	if c.AttemptStartBatchSize > api.MaxAttemptStartBatchSize {
		return Config{}, errors.New("attempt start batch size exceeds the protocol limit")
	}
	if c.CompletionBatchSize == 0 {
		c.CompletionBatchSize = DefaultCompletionBatchSize
	}
	if c.CompletionBatchSize < 1 {
		return Config{}, errors.New("completion batch size must be positive")
	}
	if c.CompletionBatchSize > api.MaxCompletionBatchSize {
		return Config{}, errors.New("completion batch size exceeds the protocol limit")
	}
	if c.CompletionBatchWait == 0 {
		c.CompletionBatchWait = DefaultCompletionBatchWait
	}
	if c.CompletionBatchWait < 0 {
		return Config{}, errors.New("completion batch wait must not be negative")
	}
	return c, nil
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type TickerFactory func(time.Duration) Ticker

type Option func(*Worker)

func WithTickerFactory(factory TickerFactory) Option {
	return func(worker *Worker) {
		if factory != nil {
			worker.newTicker = factory
		}
	}
}

type Worker struct {
	protocol  Protocol
	executor  executor.Executor
	config    Config
	newTicker TickerFactory
}

func New(protocol Protocol, runner executor.Executor, config Config, options ...Option) (*Worker, error) {
	if protocol == nil {
		return nil, errors.New("worker protocol must not be nil")
	}
	if runner == nil {
		return nil, errors.New("worker executor must not be nil")
	}
	normalized, err := config.normalize()
	if err != nil {
		return nil, err
	}

	result := &Worker{
		protocol: protocol,
		executor: runner,
		config:   normalized,
		newTicker: func(duration time.Duration) Ticker {
			return &realTicker{ticker: time.NewTicker(duration)}
		},
	}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	return result, nil
}

func (w *Worker) Run(ctx context.Context) error {
	registration, err := w.protocol.Register(ctx, api.RegisterWorkerRequest{
		WorkerID: w.config.WorkerID,
		Slots:    w.config.Slots,
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("register worker: %w", err)
	}

	workerID := registration.WorkerID
	if workerID == "" {
		workerID = w.config.WorkerID
	}
	heartbeatInterval := w.config.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = registration.HeartbeatEvery
	}
	if heartbeatInterval <= 0 {
		heartbeatInterval = DefaultHeartbeatInterval
	}

	ticker := w.newTicker(heartbeatInterval)
	defer ticker.Stop()

	channelSize := w.config.Slots + defaultWorkerChannelPadding
	acquired := make(chan acquireResult, 1)
	attemptEvents := make(chan attemptEvent, channelSize)
	completionEvents := make(chan completionEvent, channelSize)
	heartbeatEvents := make(chan heartbeatEvent, 1)
	registrationEvents := make(chan registrationEvent, 1)
	active := make(map[attemptKey]*attemptState)
	usedSlots := 0
	acquiring := false
	acquirePaused := false
	heartbeatInFlight := false
	registrationNeeded := false
	registrationInFlight := false
	var registrationEpoch uint64
	var deferredAcquire *acquireResult
	batchStartProtocol, batchStarts := w.protocol.(BatchAttemptStartProtocol)
	batchProtocol, batchCompletions := w.protocol.(BatchCompletionProtocol)
	var completionTimer *time.Timer
	var completionTimerChannel <-chan time.Time
	pendingCompletions := make([]attemptKey, 0, w.config.CompletionBatchSize)
	defer func() {
		if completionTimer != nil {
			completionTimer.Stop()
		}
	}()
	var goroutines sync.WaitGroup

	startRegistration := func() {
		acquirePaused = true
		registrationNeeded = true
		if registrationInFlight {
			return
		}
		registrationInFlight = true
		goroutines.Add(1)
		go func() {
			defer goroutines.Done()
			response, registerErr := w.protocol.Register(ctx, api.RegisterWorkerRequest{
				WorkerID: workerID,
				Slots:    w.config.Slots,
			})
			if registerErr == nil && response.WorkerID != "" && response.WorkerID != workerID {
				registerErr = fmt.Errorf(
					"re-register worker: response worker ID %q does not match %q",
					response.WorkerID,
					workerID,
				)
			}
			select {
			case registrationEvents <- registrationEvent{err: registerErr}:
			case <-ctx.Done():
			}
		}()
	}

	removeAttempt := func(key attemptKey) {
		state, ok := active[key]
		if !ok {
			return
		}
		state.cancel()
		usedSlots -= state.lease.SlotCost
		delete(active, key)
	}

	startAcquire := func() {
		available := w.config.Slots - usedSlots
		if acquiring || acquirePaused || registrationNeeded || available <= 0 {
			return
		}
		acquiring = true
		epoch := registrationEpoch
		goroutines.Add(1)
		go func(availableSlots int, requestEpoch uint64) {
			defer goroutines.Done()
			response, acquireErr := w.protocol.AcquireLeases(ctx, workerID, api.AcquireLeasesRequest{
				AvailableSlots: availableSlots,
				Limit:          w.config.LeaseBatch,
			})
			select {
			case acquired <- acquireResult{response: response, err: acquireErr, epoch: requestEpoch}:
			case <-ctx.Done():
			}
		}(available, epoch)
	}

	executeAttempt := func(lease domain.Lease, key attemptKey, attemptCtx context.Context) {
		goroutines.Add(1)
		go func() {
			defer goroutines.Done()
			result := w.executor.Execute(attemptCtx, executor.Request{
				Payload:        lease.Payload,
				IdempotencyKey: lease.IdempotencyKey,
			})
			select {
			case attemptEvents <- attemptEvent{key: key, result: result, executed: true}:
			case <-ctx.Done():
			}
		}()
	}

	startAttempt := func(key attemptKey, state *attemptState) {
		state.startInFlight = true
		epoch := registrationEpoch
		goroutines.Add(1)
		go func(requestEpoch uint64) {
			defer goroutines.Done()
			startErr := w.protocol.StartAttempt(
				state.ctx,
				workerID,
				api.StartAttemptRequest{LeaseRef: leaseRef(state.lease)},
			)
			select {
			case attemptEvents <- attemptEvent{
				key:      key,
				startErr: startErr,
				started:  true,
				epoch:    requestEpoch,
			}:
			case <-ctx.Done():
			}
		}(epoch)
	}

	startAttemptBatch := func(keys []attemptKey, refs []domain.LeaseRef) {
		for _, key := range keys {
			if state, ok := active[key]; ok {
				state.startInFlight = true
			}
		}
		epoch := registrationEpoch
		goroutines.Add(1)
		go func(requestEpoch uint64) {
			defer goroutines.Done()
			response, batchErr := batchStartProtocol.StartAttempts(
				ctx,
				workerID,
				api.StartAttemptsRequest{Leases: refs},
			)
			errs := attemptStartBatchErrors(response, refs, batchErr)
			for index, key := range keys {
				select {
				case attemptEvents <- attemptEvent{
					key:      key,
					startErr: errs[index],
					started:  true,
					epoch:    requestEpoch,
				}:
				case <-ctx.Done():
					return
				}
			}
		}(epoch)
	}

	startAttempts := func(keys []attemptKey) {
		for offset := 0; offset < len(keys); offset += w.config.AttemptStartBatchSize {
			end := min(offset+w.config.AttemptStartBatchSize, len(keys))
			chunk := keys[offset:end]
			if !batchStarts {
				for _, key := range chunk {
					startAttempt(key, active[key])
				}
				continue
			}
			refs := make([]domain.LeaseRef, len(chunk))
			for index, key := range chunk {
				refs[index] = leaseRef(active[key].lease)
			}
			startAttemptBatch(append([]attemptKey(nil), chunk...), refs)
		}
	}

	startCompletion := func(key attemptKey, state *attemptState) {
		state.phase = attemptCompleting
		state.completionAttempts++
		completion := completionFor(workerID, state.lease, state.result)
		attemptCtx := state.ctx
		epoch := registrationEpoch
		goroutines.Add(1)
		go func(requestEpoch uint64) {
			defer goroutines.Done()
			_, completeErr := w.protocol.CompleteAttempt(attemptCtx, workerID, api.CompleteAttemptRequest{
				Completion: completion,
			})
			select {
			case completionEvents <- completionEvent{
				keys:  []attemptKey{key},
				errs:  []error{completeErr},
				epoch: requestEpoch,
			}:
			case <-ctx.Done():
			}
		}(epoch)
	}

	startCompletionBatch := func(keys []attemptKey, completions []domain.Completion) {
		epoch := registrationEpoch
		goroutines.Add(1)
		go func(requestEpoch uint64) {
			defer goroutines.Done()
			response, batchErr := batchProtocol.CompleteAttempts(
				ctx,
				workerID,
				api.CompleteAttemptsRequest{Completions: completions},
			)
			errs := completionBatchErrors(response, completions, batchErr)
			select {
			case completionEvents <- completionEvent{keys: keys, errs: errs, epoch: requestEpoch}:
			case <-ctx.Done():
			}
		}(epoch)
	}

	flushCompletions := func() {
		if completionTimer != nil {
			if !completionTimer.Stop() {
				select {
				case <-completionTimer.C:
				default:
				}
			}
			completionTimer = nil
			completionTimerChannel = nil
		}
		if len(pendingCompletions) == 0 {
			return
		}

		keys := make([]attemptKey, 0, len(pendingCompletions))
		completions := make([]domain.Completion, 0, len(pendingCompletions))
		for _, key := range pendingCompletions {
			state, ok := active[key]
			if !ok || state.phase != attemptReadyToComplete {
				continue
			}
			state.phase = attemptCompleting
			state.completionAttempts++
			keys = append(keys, key)
			completions = append(completions, completionFor(workerID, state.lease, state.result))
		}
		pendingCompletions = pendingCompletions[:0]
		if len(keys) > 0 {
			startCompletionBatch(keys, completions)
		}
	}

	queueCompletion := func(key attemptKey, state *attemptState) {
		if !batchCompletions {
			startCompletion(key, state)
			return
		}
		pendingCompletions = append(pendingCompletions, key)
		if len(pendingCompletions) >= w.config.CompletionBatchSize ||
			w.config.CompletionBatchWait == 0 {
			flushCompletions()
			return
		}
		if completionTimer == nil {
			completionTimer = time.NewTimer(w.config.CompletionBatchWait)
			completionTimerChannel = completionTimer.C
		}
	}

	startHeartbeat := func() {
		if heartbeatInFlight || registrationNeeded {
			return
		}
		refs := make([]domain.LeaseRef, 0, len(active))
		for _, state := range active {
			refs = append(refs, leaseRef(state.lease))
		}
		heartbeatInFlight = true
		epoch := registrationEpoch
		goroutines.Add(1)
		go func(requested []domain.LeaseRef, requestEpoch uint64) {
			defer goroutines.Done()
			heartbeatCtx, cancel := context.WithTimeout(ctx, heartbeatInterval)
			defer cancel()
			response, heartbeatErr := w.protocol.Heartbeat(heartbeatCtx, workerID, api.HeartbeatRequest{
				Leases: requested,
			})
			select {
			case heartbeatEvents <- heartbeatEvent{
				requested: requested,
				response:  response,
				err:       heartbeatErr,
				epoch:     requestEpoch,
			}:
			case <-ctx.Done():
			}
		}(refs, epoch)
	}

	processAcquired := func(result acquireResult) {
		if result.err != nil {
			if workerRegistrationMissing(result.err) {
				if result.epoch == registrationEpoch || registrationNeeded {
					startRegistration()
				} else {
					acquirePaused = false
					startAcquire()
				}
				return
			}
			acquirePaused = true
			return
		}

		remaining := w.config.Slots - usedSlots
		startKeys := make([]attemptKey, 0, len(result.response.Leases))
		for _, lease := range result.response.Leases {
			key := keyFor(lease)
			if lease.SlotCost < 1 || lease.SlotCost > remaining {
				continue
			}
			if lease.WorkerID != "" && lease.WorkerID != workerID {
				continue
			}
			if _, exists := active[key]; exists || hasActiveJob(active, lease.JobID) {
				continue
			}

			attemptCtx, cancel := context.WithCancel(ctx)
			state := &attemptState{
				lease:  lease,
				ctx:    attemptCtx,
				cancel: cancel,
				phase:  attemptStarting,
			}
			active[key] = state
			usedSlots += lease.SlotCost
			remaining -= lease.SlotCost
			startKeys = append(startKeys, key)
		}
		startAttempts(startKeys)
		startAcquire()
	}

	startAcquire()
	for {
		if ctx.Err() != nil {
			w.shutdown(workerID, active, &goroutines)
			return nil
		}

		select {
		case <-ctx.Done():
			w.shutdown(workerID, active, &goroutines)
			return nil

		case result := <-acquired:
			acquiring = false
			if ctx.Err() != nil {
				w.shutdown(workerID, active, &goroutines)
				return nil
			}
			if result.err == nil && registrationNeeded {
				pending := result
				deferredAcquire = &pending
				continue
			}
			processAcquired(result)

		case event := <-attemptEvents:
			state, ok := active[event.key]
			if !ok {
				continue
			}
			if event.started {
				state.startInFlight = false
			}
			if event.startErr != nil {
				if workerRegistrationMissing(event.startErr) {
					if event.epoch < registrationEpoch && !registrationNeeded {
						startAttempts([]attemptKey{event.key})
					} else {
						state.startRetryPending = true
						startRegistration()
					}
					continue
				}
				removeAttempt(event.key)
				startAcquire()
				continue
			}
			if event.started {
				if state.phase != attemptStarting {
					continue
				}
				if registrationNeeded {
					state.executePending = true
					continue
				}
				state.phase = attemptExecuting
				executeAttempt(state.lease, event.key, state.ctx)
				continue
			}
			if !event.executed || state.phase != attemptExecuting {
				continue
			}
			state.result = event.result
			state.phase = attemptReadyToComplete
			queueCompletion(event.key, state)

		case event := <-completionEvents:
			for index, key := range event.keys {
				state, ok := active[key]
				if !ok {
					continue
				}
				completionErr := event.errs[index]
				if completionErr == nil || errors.Is(completionErr, domain.ErrStaleLease) {
					removeAttempt(key)
					continue
				}
				if workerRegistrationMissing(completionErr) {
					if state.completionAttempts > 0 {
						state.completionAttempts--
					}
					state.phase = attemptReadyToComplete
					if event.epoch < registrationEpoch && !registrationNeeded {
						queueCompletion(key, state)
					} else {
						state.completionRetryPending = true
						startRegistration()
					}
					continue
				}
				if retryableProtocolError(completionErr) &&
					state.completionAttempts < w.config.MaxCompletionAttempts {
					state.phase = attemptReadyToComplete
					queueCompletion(key, state)
					continue
				}
				removeAttempt(key)
			}
			startAcquire()

		case event := <-heartbeatEvents:
			heartbeatInFlight = false
			if event.err != nil {
				if workerRegistrationMissing(event.err) {
					if event.epoch < registrationEpoch && !registrationNeeded {
						startHeartbeat()
					} else {
						startRegistration()
					}
				}
				continue
			}
			for _, result := range event.response.Results {
				if result.Accepted {
					continue
				}
				for _, reference := range event.requested {
					if reference.JobID != result.JobID {
						continue
					}
					key := keyFromRef(reference)
					if state, ok := active[key]; ok && sameLease(leaseRef(state.lease), reference) {
						removeAttempt(key)
					}
				}
			}
			startAcquire()

		case event := <-registrationEvents:
			registrationInFlight = false
			if event.err != nil {
				if ctx.Err() != nil {
					w.shutdown(workerID, active, &goroutines)
					return nil
				}
				continue
			}

			registrationNeeded = false
			acquirePaused = false
			registrationEpoch++

			retryStarts := make([]attemptKey, 0)
			for key, state := range active {
				if state.startRetryPending && !state.startInFlight {
					state.startRetryPending = false
					retryStarts = append(retryStarts, key)
				}
				if state.executePending {
					state.executePending = false
					state.phase = attemptExecuting
					executeAttempt(state.lease, key, state.ctx)
				}
				if state.completionRetryPending {
					state.completionRetryPending = false
					state.phase = attemptReadyToComplete
					queueCompletion(key, state)
				}
			}
			startAttempts(retryStarts)
			startHeartbeat()
			if deferredAcquire != nil {
				pending := *deferredAcquire
				deferredAcquire = nil
				processAcquired(pending)
			}
			startAcquire()

		case <-completionTimerChannel:
			completionTimer = nil
			completionTimerChannel = nil
			flushCompletions()

		case <-ticker.C():
			if registrationNeeded {
				startRegistration()
				continue
			}
			acquirePaused = false
			startHeartbeat()
			startAcquire()
		}
	}
}

func (w *Worker) shutdown(workerID string, active map[attemptKey]*attemptState, goroutines *sync.WaitGroup) {
	references := make([]domain.LeaseRef, 0, len(active))
	for _, state := range active {
		references = append(references, leaseRef(state.lease))
		state.cancel()
	}

	if len(references) > 0 {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), w.config.ShutdownTimeout)
		_, _ = w.protocol.Heartbeat(shutdownCtx, workerID, api.HeartbeatRequest{Leases: references})
		cancel()
	}
	goroutines.Wait()
}

type realTicker struct {
	ticker *time.Ticker
}

func (t *realTicker) C() <-chan time.Time {
	return t.ticker.C
}

func (t *realTicker) Stop() {
	t.ticker.Stop()
}

type attemptPhase uint8

const (
	attemptStarting attemptPhase = iota
	attemptExecuting
	attemptReadyToComplete
	attemptCompleting
)

type attemptKey struct {
	jobID      string
	attemptNo  int
	generation int64
}

type attemptState struct {
	lease                  domain.Lease
	ctx                    context.Context
	cancel                 context.CancelFunc
	phase                  attemptPhase
	result                 executor.Result
	completionAttempts     int
	startInFlight          bool
	startRetryPending      bool
	executePending         bool
	completionRetryPending bool
}

type acquireResult struct {
	response api.AcquireLeasesResponse
	err      error
	epoch    uint64
}

type attemptEvent struct {
	key      attemptKey
	result   executor.Result
	startErr error
	started  bool
	executed bool
	epoch    uint64
}

type completionEvent struct {
	keys  []attemptKey
	errs  []error
	epoch uint64
}

type heartbeatEvent struct {
	requested []domain.LeaseRef
	response  api.HeartbeatResponse
	err       error
	epoch     uint64
}

type registrationEvent struct {
	err error
}

func keyFor(lease domain.Lease) attemptKey {
	return attemptKey{
		jobID:      lease.JobID,
		attemptNo:  lease.AttemptNo,
		generation: lease.Generation,
	}
}

func keyFromRef(reference domain.LeaseRef) attemptKey {
	return attemptKey{
		jobID:      reference.JobID,
		attemptNo:  reference.AttemptNo,
		generation: reference.Generation,
	}
}

func leaseRef(lease domain.Lease) domain.LeaseRef {
	return domain.LeaseRef{
		JobID:      lease.JobID,
		AttemptNo:  lease.AttemptNo,
		Generation: lease.Generation,
		Token:      lease.Token,
	}
}

func sameLease(left domain.LeaseRef, right domain.LeaseRef) bool {
	return left.JobID == right.JobID &&
		left.AttemptNo == right.AttemptNo &&
		left.Generation == right.Generation &&
		left.Token == right.Token
}

func hasActiveJob(active map[attemptKey]*attemptState, jobID string) bool {
	for _, state := range active {
		if state.lease.JobID == jobID {
			return true
		}
	}
	return false
}

func completionFor(workerID string, lease domain.Lease, result executor.Result) domain.Completion {
	failure := result.Failure
	if !result.Success && failure == nil {
		failure = &domain.Failure{
			Class:        "execution_failed",
			Message:      "executor returned an unsuccessful result without failure details",
			ExitCode:     result.ExitCode,
			OutputDigest: result.OutputDigest,
			Stderr:       result.Stderr,
		}
	}
	return domain.Completion{
		LeaseRef:     leaseRef(lease),
		WorkerID:     workerID,
		Success:      result.Success,
		Retryable:    result.Retryable,
		OutputDigest: result.OutputDigest,
		Failure:      failure,
	}
}

func attemptStartBatchErrors(
	response api.StartAttemptsResponse,
	refs []domain.LeaseRef,
	batchErr error,
) []error {
	errs := make([]error, len(refs))
	if batchErr != nil {
		for index := range errs {
			errs[index] = batchErr
		}
		return errs
	}
	if len(response.Results) != len(refs) {
		err := invalidBatchResponse("attempt start batch response length does not match request")
		for index := range errs {
			errs[index] = err
		}
		return errs
	}
	for index, result := range response.Results {
		if result.JobID != refs[index].JobID ||
			result.Started == (result.Error != nil) {
			errs[index] = invalidBatchResponse("attempt start batch response item is malformed")
			continue
		}
		if result.Error != nil {
			errs[index] = protocolItemError(result.Error)
		}
	}
	return errs
}

func completionBatchErrors(
	response api.CompleteAttemptsResponse,
	completions []domain.Completion,
	batchErr error,
) []error {
	errs := make([]error, len(completions))
	if batchErr != nil {
		for index := range errs {
			errs[index] = batchErr
		}
		return errs
	}
	if len(response.Results) != len(completions) {
		err := invalidBatchResponse("completion batch response length does not match request")
		for index := range errs {
			errs[index] = err
		}
		return errs
	}
	for index, result := range response.Results {
		if result.JobID != completions[index].JobID ||
			(result.Receipt == nil) == (result.Error == nil) {
			errs[index] = invalidBatchResponse("completion batch response item is malformed")
			continue
		}
		if result.Error == nil {
			if result.Receipt.JobID != completions[index].JobID {
				errs[index] = invalidBatchResponse("completion receipt does not match request")
			}
			continue
		}
		errs[index] = protocolItemError(result.Error)
	}
	return errs
}

func invalidBatchResponse(message string) error {
	return &APIError{
		StatusCode: http.StatusBadGateway,
		Code:       "invalid_batch_response",
		Message:    message,
	}
}

func protocolItemError(response *api.ErrorResponse) error {
	if response.Code == "stale_lease" {
		return domain.ErrStaleLease
	}
	status := http.StatusConflict
	if response.Code == "internal_error" {
		status = http.StatusInternalServerError
	}
	return &APIError{
		StatusCode: status,
		Code:       response.Code,
		Message:    response.Message,
		RetryAfter: time.Duration(response.RetryAfter) * time.Second,
	}
}

func retryableProtocolError(err error) bool {
	var transportErr *TransportError
	if errors.As(err, &transportErr) {
		return true
	}
	var apiErr *APIError
	return errors.As(err, &apiErr) &&
		(apiErr.StatusCode == http.StatusTooManyRequests || apiErr.StatusCode >= http.StatusInternalServerError)
}
