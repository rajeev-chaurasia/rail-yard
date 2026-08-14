package telemetry

import (
	"context"
	"errors"
	"sync"
	"time"

	telemetrymodel "github.com/rajeev-chaurasia/rail-yard/internal/telemetry/model"
)

const telemetryEventBatchSize = 1_000

type Source interface {
	TelemetrySnapshot(context.Context) (telemetrymodel.Snapshot, error)
	TelemetryEvents(context.Context, int64, int) ([]telemetrymodel.TimingEvent, error)
}

type Collector struct {
	source   Source
	metrics  *Metrics
	mu       sync.Mutex
	started  bool
	sequence int64
}

func NewCollector(source Source, metrics *Metrics) *Collector {
	return &Collector{source: source, metrics: metrics}
}

func (c *Collector) Refresh(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot, err := c.source.TelemetrySnapshot(ctx)
	if err != nil {
		return err
	}
	c.metrics.SetQueueDepth(QueuePending, snapshot.Pending)
	c.metrics.SetQueueDepth(QueueScheduled, snapshot.Scheduled)
	c.metrics.SetQueueDepth(QueueRunning, snapshot.Running)
	c.metrics.SetQueueDepth(QueueRetrying, snapshot.Retrying)
	c.metrics.SetDLQDepth(snapshot.DLQ)
	if !c.started {
		c.sequence = snapshot.Sequence
		c.started = true
	}

	for {
		events, err := c.source.TelemetryEvents(ctx, c.sequence, telemetryEventBatchSize)
		if err != nil {
			return err
		}
		for _, event := range events {
			c.observe(event)
			c.sequence = event.Sequence
		}
		if len(events) < telemetryEventBatchSize {
			return nil
		}
	}
}

func (c *Collector) Run(
	ctx context.Context,
	interval time.Duration,
	onError func(error),
) {
	if interval <= 0 {
		panic("telemetry collection interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Refresh(ctx); err != nil &&
				!errors.Is(err, context.Canceled) &&
				onError != nil {
				onError(err)
			}
		}
	}
}

func (c *Collector) observe(event telemetrymodel.TimingEvent) {
	if event.ReadyToLease != nil {
		c.metrics.ObserveJobLatency(JobReadyToLease, *event.ReadyToLease)
	}
	if event.LeaseToCompletion != nil {
		c.metrics.ObserveJobLatency(JobLeaseToCompletion, *event.LeaseToCompletion)
	}
	if event.EndToEnd != nil {
		c.metrics.ObserveJobLatency(JobEndToEnd, *event.EndToEnd)
	}
	if event.LeaseRecovery != nil {
		c.metrics.ObserveLeaseRecovery(*event.LeaseRecovery)
	}
}
