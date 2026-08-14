package server

import (
	"errors"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

const (
	defaultMaxBodyBytes = 1 << 20
)

type Config struct {
	RequestTimeout             time.Duration
	LongPollTimeout            time.Duration
	AcquirePollInterval        time.Duration
	HeartbeatEvery             time.Duration
	LeaseTTL                   time.Duration
	ReaperInterval             time.Duration
	DuePromotionInterval       time.Duration
	CronInterval               time.Duration
	BackgroundOperationTimeout time.Duration

	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration

	MaxBodyBytes         int64
	MaxHeaderBytes       int
	MaxSlotCost          int
	MaxWorkerSlots       int
	MaxLeaseBatch        int
	MaxAttemptStartBatch int
	MaxHeartbeatBatch    int
	MaxCompletionBatch   int
	MaxWorkflowNodes     int
	ReaperBatchSize      int
	PromotionBatchSize   int
	CronBatchSize        int

	AllowShell   bool
	TriggerStore store.TriggerStore
	Now          func() time.Time
	OnError      func(error)
}

func DefaultConfig() Config {
	return Config{
		RequestTimeout:             5 * time.Second,
		LongPollTimeout:            20 * time.Second,
		AcquirePollInterval:        100 * time.Millisecond,
		HeartbeatEvery:             time.Second,
		LeaseTTL:                   2500 * time.Millisecond,
		ReaperInterval:             250 * time.Millisecond,
		DuePromotionInterval:       250 * time.Millisecond,
		CronInterval:               time.Second,
		BackgroundOperationTimeout: 5 * time.Second,
		ReadTimeout:                10 * time.Second,
		ReadHeaderTimeout:          5 * time.Second,
		WriteTimeout:               30 * time.Second,
		IdleTimeout:                60 * time.Second,
		MaxBodyBytes:               defaultMaxBodyBytes,
		MaxHeaderBytes:             1 << 20,
		MaxSlotCost:                1024,
		MaxWorkerSlots:             1024,
		MaxLeaseBatch:              1024,
		MaxAttemptStartBatch:       128,
		MaxHeartbeatBatch:          1024,
		MaxCompletionBatch:         128,
		MaxWorkflowNodes:           1000,
		ReaperBatchSize:            256,
		PromotionBatchSize:         256,
		CronBatchSize:              64,
		Now:                        time.Now,
		OnError:                    func(error) {},
	}
}

func (c Config) normalized() (Config, error) {
	defaults := DefaultConfig()

	setDurationDefault(&c.RequestTimeout, defaults.RequestTimeout)
	setDurationDefault(&c.LongPollTimeout, defaults.LongPollTimeout)
	setDurationDefault(&c.AcquirePollInterval, defaults.AcquirePollInterval)
	setDurationDefault(&c.HeartbeatEvery, defaults.HeartbeatEvery)
	setDurationDefault(&c.LeaseTTL, defaults.LeaseTTL)
	setDurationDefault(&c.ReaperInterval, defaults.ReaperInterval)
	setDurationDefault(&c.DuePromotionInterval, defaults.DuePromotionInterval)
	setDurationDefault(&c.CronInterval, defaults.CronInterval)
	setDurationDefault(&c.BackgroundOperationTimeout, defaults.BackgroundOperationTimeout)
	setDurationDefault(&c.ReadTimeout, defaults.ReadTimeout)
	setDurationDefault(&c.ReadHeaderTimeout, defaults.ReadHeaderTimeout)
	setDurationDefault(&c.WriteTimeout, defaults.WriteTimeout)
	setDurationDefault(&c.IdleTimeout, defaults.IdleTimeout)

	setInt64Default(&c.MaxBodyBytes, defaults.MaxBodyBytes)
	setIntDefault(&c.MaxHeaderBytes, defaults.MaxHeaderBytes)
	setIntDefault(&c.MaxSlotCost, defaults.MaxSlotCost)
	setIntDefault(&c.MaxWorkerSlots, defaults.MaxWorkerSlots)
	setIntDefault(&c.MaxLeaseBatch, defaults.MaxLeaseBatch)
	setIntDefault(&c.MaxAttemptStartBatch, defaults.MaxAttemptStartBatch)
	setIntDefault(&c.MaxHeartbeatBatch, defaults.MaxHeartbeatBatch)
	setIntDefault(&c.MaxCompletionBatch, defaults.MaxCompletionBatch)
	setIntDefault(&c.MaxWorkflowNodes, defaults.MaxWorkflowNodes)
	setIntDefault(&c.ReaperBatchSize, defaults.ReaperBatchSize)
	setIntDefault(&c.PromotionBatchSize, defaults.PromotionBatchSize)
	setIntDefault(&c.CronBatchSize, defaults.CronBatchSize)

	if c.Now == nil {
		c.Now = defaults.Now
	}
	if c.OnError == nil {
		c.OnError = defaults.OnError
	}

	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) validate() error {
	durations := []time.Duration{
		c.RequestTimeout,
		c.LongPollTimeout,
		c.AcquirePollInterval,
		c.HeartbeatEvery,
		c.LeaseTTL,
		c.ReaperInterval,
		c.DuePromotionInterval,
		c.CronInterval,
		c.BackgroundOperationTimeout,
		c.ReadTimeout,
		c.ReadHeaderTimeout,
		c.WriteTimeout,
		c.IdleTimeout,
	}
	for _, value := range durations {
		if value <= 0 {
			return errors.New("server durations must be positive")
		}
	}
	if c.HeartbeatEvery >= c.LeaseTTL {
		return errors.New("heartbeat interval must be shorter than lease TTL")
	}

	limits := []int{
		c.MaxHeaderBytes,
		c.MaxSlotCost,
		c.MaxWorkerSlots,
		c.MaxLeaseBatch,
		c.MaxAttemptStartBatch,
		c.MaxHeartbeatBatch,
		c.MaxCompletionBatch,
		c.MaxWorkflowNodes,
		c.ReaperBatchSize,
		c.PromotionBatchSize,
		c.CronBatchSize,
	}
	for _, value := range limits {
		if value <= 0 {
			return errors.New("server limits must be positive")
		}
	}
	if c.MaxBodyBytes <= 0 {
		return errors.New("max body bytes must be positive")
	}
	if c.MaxSlotCost > c.MaxWorkerSlots {
		return errors.New("max slot cost must not exceed max worker slots")
	}
	if c.MaxAttemptStartBatch > store.MaxAttemptStartBatchSize {
		return errors.New("max attempt start batch exceeds the store limit")
	}
	if c.MaxCompletionBatch > store.MaxCompletionBatchSize {
		return errors.New("max completion batch exceeds the store limit")
	}
	return nil
}

func setDurationDefault(value *time.Duration, fallback time.Duration) {
	if *value == 0 {
		*value = fallback
	}
}

func setIntDefault(value *int, fallback int) {
	if *value == 0 {
		*value = fallback
	}
}

func setInt64Default(value *int64, fallback int64) {
	if *value == 0 {
		*value = fallback
	}
}
