package trigger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDelivery struct {
	TriggerID string
	Stream    string
	MessageID string
	Values    map[string]any
}

func (delivery RedisDelivery) IdempotencyKey() string {
	return "redis:" + delivery.TriggerID + ":" + delivery.Stream + ":" + delivery.MessageID
}

type RedisSink interface {
	DeliverRedis(context.Context, RedisDelivery) error
}

type RedisConsumerConfig struct {
	TriggerID string
	Stream    string
	Group     string
	Consumer  string
	BatchSize int64
	Block     time.Duration
	ClaimIdle time.Duration
}

func (config RedisConsumerConfig) validate() error {
	if config.TriggerID == "" || config.Stream == "" || config.Group == "" || config.Consumer == "" {
		return errors.New("trigger ID, stream, group, and consumer are required")
	}
	if config.BatchSize < 1 {
		return errors.New("batch size must be positive")
	}
	if config.Block <= 0 || config.ClaimIdle <= 0 {
		return errors.New("block and claim idle must be positive")
	}
	return nil
}

type RedisConsumer struct {
	client redis.UniversalClient
	sink   RedisSink
	config RedisConsumerConfig
}

func NewRedisConsumer(client redis.UniversalClient, sink RedisSink, config RedisConsumerConfig) (*RedisConsumer, error) {
	if client == nil || sink == nil {
		return nil, errors.New("redis client and sink are required")
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &RedisConsumer{client: client, sink: sink, config: config}, nil
}

func (consumer *RedisConsumer) EnsureGroup(ctx context.Context) error {
	err := consumer.client.XGroupCreateMkStream(
		ctx,
		consumer.config.Stream,
		consumer.config.Group,
		"0",
	).Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis consumer group: %w", err)
	}
	return nil
}

func (consumer *RedisConsumer) ReadOnce(ctx context.Context) (int, error) {
	streams, err := consumer.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumer.config.Group,
		Consumer: consumer.config.Consumer,
		Streams:  []string{consumer.config.Stream, ">"},
		Count:    consumer.config.BatchSize,
		Block:    consumer.config.Block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read Redis stream: %w", err)
	}
	return consumer.handleStreams(ctx, streams)
}

func (consumer *RedisConsumer) ClaimOnce(ctx context.Context) (int, error) {
	messages, _, err := consumer.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   consumer.config.Stream,
		Group:    consumer.config.Group,
		Consumer: consumer.config.Consumer,
		MinIdle:  consumer.config.ClaimIdle,
		Start:    "0-0",
		Count:    consumer.config.BatchSize,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("claim Redis stream entries: %w", err)
	}
	return consumer.handleMessages(ctx, consumer.config.Stream, messages)
}

func (consumer *RedisConsumer) Run(ctx context.Context) error {
	if err := consumer.EnsureGroup(ctx); err != nil {
		return err
	}
	claimEvery := consumer.config.ClaimIdle / 2
	if claimEvery < time.Second {
		claimEvery = time.Second
	}
	claimTimer := time.NewTimer(claimEvery)
	defer claimTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-claimTimer.C:
			if _, err := consumer.ClaimOnce(ctx); err != nil && ctx.Err() == nil {
				return err
			}
			claimTimer.Reset(claimEvery)
		default:
			if _, err := consumer.ReadOnce(ctx); err != nil && ctx.Err() == nil {
				return err
			}
		}
	}
}

func (consumer *RedisConsumer) handleStreams(ctx context.Context, streams []redis.XStream) (int, error) {
	handled := 0
	for _, stream := range streams {
		count, err := consumer.handleMessages(ctx, stream.Stream, stream.Messages)
		handled += count
		if err != nil {
			return handled, err
		}
	}
	return handled, nil
}

func (consumer *RedisConsumer) handleMessages(ctx context.Context, stream string, messages []redis.XMessage) (int, error) {
	handled := 0
	for _, message := range messages {
		delivery := RedisDelivery{
			TriggerID: consumer.config.TriggerID,
			Stream:    stream,
			MessageID: message.ID,
			Values:    message.Values,
		}
		if err := consumer.sink.DeliverRedis(ctx, delivery); err != nil {
			return handled, fmt.Errorf("persist Redis delivery %s: %w", message.ID, err)
		}
		if err := consumer.client.XAck(ctx, stream, consumer.config.Group, message.ID).Err(); err != nil {
			return handled, fmt.Errorf("acknowledge Redis delivery %s: %w", message.ID, err)
		}
		handled++
	}
	return handled, nil
}
