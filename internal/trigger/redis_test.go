package trigger

import (
	"testing"
	"time"
)

func TestRedisDeliveryIdempotencyKey(t *testing.T) {
	delivery := RedisDelivery{
		TriggerID: "trigger",
		Stream:    "events",
		MessageID: "123-4",
	}
	if got, want := delivery.IdempotencyKey(), "redis:trigger:events:123-4"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

func TestRedisConsumerConfigValidation(t *testing.T) {
	valid := RedisConsumerConfig{
		TriggerID: "trigger",
		Stream:    "events",
		Group:     "railyard",
		Consumer:  "server",
		BatchSize: 10,
		Block:     time.Second,
		ClaimIdle: 5 * time.Second,
	}
	if err := valid.validate(); err != nil {
		t.Fatal(err)
	}
	valid.Block = 0
	if err := valid.validate(); err == nil {
		t.Fatal("zero block accepted")
	}
}
