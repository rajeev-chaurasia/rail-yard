package p1

import (
	"fmt"
	"sync"
	"time"
)

// FakeClock is a monotonic, deterministic clock shared by the P1 oracle and
// contract suite.
type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now.UTC()}
}

func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *FakeClock) Advance(duration time.Duration) time.Time {
	if duration < 0 {
		panic("p1 fake clock cannot move backwards")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
	return c.now
}

func (c *FakeClock) Set(now time.Time) error {
	now = now.UTC()

	c.mu.Lock()
	defer c.mu.Unlock()
	if now.Before(c.now) {
		return fmt.Errorf("set fake clock backwards: %s before %s", now, c.now)
	}
	c.now = now
	return nil
}
