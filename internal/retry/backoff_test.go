package retry

import (
	"testing"
	"time"
)

func TestDelayIsStableAndBounded(t *testing.T) {
	for attempt, cap := range []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		16 * time.Second,
	} {
		got, err := Delay("job-123", attempt+1)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Delay("job-123", attempt+1)
		if err != nil {
			t.Fatal(err)
		}
		if got != again {
			t.Fatalf("attempt %d delay changed from %s to %s", attempt+1, got, again)
		}
		if got < cap*8/10 || got > cap {
			t.Fatalf("attempt %d delay %s outside [%s, %s]", attempt+1, got, cap*8/10, cap)
		}
	}
}

func TestDelayRejectsInvalidInputs(t *testing.T) {
	if _, err := Delay("", 1); err == nil {
		t.Fatal("empty job ID accepted")
	}
	if _, err := Delay("job", 0); err == nil {
		t.Fatal("zero attempt accepted")
	}
}
