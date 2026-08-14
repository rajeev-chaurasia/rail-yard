package domain

import "testing"

func TestAllowedTransitions(t *testing.T) {
	tests := []struct {
		from JobState
		to   JobState
	}{
		{StatePending, StateScheduled},
		{StateScheduled, StateRunning},
		{StateRunning, StateRetrying},
		{StateRetrying, StateScheduled},
		{StateRunning, StateSucceeded},
	}
	for _, test := range tests {
		if err := ValidateTransition(test.from, test.to); err != nil {
			t.Errorf("%s to %s: %v", test.from, test.to, err)
		}
	}
}

func TestTerminalStateCannotTransition(t *testing.T) {
	for _, state := range []JobState{StateSucceeded, StateFailed, StateDeadLetter} {
		if err := ValidateTransition(state, StatePending); err == nil {
			t.Errorf("terminal state %s transitioned to pending", state)
		}
	}
}

func TestInvalidTransitionIsRejected(t *testing.T) {
	if err := ValidateTransition(StatePending, StateSucceeded); err == nil {
		t.Fatal("pending transitioned directly to succeeded")
	}
}
