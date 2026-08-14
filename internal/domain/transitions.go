package domain

import "fmt"

var transitions = map[JobState]map[JobState]struct{}{
	StatePending: {
		StateScheduled:  {},
		StateFailed:     {},
		StateDeadLetter: {},
	},
	StateScheduled: {
		StateRunning:    {},
		StateRetrying:   {},
		StateSucceeded:  {},
		StateFailed:     {},
		StateDeadLetter: {},
	},
	StateRunning: {
		StateRetrying:   {},
		StateSucceeded:  {},
		StateFailed:     {},
		StateDeadLetter: {},
	},
	StateRetrying: {
		StatePending:    {},
		StateScheduled:  {},
		StateDeadLetter: {},
	},
}

func ValidateTransition(from, to JobState) error {
	allowed, exists := transitions[from]
	if !exists {
		return fmt.Errorf("state %s is terminal or unknown", from)
	}
	if _, exists := allowed[to]; !exists {
		return fmt.Errorf("transition %s to %s is not allowed", from, to)
	}
	return nil
}
