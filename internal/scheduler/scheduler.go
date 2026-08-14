package scheduler

import (
	"errors"
	"fmt"
	"sort"
)

const AlgorithmVersion = "drr-v1"

type Candidate struct {
	JobID    string `json:"job_id"`
	TenantID string `json:"tenant_id"`
	Queue    string `json:"queue"`
	Priority int    `json:"priority"`
	ReadySeq int64  `json:"ready_seq"`
	SlotCost int    `json:"slot_cost"`
	Recovery bool   `json:"recovery"`
}

type Queue struct {
	TenantID   string      `json:"tenant_id"`
	Name       string      `json:"name"`
	Weight     int         `json:"weight"`
	Deficit    int         `json:"deficit"`
	Candidates []Candidate `json:"candidates"`
}

type Snapshot struct {
	Sequence      int64   `json:"sequence"`
	LogicalTimeNS int64   `json:"logical_time_ns"`
	WorkerSlots   int     `json:"worker_slots"`
	BatchLimit    int     `json:"batch_limit"`
	Cursor        int     `json:"cursor"`
	Queues        []Queue `json:"queues"`
	ConfigHash    string  `json:"config_hash"`
	Algorithm     string  `json:"algorithm"`
}

type Grant struct {
	JobID    string `json:"job_id"`
	TenantID string `json:"tenant_id"`
	Queue    string `json:"queue"`
	SlotCost int    `json:"slot_cost"`
}

type QueueState struct {
	TenantID string `json:"tenant_id"`
	Queue    string `json:"queue"`
	Deficit  int    `json:"deficit"`
}

type Decision struct {
	Sequence   int64        `json:"sequence"`
	Grants     []Grant      `json:"grants"`
	Queues     []QueueState `json:"queues"`
	NextCursor int          `json:"next_cursor"`
}

func CanonicalSnapshot(input Snapshot) Snapshot {
	input.Queues = cloneAndSort(input.Queues)
	if input.Queues == nil {
		input.Queues = []Queue{}
	}
	if input.Algorithm == "" {
		input.Algorithm = AlgorithmVersion
	}
	return input
}

func Decide(input Snapshot) (Decision, error) {
	if input.WorkerSlots < 0 {
		return Decision{}, errors.New("worker slots must not be negative")
	}
	if input.BatchLimit < 0 {
		return Decision{}, errors.New("batch limit must not be negative")
	}
	if input.Algorithm != "" && input.Algorithm != AlgorithmVersion {
		return Decision{}, fmt.Errorf("unsupported scheduler algorithm %q", input.Algorithm)
	}

	queues := cloneAndSort(input.Queues)
	if len(queues) == 0 || input.WorkerSlots == 0 || input.BatchLimit == 0 {
		return buildDecision(input.Sequence, queues, 0, nil), nil
	}

	cursor := input.Cursor
	if cursor < 0 {
		cursor = 0
	}
	cursor %= len(queues)
	remaining := input.WorkerSlots
	grants := make([]Grant, 0, input.BatchLimit)
	visitedWithoutGrant := 0

	for len(grants) < input.BatchLimit && remaining > 0 && visitedWithoutGrant < len(queues) {
		queueIndex := cursor
		queue := &queues[queueIndex]
		cursor = (cursor + 1) % len(queues)

		if len(queue.Candidates) == 0 {
			visitedWithoutGrant++
			continue
		}

		quantum := queue.Weight
		if quantum < 1 {
			quantum = 1
		}
		queue.Deficit += quantum

		granted := false
		for len(queue.Candidates) > 0 && len(grants) < input.BatchLimit {
			candidate := queue.Candidates[0]
			if candidate.SlotCost < 1 {
				return Decision{}, fmt.Errorf("job %s has invalid slot cost %d", candidate.JobID, candidate.SlotCost)
			}
			if candidate.SlotCost > queue.Deficit || candidate.SlotCost > remaining {
				break
			}

			grants = append(grants, Grant{
				JobID:    candidate.JobID,
				TenantID: candidate.TenantID,
				Queue:    candidate.Queue,
				SlotCost: candidate.SlotCost,
			})
			queue.Deficit -= candidate.SlotCost
			remaining -= candidate.SlotCost
			queue.Candidates = queue.Candidates[1:]
			granted = true
		}

		if granted {
			visitedWithoutGrant = 0
		} else {
			visitedWithoutGrant++
		}
	}

	return buildDecision(input.Sequence, queues, cursor, grants), nil
}

func cloneAndSort(input []Queue) []Queue {
	queues := make([]Queue, len(input))
	for index := range input {
		queues[index] = input[index]
		queues[index].Candidates = append([]Candidate(nil), input[index].Candidates...)
		sort.Slice(queues[index].Candidates, func(left, right int) bool {
			a := queues[index].Candidates[left]
			b := queues[index].Candidates[right]
			if a.Recovery != b.Recovery {
				return a.Recovery
			}
			if a.Priority != b.Priority {
				return a.Priority > b.Priority
			}
			if a.ReadySeq != b.ReadySeq {
				return a.ReadySeq < b.ReadySeq
			}
			return a.JobID < b.JobID
		})
	}
	sort.Slice(queues, func(left, right int) bool {
		if queues[left].TenantID != queues[right].TenantID {
			return queues[left].TenantID < queues[right].TenantID
		}
		return queues[left].Name < queues[right].Name
	})
	return queues
}

func buildDecision(sequence int64, queues []Queue, cursor int, grants []Grant) Decision {
	states := make([]QueueState, len(queues))
	for index, queue := range queues {
		states[index] = QueueState{
			TenantID: queue.TenantID,
			Queue:    queue.Name,
			Deficit:  queue.Deficit,
		}
	}
	if len(queues) == 0 {
		cursor = 0
	}
	if grants == nil {
		grants = []Grant{}
	}
	return Decision{
		Sequence:   sequence,
		Grants:     grants,
		Queues:     states,
		NextCursor: cursor,
	}
}
