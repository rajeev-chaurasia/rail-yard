package scheduler

import (
	"reflect"
	"testing"
)

func TestDecideUsesPriorityThenFIFO(t *testing.T) {
	input := Snapshot{
		Sequence:    7,
		WorkerSlots: 3,
		BatchLimit:  3,
		Algorithm:   AlgorithmVersion,
		Queues: []Queue{{
			TenantID: "tenant",
			Name:     "queue",
			Weight:   3,
			Candidates: []Candidate{
				{JobID: "later", TenantID: "tenant", Queue: "queue", Priority: 1, ReadySeq: 2, SlotCost: 1},
				{JobID: "high", TenantID: "tenant", Queue: "queue", Priority: 2, ReadySeq: 3, SlotCost: 1},
				{JobID: "first", TenantID: "tenant", Queue: "queue", Priority: 1, ReadySeq: 1, SlotCost: 1},
			},
		}},
	}

	decision, err := Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{
		decision.Grants[0].JobID,
		decision.Grants[1].JobID,
		decision.Grants[2].JobID,
	}
	want := []string{"high", "first", "later"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("grant order = %v, want %v", got, want)
	}
}

func TestDecideRotatesQueues(t *testing.T) {
	input := Snapshot{
		WorkerSlots: 2,
		BatchLimit:  2,
		Queues: []Queue{
			{
				TenantID: "a",
				Name:     "q",
				Weight:   1,
				Candidates: []Candidate{
					{JobID: "a1", TenantID: "a", Queue: "q", ReadySeq: 1, SlotCost: 1},
					{JobID: "a2", TenantID: "a", Queue: "q", ReadySeq: 2, SlotCost: 1},
				},
			},
			{
				TenantID: "b",
				Name:     "q",
				Weight:   1,
				Candidates: []Candidate{
					{JobID: "b1", TenantID: "b", Queue: "q", ReadySeq: 1, SlotCost: 1},
					{JobID: "b2", TenantID: "b", Queue: "q", ReadySeq: 2, SlotCost: 1},
				},
			},
		},
	}

	decision, err := Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{decision.Grants[0].JobID, decision.Grants[1].JobID}
	want := []string{"a1", "b1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("grant order = %v, want %v", got, want)
	}
}

func TestDecideAccumulatesDeficitWithoutBypassingHead(t *testing.T) {
	input := Snapshot{
		WorkerSlots: 4,
		BatchLimit:  1,
		Queues: []Queue{{
			TenantID: "tenant",
			Name:     "queue",
			Weight:   1,
			Candidates: []Candidate{
				{JobID: "head", TenantID: "tenant", Queue: "queue", Priority: 2, ReadySeq: 1, SlotCost: 2},
				{JobID: "small", TenantID: "tenant", Queue: "queue", Priority: 1, ReadySeq: 2, SlotCost: 1},
			},
		}},
	}

	first, err := Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Grants) != 0 {
		t.Fatalf("first decision unexpectedly granted %v", first.Grants)
	}
	input.Queues[0].Deficit = first.Queues[0].Deficit
	second, err := Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Grants) != 1 || second.Grants[0].JobID != "head" {
		t.Fatalf("second decision grants = %v, want head", second.Grants)
	}
}

func TestDecideDoesNotMutateInput(t *testing.T) {
	input := Snapshot{
		WorkerSlots: 1,
		BatchLimit:  1,
		Queues: []Queue{{
			TenantID: "tenant",
			Name:     "queue",
			Weight:   1,
			Candidates: []Candidate{
				{JobID: "b", TenantID: "tenant", Queue: "queue", ReadySeq: 2, SlotCost: 1},
				{JobID: "a", TenantID: "tenant", Queue: "queue", ReadySeq: 1, SlotCost: 1},
			},
		}},
	}
	originalFirst := input.Queues[0].Candidates[0]

	if _, err := Decide(input); err != nil {
		t.Fatal(err)
	}
	if input.Queues[0].Candidates[0] != originalFirst {
		t.Fatal("Decide mutated its input")
	}
}

func TestDecidePrioritizesLeaseRecoveryWithinQueue(t *testing.T) {
	input := Snapshot{
		WorkerSlots: 1,
		BatchLimit:  1,
		Queues: []Queue{{
			TenantID: "tenant",
			Name:     "queue",
			Weight:   1,
			Candidates: []Candidate{
				{
					JobID:    "new-high-priority",
					TenantID: "tenant",
					Queue:    "queue",
					Priority: 100,
					ReadySeq: 1,
					SlotCost: 1,
				},
				{
					JobID:    "recovered",
					TenantID: "tenant",
					Queue:    "queue",
					Priority: -100,
					ReadySeq: 2,
					SlotCost: 1,
					Recovery: true,
				},
			},
		}},
	}
	decision, err := Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(decision.Grants) != 1 || decision.Grants[0].JobID != "recovered" {
		t.Fatalf("grants = %+v", decision.Grants)
	}
}
