package decisionlog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

func TestWriterProducesVerifiableChain(t *testing.T) {
	var output bytes.Buffer
	writer := NewWriter(&output, "")
	for sequence := int64(1); sequence <= 2; sequence++ {
		input := scheduler.Snapshot{
			Sequence:    sequence,
			WorkerSlots: 1,
			BatchLimit:  1,
			Queues:      []scheduler.Queue{},
		}
		decision, err := scheduler.Decide(input)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(input, decision); err != nil {
			t.Fatal(err)
		}
	}

	records, err := ReadAll(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[1].PreviousHash != records[0].Hash {
		t.Fatalf("invalid chain: %+v", records)
	}
}

func TestReadAllRejectsTampering(t *testing.T) {
	input := scheduler.Snapshot{Sequence: 1, WorkerSlots: 1, BatchLimit: 1}
	decision, err := scheduler.Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if _, err := NewWriter(&output, "").Append(input, decision); err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(output.String(), `"worker_slots":1`, `"worker_slots":2`, 1)
	if _, err := ReadAll(strings.NewReader(tampered)); err == nil {
		t.Fatal("tampered record accepted")
	}
}
