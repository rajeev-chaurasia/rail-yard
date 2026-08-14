package replay

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/rajeev-chaurasia/rail-yard/internal/decisionlog"
	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

func TestRunReproducesCanonicalDecisions(t *testing.T) {
	var log bytes.Buffer
	writer := decisionlog.NewWriter(&log, "")
	for sequence := int64(1); sequence <= 3; sequence++ {
		input := scheduler.Snapshot{
			Sequence:      sequence,
			LogicalTimeNS: sequence * 1_000,
			WorkerSlots:   1,
			BatchLimit:    1,
			Queues: []scheduler.Queue{{
				TenantID: "tenant",
				Name:     "queue",
				Weight:   1,
				Candidates: []scheduler.Candidate{{
					JobID:    string(rune('a' + sequence - 1)),
					TenantID: "tenant",
					Queue:    "queue",
					ReadySeq: sequence,
					SlotCost: 1,
				}},
			}},
		}
		decision, err := scheduler.Decide(input)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(input, decision); err != nil {
			t.Fatal(err)
		}
	}

	var output bytes.Buffer
	result, err := Run(bytes.NewReader(log.Bytes()), &output)
	if err != nil {
		t.Fatal(err)
	}
	if result.Records != 3 {
		t.Fatalf("records = %d, want 3", result.Records)
	}
	if result.Digest == "" || output.Len() == 0 {
		t.Fatal("missing replay output or digest")
	}
}

func TestRunReportsDivergence(t *testing.T) {
	input := scheduler.Snapshot{
		Sequence:    1,
		WorkerSlots: 1,
		BatchLimit:  1,
		Queues: []scheduler.Queue{{
			TenantID: "tenant",
			Name:     "queue",
			Weight:   1,
			Candidates: []scheduler.Candidate{{
				JobID:    "expected",
				TenantID: "tenant",
				Queue:    "queue",
				ReadySeq: 1,
				SlotCost: 1,
			}},
		}},
	}
	decision, err := scheduler.Decide(input)
	if err != nil {
		t.Fatal(err)
	}
	decision.Grants[0].JobID = "tampered"

	var log bytes.Buffer
	if _, err := decisionlog.NewWriter(&log, "").Append(input, decision); err != nil {
		t.Fatal(err)
	}

	_, err = Run(strings.NewReader(log.String()), &bytes.Buffer{})
	var divergence *DivergenceError
	if !errors.As(err, &divergence) {
		t.Fatalf("error = %v, want DivergenceError", err)
	}
}
