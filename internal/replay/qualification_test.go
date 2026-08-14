package replay

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/rajeev-chaurasia/rail-yard/internal/decisionlog"
	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

func TestFullReplayQualification(t *testing.T) {
	if os.Getenv("RAILYARD_FULL_REPLAY") != "1" {
		t.Skip("set RAILYARD_FULL_REPLAY=1 to run full replay qualification")
	}

	const decisions = 50_000
	var input bytes.Buffer
	var expected bytes.Buffer
	writer := decisionlog.NewWriter(&input, "")
	for sequence := int64(1); sequence <= decisions; sequence++ {
		jobID := fmt.Sprintf("%032x", sequence)
		snapshot := scheduler.Snapshot{
			Sequence:      sequence,
			LogicalTimeNS: sequence * 1_000_000,
			WorkerSlots:   1,
			BatchLimit:    1,
			Queues: []scheduler.Queue{{
				TenantID: "qualification",
				Name:     "replay",
				Weight:   1,
				Candidates: []scheduler.Candidate{{
					JobID:    jobID,
					TenantID: "qualification",
					Queue:    "replay",
					ReadySeq: sequence,
					SlotCost: 1,
				}},
			}},
		}
		decision, err := scheduler.Decide(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Append(snapshot, decision); err != nil {
			t.Fatal(err)
		}
		canonical, err := decisionlog.CanonicalDecision(decision)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := expected.Write(canonical); err != nil {
			t.Fatal(err)
		}
	}

	var digest string
	for run := 1; run <= 3; run++ {
		var output bytes.Buffer
		result, err := Run(bytes.NewReader(input.Bytes()), &output)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if result.Records != decisions {
			t.Fatalf("run %d records = %d, want %d", run, result.Records, decisions)
		}
		if !bytes.Equal(output.Bytes(), expected.Bytes()) {
			t.Fatalf("run %d output differs from captured decisions", run)
		}
		if run > 1 && result.Digest != digest {
			t.Fatalf("run %d digest = %s, want %s", run, result.Digest, digest)
		}
		digest = result.Digest
	}
	t.Logf("decisions=%d runs=3 digest=%s match=100%%", decisions, digest)
}
