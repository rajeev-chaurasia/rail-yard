package admission

import (
	"errors"
	"testing"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

func TestPolicyRejectsAtDepthLimit(t *testing.T) {
	policy := Policy{MaxTenantDepth: 2, MaxSlotCost: 8}
	err := policy.Check(domain.JobSpec{Payload: domain.Payload{Type: domain.PayloadNoop}}, 2)
	if !errors.Is(err, domain.ErrQueueFull) {
		t.Fatalf("error = %v, want queue full", err)
	}
}

func TestPolicyAcceptsNormalizedNoop(t *testing.T) {
	policy := Policy{MaxTenantDepth: 2, MaxSlotCost: 8}
	if err := policy.Check(domain.JobSpec{Payload: domain.Payload{Type: domain.PayloadNoop}}, 1); err != nil {
		t.Fatal(err)
	}
}
