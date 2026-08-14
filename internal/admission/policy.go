package admission

import (
	"fmt"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

type Policy struct {
	MaxTenantDepth int
	MaxSlotCost    int
	AllowShell     bool
}

func (policy Policy) Check(spec domain.JobSpec, currentTenantDepth int) error {
	if currentTenantDepth < 0 {
		return fmt.Errorf("current tenant depth must not be negative")
	}
	if policy.MaxTenantDepth < 1 {
		return fmt.Errorf("maximum tenant depth must be positive")
	}
	if currentTenantDepth >= policy.MaxTenantDepth {
		return domain.ErrQueueFull
	}
	if err := spec.Validate(policy.MaxSlotCost, policy.AllowShell); err != nil {
		return err
	}
	return nil
}
