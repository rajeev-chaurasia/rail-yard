package domain

import "testing"

func TestJobSpecDefaults(t *testing.T) {
	spec := JobSpec{Payload: Payload{Type: PayloadNoop}}.Normalize()
	if spec.TenantID != "default" || spec.Queue != "default" || spec.SlotCost != 1 {
		t.Fatalf("unexpected defaults: %+v", spec)
	}
	if spec.Retry.MaxAttempts != 5 || !spec.Retry.Retryable {
		t.Fatalf("unexpected retry defaults: %+v", spec.Retry)
	}
}

func TestExplicitSingleAttemptIsNotRetryable(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 1, Retryable: false}.Normalized()
	if policy.MaxAttempts != 1 || policy.Retryable {
		t.Fatalf("explicit policy changed: %+v", policy)
	}
}
