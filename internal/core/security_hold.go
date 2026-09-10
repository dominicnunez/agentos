package core

import "errors"

// ErrOrganizationFrozen is a durable containment denial, not a failed Task.
// Callers may leave held work pending and continue scheduling other tenants.
var ErrOrganizationFrozen = errors.New("organization is frozen")

// ErrContainmentUnavailable stops live work when authority cannot be checked;
// it does not assert that an operator committed a security freeze.
var ErrContainmentUnavailable = errors.New("containment authority is unavailable")

// SecurityHoldCause identifies the exact committed authority event which
// interrupted live work. It carries references only, never operator reason text.
type SecurityHoldCause struct {
	OrganizationID ID     `json:"organization_id"`
	EventRef       string `json:"event_ref"`
	Sequence       int64  `json:"sequence"`
}

// ExecutionInterruptionEvidence retains a handler's report for reconciliation.
// Handler return confirms only that local execution has stopped. Neither the
// report nor context cancellation proves cancellation or rollback of an external
// effect; the effect ledger remains authoritative for reconciliation.
type ExecutionInterruptionEvidence struct {
	Hold                  *SecurityHoldCause `json:"hold,omitempty"`
	LocalExecutionStopped bool               `json:"local_execution_stopped"`
	ExternalEffectsStatus string             `json:"external_effects_status"`
	ReportedOutcome       ToolOutcome        `json:"reported_outcome"`
}

func (SecurityHoldCause) Error() string { return "execution interrupted by committed security hold" }
func (SecurityHoldCause) Unwrap() error { return ErrOrganizationFrozen }
