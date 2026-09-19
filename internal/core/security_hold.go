package core

import "errors"

// ErrOrganizationFrozen is a durable containment denial, not a failed Task.
// Callers may leave held work pending and continue scheduling other tenants.
var ErrOrganizationFrozen = errors.New("organization is frozen")

// ErrContainmentUnavailable stops live work when authority cannot be checked;
// it does not assert that an operator committed a security freeze.
var ErrContainmentUnavailable = errors.New("containment authority is unavailable")

// ErrExecutionStopped denies new activity for an execution whose durable stop
// request remains in force, independently of the organization's current freeze.
var ErrExecutionStopped = errors.New("execution is stopped pending reconciliation")

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
	StopRequestRef        string                `json:"stop_request_ref,omitempty"`
	Hold                  *SecurityHoldCause    `json:"hold,omitempty"`
	LocalExecutionStopped bool                  `json:"local_execution_stopped"`
	ExternalEffectsStatus string                `json:"external_effects_status"`
	ReportedOutcome       ToolOutcome           `json:"reported_outcome"`
	ProviderStop          *ProviderStopEvidence `json:"provider_stop,omitempty"`
}

// ProviderStopEvidence records the adapter's observation separately from local
// handler return. Local process termination does not prove remote cancellation.
type ProviderStopEvidence struct {
	LocalTurnStopped          bool   `json:"local_turn_stopped"`
	LocalProcessStopAttempted bool   `json:"local_process_stop_attempted"`
	LocalProcessStopped       bool   `json:"local_process_stopped"`
	RemoteStatus              string `json:"remote_status"`
}

func (SecurityHoldCause) Error() string { return "execution interrupted by committed security hold" }
func (SecurityHoldCause) Unwrap() error { return ErrOrganizationFrozen }
