package events

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

// BeginExecutionContext delegates live containment to the authoritative ledger.
// A ledger without containment support cannot admit running task handlers.
func (g *Gateway) BeginExecutionContext(ctx context.Context, organization string) (context.Context, func(), error) {
	store, ok := g.ledger.(interface {
		BeginExecutionContext(context.Context, string) (context.Context, func(), error)
	})
	if !ok {
		return nil, nil, fmt.Errorf("execution containment is unavailable")
	}
	return store.BeginExecutionContext(ctx, organization)
}

// ValidateSecurityHoldOutcomes checks new containment audit evidence against
// authority admissions already proven to match their durable records. Legacy
// outcomes without a security-hold claim retain their original replay contract.
func ValidateSecurityHoldOutcomes(stream []Event, freezes []OrganizationFreezeAdmission) error {
	for _, outcomeEvent := range stream {
		if outcomeEvent.EventType != "TOOL_OUTCOME_RECORDED" {
			continue
		}
		var outcome core.ToolOutcome
		if decodeExactPayload(outcomeEvent.Payload, &outcome) != nil {
			return fmt.Errorf("invalid tool outcome at %s", outcomeEvent.EventID)
		}
		if outcome.ErrorClass != "security_hold" {
			continue
		}
		var evidence core.ExecutionInterruptionEvidence
		if !outcome.Valid() || outcomeEvent.SourceActorID != "runtime" || outcomeEvent.SourceExecutionID == "" || outcomeEvent.TaskID == "" || outcomeEvent.RecipientScope != "" || outcomeEvent.RecipientID != "" || len(outcomeEvent.AuthorizationRefs) != 0 || len(outcomeEvent.ArtifactRefs) != 0 || outcome.ToolID != "runtime-containment" || outcome.Status != core.OutcomeFailed || outcome.PostconditionStatus != core.PostconditionNotChecked || outcome.Retryability != core.NotRetryable || len(outcome.ArtifactRefs) != 0 || decodeExactPayload(outcome.ObservedEffect, &evidence) != nil || evidence.Hold == nil || !evidence.LocalExecutionStopped || evidence.ExternalEffectsStatus != "REQUIRES_RECONCILIATION" {
			return fmt.Errorf("invalid security hold outcome %s", outcomeEvent.EventID)
		}
		var startSequence int64
		for _, start := range stream {
			if start.EventType != "EXECUTION_STARTED" || start.OrganizationID != outcomeEvent.OrganizationID || start.TaskID != outcomeEvent.TaskID || start.CorrelationID != outcomeEvent.CorrelationID || start.Sequence >= outcomeEvent.Sequence {
				continue
			}
			payload, present, err := AdmittedProjection(start)
			if err != nil {
				return err
			}
			if !present || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != outcomeEvent.TaskID || fmt.Sprintf("execution-%s-v%d", outcomeEvent.TaskID, payload.Projection.Version) != outcomeEvent.SourceExecutionID {
				continue
			}
			if startSequence != 0 {
				return fmt.Errorf("security hold execution has multiple starts")
			}
			startSequence = start.Sequence
		}
		if startSequence == 0 {
			return fmt.Errorf("security hold outcome lacks execution start")
		}
		var first *OrganizationFreezeAdmission
		for index := range freezes {
			freeze := &freezes[index]
			if string(freeze.OrganizationID) == outcomeEvent.OrganizationID && freeze.Frozen && freeze.Sequence > startSequence && freeze.Sequence < outcomeEvent.Sequence && (first == nil || freeze.Sequence < first.Sequence) {
				first = freeze
			}
		}
		hold := evidence.Hold
		if first == nil || first.EventRef == "" || hold.OrganizationID != first.OrganizationID || hold.EventRef != first.EventRef || hold.Sequence != first.Sequence {
			return fmt.Errorf("security hold outcome does not bind its earliest committed freeze")
		}
	}
	return nil
}
