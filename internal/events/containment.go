package events

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

// ContainmentExecutionID resolves the runtime identity from an admitted start.
// Human continuations bind to their durable input, not a scheduler version ID.
func ContainmentExecutionID(start Event) (string, error) {
	payload, present, err := AdmittedProjection(start)
	if err != nil {
		return "", err
	}
	if start.EventType != "EXECUTION_STARTED" || !present || payload.Projection.ProjectionKind != "task" || payload.Projection.RecordID != start.TaskID {
		return "", fmt.Errorf("containment requires an admitted task execution start")
	}
	var task core.Task
	if err := json.Unmarshal(payload.Projection.Value, &task); err != nil {
		return "", err
	}
	if task.ExecutionKind == core.ExecutionHuman {
		detail, err := nonAgentExecutionStartDetail(start, core.ExecutionHuman)
		if err != nil {
			return "", err
		}
		if detail.Mode == "STRUCTURED_HUMAN_COMPLETION" {
			return "human-completion-" + detail.InputEventRef, nil
		}
		return "external-input-" + detail.InputEventRef, nil
	}
	return fmt.Sprintf("execution-%s-v%d", start.TaskID, payload.Projection.Version), nil
}

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

func (g *Gateway) SuspendHeldExecution(ctx context.Context, organization, taskID, correlation string, version int) (bool, error) {
	store, ok := g.ledger.(interface {
		SuspendHeldExecution(context.Context, string, string, string, int) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("execution hold recovery is unavailable")
	}
	return store.SuspendHeldExecution(ctx, organization, taskID, correlation, version)
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
			executionID, err := ContainmentExecutionID(start)
			if err != nil {
				return err
			}
			if executionID != outcomeEvent.SourceExecutionID {
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
