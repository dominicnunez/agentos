package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

// validateInferenceKnowledge runs under the reservation transaction. Neither
// caller-supplied source handles nor an earlier execution-start snapshot can
// substitute for the admitted manifest and current Knowledge at dispatch.
func validateInferenceKnowledge(ctx context.Context, tx *sql.Tx, request inference.InferenceRequest) error {
	if request.Scope.Purpose != inference.PurposeTaskExecution {
		return nil
	}
	if request.Scope.RequestID != request.Scope.ExecutionID {
		return fmt.Errorf("task inference request must identify its single execution")
	}
	body, found, err := latestRecordBody(ctx, tx, "task", request.Scope.TaskID)
	if err != nil {
		return err
	}
	var projection events.ProjectionRecord
	var task core.Task
	if !found || decodeExactJSONBytes(body, &projection) != nil || decodeExactJSONBytes(projection.Value, &task) != nil {
		return fmt.Errorf("task inference requires an admitted running execution")
	}
	start, task, _, stream, err := resolveAgentExecutionBoundary(ctx, tx, events.TrustedDraft{
		OrganizationID: request.Scope.OrganizationID, TaskID: request.Scope.TaskID,
		SourceActorID: string(task.AssigneeID), SourceExecutionID: request.Scope.ExecutionID, CorrelationID: request.Scope.CorrelationID,
	})
	if err != nil {
		return fmt.Errorf("task inference execution boundary: %w", err)
	}
	if task.ModelInferencePolicy == core.InferenceForbidden {
		return fmt.Errorf("task forbids model inference")
	}
	var manifest core.ExecutionContextManifest
	manifestFound := false
	var latestSequence int64
	for _, event := range stream {
		if event.EventType == "EXECUTION_FINISHED" && event.SourceExecutionID == request.Scope.ExecutionID && event.TaskID == request.Scope.TaskID && event.CorrelationID == request.Scope.CorrelationID {
			return fmt.Errorf("task inference execution has finished")
		}
		if event.Sequence > latestSequence {
			latestSequence = event.Sequence
		}
		if event.EventType != "EXECUTION_CONTEXT_MANIFESTED" || event.SourceExecutionID != request.Scope.ExecutionID {
			continue
		}
		if manifestFound || event.Sequence <= start.Sequence || event.SourceActorID != "runtime" || event.TaskID != request.Scope.TaskID ||
			event.CorrelationID != request.Scope.CorrelationID || decodeExactJSONBytes(event.Payload, &manifest) != nil {
			return fmt.Errorf("task inference manifest is invalid")
		}
		manifestFound = true
		if decision := manifest.RoutingDecision; decision != nil && (decision.SnapshotSequence <= 0 || decision.SnapshotSequence >= event.Sequence) {
			return fmt.Errorf("task routing decision does not precede its manifest")
		}
	}
	if !manifestFound || manifest.ContextBuilderVersion != "v5" || manifest.ExecutionID != core.ID(request.Scope.ExecutionID) ||
		manifest.TaskID != task.ID || manifest.AgentID != task.AssigneeID || manifest.ExecutionInputSHA256 != request.PromptSHA256 ||
		!modelinput.SameRouteRequirements(manifest.Routing, request.Scope.Routing) || !modelinput.SameRouteDecision(manifest.RoutingDecision, request.Scope.RoutingDecision) || manifest.ConnectionID != request.ConnectionID || manifest.Provider != request.Descriptor.Provider || manifest.Model != request.Descriptor.Model ||
		manifest.ExecutionProfileVersion != request.Descriptor.ExecutionProfileVersion || latestSequence == math.MaxInt64 {
		return fmt.Errorf("task inference request does not match its current execution manifest")
	}
	teamBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='team' ORDER BY r.record_id,r.version`)
	if err != nil {
		return err
	}
	teams, err := events.ResolveTeamRevisionBindings(request.Scope.OrganizationID, teamBodies, stream)
	if err != nil {
		return err
	}
	if err := events.ValidateExecutionKnowledgeAtUse(request.Scope.OrganizationID, task, manifest, latestSequence+1, teams, stream); err != nil {
		return fmt.Errorf("inference knowledge is no longer eligible: %w", err)
	}
	return nil
}
