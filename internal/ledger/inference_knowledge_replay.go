package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// New v5 reservations bind their exact manifest. Historical accounting without
// a v5 execution retains its previous contract; a v5 manifest cannot lose its
// reference or be replayed under a different inference purpose.
func validateReservedExecutionKnowledge(ctx context.Context, tx *sql.Tx, reservation events.Event, payload events.InferenceReservedPayload) error {
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND sequence<? ORDER BY sequence`, reservation.OrganizationID, reservation.Sequence))
	if err != nil {
		return err
	}
	var manifest core.ExecutionContextManifest
	var manifestEvent events.Event
	var task core.Task
	var taskRecord events.ProjectionRecord
	var taskEvent events.Event
	var teamBodies [][]byte
	for _, event := range stream {
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && event.SourceExecutionID == reservation.SourceExecutionID {
			if manifestEvent.EventID != "" || decodeExactJSONBytes(event.Payload, &manifest) != nil {
				return fmt.Errorf("inference execution manifest history is invalid")
			}
			manifestEvent = event
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		if projection.Projection.ProjectionKind == "team" {
			body, err := json.Marshal(projection.Projection)
			if err != nil {
				return err
			}
			teamBodies = append(teamBodies, body)
		}
		if projection.Projection.ProjectionKind == "task" && projection.Projection.RecordID == reservation.TaskID {
			if decodeExactJSONBytes(projection.Projection.Value, &task) != nil {
				return fmt.Errorf("inference Task history is invalid")
			}
			taskRecord, taskEvent = projection.Projection, event
		}
	}
	if manifestEvent.EventID == "" {
		if payload.ExecutionManifestRef != "" || taskEvent.EventID != "" && payload.Purpose == string(inference.PurposeTaskExecution) {
			return fmt.Errorf("inference reservation lacks its execution manifest")
		}
		return nil
	}
	if manifest.ContextBuilderVersion != "v5" {
		if payload.ExecutionManifestRef != "" {
			return fmt.Errorf("current inference reference targets a historical manifest")
		}
		switch manifest.ContextBuilderVersion {
		case "v1", "v2", "v3", "v4":
			return nil
		}
		return fmt.Errorf("inference manifest version is unsupported")
	}
	if payload.ExecutionManifestRef != manifestEvent.EventID || payload.Purpose != string(inference.PurposeTaskExecution) ||
		payload.RequestID != reservation.SourceExecutionID || manifestEvent.SourceActorID != "runtime" ||
		manifestEvent.TaskID != reservation.TaskID || manifestEvent.CorrelationID != reservation.CorrelationID ||
		manifest.ExecutionID != core.ID(reservation.SourceExecutionID) || manifest.TaskID != task.ID || manifest.AgentID != task.AssigneeID ||
		manifest.ExecutionInputSHA256 != payload.PromptSHA256 || manifest.Provider != payload.Provider || manifest.Model != payload.Model ||
		manifest.ExecutionProfileVersion != payload.ExecutionProfileVersion || taskEvent.EventType != "EXECUTION_STARTED" ||
		task.Status != core.TaskRunning || task.ModelInferencePolicy == core.InferenceForbidden ||
		manifestEvent.Sequence <= taskEvent.Sequence || reservation.SourceExecutionID != fmt.Sprintf("execution-%s-v%d", task.ID, taskRecord.Version) {
		return fmt.Errorf("inference reservation does not bind its exact admitted execution")
	}
	if err := events.ValidateAgentDispatchStart(taskEvent, task, taskRecord.Version, stream); err != nil {
		return err
	}
	for _, event := range stream {
		if event.EventType == "EXECUTION_FINISHED" && event.SourceExecutionID == reservation.SourceExecutionID && event.TaskID == reservation.TaskID && event.CorrelationID == reservation.CorrelationID {
			return fmt.Errorf("inference reservation follows execution finish")
		}
	}
	teams, err := events.ResolveTeamRevisionBindings(reservation.OrganizationID, teamBodies, stream)
	if err != nil {
		return err
	}
	if err := events.ValidateExecutionKnowledgeAtUse(reservation.OrganizationID, task, manifest, reservation.Sequence, teams, stream); err != nil {
		return fmt.Errorf("inference reservation used invalid Knowledge: %w", err)
	}
	return nil
}
