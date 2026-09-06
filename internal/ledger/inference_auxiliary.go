package ledger

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func auxiliaryInferencePurpose(purpose string) bool {
	return purpose == string(inference.PurposePlanning) || purpose == string(inference.PurposeIntentNormalization)
}

// The library accounting API also supports requests without application context.
// When context exists, bind it under the same transaction as the reservation.
func bindAuxiliaryInferenceContext(ctx context.Context, tx *sql.Tx, request inference.InferenceRequest, payload *events.InferenceReservedPayload) error {
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? AND source_execution_id=? AND event_type IN ('PLANNING_CONTEXT_MANIFESTED','INTENT_NORMALIZATION_CONTEXT_MANIFESTED','EXECUTION_CONTEXT_MANIFESTED') ORDER BY sequence`, request.Scope.OrganizationID, request.Scope.ExecutionID))
	if err != nil {
		return err
	}
	if len(stream) == 1 {
		payload.ExecutionManifestRef = stream[0].EventID
	}
	return validateAuxiliaryInferenceContext(events.Event{
		OrganizationID: request.Scope.OrganizationID, SourceExecutionID: request.Scope.ExecutionID,
		TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID,
	}, *payload, stream)
}

func validateAuxiliaryInferenceContext(reservation events.Event, payload events.InferenceReservedPayload, stream []events.Event) error {
	if len(stream) == 0 && payload.ExecutionManifestRef == "" {
		return nil
	}
	if len(stream) != 1 {
		return fmt.Errorf("auxiliary inference context history is invalid")
	}
	manifest := stream[0]
	if manifest.SourceActorID != "runtime" || manifest.OrganizationID != reservation.OrganizationID ||
		manifest.SourceExecutionID != reservation.SourceExecutionID || manifest.TaskID != reservation.TaskID ||
		manifest.CorrelationID != reservation.CorrelationID || payload.RequestID != reservation.SourceExecutionID ||
		(reservation.Sequence != 0 && manifest.Sequence >= reservation.Sequence) {
		return fmt.Errorf("auxiliary inference context scope is invalid")
	}
	var connection, provider, model, profile string
	switch payload.Purpose {
	case string(inference.PurposePlanning):
		var context events.PlanningContextPayload
		if manifest.EventType != "PLANNING_CONTEXT_MANIFESTED" || decodeExactJSONBytes(manifest.Payload, &context) != nil || context.IntentID != payload.IntentID {
			return fmt.Errorf("planning inference context is invalid")
		}
		connection, provider, model, profile = context.ConnectionID, context.Provider, context.Model, context.ExecutionProfileVersion
	case string(inference.PurposeIntentNormalization):
		var context events.IntentNormalizationContextPayload
		if manifest.EventType != "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" || decodeExactJSONBytes(manifest.Payload, &context) != nil {
			return fmt.Errorf("normalization inference context is invalid")
		}
		connection, provider, model, profile = context.ConnectionID, context.Provider, context.Model, context.ExecutionProfileVersion
	default:
		return fmt.Errorf("auxiliary inference purpose is invalid")
	}
	// Historical single-provider reservations predate context references. Named
	// connections are new and must retain their exact reference during replay.
	if payload.ExecutionManifestRef != manifest.EventID && (payload.ExecutionManifestRef != "" || connection != "" || payload.ConnectionID != "") {
		return fmt.Errorf("auxiliary inference context reference is invalid")
	}
	if connection != payload.ConnectionID || provider != payload.Provider || model != payload.Model || profile != payload.ExecutionProfileVersion {
		return fmt.Errorf("auxiliary inference model does not match its admitted context")
	}
	return nil
}
