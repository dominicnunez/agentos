package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

// This validates the selected durable admission and its exact accounting,
// policy and execution bindings. It does not replay global budget competition,
// knowledge selection or all historical capability decisions.
func validateIncidentInference(ctx context.Context, tx *sql.Tx, stream []events.Event, freezes []events.OrganizationFreezeAdmission) error {
	manifests := map[string][]events.Event{}
	starts := map[string]events.Event{}
	seen := map[string]bool{}
	for _, event := range stream {
		switch event.EventType {
		case "PLANNING_CONTEXT_MANIFESTED", "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", "EXECUTION_CONTEXT_MANIFESTED":
			manifests[event.SourceExecutionID] = append(manifests[event.SourceExecutionID], event)
		case "EXECUTION_STARTED":
			id, err := events.ContainmentExecutionID(event)
			if err != nil {
				return err
			}
			starts[id] = event
		case "INFERENCE_RESERVED":
			var payload events.InferenceReservedPayload
			if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.ReservationID == "" || seen[payload.ReservationID] {
				return fmt.Errorf("incident inference admission is malformed or duplicated")
			}
			seen[payload.ReservationID] = true
			frozen := false
			for _, freeze := range freezes {
				if freeze.Sequence < event.Sequence {
					frozen = freeze.Frozen
				}
			}
			if frozen {
				return fmt.Errorf("incident inference reservation occurred during hold")
			}
			if err := incidentReservation(ctx, tx, event, payload); err != nil {
				return err
			}
			matching := manifests[event.SourceExecutionID]
			for _, manifest := range matching {
				for _, freeze := range freezes {
					if freeze.Frozen && freeze.Sequence > manifest.Sequence && freeze.Sequence < event.Sequence {
						return fmt.Errorf("incident inference context was interrupted by a hold")
					}
				}
			}
			if auxiliaryInferencePurpose(payload.Purpose) {
				if err := validateAuxiliaryInferenceContext(event, payload, matching); err != nil {
					return err
				}
				continue
			}
			if len(matching) != 1 || starts[event.SourceExecutionID].EventID == "" {
				return fmt.Errorf("incident task inference lacks its admitted start and manifest")
			}
			manifestEvent := matching[0]
			var manifest core.ExecutionContextManifest
			start := starts[event.SourceExecutionID]
			projection, present, err := events.AdmittedProjection(start)
			var task core.Task
			if err != nil || !present || decodeExactJSONBytes(projection.Projection.Value, &task) != nil || decodeExactJSONBytes(manifestEvent.Payload, &manifest) != nil {
				return fmt.Errorf("incident inference execution is invalid")
			}
			if manifestEvent.OrganizationID != event.OrganizationID || manifestEvent.TaskID != event.TaskID || manifestEvent.CorrelationID != event.CorrelationID || manifestEvent.SourceActorID != "runtime" || manifestEvent.Sequence <= start.Sequence || manifest.ExecutionID != core.ID(event.SourceExecutionID) || manifest.TaskID != task.ID || manifest.AgentID != task.AssigneeID || task.Status != core.TaskRunning || task.ModelInferencePolicy == core.InferenceForbidden {
				return fmt.Errorf("incident inference crosses its exact execution")
			}
			if manifest.ContextBuilderVersion != "v5" {
				if payload.ExecutionManifestRef != "" {
					return fmt.Errorf("incident current inference reference targets a historical manifest")
				}
				switch manifest.ContextBuilderVersion {
				case "v1", "v2", "v3", "v4":
					continue
				default:
					return fmt.Errorf("incident inference manifest version is unsupported")
				}
			}
			if payload.Purpose != string(inference.PurposeTaskExecution) || payload.RequestID != event.SourceExecutionID || manifest.RoutingDecision != nil && (manifest.RoutingDecision.SnapshotSequence <= 0 || manifest.RoutingDecision.SnapshotSequence >= manifestEvent.Sequence) {
				return fmt.Errorf("incident task inference purpose or routing boundary is invalid")
			}
			if manifest.ContextBuilderVersion == "v5" && (payload.ExecutionManifestRef != manifestEvent.EventID || manifest.ExecutionInputSHA256 != payload.PromptSHA256 || manifest.ConnectionID != payload.ConnectionID || manifest.Provider != payload.Provider || manifest.Model != payload.Model || manifest.ExecutionProfileVersion != payload.ExecutionProfileVersion || !modelinput.SameRouteRequirements(manifest.Routing, payload.Routing) || !modelinput.SameRouteDecision(manifest.RoutingDecision, payload.RoutingDecision)) {
				return fmt.Errorf("incident inference differs from its execution manifest")
			}
		}
	}
	return nil
}

func incidentReservation(ctx context.Context, tx *sql.Tx, event events.Event, payload events.InferenceReservedPayload) error {
	var row inferenceValidationRow
	if err := tx.QueryRowContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd,window_started_at,window_expires_at,connection_id,created_at FROM inference_reservations WHERE reservation_id=?`, payload.ReservationID).Scan(&row.reservationID, &row.requestID, &row.organizationID, &row.purpose, &row.intentID, &row.taskID, &row.executionID, &row.correlationID, &row.promptSHA256, &row.provider, &row.model, &row.profile, &row.policyFingerprint, &row.state, &row.reservedInput, &row.reservedOutput, &row.reservedCost, &row.chargedInput, &row.chargedOutput, &row.chargedCost, &row.windowStart, &row.windowEnd, &row.connectionID, &row.createdAt); err != nil {
		return fmt.Errorf("incident reservation lacks accounting: %w", err)
	}
	var size int
	if err := tx.QueryRowContext(ctx, `SELECT length(body) FROM inference_policies WHERE organization_id=? AND policy_fingerprint=?`, event.OrganizationID, payload.PolicyFingerprint).Scan(&size); err != nil {
		return err
	}
	if size > 2<<20 {
		return fmt.Errorf("incident inference policy exceeds byte limit")
	}
	var body []byte
	var activationID, connection string
	if err := tx.QueryRowContext(ctx, `SELECT body,activation_event_id,connection_id FROM inference_policies WHERE organization_id=? AND policy_fingerprint=?`, event.OrganizationID, payload.PolicyFingerprint).Scan(&body, &activationID, &connection); err != nil {
		return err
	}
	var policy inference.Policy
	if decodeExactJSONBytes(body, &policy) != nil || policy.Validate() != nil || policy.OrganizationID != event.OrganizationID || policy.ConnectionID != connection {
		return fmt.Errorf("incident inference policy is invalid")
	}
	fingerprint, err := policy.Fingerprint()
	if err != nil || fingerprint != payload.PolicyFingerprint || row.validate(policy) != nil {
		return fmt.Errorf("incident reservation differs from its policy")
	}
	if err := validateInferenceReservationEvent(event, row, policy); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT `+incidentEventBytes+` FROM events WHERE event_id=?`, activationID).Scan(&size); err != nil {
		return err
	}
	if size > 2<<20 {
		return fmt.Errorf("incident policy activation exceeds byte limit")
	}
	activation, found, err := eventByID(ctx, tx, activationID)
	if err != nil {
		return err
	}
	var activated events.InferencePolicyActivatedPayload
	expected := events.InferencePolicyActivatedPayload{ConnectionID: policy.ConnectionID, PolicyFingerprint: fingerprint, Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion, AccessMode: string(policy.Mode), AuthorizedBy: policy.AuthorizedBy, AuthorizedAt: policy.AuthorizedAt, AuthorizationExpiresAt: policy.AuthorizationExpiresAt}
	if !found || activation.EventType != "INFERENCE_POLICY_ACTIVATED" || activation.OrganizationID != event.OrganizationID || activation.SourceActorID != policy.AuthorizedBy || activation.SourceExecutionID != "" || activation.TaskID != "" || activation.RecipientID != "" || activation.RecipientScope != "" || len(activation.AuthorizationRefs) != 0 || len(activation.ArtifactRefs) != 0 || activation.SchemaVersion != events.SchemaVersion || activation.Sequence >= event.Sequence || activation.CorrelationID != "inference-policy-"+fingerprint[:16] || decodeExactJSONBytes(activation.Payload, &activated) != nil || !reflect.DeepEqual(activated, expected) {
		return fmt.Errorf("incident policy lacks its exact prior activation")
	}
	return nil
}
