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

type incidentPolicy struct {
	value    inference.Policy
	sequence int64
}

type incidentPolicySupport struct {
	policies       map[[2]string]incidentPolicy
	remainingBytes int64
}

type incidentAccounting struct {
	row        inferenceValidationRow
	sequence   int64
	reconciled bool
}

const incidentReservationBytes = `length(CAST(reservation_id AS BLOB))+length(CAST(request_id AS BLOB))+length(CAST(organization_id AS BLOB))+
length(CAST(purpose AS BLOB))+length(CAST(intent_id AS BLOB))+length(CAST(task_id AS BLOB))+length(CAST(execution_id AS BLOB))+
length(CAST(correlation_id AS BLOB))+length(CAST(prompt_sha256 AS BLOB))+length(CAST(provider AS BLOB))+length(CAST(model AS BLOB))+
length(CAST(execution_profile_version AS BLOB))+length(CAST(policy_fingerprint AS BLOB))+length(CAST(state AS BLOB))+
length(CAST(window_started_at AS BLOB))+length(CAST(window_expires_at AS BLOB))+length(CAST(connection_id AS BLOB))+length(CAST(created_at AS BLOB))`

// This validates the selected durable admission and its exact accounting,
// policy and execution bindings. It does not replay global budget competition,
// knowledge selection or all historical capability decisions.
func validateIncidentInference(ctx context.Context, tx *sql.Tx, stream []events.Event, freezes []events.OrganizationFreezeAdmission) error {
	manifests := map[string][]events.Event{}
	starts := map[string]events.Event{}
	seen := map[string]bool{}
	accounting := map[string]*incidentAccounting{}
	support := incidentPolicySupport{policies: map[[2]string]incidentPolicy{}, remainingBytes: 2 << 20}
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
			row, err := incidentReservation(ctx, tx, event, payload, &support)
			if err != nil {
				return err
			}
			accounting[payload.ReservationID] = &incidentAccounting{row: row, sequence: event.Sequence}
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
		case "INFERENCE_RECONCILED":
			var payload events.InferenceReconciledPayload
			if decodeExactJSONBytes(event.Payload, &payload) != nil {
				return fmt.Errorf("incident inference reconciliation is malformed")
			}
			entry, found := accounting[payload.ReservationID]
			if !found || entry.reconciled || entry.row.state == inferenceStateReserved || event.Sequence <= entry.sequence {
				return fmt.Errorf("incident inference reconciliation lacks its exact terminal accounting")
			}
			if err := validateInferenceReconciliationEvent(event, entry.row); err != nil {
				return err
			}
			entry.reconciled = true
		}
	}
	for _, entry := range accounting {
		if entry.row.state != inferenceStateReserved && !entry.reconciled {
			return fmt.Errorf("incident inference accounting lacks its exact terminal reconciliation")
		}
	}
	// Each unique selected event already proved its exact row and scope above.
	// A bounded reverse count detects any additional accounting row without
	// materializing unrelated reservations or their caller-controlled strings.
	if len(stream) != 0 {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM inference_reservations WHERE organization_id=? AND correlation_id=? LIMIT ?)`, stream[0].OrganizationID, stream[0].CorrelationID, len(seen)+1).Scan(&count); err != nil {
			return err
		}
		if count != len(seen) {
			return fmt.Errorf("incident inference accounting lacks its exact admission history")
		}
	}
	return nil
}

func incidentReservation(ctx context.Context, tx *sql.Tx, event events.Event, payload events.InferenceReservedPayload, support *incidentPolicySupport) (inferenceValidationRow, error) {
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT `+incidentReservationBytes+` FROM inference_reservations WHERE reservation_id=?`, payload.ReservationID).Scan(&size); err != nil {
		return inferenceValidationRow{}, fmt.Errorf("incident reservation lacks accounting: %w", err)
	}
	if size > support.remainingBytes {
		return inferenceValidationRow{}, fmt.Errorf("incident inference accounting exceeds byte limit")
	}
	support.remainingBytes -= size
	var row inferenceValidationRow
	if err := tx.QueryRowContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd,window_started_at,window_expires_at,connection_id,created_at FROM inference_reservations WHERE reservation_id=?`, payload.ReservationID).Scan(&row.reservationID, &row.requestID, &row.organizationID, &row.purpose, &row.intentID, &row.taskID, &row.executionID, &row.correlationID, &row.promptSHA256, &row.provider, &row.model, &row.profile, &row.policyFingerprint, &row.state, &row.reservedInput, &row.reservedOutput, &row.reservedCost, &row.chargedInput, &row.chargedOutput, &row.chargedCost, &row.windowStart, &row.windowEnd, &row.connectionID, &row.createdAt); err != nil {
		return inferenceValidationRow{}, fmt.Errorf("incident reservation lacks accounting: %w", err)
	}
	policy, err := support.load(ctx, tx, event, payload.PolicyFingerprint)
	if err != nil {
		return inferenceValidationRow{}, err
	}
	if row.validate(policy) != nil {
		return inferenceValidationRow{}, fmt.Errorf("incident reservation differs from its policy")
	}
	if err := validateInferenceReservationEvent(event, row, policy); err != nil {
		return inferenceValidationRow{}, err
	}
	return row, nil
}

// Supporting policies are immutable within this read transaction. Decode each
// unique policy and activation once, charge their stored bytes once, and still
// validate every reservation and its chronology against that exact evidence.
func (s *incidentPolicySupport) load(ctx context.Context, tx *sql.Tx, event events.Event, fingerprint string) (inference.Policy, error) {
	key := [2]string{event.OrganizationID, fingerprint}
	if cached, ok := s.policies[key]; ok {
		if cached.sequence >= event.Sequence {
			return inference.Policy{}, fmt.Errorf("incident policy activation does not precede reservation")
		}
		return cached.value, nil
	}
	var policyBytes int64
	var activationBytes sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT length(CAST(body AS BLOB))+length(CAST(activation_event_id AS BLOB))+length(CAST(connection_id AS BLOB)),(SELECT `+incidentEventBytes+` FROM events WHERE event_id=p.activation_event_id) FROM inference_policies p WHERE organization_id=? AND policy_fingerprint=?`, event.OrganizationID, fingerprint).Scan(&policyBytes, &activationBytes); err != nil {
		return inference.Policy{}, err
	}
	if !activationBytes.Valid || policyBytes > s.remainingBytes || activationBytes.Int64 > s.remainingBytes-policyBytes {
		return inference.Policy{}, fmt.Errorf("incident inference supporting evidence exceeds byte limit or lacks activation")
	}
	var body []byte
	var activationID, connection string
	if err := tx.QueryRowContext(ctx, `SELECT body,activation_event_id,connection_id FROM inference_policies WHERE organization_id=? AND policy_fingerprint=?`, event.OrganizationID, fingerprint).Scan(&body, &activationID, &connection); err != nil {
		return inference.Policy{}, err
	}
	var policy inference.Policy
	if decodeExactJSONBytes(body, &policy) != nil || policy.Validate() != nil || policy.OrganizationID != event.OrganizationID || policy.ConnectionID != connection {
		return inference.Policy{}, fmt.Errorf("incident inference policy is invalid")
	}
	actual, err := policy.Fingerprint()
	if err != nil || actual != fingerprint {
		return inference.Policy{}, fmt.Errorf("incident inference policy fingerprint is invalid")
	}
	activation, found, err := eventByID(ctx, tx, activationID)
	if err != nil {
		return inference.Policy{}, err
	}
	var activated events.InferencePolicyActivatedPayload
	expected := events.InferencePolicyActivatedPayload{ConnectionID: policy.ConnectionID, PolicyFingerprint: fingerprint, Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion, AccessMode: string(policy.Mode), AuthorizedBy: policy.AuthorizedBy, AuthorizedAt: policy.AuthorizedAt, AuthorizationExpiresAt: policy.AuthorizationExpiresAt}
	if !found || activation.EventType != "INFERENCE_POLICY_ACTIVATED" || activation.OrganizationID != event.OrganizationID || activation.SourceActorID != policy.AuthorizedBy || activation.SourceExecutionID != "" || activation.TaskID != "" || activation.RecipientID != "" || activation.RecipientScope != "" || len(activation.AuthorizationRefs) != 0 || len(activation.ArtifactRefs) != 0 || activation.SchemaVersion != events.SchemaVersion || activation.Sequence >= event.Sequence || activation.CorrelationID != "inference-policy-"+fingerprint[:16] || decodeExactJSONBytes(activation.Payload, &activated) != nil || !reflect.DeepEqual(activated, expected) {
		return inference.Policy{}, fmt.Errorf("incident policy lacks its exact prior activation")
	}
	s.remainingBytes -= policyBytes + activationBytes.Int64
	s.policies[key] = incidentPolicy{value: policy, sequence: activation.Sequence}
	return policy, nil
}
