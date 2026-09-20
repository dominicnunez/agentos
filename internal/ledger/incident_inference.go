package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type incidentPolicy struct {
	value    inference.Policy
	sequence int64
}

type incidentInferenceSupport struct {
	reservations map[string]inferenceValidationRow
	policies     map[[2]string]incidentPolicy
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

const incidentInferencePolicyBytes = `length(CAST(p.body AS BLOB))+length(CAST(p.activation_event_id AS BLOB))+length(CAST(p.connection_id AS BLOB))`

const incidentInferenceActivationBytes = `COALESCE(length(CAST(e.event_id AS BLOB)),0)+COALESCE(length(CAST(e.organization_id AS BLOB)),0)+
COALESCE(length(CAST(e.event_type AS BLOB)),0)+COALESCE(length(CAST(e.source_actor_id AS BLOB)),0)+COALESCE(length(CAST(e.source_execution_id AS BLOB)),0)+
COALESCE(length(CAST(e.recipient_scope AS BLOB)),0)+COALESCE(length(CAST(e.recipient_id AS BLOB)),0)+COALESCE(length(CAST(e.task_id AS BLOB)),0)+
COALESCE(length(CAST(e.authorization_refs AS BLOB)),0)+COALESCE(length(CAST(e.artifact_refs AS BLOB)),0)+COALESCE(length(CAST(e.payload AS BLOB)),0)+
COALESCE(length(CAST(e.correlation_id AS BLOB)),0)+COALESCE(length(CAST(e.created_at AS BLOB)),0)+COALESCE(length(CAST(e.schema_version AS BLOB)),0)`

// This validates the selected durable admission and its exact accounting,
// policy and execution bindings. It does not replay global budget competition,
// knowledge selection or all historical capability decisions.
func validateIncidentInference(ctx context.Context, tx *sql.Tx, stream []events.Event, freezes []events.OrganizationFreezeAdmission) error {
	reserved, reservationIDs, policyFingerprints, err := incidentInferenceRequirements(stream)
	if err != nil {
		return err
	}
	organization := ""
	if len(stream) != 0 {
		organization = stream[0].OrganizationID
	}
	support, err := loadIncidentInferenceSupport(ctx, tx, organization, reservationIDs, policyFingerprints)
	if err != nil {
		return err
	}
	manifests := map[string][]events.Event{}
	starts := map[string]events.Event{}
	accounting := map[string]*incidentAccounting{}
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
			payload := reserved[event.EventID]
			frozen := false
			for _, freeze := range freezes {
				if freeze.Sequence < event.Sequence {
					frozen = freeze.Frozen
				}
			}
			if frozen {
				return fmt.Errorf("incident inference reservation occurred during hold")
			}
			row, found := support.reservations[payload.ReservationID]
			policy, policyFound := support.policies[[2]string{event.OrganizationID, payload.PolicyFingerprint}]
			if !found {
				return fmt.Errorf("incident reservation lacks accounting")
			}
			if !policyFound || policy.sequence >= event.Sequence {
				return fmt.Errorf("incident policy activation does not precede reservation")
			}
			if row.validate(policy.value) != nil {
				return fmt.Errorf("incident reservation differs from its policy")
			}
			if err := validateInferenceReservationEvent(event, row, policy.value); err != nil {
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
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM inference_reservations WHERE organization_id=? AND correlation_id=? LIMIT ?)`, stream[0].OrganizationID, stream[0].CorrelationID, len(reservationIDs)+1).Scan(&count); err != nil {
			return err
		}
		if count != len(reservationIDs) {
			return fmt.Errorf("incident inference accounting lacks its exact admission history")
		}
		if err := validateIncidentInferenceLinks(ctx, tx, stream[0].OrganizationID, accounting); err != nil {
			return err
		}
	}
	return nil
}

// Exact selected events already prove each expected row and terminal state.
// Count all same-organization events linked to those reservation identities so
// an extra event cannot escape validation by claiming another correlation.
func validateIncidentInferenceLinks(ctx context.Context, tx *sql.Tx, organization string, accounting map[string]*incidentAccounting) error {
	if len(accounting) == 0 {
		return nil
	}
	ids := make([]string, 0, len(accounting))
	expected := len(accounting)
	for id, entry := range accounting {
		ids = append(ids, id)
		if entry.reconciled {
			expected++
		}
	}
	sort.Strings(ids)
	args := make([]any, 0, len(ids)+2)
	args = append(args, organization)
	for _, id := range ids {
		args = append(args, id)
	}
	args = append(args, expected+1)
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM events WHERE organization_id=? AND event_type IN ('INFERENCE_RESERVED','INFERENCE_RECONCILED') AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.reservation_id') END IN (`+marks+`) LIMIT ?)`, args...).Scan(&count); err != nil {
		return err
	}
	if count != expected {
		return fmt.Errorf("incident inference reservation has extra or missing linked events")
	}
	return nil
}

func incidentInferenceRequirements(stream []events.Event) (map[string]events.InferenceReservedPayload, []string, []string, error) {
	reserved := make(map[string]events.InferenceReservedPayload)
	reservationSet := make(map[string]bool)
	policySet := make(map[string]bool)
	for _, event := range stream {
		if event.EventType != "INFERENCE_RESERVED" {
			continue
		}
		var payload events.InferenceReservedPayload
		if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.ReservationID == "" || reservationSet[payload.ReservationID] {
			return nil, nil, nil, fmt.Errorf("incident inference admission is malformed or duplicated")
		}
		reserved[event.EventID] = payload
		reservationSet[payload.ReservationID] = true
		policySet[payload.PolicyFingerprint] = true
	}
	reservationIDs := make([]string, 0, len(reservationSet))
	for id := range reservationSet {
		reservationIDs = append(reservationIDs, id)
	}
	sort.Strings(reservationIDs)
	policyFingerprints := make([]string, 0, len(policySet))
	for fingerprint := range policySet {
		policyFingerprints = append(policyFingerprints, fingerprint)
	}
	sort.Strings(policyFingerprints)
	return reserved, reservationIDs, policyFingerprints, nil
}

func loadIncidentInferenceSupport(ctx context.Context, tx *sql.Tx, organization string, reservationIDs, policyFingerprints []string) (incidentInferenceSupport, error) {
	support := incidentInferenceSupport{
		reservations: make(map[string]inferenceValidationRow, len(reservationIDs)),
		policies:     make(map[[2]string]incidentPolicy, len(policyFingerprints)),
	}
	if len(reservationIDs) == 0 {
		return support, nil
	}
	reservationMarks := strings.TrimSuffix(strings.Repeat("?,", len(reservationIDs)), ",")
	reservationArgs := make([]any, 0, len(reservationIDs)+1)
	reservationArgs = append(reservationArgs, organization)
	for _, id := range reservationIDs {
		reservationArgs = append(reservationArgs, id)
	}
	var reservationCount int
	var reservationBytes int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM (SELECT `+incidentReservationBytes+` AS bytes FROM inference_reservations WHERE organization_id=? AND reservation_id IN (`+reservationMarks+`) ORDER BY reservation_id LIMIT 257)`, reservationArgs...).Scan(&reservationCount, &reservationBytes); err != nil {
		return support, err
	}
	if reservationCount != len(reservationIDs) {
		return support, fmt.Errorf("incident reservation lacks accounting")
	}
	if reservationBytes > 2<<20 {
		return support, fmt.Errorf("incident inference accounting exceeds byte limit")
	}

	policyMarks := strings.TrimSuffix(strings.Repeat("?,", len(policyFingerprints)), ",")
	policyArgs := make([]any, 0, len(policyFingerprints)+1)
	policyArgs = append(policyArgs, organization)
	for _, fingerprint := range policyFingerprints {
		policyArgs = append(policyArgs, fingerprint)
	}
	var policyCount, activationCount int
	var policyBytes, activationBytes int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(policy_bytes),0),COALESCE(SUM(activation_bytes),0),COALESCE(SUM(has_activation),0) FROM (SELECT `+incidentInferencePolicyBytes+` AS policy_bytes,`+incidentInferenceActivationBytes+` AS activation_bytes,CASE WHEN e.event_id IS NULL THEN 0 ELSE 1 END AS has_activation FROM inference_policies p LEFT JOIN events e ON e.event_id=p.activation_event_id AND e.organization_id=p.organization_id WHERE p.organization_id=? AND p.policy_fingerprint IN (`+policyMarks+`) ORDER BY p.policy_fingerprint LIMIT 257)`, policyArgs...).Scan(&policyCount, &policyBytes, &activationBytes, &activationCount); err != nil {
		return support, err
	}
	if policyCount != len(policyFingerprints) || activationCount != policyCount || policyBytes > (2<<20)-reservationBytes || activationBytes > (2<<20)-reservationBytes-policyBytes {
		return support, fmt.Errorf("incident inference supporting evidence exceeds byte limit or lacks activation")
	}

	rows, err := tx.QueryContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd,window_started_at,window_expires_at,connection_id,created_at FROM inference_reservations WHERE organization_id=? AND reservation_id IN (`+reservationMarks+`) ORDER BY reservation_id`, reservationArgs...)
	if err != nil {
		return support, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row inferenceValidationRow
		if err := rows.Scan(&row.reservationID, &row.requestID, &row.organizationID, &row.purpose, &row.intentID, &row.taskID, &row.executionID, &row.correlationID, &row.promptSHA256, &row.provider, &row.model, &row.profile, &row.policyFingerprint, &row.state, &row.reservedInput, &row.reservedOutput, &row.reservedCost, &row.chargedInput, &row.chargedOutput, &row.chargedCost, &row.windowStart, &row.windowEnd, &row.connectionID, &row.createdAt); err != nil {
			_ = rows.Close()
			return support, err
		}
		if _, duplicate := support.reservations[row.reservationID]; duplicate {
			_ = rows.Close()
			return support, fmt.Errorf("incident reservation accounting is duplicated")
		}
		support.reservations[row.reservationID] = row
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return support, err
	}
	if err := rows.Close(); err != nil {
		return support, err
	}
	if len(support.reservations) != len(reservationIDs) {
		return support, fmt.Errorf("incident reservation lacks accounting")
	}

	type storedPolicy struct {
		fingerprint, activationID, connection string
		body                                  []byte
	}
	stored := make([]storedPolicy, 0, len(policyFingerprints))
	policyRows, err := tx.QueryContext(ctx, `SELECT policy_fingerprint,body,activation_event_id,connection_id FROM inference_policies WHERE organization_id=? AND policy_fingerprint IN (`+policyMarks+`) ORDER BY policy_fingerprint`, policyArgs...)
	if err != nil {
		return support, err
	}
	defer func() { _ = policyRows.Close() }()
	activationSet := make(map[string]bool, len(policyFingerprints))
	for policyRows.Next() {
		var policy storedPolicy
		if err := policyRows.Scan(&policy.fingerprint, &policy.body, &policy.activationID, &policy.connection); err != nil {
			_ = policyRows.Close()
			return support, err
		}
		stored = append(stored, policy)
		activationSet[policy.activationID] = true
	}
	if err := policyRows.Err(); err != nil {
		_ = policyRows.Close()
		return support, err
	}
	if err := policyRows.Close(); err != nil {
		return support, err
	}
	if len(stored) != len(policyFingerprints) {
		return support, fmt.Errorf("incident inference policy is missing")
	}

	activationIDs := make([]string, 0, len(activationSet))
	for id := range activationSet {
		activationIDs = append(activationIDs, id)
	}
	sort.Strings(activationIDs)
	activationMarks := strings.TrimSuffix(strings.Repeat("?,", len(activationIDs)), ",")
	activationArgs := make([]any, 0, len(activationIDs)+1)
	activationArgs = append(activationArgs, organization)
	for _, id := range activationIDs {
		activationArgs = append(activationArgs, id)
	}
	activations, err := collectEvents(tx.QueryContext(ctx, `SELECT `+incidentEventColumns+` FROM events WHERE organization_id=? AND event_id IN (`+activationMarks+`) ORDER BY event_id`, activationArgs...))
	if err != nil {
		return support, err
	}
	activationByID := make(map[string]events.Event, len(activations))
	for _, activation := range activations {
		activationByID[activation.EventID] = activation
	}
	if len(activationByID) != len(activationIDs) {
		return support, fmt.Errorf("incident inference supporting evidence lacks activation")
	}
	for _, storedPolicy := range stored {
		policy, err := validateIncidentInferencePolicy(organization, storedPolicy.fingerprint, storedPolicy.connection, storedPolicy.body, activationByID[storedPolicy.activationID])
		if err != nil {
			return support, err
		}
		support.policies[[2]string{organization, storedPolicy.fingerprint}] = incidentPolicy{value: policy, sequence: activationByID[storedPolicy.activationID].Sequence}
	}
	return support, nil
}

func validateIncidentInferencePolicy(organization, fingerprint, connection string, body []byte, activation events.Event) (inference.Policy, error) {
	var policy inference.Policy
	if decodeExactJSONBytes(body, &policy) != nil || policy.Validate() != nil || policy.OrganizationID != organization || policy.ConnectionID != connection {
		return inference.Policy{}, fmt.Errorf("incident inference policy is invalid")
	}
	actual, err := policy.Fingerprint()
	if err != nil || actual != fingerprint {
		return inference.Policy{}, fmt.Errorf("incident inference policy fingerprint is invalid")
	}
	var activated events.InferencePolicyActivatedPayload
	expected := events.InferencePolicyActivatedPayload{ConnectionID: policy.ConnectionID, PolicyFingerprint: fingerprint, Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion, AccessMode: string(policy.Mode), AuthorizedBy: policy.AuthorizedBy, AuthorizedAt: policy.AuthorizedAt, AuthorizationExpiresAt: policy.AuthorizationExpiresAt}
	if len(fingerprint) < 16 || activation.EventType != "INFERENCE_POLICY_ACTIVATED" || activation.OrganizationID != organization || activation.SourceActorID != policy.AuthorizedBy || activation.SourceExecutionID != "" || activation.TaskID != "" || activation.RecipientID != "" || activation.RecipientScope != "" || len(activation.AuthorizationRefs) != 0 || len(activation.ArtifactRefs) != 0 || activation.SchemaVersion != events.SchemaVersion || activation.CorrelationID != "inference-policy-"+fingerprint[:16] || decodeExactJSONBytes(activation.Payload, &activated) != nil || !reflect.DeepEqual(activated, expected) {
		return inference.Policy{}, fmt.Errorf("incident policy lacks its exact prior activation")
	}
	return policy, nil
}
