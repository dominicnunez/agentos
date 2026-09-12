package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

// ValidateInferenceRouteBinding validates provenance without reserving a call
// or publishing a context. Dispatch still performs authoritative admission.
func (l *SQLite) ValidateInferenceRouteBinding(ctx context.Context, binding modelinput.RouteBinding) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	ctx, tx, err := l.beginFreezeRead(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	pending := inference.InferenceRequest{Scope: inference.Scope{OrganizationID: binding.Requirements.OrganizationID, Routing: &binding.Requirements, RoutingDecision: &binding.Decision}}
	if err := validateInferenceAdmissionsSnapshot(ctx, tx, pending); err != nil {
		return err
	}
	return tx.Commit()
}

// ValidateInferenceAdmissions verifies the live ledger before runtime code can
// trust its mutable budget-accounting columns.
func (l *SQLite) ValidateInferenceAdmissions(ctx context.Context) error {
	if l == nil || l.db == nil {
		return fmt.Errorf("inference admission ledger is required")
	}
	ctx, tx, err := l.beginFreezeRead(ctx)
	if err != nil {
		return fmt.Errorf("begin inference admission snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateInferenceAdmissionsSnapshot(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ValidateInferenceAdmissions proves that every durable policy and reservation
// has its exact Event Contract and that current accounting can be reconstructed
// without trusting mutable configuration.
func ValidateInferenceAdmissions(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("inference admission database is required")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin inference admission snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateInferenceAdmissionsSnapshot(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func validateInferenceAdmissionsSnapshot(ctx context.Context, tx *sql.Tx, pending ...inference.InferenceRequest) error {
	if len(pending) > 1 {
		return fmt.Errorf("only one pending inference request can be validated")
	}
	var storageVersion int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&storageVersion); err != nil {
		return fmt.Errorf("read inference storage version: %w", err)
	}
	// Older offline snapshots have no connection column. Empty identifies only
	// the original singleton policy; it never selects a configured connection.
	connectionColumn := "''"
	if storageVersion >= 10 {
		connectionColumn = "connection_id"
	}
	_, freezes, err := authorityAdmissionsSnapshot(ctx, tx)
	if err != nil {
		return fmt.Errorf("validate inference authority history: %w", err)
	}
	byOrganization := make(map[core.ID][]events.OrganizationFreezeAdmission)
	for _, freeze := range freezes {
		byOrganization[freeze.OrganizationID] = append(byOrganization[freeze.OrganizationID], freeze)
	}
	for _, history := range byOrganization {
		sort.Slice(history, func(i, j int) bool { return history[i].Sequence < history[j].Sequence })
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events ORDER BY sequence`))
	if err != nil {
		return fmt.Errorf("read inference admission events: %w", err)
	}
	if err := validateInferenceRouteRejections(stream); err != nil {
		return err
	}
	eventsByID := make(map[string]events.Event, len(stream))
	reservedEvents := make(map[string]events.Event)
	reconciledEvents := make(map[string][]events.Event)
	lastReservation := make(map[string]int64)
	for _, event := range stream {
		if event.EventType == "INFERENCE_RESERVED" {
			lastReservation[event.OrganizationID] = event.Sequence
		}
	}
	executionHistory := make(map[string]*inferenceExecutionHistory)
	var inbox map[string]events.InboxObservationBinding
	for _, event := range stream {
		if event.Sequence < lastReservation[event.OrganizationID] {
			history := executionHistory[event.OrganizationID]
			if history == nil {
				history = newInferenceExecutionHistory()
				executionHistory[event.OrganizationID] = history
			}
			if err := history.observe(event); err != nil {
				return err
			}
		}
		if event.EventType != "INFERENCE_POLICY_ACTIVATED" && event.EventType != "INFERENCE_RESERVED" && event.EventType != "INFERENCE_RECONCILED" {
			continue
		}
		if _, exists := eventsByID[event.EventID]; exists {
			return fmt.Errorf("inference admission contains a duplicate event")
		}
		eventsByID[event.EventID] = event
		switch event.EventType {
		case "INFERENCE_RESERVED":
			history := byOrganization[core.ID(event.OrganizationID)]
			index := sort.Search(len(history), func(i int) bool { return history[i].Sequence >= event.Sequence })
			if index > 0 && history[index-1].Frozen {
				return fmt.Errorf("inference reservation was admitted while organization was frozen")
			}
			var payload events.InferenceReservedPayload
			if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.ReservationID == "" {
				return fmt.Errorf("inference reservation event is malformed")
			}
			execution := executionHistory[event.OrganizationID]
			if execution == nil {
				execution = newInferenceExecutionHistory()
				executionHistory[event.OrganizationID] = execution
			}
			if err := execution.validateReservation(ctx, tx, event, payload, &inbox); err != nil {
				return err
			}
			if _, exists := reservedEvents[payload.ReservationID]; exists {
				return fmt.Errorf("inference reservation has multiple admission events")
			}
			reservedEvents[payload.ReservationID] = event
		case "INFERENCE_RECONCILED":
			var payload events.InferenceReconciledPayload
			if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.ReservationID == "" {
				return fmt.Errorf("inference reconciliation event is malformed")
			}
			reconciledEvents[payload.ReservationID] = append(reconciledEvents[payload.ReservationID], event)
		}
	}

	type policyKey struct{ organizationID, fingerprint string }
	policies := make(map[policyKey]inference.Policy)
	activationPolicies := make(map[string]inference.Policy)
	usedEvents := make(map[string]struct{})
	type connectionKey struct{ organizationID, connectionID string }
	activeByConnection := make(map[connectionKey]int)
	latestActivation := make(map[connectionKey]int64)
	activeActivation := make(map[connectionKey]int64)
	activeOrganizationBudgets := make(map[string]inference.OrganizationBudget)
	activeRouting := make(map[string]*inference.RoutePolicy)
	connectionHistory := make(map[connectionKey][]inferencePolicyRevision)
	rows, err := tx.QueryContext(ctx, `SELECT organization_id,policy_fingerprint,body,activation_event_id,activated_at,active,`+connectionColumn+` FROM inference_policies ORDER BY organization_id,activated_at,policy_fingerprint`)
	if err != nil {
		return fmt.Errorf("read inference policy history: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var organizationID, fingerprint, activationEventID, activatedAt, connectionID string
		var body []byte
		var active int
		if err := rows.Scan(&organizationID, &fingerprint, &body, &activationEventID, &activatedAt, &active, &connectionID); err != nil {
			return fmt.Errorf("scan inference policy history: %w", err)
		}
		var policy inference.Policy
		calculated := ""
		if decodeExactJSONBytes(body, &policy) == nil {
			calculated, _ = policy.Fingerprint()
		}
		activated, timeErr := time.Parse(time.RFC3339Nano, activatedAt)
		key := policyKey{organizationID, fingerprint}
		if connectionID != policy.ConnectionID {
			return fmt.Errorf("inference policy connection differs from admitted policy")
		}
		if organizationID == "" || fingerprint == "" || policy.Validate() != nil || policy.OrganizationID != organizationID || calculated != fingerprint || activationEventID == "" || timeErr != nil || activated.IsZero() || active != 0 && active != 1 {
			return fmt.Errorf("inference policy history is invalid")
		}
		if _, exists := policies[key]; exists {
			return fmt.Errorf("inference policy history is duplicated")
		}
		connection := connectionKey{organizationID, connectionID}
		activation, found := eventsByID[activationEventID]
		var payload events.InferencePolicyActivatedPayload
		expected := events.InferencePolicyActivatedPayload{
			ConnectionID:      policy.ConnectionID,
			PolicyFingerprint: fingerprint, Provider: policy.Provider, Model: policy.Model,
			ExecutionProfileVersion: policy.ExecutionProfileVersion, AccessMode: string(policy.Mode),
			AuthorizedBy: policy.AuthorizedBy, AuthorizedAt: policy.AuthorizedAt,
			AuthorizationExpiresAt: policy.AuthorizationExpiresAt,
		}
		if !found || activation.EventType != "INFERENCE_POLICY_ACTIVATED" || activation.OrganizationID != organizationID || activation.SourceActorID != policy.AuthorizedBy || activation.SourceExecutionID != "" || activation.RecipientScope != "" || activation.RecipientID != "" || activation.TaskID != "" || len(activation.AuthorizationRefs) != 0 || len(activation.ArtifactRefs) != 0 || activation.CorrelationID != "inference-policy-"+fingerprint[:16] || activation.SchemaVersion != events.SchemaVersion || decodeExactJSONBytes(activation.Payload, &payload) != nil || !reflect.DeepEqual(payload, expected) {
			return fmt.Errorf("inference policy lacks its exact activation event")
		}
		usedEvents[activationEventID] = struct{}{}
		activationPolicies[activationEventID] = policy
		if active == 1 {
			if policy.Version == inference.ConnectionPolicyVersion {
				if prior, exists := activeRouting[organizationID]; exists && !inference.SameRoutePolicy(prior, policy.Routing) {
					return fmt.Errorf("active connections disagree on organization routing policy")
				}
				activeRouting[organizationID] = policy.Routing
			}
			activeByConnection[connection]++
			activeActivation[connection] = activation.Sequence
			if policy.OrganizationBudget != nil {
				if budget, exists := activeOrganizationBudgets[organizationID]; exists && budget != *policy.OrganizationBudget {
					return fmt.Errorf("active connections disagree on organization inference budget")
				}
				activeOrganizationBudgets[organizationID] = *policy.OrganizationBudget
			}
		}
		if activation.Sequence > latestActivation[connection] {
			latestActivation[connection] = activation.Sequence
		}
		policies[key] = policy
		if policy.Version == inference.ConnectionPolicyVersion {
			connectionHistory[connection] = append(connectionHistory[connection], inferencePolicyRevision{
				fingerprint: fingerprint, sequence: activation.Sequence, authorizedAt: policy.AuthorizedAt,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate inference policy history: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close inference policy history: %w", err)
	}
	for connection, sequence := range latestActivation {
		if activeByConnection[connection] != 1 || activeActivation[connection] != sequence {
			return fmt.Errorf("inference connection policy history has no unique current active revision")
		}
	}
	for connection, history := range connectionHistory {
		sort.Slice(history, func(i, j int) bool { return history[i].sequence < history[j].sequence })
		for i, revision := range history {
			if revision.sequence < 1 || i > 0 && (history[i-1].sequence >= revision.sequence || !revision.authorizedAt.After(history[i-1].authorizedAt)) {
				return fmt.Errorf("connection policy replacement history is invalid")
			}
		}
		connectionHistory[connection] = history
	}

	reservationRows, err := tx.QueryContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd,window_started_at,window_expires_at,`+connectionColumn+`,created_at FROM inference_reservations ORDER BY created_at,reservation_id`)
	if err != nil {
		return fmt.Errorf("read inference reservation history: %w", err)
	}
	defer func() { _ = reservationRows.Close() }()
	for reservationRows.Next() {
		var row inferenceValidationRow
		if err := reservationRows.Scan(&row.reservationID, &row.requestID, &row.organizationID, &row.purpose, &row.intentID, &row.taskID, &row.executionID, &row.correlationID, &row.promptSHA256, &row.provider, &row.model, &row.profile, &row.policyFingerprint, &row.state, &row.reservedInput, &row.reservedOutput, &row.reservedCost, &row.chargedInput, &row.chargedOutput, &row.chargedCost, &row.windowStart, &row.windowEnd, &row.connectionID, &row.createdAt); err != nil {
			return fmt.Errorf("scan inference reservation history: %w", err)
		}
		policy, found := policies[policyKey{row.organizationID, row.policyFingerprint}]
		if !found || row.validate(policy) != nil {
			return fmt.Errorf("inference reservation history is invalid")
		}
		admission, found := reservedEvents[row.reservationID]
		if !found || validateInferenceReservationEvent(admission, row, policy) != nil {
			return fmt.Errorf("inference reservation lacks its exact admission event")
		}
		usedEvents[admission.EventID] = struct{}{}
		reconciliations := reconciledEvents[row.reservationID]
		if policy.Version == inference.ConnectionPolicyVersion {
			history := connectionHistory[connectionKey{row.organizationID, row.connectionID}]
			if err := validateConnectionPolicyLifetime(history, row.policyFingerprint, admission.Sequence, reconciliations); err != nil {
				return err
			}
		}
		if row.state == inferenceStateReserved {
			if len(reconciliations) != 0 {
				return fmt.Errorf("active inference reservation has terminal reconciliation")
			}
			continue
		}
		if len(reconciliations) != 1 || validateInferenceReconciliationEvent(reconciliations[0], row) != nil || reconciliations[0].Sequence <= admission.Sequence {
			return fmt.Errorf("inference reservation lacks its exact terminal reconciliation")
		}
		usedEvents[reconciliations[0].EventID] = struct{}{}
	}
	if err := reservationRows.Err(); err != nil {
		return fmt.Errorf("iterate inference reservation history: %w", err)
	}
	for eventID := range eventsByID {
		if _, used := usedEvents[eventID]; !used {
			return fmt.Errorf("inference admission event is not materialized by durable accounting")
		}
	}
	if err := validateOrganizationBudgetHistory(stream, activationPolicies); err != nil {
		return err
	}
	for _, request := range pending {
		if request.Scope.RoutingDecision == nil {
			continue
		}
		if len(stream) == 0 || stream[len(stream)-1].Sequence == math.MaxInt64 {
			return fmt.Errorf("routing snapshot lacks a usable ledger cutoff")
		}
		// Include a pending library request even when it has no application
		// context. This synthetic observer is never persisted or charged.
		payload, err := json.Marshal(events.InferenceReservedPayload{Routing: request.Scope.Routing, RoutingDecision: request.Scope.RoutingDecision})
		if err != nil {
			return err
		}
		// An unpersisted candidate has no binding timestamp yet. Use its own
		// selection time for this observer; actual persisted copies must each
		// prove the upper bound using their immutable event timestamp.
		stream = append(stream, events.Event{OrganizationID: request.Scope.OrganizationID, EventType: "INFERENCE_RESERVED", Sequence: stream[len(stream)-1].Sequence + 1, CreatedAt: request.Scope.RoutingDecision.SelectedAt, Payload: payload})
	}
	return validateRoutingDecisionHistory(stream, activationPolicies, byOrganization)
}

type inferenceValidationRow struct {
	reservationID, requestID, organizationID, purpose, intentID, taskID, executionID, correlationID string
	promptSHA256, provider, model, profile, policyFingerprint, state, connectionID                  string
	reservedInput, reservedOutput, reservedCost, chargedInput, chargedOutput, chargedCost           int64
	windowStart, windowEnd, createdAt                                                               string
}

func (r inferenceValidationRow) validate(policy inference.Policy) error {
	start, startErr := time.Parse(time.RFC3339Nano, r.windowStart)
	end, endErr := time.Parse(time.RFC3339Nano, r.windowEnd)
	duration := time.Duration(policy.WindowDurationSeconds) * time.Second
	expectedCost, costErr := policy.ReservedCostNanoUSD()
	if r.connectionID != policy.ConnectionID || r.reservationID == "" || r.requestID == "" || r.organizationID != policy.OrganizationID || r.executionID == "" || r.correlationID == "" || r.provider != policy.Provider || r.model != policy.Model || r.profile != policy.ExecutionProfileVersion || r.policyFingerprint == "" || !validSHA256Hex(r.promptSHA256) || r.reservedInput != policy.MaxInputTokensPerRequest || r.reservedOutput != policy.MaxOutputTokensPerRequest || costErr != nil || r.reservedCost != expectedCost || startErr != nil || endErr != nil || !end.Equal(start.Add(duration)) || start.Unix()%policy.WindowDurationSeconds != 0 || r.chargedInput < 0 || r.chargedOutput < 0 || r.chargedCost < 0 {
		return fmt.Errorf("inference reservation fields are invalid")
	}
	switch inference.Purpose(r.purpose) {
	case inference.PurposeIntentNormalization, inference.PurposePlanning, inference.PurposeTaskExecution:
	default:
		return fmt.Errorf("inference reservation purpose is invalid")
	}
	if r.intentID == "" && r.taskID == "" {
		return fmt.Errorf("inference reservation is not bound to work")
	}
	switch r.state {
	case inferenceStateReserved, inferenceStateUncertain, string(inference.ReconciliationTerminalFailedNoUsage), string(inference.ReconciliationTerminalIncompleteNoUsage):
		if r.chargedInput != r.reservedInput || r.chargedOutput != r.reservedOutput || r.chargedCost != r.reservedCost {
			return fmt.Errorf("unresolved inference reservation released resources")
		}
	case inferenceStateCompleted, string(inference.ReconciliationTerminalFailed), string(inference.ReconciliationTerminalIncomplete):
		if r.chargedInput > r.reservedInput || r.chargedOutput > r.reservedOutput {
			return fmt.Errorf("completed inference reservation exceeded its limits")
		}
		usage := events.InferenceUsageRecordedPayload{Source: "recovery", Provider: r.provider, Model: r.model, InputTokens: int(r.chargedInput), OutputTokens: int(r.chargedOutput), TotalTokens: int(r.chargedInput + r.chargedOutput)}
		expected, err := policy.ActualCostNanoUSD(usage)
		if err != nil || expected != r.chargedCost {
			return fmt.Errorf("completed inference cost is invalid")
		}
	case inferenceStateViolation:
		if r.chargedInput < r.reservedInput || r.chargedOutput < r.reservedOutput || r.chargedCost < r.reservedCost {
			return fmt.Errorf("inference violation did not retain its conservative charge")
		}
	case inferenceStateNotSent:
		if r.chargedInput != 0 || r.chargedOutput != 0 || r.chargedCost != 0 {
			return fmt.Errorf("unsent inference reservation retained a charge")
		}
	default:
		return fmt.Errorf("inference reservation state is invalid")
	}
	return nil
}

func validateInferenceReservationEvent(event events.Event, row inferenceValidationRow, policy inference.Policy) error {
	var payload events.InferenceReservedPayload
	if decodeExactJSONBytes(event.Payload, &payload) != nil {
		return fmt.Errorf("inference reservation event is invalid")
	}
	start, _ := time.Parse(time.RFC3339Nano, row.windowStart)
	end, _ := time.Parse(time.RFC3339Nano, row.windowEnd)
	if payload.AdmittedAt != "" {
		admitted, err := time.Parse(time.RFC3339Nano, payload.AdmittedAt)
		if err != nil || admitted.IsZero() || payload.AdmittedAt != row.createdAt || admitted.Before(start) || !admitted.Before(end) {
			return fmt.Errorf("inference reservation admission time is invalid")
		}
		if admitted.Before(policy.AuthorizedAt) || !admitted.Before(policy.AuthorizationExpiresAt) || policy.Pricing != nil && !admitted.Before(policy.Pricing.ExpiresAt) {
			return fmt.Errorf("inference reservation was admitted outside policy or pricing validity")
		}
	} else if row.connectionID != "" {
		return fmt.Errorf("connection reservation lacks its admission time")
	}
	at, _ := time.Parse(time.RFC3339Nano, payload.AdmittedAt)
	request := inference.InferenceRequest{
		ConnectionID: row.connectionID, PromptSHA256: row.promptSHA256,
		Scope: inference.Scope{OrganizationID: row.organizationID, Purpose: inference.Purpose(row.purpose), RequestID: row.requestID,
			IntentID: row.intentID, TaskID: row.taskID, ExecutionID: row.executionID, CorrelationID: row.correlationID, Routing: payload.Routing, RoutingDecision: payload.RoutingDecision},
	}
	request.Descriptor.Provider = row.provider
	request.Descriptor.Model = row.model
	request.Descriptor.ExecutionProfileVersion = row.profile
	if err := inference.ValidateRequestRouting(at, policy, request); err != nil {
		return fmt.Errorf("inference reservation routing is invalid: %w", err)
	}
	expected := events.InferenceReservedPayload{
		Routing:         payload.Routing,
		RoutingDecision: payload.RoutingDecision,
		AdmittedAt:      payload.AdmittedAt,
		// The execution reference is independently verified against its historical
		// boundary by validateReservedExecutionKnowledge before accounting checks.
		ConnectionID: row.connectionID, ExecutionManifestRef: payload.ExecutionManifestRef,
		ReservationID: row.reservationID, RequestID: row.requestID, Purpose: row.purpose, IntentID: row.intentID,
		PolicyFingerprint: row.policyFingerprint, PromptSHA256: row.promptSHA256, Provider: row.provider, Model: row.model,
		ExecutionProfileVersion: row.profile, ReservedInputTokens: row.reservedInput,
		ReservedOutputTokens: row.reservedOutput, ReservedCostNanoUSD: row.reservedCost,
		WindowStartedAt: start, WindowExpiresAt: end,
	}
	if event.EventType != "INFERENCE_RESERVED" || event.OrganizationID != row.organizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != row.executionID || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != row.taskID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != row.correlationID || event.SchemaVersion != events.SchemaVersion || decodeExactJSONBytes(event.Payload, &payload) != nil || !reflect.DeepEqual(payload, expected) {
		return fmt.Errorf("inference reservation event is invalid")
	}
	return nil
}

func validateInferenceReconciliationEvent(event events.Event, row inferenceValidationRow) error {
	var payload events.InferenceReconciledPayload
	expected := events.InferenceReconciledPayload{
		ConnectionID: row.connectionID, ReservationID: row.reservationID, State: row.state, ChargedInputTokens: row.chargedInput,
		ChargedOutputTokens: row.chargedOutput, ChargedCostNanoUSD: row.chargedCost,
	}
	if event.EventType != "INFERENCE_RECONCILED" || event.OrganizationID != row.organizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != row.executionID || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != row.taskID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != row.correlationID || event.SchemaVersion != events.SchemaVersion || decodeExactJSONBytes(event.Payload, &payload) != nil || !reflect.DeepEqual(payload, expected) {
		return fmt.Errorf("inference reconciliation event is invalid")
	}
	return nil
}
