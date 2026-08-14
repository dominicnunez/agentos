package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// ValidateInferenceAdmissions verifies the live ledger before runtime code can
// trust its mutable budget-accounting columns.
func (l *SQLite) ValidateInferenceAdmissions(ctx context.Context) error {
	if l == nil || l.db == nil {
		return fmt.Errorf("inference admission ledger is required")
	}
	return ValidateInferenceAdmissions(ctx, l.db)
}

// ValidateInferenceAdmissions proves that every durable policy and reservation
// has its exact Event Contract and that current accounting can be reconstructed
// without trusting mutable configuration.
func ValidateInferenceAdmissions(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("inference admission database is required")
	}
	stream, err := collectEvents(db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_type IN ('INFERENCE_POLICY_ACTIVATED','INFERENCE_RESERVED','INFERENCE_RECONCILED') ORDER BY sequence`))
	if err != nil {
		return fmt.Errorf("read inference admission events: %w", err)
	}
	eventsByID := make(map[string]events.Event, len(stream))
	reservedEvents := make(map[string]events.Event)
	reconciledEvents := make(map[string][]events.Event)
	for _, event := range stream {
		if _, exists := eventsByID[event.EventID]; exists {
			return fmt.Errorf("inference admission contains a duplicate event")
		}
		eventsByID[event.EventID] = event
		switch event.EventType {
		case "INFERENCE_RESERVED":
			var payload events.InferenceReservedPayload
			if decodeExactJSONBytes(event.Payload, &payload) != nil || payload.ReservationID == "" {
				return fmt.Errorf("inference reservation event is malformed")
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
	usedEvents := make(map[string]struct{})
	activeByOrganization := make(map[string]int)
	policyOrganizations := make(map[string]struct{})
	rows, err := db.QueryContext(ctx, `SELECT organization_id,policy_fingerprint,body,activation_event_id,activated_at,active FROM inference_policies ORDER BY organization_id,activated_at,policy_fingerprint`)
	if err != nil {
		return fmt.Errorf("read inference policy history: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var organizationID, fingerprint, activationEventID, activatedAt string
		var body []byte
		var active int
		if err := rows.Scan(&organizationID, &fingerprint, &body, &activationEventID, &activatedAt, &active); err != nil {
			return fmt.Errorf("scan inference policy history: %w", err)
		}
		var policy inference.Policy
		calculated := ""
		if decodeExactJSONBytes(body, &policy) == nil {
			calculated, _ = policy.Fingerprint()
		}
		activated, timeErr := time.Parse(time.RFC3339Nano, activatedAt)
		key := policyKey{organizationID, fingerprint}
		if organizationID == "" || fingerprint == "" || policy.Validate() != nil || policy.OrganizationID != organizationID || calculated != fingerprint || activationEventID == "" || timeErr != nil || activated.IsZero() || active != 0 && active != 1 {
			return fmt.Errorf("inference policy history is invalid")
		}
		if _, exists := policies[key]; exists {
			return fmt.Errorf("inference policy history is duplicated")
		}
		if active == 1 {
			activeByOrganization[organizationID]++
		}
		policyOrganizations[organizationID] = struct{}{}
		activation, found := eventsByID[activationEventID]
		var payload events.InferencePolicyActivatedPayload
		expected := events.InferencePolicyActivatedPayload{
			PolicyFingerprint: fingerprint, Provider: policy.Provider, Model: policy.Model,
			ExecutionProfileVersion: policy.ExecutionProfileVersion, AccessMode: string(policy.Mode),
			AuthorizedBy: policy.AuthorizedBy, AuthorizedAt: policy.AuthorizedAt,
			AuthorizationExpiresAt: policy.AuthorizationExpiresAt,
		}
		if !found || activation.EventType != "INFERENCE_POLICY_ACTIVATED" || activation.OrganizationID != organizationID || activation.SourceActorID != policy.AuthorizedBy || activation.SourceExecutionID != "" || activation.RecipientScope != "" || activation.RecipientID != "" || activation.TaskID != "" || len(activation.AuthorizationRefs) != 0 || len(activation.ArtifactRefs) != 0 || activation.CorrelationID != "inference-policy-"+fingerprint[:16] || activation.SchemaVersion != events.SchemaVersion || decodeExactJSONBytes(activation.Payload, &payload) != nil || !reflect.DeepEqual(payload, expected) {
			return fmt.Errorf("inference policy lacks its exact activation event")
		}
		usedEvents[activationEventID] = struct{}{}
		policies[key] = policy
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate inference policy history: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close inference policy history: %w", err)
	}
	for organizationID := range policyOrganizations {
		if organizationID == "" || activeByOrganization[organizationID] != 1 {
			return fmt.Errorf("organization inference policy history has no unique active revision")
		}
	}

	reservationRows, err := db.QueryContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd,window_started_at,window_expires_at FROM inference_reservations ORDER BY created_at,reservation_id`)
	if err != nil {
		return fmt.Errorf("read inference reservation history: %w", err)
	}
	defer func() { _ = reservationRows.Close() }()
	for reservationRows.Next() {
		var row inferenceValidationRow
		if err := reservationRows.Scan(&row.reservationID, &row.requestID, &row.organizationID, &row.purpose, &row.intentID, &row.taskID, &row.executionID, &row.correlationID, &row.promptSHA256, &row.provider, &row.model, &row.profile, &row.policyFingerprint, &row.state, &row.reservedInput, &row.reservedOutput, &row.reservedCost, &row.chargedInput, &row.chargedOutput, &row.chargedCost, &row.windowStart, &row.windowEnd); err != nil {
			return fmt.Errorf("scan inference reservation history: %w", err)
		}
		policy, found := policies[policyKey{row.organizationID, row.policyFingerprint}]
		if !found || row.validate(policy) != nil {
			return fmt.Errorf("inference reservation history is invalid")
		}
		admission, found := reservedEvents[row.reservationID]
		if !found || validateInferenceReservationEvent(admission, row) != nil {
			return fmt.Errorf("inference reservation lacks its exact admission event")
		}
		usedEvents[admission.EventID] = struct{}{}
		reconciliations := reconciledEvents[row.reservationID]
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
	return nil
}

type inferenceValidationRow struct {
	reservationID, requestID, organizationID, purpose, intentID, taskID, executionID, correlationID string
	promptSHA256, provider, model, profile, policyFingerprint, state                                string
	reservedInput, reservedOutput, reservedCost, chargedInput, chargedOutput, chargedCost           int64
	windowStart, windowEnd                                                                          string
}

func (r inferenceValidationRow) validate(policy inference.Policy) error {
	start, startErr := time.Parse(time.RFC3339Nano, r.windowStart)
	end, endErr := time.Parse(time.RFC3339Nano, r.windowEnd)
	duration := time.Duration(policy.WindowDurationSeconds) * time.Second
	expectedCost, costErr := policy.ReservedCostNanoUSD()
	if r.reservationID == "" || r.requestID == "" || r.organizationID != policy.OrganizationID || r.executionID == "" || r.correlationID == "" || r.provider != policy.Provider || r.model != policy.Model || r.profile != policy.ExecutionProfileVersion || r.policyFingerprint == "" || !validSHA256Hex(r.promptSHA256) || r.reservedInput != policy.MaxInputTokensPerRequest || r.reservedOutput != policy.MaxOutputTokensPerRequest || costErr != nil || r.reservedCost != expectedCost || startErr != nil || endErr != nil || !end.Equal(start.Add(duration)) || start.Unix()%policy.WindowDurationSeconds != 0 || r.chargedInput < 0 || r.chargedOutput < 0 || r.chargedCost < 0 {
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
	case inferenceStateReserved, inferenceStateUncertain:
		if r.chargedInput != r.reservedInput || r.chargedOutput != r.reservedOutput || r.chargedCost != r.reservedCost {
			return fmt.Errorf("unresolved inference reservation released resources")
		}
	case inferenceStateCompleted:
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

func validateInferenceReservationEvent(event events.Event, row inferenceValidationRow) error {
	var payload events.InferenceReservedPayload
	start, _ := time.Parse(time.RFC3339Nano, row.windowStart)
	end, _ := time.Parse(time.RFC3339Nano, row.windowEnd)
	expected := events.InferenceReservedPayload{
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
		ReservationID: row.reservationID, State: row.state, ChargedInputTokens: row.chargedInput,
		ChargedOutputTokens: row.chargedOutput, ChargedCostNanoUSD: row.chargedCost,
	}
	if event.EventType != "INFERENCE_RECONCILED" || event.OrganizationID != row.organizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != row.executionID || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != row.taskID || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != row.correlationID || event.SchemaVersion != events.SchemaVersion || decodeExactJSONBytes(event.Payload, &payload) != nil || !reflect.DeepEqual(payload, expected) {
		return fmt.Errorf("inference reconciliation event is invalid")
	}
	return nil
}
