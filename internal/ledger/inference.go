package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

const (
	inferenceStateReserved  = "RESERVED"
	inferenceStateCompleted = "COMPLETED"
	inferenceStateUncertain = "UNCERTAIN"
	inferenceStateViolation = "VIOLATION"
)

// RecoverInferenceReservations converts calls left active by a stopped runtime
// into conservative uncertainty. The full reservation remains charged, but it
// no longer consumes a live concurrency slot or invites a blind retry.
func (l *SQLite) RecoverInferenceReservations(ctx context.Context, organizationID string) (int, error) {
	if organizationID == "" {
		return 0, fmt.Errorf("inference recovery organization is required")
	}
	recovered := 0
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		now := l.nowUTC()
		rows, err := tx.QueryContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd FROM inference_reservations WHERE organization_id=? AND state=? ORDER BY created_at,reservation_id`, organizationID, inferenceStateReserved)
		if err != nil {
			return fmt.Errorf("read incomplete inference reservations: %w", err)
		}
		defer func() { _ = rows.Close() }()
		type recoveryRow struct {
			reservationID, requestID, organizationID, purpose, intentID, taskID, executionID, correlationID string
			promptSHA256, provider, model, profile, policyFingerprint                                       string
			reservedInput, reservedOutput, reservedCost, chargedInput, chargedOutput, chargedCost           int64
		}
		var pending []recoveryRow
		for rows.Next() {
			var item recoveryRow
			if err := rows.Scan(&item.reservationID, &item.requestID, &item.organizationID, &item.purpose, &item.intentID, &item.taskID, &item.executionID, &item.correlationID, &item.promptSHA256, &item.provider, &item.model, &item.profile, &item.policyFingerprint, &item.reservedInput, &item.reservedOutput, &item.reservedCost, &item.chargedInput, &item.chargedOutput, &item.chargedCost); err != nil {
				return fmt.Errorf("scan incomplete inference reservation: %w", err)
			}
			scope := inference.Scope{
				OrganizationID: item.organizationID,
				Purpose:        inference.Purpose(item.purpose),
				RequestID:      item.requestID,
				IntentID:       item.intentID,
				TaskID:         item.taskID,
				ExecutionID:    item.executionID,
				CorrelationID:  item.correlationID,
			}
			if item.reservationID == "" || item.organizationID != organizationID || scope.Validate() != nil || item.provider == "" || item.model == "" || item.profile == "" || item.policyFingerprint == "" || !validSHA256Hex(item.promptSHA256) || item.reservedInput < 1 || item.reservedOutput < 1 || item.reservedCost < 0 || item.chargedInput != item.reservedInput || item.chargedOutput != item.reservedOutput || item.chargedCost != item.reservedCost {
				return fmt.Errorf("incomplete inference reservation is malformed")
			}
			pending = append(pending, item)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate incomplete inference reservations: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close incomplete inference reservations: %w", err)
		}
		for _, item := range pending {
			result, err := tx.ExecContext(ctx, `UPDATE inference_reservations SET state=?,updated_at=? WHERE reservation_id=? AND state=?`, inferenceStateUncertain, now.Format(time.RFC3339Nano), item.reservationID, inferenceStateReserved)
			if err != nil {
				return fmt.Errorf("recover inference reservation: %w", err)
			}
			changed, err := result.RowsAffected()
			if err != nil || changed != 1 {
				return fmt.Errorf("inference reservation changed during recovery")
			}
			payload := events.InferenceReconciledPayload{
				ReservationID: item.reservationID, State: inferenceStateUncertain,
				ChargedInputTokens: item.chargedInput, ChargedOutputTokens: item.chargedOutput,
				ChargedCostNanoUSD: item.chargedCost,
			}
			if _, err := appendEvent(ctx, tx, events.TrustedDraft{
				OrganizationID: item.organizationID, EventType: "INFERENCE_RECONCILED", SourceActorID: "runtime",
				SourceExecutionID: item.executionID, TaskID: item.taskID, Payload: payload,
				CorrelationID: item.correlationID,
			}); err != nil {
				return fmt.Errorf("append inference recovery: %w", err)
			}
			recovered++
		}
		return nil
	})
	return recovered, err
}

func (l *SQLite) ActivateInferencePolicy(ctx context.Context, policy inference.Policy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	fingerprint, err := policy.Fingerprint()
	if err != nil {
		return err
	}
	body, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("encode inference policy: %w", err)
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		now := l.nowUTC()
		if now.Before(policy.AuthorizedAt) || !now.Before(policy.AuthorizationExpiresAt) || policy.Pricing != nil && !now.Before(policy.Pricing.ExpiresAt) {
			return fmt.Errorf("inference policy or pricing is not currently valid")
		}
		var existingFingerprint string
		var existingBody []byte
		err := tx.QueryRowContext(ctx, `SELECT policy_fingerprint,body FROM inference_policies WHERE organization_id=? AND active=1`, policy.OrganizationID).Scan(&existingFingerprint, &existingBody)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read active inference policy: %w", err)
		}
		if err == nil {
			var existing inference.Policy
			if decodeExactJSONBytes(existingBody, &existing) != nil {
				return fmt.Errorf("active inference policy is malformed")
			}
			existingCalculated, fingerprintErr := existing.Fingerprint()
			if fingerprintErr != nil || existingCalculated != existingFingerprint {
				return fmt.Errorf("active inference policy fingerprint is invalid")
			}
			if existingFingerprint == fingerprint {
				if !reflect.DeepEqual(existing, policy) {
					return fmt.Errorf("active inference policy conflicts with its fingerprint")
				}
				return nil
			}
			if !policy.AuthorizedAt.After(existing.AuthorizedAt) {
				return fmt.Errorf("replacement inference policy is not newer than the active policy")
			}
			var active int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inference_reservations WHERE organization_id=? AND state=?`, policy.OrganizationID, inferenceStateReserved).Scan(&active); err != nil {
				return fmt.Errorf("inspect active inference reservations: %w", err)
			}
			if active != 0 {
				return fmt.Errorf("cannot replace an inference policy while provider calls are active")
			}
		}
		payload := events.InferencePolicyActivatedPayload{
			PolicyFingerprint: fingerprint, Provider: policy.Provider, Model: policy.Model,
			ExecutionProfileVersion: policy.ExecutionProfileVersion, AccessMode: string(policy.Mode),
			AuthorizedBy: policy.AuthorizedBy, AuthorizedAt: policy.AuthorizedAt,
			AuthorizationExpiresAt: policy.AuthorizationExpiresAt,
		}
		event, err := appendEvent(ctx, tx, events.TrustedDraft{
			OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED",
			SourceActorID: policy.AuthorizedBy, Payload: payload,
			CorrelationID: "inference-policy-" + fingerprint[:16],
		})
		if err != nil {
			return fmt.Errorf("append inference policy activation: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE inference_policies SET active=0 WHERE organization_id=? AND active=1`, policy.OrganizationID); err != nil {
			return fmt.Errorf("retire prior inference policy: %w", err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO inference_policies(organization_id,policy_fingerprint,body,activation_event_id,activated_at,active) VALUES(?,?,?,?,?,1)`,
			policy.OrganizationID, fingerprint, body, event.EventID, now.Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("activate inference policy: %w", err)
		}
		return nil
	})
}

func (l *SQLite) ReserveInference(ctx context.Context, request inference.InferenceRequest) (inference.Reservation, error) {
	if err := request.Scope.Validate(); err != nil {
		return inference.Reservation{}, err
	}
	if request.Descriptor.Provider == "" || request.Descriptor.Model == "" || request.Descriptor.ExecutionProfileVersion == "" || !validSHA256Hex(request.PromptSHA256) {
		return inference.Reservation{}, fmt.Errorf("inference request identity is incomplete")
	}
	var reserved inference.Reservation
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		// Authorization, pricing, and window time are read only after the
		// transaction is acquired so expiry cannot race admission.
		now := l.nowUTC()
		policy, fingerprint, err := activeInferencePolicy(ctx, tx, request.Scope.OrganizationID)
		if err != nil {
			return err
		}
		if policy.Provider != request.Descriptor.Provider || policy.Model != request.Descriptor.Model || policy.ExecutionProfileVersion != request.Descriptor.ExecutionProfileVersion {
			return fmt.Errorf("inference request does not match the active provider policy")
		}
		if now.Before(policy.AuthorizedAt) || !now.Before(policy.AuthorizationExpiresAt) {
			return fmt.Errorf("inference authorization is missing, not yet valid, or expired")
		}
		if policy.Mode == inference.MeteredAPI && (policy.Pricing == nil || !now.Before(policy.Pricing.ExpiresAt)) {
			return fmt.Errorf("metered inference pricing is missing or stale")
		}
		var prior int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inference_reservations WHERE organization_id=? AND request_id=?`, request.Scope.OrganizationID, request.Scope.RequestID).Scan(&prior); err != nil {
			return fmt.Errorf("inspect inference request replay: %w", err)
		}
		if prior != 0 {
			return fmt.Errorf("inference request was already admitted; retries fail closed")
		}
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM inference_reservations WHERE organization_id=? AND state=?`, request.Scope.OrganizationID, inferenceStateReserved).Scan(&active); err != nil {
			return fmt.Errorf("inspect concurrent inference requests: %w", err)
		}
		if active >= policy.MaxConcurrentRequests {
			return fmt.Errorf("inference concurrency limit is exhausted")
		}
		windowStart, windowEnd := inferenceWindow(now, time.Duration(policy.WindowDurationSeconds)*time.Second)
		var chargedTokens, chargedCost int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(charged_input_tokens+charged_output_tokens),0),COALESCE(SUM(charged_cost_nano_usd),0)
FROM inference_reservations WHERE organization_id=? AND provider=? AND model=? AND window_started_at=?`,
			policy.OrganizationID, policy.Provider, policy.Model, windowStart.Format(time.RFC3339Nano)).Scan(&chargedTokens, &chargedCost); err != nil {
			return fmt.Errorf("read inference budget use: %w", err)
		}
		reservedTokens := policy.MaxInputTokensPerRequest + policy.MaxOutputTokensPerRequest
		if chargedTokens > policy.MaxTokensPerWindow-policy.ContinuityReserveTokens-reservedTokens {
			return fmt.Errorf("inference token budget would consume its continuity reserve")
		}
		reservedCost, err := policy.ReservedCostNanoUSD()
		if err != nil {
			return err
		}
		if policy.Mode == inference.MeteredAPI && (reservedCost > policy.Pricing.MaxCostNanoUSDPerRequest || chargedCost > policy.Pricing.MaxCostNanoUSDPerWindow-reservedCost) {
			return fmt.Errorf("inference cost budget is exhausted")
		}
		reservationID, err := inferenceReservationID(request)
		if err != nil {
			return err
		}
		reserved = inference.Reservation{
			ID: reservationID, PolicyFingerprint: fingerprint, Mode: policy.Mode, Request: request,
			ReservedInputTokens: policy.MaxInputTokensPerRequest, ReservedOutputTokens: policy.MaxOutputTokensPerRequest,
			ReservedCostNanoUSD: reservedCost, WindowStartedAt: windowStart, WindowExpiresAt: windowEnd,
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO inference_reservations(
reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,
provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,
reserved_cost_nano_usd,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd,
window_started_at,window_expires_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			reserved.ID, request.Scope.RequestID, request.Scope.OrganizationID, request.Scope.Purpose, request.Scope.IntentID, request.Scope.TaskID,
			request.Scope.ExecutionID, request.Scope.CorrelationID, request.PromptSHA256, request.Descriptor.Provider, request.Descriptor.Model,
			request.Descriptor.ExecutionProfileVersion, fingerprint, inferenceStateReserved, reserved.ReservedInputTokens,
			reserved.ReservedOutputTokens, reserved.ReservedCostNanoUSD, reserved.ReservedInputTokens, reserved.ReservedOutputTokens,
			reserved.ReservedCostNanoUSD, windowStart.Format(time.RFC3339Nano), windowEnd.Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("reserve inference budget: %w", err)
		}
		payload := events.InferenceReservedPayload{
			ReservationID: reserved.ID, RequestID: request.Scope.RequestID, Purpose: string(request.Scope.Purpose),
			IntentID: request.Scope.IntentID, PolicyFingerprint: fingerprint, PromptSHA256: request.PromptSHA256,
			Provider: request.Descriptor.Provider, Model: request.Descriptor.Model,
			ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion,
			ReservedInputTokens:     reserved.ReservedInputTokens, ReservedOutputTokens: reserved.ReservedOutputTokens,
			ReservedCostNanoUSD: reserved.ReservedCostNanoUSD, WindowStartedAt: windowStart, WindowExpiresAt: windowEnd,
		}
		if _, err := appendEvent(ctx, tx, events.TrustedDraft{
			OrganizationID: request.Scope.OrganizationID, EventType: "INFERENCE_RESERVED", SourceActorID: "runtime",
			SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, Payload: payload,
			CorrelationID: request.Scope.CorrelationID,
		}); err != nil {
			return fmt.Errorf("append inference reservation: %w", err)
		}
		return nil
	})
	return reserved, err
}

func (l *SQLite) ReconcileInference(ctx context.Context, reservation inference.Reservation, usage *events.InferenceUsageRecordedPayload, result inference.Reconciliation) (int64, error) {
	if reservation.ID == "" || reservation.PolicyFingerprint == "" || reservation.Request.Scope.Validate() != nil {
		return 0, fmt.Errorf("inference reservation is incomplete")
	}
	if result != inference.ReconciliationCompleted && result != inference.ReconciliationUncertain && result != inference.ReconciliationViolation {
		return 0, fmt.Errorf("inference reconciliation state is invalid")
	}
	chargedInput := reservation.ReservedInputTokens
	chargedOutput := reservation.ReservedOutputTokens
	chargedCost := reservation.ReservedCostNanoUSD
	state := inferenceStateUncertain
	usageMatches := usage != nil && usage.Valid() && usage.Provider == reservation.Request.Descriptor.Provider && usage.Model == reservation.Request.Descriptor.Model
	switch result {
	case inference.ReconciliationCompleted:
		if !usageMatches {
			return 0, fmt.Errorf("inference reconciliation usage is invalid")
		}
		chargedInput = int64(usage.InputTokens)
		chargedOutput = int64(usage.OutputTokens)
		state = inferenceStateCompleted
	case inference.ReconciliationViolation:
		state = inferenceStateViolation
		if usageMatches && int64(usage.InputTokens) > chargedInput {
			chargedInput = int64(usage.InputTokens)
		}
		if usageMatches && int64(usage.OutputTokens) > chargedOutput {
			chargedOutput = int64(usage.OutputTokens)
		}
	case inference.ReconciliationUncertain:
	}
	violated := state == inferenceStateViolation
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		now := l.nowUTC()
		policy, fingerprint, err := activeInferencePolicy(ctx, tx, reservation.Request.Scope.OrganizationID)
		if err != nil {
			return err
		}
		if fingerprint != reservation.PolicyFingerprint || policy.Mode != reservation.Mode {
			return fmt.Errorf("inference reservation policy is no longer authoritative")
		}
		if usageMatches && policy.Mode == inference.MeteredAPI {
			actualCost, err := policy.ActualCostNanoUSD(*usage)
			if err != nil {
				return err
			}
			chargedCost = actualCost
			if state == inferenceStateViolation && chargedCost < reservation.ReservedCostNanoUSD {
				chargedCost = reservation.ReservedCostNanoUSD
			}
		} else if policy.Mode != inference.MeteredAPI {
			chargedCost = 0
		}
		var stored inferenceReservationRow
		if err := scanInferenceReservation(tx.QueryRowContext(ctx, `SELECT reservation_id,request_id,organization_id,purpose,intent_id,task_id,execution_id,correlation_id,prompt_sha256,provider,model,execution_profile_version,policy_fingerprint,state,reserved_input_tokens,reserved_output_tokens,reserved_cost_nano_usd,window_started_at,window_expires_at FROM inference_reservations WHERE reservation_id=?`, reservation.ID), &stored); err != nil {
			return err
		}
		if stored.state != inferenceStateReserved || !stored.matches(reservation) {
			return fmt.Errorf("inference reservation is not the exact active admission")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE inference_reservations SET state=?,charged_input_tokens=?,charged_output_tokens=?,charged_cost_nano_usd=?,updated_at=? WHERE reservation_id=? AND state=?`,
			state, chargedInput, chargedOutput, chargedCost, now.Format(time.RFC3339Nano), reservation.ID, inferenceStateReserved); err != nil {
			return fmt.Errorf("reconcile inference reservation: %w", err)
		}
		payload := events.InferenceReconciledPayload{
			ReservationID: reservation.ID, State: state, ChargedInputTokens: chargedInput,
			ChargedOutputTokens: chargedOutput, ChargedCostNanoUSD: chargedCost,
		}
		if _, err := appendEvent(ctx, tx, events.TrustedDraft{
			OrganizationID: reservation.Request.Scope.OrganizationID, EventType: "INFERENCE_RECONCILED", SourceActorID: "runtime",
			SourceExecutionID: reservation.Request.Scope.ExecutionID, TaskID: reservation.Request.Scope.TaskID,
			Payload: payload, CorrelationID: reservation.Request.Scope.CorrelationID,
		}); err != nil {
			return fmt.Errorf("append inference reconciliation: %w", err)
		}
		return nil
	})
	if err == nil && violated {
		err = fmt.Errorf("provider usage violated its inference reservation")
	}
	return chargedCost, err
}

func activeInferencePolicy(ctx context.Context, tx *sql.Tx, organizationID string) (inference.Policy, string, error) {
	var fingerprint string
	var body []byte
	if err := tx.QueryRowContext(ctx, `SELECT policy_fingerprint,body FROM inference_policies WHERE organization_id=? AND active=1`, organizationID).Scan(&fingerprint, &body); err != nil {
		if err == sql.ErrNoRows {
			return inference.Policy{}, "", fmt.Errorf("organization has no active inference policy")
		}
		return inference.Policy{}, "", fmt.Errorf("read active inference policy: %w", err)
	}
	var policy inference.Policy
	if decodeExactJSONBytes(body, &policy) != nil || policy.Validate() != nil || policy.OrganizationID != organizationID {
		return inference.Policy{}, "", fmt.Errorf("active inference policy is invalid")
	}
	calculated, err := policy.Fingerprint()
	if err != nil || calculated != fingerprint {
		return inference.Policy{}, "", fmt.Errorf("active inference policy fingerprint is invalid")
	}
	return policy, fingerprint, nil
}

func inferenceWindow(now time.Time, duration time.Duration) (time.Time, time.Time) {
	seconds := int64(duration / time.Second)
	start := now.Unix() - now.Unix()%seconds
	windowStart := time.Unix(start, 0).UTC()
	return windowStart, windowStart.Add(duration)
}

func inferenceReservationID(request inference.InferenceRequest) (string, error) {
	body, err := json.Marshal(struct {
		OrganizationID          string `json:"organization_id"`
		Purpose                 string `json:"purpose"`
		RequestID               string `json:"request_id"`
		IntentID                string `json:"intent_id"`
		TaskID                  string `json:"task_id"`
		ExecutionID             string `json:"execution_id"`
		CorrelationID           string `json:"correlation_id"`
		Provider                string `json:"provider"`
		Model                   string `json:"model"`
		ExecutionProfileVersion string `json:"execution_profile_version"`
		PromptSHA256            string `json:"prompt_sha256"`
	}{
		OrganizationID: request.Scope.OrganizationID, Purpose: string(request.Scope.Purpose),
		RequestID: request.Scope.RequestID, IntentID: request.Scope.IntentID, TaskID: request.Scope.TaskID,
		ExecutionID: request.Scope.ExecutionID, CorrelationID: request.Scope.CorrelationID,
		Provider: request.Descriptor.Provider, Model: request.Descriptor.Model,
		ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion, PromptSHA256: request.PromptSHA256,
	})
	if err != nil {
		return "", fmt.Errorf("encode inference reservation identity: %w", err)
	}
	digest := sha256.Sum256(body)
	return "inference-" + hex.EncodeToString(digest[:]), nil
}

func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

type inferenceReservationRow struct {
	reservationID, requestID, organizationID, purpose, intentID, taskID, executionID, correlationID string
	promptSHA256, provider, model, profile, policyFingerprint, state                                string
	reservedInput, reservedOutput, reservedCost                                                     int64
	windowStart, windowEnd                                                                          string
}

func scanInferenceReservation(row *sql.Row, target *inferenceReservationRow) error {
	if err := row.Scan(&target.reservationID, &target.requestID, &target.organizationID, &target.purpose, &target.intentID, &target.taskID, &target.executionID, &target.correlationID, &target.promptSHA256, &target.provider, &target.model, &target.profile, &target.policyFingerprint, &target.state, &target.reservedInput, &target.reservedOutput, &target.reservedCost, &target.windowStart, &target.windowEnd); err != nil {
		return fmt.Errorf("read inference reservation: %w", err)
	}
	return nil
}

func (r inferenceReservationRow) matches(expected inference.Reservation) bool {
	start, startErr := time.Parse(time.RFC3339Nano, r.windowStart)
	end, endErr := time.Parse(time.RFC3339Nano, r.windowEnd)
	request := expected.Request
	return startErr == nil && endErr == nil &&
		r.reservationID == expected.ID && r.requestID == request.Scope.RequestID && r.organizationID == request.Scope.OrganizationID &&
		r.purpose == string(request.Scope.Purpose) && r.intentID == request.Scope.IntentID && r.taskID == request.Scope.TaskID &&
		r.executionID == request.Scope.ExecutionID && r.correlationID == request.Scope.CorrelationID && r.promptSHA256 == request.PromptSHA256 &&
		r.provider == request.Descriptor.Provider && r.model == request.Descriptor.Model && r.profile == request.Descriptor.ExecutionProfileVersion &&
		r.policyFingerprint == expected.PolicyFingerprint && r.reservedInput == expected.ReservedInputTokens &&
		r.reservedOutput == expected.ReservedOutputTokens && r.reservedCost == expected.ReservedCostNanoUSD &&
		start.Equal(expected.WindowStartedAt) && end.Equal(expected.WindowExpiresAt)
}
