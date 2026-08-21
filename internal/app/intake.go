package app

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/lab"
	"github.com/dominicnunez/agentos/internal/planning"
)

type IntakeMessage struct {
	RequestID           string
	OrganizationID      string
	MessageID           string
	Text                string
	SourcePrincipalID   core.ID
	SourcePrincipalKind core.PrincipalKind
	SourceChannel       string
	RequestedKind       core.ExecutionKind
}

type IntentConfirmation struct {
	RequestID           string
	OrganizationID      string
	MessageID           string
	Fingerprint         string
	SourcePrincipalID   core.ID
	SourcePrincipalKind core.PrincipalKind
	SourceChannel       string
	Kind                core.ExecutionKind
}

type IntentNormalizationContext struct {
	ExecutionID             string
	SourceMessageID         string
	PromptVersion           string
	Provider                string
	Model                   string
	ExecutionProfileVersion string
}

func (s *Service) ActiveIntake(ctx context.Context, organizationID string, principalID core.ID, principalKind core.PrincipalKind, sourceChannel string) (string, []events.Event, bool, error) {
	if ctx == nil || organizationID == "" || principalID == "" || principalKind == "" || sourceChannel == "" {
		return "", nil, false, fmt.Errorf("organization and complete principal identity are required")
	}
	requestID, correlationID, found, err := s.gateway.ResolveActiveIntake(ctx, organizationID, string(principalID), string(principalKind), sourceChannel)
	if err != nil {
		return "", nil, false, err
	}
	if !found {
		return "", nil, false, nil
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return "", nil, false, err
	}
	return requestID, stream, true, nil
}

func (s *Service) RecordIntakeMessage(ctx context.Context, in IntakeMessage) ([]events.Event, error) {
	if ctx == nil || in.RequestID == "" || in.OrganizationID == "" || in.MessageID == "" || in.Text == "" || in.SourcePrincipalID == "" || in.SourcePrincipalKind == "" || in.SourceChannel == "" {
		return nil, fmt.Errorf("complete intake message identity and content are required")
	}
	correlationID, err := s.gateway.ReserveExternalWork(ctx, in.OrganizationID, in.RequestID)
	if err != nil {
		return nil, err
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return nil, err
	}
	payload := events.IntakeMessageRecordedPayload{MessageID: in.MessageID, Text: in.Text, SourcePrincipalID: string(in.SourcePrincipalID), SourcePrincipalKind: string(in.SourcePrincipalKind), SourceChannel: in.SourceChannel, RequestedExecutionKind: in.RequestedKind}
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" {
			return nil, fmt.Errorf("confirmed intent cannot accept more intake messages")
		}
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var recorded events.IntakeMessageRecordedPayload
		if json.Unmarshal(event.Payload, &recorded) != nil {
			return nil, fmt.Errorf("durable intake message is invalid")
		}
		if recorded.MessageID != in.MessageID {
			continue
		}
		if recorded != payload || event.SourceActorID != string(in.SourcePrincipalID) {
			return nil, fmt.Errorf("intake message id is bound to different content")
		}
		return stream, nil
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: in.OrganizationID, EventType: "INTAKE_MESSAGE_RECORDED",
		SourceActorID: string(in.SourcePrincipalID), TaskID: "task-" + correlationID,
		CorrelationID: correlationID, Payload: payload,
	}); err != nil {
		return nil, fmt.Errorf("persist intake message: %w", err)
	}
	return s.gateway.Events(ctx, correlationID)
}

func (s *Service) RecordIntentNormalizationContext(ctx context.Context, organizationID, requestID string, in IntentNormalizationContext) ([]events.Event, error) {
	if in.ExecutionID == "" || in.SourceMessageID == "" || in.PromptVersion == "" || in.Provider == "" || in.Model == "" || in.ExecutionProfileVersion == "" {
		return nil, fmt.Errorf("complete intent normalization context is required")
	}
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, organizationID, requestID)
	if err != nil || !found {
		return nil, fmt.Errorf("resolve intake work")
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return nil, err
	}
	latest, found, err := latestIntakeMessage(stream)
	if err != nil || !found || latest.MessageID != in.SourceMessageID {
		return nil, fmt.Errorf("normalization must reference the latest durable intake message")
	}
	refs := make([]string, 0)
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" {
			return nil, fmt.Errorf("confirmed intent cannot be normalized")
		}
		if event.EventType == "INTAKE_MESSAGE_RECORDED" {
			refs = append(refs, event.EventID)
		}
	}
	payload := events.IntentNormalizationContextPayload{
		SourceMessageID: in.SourceMessageID, PromptVersion: in.PromptVersion,
		Provider: in.Provider, Model: in.Model, ExecutionProfileVersion: in.ExecutionProfileVersion,
		InputEventRefs: refs,
	}
	for _, event := range stream {
		if event.EventType != "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" || event.SourceExecutionID != in.ExecutionID {
			continue
		}
		var recorded events.IntentNormalizationContextPayload
		if json.Unmarshal(event.Payload, &recorded) != nil || !sameNormalizationContext(recorded, payload) {
			return nil, fmt.Errorf("normalization execution id is bound to different context")
		}
		return stream, nil
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTENT_NORMALIZATION_CONTEXT_MANIFESTED",
		SourceActorID: "runtime", SourceExecutionID: in.ExecutionID, TaskID: "task-" + correlationID,
		CorrelationID: correlationID, Payload: payload,
	}); err != nil {
		return nil, fmt.Errorf("persist intent normalization context: %w", err)
	}
	return s.gateway.Events(ctx, correlationID)
}

func (s *Service) RecordIntentNormalizationUsage(ctx context.Context, organizationID, requestID, executionID string, usage events.InferenceUsageRecordedPayload) ([]events.Event, error) {
	if executionID == "" || !usage.Valid() {
		return nil, fmt.Errorf("valid intent normalization usage is required")
	}
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, organizationID, requestID)
	if err != nil || !found {
		return nil, fmt.Errorf("resolve intake work")
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return nil, err
	}
	manifested := false
	for _, event := range stream {
		if event.SourceExecutionID != executionID {
			continue
		}
		switch event.EventType {
		case "INTENT_NORMALIZATION_CONTEXT_MANIFESTED":
			var context events.IntentNormalizationContextPayload
			if json.Unmarshal(event.Payload, &context) != nil || context.Provider != usage.Provider || context.Model != usage.Model {
				return nil, fmt.Errorf("intent normalization usage identity does not match context")
			}
			manifested = true
		case "INFERENCE_USAGE_RECORDED":
			if !manifested {
				return nil, fmt.Errorf("intent normalization usage precedes its context")
			}
			var recorded events.InferenceUsageRecordedPayload
			if json.Unmarshal(event.Payload, &recorded) != nil || !sameInferenceUsage(recorded, usage) {
				return nil, fmt.Errorf("normalization execution id is bound to different usage")
			}
			return stream, nil
		}
	}
	if !manifested {
		return nil, fmt.Errorf("intent normalization context must be manifested before usage")
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INFERENCE_USAGE_RECORDED",
		SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: "task-" + correlationID,
		CorrelationID: correlationID, Payload: usage,
	}); err != nil {
		return nil, fmt.Errorf("persist intent normalization usage: %w", err)
	}
	return s.gateway.Events(ctx, correlationID)
}

func (s *Service) RecordIntentDraft(ctx context.Context, organizationID, requestID, sourceMessageID string, draft core.IntentDraft, reply string) ([]events.Event, error) {
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, organizationID, requestID)
	if err != nil || !found {
		return nil, fmt.Errorf("resolve intake work")
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return nil, err
	}
	latest, found, err := latestIntakeMessage(stream)
	if err != nil || !found || latest.MessageID != sourceMessageID {
		return nil, fmt.Errorf("intent draft must reference the latest durable intake message")
	}
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" {
			return nil, fmt.Errorf("confirmed intent cannot be revised")
		}
		if event.EventType == "INTENT_DRAFTED" {
			var recorded events.IntentDraftedPayload
			if json.Unmarshal(event.Payload, &recorded) != nil {
				return nil, fmt.Errorf("durable intent draft is invalid")
			}
			if recorded.SourceMessageID == sourceMessageID {
				if recorded.Draft.Fingerprint != draft.Fingerprint || recorded.Reply != reply {
					return nil, fmt.Errorf("intake message is bound to a different intent draft")
				}
				return stream, nil
			}
		}
	}
	payload := events.IntentDraftedPayload{SourceMessageID: sourceMessageID, Draft: draft, Reply: reply}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTENT_DRAFTED", SourceActorID: "runtime",
		TaskID: "task-" + correlationID, CorrelationID: correlationID, Payload: payload,
	}); err != nil {
		return nil, fmt.Errorf("persist intent draft: %w", err)
	}
	return s.gateway.Events(ctx, correlationID)
}

func (s *Service) ConfirmIntent(ctx context.Context, in IntentConfirmation) (Result, error) {
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, in.OrganizationID, in.RequestID)
	if err != nil || !found {
		return Result{}, fmt.Errorf("resolve intake work")
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return Result{}, err
	}
	draft, found, err := latestIntentDraft(stream)
	if err != nil || !found || draft.Status != core.IntentStatusReadyForReview || len(draft.MissingUserInputs) != 0 || draft.Objective == "" || len(draft.Deliverables) == 0 || len(draft.CompletionCriteria) == 0 {
		return Result{}, fmt.Errorf("reviewable complete intent is required")
	}
	if draft.Fingerprint == "" || draft.Fingerprint != in.Fingerprint {
		return Result{}, fmt.Errorf("intent fingerprint does not match current draft")
	}
	recomputed, err := core.FingerprintIntentDraft(draft)
	if err != nil || recomputed != draft.Fingerprint {
		return Result{}, fmt.Errorf("durable intent fingerprint is invalid")
	}
	if err := ValidateReviewedIntentExecution(draft, core.ID(in.OrganizationID), in.Kind); err != nil {
		return Result{}, fmt.Errorf("reviewed intent is not executable: %w", err)
	}
	goalID, err := acceptedGoalID(draft)
	if err != nil {
		return Result{}, fmt.Errorf("accepted Intent Goal is invalid: %w", err)
	}
	original, found, err := initialIntakeMessage(stream)
	if err != nil || !found {
		return Result{}, fmt.Errorf("durable initial intake message is required")
	}
	for _, event := range stream {
		if event.EventType != "INTENT_CONFIRMED" {
			continue
		}
		var recorded events.IntentConfirmedPayload
		if json.Unmarshal(event.Payload, &recorded) != nil || recorded.IntentID != string(draft.ID) || recorded.GoalID != string(goalID) || recorded.Version != draft.Version || recorded.Fingerprint != in.Fingerprint || recorded.MessageID != in.MessageID ||
			recorded.ConfirmingActorID != string(in.SourcePrincipalID) || recorded.ConfirmingActorKind != string(in.SourcePrincipalKind) || recorded.SourceChannel != in.SourceChannel {
			return Result{}, fmt.Errorf("intent confirmation conflicts with durable state")
		}
		return s.submitConfirmedIntent(ctx, submitFromIntent(in, draft, original, correlationID), draft.Mode)
	}
	payload := events.IntentConfirmedPayload{IntentID: string(draft.ID), GoalID: string(goalID), Version: draft.Version, Fingerprint: draft.Fingerprint, ConfirmingActorID: string(in.SourcePrincipalID), ConfirmingActorKind: string(in.SourcePrincipalKind), SourceChannel: in.SourceChannel, MessageID: in.MessageID}
	confirmation := events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: "INTENT_CONFIRMED", SourceActorID: string(in.SourcePrincipalID), TaskID: "task-" + correlationID, CorrelationID: correlationID, Payload: payload}
	_, err = s.gateway.PublishIntentConfirmation(ctx, confirmation, goalID)
	if err != nil {
		return Result{}, fmt.Errorf("persist intent confirmation: %w", err)
	}
	return s.submitConfirmedIntent(ctx, submitFromIntent(in, draft, original, correlationID), draft.Mode)
}

// ValidateReviewedIntentExecution applies runtime routability checks before a
// reviewed Intent can cross its durable confirmation boundary.
func ValidateReviewedIntentExecution(draft core.IntentDraft, organizationID core.ID, kind core.ExecutionKind) error {
	if err := core.ValidateAcceptedIntentDraft(draft, organizationID, kind); err != nil {
		return err
	}
	if draft.Mode == core.IntentModeExperiment {
		if kind != core.ExecutionDeterministic || draft.RequestedExecutionKind != core.ExecutionDeterministic {
			return fmt.Errorf("V1 experimental intent requires deterministic execution")
		}
		if err := planning.ValidateDeterministicObjective(draft.Objective); err != nil {
			return fmt.Errorf("reviewed experiment is not executable: %w", err)
		}
	}
	return nil
}

func (s *Service) submitConfirmedIntent(ctx context.Context, submit Submit, mode core.IntentMode) (Result, error) {
	switch mode {
	case core.IntentModeStandard:
		return s.Submit(ctx, submit)
	case core.IntentModeExperiment:
		return s.SubmitExperiment(ctx, submit, lab.DefaultSpec())
	default:
		return Result{}, fmt.Errorf("confirmed intent mode is unsupported")
	}
}

func submitFromIntent(in IntentConfirmation, draft core.IntentDraft, original events.IntakeMessageRecordedPayload, correlationID string) Submit {
	var goalID core.ID
	if draft.Goal != nil {
		goalID = core.ID(draft.Goal.Value)
	}
	return Submit{RequestID: in.RequestID, OrganizationID: in.OrganizationID, GoalID: goalID, Statement: original.Text, Kind: in.Kind, MessageID: original.MessageID, SourcePrincipalID: core.ID(original.SourcePrincipalID), SourcePrincipalKind: core.PrincipalKind(original.SourcePrincipalKind), SourceChannel: original.SourceChannel, correlationID: correlationID, NormalizedIntent: &draft}
}

func initialIntakeMessage(stream []events.Event) (events.IntakeMessageRecordedPayload, bool, error) {
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var payload events.IntakeMessageRecordedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return events.IntakeMessageRecordedPayload{}, false, err
		}
		return payload, true, nil
	}
	return events.IntakeMessageRecordedPayload{}, false, nil
}

func latestIntentDraft(stream []events.Event) (core.IntentDraft, bool, error) {
	payload, found, err := events.LatestPayload[events.IntentDraftedPayload](stream, "INTENT_DRAFTED")
	return payload.Draft, found, err
}

func latestIntakeMessage(stream []events.Event) (events.IntakeMessageRecordedPayload, bool, error) {
	return events.LatestPayload[events.IntakeMessageRecordedPayload](stream, "INTAKE_MESSAGE_RECORDED")
}

func sameNormalizationContext(left, right events.IntentNormalizationContextPayload) bool {
	return left.SourceMessageID == right.SourceMessageID && left.PromptVersion == right.PromptVersion &&
		left.Provider == right.Provider && left.Model == right.Model &&
		left.ExecutionProfileVersion == right.ExecutionProfileVersion && slices.Equal(left.InputEventRefs, right.InputEventRefs)
}

func sameInferenceUsage(left, right events.InferenceUsageRecordedPayload) bool {
	if left.Source != right.Source || left.Provider != right.Provider || left.Model != right.Model ||
		left.InputTokens != right.InputTokens || left.OutputTokens != right.OutputTokens || left.TotalTokens != right.TotalTokens {
		return false
	}
	if left.CostUSD == nil || right.CostUSD == nil {
		return left.CostUSD == nil && right.CostUSD == nil
	}
	return *left.CostUSD == *right.CostUSD
}
