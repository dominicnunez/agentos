package intake

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

const (
	ChannelA2A         = "A2A"
	ChannelHumanDirect = "HUMAN_DIRECT"

	CapabilitySubmitWork           = "submit_work"
	CapabilityConfirmIntent        = "confirm_intent"
	CapabilityReadStatus           = "read_status"
	CapabilityReadResult           = "read_result"
	CapabilityProvideInput         = "provide_input"
	CapabilityReviewCompletion     = "review_completion"
	MaximumReviewFeedbackBytes     = 64 << 10
	MaximumIntentTurns             = 32
	MaximumIntentConversationBytes = 128 << 10

	WorkScopeOwn          WorkScope = "OWN"
	WorkScopeOrganization WorkScope = "ORGANIZATION"

	StateWorking              = "WORKING"
	StateAwaitingConfirmation = "AWAITING_CONFIRMATION"
	StateInputRequired        = "INPUT_REQUIRED"
	StateCompleted            = "COMPLETED"
	StateFailed               = "FAILED"
)

type WorkScope string

var (
	ErrInvalid     = errors.New("invalid operator message")
	ErrForbidden   = errors.New("operator capability denied")
	ErrNotFound    = errors.New("operator work not found")
	ErrConflict    = errors.New("operator conversation conflict")
	ErrUnavailable = errors.New("operator work unavailable")
)

type Principal struct {
	ID             string
	Kind           core.PrincipalKind
	OrganizationID string
	Channel        string
	Capabilities   []string
	WorkScope      WorkScope
}

func (p Principal) Allowed(capability string) bool {
	for _, candidate := range p.Capabilities {
		if candidate == capability {
			return true
		}
	}
	return false
}

type Message struct {
	ConversationID string
	TaskID         string
	MessageID      string
	Text           string
	RequestedKind  core.ExecutionKind
}

type View struct {
	TaskID             string
	WorkID             string
	ConversationID     string
	State              string
	Prompt             string
	Result             string
	Mode               core.IntentMode
	TrustLabel         string
	UpdatedAt          time.Time
	CompletionContract *core.CompletionContract
	Intent             *core.IntentDraft
}

type CompletionReviewView struct {
	ReviewID     string                     `json:"review_id"`
	TaskID       string                     `json:"task_id"`
	TaskVersion  int                        `json:"task_version"`
	Fingerprint  string                     `json:"fingerprint"`
	State        string                     `json:"state"`
	ReviewerID   string                     `json:"reviewer_id,omitempty"`
	Feedback     string                     `json:"feedback,omitempty"`
	Objective    string                     `json:"objective"`
	Result       string                     `json:"candidate_result"`
	Criteria     []core.CompletionCriterion `json:"criteria"`
	EvidenceRefs []string                   `json:"evidence_refs"`
	UpdatedAt    time.Time                  `json:"updated_at"`
}

type CompletionReviewList struct {
	Reviews   []CompletionReviewView `json:"reviews"`
	NextAfter string                 `json:"next_after,omitempty"`
}

type CompletionReviewDecision struct {
	TaskID      string
	ReviewID    string
	Fingerprint string
	Decision    core.CompletionReviewDecision
	Feedback    string
}

type Router struct{}

// Route uses registered deterministic handlers first. Unstructured work that
// has no deterministic handler is routed to an AgentExecution because natural-
// language interpretation is intrinsic to that bounded step.
func (Router) Route(message Message) (core.ExecutionKind, error) {
	if message.RequestedKind != "" {
		switch message.RequestedKind {
		case core.ExecutionDeterministic, core.ExecutionAgent, core.ExecutionHuman:
			return message.RequestedKind, nil
		case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed:
			return "", fmt.Errorf("%w: requested execution kind is unavailable", ErrInvalid)
		default:
			return "", fmt.Errorf("%w: requested execution kind is unknown", ErrInvalid)
		}
	}
	if strings.HasPrefix(message.Text, "echo ") {
		return core.ExecutionDeterministic, nil
	}
	return core.ExecutionAgent, nil
}

type Service struct {
	app         *app.Service
	router      Router
	normalizer  Normalizer
	streamLocks [256]sync.Mutex
}

func New(service *app.Service) *Service {
	return NewWithNormalizer(service, literalNormalizer{})
}

func NewWithNormalizer(service *app.Service, normalizer Normalizer) *Service {
	if service == nil || normalizer == nil {
		panic("intake service and normalizer are required")
	}
	return &Service{app: service, normalizer: normalizer}
}

func (s *Service) Handle(ctx context.Context, principal Principal, message Message) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if err := validateMessageContent(message); err != nil {
		return View{}, err
	}
	unlock := s.lockStream(principal.OrganizationID, message.ConversationID)
	defer unlock()
	submissionReceiptOnly := false
	stream, err := s.app.ExternalEvents(ctx, principal.OrganizationID, message.ConversationID)
	if err != nil {
		return View{}, fmt.Errorf("%w: load work stream", ErrUnavailable)
	}
	if !streamHasEvent(stream, "INTENT_CONFIRMED") {
		return s.handleIntentConversation(ctx, principal, message, stream)
	}
	initial, err := authorizedInitialWork(principal, stream)
	if err != nil {
		return View{}, err
	}
	initialIDMatches := initial.MessageID == message.MessageID
	initialReplay := initialIDMatches && initial.Text == message.Text && initial.RequestedKind == message.RequestedKind
	if initialReplay && !principal.Allowed(CapabilityReadStatus) {
		if !principal.Allowed(CapabilitySubmitWork) || !initial.matches(principal) {
			return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityReadStatus)
		}
		submissionReceiptOnly = true
	}
	durableTaskID := streamTaskID(stream)
	if durableTaskID == "" {
		return View{}, fmt.Errorf("%w: work has no durable task", ErrUnavailable)
	}
	if principal.Channel == ChannelA2A {
		if initialReplay {
			if message.TaskID != "" && message.TaskID != durableTaskID {
				return View{}, fmt.Errorf("%w: initial retry task does not match durable work", ErrConflict)
			}
		} else if message.TaskID == "" {
			return View{}, fmt.Errorf("%w: continuation requires task identifier", ErrConflict)
		} else if message.TaskID != durableTaskID {
			return View{}, fmt.Errorf("%w: continuation task does not match durable work", ErrConflict)
		}
	}
	durableInputReplay, err := matchesDurableInput(stream, principal, message)
	if err != nil {
		return View{}, fmt.Errorf("%w: durable operator input is invalid", ErrUnavailable)
	}
	if err := ValidateIdentifier("message", message.MessageID); err != nil && !initialReplay && !durableInputReplay {
		return View{}, err
	}
	if initialIDMatches && !initialReplay {
		return View{}, fmt.Errorf("%w: initial message id is bound to different input", ErrConflict)
	}
	switch externalState(stream) {
	case StateInputRequired:
		if initialReplay && !submissionReceiptOnly {
			if !principal.Allowed(CapabilityReadStatus) {
				return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityReadStatus)
			}
		} else if !initialReplay {
			if !principal.Allowed(CapabilityProvideInput) {
				return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityProvideInput)
			}
			taskID := streamTaskID(stream)
			if taskID == "" {
				return View{}, fmt.Errorf("%w: blocked work has no task", ErrUnavailable)
			}
			if task, found := streamTask(stream); found && task.ExecutionKind == core.ExecutionHuman && task.CompletionContract != nil {
				return View{}, fmt.Errorf("%w: user tasks require structured completion through the user gateway", ErrConflict)
			}
			if err := s.app.ProvideOperatorInput(ctx, app.OperatorInput{
				OrganizationID: principal.OrganizationID, PrincipalID: principal.ID,
				PrincipalKind: principal.Kind, SourceChannel: principal.Channel,
				RequestID: message.ConversationID, TaskID: taskID,
				MessageID: message.MessageID, Text: message.Text,
			}); err != nil {
				return View{}, fmt.Errorf("%w: continue blocked work", ErrConflict)
			}
			stream, err = s.app.ExternalEvents(ctx, principal.OrganizationID, message.ConversationID)
			if err != nil {
				return View{}, fmt.Errorf("%w: reload continued work", ErrUnavailable)
			}
		}
	case StateWorking, StateCompleted, StateFailed:
		if !initialReplay && !durableInputReplay {
			return View{}, fmt.Errorf("%w: conversation is bound to different work", ErrConflict)
		}
		if !principal.Allowed(CapabilityReadStatus) && !submissionReceiptOnly {
			return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityReadStatus)
		}
	default:
		return View{}, fmt.Errorf("%w: work has unknown state", ErrUnavailable)
	}
	if streamTaskID(stream) == "" {
		return View{}, fmt.Errorf("%w: work did not create a task", ErrUnavailable)
	}
	if submissionReceiptOnly {
		return projectSubmissionReceipt(message.ConversationID, stream), nil
	}
	return projectView(message.ConversationID, stream, principal.Allowed(CapabilityReadResult)), nil
}

type IntentConfirmation struct {
	ConversationID string
	TaskID         string
	MessageID      string
	Fingerprint    string
}

func (s *Service) ConfirmIntent(ctx context.Context, principal Principal, confirmation IntentConfirmation) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if !principal.Allowed(CapabilityConfirmIntent) {
		return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityConfirmIntent)
	}
	if ValidateIdentifier("conversation", confirmation.ConversationID) != nil || ValidateIdentifier("message", confirmation.MessageID) != nil || len(confirmation.Fingerprint) != 64 {
		return View{}, fmt.Errorf("%w: valid confirmation identity and fingerprint are required", ErrInvalid)
	}
	unlock := s.lockStream(principal.OrganizationID, confirmation.ConversationID)
	defer unlock()
	stream, err := s.app.ExternalEvents(ctx, principal.OrganizationID, confirmation.ConversationID)
	if err != nil || len(stream) == 0 {
		return View{}, ErrNotFound
	}
	if err := authorizeIntakePrincipal(principal, stream); err != nil {
		return View{}, err
	}
	if confirmation.TaskID != "" && confirmation.TaskID != streamTaskID(stream) {
		return View{}, fmt.Errorf("%w: confirmation task does not match durable intake", ErrConflict)
	}
	payload, found, err := latestIntentPayload(stream)
	if err != nil || !found || payload.Draft.Fingerprint != confirmation.Fingerprint {
		return View{}, fmt.Errorf("%w: confirmation does not match the current intent", ErrConflict)
	}
	draft := payload.Draft
	kind, err := s.router.Route(Message{Text: draft.Objective, RequestedKind: draft.RequestedExecutionKind})
	if err != nil {
		return View{}, err
	}
	if err := app.ValidateReviewedIntentExecution(draft, core.ID(principal.OrganizationID), kind); err != nil {
		return View{}, fmt.Errorf("%w: reviewed intent is not executable: %w", ErrInvalid, err)
	}
	_, err = s.app.ConfirmIntent(ctx, app.IntentConfirmation{
		RequestID: confirmation.ConversationID, OrganizationID: principal.OrganizationID,
		MessageID: confirmation.MessageID, Fingerprint: confirmation.Fingerprint,
		SourcePrincipalID: core.ID(principal.ID), SourcePrincipalKind: principal.Kind,
		SourceChannel: principal.Channel, Kind: kind,
	})
	if err != nil {
		return View{}, fmt.Errorf("%w: confirm intent", ErrConflict)
	}
	stream, err = s.app.ExternalEvents(ctx, principal.OrganizationID, confirmation.ConversationID)
	if err != nil {
		return View{}, fmt.Errorf("%w: reload confirmed work", ErrUnavailable)
	}
	if !principal.Allowed(CapabilityReadStatus) {
		return projectSubmissionReceipt(confirmation.ConversationID, stream), nil
	}
	return projectView(confirmation.ConversationID, stream, principal.Allowed(CapabilityReadResult)), nil
}

func (s *Service) ActiveIntent(ctx context.Context, principal Principal) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if !principal.Allowed(CapabilityProvideInput) {
		return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityProvideInput)
	}
	conversationID, stream, found, err := s.app.ActiveIntake(ctx, principal.OrganizationID, core.ID(principal.ID), principal.Kind, principal.Channel)
	if err != nil {
		return View{}, fmt.Errorf("%w: load active intent", ErrUnavailable)
	}
	if !found {
		return View{}, ErrNotFound
	}
	return projectCurrentIntentView(conversationID, stream)
}

func (s *Service) handleIntentConversation(ctx context.Context, principal Principal, message Message, stream []events.Event) (View, error) {
	if err := ValidateIdentifier("conversation", message.ConversationID); err != nil {
		return View{}, err
	}
	if err := ValidateIdentifier("message", message.MessageID); err != nil {
		return View{}, err
	}
	if len(stream) == 0 {
		if message.TaskID != "" || !principal.Allowed(CapabilitySubmitWork) {
			return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilitySubmitWork)
		}
	} else {
		if err := authorizeIntakePrincipal(principal, stream); err != nil {
			return View{}, err
		}
		if replay, found, replayErr := replayedIntentView(message.ConversationID, principal, message, stream); replayErr != nil {
			return View{}, replayErr
		} else if found {
			return replay, nil
		}
		if !principal.Allowed(CapabilityProvideInput) {
			return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityProvideInput)
		}
		if principal.Channel == ChannelA2A && message.TaskID != streamTaskID(stream) {
			return View{}, fmt.Errorf("%w: intake continuation requires its durable task identifier", ErrConflict)
		}
	}
	if err := validateIntentConversationCapacity(stream, message); err != nil {
		return View{}, err
	}
	stream, err := s.app.RecordIntakeMessage(ctx, app.IntakeMessage{
		RequestID: message.ConversationID, OrganizationID: principal.OrganizationID,
		MessageID: message.MessageID, Text: message.Text, SourcePrincipalID: core.ID(principal.ID),
		SourcePrincipalKind: principal.Kind, SourceChannel: principal.Channel, RequestedKind: message.RequestedKind,
	})
	if err != nil {
		return View{}, fmt.Errorf("%w: record intake message", ErrConflict)
	}
	turns, err := intakeTurns(stream)
	if err != nil {
		return View{}, fmt.Errorf("%w: load intake conversation", ErrUnavailable)
	}
	version := intentDraftVersion(stream) + 1
	attempt := intentNormalizationAttempt(stream, message.MessageID) + 1
	executionID := fmt.Sprintf("intent-normalization-%s-%s-a%d", stream[0].CorrelationID, message.MessageID, attempt)
	descriptor, usesModel := s.normalizer.Descriptor()
	if usesModel {
		stream, err = s.app.RecordIntentNormalizationContext(ctx, principal.OrganizationID, message.ConversationID, app.IntentNormalizationContext{
			ExecutionID: executionID, SourceMessageID: message.MessageID, PromptVersion: descriptor.PromptVersion,
			Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion,
		})
		if err != nil {
			return View{}, fmt.Errorf("%w: manifest intent normalization context", ErrUnavailable)
		}
	}
	normalizationCtx := ctx
	if usesModel {
		normalizationCtx, err = inference.WithScope(ctx, inference.Scope{
			OrganizationID: principal.OrganizationID, Purpose: inference.PurposeIntentNormalization,
			RequestID: executionID, IntentID: "intent-" + stream[0].CorrelationID,
			TaskID: "task-" + stream[0].CorrelationID, ExecutionID: executionID,
			CorrelationID: stream[0].CorrelationID,
		})
		if err != nil {
			return View{}, fmt.Errorf("%w: bind intent normalization inference scope", ErrUnavailable)
		}
	}
	normalized, err := s.normalizer.Normalize(normalizationCtx, turns)
	if normalized.Usage != nil {
		_, usageErr := s.app.RecordIntentNormalizationUsage(ctx, principal.OrganizationID, message.ConversationID, executionID, *normalized.Usage)
		if usageErr != nil {
			return View{}, fmt.Errorf("%w: persist intent normalization usage", ErrUnavailable)
		}
	}
	if err != nil {
		return View{}, fmt.Errorf("%w: normalize intent", ErrUnavailable)
	}
	if err := validateNormalization(normalized); err != nil {
		return View{}, fmt.Errorf("%w: validate normalized intent", ErrUnavailable)
	}
	if err := validateNormalizationProvenance(normalized, turns); err != nil {
		return View{}, fmt.Errorf("%w: validate normalized intent provenance", ErrUnavailable)
	}
	if usesModel != (normalized.Usage != nil) {
		return View{}, fmt.Errorf("%w: intent normalizer model usage contract is inconsistent", ErrUnavailable)
	}
	if normalized.Candidate.ReplacesWork != nil {
		goalID, resolveErr := s.app.ResolveReplacementGoal(ctx, principal.OrganizationID, core.ID(normalized.Candidate.ReplacesWork.Value))
		if resolveErr != nil {
			return View{}, fmt.Errorf("%w: resolve replacement Work binding", ErrConflict)
		}
		if normalized.Candidate.Goal == nil && goalID != "" {
			normalized.Candidate.Goal = &core.IntentValue{Value: string(goalID), Origin: "POLICY"}
		} else if normalized.Candidate.Goal != nil && core.ID(normalized.Candidate.Goal.Value) != goalID {
			return View{}, fmt.Errorf("%w: replacement Work Goal conflicts with its durable predecessor", ErrConflict)
		}
	}
	requestedKind, err := explicitRequestedKind(stream)
	if err != nil {
		return View{}, fmt.Errorf("%w: load explicit execution route", ErrUnavailable)
	}
	if requestedKind == "" {
		requestedKind, err = s.router.Route(Message{Text: normalized.Candidate.Objective})
		if err != nil {
			return View{}, err
		}
	}
	status := core.IntentStatusAwaitingInput
	if normalized.State == normalizationReady {
		status = core.IntentStatusReadyForReview
	}
	draft := core.IntentDraft{
		ID: core.ID("intent-" + stream[0].CorrelationID), OrganizationID: core.ID(principal.OrganizationID),
		Version: version, Status: status, Mode: normalized.Candidate.Mode, RequestedExecutionKind: requestedKind,
		Goal:         cloneIntentValue(normalized.Candidate.Goal),
		ReplacesWork: cloneIntentValue(normalized.Candidate.ReplacesWork),
		Objective:    normalized.Candidate.Objective, Context: normalized.Candidate.Context,
		Deliverables: normalized.Candidate.Deliverables, CompletionCriteria: normalized.Candidate.CompletionCriteria,
		Constraints: normalized.Candidate.Constraints, ResolvedDecisions: normalized.Candidate.ResolvedDecisions,
		ConsequenceCandidates: normalized.Candidate.ConsequenceCandidates, MissingUserInputs: normalized.Candidate.MissingUserInputs,
		CreatedAt: time.Now().UTC(),
	}
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		return View{}, fmt.Errorf("%w: fingerprint intent", ErrUnavailable)
	}
	stream, err = s.app.RecordIntentDraft(ctx, principal.OrganizationID, message.ConversationID, message.MessageID, draft, normalized.Reply)
	if err != nil {
		return View{}, fmt.Errorf("%w: persist intent draft", ErrUnavailable)
	}
	return projectIntentView(message.ConversationID, stream, draft, normalized.Reply), nil
}

func cloneIntentValue(value *core.IntentValue) *core.IntentValue {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func explicitRequestedKind(stream []events.Event) (core.ExecutionKind, error) {
	var requested core.ExecutionKind
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var message events.IntakeMessageRecordedPayload
		if err := json.Unmarshal(event.Payload, &message); err != nil {
			return "", err
		}
		if message.RequestedExecutionKind != "" {
			requested = message.RequestedExecutionKind
		}
	}
	return requested, nil
}

func (s *Service) CompleteHumanTask(ctx context.Context, principal Principal, taskID string, submission core.HumanTaskSubmission) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if principal.Kind != core.PrincipalHuman || principal.Channel != ChannelHumanDirect || !principal.Allowed(CapabilityProvideInput) {
		return View{}, fmt.Errorf("%w: structured user completion requires local user access", ErrForbidden)
	}
	if err := ValidateIdentifier("task", taskID); err != nil {
		return View{}, err
	}
	if err := ValidateIdentifier("message", submission.MessageID); err != nil {
		return View{}, err
	}
	conversationID, stream, err := s.app.ExternalTaskEvents(ctx, principal.OrganizationID, taskID)
	if err != nil || len(stream) == 0 {
		return View{}, ErrNotFound
	}
	if _, err := authorizedInitialWork(principal, stream); err != nil {
		return View{}, err
	}
	if err := s.app.ProvideHumanCompletion(ctx, app.HumanCompletionInput{
		OrganizationID: principal.OrganizationID, PrincipalID: principal.ID, SourceChannel: principal.Channel,
		RequestID: conversationID, TaskID: taskID, Submission: submission,
	}); err != nil {
		return View{}, fmt.Errorf("%w: %w", ErrConflict, err)
	}
	stream, err = s.app.ExternalEvents(ctx, principal.OrganizationID, conversationID)
	if err != nil {
		return View{}, fmt.Errorf("%w: reload user task", ErrUnavailable)
	}
	return projectView(conversationID, stream, principal.Allowed(CapabilityReadResult)), nil
}

func (s *Service) Get(ctx context.Context, principal Principal, taskID string) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if !principal.Allowed(CapabilityReadStatus) {
		return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityReadStatus)
	}
	if taskID == "" {
		return View{}, ValidateIdentifier("task", taskID)
	}
	conversationID, stream, err := s.app.ExternalTaskEvents(ctx, principal.OrganizationID, taskID)
	if err != nil {
		return View{}, fmt.Errorf("%w: load work stream", ErrUnavailable)
	}
	if len(stream) == 0 || streamTaskID(stream) != taskID {
		if err := ValidateIdentifier("task", taskID); err != nil {
			return View{}, err
		}
		return View{}, ErrNotFound
	}
	if !streamHasEvent(stream, "INTENT_CONFIRMED") {
		if err := authorizeIntakePrincipal(principal, stream); err != nil {
			return View{}, err
		}
		return projectCurrentIntentView(conversationID, stream)
	}
	if _, err := authorizedInitialWork(principal, stream); err != nil {
		return View{}, err
	}
	return projectView(conversationID, stream, principal.Allowed(CapabilityReadResult)), nil
}

func (s *Service) LatestTask(ctx context.Context, principal Principal) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if !principal.Allowed(CapabilityReadStatus) {
		return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilityReadStatus)
	}
	conversationID, stream, found, err := s.app.LatestConfirmedIntake(ctx, principal.OrganizationID, core.ID(principal.ID), principal.Kind, principal.Channel)
	if err != nil {
		return View{}, fmt.Errorf("%w: load recent work", ErrUnavailable)
	}
	if !found || len(stream) == 0 || !streamHasEvent(stream, "INTENT_CONFIRMED") {
		return View{}, ErrNotFound
	}
	if _, err := authorizedInitialWork(principal, stream); err != nil {
		return View{}, err
	}
	return projectView(conversationID, stream, principal.Allowed(CapabilityReadResult)), nil
}

func (s *Service) GetCompletionReview(ctx context.Context, principal Principal, taskID string) (CompletionReviewView, error) {
	if err := validateCompletionReviewer(principal); err != nil {
		return CompletionReviewView{}, err
	}
	if err := ValidateIdentifier("task", taskID); err != nil {
		return CompletionReviewView{}, err
	}
	view, found, err := s.app.CompletionReview(ctx, principal.OrganizationID, taskID)
	if err != nil {
		return CompletionReviewView{}, fmt.Errorf("%w: load completion review", ErrUnavailable)
	}
	if !found {
		return CompletionReviewView{}, ErrNotFound
	}
	state := "PENDING"
	if view.Decision != "" {
		state = string(view.Decision)
	}
	return projectCompletionReview(view, state), nil
}

func (s *Service) GetCompletionReviewRecord(ctx context.Context, principal Principal, taskID, reviewID string) (CompletionReviewView, error) {
	if err := validateCompletionReviewer(principal); err != nil {
		return CompletionReviewView{}, err
	}
	if err := ValidateIdentifier("task", taskID); err != nil {
		return CompletionReviewView{}, err
	}
	if err := ValidateIdentifier("review", reviewID); err != nil {
		return CompletionReviewView{}, err
	}
	view, found, err := s.app.CompletionReviewRecord(ctx, principal.OrganizationID, taskID, core.ID(reviewID))
	if err != nil {
		return CompletionReviewView{}, fmt.Errorf("%w: load completion review record", ErrUnavailable)
	}
	if !found {
		return CompletionReviewView{}, ErrNotFound
	}
	state := "PENDING"
	if view.Decision != "" {
		state = string(view.Decision)
	}
	return projectCompletionReview(view, state), nil
}

func (s *Service) ListCompletionReviews(ctx context.Context, principal Principal, after string, limit int) (CompletionReviewList, error) {
	if err := validateCompletionReviewer(principal); err != nil {
		return CompletionReviewList{}, err
	}
	if after != "" {
		if err := ValidateIdentifier("review cursor", after); err != nil {
			return CompletionReviewList{}, err
		}
	}
	if limit < 1 || limit > 100 {
		return CompletionReviewList{}, fmt.Errorf("%w: review page limit must be between 1 and 100", ErrInvalid)
	}
	page, err := s.app.PendingCompletionReviews(ctx, principal.OrganizationID, core.ID(after), limit)
	if err != nil {
		return CompletionReviewList{}, fmt.Errorf("%w: list completion reviews", ErrUnavailable)
	}
	result := CompletionReviewList{Reviews: make([]CompletionReviewView, 0, len(page.Reviews)), NextAfter: string(page.NextAfter)}
	for _, view := range page.Reviews {
		result.Reviews = append(result.Reviews, projectCompletionReview(view, "PENDING"))
	}
	return result, nil
}

func (s *Service) DecideCompletionReview(ctx context.Context, principal Principal, decision CompletionReviewDecision) (CompletionReviewView, error) {
	if err := validateCompletionReviewer(principal); err != nil {
		return CompletionReviewView{}, err
	}
	if err := ValidateIdentifier("task", decision.TaskID); err != nil {
		return CompletionReviewView{}, err
	}
	if err := ValidateIdentifier("review", decision.ReviewID); err != nil {
		return CompletionReviewView{}, err
	}
	if len(decision.Fingerprint) != 64 {
		return CompletionReviewView{}, fmt.Errorf("%w: review fingerprint must be 64 lowercase hexadecimal characters", ErrInvalid)
	}
	for _, character := range decision.Fingerprint {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return CompletionReviewView{}, fmt.Errorf("%w: review fingerprint must be 64 lowercase hexadecimal characters", ErrInvalid)
		}
	}
	if !utf8.ValidString(decision.Feedback) || len(decision.Feedback) > MaximumReviewFeedbackBytes {
		return CompletionReviewView{}, fmt.Errorf("%w: review feedback must be valid UTF-8 no larger than 65536 bytes", ErrInvalid)
	}
	switch decision.Decision {
	case core.CompletionReviewApprove, core.CompletionReviewReject:
	case core.CompletionReviewRevise:
		if strings.TrimSpace(decision.Feedback) == "" {
			return CompletionReviewView{}, fmt.Errorf("%w: revision feedback is required", ErrInvalid)
		}
	default:
		return CompletionReviewView{}, fmt.Errorf("%w: review decision is unsupported", ErrInvalid)
	}
	view, err := s.app.ReviewCompletion(ctx, app.CompletionReviewInput{
		OrganizationID: principal.OrganizationID, TaskID: decision.TaskID,
		ReviewID: decision.ReviewID, Fingerprint: decision.Fingerprint,
		Decision: decision.Decision, ReviewerID: principal.ID, ReviewerKind: principal.Kind,
		SourceChannel: principal.Channel, Feedback: decision.Feedback,
	})
	if err != nil {
		return CompletionReviewView{}, fmt.Errorf("%w: decide completion review", ErrConflict)
	}
	return projectCompletionReview(view, string(view.Decision)), nil
}

func validateCompletionReviewer(principal Principal) error {
	if err := validatePrincipal(principal); err != nil {
		return err
	}
	if principal.Kind != core.PrincipalHuman || principal.Channel != ChannelHumanDirect || principal.WorkScope != WorkScopeOrganization || !principal.Allowed(CapabilityReviewCompletion) {
		return fmt.Errorf("%w: %s", ErrForbidden, CapabilityReviewCompletion)
	}
	return nil
}

func projectCompletionReview(view app.CompletionReviewView, state string) CompletionReviewView {
	return CompletionReviewView{
		ReviewID: string(view.Request.ID), TaskID: string(view.Request.TaskID),
		TaskVersion: view.Request.TaskVersion, Fingerprint: view.Request.Fingerprint,
		State: state, ReviewerID: string(view.ReviewerID), Feedback: view.Feedback,
		Objective: view.Request.Objective, Result: view.Result,
		Criteria:     append([]core.CompletionCriterion(nil), view.Request.Contract.Criteria...),
		EvidenceRefs: append([]string(nil), view.Request.EvidenceRefs...), UpdatedAt: view.UpdatedAt,
	}
}

func validatePrincipal(principal Principal) error {
	if ValidateIdentifier("principal", principal.ID) != nil || ValidateIdentifier("organization", principal.OrganizationID) != nil || principal.Channel == "" {
		return fmt.Errorf("%w: authenticated principal, organization, and channel are required", ErrInvalid)
	}
	switch principal.Kind {
	case core.PrincipalHuman:
		if principal.Channel != ChannelHumanDirect || principal.WorkScope != WorkScopeOrganization {
			return fmt.Errorf("%w: local user principal channel mismatch", ErrInvalid)
		}
	case core.PrincipalExternalAgent:
		if principal.Channel != ChannelA2A || (principal.WorkScope != WorkScopeOwn && principal.WorkScope != WorkScopeOrganization) {
			return fmt.Errorf("%w: external-agent principal channel mismatch", ErrInvalid)
		}
	case core.PrincipalRuntime:
		return fmt.Errorf("%w: runtime is not an operator principal", ErrInvalid)
	default:
		return fmt.Errorf("%w: principal kind is unsupported", ErrInvalid)
	}
	return nil
}

func validateMessageContent(message Message) error {
	if message.ConversationID == "" || message.MessageID == "" {
		return fmt.Errorf("%w: conversation and message identifiers are required", ErrInvalid)
	}
	if strings.TrimSpace(message.Text) == "" || len(message.Text) > 64<<10 || !utf8.ValidString(message.Text) {
		return fmt.Errorf("%w: text must be valid UTF-8 between 1 and 65536 bytes", ErrInvalid)
	}
	return nil
}

// ValidateIdentifier applies the canonical operator-boundary identifier rules.
func ValidateIdentifier(name, value string) error {
	if value == "" || len(value) > 256 || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s identifier must be valid UTF-8 between 1 and 256 bytes", ErrInvalid, name)
	}
	for _, character := range value {
		if unicode.IsControl(character) || unicode.IsSpace(character) {
			return fmt.Errorf("%w: %s identifier cannot contain whitespace or control characters", ErrInvalid, name)
		}
	}
	return nil
}

func projectView(conversationID string, stream []events.Event, includeResult bool) View {
	view := View{TaskID: streamTaskID(stream), WorkID: streamWorkID(stream), ConversationID: conversationID, State: externalState(stream)}
	if payload, found, err := latestIntentPayload(stream); err == nil && found {
		view.Mode = payload.Draft.Mode
	}
	for _, event := range stream {
		if event.TaskID == view.TaskID || event.EventType == "WORK_PLANNING_FAILED" {
			view.UpdatedAt = event.CreatedAt
		}
		if projection, present, err := events.AdmittedProjection(event); err == nil && present && projection.Projection.ProjectionKind == "lab_experiment" {
			var experiment core.Experiment
			if json.Unmarshal(projection.Projection.Value, &experiment) == nil && experiment.OrganizationID == core.ID(event.OrganizationID) && experiment.TrustLabel != "" {
				view.Mode = core.IntentModeExperiment
				view.TrustLabel = experiment.TrustLabel
			}
		}
	}
	if view.State == StateInputRequired {
		view.Prompt = blockedStatusText(stream)
		if task, found := streamTask(stream); found && task.CompletionContract != nil {
			contract := *task.CompletionContract
			view.CompletionContract = &contract
		}
	}
	if view.State == StateFailed {
		view.Prompt = "Agent OS could not complete the task."
	}
	if includeResult && view.State == StateCompleted {
		if result, ok := publishedResult(stream); ok {
			view.Result = result.Summary
		}
	}
	return view
}

func streamTask(stream []events.Event) (core.Task, bool) {
	rootID := streamTaskID(stream)
	if rootID == "" {
		return core.Task{}, false
	}
	for index := len(stream) - 1; index >= 0; index-- {
		if stream[index].TaskID != rootID {
			continue
		}
		switch stream[index].EventType {
		case "TASK_CREATED", "TASK_BLOCKED", "TASK_RESUMED", "EXECUTION_STARTED", "TASK_VERIFIED_COMPLETE", "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED":
		default:
			continue
		}
		var projection events.ProjectionEventPayload
		var task core.Task
		if json.Unmarshal(stream[index].Payload, &projection) == nil && json.Unmarshal(projection.Projection.Value, &task) == nil && task.ID != "" {
			return task, true
		}
	}
	return core.Task{}, false
}

func streamWorkID(stream []events.Event) string {
	for index := len(stream) - 1; index >= 0; index-- {
		projection, present, err := events.AdmittedProjection(stream[index])
		if err != nil || !present || projection.Projection.ProjectionKind != "work" {
			continue
		}
		var work core.Work
		if json.Unmarshal(projection.Projection.Value, &work) == nil && work.ID != "" && string(work.ID) == projection.Projection.RecordID {
			return string(work.ID)
		}
	}
	return ""
}

func projectSubmissionReceipt(conversationID string, stream []events.Event) View {
	view := View{TaskID: streamTaskID(stream), WorkID: streamWorkID(stream), ConversationID: conversationID, State: StateWorking}
	for _, event := range stream {
		if event.EventType == "INTENT_CREATED" {
			view.UpdatedAt = event.CreatedAt
			break
		}
	}
	return view
}

func externalState(stream []events.Event) string {
	state := StateWorking
	rootID := streamTaskID(stream)
	for _, event := range stream {
		if event.EventType == "WORK_PLANNING_FAILED" {
			state = StateFailed
			continue
		}
		if rootID == "" || event.TaskID != rootID {
			continue
		}
		switch event.EventType {
		case "TASK_BLOCKED":
			state = StateInputRequired
		case "TASK_RESUMED":
			state = StateWorking
		case "TASK_VERIFIED_COMPLETE":
			state = StateCompleted
		case "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED":
			state = StateFailed
		}
	}
	return state
}

func streamTaskID(stream []events.Event) string {
	for _, event := range stream {
		if event.EventType == "INTAKE_MESSAGE_RECORDED" && event.TaskID != "" {
			return event.TaskID
		}
	}
	// Internal callers do not have an intake event. Select only a root Task
	// projection; child DAG nodes are never an external task identity.
	for _, event := range stream {
		if event.EventType != "TASK_CREATED" {
			continue
		}
		var projection events.ProjectionEventPayload
		var task core.Task
		if json.Unmarshal(event.Payload, &projection) == nil && json.Unmarshal(projection.Projection.Value, &task) == nil && task.ID != "" && task.ParentID == "" {
			return string(task.ID)
		}
	}
	return ""
}

type initialWork struct {
	Message
	PrincipalID   core.ID
	PrincipalKind core.PrincipalKind
	Channel       string
}

func (work initialWork) matches(principal Principal) bool {
	return work.PrincipalID == core.ID(principal.ID) &&
		work.PrincipalKind == principal.Kind &&
		work.Channel == principal.Channel
}

func initialMessage(stream []events.Event) (initialWork, bool) {
	for _, event := range stream {
		if event.EventType != "INTENT_CREATED" {
			continue
		}
		var payload events.ProjectionEventPayload
		var intent core.Intent
		if json.Unmarshal(event.Payload, &payload) == nil && json.Unmarshal(payload.Projection.Value, &intent) == nil {
			if intent.SourceMessageID == "" || intent.OriginalInstruction == "" {
				return initialWork{}, false
			}
			requestedKind, found := initialRequestedKind(stream, intent)
			if intent.SourceChannel != "INTERNAL" && !found {
				return initialWork{}, false
			}
			return initialWork{
				Message:       Message{MessageID: intent.SourceMessageID, Text: intent.OriginalInstruction, RequestedKind: requestedKind},
				PrincipalID:   intent.SourcePrincipalID,
				PrincipalKind: intent.SourcePrincipalKind,
				Channel:       intent.SourceChannel,
			}, true
		}
	}
	return initialWork{}, false
}

func initialRequestedKind(stream []events.Event, intent core.Intent) (core.ExecutionKind, bool) {
	found := false
	requestedKind := core.ExecutionKind("")
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var payload events.IntakeMessageRecordedPayload
		if json.Unmarshal(event.Payload, &payload) != nil || payload.MessageID != intent.SourceMessageID {
			continue
		}
		if found || event.SourceActorID != string(intent.SourcePrincipalID) || payload.Text != intent.OriginalInstruction ||
			payload.SourcePrincipalID != string(intent.SourcePrincipalID) || payload.SourcePrincipalKind != string(intent.SourcePrincipalKind) || payload.SourceChannel != intent.SourceChannel {
			return "", false
		}
		found = true
		requestedKind = payload.RequestedExecutionKind
	}
	return requestedKind, found
}

func principalCanAccess(principal Principal, work initialWork) bool {
	return principal.WorkScope == WorkScopeOrganization || (principal.WorkScope == WorkScopeOwn && work.matches(principal))
}

func authorizedInitialWork(principal Principal, stream []events.Event) (initialWork, error) {
	initial, found := initialMessage(stream)
	if !found {
		return initialWork{}, fmt.Errorf("%w: work has no durable initial message", ErrUnavailable)
	}
	if !principalCanAccess(principal, initial) {
		return initialWork{}, ErrNotFound
	}
	return initial, nil
}

func authorizeIntakePrincipal(principal Principal, stream []events.Event) error {
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var payload events.IntakeMessageRecordedPayload
		if json.Unmarshal(event.Payload, &payload) != nil || payload.SourcePrincipalID == "" {
			return fmt.Errorf("%w: intake has no durable initial message", ErrUnavailable)
		}
		if principal.WorkScope == WorkScopeOwn && (payload.SourcePrincipalID != principal.ID || payload.SourcePrincipalKind != string(principal.Kind) || payload.SourceChannel != principal.Channel) {
			return ErrNotFound
		}
		return nil
	}
	return fmt.Errorf("%w: intake has no durable initial message", ErrUnavailable)
}

func streamHasEvent(stream []events.Event, eventType string) bool {
	for _, event := range stream {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}

func intakeTurns(stream []events.Event) ([]ConversationTurn, error) {
	turns := make([]ConversationTurn, 0)
	totalBytes := 0
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var payload events.IntakeMessageRecordedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.MessageID == "" || payload.Text == "" {
			return nil, fmt.Errorf("invalid durable intake message")
		}
		totalBytes += len(payload.Text)
		turns = append(turns, ConversationTurn{MessageID: payload.MessageID, Text: payload.Text})
	}
	if len(turns) == 0 {
		return nil, fmt.Errorf("intake conversation is empty")
	}
	if len(turns) > MaximumIntentTurns || totalBytes > MaximumIntentConversationBytes {
		return nil, fmt.Errorf("intake conversation exceeds its bounded context")
	}
	return turns, nil
}

func replayedIntentView(conversationID string, principal Principal, message Message, stream []events.Event) (View, bool, error) {
	found := false
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var payload events.IntakeMessageRecordedPayload
		if json.Unmarshal(event.Payload, &payload) != nil {
			return View{}, false, fmt.Errorf("%w: durable intake message is invalid", ErrUnavailable)
		}
		if payload.MessageID != message.MessageID {
			continue
		}
		if payload.SourcePrincipalID != principal.ID || payload.SourcePrincipalKind != string(principal.Kind) || payload.SourceChannel != principal.Channel {
			return View{}, false, fmt.Errorf("%w: intake replay does not match its authenticated source", ErrForbidden)
		}
		if payload.Text != message.Text || payload.RequestedExecutionKind != message.RequestedKind {
			return View{}, false, fmt.Errorf("%w: intake message id is already bound to different input", ErrConflict)
		}
		found = true
	}
	if !found {
		return View{}, false, nil
	}
	payload, present, err := intentPayloadForMessage(stream, message.MessageID)
	if err != nil {
		return View{}, false, fmt.Errorf("%w: replay has invalid durable intent draft", ErrUnavailable)
	}
	if !present {
		return View{}, false, nil
	}
	reply := payload.Reply
	if reply == "" {
		reply = "Review the current proposed intent before work begins."
	}
	return projectIntentView(conversationID, stream, payload.Draft, reply), true, nil
}

func intentDraftVersion(stream []events.Event) int {
	version := 0
	for _, event := range stream {
		if event.EventType == "INTENT_DRAFTED" {
			version++
		}
	}
	return version
}

func intentNormalizationAttempt(stream []events.Event, messageID string) int {
	attempts := 0
	for _, event := range stream {
		if event.EventType != "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" {
			continue
		}
		var payload events.IntentNormalizationContextPayload
		if json.Unmarshal(event.Payload, &payload) == nil && payload.SourceMessageID == messageID {
			attempts++
		}
	}
	return attempts
}

func latestIntentPayload(stream []events.Event) (events.IntentDraftedPayload, bool, error) {
	return events.LatestPayload[events.IntentDraftedPayload](stream, "INTENT_DRAFTED")
}

func intentPayloadForMessage(stream []events.Event, messageID string) (events.IntentDraftedPayload, bool, error) {
	for index := len(stream) - 1; index >= 0; index-- {
		if stream[index].EventType != "INTENT_DRAFTED" {
			continue
		}
		var payload events.IntentDraftedPayload
		if err := json.Unmarshal(stream[index].Payload, &payload); err != nil {
			return events.IntentDraftedPayload{}, false, err
		}
		if payload.SourceMessageID == messageID {
			return payload, true, nil
		}
	}
	return events.IntentDraftedPayload{}, false, nil
}

func validateIntentConversationCapacity(stream []events.Event, next Message) error {
	turns := 0
	totalBytes := 0
	alreadyRecorded := false
	for _, event := range stream {
		if event.EventType != "INTAKE_MESSAGE_RECORDED" {
			continue
		}
		var payload events.IntakeMessageRecordedPayload
		if json.Unmarshal(event.Payload, &payload) != nil {
			return fmt.Errorf("%w: durable intake message is invalid", ErrUnavailable)
		}
		turns++
		totalBytes += len(payload.Text)
		if payload.MessageID == next.MessageID {
			alreadyRecorded = true
		}
	}
	if !alreadyRecorded {
		turns++
		totalBytes += len(next.Text)
	}
	if turns > MaximumIntentTurns || totalBytes > MaximumIntentConversationBytes {
		return fmt.Errorf("%w: intent conversation exceeds %d turns or %d bytes", ErrInvalid, MaximumIntentTurns, MaximumIntentConversationBytes)
	}
	return nil
}

func (s *Service) lockStream(organizationID, conversationID string) func() {
	digest := sha256.Sum256([]byte(organizationID + "\x00" + conversationID))
	lock := &s.streamLocks[digest[0]]
	lock.Lock()
	return lock.Unlock
}

func projectCurrentIntentView(conversationID string, stream []events.Event) (View, error) {
	payload, found, err := latestIntentPayload(stream)
	if err != nil || !found {
		return View{}, fmt.Errorf("%w: intake has no durable draft", ErrUnavailable)
	}
	reply := payload.Reply
	if reply == "" {
		reply = "Continue the intake conversation."
	}
	return projectIntentView(conversationID, stream, payload.Draft, reply), nil
}

func projectIntentView(conversationID string, stream []events.Event, draft core.IntentDraft, reply string) View {
	state := StateInputRequired
	if draft.Status == core.IntentStatusReadyForReview {
		state = StateAwaitingConfirmation
	}
	copy := draft
	return View{TaskID: streamTaskID(stream), WorkID: streamWorkID(stream), ConversationID: conversationID, State: state, Prompt: reply, UpdatedAt: stream[len(stream)-1].CreatedAt, Intent: &copy}
}

func matchesDurableInput(stream []events.Event, principal Principal, message Message) (bool, error) {
	for _, event := range stream {
		if event.EventType != "A2A_INPUT_RECEIVED" && event.EventType != "HUMAN_INPUT_RECEIVED" {
			continue
		}
		input, err := events.DecodeDurableOperatorInput(event)
		if err != nil {
			return false, err
		}
		if input.MessageID == message.MessageID {
			return input.Text == message.Text && input.SourcePrincipalID == principal.ID && input.SourcePrincipalKind == string(principal.Kind) && input.SourceChannel == principal.Channel, nil
		}
	}
	return false, nil
}

func blockedStatusText(stream []events.Event) string {
	rootID := streamTaskID(stream)
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].EventType != "TASK_BLOCKED" || stream[i].TaskID != rootID {
			continue
		}
		var projection events.ProjectionEventPayload
		var blocked events.TaskBlockedPayload
		if json.Unmarshal(stream[i].Payload, &projection) == nil && json.Unmarshal(projection.Detail, &blocked) == nil && blocked.Missing != "" {
			return blocked.Reason + " Missing: " + blocked.Missing + " Why needed: " + blocked.WhyNeeded
		}
	}
	return "Agent OS requires additional input to continue this task."
}

func publishedResult(stream []events.Event) (events.ResultPublishedPayload, bool) {
	rootID := streamTaskID(stream)
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].EventType != "RESULT_PUBLISHED" || stream[i].TaskID != rootID {
			continue
		}
		var result events.ResultPublishedPayload
		if json.Unmarshal(stream[i].Payload, &result) == nil && result.ValidFor(stream[i].ArtifactRefs) {
			return result, true
		}
	}
	return events.ResultPublishedPayload{}, false
}
