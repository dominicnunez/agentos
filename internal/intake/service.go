package intake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	ChannelA2A         = "A2A"
	ChannelHumanDirect = "HUMAN_DIRECT"

	CapabilitySubmitWork       = "submit_work"
	CapabilityReadStatus       = "read_status"
	CapabilityReadResult       = "read_result"
	CapabilityProvideInput     = "provide_input"
	CapabilityReviewCompletion = "review_completion"
	MaximumReviewFeedbackBytes = 64 << 10

	WorkScopeOwn          WorkScope = "OWN"
	WorkScopeOrganization WorkScope = "ORGANIZATION"

	StateWorking       = "WORKING"
	StateInputRequired = "INPUT_REQUIRED"
	StateCompleted     = "COMPLETED"
	StateFailed        = "FAILED"
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
	ConversationID     string
	State              string
	Prompt             string
	Result             string
	UpdatedAt          time.Time
	CompletionContract *core.CompletionContract
}

type CompletionReviewView struct {
	ReviewID     string
	TaskID       string
	TaskVersion  int
	Fingerprint  string
	State        string
	Objective    string
	Result       string
	Criteria     []core.CompletionCriterion
	EvidenceRefs []string
	UpdatedAt    time.Time
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
	app    *app.Service
	router Router
}

func New(service *app.Service) *Service {
	return &Service{app: service}
}

func (s *Service) Handle(ctx context.Context, principal Principal, message Message) (View, error) {
	if err := validatePrincipal(principal); err != nil {
		return View{}, err
	}
	if err := validateMessageContent(message); err != nil {
		return View{}, err
	}
	submissionReceiptOnly := false
	stream, err := s.app.ExternalEvents(ctx, principal.OrganizationID, message.ConversationID)
	if err != nil {
		return View{}, fmt.Errorf("%w: load work stream", ErrUnavailable)
	}
	if len(stream) == 0 {
		if message.TaskID != "" {
			return View{}, fmt.Errorf("%w: continuation task does not match durable work", ErrConflict)
		}
		if err := ValidateIdentifier("conversation", message.ConversationID); err != nil {
			return View{}, err
		}
		if err := ValidateIdentifier("message", message.MessageID); err != nil {
			return View{}, err
		}
		if !principal.Allowed(CapabilitySubmitWork) {
			return View{}, fmt.Errorf("%w: %s", ErrForbidden, CapabilitySubmitWork)
		}
		kind, err := s.router.Route(message)
		if err != nil {
			return View{}, err
		}
		submitted, submitErr := s.app.Submit(ctx, app.Submit{
			RequestID: message.ConversationID, OrganizationID: principal.OrganizationID,
			Statement: message.Text, Kind: kind, MessageID: message.MessageID, SourcePrincipalID: core.ID(principal.ID),
			SourcePrincipalKind: principal.Kind, SourceChannel: principal.Channel,
		})
		stream, err = s.app.ExternalEvents(ctx, principal.OrganizationID, message.ConversationID)
		if err != nil || (submitErr != nil && submitted.Task.ID == "") {
			return View{}, fmt.Errorf("%w: submit work", ErrUnavailable)
		}
		submissionReceiptOnly = !principal.Allowed(CapabilityReadStatus)
	} else {
		initial, err := authorizedInitialWork(principal, stream)
		if err != nil {
			return View{}, err
		}
		initialIDMatches := initial.MessageID == message.MessageID
		initialReplay := initialIDMatches && initial.Text == message.Text
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
		durableInputReplay := matchesDurableInput(stream, principal, message)
		if err := ValidateIdentifier("message", message.MessageID); err != nil && !initialReplay && !durableInputReplay {
			return View{}, err
		}
		if initialIDMatches && !initialReplay {
			return View{}, fmt.Errorf("%w: initial message id is bound to different content", ErrConflict)
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
	}
	if streamTaskID(stream) == "" {
		return View{}, fmt.Errorf("%w: work did not create a task", ErrUnavailable)
	}
	if submissionReceiptOnly {
		return projectSubmissionReceipt(message.ConversationID, stream), nil
	}
	return projectView(message.ConversationID, stream, principal.Allowed(CapabilityReadResult)), nil
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
	return projectCompletionReview(view, "PENDING"), nil
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
		State: state, Objective: view.Request.Objective, Result: view.Result,
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
	view := View{TaskID: streamTaskID(stream), ConversationID: conversationID, State: externalState(stream)}
	if len(stream) > 0 {
		view.UpdatedAt = stream[len(stream)-1].CreatedAt
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
	for index := len(stream) - 1; index >= 0; index-- {
		switch stream[index].EventType {
		case "TASK_CREATED", "TASK_BLOCKED", "TASK_RESUMED", "EXECUTION_STARTED", "TASK_VERIFIED_COMPLETE", "COMPLETION_REJECTED":
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

func projectSubmissionReceipt(conversationID string, stream []events.Event) View {
	view := View{TaskID: streamTaskID(stream), ConversationID: conversationID, State: StateWorking}
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
	for _, event := range stream {
		switch event.EventType {
		case "TASK_BLOCKED":
			state = StateInputRequired
		case "TASK_RESUMED":
			state = StateWorking
		case "TASK_VERIFIED_COMPLETE":
			state = StateCompleted
		case "COMPLETION_REJECTED":
			state = StateFailed
		}
	}
	return state
}

func streamTaskID(stream []events.Event) string {
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].TaskID != "" {
			return stream[i].TaskID
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
			return initialWork{
				Message:       Message{MessageID: intent.SourceMessageID, Text: intent.OriginalInstruction},
				PrincipalID:   intent.SourcePrincipalID,
				PrincipalKind: intent.SourcePrincipalKind,
				Channel:       intent.SourceChannel,
			}, true
		}
	}
	return initialWork{}, false
}

func principalCanAccess(principal Principal, work initialWork) bool {
	return principal.WorkScope == WorkScopeOrganization || (principal.WorkScope == WorkScopeOwn && work.PrincipalID == core.ID(principal.ID))
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

func matchesDurableInput(stream []events.Event, principal Principal, message Message) bool {
	for _, event := range stream {
		if event.EventType != "A2A_INPUT_RECEIVED" && event.EventType != "HUMAN_INPUT_RECEIVED" {
			continue
		}
		var input events.OperatorInputReceivedPayload
		if json.Unmarshal(event.Payload, &input) == nil {
			return input.MessageID == message.MessageID && input.Text == message.Text && input.SourcePrincipalID == principal.ID && input.SourcePrincipalKind == string(principal.Kind) && input.SourceChannel == principal.Channel
		}
	}
	return false
}

func blockedStatusText(stream []events.Event) string {
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].EventType != "TASK_BLOCKED" {
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
	for i := len(stream) - 1; i >= 0; i-- {
		if stream[i].EventType != "RESULT_PUBLISHED" {
			continue
		}
		var result events.ResultPublishedPayload
		if json.Unmarshal(stream[i].Payload, &result) == nil && result.ValidFor(stream[i].ArtifactRefs) {
			return result, true
		}
	}
	return events.ResultPublishedPayload{}, false
}
