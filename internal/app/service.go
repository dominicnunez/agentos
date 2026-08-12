package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/projections"
	"github.com/dominicnunez/agentos/internal/telemetry"
	"github.com/dominicnunez/agentos/internal/workflow"
)

const submissionTimeout = 25 * time.Second

type Submit struct {
	RequestID           string
	OrganizationID      string
	Statement           string
	Kind                core.ExecutionKind
	MessageID           string
	SourcePrincipalID   core.ID
	SourcePrincipalKind core.PrincipalKind
	SourceChannel       string
	correlationID       string
}

type Result struct {
	Intent     core.Intent       `json:"intent"`
	Goal       core.Goal         `json:"goal"`
	Task       core.Task         `json:"task"`
	Outcome    core.ToolOutcome  `json:"outcome"`
	Completion completion.Result `json:"completion"`
	Events     []events.Event    `json:"events"`
}

type RecoveryResult struct {
	PendingFound     int `json:"pending_found"`
	BlockedPreserved int `json:"blocked_preserved"`
	RunningRecovered int `json:"running_recovered"`
	TasksExecuted    int `json:"tasks_executed"`
}

type Service struct {
	permit        chan struct{}
	gateway       *events.Gateway
	state         *projections.Repository
	scheduler     workflow.Scheduler
	deterministic execution.Handler
	agent         execution.Handler
	agentModel    execution.ModelDescriptor
	verifier      completion.Verifier
	completion    completion.Engine
}

func New(g *events.Gateway) *Service {
	return NewWithModel(g, execution.FakeModel{})
}

func NewWithModel(g *events.Gateway, model execution.ModelAdapter) *Service {
	if g == nil || model == nil {
		panic("event gateway and model adapter are required")
	}
	agent := execution.NewAgentExecution(model)
	descriptor := agent.Descriptor()
	if descriptor.Provider == "" || descriptor.Model == "" || descriptor.ExecutionProfileVersion == "" {
		panic("model adapter descriptor is incomplete")
	}
	service := &Service{
		permit:        make(chan struct{}, 1),
		gateway:       g,
		state:         projections.New(g),
		deterministic: execution.Deterministic{},
		agent:         agent,
		agentModel:    descriptor,
		verifier:      completion.Verifier{},
	}
	g.SetRouteValidator(service)
	return service
}

func (s *Service) Events(ctx context.Context, requestID string) ([]events.Event, error) {
	return s.gateway.Events(ctx, requestID)
}

// ExternalEvents returns a request stream only when every event belongs to the
// authenticated external actor's organization. A mismatched or mixed stream is
// indistinguishable from an unknown request and never leaks tenant existence.
func (s *Service) ExternalEvents(ctx context.Context, organizationID, requestID string) ([]events.Event, error) {
	if organizationID == "" || requestID == "" {
		return nil, fmt.Errorf("organization and request are required")
	}
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, organizationID, requestID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return nil, err
	}
	for _, event := range stream {
		if event.OrganizationID != organizationID {
			return nil, nil
		}
	}
	return stream, nil
}

// ExternalTaskEvents resolves an opaque task identifier within one tenant and
// returns the original external conversation identifier with its event stream.
func (s *Service) ExternalTaskEvents(ctx context.Context, organizationID, taskID string) (string, []events.Event, error) {
	if organizationID == "" || taskID == "" {
		return "", nil, fmt.Errorf("organization and task are required")
	}
	requestID, correlationID, found, err := s.gateway.ResolveExternalTask(ctx, organizationID, taskID)
	if err != nil {
		return "", nil, err
	}
	if !found {
		return "", nil, nil
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return "", nil, err
	}
	for _, event := range stream {
		if event.OrganizationID != organizationID {
			return "", nil, nil
		}
	}
	return requestID, stream, nil
}

func (s *Service) requireExternalWorkCorrelation(ctx context.Context, organizationID, requestID string) (string, error) {
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, organizationID, requestID)
	if err != nil {
		return "", err
	}
	if found {
		return correlationID, nil
	}
	return "", fmt.Errorf("external request is not registered")
}

// SendMessage is the lateral Agent-to-Agent/Team/Task path. It deliberately
// uses an EventDraft; the gateway owns trusted sender metadata, persistence,
// and inbox availability.
func (s *Service) SendMessage(ctx context.Context, organizationID, actorID, executionID, correlationID string, draft events.Draft) (events.Event, error) {
	if err := s.acquire(ctx); err != nil {
		return events.Event{}, err
	}
	defer s.release()
	if draft.EventType != "MESSAGE" {
		return events.Event{}, fmt.Errorf("SendMessage accepts only MESSAGE drafts")
	}
	return s.gateway.PublishAgentDraft(ctx, organizationID, actorID, executionID, correlationID, draft)
}

// ValidateAddressedRoute implements events.RouteValidator with durable identity
// and task projections. Authenticated envelope identity, never payload text,
// determines the sender and recipient.
func (s *Service) ValidateAddressedRoute(ctx context.Context, route events.AddressedRoute) error {
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return err
	}
	organizationID := core.ID(route.OrganizationID)
	if _, ok := snapshot.Organizations[organizationID]; !ok {
		return fmt.Errorf("addressed event organization does not exist")
	}
	var source *projections.Versioned[core.Agent]
	if route.ValidateSource {
		state, ok := snapshot.Agents[core.ID(route.SourceActorID)]
		if !ok || state.Value.OrganizationID != organizationID {
			return fmt.Errorf("addressed event source is not an Agent in the organization")
		}
		source = &state
	}
	var sourceTask core.Task
	if route.TaskID != "" {
		task, err := addressedTaskInOrganization(snapshot, core.ID(route.TaskID), organizationID, "source")
		if err != nil {
			return err
		}
		if source != nil && !agentParticipates(snapshot, source.Value.ID, task) {
			return fmt.Errorf("addressed event source is not a participant in the task")
		}
		sourceTask = task
	}
	switch route.RecipientScope {
	case events.RecipientAgent:
		recipient, ok := snapshot.Agents[core.ID(route.RecipientID)]
		if !ok || recipient.Value.OrganizationID != organizationID {
			return fmt.Errorf("addressed event recipient Agent is outside the organization")
		}
	case events.RecipientTeam:
		recipient, ok := snapshot.Teams[core.ID(route.RecipientID)]
		if !ok || recipient.Value.OrganizationID != organizationID {
			return fmt.Errorf("addressed event recipient Team is outside the organization")
		}
	case events.RecipientTask:
		if _, err := addressedTaskInOrganization(snapshot, core.ID(route.RecipientID), organizationID, "recipient"); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported addressed event recipient scope")
	}
	if route.EventType == "TASK_BLOCKED" {
		if route.TaskID == "" || sourceTask.ParentID == "" {
			return fmt.Errorf("blocked event source must be a child task with an existing parent")
		}
		if route.RecipientScope != events.RecipientTask || core.ID(route.RecipientID) != sourceTask.ParentID {
			return fmt.Errorf("blocked child task must return control to its parent task")
		}
	}
	return nil
}

func addressedTaskInOrganization(snapshot projections.Snapshot, taskID, organizationID core.ID, role string) (core.Task, error) {
	task, ok := snapshot.Tasks[taskID]
	if !ok {
		return core.Task{}, fmt.Errorf("addressed event %s task does not exist", role)
	}
	actualOrganizationID, err := taskOrganization(snapshot, task.Value)
	if err != nil || actualOrganizationID != organizationID {
		return core.Task{}, fmt.Errorf("addressed event %s task is outside the organization", role)
	}
	return task.Value, nil
}

// Recover validates all durable work before the process exposes an operator
// endpoint, preserves blocked tasks, and executes dependency-ready pending
// work. Interrupted deterministic work is safe to retry; interrupted adaptive
// execution fails closed as blocked because its outcome may be uncertain.
func (s *Service) Recover(ctx context.Context) (RecoveryResult, error) {
	if err := s.acquire(ctx); err != nil {
		return RecoveryResult{}, err
	}
	defer s.release()

	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("load durable runtime state: %w", err)
	}
	for _, state := range sortedTaskStates(snapshot.Tasks) {
		stream, err := s.gateway.Events(ctx, state.CorrelationID)
		if err != nil {
			return RecoveryResult{}, err
		}
		requests, decisions, err := completionReviewRecords(stream)
		if err != nil {
			return RecoveryResult{}, fmt.Errorf("validate completion review recovery for task %s: %w", state.Value.ID, err)
		}
		var latest completion.ReviewRequest
		for _, event := range stream {
			if event.EventType != "COMPLETION_REVIEW_REQUESTED" {
				continue
			}
			var request completion.ReviewRequest
			if json.Unmarshal(event.Payload, &request) == nil && request.TaskID == state.Value.ID {
				latest = requests[request.ID]
			}
		}
		if recorded, decided := decisions[latest.ID]; decided {
			if err := s.continueCompletionReview(ctx, latest, recorded.Review, recorded.Event); err != nil {
				return RecoveryResult{}, fmt.Errorf("recover completion review for task %s: %w", state.Value.ID, err)
			}
		}
	}
	snapshot, err = s.state.Load(ctx)
	if err != nil {
		return RecoveryResult{}, fmt.Errorf("reload durable runtime state after completion reviews: %w", err)
	}
	result := RecoveryResult{}
	continuedInputs := 0
	for _, state := range sortedTaskStates(snapshot.Tasks) {
		organizationID, err := taskOrganization(snapshot, state.Value)
		if err != nil {
			return RecoveryResult{}, err
		}
		if state.Value.ExecutionKind == core.ExecutionHuman && state.Value.Status != core.TaskCompleted {
			stream, err := s.gateway.Events(ctx, state.CorrelationID)
			if err != nil {
				return RecoveryResult{}, err
			}
			completionEvent, completionPayload, completionFound, err := humanCompletionForTask(stream, state.Value.ID)
			if err != nil {
				return RecoveryResult{}, err
			}
			if completionFound {
				if state.Value.Status == core.TaskPending {
					result.PendingFound++
				}
				if state.Value.Status == core.TaskRunning {
					result.RunningRecovered++
				}
				if err := s.continueHumanCompletionTask(ctx, organizationID, state.Value.ID, state.CorrelationID, completionEvent, completionPayload); err != nil {
					return RecoveryResult{}, fmt.Errorf("recover user completion for task %s: %w", state.Value.ID, err)
				}
				continuedInputs++
				continue
			}
			inputEvent, _, found, err := externalInputForTask(stream, state.Value.ID)
			if err != nil {
				return RecoveryResult{}, err
			}
			if found {
				if state.Value.Status == core.TaskPending {
					result.PendingFound++
				}
				if state.Value.Status == core.TaskRunning {
					result.RunningRecovered++
				}
				if err := s.continueExternalInputTask(ctx, organizationID, state.Value.ID, state.CorrelationID, inputEvent); err != nil {
					return RecoveryResult{}, fmt.Errorf("recover external input continuation for task %s: %w", state.Value.ID, err)
				}
				continuedInputs++
				continue
			}
		}
		switch state.Value.Status {
		case core.TaskPending:
			result.PendingFound++
		case core.TaskBlocked:
			result.BlockedPreserved++
		case core.TaskCompleted, core.TaskFailed:
			// Terminal tasks require no recovery action.
		case core.TaskRunning:
			task := state.Value
			detail := any(map[string]string{"reason": "process restarted before execution reached a durable terminal state"})
			eventType := "TASK_RECOVERED"
			var blocked events.TaskBlockedPayload
			if task.ExecutionKind == core.ExecutionDeterministic {
				task.Status = core.TaskPending
				result.PendingFound++
			} else {
				task.Status = core.TaskBlocked
				eventType = "TASK_BLOCKED"
				blocked = blockedDetail("interrupted adaptive execution has an uncertain outcome", "operator reconciliation", "blind replay could duplicate cost or nondeterministic work")
				detail = blocked
				result.BlockedPreserved++
			}
			var saveErr error
			if eventType == "TASK_BLOCKED" {
				saveErr = s.saveBlockedTask(ctx, snapshot, state, organizationID, task, blocked)
			} else {
				saveErr = s.state.SaveTask(ctx, organizationID, eventType, "runtime", state.CorrelationID, state.Version+1, task, detail)
			}
			if saveErr != nil {
				return RecoveryResult{}, fmt.Errorf("persist recovery for task %s: %w", task.ID, saveErr)
			}
			result.RunningRecovered++
		}
	}
	runs, err := s.runReady(ctx)
	if err != nil {
		return RecoveryResult{}, err
	}
	result.TasksExecuted = len(runs) + continuedInputs
	if err := s.reconcileGoals(ctx); err != nil {
		return RecoveryResult{}, err
	}
	return result, nil
}

type OperatorInput struct {
	OrganizationID string
	PrincipalID    string
	PrincipalKind  core.PrincipalKind
	SourceChannel  string
	RequestID      string
	TaskID         string
	MessageID      string
	Text           string
}

type HumanCompletionInput struct {
	OrganizationID string
	PrincipalID    string
	SourceChannel  string
	RequestID      string
	TaskID         string
	Submission     core.HumanTaskSubmission
}

func (s *Service) ProvideHumanCompletion(ctx context.Context, input HumanCompletionInput) error {
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	if input.OrganizationID == "" || input.PrincipalID == "" || input.SourceChannel != "HUMAN_DIRECT" || input.RequestID == "" || input.TaskID == "" || input.Submission.MessageID == "" {
		return fmt.Errorf("organization, local user principal, request, task, and submission identity are required")
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return err
	}
	correlationID, err := s.requireExternalWorkCorrelation(ctx, input.OrganizationID, input.RequestID)
	if err != nil {
		return err
	}
	state, ok := snapshot.Tasks[core.ID(input.TaskID)]
	if !ok || state.CorrelationID != correlationID || state.Value.ExecutionKind != core.ExecutionHuman || state.Value.CompletionContract == nil {
		return fmt.Errorf("task is not a structured user task for this request")
	}
	actualOrganizationID, err := taskOrganization(snapshot, state.Value)
	if err != nil || actualOrganizationID != core.ID(input.OrganizationID) {
		return fmt.Errorf("task is not mapped to this request and organization")
	}
	result := s.completion.EvaluateHumanTask(*state.Value.CompletionContract, input.Submission)
	if !result.Complete {
		return fmt.Errorf("user task completion contract is not satisfied: %s", strings.Join(result.Reasons, "; "))
	}
	for _, artifact := range input.Submission.Artifacts {
		if artifact.Origin != input.PrincipalID {
			return fmt.Errorf("user task artifact origin does not match the authenticated principal")
		}
	}
	payload := events.HumanTaskCompletionSubmittedPayload{
		MessageID: input.Submission.MessageID, Fields: input.Submission.Fields, Artifacts: input.Submission.Artifacts,
		SourcePrincipalID: input.PrincipalID, SourceChannel: input.SourceChannel,
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return err
	}
	completionEvent, existing, found, err := humanCompletionForTask(stream, state.Value.ID)
	if err != nil {
		return err
	}
	if found {
		existingBody, _ := json.Marshal(existing)
		payloadBody, _ := json.Marshal(payload)
		if completionEvent.SourceActorID != input.PrincipalID || string(existingBody) != string(payloadBody) {
			return fmt.Errorf("task already has a different durable user completion")
		}
	} else {
		if state.Value.Status != core.TaskBlocked {
			return fmt.Errorf("task is not blocked awaiting structured user completion")
		}
		completionEvent, err = s.gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: input.OrganizationID, EventType: "HUMAN_TASK_COMPLETION_SUBMITTED",
			SourceActorID: input.PrincipalID, TaskID: input.TaskID, CorrelationID: correlationID,
			ArtifactRefs: artifactRefs(input.Submission.Artifacts), Payload: payload,
		})
		if err != nil {
			return err
		}
	}
	if err := s.continueHumanCompletionTask(ctx, actualOrganizationID, state.Value.ID, correlationID, completionEvent, payload); err != nil {
		return err
	}
	return s.reconcileGoals(ctx)
}

func (s *Service) continueHumanCompletionTask(ctx context.Context, organizationID, taskID core.ID, correlationID string, completionEvent events.Event, payload events.HumanTaskCompletionSubmittedPayload) error {
	if completionEvent.EventID == "" || completionEvent.EventType != "HUMAN_TASK_COMPLETION_SUBMITTED" || core.ID(completionEvent.TaskID) != taskID {
		return fmt.Errorf("valid durable user completion event is required")
	}
	for {
		snapshot, err := s.state.Load(ctx)
		if err != nil {
			return err
		}
		state, ok := snapshot.Tasks[taskID]
		if !ok || state.CorrelationID != correlationID || state.Value.ExecutionKind != core.ExecutionHuman || state.Value.CompletionContract == nil {
			return fmt.Errorf("user completion task is invalid")
		}
		task := state.Value
		switch task.Status {
		case core.TaskCompleted:
			return nil
		case core.TaskFailed:
			return fmt.Errorf("user completion cannot advance failed task %s", task.ID)
		case core.TaskBlocked:
			task.Status = core.TaskPending
			detail := map[string]string{"reason": "structured user completion received", "completion_event_ref": completionEvent.EventID}
			if err := s.state.SaveTask(ctx, organizationID, "TASK_RESUMED", "runtime", correlationID, state.Version+1, task, detail); err != nil {
				return err
			}
		case core.TaskPending:
			task.Status = core.TaskRunning
			detail := map[string]string{"mode": "STRUCTURED_HUMAN_COMPLETION", "completion_event_ref": completionEvent.EventID}
			if err := s.state.SaveTask(ctx, organizationID, "EXECUTION_STARTED", "runtime", correlationID, state.Version+1, task, detail); err != nil {
				return err
			}
		case core.TaskRunning:
			return s.finishHumanCompletionTask(ctx, organizationID, state, completionEvent, payload)
		default:
			return fmt.Errorf("user completion cannot advance task in status %s", task.Status)
		}
	}
}

func (s *Service) finishHumanCompletionTask(ctx context.Context, organizationID core.ID, state projections.Versioned[core.Task], completionEvent events.Event, payload events.HumanTaskCompletionSubmittedPayload) error {
	task := state.Value
	contract := *task.CompletionContract
	submission := core.HumanTaskSubmission{MessageID: payload.MessageID, Fields: payload.Fields, Artifacts: payload.Artifacts}
	complete := s.completion.EvaluateHumanTask(contract, submission)
	if !complete.Complete {
		return fmt.Errorf("durable user completion no longer satisfies its contract")
	}
	executionID := core.ID("human-completion-" + completionEvent.EventID)
	now := time.Now().UTC()
	outcome := core.ToolOutcome{
		ToolInvocationID: core.ID("human-task-" + completionEvent.EventID), ToolID: "human.task-completion", ToolVersion: "v1",
		Status: core.OutcomeSucceeded, ObservedEffect: map[string]any{"status": "structured user completion persisted", "completion_event_ref": completionEvent.EventID},
		PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable,
		ArtifactRefs: artifactRefs(payload.Artifacts), StartedAt: now, FinishedAt: now,
	}
	stream, err := s.gateway.Events(ctx, state.CorrelationID)
	if err != nil {
		return err
	}
	if existing, found, err := continuationEvent(stream, "TOOL_OUTCOME_RECORDED", task.ID, executionID); err != nil {
		return err
	} else if found {
		if err := json.Unmarshal(existing.Payload, &outcome); err != nil || outcome.ToolID != "human.task-completion" || outcome.Status != core.OutcomeSucceeded {
			return fmt.Errorf("durable user completion outcome is invalid")
		}
	} else if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: outcome, CorrelationID: state.CorrelationID}); err != nil {
		return err
	}
	if err := s.publishContinuationEventIfMissing(ctx, stream, organizationID, task.ID, state.CorrelationID, executionID, "EXECUTION_FINISHED", map[string]any{"status": outcome.Status}); err != nil {
		return err
	}
	if err := s.publishContinuationEventIfMissing(ctx, stream, organizationID, task.ID, state.CorrelationID, executionID, "RESULT_PUBLISHED", events.ResultPublishedPayload{Summary: "Structured user completion accepted.", ArtifactRefs: outcome.ArtifactRefs}); err != nil {
		return err
	}
	if err := s.publishContinuationEventIfMissing(ctx, stream, organizationID, task.ID, state.CorrelationID, executionID, "CANDIDATE_COMPLETE", map[string]any{"completion_event_ref": completionEvent.EventID}); err != nil {
		return err
	}
	detail := completionDetail{Contract: contract, Result: complete}
	if _, found, err := continuationEvent(stream, "COMPLETION_VERIFIED", task.ID, executionID); err != nil {
		return err
	} else if !found {
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: detail, CorrelationID: state.CorrelationID}); err != nil {
			return err
		}
	}
	task.Status = core.TaskCompleted
	return s.state.SaveTask(ctx, organizationID, "TASK_VERIFIED_COMPLETE", "runtime", state.CorrelationID, state.Version+1, task, detail)
}

func humanCompletionForTask(stream []events.Event, taskID core.ID) (events.Event, events.HumanTaskCompletionSubmittedPayload, bool, error) {
	var found events.Event
	var payload events.HumanTaskCompletionSubmittedPayload
	for _, event := range stream {
		if event.EventType != "HUMAN_TASK_COMPLETION_SUBMITTED" || core.ID(event.TaskID) != taskID {
			continue
		}
		if found.EventID != "" {
			return events.Event{}, events.HumanTaskCompletionSubmittedPayload{}, false, fmt.Errorf("task has multiple durable user completion events")
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.MessageID == "" || payload.SourcePrincipalID != event.SourceActorID || payload.SourceChannel != "HUMAN_DIRECT" {
			return events.Event{}, events.HumanTaskCompletionSubmittedPayload{}, false, fmt.Errorf("durable user completion event is invalid")
		}
		found = event
	}
	return found, payload, found.EventID != "", nil
}

func artifactRefs(artifacts []core.ArtifactEvidence) []string {
	refs := make([]string, len(artifacts))
	for index, artifact := range artifacts {
		refs[index] = artifact.Ref
	}
	return refs
}

func (s *Service) ProvideOperatorInput(ctx context.Context, input OperatorInput) error {
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()

	if input.OrganizationID == "" || input.PrincipalID == "" || input.PrincipalKind == "" || input.SourceChannel == "" || input.RequestID == "" || input.TaskID == "" || input.MessageID == "" || input.Text == "" {
		return fmt.Errorf("organization, principal, source, request, task, message, and text are required")
	}
	eventType, err := operatorInputEventType(input.SourceChannel)
	if err != nil {
		return err
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return err
	}
	correlationID, err := s.requireExternalWorkCorrelation(ctx, input.OrganizationID, input.RequestID)
	if err != nil {
		return err
	}
	state, ok := snapshot.Tasks[core.ID(input.TaskID)]
	if !ok || state.CorrelationID != correlationID {
		return fmt.Errorf("task is not mapped to this external request")
	}
	actualOrganizationID, err := taskOrganization(snapshot, state.Value)
	if err != nil {
		return err
	}
	if actualOrganizationID != core.ID(input.OrganizationID) {
		return fmt.Errorf("task is not mapped to this external request and organization")
	}
	if state.Value.ExecutionKind != core.ExecutionHuman {
		return fmt.Errorf("external input can continue only a user-operated task")
	}
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return err
	}
	inputEvent, existing, found, err := externalInputForTask(stream, core.ID(input.TaskID))
	if err != nil {
		return err
	}
	if found {
		if inputEvent.SourceActorID != input.PrincipalID || existing.SourcePrincipalID != input.PrincipalID || existing.SourcePrincipalKind != string(input.PrincipalKind) || existing.SourceChannel != input.SourceChannel || existing.MessageID != input.MessageID || existing.Text != input.Text {
			return fmt.Errorf("task already has different durable external input")
		}
	} else {
		if state.Value.Status != core.TaskBlocked {
			return fmt.Errorf("task is not blocked awaiting external input")
		}
		inputEvent, err = s.gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: input.OrganizationID,
			EventType:      eventType,
			SourceActorID:  input.PrincipalID,
			TaskID:         input.TaskID,
			CorrelationID:  correlationID,
			Payload: events.OperatorInputReceivedPayload{
				MessageID: input.MessageID, Text: input.Text, SourcePrincipalID: input.PrincipalID,
				SourcePrincipalKind: string(input.PrincipalKind), SourceChannel: input.SourceChannel,
			},
		})
		if err != nil {
			return err
		}
	}
	if err := s.continueExternalInputTask(ctx, actualOrganizationID, core.ID(input.TaskID), correlationID, inputEvent); err != nil {
		return err
	}
	return s.reconcileGoals(ctx)
}

// continueExternalInputTask resumes from the durable input Event Contract and
// appends only missing phases. The external actor supplies content; the runtime
// alone records outcome and completion attestations.
func (s *Service) continueExternalInputTask(ctx context.Context, organizationID, taskID core.ID, correlationID string, inputEvent events.Event) error {
	if inputEvent.EventID == "" || !isOperatorInputEvent(inputEvent.EventType) || core.ID(inputEvent.TaskID) != taskID {
		return fmt.Errorf("valid durable external input event is required")
	}
	for {
		snapshot, err := s.state.Load(ctx)
		if err != nil {
			return err
		}
		state, ok := snapshot.Tasks[taskID]
		if !ok || state.CorrelationID != correlationID || state.Value.ExecutionKind != core.ExecutionHuman {
			return fmt.Errorf("external input continuation task is invalid")
		}
		task := state.Value
		switch task.Status {
		case core.TaskCompleted:
			return nil
		case core.TaskFailed:
			return fmt.Errorf("external input continuation cannot advance failed task %s", task.ID)
		case core.TaskBlocked:
			task.Status = core.TaskPending
			detail := map[string]string{"reason": "authorized external input received", "input_event_ref": inputEvent.EventID}
			if err := s.state.SaveTask(ctx, organizationID, "TASK_RESUMED", "runtime", correlationID, state.Version+1, task, detail); err != nil {
				return err
			}
			continue
		case core.TaskPending:
			task.Status = core.TaskRunning
			detail := map[string]string{"mode": "OPERATOR_HUMAN_INPUT", "input_event_ref": inputEvent.EventID}
			if err := s.state.SaveTask(ctx, organizationID, "EXECUTION_STARTED", "runtime", correlationID, state.Version+1, task, detail); err != nil {
				return fmt.Errorf("persist external input execution start for task %s: %w", task.ID, err)
			}
			continue
		case core.TaskRunning:
			return s.finishExternalInputTask(ctx, organizationID, state, inputEvent)
		default:
			return fmt.Errorf("external input continuation cannot advance task in status %s", task.Status)
		}
	}
}

func (s *Service) finishExternalInputTask(ctx context.Context, organizationID core.ID, state projections.Versioned[core.Task], inputEvent events.Event) error {
	task := state.Value
	executionID := core.ID("external-input-" + inputEvent.EventID)
	stream, err := s.gateway.Events(ctx, state.CorrelationID)
	if err != nil {
		return err
	}
	outcomeEvent, hasOutcome, err := continuationEvent(stream, "TOOL_OUTCOME_RECORDED", task.ID, executionID)
	if err != nil {
		return err
	}
	var outcome core.ToolOutcome
	if hasOutcome {
		if err := json.Unmarshal(outcomeEvent.Payload, &outcome); err != nil || outcome.ToolID != "a2a.external-input" || outcome.Status != core.OutcomeSucceeded || outcome.PostconditionStatus != core.PostconditionVerified {
			return fmt.Errorf("durable external input outcome is invalid")
		}
	} else {
		now := time.Now().UTC()
		outcome = core.ToolOutcome{
			ToolInvocationID:    core.ID("a2a-input-" + inputEvent.EventID),
			ToolID:              "a2a.external-input",
			ToolVersion:         "v1",
			Status:              core.OutcomeSucceeded,
			ObservedEffect:      map[string]string{"status": "authorized external input persisted", "input_event_ref": inputEvent.EventID},
			PostconditionStatus: core.PostconditionVerified,
			Retryability:        core.NotRetryable,
			StartedAt:           now,
			FinishedAt:          now,
		}
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: outcome, CorrelationID: state.CorrelationID}); err != nil {
			return fmt.Errorf("persist external input outcome for task %s: %w", task.ID, err)
		}
	}
	if err := s.publishContinuationEventIfMissing(ctx, stream, organizationID, task.ID, state.CorrelationID, executionID, "EXECUTION_FINISHED", map[string]any{"status": outcome.Status}); err != nil {
		return err
	}
	summary, err := outcomeSummary(outcome)
	if err != nil {
		return fmt.Errorf("materialize external input result for task %s: %w", task.ID, err)
	}
	if err := s.publishContinuationEventIfMissing(ctx, stream, organizationID, task.ID, state.CorrelationID, executionID, "RESULT_PUBLISHED", events.ResultPublishedPayload{Summary: summary, ArtifactRefs: outcome.ArtifactRefs}); err != nil {
		return err
	}
	if err := s.publishContinuationEventIfMissing(ctx, stream, organizationID, task.ID, state.CorrelationID, executionID, "CANDIDATE_COMPLETE", map[string]any{"tool_invocation_id": outcome.ToolInvocationID, "input_event_ref": inputEvent.EventID}); err != nil {
		return err
	}
	contract := core.CompletionContract{TaskID: task.ID, TaskVersion: state.Version, Criteria: []core.CompletionCriterion{{ID: "durable-external-input", Description: "authorized external input was durably recorded", Assurance: core.AssuranceDeterministic, Required: true}}}
	complete := s.completion.Evaluate(contract, outcome)
	detail := completionDetail{Contract: contract, Result: complete}
	if !complete.Complete {
		task.Status = core.TaskFailed
		if err := s.state.SaveTask(ctx, organizationID, "COMPLETION_REJECTED", "runtime", state.CorrelationID, state.Version+1, task, detail); err != nil {
			return fmt.Errorf("persist rejected external input completion for task %s: %w", task.ID, err)
		}
		return nil
	}
	verifiedEvent, verified, err := continuationEvent(stream, "COMPLETION_VERIFIED", task.ID, executionID)
	if err != nil {
		return err
	}
	if verified {
		var recorded completionDetail
		if err := json.Unmarshal(verifiedEvent.Payload, &recorded); err != nil || !recorded.Result.Complete || recorded.Contract.TaskID != task.ID {
			return fmt.Errorf("durable external input completion verification is invalid")
		}
		detail = recorded
	} else if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: detail, CorrelationID: state.CorrelationID}); err != nil {
		return fmt.Errorf("persist external input completion verification for task %s: %w", task.ID, err)
	}
	task.Status = core.TaskCompleted
	if err := s.state.SaveTask(ctx, organizationID, "TASK_VERIFIED_COMPLETE", "runtime", state.CorrelationID, state.Version+1, task, detail); err != nil {
		return fmt.Errorf("persist completed external input task %s: %w", task.ID, err)
	}
	return nil
}

func (s *Service) publishContinuationEventIfMissing(ctx context.Context, stream []events.Event, organizationID, taskID core.ID, correlationID string, executionID core.ID, eventType string, payload any) error {
	if _, found, err := continuationEvent(stream, eventType, taskID, executionID); err != nil {
		return err
	} else if found {
		return nil
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: eventType, SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(taskID), Payload: payload, CorrelationID: correlationID}); err != nil {
		return fmt.Errorf("persist external input %s for task %s: %w", eventType, taskID, err)
	}
	return nil
}

func externalInputForTask(stream []events.Event, taskID core.ID) (events.Event, events.OperatorInputReceivedPayload, bool, error) {
	var found events.Event
	var payload events.OperatorInputReceivedPayload
	for _, event := range stream {
		if !isOperatorInputEvent(event.EventType) || core.ID(event.TaskID) != taskID {
			continue
		}
		if found.EventID != "" {
			return events.Event{}, events.OperatorInputReceivedPayload{}, false, fmt.Errorf("task has multiple durable external input events")
		}
		var err error
		payload, err = decodeOperatorInput(event)
		if err != nil {
			return events.Event{}, events.OperatorInputReceivedPayload{}, false, fmt.Errorf("durable external input event is invalid")
		}
		found = event
	}
	return found, payload, found.EventID != "", nil
}

func decodeOperatorInput(event events.Event) (events.OperatorInputReceivedPayload, error) {
	var current events.OperatorInputReceivedPayload
	if err := json.Unmarshal(event.Payload, &current); err == nil && validOperatorInput(event, current) {
		return current, nil
	}
	if event.EventType != "A2A_INPUT_RECEIVED" || event.EventID == "" {
		return events.OperatorInputReceivedPayload{}, fmt.Errorf("unsupported operator input contract")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &fields); err != nil || len(fields) != 2 {
		return events.OperatorInputReceivedPayload{}, fmt.Errorf("invalid legacy A2A input contract")
	}
	var text, sourceExternalActor string
	if err := json.Unmarshal(fields["text"], &text); err != nil || json.Unmarshal(fields["source_external_actor"], &sourceExternalActor) != nil || text == "" || sourceExternalActor == "" || sourceExternalActor != event.SourceActorID {
		return events.OperatorInputReceivedPayload{}, fmt.Errorf("invalid legacy A2A input contract")
	}
	return events.OperatorInputReceivedPayload{
		MessageID:           "legacy-a2a-" + event.EventID,
		Text:                text,
		SourcePrincipalID:   sourceExternalActor,
		SourcePrincipalKind: string(core.PrincipalExternalAgent),
		SourceChannel:       "A2A",
	}, nil
}

func validOperatorInput(event events.Event, input events.OperatorInputReceivedPayload) bool {
	if input.MessageID == "" || input.Text == "" || input.SourcePrincipalID == "" || input.SourcePrincipalID != event.SourceActorID {
		return false
	}
	switch event.EventType {
	case "A2A_INPUT_RECEIVED":
		return input.SourcePrincipalKind == string(core.PrincipalExternalAgent) && input.SourceChannel == "A2A"
	case "HUMAN_INPUT_RECEIVED":
		return input.SourcePrincipalKind == string(core.PrincipalHuman) && input.SourceChannel == "HUMAN_DIRECT"
	default:
		return false
	}
}

func operatorInputEventType(channel string) (string, error) {
	switch channel {
	case "A2A":
		return "A2A_INPUT_RECEIVED", nil
	case "HUMAN_DIRECT":
		return "HUMAN_INPUT_RECEIVED", nil
	default:
		return "", fmt.Errorf("operator input channel is not supported")
	}
}

func isOperatorInputEvent(eventType string) bool {
	return eventType == "A2A_INPUT_RECEIVED" || eventType == "HUMAN_INPUT_RECEIVED"
}

func continuationEvent(stream []events.Event, eventType string, taskID, executionID core.ID) (events.Event, bool, error) {
	var found events.Event
	for _, event := range stream {
		if event.EventType != eventType || core.ID(event.TaskID) != taskID || core.ID(event.SourceExecutionID) != executionID {
			continue
		}
		if found.EventID != "" {
			return events.Event{}, false, fmt.Errorf("duplicate %s event for external input continuation", eventType)
		}
		found = event
	}
	return found, found.EventID != "", nil
}

func (s *Service) Submit(ctx context.Context, in Submit) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("submission context is required")
	}
	ctx, cancel := context.WithTimeout(ctx, submissionTimeout)
	defer cancel()
	if err := s.acquire(ctx); err != nil {
		return Result{}, fmt.Errorf("wait for submission service: %w", err)
	}
	defer s.release()

	if in.RequestID == "" || in.OrganizationID == "" || in.Statement == "" {
		return Result{}, fmt.Errorf("request_id, organization_id, and statement are required")
	}
	if in.Kind == "" {
		in.Kind = core.ExecutionDeterministic
	}
	if in.SourcePrincipalID == "" {
		in.SourcePrincipalID = "runtime"
	}
	if in.SourcePrincipalKind == "" {
		in.SourcePrincipalKind = core.PrincipalRuntime
	}
	if in.SourceChannel == "" {
		in.SourceChannel = "INTERNAL"
	}
	operatorSubmission := in.SourceChannel == "A2A" || in.SourceChannel == "HUMAN_DIRECT"
	if in.SourceChannel != "INTERNAL" && !operatorSubmission {
		return Result{}, fmt.Errorf("operator work channel is not supported")
	}
	if operatorSubmission && in.MessageID == "" {
		return Result{}, fmt.Errorf("operator submission message id is required")
	}
	correlationID, err := s.gateway.ReserveExternalWork(ctx, in.OrganizationID, in.RequestID)
	if err != nil {
		return Result{}, err
	}
	in.correlationID = correlationID
	intent, goal, task, err := s.ensureSubmission(ctx, in)
	if err != nil {
		return Result{}, err
	}
	if err := s.ensureOperatorAcceptance(ctx, in, task.ID); err != nil {
		return Result{}, err
	}
	runs, err := s.runReady(ctx)
	if err != nil {
		return Result{}, err
	}
	if err := s.reconcileGoals(ctx); err != nil {
		return Result{}, err
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return Result{}, err
	}
	intent = snapshot.Intents[intent.ID].Value
	goal = snapshot.Goals[goal.ID].Value
	task = snapshot.Tasks[task.ID].Value
	run, ok := runs[task.ID]
	if !ok {
		run, err = s.readTaskResult(ctx, correlationID)
		if err != nil {
			return Result{}, err
		}
	}
	eventStream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return Result{}, err
	}
	return Result{Intent: intent, Goal: goal, Task: task, Outcome: run.Outcome, Completion: run.Completion, Events: eventStream}, run.ExecutionError
}

func (s *Service) acquire(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("service context is required")
	}
	select {
	case s.permit <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) release() {
	<-s.permit
}

func (s *Service) ensureOperatorAcceptance(ctx context.Context, in Submit, taskID core.ID) error {
	if in.SourceChannel == "INTERNAL" {
		return nil
	}
	if in.MessageID == "" {
		return fmt.Errorf("operator submission message id is required")
	}
	eventType, err := operatorWorkAcceptedEventType(in.SourceChannel)
	if err != nil {
		return err
	}
	payload := events.OperatorWorkAcceptedPayload{
		MessageID: in.MessageID, SourcePrincipalID: string(in.SourcePrincipalID),
		SourcePrincipalKind: string(in.SourcePrincipalKind), SourceChannel: in.SourceChannel,
	}
	correlationID := in.correlationID
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return err
	}
	for _, event := range stream {
		if event.EventType != eventType || core.ID(event.TaskID) != taskID {
			continue
		}
		var recorded events.OperatorWorkAcceptedPayload
		if event.SourceActorID != string(in.SourcePrincipalID) || json.Unmarshal(event.Payload, &recorded) != nil || recorded != payload {
			return fmt.Errorf("durable operator acceptance conflicts with submission")
		}
		return nil
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: in.OrganizationID, EventType: eventType,
		SourceActorID: string(in.SourcePrincipalID), TaskID: string(taskID),
		CorrelationID: correlationID, Payload: payload,
	}); err != nil {
		return fmt.Errorf("persist operator work acceptance: %w", err)
	}
	return nil
}

func operatorWorkAcceptedEventType(channel string) (string, error) {
	switch channel {
	case "A2A":
		return "A2A_WORK_ACCEPTED", nil
	case "HUMAN_DIRECT":
		return "HUMAN_WORK_ACCEPTED", nil
	default:
		return "", fmt.Errorf("operator work channel is not supported")
	}
}

type taskRun struct {
	Outcome        core.ToolOutcome
	Completion     completion.Result
	ExecutionError error
}

func (s *Service) ensureSubmission(ctx context.Context, in Submit) (core.Intent, core.Goal, core.Task, error) {
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("load durable runtime state: %w", err)
	}
	now := time.Now().UTC()
	organizationID := core.ID(in.OrganizationID)
	correlationID := in.correlationID
	if existing, ok := snapshot.Organizations[organizationID]; !ok {
		organization := core.Organization{ID: organizationID, Name: in.OrganizationID, PolicyVersion: "v1", CreatedAt: now}
		if err := s.state.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", correlationID, 1, organization, nil); err != nil {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("persist organization: %w", err)
		}
		snapshot.Organizations[organizationID] = projections.Versioned[core.Organization]{Version: 1, CorrelationID: correlationID, Value: organization}
	} else if existing.Value.ID != organizationID {
		return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("organization projection mismatch")
	}

	agentID := core.ID("agent-local-" + in.OrganizationID)
	if existing, ok := snapshot.Agents[agentID]; !ok {
		agent := core.Agent{ID: agentID, OrganizationID: organizationID, BlueprintVersion: "v1-local-worker", ExecutionProfileVersion: "v1-fake", RuntimeAdapter: "local", Status: "ACTIVE"}
		if err := s.state.SaveAgent(ctx, "AGENT_CREATED", "runtime", correlationID, 1, agent, nil); err != nil {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("persist agent identity: %w", err)
		}
		snapshot.Agents[agentID] = projections.Versioned[core.Agent]{Version: 1, CorrelationID: correlationID, Value: agent}
	} else if existing.Value.OrganizationID != organizationID {
		return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("durable agent identity belongs to a different organization")
	}

	intent := core.Intent{ID: core.ID("intent-" + correlationID), OrganizationID: organizationID, OriginalInstruction: in.Statement, NormalizedObjective: in.Statement, HardConstraints: []string{}, ConsequenceBoundaries: []string{}, SourcePrincipalID: in.SourcePrincipalID, SourcePrincipalKind: in.SourcePrincipalKind, SourceChannel: in.SourceChannel, ExternalRequestID: in.RequestID, SourceMessageID: in.MessageID, CreatedAt: now}
	if in.SourcePrincipalKind == core.PrincipalHuman {
		intent.SourceHumanID = in.SourcePrincipalID
	}
	if existing, ok := snapshot.Intents[intent.ID]; ok {
		if existing.Value.OrganizationID != organizationID || existing.Value.OriginalInstruction != in.Statement || existing.Value.SourcePrincipalID != in.SourcePrincipalID || existing.Value.SourcePrincipalKind != in.SourcePrincipalKind || existing.Value.SourceChannel != in.SourceChannel || existing.Value.ExternalRequestID != in.RequestID || existing.Value.SourceMessageID != in.MessageID {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("request id is already bound to different work")
		}
		intent = existing.Value
	} else {
		if err := s.state.SaveIntent(ctx, "INTENT_CREATED", "runtime", correlationID, 1, intent, nil); err != nil {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("persist intent: %w", err)
		}
		snapshot.Intents[intent.ID] = projections.Versioned[core.Intent]{Version: 1, CorrelationID: correlationID, Value: intent}
	}

	goal := core.Goal{ID: core.ID("goal-" + correlationID), IntentID: intent.ID, Objective: in.Statement, Status: "ACTIVE", CreatedAt: now}
	if existing, ok := snapshot.Goals[goal.ID]; ok {
		if existing.Value.IntentID != intent.ID || existing.Value.Objective != in.Statement {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("request goal projection does not match submitted work")
		}
		goal = existing.Value
	} else {
		if err := s.state.SaveGoal(ctx, organizationID, "GOAL_CREATED", "runtime", correlationID, 1, goal, nil); err != nil {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("persist goal: %w", err)
		}
		snapshot.Goals[goal.ID] = projections.Versioned[core.Goal]{Version: 1, CorrelationID: correlationID, Value: goal}
	}

	policy := core.InferenceForbidden
	if in.Kind == core.ExecutionAgent {
		policy = core.InferenceAllowed
	}
	task := core.Task{
		ID:                   core.ID("task-" + correlationID),
		GoalID:               goal.ID,
		Description:          in.Statement,
		ExecutionKind:        in.Kind,
		ModelInferencePolicy: policy,
		AssigneeType:         "AGENT",
		AssigneeID:           agentID,
		TaskContractVersion:  "1",
		Status:               core.TaskPending,
	}
	if in.Kind == core.ExecutionHuman && in.SourcePrincipalKind == core.PrincipalHuman {
		task.CompletionContract = &core.CompletionContract{
			TaskID: task.ID, TaskVersion: 1,
			RequiredFields: []core.CompletionFieldRequirement{{
				Name: "response", Description: "the information requested by the user task", MinBytes: 1, MaxBytes: 64 << 10,
			}},
		}
	}
	if existing, ok := snapshot.Tasks[task.ID]; ok {
		if existing.Value.GoalID != goal.ID || existing.Value.Description != in.Statement || existing.Value.ExecutionKind != in.Kind {
			return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("request task projection does not match submitted work")
		}
		task = existing.Value
	} else if err := s.state.SaveTask(ctx, organizationID, "TASK_CREATED", "runtime", correlationID, 1, task, nil); err != nil {
		return core.Intent{}, core.Goal{}, core.Task{}, fmt.Errorf("persist task before scheduling: %w", err)
	}
	return intent, goal, task, nil
}

func (s *Service) runReady(ctx context.Context) (map[core.ID]taskRun, error) {
	runs := make(map[core.ID]taskRun)
	for {
		snapshot, err := s.state.Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("load scheduler state: %w", err)
		}
		tasks := make(map[core.ID]core.Task, len(snapshot.Tasks))
		for id, state := range snapshot.Tasks {
			tasks[id] = state.Value
		}
		ready, err := s.scheduler.Ready(tasks)
		if err != nil {
			return nil, fmt.Errorf("validate durable task graph: %w", err)
		}
		remediation := false
		if len(ready) == 0 {
			ready, err = s.scheduler.RemediationReady(tasks)
			if err != nil {
				return nil, fmt.Errorf("validate durable remediation graph: %w", err)
			}
			if len(ready) == 0 {
				return runs, nil
			}
			remediation = true
		}
		for _, task := range ready {
			state := snapshot.Tasks[task.ID]
			run, err := s.executeTask(ctx, snapshot, state, remediation)
			if err != nil {
				return nil, err
			}
			runs[task.ID] = run
		}
	}
}

func (s *Service) executeTask(ctx context.Context, snapshot projections.Snapshot, state projections.Versioned[core.Task], remediation bool) (taskRun, error) {
	task := state.Value
	organizationID, err := taskOrganization(snapshot, task)
	if err != nil {
		return taskRun{}, err
	}
	var handler execution.Handler
	switch task.ExecutionKind {
	case core.ExecutionDeterministic:
		handler = s.deterministic
	case core.ExecutionAgent:
		handler = s.agent
	case core.ExecutionHuman:
		task.Status = core.TaskBlocked
		detail := blockedDetail("user task is awaiting structured completion", "every field and artifact required by its CompletionContract", "the runtime cannot invent, infer, or waive required user evidence")
		if err := s.saveBlockedTask(ctx, snapshot, state, organizationID, task, detail); err != nil {
			return taskRun{}, fmt.Errorf("persist input-required user task %s: %w", task.ID, err)
		}
		return taskRun{}, nil
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed:
		task.Status = core.TaskBlocked
		detail := blockedDetail("execution kind is declared but unavailable in this V1 slice", "authorized runtime handler", "the worker cannot expand its own execution authority")
		if err := s.saveBlockedTask(ctx, snapshot, state, organizationID, task, detail); err != nil {
			return taskRun{}, fmt.Errorf("persist blocked task %s: %w", task.ID, err)
		}
		return taskRun{}, nil
	default:
		task.Status = core.TaskBlocked
		detail := blockedDetail("execution kind is unknown and unavailable", "recognized authorized runtime handler", "the worker cannot expand its own execution authority")
		if err := s.saveBlockedTask(ctx, snapshot, state, organizationID, task, detail); err != nil {
			return taskRun{}, fmt.Errorf("persist blocked task %s: %w", task.ID, err)
		}
		return taskRun{}, nil
	}

	task.Status = core.TaskRunning
	var startDetail any
	if remediation {
		startDetail = map[string]any{"mode": "BLOCKED_CHILD_REMEDIATION"}
	}
	if err := s.state.SaveTask(ctx, organizationID, "EXECUTION_STARTED", "runtime", state.CorrelationID, state.Version+1, task, startDetail); err != nil {
		return taskRun{}, fmt.Errorf("persist execution start for task %s: %w", task.ID, err)
	}
	executionID := core.ID(fmt.Sprintf("execution-%s-v%d", task.ID, state.Version+1))
	manifest := core.ExecutionContextManifest{}
	executionTask := task
	var inboxBatches []inboxBatch
	if task.ExecutionKind == core.ExecutionAgent {
		inboxBatches, err = s.actionBoundaryInbox(ctx, snapshot, task)
		if err != nil {
			return taskRun{}, fmt.Errorf("load action-boundary inbox for task %s: %w", task.ID, err)
		}
		eventRefs := inboxEventRefs(inboxBatches)
		stream, err := s.gateway.Events(ctx, state.CorrelationID)
		if err != nil {
			return taskRun{}, fmt.Errorf("load completion revision context for task %s: %w", task.ID, err)
		}
		revision, revisionEvent, hasRevision, err := latestRevision(stream, task.ID)
		if err != nil {
			return taskRun{}, fmt.Errorf("validate completion revision context for task %s: %w", task.ID, err)
		}
		if hasRevision {
			eventRefs = append(eventRefs, revisionEvent.EventID)
		}
		manifest = core.ExecutionContextManifest{
			ExecutionID:             executionID,
			AgentID:                 task.AssigneeID,
			ExecutionProfileVersion: s.agentModel.ExecutionProfileVersion,
			Provider:                s.agentModel.Provider,
			Model:                   s.agentModel.Model,
			TaskID:                  task.ID,
			TaskContractVersion:     task.TaskContractVersion,
			PromptVersion:           "v1",
			PolicyVersion:           "v1",
			EventRefs:               eventRefs,
			KnowledgeRefs:           []core.VersionedRef{},
			SkillRefs:               []core.VersionedRef{},
			ToolDefinitions:         []core.VersionedRef{},
			ArtifactRefs:            []core.VersionedRef{},
			AdditionalContextRefs:   []core.VersionedRef{},
			ContextBuilderVersion:   "v1",
			CreatedAt:               time.Now().UTC(),
		}
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "EXECUTION_CONTEXT_MANIFESTED", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: manifest, CorrelationID: state.CorrelationID}); err != nil {
			return taskRun{}, fmt.Errorf("persist execution context for task %s: %w", task.ID, err)
		}
		if len(eventRefs) > 0 {
			executionTask.Description, err = materializeInboxContext(task.Description, inboxBatches)
			if err != nil {
				return taskRun{}, fmt.Errorf("materialize action-boundary messages for task %s: %w", task.ID, err)
			}
		}
		if hasRevision {
			executionTask.Description, err = materializeRevisionContext(executionTask.Description, revision, revisionEvent.EventID)
			if err != nil {
				return taskRun{}, fmt.Errorf("materialize completion revision for task %s: %w", task.ID, err)
			}
		}
	}

	executionResult, executionErr := handler.Execute(ctx, executionTask, manifest)
	outcome, verifierAvailable := s.verifier.Verify(executionTask, executionResult.Outcome)
	outcomeEvent, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "TOOL_OUTCOME_RECORDED", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: outcome, CorrelationID: state.CorrelationID})
	if err != nil {
		return taskRun{}, fmt.Errorf("persist outcome for task %s: %w", task.ID, err)
	}
	if executionResult.InferenceUsage != nil {
		if !executionResult.InferenceUsage.Valid() {
			return taskRun{}, fmt.Errorf("model execution for task %s returned invalid usage telemetry", task.ID)
		}
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "INFERENCE_USAGE_RECORDED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: executionResult.InferenceUsage, CorrelationID: state.CorrelationID}); err != nil {
			return taskRun{}, fmt.Errorf("persist inference usage for task %s: %w", task.ID, err)
		}
	}
	for _, batch := range inboxBatches {
		if len(batch.Events) == 0 {
			continue
		}
		if _, err := s.gateway.ObserveInbox(ctx, string(organizationID), string(task.AssigneeID), string(executionID), string(task.ID), state.CorrelationID, batch.Scope, batch.ID, eventIDs(batch.Events)); err != nil {
			return taskRun{}, fmt.Errorf("persist inbox observation for task %s: %w", task.ID, err)
		}
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "EXECUTION_FINISHED", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: map[string]any{"status": outcome.Status}, CorrelationID: state.CorrelationID}); err != nil {
		return taskRun{}, fmt.Errorf("persist execution finish for task %s: %w", task.ID, err)
	}
	if remediation {
		task.Status = core.TaskBlocked
		detail := events.TaskBlockedPayload{
			Reason:        "a direct child remains blocked after the parent remediation pass",
			Missing:       "an authorized remediation decision for the blocked child",
			WhyNeeded:     "a blocked dependency cannot be treated as completed or gain authority automatically",
			WorkCompleted: "the parent observed the blocked-work event and completed a bounded remediation pass",
		}
		running := projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: state.CorrelationID, Value: task}
		if err := s.saveBlockedTask(ctx, snapshot, running, organizationID, task, detail); err != nil {
			return taskRun{}, fmt.Errorf("persist remediation-required parent task %s: %w", task.ID, err)
		}
		return taskRun{Outcome: outcome, ExecutionError: executionErr}, nil
	}
	resultEvent, err := s.publishTaskResult(ctx, organizationID, state.CorrelationID, executionID, task, outcome)
	if err != nil {
		return taskRun{}, err
	}
	candidatePayload := map[string]any{"tool_invocation_id": outcome.ToolInvocationID, "result_event_id": resultEvent.EventID, "artifact_refs": outcome.ArtifactRefs}
	candidate := events.TrustedDraft{OrganizationID: string(organizationID), EventType: "CANDIDATE_COMPLETE", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: candidatePayload, CorrelationID: state.CorrelationID}
	var candidateEvent events.Event
	if task.ExecutionKind == core.ExecutionAgent {
		candidateEvent, err = s.gateway.PublishAgentDraft(ctx, string(organizationID), string(task.AssigneeID), string(executionID), state.CorrelationID, events.Draft{EventType: "CANDIDATE_COMPLETE", TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: candidate.Payload})
		if err != nil {
			return taskRun{}, fmt.Errorf("persist completion candidate for task %s: %w", task.ID, err)
		}
	} else {
		candidateEvent, err = s.gateway.PublishTrusted(ctx, candidate)
		if err != nil {
			return taskRun{}, fmt.Errorf("persist completion candidate for task %s: %w", task.ID, err)
		}
	}

	assurance := core.AssuranceDeterministic
	criterionID := "verified-outcome"
	criterionDescription := "work produced a verified successful outcome"
	if task.ExecutionKind == core.ExecutionAgent && outcome.Status == core.OutcomeSucceeded && !verifierAvailable {
		assurance = core.AssuranceHumanJudgment
		criterionID = "reviewed-outcome"
		criterionDescription = "an authorized reviewer judged the recorded candidate against the task objective"
	}
	contract := core.CompletionContract{TaskID: task.ID, TaskVersion: state.Version + 1, Criteria: []core.CompletionCriterion{{ID: criterionID, Description: criterionDescription, Assurance: assurance, Required: true}}}
	complete := s.completion.Evaluate(contract, outcome)
	detail := completionDetail{Contract: contract, Result: complete}
	if complete.Complete {
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "COMPLETION_VERIFIED", TaskID: string(task.ID), Payload: detail, CorrelationID: state.CorrelationID}); err != nil {
			return taskRun{}, fmt.Errorf("persist completion verification for task %s: %w", task.ID, err)
		}
		task.Status = core.TaskCompleted
		if err := s.state.SaveTask(ctx, organizationID, "TASK_VERIFIED_COMPLETE", "runtime", state.CorrelationID, state.Version+2, task, detail); err != nil {
			return taskRun{}, fmt.Errorf("persist completed task %s: %w", task.ID, err)
		}
	} else if task.ExecutionKind == core.ExecutionAgent && outcome.Status == core.OutcomeSucceeded && !verifierAvailable {
		request, err := completion.NewReviewRequest(organizationID, task.ID, contract.TaskVersion, task.Description, contract, []string{outcomeEvent.EventID, resultEvent.EventID, candidateEvent.EventID}, time.Now().UTC())
		if err != nil {
			return taskRun{}, fmt.Errorf("build completion review request for task %s: %w", task.ID, err)
		}
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "COMPLETION_REVIEW_REQUESTED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: request, CorrelationID: state.CorrelationID}); err != nil {
			return taskRun{}, fmt.Errorf("persist completion review requirement for task %s: %w", task.ID, err)
		}
		task.Status = core.TaskBlocked
		blocked := events.TaskBlockedPayload{
			Reason:        "the configured model produced a candidate without an independent completion verifier",
			Missing:       "authorized independent completion judgment against the task contract",
			WhyNeeded:     "model output is work content and cannot certify its own completion",
			WorkCompleted: "the provider output, runtime outcome, and completion candidate were durably recorded",
			RemainingWork: "evaluate the recorded candidate using an approved independent or user judgment path",
			EvidenceRefs:  append([]string(nil), request.EvidenceRefs...),
		}
		running := projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: state.CorrelationID, Value: task}
		if err := s.saveBlockedTask(ctx, snapshot, running, organizationID, task, blocked); err != nil {
			return taskRun{}, fmt.Errorf("persist completion-review block for task %s: %w", task.ID, err)
		}
	} else {
		task.Status = core.TaskFailed
		if err := s.state.SaveTask(ctx, organizationID, "COMPLETION_REJECTED", "runtime", state.CorrelationID, state.Version+2, task, detail); err != nil {
			return taskRun{}, fmt.Errorf("persist failed task %s: %w", task.ID, err)
		}
	}
	return taskRun{Outcome: outcome, Completion: complete, ExecutionError: executionErr}, nil
}

func (s *Service) publishTaskResult(ctx context.Context, organizationID core.ID, correlationID string, executionID core.ID, task core.Task, outcome core.ToolOutcome) (events.Event, error) {
	summary, err := outcomeSummary(outcome)
	if err != nil {
		return events.Event{}, fmt.Errorf("materialize result summary for task %s: %w", task.ID, err)
	}
	payload := events.ResultPublishedPayload{Summary: summary, ArtifactRefs: outcome.ArtifactRefs}
	if task.ExecutionKind == core.ExecutionAgent {
		event, err := s.gateway.PublishAgentDraft(ctx, string(organizationID), string(task.AssigneeID), string(executionID), correlationID, events.Draft{EventType: "RESULT_PUBLISHED", TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: payload})
		if err != nil {
			return events.Event{}, fmt.Errorf("persist agent result for task %s: %w", task.ID, err)
		}
		return event, nil
	}
	event, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(organizationID), EventType: "RESULT_PUBLISHED", SourceActorID: "runtime", SourceExecutionID: string(executionID), TaskID: string(task.ID), ArtifactRefs: outcome.ArtifactRefs, Payload: payload, CorrelationID: correlationID})
	if err != nil {
		return events.Event{}, fmt.Errorf("persist runtime result for task %s: %w", task.ID, err)
	}
	return event, nil
}

func outcomeSummary(outcome core.ToolOutcome) (string, error) {
	if summary, ok := outcome.ObservedEffect.(string); ok && summary != "" {
		return summary, nil
	}
	if fields, ok := outcome.ObservedEffect.(map[string]string); ok && fields["status"] != "" {
		return fields["status"], nil
	}
	if fields, ok := outcome.ObservedEffect.(map[string]any); ok {
		if summary, ok := fields["status"].(string); ok && summary != "" {
			return summary, nil
		}
	}
	if outcome.ObservedEffect != nil {
		encoded, err := json.Marshal(outcome.ObservedEffect)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
	if outcome.ErrorDetail != "" {
		return outcome.ErrorDetail, nil
	}
	return fmt.Sprintf("task outcome: %s", outcome.Status), nil
}

type completionDetail struct {
	Contract    core.CompletionContract `json:"contract"`
	Result      completion.Result       `json:"result"`
	JudgmentRef string                  `json:"judgment_ref,omitempty"`
}

func (s *Service) reconcileGoals(ctx context.Context) error {
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return err
	}
	goalIDs := make([]core.ID, 0, len(snapshot.Goals))
	for id := range snapshot.Goals {
		goalIDs = append(goalIDs, id)
	}
	sort.Slice(goalIDs, func(i, j int) bool { return goalIDs[i] < goalIDs[j] })
	for _, goalID := range goalIDs {
		state := snapshot.Goals[goalID]
		if state.Value.Status != "ACTIVE" {
			continue
		}
		hasTasks := false
		allComplete := true
		allTerminal := true
		correlationID := state.CorrelationID
		for _, task := range snapshot.Tasks {
			if task.Value.GoalID != goalID {
				continue
			}
			hasTasks = true
			if task.Value.Status != core.TaskCompleted {
				allComplete = false
			}
			if task.Value.Status != core.TaskCompleted && task.Value.Status != core.TaskFailed {
				allTerminal = false
			}
			correlationID = task.CorrelationID
		}
		if !hasTasks || !allTerminal {
			continue
		}
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("goal %s references missing intent %s", goalID, state.Value.IntentID)
		}
		goal := state.Value
		stream, err := s.gateway.Events(ctx, correlationID)
		if err != nil {
			return fmt.Errorf("load run telemetry source for goal %s: %w", goalID, err)
		}
		recordedRun, telemetryRecorded, err := telemetry.Recorded(stream)
		if err != nil {
			return fmt.Errorf("validate recorded run telemetry for goal %s: %w", goalID, err)
		}
		if telemetryRecorded && (recordedRun.CorrelationID != correlationID || recordedRun.OrganizationID != string(intent.Value.OrganizationID)) {
			return fmt.Errorf("recorded run telemetry for goal %s crosses its trust boundary", goalID)
		}
		if !telemetryRecorded {
			run, err := telemetry.Project(correlationID, stream)
			if err != nil {
				return fmt.Errorf("project run telemetry for goal %s: %w", goalID, err)
			}
			if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(intent.Value.OrganizationID), EventType: "RUN_TELEMETRY_RECORDED", SourceActorID: "runtime", Payload: run, CorrelationID: correlationID}); err != nil {
				return fmt.Errorf("persist run telemetry for goal %s: %w", goalID, err)
			}
		}
		goalEventType := "GOAL_COMPLETED"
		goal.Status = "COMPLETED"
		if !allComplete {
			goalEventType = "GOAL_FAILED"
			goal.Status = "FAILED"
		}
		if err := s.state.SaveGoal(ctx, intent.Value.OrganizationID, goalEventType, "runtime", correlationID, state.Version+1, goal, nil); err != nil {
			return fmt.Errorf("persist terminal goal %s: %w", goalID, err)
		}
	}
	return nil
}

func (s *Service) readTaskResult(ctx context.Context, correlationID string) (taskRun, error) {
	stream, err := s.gateway.Events(ctx, correlationID)
	if err != nil {
		return taskRun{}, err
	}
	var result taskRun
	for _, event := range stream {
		switch event.EventType {
		case "TOOL_OUTCOME_RECORDED":
			if err := json.Unmarshal(event.Payload, &result.Outcome); err != nil {
				return taskRun{}, err
			}
		case "COMPLETION_VERIFIED":
			var detail completionDetail
			if err := json.Unmarshal(event.Payload, &detail); err != nil {
				return taskRun{}, err
			}
			result.Completion = detail.Result
		case "COMPLETION_REVIEW_REQUESTED":
			var request completion.ReviewRequest
			if err := json.Unmarshal(event.Payload, &request); err != nil || !request.Valid() {
				return taskRun{}, fmt.Errorf("invalid completion review request")
			}
			result.Completion = s.completion.Evaluate(request.Contract, result.Outcome)
		case "COMPLETION_REJECTED":
			var payload events.ProjectionEventPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return taskRun{}, err
			}
			var detail completionDetail
			if err := json.Unmarshal(payload.Detail, &detail); err != nil {
				return taskRun{}, err
			}
			result.Completion = detail.Result
		}
	}
	return result, nil
}

func taskOrganization(snapshot projections.Snapshot, task core.Task) (core.ID, error) {
	goal, ok := snapshot.Goals[task.GoalID]
	if !ok {
		return "", fmt.Errorf("task %s references missing goal %s", task.ID, task.GoalID)
	}
	intent, ok := snapshot.Intents[goal.Value.IntentID]
	if !ok {
		return "", fmt.Errorf("goal %s references missing intent %s", goal.Value.ID, goal.Value.IntentID)
	}
	if _, ok := snapshot.Organizations[intent.Value.OrganizationID]; !ok {
		return "", fmt.Errorf("intent %s references missing organization %s", intent.Value.ID, intent.Value.OrganizationID)
	}
	return intent.Value.OrganizationID, nil
}

func sortedTaskStates(tasks map[core.ID]projections.Versioned[core.Task]) []projections.Versioned[core.Task] {
	states := make([]projections.Versioned[core.Task], 0, len(tasks))
	for _, state := range tasks {
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Value.ID < states[j].Value.ID })
	return states
}

func (s *Service) saveBlockedTask(ctx context.Context, snapshot projections.Snapshot, previous projections.Versioned[core.Task], organizationID core.ID, blocked core.Task, detail events.TaskBlockedPayload) error {
	if blocked.ParentID == "" {
		return s.state.SaveTask(ctx, organizationID, "TASK_BLOCKED", "runtime", previous.CorrelationID, previous.Version+1, blocked, detail)
	}
	parent, ok := snapshot.Tasks[blocked.ParentID]
	if !ok || parent.Value.GoalID != blocked.GoalID {
		return fmt.Errorf("blocked child task %s references invalid parent %s", blocked.ID, blocked.ParentID)
	}
	return s.state.SaveBlockedTask(ctx, organizationID, "runtime", previous.CorrelationID, previous.Version+1, blocked, detail, parent.Value.ID)
}

func blockedDetail(reason, missing, whyNeeded string) events.TaskBlockedPayload {
	return events.TaskBlockedPayload{Reason: reason, Missing: missing, WhyNeeded: whyNeeded, WorkCompleted: "none"}
}

type inboxBatch struct {
	Scope  string
	ID     string
	Events []events.Event
}

func (s *Service) actionBoundaryInbox(ctx context.Context, snapshot projections.Snapshot, task core.Task) ([]inboxBatch, error) {
	routes := []struct{ scope, id string }{{events.RecipientTask, string(task.ID)}}
	switch task.AssigneeType {
	case "AGENT":
		routes = append(routes, struct{ scope, id string }{events.RecipientAgent, string(task.AssigneeID)})
		teamIDs := make([]core.ID, 0)
		for teamID, team := range snapshot.Teams {
			for _, memberID := range team.Value.MemberAgentIDs {
				if memberID == task.AssigneeID {
					teamIDs = append(teamIDs, teamID)
					break
				}
			}
		}
		sort.Slice(teamIDs, func(i, j int) bool { return teamIDs[i] < teamIDs[j] })
		for _, teamID := range teamIDs {
			routes = append(routes, struct{ scope, id string }{events.RecipientTeam, string(teamID)})
		}
	case "TEAM":
		routes = append(routes, struct{ scope, id string }{events.RecipientTeam, string(task.AssigneeID)})
	}
	batches := make([]inboxBatch, 0, len(routes))
	for _, route := range routes {
		available, err := s.gateway.Inbox(ctx, route.scope, route.id)
		if err != nil {
			return nil, err
		}
		batches = append(batches, inboxBatch{Scope: route.scope, ID: route.id, Events: available})
	}
	return batches, nil
}

func inboxEventRefs(batches []inboxBatch) []string {
	stream := sortedInboxEvents(batches)
	refs := make([]string, 0, len(stream))
	for _, event := range stream {
		refs = append(refs, event.EventID)
	}
	return refs
}

func eventIDs(stream []events.Event) []string {
	ids := make([]string, 0, len(stream))
	for _, event := range stream {
		ids = append(ids, event.EventID)
	}
	return ids
}

func materializeInboxContext(objective string, batches []inboxBatch) (string, error) {
	type eventView struct {
		EventID        string          `json:"event_id"`
		EventType      string          `json:"event_type"`
		SourceActorID  string          `json:"source_actor_id"`
		RecipientScope string          `json:"recipient_scope"`
		RecipientID    string          `json:"recipient_id"`
		TaskID         string          `json:"task_id,omitempty"`
		CreatedAt      time.Time       `json:"created_at"`
		Payload        json.RawMessage `json:"payload"`
	}
	stream := sortedInboxEvents(batches)
	available := make([]eventView, 0, len(stream))
	for _, event := range stream {
		available = append(available, eventView{
			EventID:        event.EventID,
			EventType:      event.EventType,
			SourceActorID:  event.SourceActorID,
			RecipientScope: event.RecipientScope,
			RecipientID:    event.RecipientID,
			TaskID:         event.TaskID,
			CreatedAt:      event.CreatedAt,
			Payload:        event.Payload,
		})
	}
	contextView := struct {
		Objective string      `json:"objective"`
		Events    []eventView `json:"events"`
	}{Objective: objective, Events: available}
	encoded, err := json.Marshal(contextView)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func materializeRevisionContext(objective string, review completion.HumanReview, eventRef string) (string, error) {
	contextView := struct {
		Objective string `json:"objective"`
		Revision  struct {
			EventRef      string  `json:"event_ref"`
			ReviewerID    core.ID `json:"reviewer_id"`
			UntrustedText string  `json:"untrusted_text"`
		} `json:"completion_revision"`
	}{Objective: objective}
	contextView.Revision.EventRef = eventRef
	contextView.Revision.ReviewerID = review.ReviewerID
	contextView.Revision.UntrustedText = review.Feedback
	encoded, err := json.Marshal(contextView)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func sortedInboxEvents(batches []inboxBatch) []events.Event {
	var stream []events.Event
	for _, batch := range batches {
		stream = append(stream, batch.Events...)
	}
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	return stream
}

func agentParticipates(snapshot projections.Snapshot, agentID core.ID, task core.Task) bool {
	switch task.AssigneeType {
	case "AGENT":
		return task.AssigneeID == agentID
	case "TEAM":
		team, ok := snapshot.Teams[task.AssigneeID]
		if !ok {
			return false
		}
		for _, memberID := range team.Value.MemberAgentIDs {
			if memberID == agentID {
				return true
			}
		}
	}
	return false
}
