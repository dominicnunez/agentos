package app

import (
	"context"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
)

type Submit struct {
	RequestID      string
	OrganizationID string
	Statement      string
	Kind           core.ExecutionKind
}
type Result struct {
	Intent     core.Intent       `json:"intent"`
	Goal       core.Goal         `json:"goal"`
	Task       core.Task         `json:"task"`
	Outcome    core.ToolOutcome  `json:"outcome"`
	Completion completion.Result `json:"completion"`
	Events     []events.Event    `json:"events"`
}
type Service struct {
	gateway       *events.Gateway
	deterministic execution.Handler
	agent         execution.Handler
	completion    completion.Engine
}

func New(g *events.Gateway) *Service {
	return &Service{gateway: g, deterministic: execution.Deterministic{}, agent: execution.NewAgentExecution(execution.FakeModel{})}
}

func (s *Service) Events(ctx context.Context, requestID string) ([]events.Event, error) {
	return s.gateway.Events(ctx, requestID)
}

func (s *Service) ProvideExternalInput(ctx context.Context, organizationID, actorID, requestID, taskID, text string) error {
	if organizationID == "" || actorID == "" || requestID == "" || taskID == "" || text == "" {
		return fmt.Errorf("organization, actor, request, task, and text are required")
	}
	es, err := s.gateway.Events(ctx, requestID)
	if err != nil {
		return err
	}
	matched := false
	for _, e := range es {
		if e.OrganizationID == organizationID && e.TaskID == taskID {
			matched = true
			break
		}
	}
	if !matched {
		return fmt.Errorf("task is not mapped to this external request and organization")
	}
	_, err = s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: organizationID, EventType: "A2A_INPUT_RECEIVED", SourceActorID: actorID, TaskID: taskID, CorrelationID: requestID, Payload: map[string]string{"text": text, "source_external_actor": actorID}})
	return err
}

func (s *Service) Submit(ctx context.Context, in Submit) (Result, error) {
	if in.RequestID == "" || in.OrganizationID == "" || in.Statement == "" {
		return Result{}, fmt.Errorf("request_id, organization_id, and statement are required")
	}
	now := time.Now().UTC()
	corr := in.RequestID
	intent := core.Intent{ID: core.ID("intent-" + corr), OrganizationID: core.ID(in.OrganizationID), OriginalInstruction: in.Statement, NormalizedObjective: in.Statement, HardConstraints: []string{}, ConsequenceBoundaries: []string{}, CreatedAt: now}
	goal := core.Goal{ID: core.ID("goal-" + corr), IntentID: intent.ID, Objective: in.Statement, Status: "ACTIVE", CreatedAt: now}
	policy := core.InferenceForbidden
	if in.Kind == core.ExecutionAgent {
		policy = core.InferenceRequired
	}
	task := core.Task{ID: core.ID("task-" + corr), GoalID: goal.ID, Description: in.Statement, ExecutionKind: in.Kind, ModelInferencePolicy: policy, TaskContractVersion: "1", Status: core.TaskPending}
	for _, d := range []events.TrustedDraft{{OrganizationID: in.OrganizationID, EventType: "INTENT_CREATED", Payload: intent, CorrelationID: corr}, {OrganizationID: in.OrganizationID, EventType: "GOAL_CREATED", Payload: goal, CorrelationID: corr}, {OrganizationID: in.OrganizationID, EventType: "TASK_CREATED", TaskID: string(task.ID), Payload: task, CorrelationID: corr}} {
		if _, err := s.gateway.PublishTrusted(ctx, d); err != nil {
			return Result{}, err
		}
	}
	task.Status = core.TaskRunning
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: "EXECUTION_STARTED", TaskID: string(task.ID), Payload: task, CorrelationID: corr}); err != nil {
		return Result{}, err
	}
	executionID := core.ID("execution-" + corr)
	manifest := core.ExecutionContextManifest{ExecutionID: executionID, AgentID: "runtime", ExecutionProfileVersion: "v1-fake", Provider: "fake", Model: "fake-model/v1", TaskID: task.ID, TaskContractVersion: "1", PromptVersion: "v1", PolicyVersion: "v4.2", EventRefs: []string{}, KnowledgeRefs: []core.VersionedRef{}, SkillRefs: []core.VersionedRef{}, ToolDefinitions: []core.VersionedRef{}, ContextBuilderVersion: "v1", CreatedAt: time.Now().UTC()}
	if in.Kind == core.ExecutionAgent {
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: "EXECUTION_CONTEXT_MANIFESTED", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: manifest, CorrelationID: corr}); err != nil {
			return Result{}, err
		}
	}
	var handler execution.Handler
	switch in.Kind {
	case core.ExecutionDeterministic:
		handler = s.deterministic
	case core.ExecutionAgent:
		handler = s.agent
	default:
		task.Status = core.TaskBlocked
		return Result{}, fmt.Errorf("execution kind %s is declared but not implemented in this slice", in.Kind)
	}
	outcome, execErr := handler.Execute(ctx, task, manifest)
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: "TOOL_OUTCOME_RECORDED", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: outcome, CorrelationID: corr}); err != nil {
		return Result{}, err
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: "EXECUTION_FINISHED", SourceExecutionID: string(executionID), TaskID: string(task.ID), Payload: map[string]any{"status": outcome.Status}, CorrelationID: corr}); err != nil {
		return Result{}, err
	}
	if _, err := s.gateway.PublishAgentDraft(ctx, in.OrganizationID, "runtime", string(executionID), corr, events.Draft{EventType: "CANDIDATE_COMPLETE", TaskID: string(task.ID), Payload: map[string]any{"tool_invocation_id": outcome.ToolInvocationID}}); err != nil {
		return Result{}, err
	}
	contract := core.CompletionContract{TaskID: task.ID, TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "verified-outcome", Description: "work produced a verified successful outcome", Assurance: core.AssuranceDeterministic, Required: true}}}
	complete := s.completion.Evaluate(contract, outcome)
	if complete.Complete {
		task.Status = core.TaskCompleted
	} else {
		task.Status = core.TaskFailed
	}
	eventType := "COMPLETION_REJECTED"
	if complete.Complete {
		eventType = "COMPLETION_VERIFIED"
	}
	if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: eventType, TaskID: string(task.ID), Payload: struct {
		Task     core.Task               `json:"task"`
		Contract core.CompletionContract `json:"contract"`
		Result   completion.Result       `json:"result"`
	}{task, contract, complete}, CorrelationID: corr}); err != nil {
		return Result{}, err
	}
	if complete.Complete {
		if _, err := s.gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: in.OrganizationID, EventType: "TASK_VERIFIED_COMPLETE", TaskID: string(task.ID), Payload: task, CorrelationID: corr}); err != nil {
			return Result{}, err
		}
	}
	evts, err := s.gateway.Events(ctx, corr)
	if err != nil {
		return Result{}, err
	}
	return Result{Intent: intent, Goal: goal, Task: task, Outcome: outcome, Completion: complete, Events: evts}, execErr
}
