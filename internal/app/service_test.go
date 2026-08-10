package app

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestVerticalSlice(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	s := New(events.NewGateway(l))
	r, err := s.Submit(context.Background(), Submit{RequestID: "r1", OrganizationID: "o1", Statement: "echo hello", Kind: core.ExecutionDeterministic})
	if err != nil {
		t.Fatal(err)
	}
	if !r.Completion.Complete || r.Task.Status != core.TaskCompleted || r.Goal.Status != "COMPLETED" {
		t.Fatalf("unexpected result: %#v", r)
	}
	assertEventOrder(t, r.Events, "TASK_CREATED", "EXECUTION_STARTED", "TOOL_OUTCOME_RECORDED", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE")
}
func TestAgentExecutionUsesFakeAdapter(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	r, err := New(events.NewGateway(l)).Submit(context.Background(), Submit{RequestID: "r2", OrganizationID: "o1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome.ObservedEffect != "fake-model: summarize" {
		t.Fatalf("effect=%q", r.Outcome.ObservedEffect)
	}
}

func TestRecoverExecutesPersistedPendingWorkAndPreservesIdentity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	g := events.NewGateway(l)
	repository := projections.New(g)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "echo after restart", NormalizedObjective: "echo after restart"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "echo after restart", Status: "ACTIVE"}
	first := core.Task{ID: "task-1", GoalID: goal.ID, Description: "echo already done", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskCompleted}
	second := core.Task{ID: "task-2", GoalID: goal.ID, Description: "echo after restart", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, DependsOn: []core.ID{first.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error { return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, agent, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, first, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, second, nil)
		},
	} {
		if err := save(); err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service := New(events.NewGateway(l))
	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.PendingFound != 1 || recovery.TasksExecuted != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Agents[agent.ID].Value != agent {
		t.Fatalf("agent identity changed across restart: %+v", snapshot.Agents[agent.ID].Value)
	}
	if snapshot.Tasks[second.ID].Value.Status != core.TaskCompleted || snapshot.Goals[goal.ID].Value.Status != "COMPLETED" {
		t.Fatalf("pending work not recovered: task=%+v goal=%+v", snapshot.Tasks[second.ID].Value, snapshot.Goals[goal.ID].Value)
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
			t.Fatal("deterministic work owned by a durable agent created model execution context")
		}
	}
}

func TestTaskPersistenceFailurePreventsExecutionVisibility(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	store := failTaskProjection{SQLite: l}
	_, err = New(events.NewGateway(store)).Submit(ctx, Submit{RequestID: "request-1", OrganizationID: "org-1", Statement: "echo must not run", Kind: core.ExecutionDeterministic})
	if err == nil || !errors.Is(err, errProjectionWrite) {
		t.Fatalf("submit error=%v", err)
	}
	stream, readErr := l.Events(ctx, "request-1")
	if readErr != nil {
		t.Fatal(readErr)
	}
	for _, event := range stream {
		if event.EventType == "EXECUTION_STARTED" || event.EventType == "TOOL_OUTCOME_RECORDED" {
			t.Fatalf("unpersisted task became executable: %+v", event)
		}
	}
	records, err := l.Records(ctx, projections.KindTask, "")
	if err != nil || len(records) != 0 {
		t.Fatalf("task projection became visible: records=%d err=%v", len(records), err)
	}
}

func TestRecoverRetriesDeterministicWorkAndBlocksUncertainAgentWork(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	repository := projections.New(events.NewGateway(l))
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "bootstrap", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "bootstrap", 1, agent, nil); err != nil {
		t.Fatal(err)
	}
	seedRunning := func(requestID string, kind core.ExecutionKind, statement string) core.Task {
		t.Helper()
		intent := core.Intent{ID: core.ID("intent-" + requestID), OrganizationID: organization.ID, OriginalInstruction: statement, NormalizedObjective: statement}
		goal := core.Goal{ID: core.ID("goal-" + requestID), IntentID: intent.ID, Objective: statement, Status: "ACTIVE"}
		policy := core.InferenceForbidden
		if kind == core.ExecutionAgent {
			policy = core.InferenceAllowed
		}
		task := core.Task{ID: core.ID("task-" + requestID), GoalID: goal.ID, Description: statement, ExecutionKind: kind, ModelInferencePolicy: policy, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskRunning}
		for _, save := range []func() error{
			func() error {
				return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", requestID, 1, intent, nil)
			},
			func() error {
				return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", requestID, 1, goal, nil)
			},
			func() error {
				return repository.SaveTask(ctx, organization.ID, "EXECUTION_STARTED", "runtime", requestID, 1, task, nil)
			},
		} {
			if err := save(); err != nil {
				t.Fatal(err)
			}
		}
		return task
	}
	deterministicTask := seedRunning("deterministic", core.ExecutionDeterministic, "echo recovered")
	agentTask := seedRunning("agent", core.ExecutionAgent, "summarize")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	recovery, err := New(events.NewGateway(l)).Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.RunningRecovered != 2 || recovery.TasksExecuted != 1 || recovery.BlockedPreserved != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	snapshot, err := projections.New(events.NewGateway(l)).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[deterministicTask.ID].Value.Status != core.TaskCompleted {
		t.Fatalf("deterministic task was not safely retried: %+v", snapshot.Tasks[deterministicTask.ID])
	}
	if snapshot.Tasks[agentTask.ID].Value.Status != core.TaskBlocked {
		t.Fatalf("uncertain adaptive task was replayed: %+v", snapshot.Tasks[agentTask.ID])
	}
	agentEvents, err := l.Events(ctx, "agent")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range agentEvents {
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" || event.EventType == "TOOL_OUTCOME_RECORDED" {
			t.Fatalf("uncertain adaptive execution was blindly replayed: %+v", event)
		}
	}
}

func TestRecoverCompletesDurableExternalInputExactlyOnce(t *testing.T) {
	for _, stage := range []string{"input_durable", "task_resumed", "completion_verified"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			l, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			gateway := events.NewGateway(l)
			service := New(gateway)
			result, err := service.Submit(ctx, Submit{RequestID: "request-1", OrganizationID: "org-1", Statement: "human response", Kind: core.ExecutionHuman})
			if err != nil || result.Task.Status != core.TaskBlocked {
				t.Fatalf("submit=%+v err=%v", result, err)
			}
			inputEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
				OrganizationID: "org-1",
				EventType:      "A2A_INPUT_RECEIVED",
				SourceActorID:  "hermes",
				TaskID:         string(result.Task.ID),
				CorrelationID:  "request-1",
				Payload:        externalInputPayload{Text: "approved task input", SourceExternalActor: "hermes"},
			})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := service.state.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			state := snapshot.Tasks[result.Task.ID]
			if stage != "input_durable" {
				task := state.Value
				task.Status = core.TaskPending
				if err := service.state.SaveTask(ctx, "org-1", "TASK_RESUMED", "runtime", "request-1", state.Version+1, task, map[string]string{"input_event_ref": inputEvent.EventID}); err != nil {
					t.Fatal(err)
				}
				state = projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: "request-1", Value: task}
			}
			if stage == "completion_verified" {
				task := state.Value
				task.Status = core.TaskRunning
				if err := service.state.SaveTask(ctx, "org-1", "EXECUTION_STARTED", "runtime", "request-1", state.Version+1, task, map[string]string{"input_event_ref": inputEvent.EventID}); err != nil {
					t.Fatal(err)
				}
				state = projections.Versioned[core.Task]{Version: state.Version + 1, CorrelationID: "request-1", Value: task}
				executionID := "external-input-" + inputEvent.EventID
				now := time.Now().UTC()
				outcome := core.ToolOutcome{ToolInvocationID: core.ID("a2a-input-" + inputEvent.EventID), ToolID: "a2a.external-input", ToolVersion: "v1", Status: core.OutcomeSucceeded, ObservedEffect: map[string]string{"input_event_ref": inputEvent.EventID}, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
				for _, draft := range []events.TrustedDraft{
					{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: outcome, CorrelationID: "request-1"},
					{OrganizationID: "org-1", EventType: "EXECUTION_FINISHED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: map[string]any{"status": outcome.Status}, CorrelationID: "request-1"},
					{OrganizationID: "org-1", EventType: "CANDIDATE_COMPLETE", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: map[string]any{"tool_invocation_id": outcome.ToolInvocationID}, CorrelationID: "request-1"},
				} {
					if _, err := gateway.PublishTrusted(ctx, draft); err != nil {
						t.Fatal(err)
					}
				}
				contract := core.CompletionContract{TaskID: task.ID, TaskVersion: state.Version, Criteria: []core.CompletionCriterion{{ID: "durable-external-input", Assurance: core.AssuranceDeterministic, Required: true}}}
				detail := completionDetail{Contract: contract, Result: service.completion.Evaluate(contract, outcome)}
				if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", SourceExecutionID: executionID, TaskID: string(task.ID), Payload: detail, CorrelationID: "request-1"}); err != nil {
					t.Fatal(err)
				}
			}

			recovered := New(gateway)
			recovery, err := recovered.Recover(ctx)
			if err != nil || recovery.TasksExecuted != 1 {
				t.Fatalf("recovery=%+v err=%v", recovery, err)
			}
			snapshot, err = recovered.state.Load(ctx)
			if err != nil || snapshot.Tasks[result.Task.ID].Value.Status != core.TaskCompleted {
				t.Fatalf("recovered task=%+v err=%v", snapshot.Tasks[result.Task.ID], err)
			}
			stream, err := gateway.Events(ctx, "request-1")
			if err != nil {
				t.Fatal(err)
			}
			for _, eventType := range []string{"A2A_INPUT_RECEIVED", "TASK_RESUMED", "EXECUTION_STARTED", "TOOL_OUTCOME_RECORDED", "EXECUTION_FINISHED", "CANDIDATE_COMPLETE", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE"} {
				count := 0
				for _, event := range stream {
					if event.EventType == eventType && event.TaskID == string(result.Task.ID) {
						count++
					}
				}
				if count != 1 {
					t.Fatalf("%s count=%d stream=%+v", eventType, count, stream)
				}
			}
			eventCount := len(stream)
			if _, err := recovered.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			stream, err = gateway.Events(ctx, "request-1")
			if err != nil || len(stream) != eventCount {
				t.Fatalf("second recovery appended events: count=%d want=%d err=%v", len(stream), eventCount, err)
			}
		})
	}
}

func TestBlockedChildReturnsControlToParentWithoutAuthorityExpansion(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	service := New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "complete governed work", NormalizedObjective: "complete governed work"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "complete governed work", Status: "ACTIVE"}
	child := core.Task{ID: "task-child", GoalID: goal.ID, ParentID: "task-parent", Description: "use unavailable tool", ExecutionKind: core.ExecutionTool, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	parent := core.Task{ID: "task-parent", GoalID: goal.ID, Description: "govern child remediation", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{child.ID}, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error { return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, agent, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, parent, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, child, nil)
		},
	} {
		if err := save(); err != nil {
			t.Fatal(err)
		}
	}

	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.TasksExecuted != 2 {
		t.Fatalf("recovery=%+v", recovery)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Tasks[child.ID].Value.Status != core.TaskBlocked || snapshot.Tasks[parent.ID].Value.Status != core.TaskBlocked {
		t.Fatalf("blocked child or governing parent state changed unexpectedly: child=%+v parent=%+v", snapshot.Tasks[child.ID], snapshot.Tasks[parent.ID])
	}
	if err := service.ValidateAddressedRoute(ctx, events.AddressedRoute{OrganizationID: string(organization.ID), EventType: "TASK_BLOCKED", SourceActorID: string(agent.ID), ValidateSource: true, RecipientScope: events.RecipientTask, RecipientID: string(child.ID), TaskID: string(child.ID)}); err == nil {
		t.Fatal("blocked child could route its escalation somewhere other than its parent")
	}
	if err := service.ValidateAddressedRoute(ctx, events.AddressedRoute{OrganizationID: string(organization.ID), EventType: "TASK_BLOCKED", SourceActorID: string(agent.ID), ValidateSource: true, RecipientScope: events.RecipientTask, RecipientID: string(parent.ID)}); err == nil {
		t.Fatal("blocked event without a source child task was accepted")
	}
	if err := service.ValidateAddressedRoute(ctx, events.AddressedRoute{OrganizationID: string(organization.ID), EventType: "TASK_BLOCKED", SourceActorID: string(agent.ID), ValidateSource: true, RecipientScope: events.RecipientTask, RecipientID: string(parent.ID), TaskID: string(parent.ID)}); err == nil {
		t.Fatal("root task was accepted as a blocked child source")
	}
	upward, err := gateway.Inbox(ctx, events.RecipientTask, string(parent.ID))
	if err != nil || len(upward) != 0 {
		t.Fatalf("parent remediation inbox=%+v err=%v", upward, err)
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	var blockedEvent events.Event
	var parentManifest core.ExecutionContextManifest
	observed := false
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "APPROVAL_") {
			t.Fatalf("blocked worker changed authority: %+v", event)
		}
		if event.EventType == "TASK_BLOCKED" && event.TaskID == string(child.ID) && event.RecipientID == string(parent.ID) {
			blockedEvent = event
		}
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && event.TaskID == string(parent.ID) {
			if err := json.Unmarshal(event.Payload, &parentManifest); err != nil {
				t.Fatal(err)
			}
		}
		if event.EventType == "INBOX_EVENTS_OBSERVED" && event.TaskID == string(parent.ID) {
			observed = true
		}
	}
	if blockedEvent.EventID == "" || len(blockedEvent.AuthorizationRefs) != 0 {
		t.Fatalf("blocked work gained authority or lost its upward route: %+v", blockedEvent)
	}
	var payload events.ProjectionEventPayload
	if err := json.Unmarshal(blockedEvent.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	var detail events.TaskBlockedPayload
	if err := json.Unmarshal(payload.Detail, &detail); err != nil || detail.Reason == "" || detail.Missing == "" || detail.WhyNeeded == "" || detail.WorkCompleted == "" {
		t.Fatalf("blocked-work contract=%+v err=%v", detail, err)
	}
	if !observed || len(parentManifest.EventRefs) != 1 || parentManifest.EventRefs[0] != blockedEvent.EventID {
		t.Fatalf("parent did not receive a bounded remediation pass: manifest=%+v observed=%v blocked=%+v", parentManifest, observed, blockedEvent)
	}
}

func TestLateralMessagesSurviveRestartAndSurfaceAtAgentActionBoundary(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(l)
	repository := projections.New(gateway)
	service := New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	sender := core.Agent{ID: "agent-sender", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	recipient := core.Agent{ID: "agent-recipient", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Delivery", MemberAgentIDs: []core.ID{recipient.ID}, Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "finish from handoff", NormalizedObjective: "finish from handoff"}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "finish from handoff", Status: "ACTIVE"}
	sourceTask := core.Task{ID: "task-source", GoalID: goal.ID, Description: "prepare handoff", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: sender.ID, TaskContractVersion: "1", Status: core.TaskCompleted}
	recipientTask := core.Task{ID: "task-recipient", GoalID: goal.ID, Description: "finish work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, DependsOn: []core.ID{sourceTask.ID}, AssigneeType: "AGENT", AssigneeID: recipient.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error {
			return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, sender, nil)
		},
		func() error {
			return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, recipient, nil)
		},
		func() error { return repository.SaveTeam(ctx, "TEAM_CREATED", "runtime", "request-1", 1, team, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, sourceTask, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, recipientTask, nil)
		},
	} {
		if err := save(); err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
	}
	routes := []struct {
		scope string
		id    string
		body  string
	}{
		{events.RecipientAgent, string(recipient.ID), "direct handoff detail"},
		{events.RecipientTeam, string(team.ID), "team handoff detail"},
		{events.RecipientTask, string(recipientTask.ID), "task handoff detail"},
	}
	messageIDs := make([]string, 0, len(routes))
	for _, route := range routes {
		message, err := service.SendMessage(ctx, string(organization.ID), string(sender.ID), "execution-source", "request-1", events.Draft{
			EventType:      "MESSAGE",
			RecipientScope: route.scope,
			RecipientID:    route.id,
			TaskID:         string(sourceTask.ID),
			Payload: map[string]any{
				"body":            route.body,
				"source_actor_id": "admin",
			},
		})
		if err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
		if message.SourceActorID != string(sender.ID) {
			_ = l.Close()
			t.Fatalf("payload spoofed sender envelope: %+v", message)
		}
		messageIDs = append(messageIDs, message.EventID)
	}
	if _, err := service.SendMessage(ctx, string(organization.ID), string(sender.ID), "execution-source", "request-1", events.Draft{
		EventType:      "MESSAGE",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "unknown-agent",
		TaskID:         string(sourceTask.ID),
		Payload:        map[string]any{"body": "must fail closed"},
	}); err == nil {
		_ = l.Close()
		t.Fatal("unknown recipient accepted")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway = events.NewGateway(l)
	service = New(gateway)
	recovery, err := service.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.PendingFound != 1 || recovery.TasksExecuted != 1 {
		t.Fatalf("recovery=%+v", recovery)
	}
	stream, err := gateway.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	var manifest core.ExecutionContextManifest
	var outcome core.ToolOutcome
	for _, event := range stream {
		switch event.EventType {
		case "EXECUTION_CONTEXT_MANIFESTED":
			if event.TaskID == string(recipientTask.ID) {
				if err := json.Unmarshal(event.Payload, &manifest); err != nil {
					t.Fatal(err)
				}
			}
		case "TOOL_OUTCOME_RECORDED":
			if event.TaskID == string(recipientTask.ID) {
				if err := json.Unmarshal(event.Payload, &outcome); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if strings.Join(manifest.EventRefs, ",") != strings.Join(messageIDs, ",") {
		t.Fatalf("manifest event refs=%v want=%v", manifest.EventRefs, messageIDs)
	}
	observed, ok := outcome.ObservedEffect.(string)
	if !ok {
		t.Fatalf("observed effect type=%T value=%v", outcome.ObservedEffect, outcome.ObservedEffect)
	}
	for _, route := range routes {
		if !strings.Contains(observed, route.body) {
			t.Fatalf("model context omitted %q: %s", route.body, observed)
		}
		available, err := gateway.Inbox(ctx, route.scope, route.id)
		if err != nil || len(available) != 0 {
			t.Fatalf("observed inbox %s/%s=%+v err=%v", route.scope, route.id, available, err)
		}
	}
}

var errProjectionWrite = errors.New("injected task projection failure")

type failTaskProjection struct{ *ledger.SQLite }

func (f failTaskProjection) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if draft.ProjectionKind == projections.KindTask {
		return events.Event{}, errProjectionWrite
	}
	return f.SQLite.AppendProjection(ctx, draft)
}

func assertEventOrder(t *testing.T, stream []events.Event, expected ...string) {
	t.Helper()
	next := 0
	for _, event := range stream {
		if next < len(expected) && event.EventType == expected[next] {
			next++
		}
	}
	if next != len(expected) {
		t.Fatalf("event order missing %q after index %d: %+v", expected[next], next, stream)
	}
}
