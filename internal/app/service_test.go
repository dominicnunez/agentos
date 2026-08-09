package app

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

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
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v4.2"}
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
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v4.2"}
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
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v4.2"}
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
