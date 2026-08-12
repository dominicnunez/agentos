package projections

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestDurableObjectsSurviveRestartAndRebuildFromEvents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintVersion: "v1", ExecutionProfileVersion: "v1", RuntimeAdapter: "fake", Status: "ACTIVE"}
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Delivery", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "echo hello", NormalizedObjective: "echo hello", HardConstraints: []string{}, ConsequenceBoundaries: []string{}, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", IntentID: intent.ID, Objective: "echo hello", Status: "ACTIVE", CreatedAt: now}
	task := core.Task{ID: "task-1", GoalID: goal.ID, Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	repository := New(events.NewGateway(l))
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error { return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, agent, nil) },
		func() error { return repository.SaveTeam(ctx, "TEAM_CREATED", "runtime", "request-1", 1, team, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveGoal(ctx, organization.ID, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil)
		},
		func() error {
			return repository.SaveTask(ctx, organization.ID, "TASK_CREATED", "runtime", "request-1", 1, task, nil)
		},
	} {
		if err := save(); err != nil {
			_ = l.Close()
			t.Fatal(err)
		}
	}
	task.Status = core.TaskRunning
	if err := repository.SaveTask(ctx, organization.ID, "EXECUTION_STARTED", "runtime", "request-1", 2, task, nil); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	repository = New(events.NewGateway(l))
	loaded, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := repository.Rebuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, rebuilt) {
		t.Fatalf("records projection differs from event replay:\nloaded=%+v\nrebuilt=%+v", loaded, rebuilt)
	}
	if loaded.Organizations[organization.ID].Value != organization || loaded.Agents[agent.ID].Value != agent {
		t.Fatalf("durable identity changed after restart: org=%+v agent=%+v", loaded.Organizations[organization.ID].Value, loaded.Agents[agent.ID].Value)
	}
	if !reflect.DeepEqual(loaded.Teams[team.ID].Value, team) {
		t.Fatalf("team changed after restart: %+v", loaded.Teams[team.ID].Value)
	}
	if loaded.Tasks[task.ID].Version != 2 || loaded.Tasks[task.ID].Value.Status != core.TaskRunning {
		t.Fatalf("latest task state not restored: %+v", loaded.Tasks[task.ID])
	}
}

func TestSnapshotRejectsCrossBoundaryTaskGraph(t *testing.T) {
	tests := map[string]func(Snapshot){
		"goal correlation": func(snapshot Snapshot) {
			goal := snapshot.Goals["goal-1"]
			goal.CorrelationID = "other"
			snapshot.Goals["goal-1"] = goal
		},
		"task correlation": func(snapshot Snapshot) {
			task := snapshot.Tasks["task-1"]
			task.CorrelationID = "other"
			snapshot.Tasks["task-1"] = task
		},
		"cross-goal dependency": func(snapshot Snapshot) {
			task := snapshot.Tasks["task-1"]
			task.Value.DependsOn = []core.ID{"task-2"}
			snapshot.Tasks["task-1"] = task
		},
		"cross-goal parent": func(snapshot Snapshot) {
			task := snapshot.Tasks["task-1"]
			task.Value.ParentID = "task-2"
			snapshot.Tasks["task-1"] = task
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := validBoundarySnapshot()
			mutate(snapshot)
			if err := validateSnapshot(snapshot); err == nil {
				t.Fatal("cross-boundary graph was accepted")
			}
		})
	}
}

func TestDecodeKindRejectsHistoricalCorrelationChange(t *testing.T) {
	records := make([][]byte, 0, 3)
	for version, correlationID := range []string{"work-a", "work-b", "work-a"} {
		value, err := json.Marshal(core.Task{ID: "task-1"})
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(events.ProjectionRecord{
			ProjectionKind: KindTask, RecordID: "task-1", Version: version + 1,
			CorrelationID: correlationID, Value: value,
		})
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, body)
	}
	if err := decodeKind(records, map[core.ID]Versioned[core.Task]{}, true); err == nil {
		t.Fatal("historical correlation change was accepted")
	}
}

func TestRebuildRejectsProjectionCorrelationMismatch(t *testing.T) {
	value, err := json.Marshal(core.Task{ID: "task-1"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(events.ProjectionEventPayload{Projection: events.ProjectionRecord{
		ProjectionKind: KindTask, RecordID: "task-1", Version: 1,
		CorrelationID: "work-a", Value: value,
	}})
	if err != nil {
		t.Fatal(err)
	}
	repository := New(events.NewGateway(replayLedger{stream: []events.Event{{
		EventID: "evt-1", CorrelationID: "work-b", Payload: payload,
	}}}))
	if _, err := repository.Rebuild(context.Background()); err == nil {
		t.Fatal("event-to-projection correlation mismatch was accepted")
	}
}

type replayLedger struct{ stream []events.Event }

func (replayLedger) Append(context.Context, events.TrustedDraft) (events.Event, error) {
	return events.Event{}, nil
}

func (l replayLedger) Events(context.Context, string) ([]events.Event, error) {
	return l.stream, nil
}

func validBoundarySnapshot() Snapshot {
	organizations := map[core.ID]Versioned[core.Organization]{
		"org-1": {CorrelationID: "work-1", Value: core.Organization{ID: "org-1"}},
		"org-2": {CorrelationID: "work-2", Value: core.Organization{ID: "org-2"}},
	}
	intents := map[core.ID]Versioned[core.Intent]{
		"intent-1": {CorrelationID: "work-1", Value: core.Intent{ID: "intent-1", OrganizationID: "org-1"}},
		"intent-2": {CorrelationID: "work-2", Value: core.Intent{ID: "intent-2", OrganizationID: "org-2"}},
	}
	goals := map[core.ID]Versioned[core.Goal]{
		"goal-1": {CorrelationID: "work-1", Value: core.Goal{ID: "goal-1", IntentID: "intent-1"}},
		"goal-2": {CorrelationID: "work-2", Value: core.Goal{ID: "goal-2", IntentID: "intent-2"}},
	}
	tasks := map[core.ID]Versioned[core.Task]{
		"task-1": {CorrelationID: "work-1", Value: core.Task{ID: "task-1", GoalID: "goal-1"}},
		"task-2": {CorrelationID: "work-2", Value: core.Task{ID: "task-2", GoalID: "goal-2"}},
	}
	return Snapshot{
		Organizations: organizations, Teams: map[core.ID]Versioned[core.Team]{}, Agents: map[core.ID]Versioned[core.Agent]{},
		Intents: intents, Goals: goals, Tasks: tasks,
	}
}
