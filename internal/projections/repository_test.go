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
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "build a durable business", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "deliver sustained value", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified outcome", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "blueprint-v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "profile-v1", ModelProvider: "fake", Model: "fake-model/v1", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "fake", Status: "ACTIVE"}
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Delivery", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "echo hello", NormalizedObjective: "echo hello", HardConstraints: []string{}, ConsequenceBoundaries: []string{}, CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: "echo hello", Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: rosterAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	repository := New(events.NewGateway(l))
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error {
			return repository.SaveMission(ctx, "MISSION_CREATED", "runtime", "request-1", 1, mission, nil)
		},
		func() error { return repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil) },
		func() error {
			return repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_CREATED", "runtime", "request-1", 1, blueprint, nil)
		},
		func() error {
			return repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_CREATED", "runtime", "request-1", 1, profile, nil)
		},
		func() error { return repository.SaveAgent(ctx, "AGENT_CREATED", "runtime", "request-1", 1, agent, nil) },
		func() error { return repository.SaveTeam(ctx, "TEAM_CREATED", "runtime", "request-1", 1, team, nil) },
		func() error {
			return repository.SaveIntent(ctx, "INTENT_CREATED", "runtime", "request-1", 1, intent, nil)
		},
		func() error {
			return repository.SaveWork(ctx, organization.ID, "WORK_CREATED", "runtime", "request-1", 1, work, nil)
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
	if !reflect.DeepEqual(loaded.Missions[mission.ID].Value, mission) || !reflect.DeepEqual(loaded.Goals[goal.ID].Value, goal) || loaded.Works[work.ID].Value.GoalID != goal.ID {
		t.Fatalf("organizational hierarchy changed after restart: mission=%+v goal=%+v work=%+v", loaded.Missions[mission.ID].Value, loaded.Goals[goal.ID].Value, loaded.Works[work.ID].Value)
	}
	if !reflect.DeepEqual(loaded.AgentBlueprints[blueprint.ID].Value, blueprint) || !reflect.DeepEqual(loaded.ExecutionProfiles[profile.ID].Value, profile) {
		t.Fatalf("durable roster configuration changed after restart: blueprint=%+v profile=%+v", loaded.AgentBlueprints[blueprint.ID].Value, loaded.ExecutionProfiles[profile.ID].Value)
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
		"work correlation": func(snapshot Snapshot) {
			work := snapshot.Works["work-1"]
			work.CorrelationID = "other"
			snapshot.Works["work-1"] = work
		},
		"task correlation": func(snapshot Snapshot) {
			task := snapshot.Tasks["task-1"]
			task.CorrelationID = "other"
			snapshot.Tasks["task-1"] = task
		},
		"cross-work dependency": func(snapshot Snapshot) {
			task := snapshot.Tasks["task-1"]
			task.Value.DependsOn = []core.ID{"task-2"}
			snapshot.Tasks["task-1"] = task
		},
		"cross-work parent": func(snapshot Snapshot) {
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

func TestSnapshotRejectsUnknownWorkStatus(t *testing.T) {
	snapshot := validBoundarySnapshot()
	work := snapshot.Works["work-1"]
	work.Value.Status = "UNKNOWN"
	snapshot.Works["work-1"] = work
	if err := validateSnapshot(snapshot); err == nil {
		t.Fatal("unknown Work status was accepted")
	}
}

func TestMissionGoalWorkHierarchyIsTenantBounded(t *testing.T) {
	snapshot := validBoundarySnapshot()
	snapshot.Missions["mission-1"] = Versioned[core.Mission]{Value: core.Mission{
		ID: "mission-1", OrganizationID: "org-1", Statement: "build a durable business", Status: core.MissionActive,
	}}
	snapshot.Goals["goal-1"] = Versioned[core.Goal]{Value: core.Goal{
		ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "reach a sustained outcome",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified target", Origin: "USER"}}, Status: core.GoalActive,
	}}
	work := snapshot.Works["work-1"]
	work.Value.GoalID = "goal-1"
	snapshot.Works["work-1"] = work
	if err := validateSnapshot(snapshot); err != nil {
		t.Fatalf("valid Mission > Goal > Work hierarchy was rejected: %v", err)
	}

	crossTenant := snapshot
	crossTenant.Goals = make(map[core.ID]Versioned[core.Goal], len(snapshot.Goals)+1)
	for id, state := range snapshot.Goals {
		crossTenant.Goals[id] = state
	}
	crossTenant.Works = make(map[core.ID]Versioned[core.Work], len(snapshot.Works))
	for id, state := range snapshot.Works {
		crossTenant.Works[id] = state
	}
	crossTenant.Goals["goal-2"] = Versioned[core.Goal]{Value: core.Goal{
		ID: "goal-2", OrganizationID: "org-2", MissionID: "mission-1", Objective: "cross tenant",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "never", Origin: "USER"}}, Status: core.GoalActive,
	}}
	work = crossTenant.Works["work-1"]
	work.Value.GoalID = "goal-2"
	crossTenant.Works["work-1"] = work
	if err := validateSnapshot(crossTenant); err == nil {
		t.Fatal("cross-organization Goal and Work linkage was accepted")
	}

	continuousAchieved := snapshot
	continuousAchieved.Goals = make(map[core.ID]Versioned[core.Goal], len(snapshot.Goals))
	for id, state := range snapshot.Goals {
		continuousAchieved.Goals[id] = state
	}
	continuous := continuousAchieved.Goals["goal-1"]
	continuous.Value.Mode = core.GoalContinuous
	continuous.Value.Status = core.GoalAchieved
	continuousAchieved.Goals["goal-1"] = continuous
	if err := validateSnapshot(continuousAchieved); err == nil {
		t.Fatal("continuous Goal was accepted as achieved")
	}
}

func TestHierarchyRevisionsPreserveIdentityAndDirectionBoundaries(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "initial direction", Status: core.MissionActive, CreatedAt: now}
	revisedMission := mission
	revisedMission.Statement = "refined direction"
	if err := decodeKind(projectionBodies(t, KindMission, string(mission.ID), mission, revisedMission), map[core.ID]Versioned[core.Mission]{}, false, sameMissionRecord); err != nil {
		t.Fatalf("versioned Mission refinement was rejected: %v", err)
	}
	crossOrganizationMission := revisedMission
	crossOrganizationMission.OrganizationID = "org-2"
	if err := decodeKind(projectionBodies(t, KindMission, string(mission.ID), mission, crossOrganizationMission), map[core.ID]Versioned[core.Mission]{}, false, sameMissionRecord); err == nil {
		t.Fatal("Mission revision changed organization")
	}
	invalidMission := mission
	invalidMission.Status = "UNKNOWN"
	if err := decodeKind(projectionBodies(t, KindMission, string(mission.ID), invalidMission, mission), map[core.ID]Versioned[core.Mission]{}, false, sameMissionRecord); err == nil {
		t.Fatal("invalid historical Mission state was hidden by a later revision")
	}

	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "initial outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "initial measure", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	revisedGoal := goal
	revisedGoal.Objective = "more specific outcome"
	revisedGoal.SuccessCriteria = []core.IntentValue{{Value: "refined measure", Origin: "USER"}}
	if err := decodeKind(projectionBodies(t, KindGoal, string(goal.ID), goal, revisedGoal), map[core.ID]Versioned[core.Goal]{}, false, sameGoalRecord); err != nil {
		t.Fatalf("versioned Goal refinement was rejected: %v", err)
	}
	reparentedGoal := revisedGoal
	reparentedGoal.MissionID = "mission-2"
	if err := decodeKind(projectionBodies(t, KindGoal, string(goal.ID), goal, reparentedGoal), map[core.ID]Versioned[core.Goal]{}, false, sameGoalRecord); err == nil {
		t.Fatal("Goal revision changed parent Mission")
	}
	achievedWithChangedCriteria := revisedGoal
	achievedWithChangedCriteria.Status = core.GoalAchieved
	if err := decodeKind(projectionBodies(t, KindGoal, string(goal.ID), goal, achievedWithChangedCriteria), map[core.ID]Versioned[core.Goal]{}, false, sameGoalRecord); err == nil {
		t.Fatal("Goal achievement changed success criteria at the terminal boundary")
	}

	work := core.Work{ID: "work-1", IntentID: "intent-1", GoalID: goal.ID, Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	completed := work
	completed.Status = core.WorkCompleted
	if err := decodeKind(projectionBodies(t, KindWork, string(work.ID), work, completed), map[core.ID]Versioned[core.Work]{}, false, sameWorkRecord); err != nil {
		t.Fatalf("Work status transition was rejected: %v", err)
	}
	relinked := completed
	relinked.GoalID = "goal-2"
	if err := decodeKind(projectionBodies(t, KindWork, string(work.ID), work, relinked), map[core.ID]Versioned[core.Work]{}, false, sameWorkRecord); err == nil {
		t.Fatal("Work transition changed Goal binding")
	}
	reopened := completed
	reopened.Status = core.WorkActive
	if err := decodeKind(projectionBodies(t, KindWork, string(work.ID), completed, reopened), map[core.ID]Versioned[core.Work]{}, false, sameWorkRecord); err == nil {
		t.Fatal("terminal Work was reopened")
	}
	retiredMission := mission
	retiredMission.Status = core.MissionRetired
	reopenedMission := retiredMission
	reopenedMission.Status = core.MissionActive
	if err := decodeKind(projectionBodies(t, KindMission, string(mission.ID), retiredMission, reopenedMission), map[core.ID]Versioned[core.Mission]{}, false, sameMissionRecord); err == nil {
		t.Fatal("retired Mission was reopened")
	}
	achievedGoal := goal
	achievedGoal.Status = core.GoalAchieved
	reopenedGoal := achievedGoal
	reopenedGoal.Status = core.GoalActive
	if err := decodeKind(projectionBodies(t, KindGoal, string(goal.ID), achievedGoal, reopenedGoal), map[core.ID]Versioned[core.Goal]{}, false, sameGoalRecord); err == nil {
		t.Fatal("achieved Goal was reopened")
	}
}

func TestSnapshotRejectsMalformedDurableRoster(t *testing.T) {
	tests := map[string]func(Snapshot){
		"missing blueprint": func(snapshot Snapshot) {
			delete(snapshot.AgentBlueprints, "blueprint-1")
		},
		"wrong blueprint version": func(snapshot Snapshot) {
			agent := snapshot.Agents["agent-1"]
			agent.Value.BlueprintVersion = "other"
			snapshot.Agents["agent-1"] = agent
		},
		"cross-organization profile": func(snapshot Snapshot) {
			profile := snapshot.ExecutionProfiles["profile-1"]
			profile.Value.OrganizationID = "org-2"
			snapshot.ExecutionProfiles["profile-1"] = profile
		},
		"duplicate capability prerequisite": func(snapshot Snapshot) {
			blueprint := snapshot.AgentBlueprints["blueprint-1"]
			blueprint.Value.RequiredCapabilityClasses = []string{"repository.write", "repository.write"}
			snapshot.AgentBlueprints["blueprint-1"] = blueprint
		},
		"unknown Agent status": func(snapshot Snapshot) {
			agent := snapshot.Agents["agent-1"]
			agent.Value.Status = "UNKNOWN"
			snapshot.Agents["agent-1"] = agent
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := validRosterSnapshot()
			mutate(snapshot)
			if err := validateSnapshot(snapshot); err == nil {
				t.Fatal("malformed durable roster was accepted")
			}
		})
	}
}

func TestSnapshotRejectsMalformedPinnedAgentConfiguration(t *testing.T) {
	tests := map[string]func(*core.Task){
		"missing configuration":   func(task *core.Task) { task.AgentConfig = nil },
		"wrong blueprint version": func(task *core.Task) { task.AgentConfig.BlueprintVersion = "other" },
		"missing profile":         func(task *core.Task) { task.AgentConfig.ProfileID = "missing" },
		"missing runtime":         func(task *core.Task) { task.AgentConfig.RuntimeAdapter = "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := validRosterSnapshot()
			intent := core.Intent{ID: "intent-1", OrganizationID: "org-1"}
			work := core.Work{ID: "work-1", IntentID: intent.ID}
			agent := snapshot.Agents["agent-1"].Value
			task := core.Task{ID: "task-1", WorkID: work.ID, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: rosterAgentConfig(agent)}
			mutate(&task)
			snapshot.Intents[intent.ID] = Versioned[core.Intent]{CorrelationID: "work-1", Value: intent}
			snapshot.Works[work.ID] = Versioned[core.Work]{CorrelationID: "work-1", Value: work}
			snapshot.Tasks[task.ID] = Versioned[core.Task]{CorrelationID: "work-1", Value: task}
			if err := validateSnapshot(snapshot); err == nil {
				t.Fatal("malformed pinned Agent configuration was accepted")
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
	if err := decodeKind(records, map[core.ID]Versioned[core.Task]{}, true, nil); err == nil {
		t.Fatal("historical correlation change was accepted")
	}
}

func TestRosterRevisionsPreserveConfigurationAndAgentIdentity(t *testing.T) {
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: "org-1", Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: time.Unix(1, 0).UTC()}
	changedBlueprint := blueprint
	changedBlueprint.OperatingInstructions = "expanded work"
	if err := decodeKind(projectionBodies(t, KindAgentBlueprint, string(blueprint.ID), blueprint, changedBlueprint), map[core.ID]Versioned[core.AgentBlueprint]{}, false, sameAgentBlueprintRecord); err == nil {
		t.Fatal("blueprint instructions changed without a new durable identity")
	}

	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: "org-1", Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: time.Unix(1, 0).UTC()}
	changedProfile := profile
	changedProfile.PromptVersion = "v2"
	if err := decodeKind(projectionBodies(t, KindExecutionProfile, string(profile.ID), profile, changedProfile), map[core.ID]Versioned[core.ExecutionProfile]{}, false, sameExecutionProfileRecord); err == nil {
		t.Fatal("execution profile changed without a new durable identity")
	}

	agent := core.Agent{ID: "agent-1", OrganizationID: "org-1", BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	changedAgent := agent
	changedAgent.ExecutionProfileID = "profile-2"
	if err := decodeKind(projectionBodies(t, KindAgent, string(agent.ID), agent, changedAgent), map[core.ID]Versioned[core.Agent]{}, false, sameAgentRecord); err != nil {
		t.Fatalf("Agent configuration revision changed its durable identity: %v", err)
	}

	inactive := agent
	inactive.Status = "INACTIVE"
	if err := decodeKind(projectionBodies(t, KindAgent, string(agent.ID), agent, inactive), map[core.ID]Versioned[core.Agent]{}, false, sameAgentRecord); err != nil {
		t.Fatalf("status-only Agent transition was rejected: %v", err)
	}

	otherOrganization := agent
	otherOrganization.OrganizationID = "org-2"
	if err := decodeKind(projectionBodies(t, KindAgent, string(agent.ID), agent, otherOrganization), map[core.ID]Versioned[core.Agent]{}, false, sameAgentRecord); err == nil {
		t.Fatal("Agent identity crossed its organization boundary")
	}
}

func projectionBodies[T any](t *testing.T, kind, id string, values ...T) [][]byte {
	t.Helper()
	bodies := make([][]byte, 0, len(values))
	for index, value := range values {
		encodedValue, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(events.ProjectionRecord{ProjectionKind: kind, RecordID: id, Version: index + 1, Value: encodedValue})
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	return bodies
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
	works := map[core.ID]Versioned[core.Work]{
		"work-1": {CorrelationID: "work-1", Value: core.Work{ID: "work-1", IntentID: "intent-1", Status: core.WorkActive}},
		"work-2": {CorrelationID: "work-2", Value: core.Work{ID: "work-2", IntentID: "intent-2", Status: core.WorkActive}},
	}
	tasks := map[core.ID]Versioned[core.Task]{
		"task-1": {CorrelationID: "work-1", Value: core.Task{ID: "task-1", WorkID: "work-1"}},
		"task-2": {CorrelationID: "work-2", Value: core.Task{ID: "task-2", WorkID: "work-2"}},
	}
	return Snapshot{
		Organizations: organizations, Teams: map[core.ID]Versioned[core.Team]{}, AgentBlueprints: map[core.ID]Versioned[core.AgentBlueprint]{}, ExecutionProfiles: map[core.ID]Versioned[core.ExecutionProfile]{}, Agents: map[core.ID]Versioned[core.Agent]{},
		Missions: map[core.ID]Versioned[core.Mission]{}, Goals: map[core.ID]Versioned[core.Goal]{}, Intents: intents, Works: works, Tasks: tasks,
	}
}

func validRosterSnapshot() Snapshot {
	organization := core.Organization{ID: "org-1"}
	otherOrganization := core.Organization{ID: "org-2"}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "blueprint-v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE"}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "profile-v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE"}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	return Snapshot{
		Organizations: map[core.ID]Versioned[core.Organization]{organization.ID: {Value: organization}, otherOrganization.ID: {Value: otherOrganization}},
		Teams:         map[core.ID]Versioned[core.Team]{}, AgentBlueprints: map[core.ID]Versioned[core.AgentBlueprint]{blueprint.ID: {Value: blueprint}},
		ExecutionProfiles: map[core.ID]Versioned[core.ExecutionProfile]{profile.ID: {Value: profile}}, Agents: map[core.ID]Versioned[core.Agent]{agent.ID: {Value: agent}},
		Intents: map[core.ID]Versioned[core.Intent]{}, Works: map[core.ID]Versioned[core.Work]{}, Tasks: map[core.ID]Versioned[core.Task]{},
	}
}

func rosterAgentConfig(agent core.Agent) *core.AgentConfig {
	return &core.AgentConfig{
		BlueprintID: agent.BlueprintID, BlueprintVersion: agent.BlueprintVersion,
		ProfileID: agent.ExecutionProfileID, ProfileVersion: agent.ExecutionProfileVersion,
		RuntimeAdapter: agent.RuntimeAdapter,
	}
}
