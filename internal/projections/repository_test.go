package projections

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type eventReadLedger struct {
	*ledger.SQLite
	eventReads int
}

func (l *eventReadLedger) Events(ctx context.Context, correlationID string) ([]events.Event, error) {
	l.eventReads++
	return l.SQLite.Events(ctx, correlationID)
}

func TestRoutineLoadDoesNotReplayHistoricalLedger(t *testing.T) {
	ctx := context.Background()
	sqlite, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlite.Close() })
	counted := &eventReadLedger{SQLite: sqlite}
	repository := New(events.NewGateway(counted))
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "setup", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if counted.eventReads != 0 {
		t.Fatalf("routine projection Load replayed the event ledger %d times", counted.eventReads)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.ValidateCompletionAdmissions(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if counted.eventReads != 1 {
		t.Fatalf("explicit recovery audit event reads=%d want=1", counted.eventReads)
	}
}

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
	sourceText := "Use goal-1 to echo hello"
	reviewed := core.IntentDraft{
		ID: "intent-request-1", OrganizationID: organization.ID, Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard, RequestedExecutionKind: core.ExecutionDeterministic,
		Goal: &core.IntentValue{Value: string(goal.ID), Origin: "EXPLICIT", SourceMessageID: "message-1"}, Objective: "echo hello",
		Context: []core.IntentValue{}, Deliverables: []core.IntentValue{{Value: "hello", Origin: "EXPLICIT", SourceMessageID: "message-1"}},
		CompletionCriteria: []core.IntentValue{{Value: "verified outcome", Origin: "DEFAULT"}}, Constraints: []core.IntentValue{}, ResolvedDecisions: []core.IntentDecision{},
		ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: now,
	}
	reviewed.Fingerprint, err = core.FingerprintIntentDraft(reviewed)
	if err != nil {
		t.Fatal(err)
	}
	intent := core.Intent{
		ID: "intent-request-1", OrganizationID: organization.ID, GoalID: goal.ID, OriginalInstruction: sourceText, NormalizedObjective: "echo hello", HardConstraints: []string{}, ConsequenceBoundaries: []string{},
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", ExternalRequestID: "request-1", SourceMessageID: "message-1", AcceptedFingerprint: reviewed.Fingerprint, CreatedAt: now,
	}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: "echo hello", Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: rosterAgentConfig(agent), TaskContractVersion: "1", Status: core.TaskPending}
	gateway := events.NewGateway(l)
	repository := New(gateway)
	for _, save := range []func() error{
		func() error {
			return repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "request-1", 1, organization, nil)
		},
		func() error {
			return repository.SaveMission(ctx, "MISSION_CREATED", "runtime", "request-1", 1, mission, nil)
		},
		func() error { return repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", "request-1", 1, goal, nil) },
		func() error {
			if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
				OrganizationID: string(organization.ID), EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: "task-request-1", CorrelationID: "request-1",
				Payload: events.IntakeMessageRecordedPayload{MessageID: "message-1", Text: sourceText, SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic},
			}); err != nil {
				return err
			}
			if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
				OrganizationID: string(organization.ID), EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: "task-request-1", CorrelationID: "request-1",
				Payload: events.IntentDraftedPayload{SourceMessageID: "message-1", Draft: reviewed, Reply: "Review the proposed intent before work begins."},
			}); err != nil {
				return err
			}
			_, err := gateway.PublishIntentConfirmation(ctx, events.TrustedDraft{
				OrganizationID: string(organization.ID), EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-request-1", CorrelationID: "request-1",
				Payload: events.IntentConfirmedPayload{IntentID: string(intent.ID), GoalID: string(goal.ID), Version: 1, Fingerprint: intent.AcceptedFingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"},
			}, goal.ID)
			return err
		},
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
	stream, err := l.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	withoutConfirmation := make([]events.Event, 0, len(stream)-1)
	for _, event := range stream {
		if event.EventType != "INTENT_CONFIRMED" {
			withoutConfirmation = append(withoutConfirmation, event)
		}
	}
	if _, err := New(events.NewGateway(replayLedger{stream: withoutConfirmation})).Rebuild(ctx); err == nil || !strings.Contains(err.Error(), "prior reviewed intent confirmation") {
		t.Fatalf("startup admitted Goal-bound Work without review evidence: %v", err)
	}
	withUnboundConfirmation := insertUnboundReplayConfirmation(t, stream, "request-1", intent)
	if _, err := New(events.NewGateway(replayLedger{stream: withUnboundConfirmation})).Rebuild(ctx); err == nil || !strings.Contains(err.Error(), "reviewed Goal provenance") {
		t.Fatalf("startup admitted Goal-bound Work after conflicting unbound confirmation: %v", err)
	}
	workBeforeIntent := swapReplayProjectionSequences(t, stream, "INTENT_CREATED", "WORK_CREATED")
	if _, err := New(events.NewGateway(replayLedger{stream: workBeforeIntent})).Rebuild(ctx); err == nil || !strings.Contains(err.Error(), "prior Intent") {
		t.Fatalf("startup admitted Work before its Intent: %v", err)
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
			if err := ValidateSnapshot(snapshot); err == nil {
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
	if err := ValidateSnapshot(snapshot); err == nil {
		t.Fatal("unknown Work status was accepted")
	}
}

func TestRepositoryRejectsBareCompletedWork(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	repository := New(events.NewGateway(l))
	work := core.Work{ID: "work-1", IntentID: "intent-1", Objective: "forged completion", Status: core.WorkCompleted, CreatedAt: time.Now().UTC()}
	if err := repository.SaveWork(ctx, "org-1", "WORK_COMPLETED", "runtime", "run-1", 2, work, events.WorkCompletionTransitionPayload{}); err == nil {
		t.Fatal("bare completed Work reached the generic repository path")
	}
	stream, err := l.Events(ctx, "run-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected Work completion reached ledger: events=%+v err=%v", stream, err)
	}
}

func TestRepositoryRejectsUntrustedWorkLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	repository := New(events.NewGateway(l))
	now := time.Now().UTC()
	active := core.Work{ID: "work-1", IntentID: "intent-1", Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	failed := active
	failed.Status = core.WorkFailed
	completed := active
	completed.Status = core.WorkCompleted
	for name, input := range map[string]struct {
		eventType string
		actorID   string
		version   int
		work      core.Work
	}{
		"Agent creation":   {eventType: "WORK_CREATED", actorID: "agent-1", version: 1, work: active},
		"mislabeled start": {eventType: "WORK_FAILED", actorID: "runtime", version: 1, work: active},
		"Agent failure":    {eventType: "WORK_FAILED", actorID: "agent-1", version: 2, work: failed},
		"mislabeled fail":  {eventType: "WORK_CREATED", actorID: "runtime", version: 2, work: failed},
		"mislabeled complete": {
			eventType: "WORK_FAILED", actorID: "runtime", version: 2, work: completed,
		},
		"completion label on active": {
			eventType: "WORK_COMPLETED", actorID: "runtime", version: 2, work: active,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := repository.SaveWork(ctx, "org-1", input.eventType, input.actorID, "run-1", input.version, input.work, nil); err == nil {
				t.Fatal("untrusted Work lifecycle event reached the repository")
			}
		})
	}
	stream, err := l.Events(ctx, "run-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected Work lifecycle event reached ledger: events=%+v err=%v", stream, err)
	}
}

func TestCompletedWorkSnapshotRequiresDurableAdmissionEvidence(t *testing.T) {
	snapshot := validBoundarySnapshot()
	work := snapshot.Works["work-1"]
	work.Version = 2
	work.Value.Status = core.WorkCompleted
	snapshot.Works["work-1"] = work
	if err := validateWorkCompletionAdmissions(snapshot, nil, nil, nil); err == nil {
		t.Fatal("completed Work was exposed without a durable transition and evidence")
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
	intent := snapshot.Intents["intent-1"]
	intent.Value.GoalID = "goal-1"
	snapshot.Intents["intent-1"] = intent
	if err := ValidateSnapshot(snapshot); err != nil {
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
	if err := ValidateSnapshot(crossTenant); err == nil {
		t.Fatal("cross-organization Goal and Work linkage was accepted")
	}

	mismatched := snapshot
	mismatched.Intents = make(map[core.ID]Versioned[core.Intent], len(snapshot.Intents))
	for id, state := range snapshot.Intents {
		mismatched.Intents[id] = state
	}
	intent = mismatched.Intents["intent-1"]
	intent.Value.GoalID = ""
	mismatched.Intents["intent-1"] = intent
	if err := ValidateSnapshot(mismatched); err == nil {
		t.Fatal("Work Goal differed from its accepted Intent Goal")
	}

	bareAchievement := snapshot
	bareAchievement.Goals = make(map[core.ID]Versioned[core.Goal], len(snapshot.Goals))
	for id, state := range snapshot.Goals {
		bareAchievement.Goals[id] = state
	}
	achieved := bareAchievement.Goals["goal-1"]
	achieved.Value.Mode = core.GoalContinuous
	achieved.Value.Status = core.GoalAchieved
	bareAchievement.Goals["goal-1"] = achieved
	if err := ValidateSnapshot(bareAchievement); err == nil {
		t.Fatal("continuous Goal was accepted as terminally achieved")
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
	bareAchievement := goal
	bareAchievement.Status = core.GoalAchieved
	if err := decodeKind(projectionBodies(t, KindGoal, string(goal.ID), goal, bareAchievement), map[core.ID]Versioned[core.Goal]{}, false, sameGoalRecord); err != nil {
		t.Fatalf("evidence-backed Goal achievement could not be rebuilt: %v", err)
	}

	team := core.Team{ID: "team-1", OrganizationID: "org-1", Name: "Team", MemberAgentIDs: []core.ID{}, Status: "ACTIVE", CreatedAt: now}
	revisedTeam := team
	revisedTeam.Name = "Revised Team"
	if err := decodeKind(projectionBodies(t, KindTeam, string(team.ID), team, revisedTeam), map[core.ID]Versioned[core.Team]{}, false, sameTeamRecord); err != nil {
		t.Fatalf("valid Team revision was rejected: %v", err)
	}
	reassignedTeam := revisedTeam
	reassignedTeam.OrganizationID = "org-2"
	if err := decodeKind(projectionBodies(t, KindTeam, string(team.ID), team, reassignedTeam), map[core.ID]Versioned[core.Team]{}, false, sameTeamRecord); err == nil {
		t.Fatal("Team revision changed tenant ownership")
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
	retiredGoal := goal
	retiredGoal.Status = core.GoalRetired
	reopenedGoal := retiredGoal
	reopenedGoal.Status = core.GoalActive
	if err := decodeKind(projectionBodies(t, KindGoal, string(goal.ID), retiredGoal, reopenedGoal), map[core.ID]Versioned[core.Goal]{}, false, sameGoalRecord); err == nil {
		t.Fatal("retired Goal was reopened")
	}
}

func TestSaveMissionAndGoalRejectInvalidHierarchyBeforeAppending(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(filepath.Join(t.TempDir(), "agentos.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := New(gateway)
	now := time.Unix(1, 0).UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "durable direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{
		ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "measurable outcome", Mode: core.GoalTarget,
		SuccessCriteria: []core.IntentValue{{Value: "verified result", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now,
	}
	orphanMission := mission
	orphanMission.ID = "mission-orphan"
	orphanMission.OrganizationID = "org-missing"
	if err := repository.SaveMission(ctx, "MISSION_CREATED", "runtime", "orphan-mission-create", 1, orphanMission, nil); err == nil {
		t.Fatal("Mission without a durable parent Organization was appended")
	}
	assertEmptyEventStream(t, ctx, gateway, "orphan-mission-create")
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "organization-create", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveMission(ctx, "MISSION_CREATED", "agent-1", "agent-mission-create", 1, mission, nil); err == nil {
		t.Fatal("Agent authored Mission creation was admitted")
	}
	assertEmptyEventStream(t, ctx, gateway, "agent-mission-create")
	if err := repository.SaveMission(ctx, "MISSION_CREATED", "runtime", "mission-create", 1, mission, nil); err != nil {
		t.Fatal(err)
	}
	for name, test := range map[string]struct {
		version int
		change  func(*core.Mission)
	}{
		"unknown status": {version: 2, change: func(candidate *core.Mission) { candidate.Status = "UNKNOWN" }},
		"version gap":    {version: 3, change: func(*core.Mission) {}},
	} {
		t.Run("Mission "+name, func(t *testing.T) {
			candidate := mission
			test.change(&candidate)
			correlationID := "invalid-mission-" + name
			if err := repository.SaveMission(ctx, "MISSION_REVISED", "runtime", correlationID, test.version, candidate, nil); err == nil {
				t.Fatal("invalid Mission revision was appended")
			}
			assertEmptyEventStream(t, ctx, gateway, correlationID)
		})
	}
	for name, candidate := range map[string]core.Goal{
		"missing Mission": func() core.Goal { value := goal; value.MissionID = "missing"; return value }(),
		"cross-organization Mission": func() core.Goal {
			value := goal
			value.OrganizationID = "org-2"
			return value
		}(),
	} {
		t.Run("Goal "+name, func(t *testing.T) {
			correlationID := "invalid-goal-parent-" + name
			if err := repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", correlationID, 1, candidate, nil); err == nil {
				t.Fatal("Goal with invalid parent Mission was appended")
			}
			assertEmptyEventStream(t, ctx, gateway, correlationID)
		})
	}
	if err := repository.SaveGoal(ctx, "GOAL_CREATED", "agent-1", "agent-goal-create", 1, goal, nil); err == nil {
		t.Fatal("Agent authored Goal creation was admitted")
	}
	assertEmptyEventStream(t, ctx, gateway, "agent-goal-create")
	if err := repository.SaveGoal(ctx, "GOAL_ACHIEVED", "runtime", "mislabeled-goal-create", 1, goal, nil); err == nil {
		t.Fatal("mislabeled Goal creation was admitted")
	}
	assertEmptyEventStream(t, ctx, gateway, "mislabeled-goal-create")
	if err := repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", "goal-create", 1, goal, nil); err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		version int
		change  func(*core.Goal)
	}{
		"bare achievement": {version: 2, change: func(candidate *core.Goal) { candidate.Status = core.GoalStatus("ACHIEVED") }},
		"changed Mission":  {version: 2, change: func(candidate *core.Goal) { candidate.MissionID = "mission-2" }},
		"version gap":      {version: 3, change: func(*core.Goal) {}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := goal
			test.change(&candidate)
			correlationID := "invalid-" + name
			if err := repository.SaveGoal(ctx, "GOAL_REFINED", "runtime", correlationID, test.version, candidate, nil); err == nil {
				t.Fatal("invalid Goal revision was appended")
			}
			assertEmptyEventStream(t, ctx, gateway, correlationID)
		})
	}
	retired := mission
	retired.Status = core.MissionRetired
	if err := repository.SaveMission(ctx, "MISSION_RETIRED", "runtime", "mission-retire", 2, retired, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveMission(ctx, "MISSION_REOPENED", "runtime", "invalid-mission-reopen", 3, mission, nil); err == nil {
		t.Fatal("retired Mission was reopened")
	}
	assertEmptyEventStream(t, ctx, gateway, "invalid-mission-reopen")
	loaded, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state := loaded.Goals[goal.ID]; state.Version != 1 || !reflect.DeepEqual(state.Value, goal) {
		t.Fatalf("rejected revision changed durable Goal: %+v", state)
	}
	if state := loaded.Missions[mission.ID]; state.Version != 2 || !reflect.DeepEqual(state.Value, retired) {
		t.Fatalf("rejected revision changed durable Mission: %+v", state)
	}
}

func assertEmptyEventStream(t *testing.T, ctx context.Context, gateway *events.Gateway, correlationID string) {
	t.Helper()
	stream, err := gateway.Events(ctx, correlationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stream) != 0 {
		t.Fatalf("rejected hierarchy revision left %d authoritative events", len(stream))
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
			if err := ValidateSnapshot(snapshot); err == nil {
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
			if err := ValidateSnapshot(snapshot); err == nil {
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

func TestRosterConfigurationRevisionRejectedBeforeCommit(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	gateway := events.NewGateway(l)
	repository := New(gateway)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1"}
	if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "setup", 1, organization, nil); err != nil {
		t.Fatal(err)
	}
	blueprint := core.AgentBlueprint{
		ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker",
		OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE",
	}
	profile := core.ExecutionProfile{
		ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "review-provider",
		Model: "review-model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE",
	}
	whitespaceBlueprint := blueprint
	whitespaceBlueprint.ID = "blueprint-whitespace"
	whitespaceBlueprint.RequiredCapabilityClasses = []string{"   "}
	if err := repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_CREATED", "runtime", "whitespace-blueprint", 1, whitespaceBlueprint, nil); err == nil {
		t.Fatal("whitespace-only Agent blueprint capability reached persistence")
	}
	whitespaceProfile := profile
	whitespaceProfile.ID = "profile-whitespace"
	whitespaceProfile.ToolRefs = []string{"\t"}
	if err := repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_CREATED", "runtime", "whitespace-profile", 1, whitespaceProfile, nil); err == nil {
		t.Fatal("whitespace-only execution profile tool reference reached persistence")
	}
	if err := repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_CREATED", "runtime", "setup", 1, blueprint, nil); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_CREATED", "runtime", "setup", 1, profile, nil); err != nil {
		t.Fatal(err)
	}
	forgedBlueprint := blueprint
	forgedBlueprint.OperatingInstructions = "substituted instructions"
	if err := repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_UPDATED", "runtime", "forged-blueprint", 2, forgedBlueprint, nil); err == nil {
		t.Fatal("Agent blueprint configuration changed under its pinned domain version")
	}
	forgedProfile := profile
	forgedProfile.ModelProvider = "fake"
	forgedProfile.Model = "fake-model/v1"
	if err := repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_UPDATED", "runtime", "forged-profile", 2, forgedProfile, nil); err == nil {
		t.Fatal("execution profile configuration changed under its pinned domain version")
	}
	for _, correlationID := range []string{"whitespace-blueprint", "whitespace-profile", "forged-blueprint", "forged-profile"} {
		stream, err := gateway.Events(ctx, correlationID)
		if err != nil {
			t.Fatal(err)
		}
		if len(stream) != 0 {
			t.Fatalf("rejected roster revision left authoritative events for %s: %+v", correlationID, stream)
		}
	}
	blueprint.Status = "INACTIVE"
	if err := repository.SaveAgentBlueprint(ctx, "AGENT_BLUEPRINT_UPDATED", "runtime", "blueprint-status", 2, blueprint, nil); err != nil {
		t.Fatalf("status-only Agent blueprint revision was rejected: %v", err)
	}
	profile.Status = "INACTIVE"
	if err := repository.SaveExecutionProfile(ctx, "EXECUTION_PROFILE_UPDATED", "runtime", "profile-status", 2, profile, nil); err != nil {
		t.Fatalf("status-only execution profile revision was rejected: %v", err)
	}
	snapshot, err := repository.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.AgentBlueprints[blueprint.ID].Value, blueprint) || !reflect.DeepEqual(snapshot.ExecutionProfiles[profile.ID].Value, profile) {
		t.Fatalf("admitted roster state differs from valid status revisions: blueprint=%+v profile=%+v", snapshot.AgentBlueprints[blueprint.ID], snapshot.ExecutionProfiles[profile.ID])
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
	record := events.ProjectionRecord{
		ProjectionKind: KindTask, RecordID: "task-1", Version: 1,
		CorrelationID: "work-a", Value: value,
	}
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: "task-1", CorrelationID: "work-a"}
	boundary := events.Event{
		EventID: "evt-1", Sequence: 1, OrganizationID: draft.OrganizationID, EventType: draft.EventType,
		SourceActorID: draft.SourceActorID, TaskID: draft.TaskID, CorrelationID: draft.CorrelationID,
		CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion,
	}
	sealed, err := events.SealProjectionEvent(boundary, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	repository := New(events.NewGateway(replayLedger{stream: []events.Event{{
		EventID: boundary.EventID, Sequence: boundary.Sequence, OrganizationID: boundary.OrganizationID, EventType: boundary.EventType, SourceActorID: boundary.SourceActorID, TaskID: boundary.TaskID,
		CorrelationID: "work-b", CreatedAt: boundary.CreatedAt, SchemaVersion: boundary.SchemaVersion, Payload: payload,
	}}}))
	if _, err := repository.Rebuild(context.Background()); err == nil {
		t.Fatal("event-to-projection correlation mismatch was accepted")
	}
}

func TestRebuildRejectsMislabeledTaskLifecycle(t *testing.T) {
	base := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "bounded work",
		ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden,
		TaskContractVersion: "1", Status: core.TaskPending,
	}
	completed := base
	completed.Status = core.TaskCompleted
	stream := make([]events.Event, 0, 2)
	for index, revision := range []struct {
		eventType string
		version   int
		value     core.Task
	}{{"TASK_CREATED", 1, base}, {"TASK_RESUMED", 2, completed}} {
		value, err := json.Marshal(revision.value)
		if err != nil {
			t.Fatal(err)
		}
		record := events.ProjectionRecord{ProjectionKind: KindTask, RecordID: string(base.ID), Version: revision.version, CorrelationID: "work-1", Value: value}
		boundary := events.Event{
			EventID: fmt.Sprintf("event-%d", index+1), Sequence: int64(index + 1), OrganizationID: "org-1", EventType: revision.eventType,
			SourceActorID: "runtime", TaskID: string(base.ID), CorrelationID: "work-1", CreatedAt: time.Unix(int64(index+1), 0).UTC(), SchemaVersion: events.SchemaVersion,
		}
		sealed, err := events.SealProjectionEvent(boundary, record, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, boundary)
	}
	if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
		t.Fatal("event replay accepted a Task status under the wrong lifecycle event")
	}
}

func TestRebuildRejectsMislabeledAgentLifecycle(t *testing.T) {
	agent := core.Agent{ID: "agent-1", OrganizationID: "org-1", BlueprintID: "blueprint-1", BlueprintVersion: "v1", ExecutionProfileID: "profile-1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	stream := make([]events.Event, 0, 2)
	for index, revision := range []struct {
		eventType string
		version   int
	}{{"AGENT_CREATED", 1}, {"AGENT_DEACTIVATED", 2}} {
		value, err := json.Marshal(agent)
		if err != nil {
			t.Fatal(err)
		}
		record := events.ProjectionRecord{ProjectionKind: KindAgent, RecordID: string(agent.ID), Version: revision.version, CorrelationID: "setup", Value: value}
		boundary := events.Event{EventID: fmt.Sprintf("agent-event-%d", index+1), Sequence: int64(index + 1), OrganizationID: "org-1", EventType: revision.eventType, SourceActorID: "runtime", CorrelationID: "setup", CreatedAt: time.Unix(int64(index+1), 0).UTC(), SchemaVersion: events.SchemaVersion}
		sealed, err := events.SealProjectionEvent(boundary, record, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, boundary)
	}
	if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
		t.Fatal("event replay accepted an ACTIVE Agent under AGENT_DEACTIVATED")
	}
}

func TestRebuildRejectsMislabeledGoalLifecycle(t *testing.T) {
	goal := core.Goal{
		ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded outcome", Mode: core.GoalTarget,
		SuccessCriteria: []core.IntentValue{{Value: "verified outcome", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: time.Unix(1, 0).UTC(),
	}
	stream := make([]events.Event, 0, 2)
	for index, revision := range []struct {
		eventType string
		version   int
	}{{"GOAL_CREATED", 1}, {"GOAL_PAUSED", 2}} {
		value, err := json.Marshal(goal)
		if err != nil {
			t.Fatal(err)
		}
		record := events.ProjectionRecord{ProjectionKind: KindGoal, RecordID: string(goal.ID), Version: revision.version, CorrelationID: "goal-1", Value: value}
		boundary := events.Event{EventID: fmt.Sprintf("goal-event-%d", index+1), Sequence: int64(index + 1), OrganizationID: "org-1", EventType: revision.eventType, SourceActorID: "runtime", CorrelationID: "goal-1", CreatedAt: time.Unix(int64(index+1), 0).UTC(), SchemaVersion: events.SchemaVersion}
		sealed, err := events.SealProjectionEvent(boundary, record, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, boundary)
	}
	if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
		t.Fatal("event replay accepted an ACTIVE Goal under GOAL_PAUSED")
	}
}

func TestRebuildRejectsMislabeledWorkLifecycle(t *testing.T) {
	work := core.Work{ID: "work-1", IntentID: "intent-1", Objective: "bounded work", Status: core.WorkActive, CreatedAt: time.Unix(1, 0).UTC()}
	stream := make([]events.Event, 0, 2)
	for index, revision := range []struct {
		eventType string
		version   int
	}{{"WORK_CREATED", 1}, {"WORK_FAILED", 2}} {
		value, err := json.Marshal(work)
		if err != nil {
			t.Fatal(err)
		}
		record := events.ProjectionRecord{ProjectionKind: KindWork, RecordID: string(work.ID), Version: revision.version, CorrelationID: "work-1", Value: value}
		boundary := events.Event{EventID: fmt.Sprintf("work-event-%d", index+1), Sequence: int64(index + 1), OrganizationID: "org-1", EventType: revision.eventType, SourceActorID: "runtime", CorrelationID: "work-1", CreatedAt: time.Unix(int64(index+1), 0).UTC(), SchemaVersion: events.SchemaVersion}
		sealed, err := events.SealProjectionEvent(boundary, record, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, boundary)
	}
	if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
		t.Fatal("event replay accepted ACTIVE Work under WORK_FAILED")
	}
}

func TestRebuildRejectsMislabeledMissionLifecycle(t *testing.T) {
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "durable direction", Status: core.MissionActive, CreatedAt: time.Unix(1, 0).UTC()}
	stream := make([]events.Event, 0, 2)
	for index, revision := range []struct {
		eventType string
		version   int
	}{{"MISSION_CREATED", 1}, {"MISSION_RETIRED", 2}} {
		value, err := json.Marshal(mission)
		if err != nil {
			t.Fatal(err)
		}
		record := events.ProjectionRecord{ProjectionKind: KindMission, RecordID: string(mission.ID), Version: revision.version, CorrelationID: "mission-1", Value: value}
		boundary := events.Event{EventID: fmt.Sprintf("mission-event-%d", index+1), Sequence: int64(index + 1), OrganizationID: "org-1", EventType: revision.eventType, SourceActorID: "runtime", CorrelationID: "mission-1", CreatedAt: time.Unix(int64(index+1), 0).UTC(), SchemaVersion: events.SchemaVersion}
		sealed, err := events.SealProjectionEvent(boundary, record, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, boundary)
	}
	if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
		t.Fatal("event replay accepted an ACTIVE Mission under MISSION_RETIRED")
	}
}

func TestEventAuditRejectsHistoricalAgentConfigurationBinding(t *testing.T) {
	agent := core.Agent{ID: "agent-1", OrganizationID: "org-1", BlueprintID: "missing-blueprint", BlueprintVersion: "v1", ExecutionProfileID: "profile-1", ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	value, err := json.Marshal(agent)
	if err != nil {
		t.Fatal(err)
	}
	record := events.ProjectionRecord{ProjectionKind: KindAgent, RecordID: string(agent.ID), Version: 1, CorrelationID: "setup", Value: value}
	event := events.Event{EventID: "agent-event-1", Sequence: 1, OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "setup", CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion}
	sealed, err := events.SealProjectionEvent(event, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		Organizations:     map[core.ID]Versioned[core.Organization]{"org-1": {Version: 1, Value: core.Organization{ID: "org-1"}}},
		AgentBlueprints:   map[core.ID]Versioned[core.AgentBlueprint]{"blueprint-1": {Version: 1, Value: core.AgentBlueprint{ID: "blueprint-1", OrganizationID: "org-1", Version: "v1"}}},
		ExecutionProfiles: map[core.ID]Versioned[core.ExecutionProfile]{"profile-1": {Version: 1, Value: core.ExecutionProfile{ID: "profile-1", OrganizationID: "org-1", Version: "v1"}}},
	}
	if err := validateProjectionEventOrganizationBindings(snapshot, []events.Event{event}); err == nil || !strings.Contains(err.Error(), "invalid pinned configuration") {
		t.Fatalf("event audit accepted an Agent revision with an unbound blueprint: %v", err)
	}
}

func TestRebuildRejectsTaskCompletionWithoutEvidenceChain(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, NormalizedObjective: "objective", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "recovery task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	var stream []events.Event
	appendProjection := func(eventType, kind, id string, version int, value any, detail any) {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var detailBody json.RawMessage
		if detail != nil {
			detailBody, err = json.Marshal(detail)
			if err != nil {
				t.Fatal(err)
			}
		}
		record := events.ProjectionRecord{ProjectionKind: kind, RecordID: id, Version: version, CorrelationID: "work-1", Value: body}
		boundary := events.Event{
			EventID: fmt.Sprintf("event-%d", len(stream)+1), Sequence: int64(len(stream) + 1), OrganizationID: string(organization.ID), EventType: eventType,
			SourceActorID: "runtime", AuthorizationRefs: []string{}, ArtifactRefs: []string{}, CorrelationID: record.CorrelationID, CreatedAt: now.Add(time.Duration(len(stream)) * time.Second), SchemaVersion: events.SchemaVersion,
		}
		if kind == KindTask {
			boundary.TaskID = id
		}
		sealed, err := events.SealProjectionEvent(boundary, record, detailBody)
		if err != nil {
			t.Fatal(err)
		}
		boundary.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, boundary)
	}
	appendProjection("ORGANIZATION_CREATED", KindOrganization, string(organization.ID), 1, organization, nil)
	appendProjection("INTENT_CREATED", KindIntent, string(intent.ID), 1, intent, nil)
	appendProjection("WORK_CREATED", KindWork, string(work.ID), 1, work, nil)
	appendProjection("TASK_CREATED", KindTask, string(task.ID), 1, task, nil)
	task.Status = core.TaskRunning
	appendProjection("EXECUTION_STARTED", KindTask, string(task.ID), 2, task, nil)
	task.Status = core.TaskCompleted
	decision := events.CompletionDecisionPayload{Contract: core.CompletionContract{TaskID: task.ID, TaskVersion: 2}, Result: events.CompletionDecisionResultPayload{Complete: true}, OutcomeEventRef: "missing-outcome"}
	appendProjection("TASK_VERIFIED_COMPLETE", KindTask, string(task.ID), 3, task, decision)

	if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil || !strings.Contains(err.Error(), "exact verification decision") {
		t.Fatalf("event replay accepted a status-only Task completion: %v", err)
	}
}

func TestFullAuditRejectsProjectionEventWithoutMaterializedRecord(t *testing.T) {
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Unix(1, 0).UTC()}
	value, err := json.Marshal(organization)
	if err != nil {
		t.Fatal(err)
	}
	record := events.ProjectionRecord{
		ProjectionKind: KindOrganization, RecordID: string(organization.ID), Version: 1,
		CorrelationID: "setup-1", Value: value,
	}
	boundary := events.Event{
		EventID: "event-1", Sequence: 1, OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED",
		SourceActorID: "runtime", CorrelationID: record.CorrelationID,
		CreatedAt: time.Unix(2, 0).UTC(), SchemaVersion: events.SchemaVersion,
	}
	sealed, err := events.SealProjectionEvent(boundary, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{
		Organizations: map[core.ID]Versioned[core.Organization]{organization.ID: {Version: 1, CorrelationID: record.CorrelationID, Value: organization}},
		Missions:      map[core.ID]Versioned[core.Mission]{}, Goals: map[core.ID]Versioned[core.Goal]{}, Teams: map[core.ID]Versioned[core.Team]{},
		AgentBlueprints: map[core.ID]Versioned[core.AgentBlueprint]{}, ExecutionProfiles: map[core.ID]Versioned[core.ExecutionProfile]{}, Agents: map[core.ID]Versioned[core.Agent]{},
		Intents: map[core.ID]Versioned[core.Intent]{}, Works: map[core.ID]Versioned[core.Work]{}, Tasks: map[core.ID]Versioned[core.Task]{},
	}
	repository := New(events.NewGateway(replayLedger{stream: []events.Event{boundary}}))
	err = repository.ValidateCompletionAdmissions(context.Background(), snapshot)
	if err == nil || !strings.Contains(err.Error(), "lacks one exact materialized record") {
		t.Fatalf("full audit accepted orphan projection event: %v", err)
	}
}

func TestRebuildRejectsProjectionShapedOrdinaryEvents(t *testing.T) {
	for _, kind := range []string{KindTeam, KindAgentBlueprint, "role", "grant"} {
		t.Run(kind, func(t *testing.T) {
			value, err := json.Marshal(map[string]string{"id": kind + "-1"})
			if err != nil {
				t.Fatal(err)
			}
			record := events.ProjectionRecord{
				ProjectionKind: kind, RecordID: kind + "-1", Version: 1,
				CorrelationID: "work-1", Value: value,
			}
			ordinaryDraft := events.TrustedDraft{
				OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1",
			}
			boundary := events.Event{
				EventID: "event-1", Sequence: 1, OrganizationID: ordinaryDraft.OrganizationID, EventType: ordinaryDraft.EventType,
				SourceActorID: ordinaryDraft.SourceActorID, CorrelationID: ordinaryDraft.CorrelationID,
				CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion,
			}
			sealed, err := events.SealProjectionEvent(boundary, record, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(sealed)
			if err != nil {
				t.Fatal(err)
			}
			stream := []events.Event{{
				EventID: boundary.EventID, Sequence: boundary.Sequence, OrganizationID: boundary.OrganizationID, EventType: boundary.EventType,
				SourceActorID: boundary.SourceActorID, CorrelationID: boundary.CorrelationID,
				CreatedAt: boundary.CreatedAt, SchemaVersion: boundary.SchemaVersion, Payload: body,
			}}
			if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
				t.Fatalf("ordinary event became authoritative %s state", kind)
			}

			copied := stream[0]
			copied.EventID = "event-2"
			if _, err := New(events.NewGateway(replayLedger{stream: []events.Event{copied}})).Rebuild(context.Background()); err == nil {
				t.Fatalf("copied admission became authoritative %s state", kind)
			}

			unsealed, err := json.Marshal(events.ProjectionEventPayload{Projection: record})
			if err != nil {
				t.Fatal(err)
			}
			stream[0].Payload = unsealed
			if _, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background()); err == nil {
				t.Fatalf("unsealed event became authoritative %s state", kind)
			}
		})
	}
}

func TestRebuildRejectsUnsupportedOrdinaryEventSchema(t *testing.T) {
	stream := []events.Event{{
		EventID: "event-1", Sequence: 1, OrganizationID: "org-1", EventType: "INTENT_DRAFTED",
		SourceActorID: "runtime", TaskID: "task-work-1", CorrelationID: "work-1",
		CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion + 1,
		Payload: json.RawMessage(`{"draft":"unsupported"}`),
	}}
	_, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported schema version") {
		t.Fatalf("startup admitted unsupported ordinary event schema: %v", err)
	}
}

func TestRebuildRejectsInvalidHistoricalTeamRoster(t *testing.T) {
	now := time.Unix(1, 0).UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	invalid := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Delivery", MemberAgentIDs: []core.ID{"missing-agent"}, Status: "ACTIVE", CreatedAt: now}
	corrected := invalid
	corrected.MemberAgentIDs = []core.ID{}
	stream := []events.Event{
		sealedReplayProjection(t, 1, "ORGANIZATION_CREATED", KindOrganization, string(organization.ID), 1, "setup", organization.ID, organization),
		sealedReplayProjection(t, 2, "TEAM_CREATED", KindTeam, string(invalid.ID), 1, "roster", organization.ID, invalid),
		sealedReplayProjection(t, 3, "TEAM_REVISED", KindTeam, string(corrected.ID), 2, "roster", organization.ID, corrected),
	}
	_, err := New(events.NewGateway(replayLedger{stream: stream})).Rebuild(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid member Agent") {
		t.Fatalf("startup accepted an invalid historical Team roster hidden by a later revision: %v", err)
	}
}

func sealedReplayProjection(t *testing.T, sequence int64, eventType, kind, recordID string, version int, correlationID string, organizationID core.ID, value any) events.Event {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	record := events.ProjectionRecord{ProjectionKind: kind, RecordID: recordID, Version: version, CorrelationID: correlationID, Value: encoded}
	event := events.Event{
		EventID: fmt.Sprintf("event-%d", sequence), Sequence: sequence, OrganizationID: string(organizationID), EventType: eventType,
		SourceActorID: "runtime", CorrelationID: correlationID, CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: events.SchemaVersion,
	}
	sealed, err := events.SealProjectionEvent(event, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func swapReplayProjectionSequences(t *testing.T, stream []events.Event, firstType, secondType string) []events.Event {
	t.Helper()
	result := append([]events.Event(nil), stream...)
	first, second := -1, -1
	for index, event := range result {
		switch event.EventType {
		case firstType:
			first = index
		case secondType:
			second = index
		}
	}
	if first < 0 || second < 0 {
		t.Fatalf("projection events %s/%s were not found", firstType, secondType)
	}
	firstPayload, firstPresent, firstErr := events.AdmittedProjection(result[first])
	secondPayload, secondPresent, secondErr := events.AdmittedProjection(result[second])
	if firstErr != nil || secondErr != nil || !firstPresent || !secondPresent {
		t.Fatalf("projection events cannot be resealed: first=%v/%t second=%v/%t", firstErr, firstPresent, secondErr, secondPresent)
	}
	result[first].Sequence, result[second].Sequence = result[second].Sequence, result[first].Sequence
	for _, item := range []struct {
		index   int
		payload events.ProjectionEventPayload
	}{{first, firstPayload}, {second, secondPayload}} {
		sealed, err := events.SealProjectionEvent(result[item.index], item.payload.Projection, item.payload.Detail)
		if err != nil {
			t.Fatal(err)
		}
		result[item.index].Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func insertUnboundReplayConfirmation(t *testing.T, stream []events.Event, correlationID string, intent core.Intent) []events.Event {
	t.Helper()
	result := append([]events.Event(nil), stream...)
	insertSequence := int64(0)
	var template events.Event
	for _, event := range result {
		if event.CorrelationID == correlationID && event.EventType == "INTENT_CONFIRMED" {
			insertSequence = event.Sequence
			template = event
			break
		}
	}
	if insertSequence == 0 {
		t.Fatal("Goal-bound confirmation was not found")
	}
	for index := range result {
		if result[index].Sequence < insertSequence {
			continue
		}
		payload, present, err := events.AdmittedProjection(result[index])
		result[index].Sequence++
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			continue
		}
		sealed, err := events.SealProjectionEvent(result[index], payload.Projection, payload.Detail)
		if err != nil {
			t.Fatal(err)
		}
		result[index].Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
	}
	unbound := events.IntentConfirmedPayload{
		IntentID: string(intent.ID), Version: 1, Fingerprint: intent.AcceptedFingerprint,
		ConfirmingActorID: string(intent.SourcePrincipalID), ConfirmingActorKind: string(intent.SourcePrincipalKind),
		SourceChannel: intent.SourceChannel, MessageID: "unbound-confirmation",
	}
	body, err := json.Marshal(unbound)
	if err != nil {
		t.Fatal(err)
	}
	template.EventID = "event-unbound-confirmation"
	template.Sequence = insertSequence
	template.Payload = body
	result = append(result, template)
	return result
}

type replayLedger struct {
	stream  []events.Event
	records map[string][][]byte
}

func (replayLedger) Append(context.Context, events.TrustedDraft) (events.Event, error) {
	return events.Event{}, nil
}

func (l replayLedger) Events(context.Context, string) ([]events.Event, error) {
	return l.stream, nil
}

func (l replayLedger) Records(_ context.Context, kind, _ string) ([][]byte, error) {
	return l.records[kind], nil
}

func (replayLedger) InboxObservations(context.Context) (map[string]events.InboxObservationBinding, error) {
	return map[string]events.InboxObservationBinding{}, nil
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
		"task-1": {CorrelationID: "work-1", Value: core.Task{ID: "task-1", WorkID: "work-1", Description: "test work one", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}},
		"task-2": {CorrelationID: "work-2", Value: core.Task{ID: "task-2", WorkID: "work-2", Description: "test work two", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}},
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
