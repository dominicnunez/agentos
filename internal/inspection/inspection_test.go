package inspection

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestProjectProducesDeterministicPayloadFreeGovernanceReport(t *testing.T) {
	now := time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC)
	snapshot := inspectionSnapshot(now)
	verified := inspectionEvents(t, snapshot, "org-1", now)

	first, err := Project(snapshot, verified, "org-1", now.Add(time.Hour), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Project(snapshot, verified, "org-1", now.Add(time.Hour), 24*time.Hour)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("inspection is not deterministic: first=%+v second=%+v err=%v", first, second, err)
	}
	if first.Summary.Findings != 0 || first.Summary.RulesExecuted != len(rules) || first.SHA256 == "" || first.Boundary.Certified {
		t.Fatalf("unexpected clean report: %+v", first)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"secret operating instructions", "provider-secret", "private task result"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("inspection leaked private content %q: %s", private, encoded)
		}
	}
}

func TestProjectFindsRuntimeGovernanceHolesWithExactEvidence(t *testing.T) {
	now := time.Date(2026, 8, 27, 2, 3, 4, 0, time.UTC)
	snapshot := inspectionSnapshot(now)
	mission := snapshot.Missions["mission-1"]
	mission.Value.Status = core.MissionRetired
	mission.Version = 2
	snapshot.Missions["mission-1"] = mission
	profile := snapshot.ExecutionProfiles["profile-1"]
	profile.Value.Status = "INACTIVE"
	profile.Version = 2
	snapshot.ExecutionProfiles["profile-1"] = profile
	team := snapshot.Teams["team-1"]
	team.Value.MemberAgentIDs = []core.ID{}
	team.Version = 2
	snapshot.Teams["team-1"] = team
	intent := core.Intent{ID: "intent-1", OrganizationID: "org-1", NormalizedObjective: "governed work", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{
		ID: "task-1", WorkID: work.ID, Description: "private task result", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceRequired, AssigneeType: "AGENT", AssigneeID: "agent-1",
		AgentConfig:         &core.AgentConfig{BlueprintID: "blueprint-1", BlueprintVersion: "v1", ProfileID: "profile-1", ProfileVersion: "v1", RuntimeAdapter: "local"},
		TaskContractVersion: "v1", Status: core.TaskRunning,
	}
	snapshot.Intents[intent.ID] = core.DurableState[core.Intent]{Version: 1, CorrelationID: "work-1", Value: intent}
	snapshot.Works[work.ID] = core.DurableState[core.Work]{Version: 1, CorrelationID: "work-1", Value: work}
	snapshot.Tasks[task.ID] = core.DurableState[core.Task]{Version: 2, CorrelationID: "work-1", Value: task}
	verified := inspectionEvents(t, snapshot, "org-1", now)

	report, err := Project(snapshot, verified, "org-1", now.Add(time.Hour), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	wantRules := map[string]bool{
		"direction/active-mission-required":  false,
		"direction/active-goal-mission":      false,
		"operations/active-work-goal":        false,
		"roster/active-agent-configuration":  false,
		"coordination/active-team-roster":    false,
		"execution/running-agent-assignment": false,
	}
	for _, finding := range report.Findings {
		if _, wanted := wantRules[finding.RuleID]; wanted {
			wantRules[finding.RuleID] = true
			if len(finding.EvidenceRefs) == 0 {
				t.Fatalf("finding lacks exact evidence: %+v", finding)
			}
		}
	}
	for ruleID, found := range wantRules {
		if !found {
			t.Fatalf("missing governance finding %s: %+v", ruleID, report.Findings)
		}
	}
}

func TestProjectRejectsProjectionRaceAndInvalidIntegrity(t *testing.T) {
	now := time.Date(2026, 8, 27, 3, 4, 5, 0, time.UTC)
	snapshot := inspectionSnapshot(now)
	verified := inspectionEvents(t, snapshot, "org-1", now)
	changed := snapshot
	changed.Goals = cloneStateMap(snapshot.Goals)
	goal := changed.Goals["goal-1"]
	goal.Value.Objective = "changed after the verified event snapshot"
	changed.Goals["goal-1"] = goal
	if _, err := Project(changed, verified, "org-1", now.Add(time.Hour), time.Hour); err == nil || !strings.Contains(err.Error(), "boundary changed") {
		t.Fatalf("projection race was accepted: %v", err)
	}
	verified.LedgerSHA256 = "forged"
	if _, err := Project(snapshot, verified, "org-1", now.Add(time.Hour), time.Hour); err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("invalid integrity head was accepted: %v", err)
	}
}

func inspectionSnapshot(now time.Time) projections.Snapshot {
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "policy-v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Durable direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "Verified outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "Evidence exists", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "secret operating instructions", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider-secret", Model: "model-1", PromptVersion: "prompt-v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	team := core.Team{ID: "team-1", OrganizationID: organization.ID, Name: "Team", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: now}
	return projections.Snapshot{
		Organizations:     map[core.ID]core.DurableState[core.Organization]{organization.ID: {Version: 1, CorrelationID: "setup-org-1", Value: organization}},
		Missions:          map[core.ID]core.DurableState[core.Mission]{mission.ID: {Version: 1, CorrelationID: "strategy-1", Value: mission}},
		Goals:             map[core.ID]core.DurableState[core.Goal]{goal.ID: {Version: 1, CorrelationID: "strategy-1", Value: goal}},
		Teams:             map[core.ID]core.DurableState[core.Team]{team.ID: {Version: 1, CorrelationID: "roster-1", Value: team}},
		AgentBlueprints:   map[core.ID]core.DurableState[core.AgentBlueprint]{blueprint.ID: {Version: 1, CorrelationID: "roster-1", Value: blueprint}},
		ExecutionProfiles: map[core.ID]core.DurableState[core.ExecutionProfile]{profile.ID: {Version: 1, CorrelationID: "roster-1", Value: profile}},
		Agents:            map[core.ID]core.DurableState[core.Agent]{agent.ID: {Version: 1, CorrelationID: "roster-1", Value: agent}},
		Intents:           map[core.ID]core.DurableState[core.Intent]{}, Works: map[core.ID]core.DurableState[core.Work]{}, Tasks: map[core.ID]core.DurableState[core.Task]{},
		Experiments: map[core.ID]core.DurableState[core.Experiment]{}, PromotionCandidates: map[core.ID]core.DurableState[core.PromotionCandidate]{}, Knowledge: map[core.ID]core.DurableState[core.KnowledgeRecord]{},
	}
}

func inspectionEvents(t *testing.T, snapshot projections.Snapshot, organizationID core.ID, now time.Time) events.VerifiedEventSnapshot {
	t.Helper()
	type queued struct {
		kind, id, eventType string
		version             int
		correlationID       string
		value               any
	}
	var values []queued
	appendState := func(kind, id, eventType string, version int, correlationID string, value any) {
		values = append(values, queued{kind: kind, id: id, eventType: eventType, version: version, correlationID: correlationID, value: value})
	}
	appendState(projections.KindOrganization, string(organizationID), "ORGANIZATION_CREATED", snapshot.Organizations[organizationID].Version, snapshot.Organizations[organizationID].CorrelationID, snapshot.Organizations[organizationID].Value)
	for id, state := range snapshot.Missions {
		if state.Value.OrganizationID == organizationID {
			appendState(projections.KindMission, string(id), map[bool]string{true: "MISSION_CREATED", false: "MISSION_RETIRED"}[state.Version == 1], state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.Goals {
		if state.Value.OrganizationID == organizationID {
			appendState(projections.KindGoal, string(id), "GOAL_CREATED", state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.AgentBlueprints {
		if state.Value.OrganizationID == organizationID {
			eventType := "AGENT_BLUEPRINT_CREATED"
			if state.Version > 1 {
				eventType = "AGENT_BLUEPRINT_UPDATED"
			}
			appendState(projections.KindAgentBlueprint, string(id), eventType, state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.ExecutionProfiles {
		if state.Value.OrganizationID == organizationID {
			eventType := "EXECUTION_PROFILE_CREATED"
			if state.Version > 1 {
				eventType = "EXECUTION_PROFILE_UPDATED"
			}
			appendState(projections.KindExecutionProfile, string(id), eventType, state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.Agents {
		if state.Value.OrganizationID == organizationID {
			appendState(projections.KindAgent, string(id), "AGENT_CREATED", state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.Teams {
		if state.Value.OrganizationID == organizationID {
			eventType := "TEAM_CREATED"
			if state.Version > 1 {
				eventType = "TEAM_REVISED"
			}
			appendState(projections.KindTeam, string(id), eventType, state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.Intents {
		if state.Value.OrganizationID == organizationID {
			appendState(projections.KindIntent, string(id), "INTENT_CREATED", state.Version, state.CorrelationID, state.Value)
		}
	}
	for id, state := range snapshot.Works {
		appendState(projections.KindWork, string(id), "WORK_CREATED", state.Version, state.CorrelationID, state.Value)
	}
	for id, state := range snapshot.Tasks {
		appendState(projections.KindTask, string(id), "EXECUTION_STARTED", state.Version, state.CorrelationID, state.Value)
	}
	sort.Slice(values, func(left, right int) bool {
		if values[left].kind == values[right].kind {
			return values[left].id < values[right].id
		}
		return values[left].kind < values[right].kind
	})
	stream := make([]events.Event, 0, len(values))
	for index, item := range values {
		value, err := json.Marshal(item.value)
		if err != nil {
			t.Fatal(err)
		}
		event := events.Event{EventID: "event-" + item.kind + "-" + item.id, Sequence: int64(index + 1), OrganizationID: string(organizationID), EventType: item.eventType, SourceActorID: "runtime", CorrelationID: item.correlationID, CreatedAt: now.Add(time.Duration(index) * time.Second), SchemaVersion: events.SchemaVersion}
		sealed, err := events.SealProjectionEvent(event, events.ProjectionRecord{ProjectionKind: item.kind, RecordID: item.id, Version: item.version, CorrelationID: item.correlationID, Value: value}, nil)
		if err != nil {
			t.Fatal(err)
		}
		event.Payload, err = json.Marshal(sealed)
		if err != nil {
			t.Fatal(err)
		}
		stream = append(stream, event)
	}
	return events.VerifiedEventSnapshot{OrganizationID: string(organizationID), Algorithm: "SHA-256", LedgerEvents: int64(len(stream)), LedgerSequence: stream[len(stream)-1].Sequence, LedgerEventID: stream[len(stream)-1].EventID, LedgerSHA256: strings.Repeat("a", 64), Events: stream}
}

func cloneStateMap[K comparable, V any](source map[K]core.DurableState[V]) map[K]core.DurableState[V] {
	cloned := make(map[K]core.DurableState[V], len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}
