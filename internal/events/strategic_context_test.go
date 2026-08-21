package events

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestResolveStrategicContextSelectsExactLatestRevisions(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "initial direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "initial outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified evidence", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	stream := []Event{
		strategicProjectionEvent(t, 1, "MISSION_CREATED", "mission", mission.ID, 1, mission),
		strategicProjectionEvent(t, 2, "GOAL_CREATED", "goal", goal.ID, 1, goal),
	}
	mission.Statement = "refined direction"
	goal.Objective = "refined outcome"
	stream = append(stream,
		strategicProjectionEvent(t, 3, "MISSION_REVISED", "mission", mission.ID, 2, mission),
		strategicProjectionEvent(t, 4, "GOAL_REFINED", "goal", goal.ID, 2, goal),
	)

	context, eventRefs, versionRefs, err := ResolveStrategicContext("org-1", core.Work{ID: "work-1", GoalID: goal.ID}, stream, 0)
	if err != nil {
		t.Fatal(err)
	}
	if context.Mission.Statement != "refined direction" || context.Goal.Objective != "refined outcome" || context.MissionVersion != 2 || context.GoalVersion != 2 {
		t.Fatalf("latest strategic context was not selected: %+v", context)
	}
	if len(eventRefs) != 2 || eventRefs[0] != "event-3" || eventRefs[1] != "event-4" || len(versionRefs) != 2 || versionRefs[0].ID != "mission/mission-1" || versionRefs[1].ID != "goal/goal-1" {
		t.Fatalf("strategic provenance was not canonical: events=%v versions=%+v", eventRefs, versionRefs)
	}
	frozen, err := ResolveStrategicContextByRefs("org-1", core.Work{ID: "work-1", GoalID: goal.ID}, stream, eventRefs, versionRefs)
	if err != nil || frozen.MissionVersion != 2 || frozen.GoalVersion != 2 {
		t.Fatalf("exact strategic revisions were not replayed: context=%+v err=%v", frozen, err)
	}
	forged := append([]core.VersionedRef(nil), versionRefs...)
	forged[1].Version = "1"
	if _, err := ResolveStrategicContextByRefs("org-1", core.Work{ID: "work-1", GoalID: goal.ID}, stream, eventRefs, forged); err == nil {
		t.Fatal("forged strategic version reference was accepted")
	}

	prior, _, _, err := ResolveStrategicContext("org-1", core.Work{ID: "work-1", GoalID: goal.ID}, stream, 4)
	if err != nil || prior.MissionVersion != 2 || prior.GoalVersion != 1 {
		t.Fatalf("boundary did not freeze visible revisions: context=%+v err=%v", prior, err)
	}
}

func TestResolveStrategicContextRejectsMissingOrCrossTenantAncestry(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-2", Statement: "direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "evidence", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	stream := []Event{
		strategicProjectionEvent(t, 1, "MISSION_CREATED", "mission", mission.ID, 1, mission),
		strategicProjectionEvent(t, 2, "GOAL_CREATED", "goal", goal.ID, 1, goal),
	}
	if _, _, _, err := ResolveStrategicContext("org-1", core.Work{ID: "work-1", GoalID: goal.ID}, stream, 0); err == nil {
		t.Fatal("cross-organization Mission ancestry was accepted")
	}
	context, events, refs, err := ResolveStrategicContext("org-1", core.Work{ID: "work-2"}, stream, 0)
	if err != nil || context != nil || len(events) != 0 || len(refs) != 0 {
		t.Fatalf("ad hoc Work unexpectedly received strategic context: context=%+v events=%v refs=%v err=%v", context, events, refs, err)
	}
}

func TestResolvePlanBindsAcceptedIntentFingerprint(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: "goal-1", Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	plan := core.Plan{
		ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1,
		StrategicEventRefs: []string{"mission-event", "goal-event"},
		StrategicContextRefs: []core.VersionedRef{
			{ID: "mission/mission-1", Version: "1", MaterializationState: core.MaterializedFull},
			{ID: "goal/goal-1", Version: "1", MaterializationState: core.MaterializedFull},
		},
		Tasks: []core.PlanTask{{Key: "root", Description: "bounded work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed}}, CreatedAt: now,
	}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	planEvent := func(value core.Plan) Event {
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return Event{EventID: "plan-event", Sequence: 3, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-run-1", Payload: body, CorrelationID: "run-1", CreatedAt: now, SchemaVersion: SchemaVersion}
	}
	if resolved, err := ResolvePlan("org-1", "run-1", work, intent, []Event{planEvent(plan)}); err != nil || resolved.Fingerprint != plan.Fingerprint {
		t.Fatalf("valid accepted Plan was rejected: plan=%+v err=%v", resolved, err)
	}
	plan.IntentFingerprint = "different-reviewed-intent"
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	if _, err := ResolvePlan("org-1", "run-1", work, intent, []Event{planEvent(plan)}); err == nil {
		t.Fatal("self-consistent Plan for a different accepted Intent fingerprint was admitted")
	}
}

func TestExecutionStrategicContextUsesAtomicStartReferences(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "durable direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "admitted outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified evidence", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	strategicEventRefs := []string{"event-1", "event-2"}
	strategicContextRefs := []core.VersionedRef{
		{ID: "mission/mission-1", Version: "1", MaterializationState: core.MaterializedFull},
		{ID: "goal/goal-1", Version: "1", MaterializationState: core.MaterializedFull},
	}
	plan := core.Plan{
		ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1,
		StrategicEventRefs: strategicEventRefs, StrategicContextRefs: strategicContextRefs,
		Tasks: []core.PlanTask{{Key: "root", Description: "bounded work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed}}, CreatedAt: now,
	}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	planBody, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	stream := []Event{
		strategicProjectionEvent(t, 1, "MISSION_CREATED", "mission", mission.ID, 1, mission),
		strategicProjectionEvent(t, 2, "GOAL_CREATED", "goal", goal.ID, 1, goal),
		{EventID: "plan-event", Sequence: 3, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-run-1", Payload: planBody, CorrelationID: "run-1", CreatedAt: now, SchemaVersion: SchemaVersion},
	}
	start := strategicExecutionStartEvent(t, 4, strategicEventRefs, strategicContextRefs)
	stream = append(stream, start)
	goal.Objective = "newer outcome"
	stream = append(stream, strategicProjectionEvent(t, 5, "GOAL_REFINED", "goal", goal.ID, 2, goal))

	strategy, eventRefs, contextRefs, err := executionStrategicContext(WorkCompletionBinding{
		OrganizationID: "org-1", CorrelationID: "run-1", Work: work, Intent: intent,
	}, start, stream)
	if err != nil {
		t.Fatal(err)
	}
	if strategy.Goal.Objective != "admitted outcome" || strategy.GoalVersion != 1 {
		t.Fatalf("execution did not retain its atomically admitted strategy: %+v", strategy)
	}
	if !sameStrings(eventRefs, strategicEventRefs) || !sameVersionedRefs(contextRefs, strategicContextRefs) {
		t.Fatalf("execution strategy provenance changed: events=%v refs=%+v", eventRefs, contextRefs)
	}
}

func TestResolvePlanStrategicContextRejectsPostdatedRevisions(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "evidence", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	plan := core.Plan{
		ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1,
		StrategicEventRefs: []string{"event-4", "event-5"},
		StrategicContextRefs: []core.VersionedRef{
			{ID: "mission/mission-1", Version: "1", MaterializationState: core.MaterializedFull},
			{ID: "goal/goal-1", Version: "1", MaterializationState: core.MaterializedFull},
		},
		Tasks: []core.PlanTask{{Key: "root", Description: "bounded work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden}}, CreatedAt: now,
	}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	planBody, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	stream := []Event{
		{EventID: "plan-event", Sequence: 3, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-run-1", Payload: planBody, CorrelationID: "run-1", CreatedAt: now, SchemaVersion: SchemaVersion},
		strategicProjectionEvent(t, 4, "MISSION_CREATED", "mission", mission.ID, 1, mission),
		strategicProjectionEvent(t, 5, "GOAL_CREATED", "goal", goal.ID, 1, goal),
	}
	if _, _, err := ResolvePlanStrategicContext("org-1", "run-1", work, intent, stream); err == nil {
		t.Fatal("Plan accepted strategic revisions that were not durable when planning occurred")
	}
}

func TestValidateTaskExecutionStartRejectsMissingNonAgentStrategy(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "direction", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: mission.ID, Objective: "outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "evidence", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: goal.ID, Objective: "echo hello", Status: core.WorkActive, CreatedAt: now}
	refs := []string{"event-1", "event-2"}
	versions := []core.VersionedRef{
		{ID: "mission/mission-1", Version: "1", MaterializationState: core.MaterializedFull},
		{ID: "goal/goal-1", Version: "1", MaterializationState: core.MaterializedFull},
	}
	plan := core.Plan{
		ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, StrategicEventRefs: refs, StrategicContextRefs: versions,
		Tasks: []core.PlanTask{{Key: "root", Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden}}, CreatedAt: now,
	}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	planBody, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	stream := []Event{
		strategicProjectionEvent(t, 1, "MISSION_CREATED", "mission", mission.ID, 1, mission),
		strategicProjectionEvent(t, 2, "GOAL_CREATED", "goal", goal.ID, 1, goal),
		{EventID: "plan-event", Sequence: 3, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-run-1", Payload: planBody, CorrelationID: "run-1", CreatedAt: now, SchemaVersion: SchemaVersion},
	}
	task := core.Task{ID: "task-run-1", WorkID: work.ID, Description: "echo hello", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, Status: core.TaskRunning}
	valid := nonAgentStrategicExecutionStartEvent(t, 4, task, refs, versions)
	if err := ValidateTaskExecutionStart(valid, task, 2, work, intent, append(stream, valid)); err != nil {
		t.Fatalf("valid deterministic strategic start was rejected: %v", err)
	}
	missing := nonAgentStrategicExecutionStartEvent(t, 4, task, nil, nil)
	if err := ValidateTaskExecutionStart(missing, task, 2, work, intent, append(stream, missing)); err == nil {
		t.Fatal("deterministic replay accepted a start without its Goal-bound strategic references")
	}
}

func strategicExecutionStartEvent(t *testing.T, sequence int64, eventRefs []string, contextRefs []core.VersionedRef) Event {
	t.Helper()
	task := core.Task{ID: "task-run-1", WorkID: "work-1", Description: "bounded work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, Status: core.TaskRunning, AssigneeType: "AGENT", AssigneeID: "agent-1"}
	taskBody, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	record := ProjectionRecord{ProjectionKind: "task", RecordID: string(task.ID), Version: 2, CorrelationID: "run-1", Value: taskBody}
	event := Event{EventID: "start-event", Sequence: sequence, OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "run-1", CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion}
	detail, err := json.Marshal(ExecutionStartDetail{
		InboxCutoffSequence: sequence - 1, DispatchBinding: &AgentDispatchBinding{},
		StrategicEventRefs: append([]string(nil), eventRefs...), StrategicContextRefs: append([]core.VersionedRef(nil), contextRefs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := SealProjectionEvent(event, record, detail)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func nonAgentStrategicExecutionStartEvent(t *testing.T, sequence int64, task core.Task, eventRefs []string, contextRefs []core.VersionedRef) Event {
	t.Helper()
	taskBody, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	record := ProjectionRecord{ProjectionKind: "task", RecordID: string(task.ID), Version: 2, CorrelationID: "run-1", Value: taskBody}
	event := Event{EventID: "start-event", Sequence: sequence, OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "run-1", CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion}
	detail, err := json.Marshal(ExecutionStartDetail{StrategicEventRefs: append([]string(nil), eventRefs...), StrategicContextRefs: append([]core.VersionedRef(nil), contextRefs...)})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := SealProjectionEvent(event, record, detail)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func sameVersionedRefs(left, right []core.VersionedRef) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func strategicProjectionEvent(t *testing.T, sequence int64, eventType, kind string, id core.ID, version int, value any) Event {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		EventID: fmt.Sprintf("event-%d", sequence), Sequence: sequence, OrganizationID: "org-1", EventType: eventType,
		SourceActorID: "runtime", CorrelationID: kind + "-correlation", CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion,
	}
	if kind == "mission" && id == "mission-1" {
		var mission core.Mission
		if json.Unmarshal(body, &mission) == nil {
			event.OrganizationID = string(mission.OrganizationID)
		}
	}
	record := ProjectionRecord{ProjectionKind: kind, RecordID: string(id), Version: version, CorrelationID: event.CorrelationID, Value: body}
	sealed, err := SealProjectionEvent(event, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
