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
