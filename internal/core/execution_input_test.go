package core

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMaterializeAgentExecutionInputBindsAllDurableContext(t *testing.T) {
	task := Task{ID: "task-1", Description: "deliver the report", ExecutionBrief: "reviewed objective"}
	blueprint := AgentBlueprint{ID: "blueprint-1", Version: "v1", Role: "analyst", OperatingInstructions: "cite evidence"}
	created := time.Unix(2, 0).UTC()
	materialized, input, err := MaterializeAgentExecutionInput(AgentExecutionInputContext{
		Blueprint: blueprint,
		Task:      task,
		Strategy: &StrategicContext{
			Mission: Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "build durable value", Status: MissionActive, CreatedAt: created}, MissionVersion: 2,
			Goal: Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "deliver a verified report", Mode: GoalTarget, SuccessCriteria: []IntentValue{{Value: "report accepted", Origin: "USER"}}, Status: GoalActive, CreatedAt: created}, GoalVersion: 3,
		},
		DependencyResults: []AgentExecutionDependencyResult{{
			TaskID: "task-dependency", ResultEvent: "evt-result", Summary: "verified", ArtifactRefs: []string{"artifact-1"},
		}},
		InboxEvents: []AgentExecutionInboxEvent{{
			Sequence: 2, EventID: "evt-message", EventType: "MESSAGE", SourceActorID: "agent-2",
			RecipientScope: "AGENT", RecipientID: "agent-1", CreatedAt: created, Payload: json.RawMessage(`{"text":"consider this"}`),
		}},
		Revision: &AgentExecutionRevision{EventRef: "evt-revision", ReviewerID: "operator-1", UntrustedText: "add evidence"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"blueprint-1", "reviewed objective", "mission-1", "build durable value", "goal-1", "deliver a verified report", "evt-result", "evt-message", "evt-revision", "add evidence"} {
		if !strings.Contains(input, expected) {
			t.Fatalf("materialized input omitted %q: %s", expected, input)
		}
	}
	if materialized.ExecutionBrief != input || materialized.Description == task.Description {
		t.Fatal("materialized Task and exact execution input diverged")
	}
}

func TestMaterializeAgentExecutionInputRejectsInvalidStrategicContext(t *testing.T) {
	_, _, err := MaterializeAgentExecutionInput(AgentExecutionInputContext{
		Blueprint: AgentBlueprint{ID: "blueprint-1", Version: "v1"},
		Task:      Task{Description: "work"},
		Strategy: &StrategicContext{
			Mission: Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "direction", Status: MissionActive}, MissionVersion: 1,
			Goal: Goal{ID: "goal-1", OrganizationID: "org-2", MissionID: "mission-1", Objective: "outcome", Mode: GoalTarget, SuccessCriteria: []IntentValue{{Value: "evidence", Origin: "USER"}}, Status: GoalActive}, GoalVersion: 1,
		},
	})
	if err == nil {
		t.Fatal("cross-organization strategic context was accepted")
	}
}

func TestMaterializeAgentExecutionInputRejectsMixedDependencyModes(t *testing.T) {
	_, _, err := MaterializeAgentExecutionInput(AgentExecutionInputContext{
		Blueprint:           AgentBlueprint{ID: "blueprint-1", Version: "v1"},
		Task:                Task{Description: "work"},
		DependencyResults:   []AgentExecutionDependencyResult{{TaskID: "task-1"}},
		BlockedDependencies: []AgentExecutionBlockedDependency{{TaskID: "task-2"}},
	})
	if err == nil {
		t.Fatal("mixed completed and blocked dependency evidence was accepted")
	}
}
