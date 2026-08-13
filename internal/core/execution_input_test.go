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
	for _, expected := range []string{"blueprint-1", "reviewed objective", "evt-result", "evt-message", "evt-revision", "add evidence"} {
		if !strings.Contains(input, expected) {
			t.Fatalf("materialized input omitted %q: %s", expected, input)
		}
	}
	if materialized.ExecutionBrief != input || materialized.Description == task.Description {
		t.Fatal("materialized Task and exact execution input diverged")
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
