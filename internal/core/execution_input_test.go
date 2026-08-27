package core

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMaterializeAgentExecutionInputBindsAllDurableContext(t *testing.T) {
	task := Task{ID: "task-1", Description: "deliver the report", ExecutionBrief: "reviewed objective"}
	blueprint := AgentBlueprint{ID: "blueprint-1", Version: "v1", Role: "analyst", OperatingInstructions: "cite evidence"}
	created := time.Unix(2, 0).UTC()
	verified := created.Add(time.Second)
	supersedes := 1
	materialized, input, err := MaterializeAgentExecutionInput(AgentExecutionInputContext{
		Blueprint: blueprint,
		Task:      task,
		Strategy: &StrategicContext{
			Mission: Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "build durable value", Status: MissionActive, CreatedAt: created}, MissionVersion: 2,
			Goal: Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "deliver a verified report", Mode: GoalTarget, SuccessCriteria: []IntentValue{{Value: "report accepted", Origin: "USER"}}, Status: GoalActive, CreatedAt: created}, GoalVersion: 3,
		},
		Knowledge: []KnowledgeRecord{{
			KnowledgeID: "knowledge-1", OrganizationID: "org-1", Version: 2, Type: KnowledgeProcedure, Scope: KnowledgeScopeOrganization, ScopeID: "org-1",
			Status: KnowledgeActive, Title: "Evidence procedure", Content: "Verify the cited evidence.", Basis: KnowledgeBasisHumanInput,
			ProvenanceEventRefs: []string{"event-proposal"}, EvidenceArtifactRefs: []string{}, CreatedBy: "user-1", CreatedByKind: PrincipalHuman,
			CreatedAt: created, LastVerifiedAt: &verified, ValidationMethod: KnowledgeValidationHuman, ValidationRefs: []string{"event-validation"},
			ValidatedBy: "user-2", ValidatedByKind: PrincipalHuman, SupersedesVersion: &supersedes,
		}},
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
	for _, expected := range []string{"blueprint-1", "reviewed objective", "mission-1", "build durable value", "goal-1", "deliver a verified report", "knowledge-1", "Verify the cited evidence.", "evt-result", "evt-message", "evt-revision", "add evidence"} {
		if !strings.Contains(input, expected) {
			t.Fatalf("materialized input omitted %q: %s", expected, input)
		}
	}
	if materialized.ExecutionBrief != input || materialized.Description == task.Description {
		t.Fatal("materialized Task and exact execution input diverged")
	}
}

func TestMaterializeAgentExecutionInputRejectsNonActiveOrDuplicateKnowledge(t *testing.T) {
	record := KnowledgeRecord{KnowledgeID: "knowledge-1", Status: KnowledgeCandidate}
	if _, _, err := MaterializeAgentExecutionInput(AgentExecutionInputContext{
		Blueprint: AgentBlueprint{ID: "blueprint-1", Version: "v1"}, Task: Task{Description: "work"}, Knowledge: []KnowledgeRecord{record},
	}); err == nil {
		t.Fatal("non-active execution knowledge was accepted")
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

func TestValidateStrategicExecutionContextRejectsOversizedDirectionBeforeStart(t *testing.T) {
	created := time.Unix(2, 0).UTC()
	context := &StrategicContext{
		Mission: Mission{ID: "mission-1", OrganizationID: "org-1", Statement: strings.Repeat("x", maximumExecutionContextBytes), Status: MissionActive, CreatedAt: created}, MissionVersion: 1,
		Goal: Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded outcome", Mode: GoalTarget, SuccessCriteria: []IntentValue{{Value: "evidence", Origin: "USER"}}, Status: GoalActive, CreatedAt: created}, GoalVersion: 1,
	}
	if err := ValidateStrategicExecutionContext(context); err == nil {
		t.Fatal("oversized strategic direction crossed the pre-start execution-context bound")
	}
}

func TestMaterializeAgentExecutionInputRejectsOversizedAggregate(t *testing.T) {
	_, _, err := MaterializeAgentExecutionInput(AgentExecutionInputContext{
		Blueprint: AgentBlueprint{ID: "blueprint-1", Version: "v1", Role: "worker", OperatingInstructions: strings.Repeat("x", maximumExecutionContextBytes)},
		Task:      Task{Description: "bounded work"},
	})
	if !errors.Is(err, ErrExecutionContextLimitExceeded) {
		t.Fatalf("oversized aggregate execution input was not rejected: %v", err)
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
