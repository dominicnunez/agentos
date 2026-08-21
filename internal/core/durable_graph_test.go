package core

import (
	"strings"
	"testing"
)

func TestValidateDurableGraphRejectsInvalidTeamRoster(t *testing.T) {
	graph := DurableGraph{
		Organizations: map[ID]DurableState[Organization]{
			"org-1": {Value: Organization{ID: "org-1"}},
		},
		Teams: map[ID]DurableState[Team]{
			"team-1": {Value: Team{
				ID: "team-1", OrganizationID: "org-1", Name: "Team", Status: "ACTIVE",
				MemberAgentIDs: []ID{""},
			}},
		},
	}
	if err := ValidateDurableGraph(graph); err == nil || !strings.Contains(err.Error(), "empty member identity") {
		t.Fatalf("invalid Team roster graph error=%v", err)
	}
}

func TestValidateDurableGraphRejectsAmbiguousReplacementLineage(t *testing.T) {
	graph := DurableGraph{
		Organizations: map[ID]DurableState[Organization]{"org-1": {Value: Organization{ID: "org-1"}}},
		Intents: map[ID]DurableState[Intent]{
			"intent-old": {CorrelationID: "old", Value: Intent{ID: "intent-old", OrganizationID: "org-1", NormalizedObjective: "old"}},
			"intent-a":   {CorrelationID: "a", Value: Intent{ID: "intent-a", OrganizationID: "org-1", ReplacesWorkID: "work-old", NormalizedObjective: "a"}},
			"intent-b":   {CorrelationID: "b", Value: Intent{ID: "intent-b", OrganizationID: "org-1", ReplacesWorkID: "work-old", NormalizedObjective: "b"}},
		},
		Works: map[ID]DurableState[Work]{
			"work-old": {CorrelationID: "old", Value: Work{ID: "work-old", IntentID: "intent-old", Objective: "old", Status: WorkFailed}},
			"work-a":   {CorrelationID: "a", Value: Work{ID: "work-a", IntentID: "intent-a", ReplacesWorkID: "work-old", Objective: "a", Status: WorkActive}},
			"work-b":   {CorrelationID: "b", Value: Work{ID: "work-b", IntentID: "intent-b", ReplacesWorkID: "work-old", Objective: "b", Status: WorkActive}},
		},
	}
	if err := ValidateDurableGraph(graph); err == nil || !strings.Contains(err.Error(), "multiple replacements") {
		t.Fatalf("ambiguous replacement graph error=%v", err)
	}

	graph.Intents = map[ID]DurableState[Intent]{
		"intent-a": {CorrelationID: "a", Value: Intent{ID: "intent-a", OrganizationID: "org-1", ReplacesWorkID: "work-b", NormalizedObjective: "a"}},
		"intent-b": {CorrelationID: "b", Value: Intent{ID: "intent-b", OrganizationID: "org-1", ReplacesWorkID: "work-a", NormalizedObjective: "b"}},
	}
	graph.Works = map[ID]DurableState[Work]{
		"work-a": {CorrelationID: "a", Value: Work{ID: "work-a", IntentID: "intent-a", ReplacesWorkID: "work-b", Objective: "a", Status: WorkFailed}},
		"work-b": {CorrelationID: "b", Value: Work{ID: "work-b", IntentID: "intent-b", ReplacesWorkID: "work-a", Objective: "b", Status: WorkFailed}},
	}
	if err := ValidateDurableGraph(graph); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cyclic replacement graph error=%v", err)
	}
}
