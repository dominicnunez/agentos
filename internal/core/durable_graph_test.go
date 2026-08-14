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
