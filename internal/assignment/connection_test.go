package assignment

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestAssignmentPinsSameModelConnectionAcrossAgentUpdate(t *testing.T) {
	roster := testRoster()
	first := roster.ExecutionProfiles["profile-1"]
	first.ConnectionID = "account-a"
	roster.ExecutionProfiles[first.ID] = first
	second := first
	second.ID, second.ConnectionID = "profile-2", "account-b"
	roster.ExecutionProfiles[second.ID] = second
	other := roster.Agents["agent-b"]
	other.ID, other.ExecutionProfileID = "agent-a", second.ID
	roster.Agents[other.ID] = other
	requirement := testRequirement()
	requirement.ConnectionID = first.ConnectionID
	selected, err := Select(roster, requirement)
	if err != nil || selected.ExecutionProfile.ID != first.ID {
		t.Fatalf("same-model account replaced required connection: %+v, %v", selected, err)
	}
	task := core.Task{ExecutionKind: core.ExecutionAgent, AssigneeType: "AGENT", AssigneeID: selected.Agent.ID, AgentConfig: Config(selected)}
	updated := selected.Agent
	updated.ExecutionProfileID = second.ID
	roster.Agents[updated.ID] = updated
	resolved, err := ResolveAssigned(roster, task, requirement)
	if err != nil || resolved.ExecutionProfile.ConnectionID != first.ConnectionID {
		t.Fatalf("agent update redirected approved task: %+v, %v", resolved, err)
	}
	for _, connection := range []string{"", "account-b", "unconfigured", "../account-a"} {
		requirement.ConnectionID = connection
		if _, err := ResolveAssigned(roster, task, requirement); err == nil {
			t.Fatalf("approved task accepted substituted connection %q", connection)
		}
	}
	if core.ValidExecutionProfileRevision(first, second) {
		t.Fatal("connection replacement was accepted as a profile revision")
	}
	second = first
	second.ConnectionID = "account-b"
	if core.ValidExecutionProfileRevision(first, second) {
		t.Fatal("same profile identity could change accounts")
	}
}

func TestAssignmentLegacyConnectionDoesNotSelectNamedAccount(t *testing.T) {
	roster := testRoster()
	profile := roster.ExecutionProfiles["profile-1"]
	profile.ConnectionID = "account-a"
	roster.ExecutionProfiles[profile.ID] = profile
	if _, err := Select(roster, testRequirement()); err == nil {
		t.Fatal("legacy empty identity selected a named connection")
	}
}
