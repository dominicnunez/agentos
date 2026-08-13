package assignment

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestSelectUsesStableEligibleAgentWithoutGrantingRequirements(t *testing.T) {
	roster := testRoster()
	second := roster.Agents["agent-b"]
	second.ID = "agent-a"
	roster.Agents[second.ID] = second
	requirement := testRequirement()

	selection, err := Select(roster, requirement)
	if err != nil {
		t.Fatal(err)
	}
	if selection.Agent.ID != "agent-a" {
		t.Fatalf("selection was not stable: %+v", selection.Agent)
	}
	selection.Blueprint.RequiredCapabilityClasses = []string{"repository.write"}
	roster.Blueprints[selection.Blueprint.ID] = selection.Blueprint
	if _, err := Select(roster, requirement); err == nil {
		t.Fatal("blueprint requirement was treated as an implicit capability grant")
	}
}

func TestSelectExcludesInactiveAndMismatchedRosterEntries(t *testing.T) {
	tests := map[string]func(Roster){
		"inactive Agent": func(roster Roster) {
			agent := roster.Agents["agent-b"]
			agent.Status = "INACTIVE"
			roster.Agents[agent.ID] = agent
		},
		"inactive blueprint": func(roster Roster) {
			blueprint := roster.Blueprints["blueprint-1"]
			blueprint.Status = "INACTIVE"
			roster.Blueprints[blueprint.ID] = blueprint
		},
		"inactive profile": func(roster Roster) {
			profile := roster.ExecutionProfiles["profile-1"]
			profile.Status = "INACTIVE"
			roster.ExecutionProfiles[profile.ID] = profile
		},
		"wrong provider": func(roster Roster) {
			profile := roster.ExecutionProfiles["profile-1"]
			profile.ModelProvider = "other"
			roster.ExecutionProfiles[profile.ID] = profile
		},
		"wrong organization": func(roster Roster) {
			agent := roster.Agents["agent-b"]
			agent.OrganizationID = "org-2"
			roster.Agents[agent.ID] = agent
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			roster := testRoster()
			mutate(roster)
			if _, err := Select(roster, testRequirement()); err == nil {
				t.Fatal("ineligible Agent was selected")
			}
		})
	}
}

func TestResolveAssignedNeverFallsBackToAnotherAgent(t *testing.T) {
	roster := testRoster()
	inactive := roster.Agents["agent-b"]
	inactive.Status = "INACTIVE"
	roster.Agents[inactive.ID] = inactive
	active := inactive
	active.ID = "agent-c"
	active.Status = Active
	roster.Agents[active.ID] = active
	task := core.Task{ExecutionKind: core.ExecutionAgent, AssigneeType: "AGENT", AssigneeID: inactive.ID, AgentConfig: agentConfig(inactive)}

	if _, err := ResolveAssigned(roster, task, testRequirement()); err == nil {
		t.Fatal("stale assignment fell back to a different active Agent")
	}
}

func TestResolveAssignedBindsTaskKindAndRosterIdentity(t *testing.T) {
	roster := testRoster()
	task := core.Task{ExecutionKind: core.ExecutionDeterministic, AssigneeType: "AGENT", AssigneeID: "agent-b", AgentConfig: agentConfig(roster.Agents["agent-b"])}
	if _, err := ResolveAssigned(roster, task, testRequirement()); err == nil {
		t.Fatal("assignment requirement changed the durable task execution kind")
	}
	bad := roster.Agents[task.AssigneeID]
	bad.ID = "different-agent"
	roster.Agents[task.AssigneeID] = bad
	task.ExecutionKind = core.ExecutionAgent
	if _, err := ResolveAssigned(roster, task, testRequirement()); err == nil {
		t.Fatal("roster map key substituted a different Agent identity")
	}
}

func TestResolveAssignedUsesTaskPinnedConfigurationAfterAgentUpdate(t *testing.T) {
	roster := testRoster()
	agent := roster.Agents["agent-b"]
	task := core.Task{ExecutionKind: core.ExecutionAgent, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: agentConfig(agent)}
	replacement := roster.ExecutionProfiles[agent.ExecutionProfileID]
	replacement.ID = "profile-2"
	replacement.Version = "profile-v2"
	replacement.Model = "new-model"
	roster.ExecutionProfiles[replacement.ID] = replacement
	agent.ExecutionProfileID = replacement.ID
	agent.ExecutionProfileVersion = replacement.Version
	agent.RuntimeAdapter = "new-runtime"
	roster.Agents[agent.ID] = agent

	selection, err := ResolveAssigned(roster, task, testRequirement())
	if err != nil {
		t.Fatal(err)
	}
	if selection.Agent.ID != agent.ID || selection.ExecutionProfile.ID != "profile-1" {
		t.Fatalf("durable Task configuration was replaced by current Agent configuration: %+v", selection)
	}
}

func TestDeterministicAssignmentDoesNotRequireConfiguredModel(t *testing.T) {
	roster := testRoster()
	profile := roster.ExecutionProfiles["profile-1"]
	profile.Status = "INACTIVE"
	roster.ExecutionProfiles[profile.ID] = profile
	requirement := testRequirement()
	requirement.ExecutionKind = core.ExecutionDeterministic
	requirement.ModelProvider = ""
	requirement.Model = ""
	requirement.ExecutionProfileVersion = ""
	requirement.PromptVersion = ""
	requirement.ToolRefs = nil
	if _, err := Select(roster, requirement); err != nil {
		t.Fatalf("deterministic work was coupled to model-profile availability: %v", err)
	}
}

func TestAdaptiveAssignmentRejectsUnsupportedExecutionProfileSettings(t *testing.T) {
	tests := map[string]func(*core.ExecutionProfile){
		"prompt version":    func(profile *core.ExecutionProfile) { profile.PromptVersion = "other-prompt" },
		"reasoning setting": func(profile *core.ExecutionProfile) { profile.ReasoningSetting = "high" },
		"tool references":   func(profile *core.ExecutionProfile) { profile.ToolRefs = []string{"unavailable-tool"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			roster := testRoster()
			profile := roster.ExecutionProfiles["profile-1"]
			mutate(&profile)
			roster.ExecutionProfiles[profile.ID] = profile
			if _, err := Select(roster, testRequirement()); err == nil {
				t.Fatal("unsupported execution-profile setting was accepted")
			}
		})
	}
}

func testRoster() Roster {
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: "org-1", Version: "blueprint-v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: Active}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: "org-1", Version: "profile-v1", ModelProvider: "provider", Model: "model", PromptVersion: "prompt-v1", ToolRefs: []string{}, Status: Active}
	agent := core.Agent{ID: "agent-b", OrganizationID: "org-1", BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: Active}
	return Roster{
		Agents:            map[core.ID]core.Agent{agent.ID: agent},
		Blueprints:        map[core.ID]core.AgentBlueprint{blueprint.ID: blueprint},
		ExecutionProfiles: map[core.ID]core.ExecutionProfile{profile.ID: profile},
	}
}

func testRequirement() Requirement {
	return Requirement{OrganizationID: "org-1", ExecutionKind: core.ExecutionAgent, RuntimeAdapter: "local", ModelProvider: "provider", Model: "model", ExecutionProfileVersion: "profile-v1", PromptVersion: "prompt-v1", ToolRefs: []string{}, AvailableCapabilityClasses: []string{}}
}

func agentConfig(agent core.Agent) *core.AgentConfig {
	return &core.AgentConfig{
		BlueprintID: agent.BlueprintID, BlueprintVersion: agent.BlueprintVersion,
		ProfileID: agent.ExecutionProfileID, ProfileVersion: agent.ExecutionProfileVersion,
		RuntimeAdapter: agent.RuntimeAdapter,
	}
}
