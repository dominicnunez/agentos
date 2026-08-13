// Package assignment selects durable Agents for bounded work. Selection proves
// roster compatibility only; it never grants a capability, approval, or effect
// authority.
package assignment

import (
	"fmt"
	"slices"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

const Active = "ACTIVE"

type Roster struct {
	Agents            map[core.ID]core.Agent
	Blueprints        map[core.ID]core.AgentBlueprint
	ExecutionProfiles map[core.ID]core.ExecutionProfile
}

type Requirement struct {
	OrganizationID             core.ID
	ExecutionKind              core.ExecutionKind
	RuntimeAdapter             string
	ModelProvider              string
	Model                      string
	ExecutionProfileVersion    string
	AvailableCapabilityClasses []string
}

type Selection struct {
	Agent            core.Agent
	Blueprint        core.AgentBlueprint
	ExecutionProfile core.ExecutionProfile
}

// Select returns the lexicographically first eligible Agent. The stable order
// makes assignment replayable and independent of Go map iteration.
func Select(roster Roster, requirement Requirement) (Selection, error) {
	if err := validateRequirement(requirement); err != nil {
		return Selection{}, err
	}
	ids := make([]core.ID, 0, len(roster.Agents))
	for id := range roster.Agents {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		agent := roster.Agents[id]
		if agent.ID != id {
			continue
		}
		selection, eligible := eligibleSelection(roster, agent, requirement)
		if eligible {
			return selection, nil
		}
	}
	return Selection{}, fmt.Errorf("no active Agent satisfies the bounded assignment requirement")
}

// ResolveAssigned revalidates one durable assignment immediately before work
// begins. A stale or tampered assignment therefore cannot fall back to another
// Agent or silently change execution profiles.
func ResolveAssigned(roster Roster, task core.Task, requirement Requirement) (Selection, error) {
	if task.AssigneeType != "AGENT" || task.AssigneeID == "" {
		return Selection{}, fmt.Errorf("task requires an explicit Agent assignment")
	}
	if task.ExecutionKind != requirement.ExecutionKind {
		return Selection{}, fmt.Errorf("task execution kind does not match its assignment requirement")
	}
	if err := validateRequirement(requirement); err != nil {
		return Selection{}, err
	}
	agent, ok := roster.Agents[task.AssigneeID]
	if !ok || agent.ID != task.AssigneeID {
		return Selection{}, fmt.Errorf("assigned Agent is not in the durable roster")
	}
	selection, eligible := eligibleSelection(roster, agent, requirement)
	if !eligible {
		return Selection{}, fmt.Errorf("assigned Agent no longer satisfies the bounded assignment requirement")
	}
	return selection, nil
}

func validateRequirement(requirement Requirement) error {
	if requirement.OrganizationID == "" || requirement.RuntimeAdapter == "" {
		return fmt.Errorf("organization and runtime adapter are required for assignment")
	}
	switch requirement.ExecutionKind {
	case core.ExecutionDeterministic:
	case core.ExecutionAgent:
		if requirement.ModelProvider == "" || requirement.Model == "" || requirement.ExecutionProfileVersion == "" {
			return fmt.Errorf("adaptive assignment requires an exact model execution profile")
		}
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionHuman, core.ExecutionMixed:
		return fmt.Errorf("execution kind %s is not assignable to an Agent", requirement.ExecutionKind)
	default:
		return fmt.Errorf("execution kind %s is not assignable to an Agent", requirement.ExecutionKind)
	}
	return nil
}

func eligibleSelection(roster Roster, agent core.Agent, requirement Requirement) (Selection, bool) {
	if agent.ID == "" || agent.OrganizationID != requirement.OrganizationID || agent.Status != Active || agent.RuntimeAdapter != requirement.RuntimeAdapter {
		return Selection{}, false
	}
	blueprint, ok := roster.Blueprints[agent.BlueprintID]
	if !ok || blueprint.ID != agent.BlueprintID || blueprint.OrganizationID != requirement.OrganizationID || blueprint.Version != agent.BlueprintVersion || blueprint.Status != Active {
		return Selection{}, false
	}
	if !containsAll(requirement.AvailableCapabilityClasses, blueprint.RequiredCapabilityClasses) {
		return Selection{}, false
	}
	profile, ok := roster.ExecutionProfiles[agent.ExecutionProfileID]
	if !ok || profile.ID != agent.ExecutionProfileID || profile.OrganizationID != requirement.OrganizationID || profile.Version != agent.ExecutionProfileVersion || profile.Status != Active {
		return Selection{}, false
	}
	if requirement.ExecutionKind == core.ExecutionAgent {
		if profile.ModelProvider != requirement.ModelProvider || profile.Model != requirement.Model || profile.Version != requirement.ExecutionProfileVersion {
			return Selection{}, false
		}
	}
	return Selection{Agent: agent, Blueprint: blueprint, ExecutionProfile: profile}, true
}

func containsAll(available, required []string) bool {
	for _, capability := range required {
		if !slices.Contains(available, capability) {
			return false
		}
	}
	return true
}
