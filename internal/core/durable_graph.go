package core

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// DurableState is the current admitted value of one versioned organizational
// record. CorrelationID binds Intent, Work, and Task streams.
type DurableState[T any] struct {
	Version       int
	CorrelationID string
	// Generic instantiations are read throughout validation, but Gallow cannot
	// currently connect those reads to this generic declaration.
	// gallow-ignore-next-line unused-field
	Value T
}

// AdmitDurableRevision applies the ordering, correlation, and immutable-value
// rules shared by routine materialization and recovery certification.
func AdmitDurableRevision[T any](target map[ID]DurableState[T], recordID ID, version int, correlationID string, value T, correlationStable bool, validRevision func(T, T) bool) error {
	previous, exists := target[recordID]
	wantVersion := 1
	if exists {
		wantVersion = previous.Version + 1
	}
	if version != wantVersion {
		return fmt.Errorf("record %s version %d follows %d", recordID, version, previous.Version)
	}
	if correlationStable {
		if correlationID == "" {
			return fmt.Errorf("record %s version %d has no correlation boundary", recordID, version)
		}
		if exists && correlationID != previous.CorrelationID {
			return fmt.Errorf("record %s changes correlation boundary at version %d", recordID, version)
		}
	}
	if exists && validRevision != nil && !validRevision(previous.Value, value) {
		return fmt.Errorf("record %s changes immutable configuration at version %d", recordID, version)
	}
	target[recordID] = DurableState[T]{Version: version, CorrelationID: correlationID, Value: value}
	return nil
}

// ValidAgentBlueprintRevision permits status changes while preserving the
// exact blueprint definition selected by durable Agent configurations.
func ValidAgentBlueprintRevision(previous, next AgentBlueprint) bool {
	next.Status = previous.Status
	return reflect.DeepEqual(previous, next)
}

// ValidExecutionProfileRevision permits status changes while preserving the
// exact provider, model, prompt, and tool configuration.
func ValidExecutionProfileRevision(previous, next ExecutionProfile) bool {
	next.Status = previous.Status
	return reflect.DeepEqual(previous, next)
}

// ValidTeamRevision permits roster and descriptive changes while preserving
// durable Team identity, tenant ownership, and creation time.
func ValidTeamRevision(previous, next Team) bool {
	return previous.ID == next.ID && previous.OrganizationID == next.OrganizationID && previous.CreatedAt.Equal(next.CreatedAt)
}

// ValidateTeamRoster proves that one Team has a complete definition, a
// durable parent Organization, and unique same-organization Agent members.
// Startup replay and recovery certification share this exact boundary.
func ValidateTeamRoster(team Team, graph DurableGraph) error {
	if strings.TrimSpace(team.Name) == "" || strings.TrimSpace(team.Status) == "" {
		return fmt.Errorf("team is incomplete")
	}
	organization, found := graph.Organizations[team.OrganizationID]
	if !found || organization.Value.ID != team.OrganizationID {
		return fmt.Errorf("team requires its durable parent Organization")
	}
	members := make(map[ID]struct{}, len(team.MemberAgentIDs))
	for _, memberID := range team.MemberAgentIDs {
		if memberID == "" {
			return fmt.Errorf("team contains an empty member identity")
		}
		if _, duplicate := members[memberID]; duplicate {
			return fmt.Errorf("team contains a duplicate member identity")
		}
		members[memberID] = struct{}{}
		agent, found := graph.Agents[memberID]
		if !found || agent.Value.ID != memberID || agent.Value.OrganizationID != team.OrganizationID {
			return fmt.Errorf("team references invalid member Agent %s", memberID)
		}
	}
	return nil
}

// ValidAgentRevision preserves durable Agent identity and tenant ownership
// while allowing reviewed configuration and lifecycle changes.
func ValidAgentRevision(previous, next Agent) bool {
	return ValidAgent(previous) && ValidAgent(next) && previous.ID == next.ID && previous.OrganizationID == next.OrganizationID
}

// ValidAgent reports whether a durable Agent has a complete pinned runtime
// configuration and a recognized lifecycle state.
func ValidAgent(agent Agent) bool {
	return agent.ID != "" && agent.OrganizationID != "" && agent.BlueprintID != "" && agent.BlueprintVersion != "" &&
		agent.ExecutionProfileID != "" && agent.ExecutionProfileVersion != "" && agent.RuntimeAdapter != "" &&
		(agent.Status == "ACTIVE" || agent.Status == "INACTIVE")
}

// ValidAgentConfigurationBinding proves that an Agent's pinned blueprint and
// execution profile are exact, same-organization durable definitions.
func ValidAgentConfigurationBinding(agent Agent, blueprint AgentBlueprint, profile ExecutionProfile) bool {
	return blueprint.ID == agent.BlueprintID && blueprint.OrganizationID == agent.OrganizationID && blueprint.Version == agent.BlueprintVersion &&
		profile.ID == agent.ExecutionProfileID && profile.OrganizationID == agent.OrganizationID && profile.Version == agent.ExecutionProfileVersion
}

// ValidAgentBlueprint reports whether one complete blueprint definition can
// enter durable organizational state.
func ValidAgentBlueprint(blueprint AgentBlueprint) bool {
	return blueprint.ID != "" && blueprint.OrganizationID != "" && blueprint.Version != "" &&
		strings.TrimSpace(blueprint.Role) != "" && strings.TrimSpace(blueprint.OperatingInstructions) != "" &&
		validDurableRosterStatus(blueprint.Status) && DistinctNonemptyStrings(blueprint.RequiredCapabilityClasses)
}

// ValidExecutionProfile reports whether one complete execution profile can
// enter durable organizational state.
func ValidExecutionProfile(profile ExecutionProfile) bool {
	return profile.ID != "" && profile.OrganizationID != "" && profile.Version != "" && profile.ModelProvider != "" &&
		profile.Model != "" && profile.PromptVersion != "" && validDurableRosterStatus(profile.Status) && DistinctNonemptyStrings(profile.ToolRefs)
}

// DistinctNonemptyStrings rejects empty, whitespace-only, and duplicate
// durable references without normalizing their security-relevant identity.
func DistinctNonemptyStrings(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

// DurableGraph is the current organizational state whose cross-record
// relationships must remain valid regardless of how it was materialized.
type DurableGraph struct {
	Organizations       map[ID]DurableState[Organization]
	Missions            map[ID]DurableState[Mission]
	Goals               map[ID]DurableState[Goal]
	Teams               map[ID]DurableState[Team]
	AgentBlueprints     map[ID]DurableState[AgentBlueprint]
	ExecutionProfiles   map[ID]DurableState[ExecutionProfile]
	Agents              map[ID]DurableState[Agent]
	Intents             map[ID]DurableState[Intent]
	Works               map[ID]DurableState[Work]
	Tasks               map[ID]DurableState[Task]
	Experiments         map[ID]DurableState[Experiment]
	PromotionCandidates map[ID]DurableState[PromotionCandidate]
	Knowledge           map[ID]DurableState[KnowledgeRecord]
}

// ValidateDurableGraph applies the complete fail-closed organizational graph
// contract shared by routine materialization and recovery certification.
func ValidateDurableGraph(graph DurableGraph) error {
	for id, state := range graph.Organizations {
		if err := validateDurableIdentity("organization", id, state.Value.ID); err != nil {
			return err
		}
	}
	organized := make([]durableOrganizedIdentity, 0, len(graph.Missions)+len(graph.Goals)+len(graph.AgentBlueprints)+len(graph.ExecutionProfiles)+len(graph.Agents)+len(graph.Teams)+len(graph.Intents)+len(graph.Experiments)+len(graph.PromotionCandidates)+len(graph.Knowledge))
	for id, state := range graph.Missions {
		organized = append(organized, durableOrganizedIdentity{"mission", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.Goals {
		organized = append(organized, durableOrganizedIdentity{"goal", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.AgentBlueprints {
		organized = append(organized, durableOrganizedIdentity{"Agent blueprint", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.ExecutionProfiles {
		organized = append(organized, durableOrganizedIdentity{"execution profile", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.Agents {
		organized = append(organized, durableOrganizedIdentity{"agent", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.Teams {
		organized = append(organized, durableOrganizedIdentity{"team", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.Intents {
		organized = append(organized, durableOrganizedIdentity{"intent", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.Experiments {
		organized = append(organized, durableOrganizedIdentity{"experiment", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.PromotionCandidates {
		organized = append(organized, durableOrganizedIdentity{"promotion candidate", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range graph.Knowledge {
		organized = append(organized, durableOrganizedIdentity{"knowledge", id, state.Value.KnowledgeID, state.Value.OrganizationID})
	}
	for _, record := range organized {
		if err := validateDurableOrganizedIdentity(record, graph.Organizations); err != nil {
			return err
		}
	}
	if err := validateDurableRoster(graph); err != nil {
		return err
	}
	for id, state := range graph.Missions {
		if !ValidMission(state.Value) {
			return fmt.Errorf("mission %s is incomplete or has unsupported status", id)
		}
	}
	for id, state := range graph.Goals {
		goal := state.Value
		mission, ok := graph.Missions[goal.MissionID]
		if !ok || mission.Value.OrganizationID != goal.OrganizationID {
			return fmt.Errorf("goal %s references invalid mission %s", id, goal.MissionID)
		}
		if !ValidGoal(goal) {
			return fmt.Errorf("goal %s is incomplete or has unsupported mode or status", id)
		}
	}
	for id, state := range graph.Teams {
		if err := ValidateTeamRoster(state.Value, graph); err != nil {
			return fmt.Errorf("team %s: %w", id, err)
		}
	}
	for id, state := range graph.Intents {
		if state.Value.ReplacesWorkID != "" && !ValidWorkReferenceID(string(state.Value.ReplacesWorkID)) {
			return fmt.Errorf("intent %s references an invalid replacement Work", id)
		}
	}
	replacementByPredecessor := make(map[ID]ID)
	replacementOf := make(map[ID]ID)
	for id, state := range graph.Works {
		if err := validateDurableIdentity("work", id, state.Value.ID); err != nil {
			return err
		}
		if state.Value.Status != WorkActive && state.Value.Status != WorkCompleted && state.Value.Status != WorkFailed {
			return fmt.Errorf("work %s has unsupported status %s", id, state.Value.Status)
		}
		intent, ok := graph.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("work %s references missing intent %s", id, state.Value.IntentID)
		}
		if state.Value.GoalID != intent.Value.GoalID {
			return fmt.Errorf("work %s does not match its accepted intent goal", id)
		}
		if state.Value.Objective != intent.Value.NormalizedObjective {
			return fmt.Errorf("work %s does not match its accepted intent objective", id)
		}
		if state.Value.ReplacesWorkID != intent.Value.ReplacesWorkID {
			return fmt.Errorf("work %s does not match its accepted intent replacement lineage", id)
		}
		if state.CorrelationID == "" || intent.CorrelationID != state.CorrelationID {
			return fmt.Errorf("work %s crosses its intent correlation boundary", id)
		}
		if state.Value.GoalID != "" {
			goal, ok := graph.Goals[state.Value.GoalID]
			if !ok || goal.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("work %s references invalid goal %s", id, state.Value.GoalID)
			}
		}
		if predecessorID := state.Value.ReplacesWorkID; predecessorID != "" {
			predecessor, ok := graph.Works[predecessorID]
			if !ok || predecessor.Value.Status != WorkFailed || predecessor.Value.GoalID != state.Value.GoalID {
				return fmt.Errorf("work %s does not replace a failed Work with the same Goal binding", id)
			}
			predecessorIntent, ok := graph.Intents[predecessor.Value.IntentID]
			if !ok || predecessorIntent.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("work %s replacement lineage crosses its organization", id)
			}
			if existing, duplicate := replacementByPredecessor[predecessorID]; duplicate && existing != id {
				return fmt.Errorf("failed work %s has multiple replacements", predecessorID)
			}
			replacementByPredecessor[predecessorID] = id
			replacementOf[id] = predecessorID
		}
	}
	for start := range replacementOf {
		seen := make(map[ID]struct{})
		for current := start; current != ""; current = replacementOf[current] {
			if _, cycle := seen[current]; cycle {
				return fmt.Errorf("work replacement lineage contains a cycle at %s", current)
			}
			seen[current] = struct{}{}
		}
	}
	tasks := make(map[ID]Task, len(graph.Tasks))
	for id, state := range graph.Tasks {
		task := state.Value
		if err := validateDurableIdentity("task", id, task.ID); err != nil {
			return err
		}
		if !ValidTask(task) {
			return fmt.Errorf("task %s has an incomplete or unsupported execution contract", id)
		}
		work, ok := graph.Works[task.WorkID]
		if !ok {
			return fmt.Errorf("task %s references missing work %s", id, task.WorkID)
		}
		if state.CorrelationID == "" || work.CorrelationID != state.CorrelationID {
			return fmt.Errorf("task %s crosses its work correlation boundary", id)
		}
		intent := graph.Intents[work.Value.IntentID]
		if err := ValidateTaskAssignment(task, intent.Value.OrganizationID, graph); err != nil {
			return err
		}
		if task.ParentID != "" {
			parent, ok := graph.Tasks[task.ParentID]
			if !ok || parent.Value.WorkID != task.WorkID || parent.CorrelationID != state.CorrelationID || task.ParentID == id {
				return fmt.Errorf("task %s references invalid parent %s", id, task.ParentID)
			}
		}
		for _, dependencyID := range task.DependsOn {
			dependency, ok := graph.Tasks[dependencyID]
			if !ok || dependency.Value.WorkID != task.WorkID || dependency.CorrelationID != state.CorrelationID || dependencyID == id {
				return fmt.Errorf("task %s references invalid dependency %s", id, dependencyID)
			}
		}
		tasks[id] = task
	}
	if err := ValidateTaskDAG(tasks); err != nil {
		return err
	}
	for id, state := range graph.Experiments {
		experiment := state.Value
		if !ValidExperiment(experiment) {
			return fmt.Errorf("experiment %s is incomplete or unsafe", id)
		}
		work, found := graph.Works[experiment.WorkID]
		if !found || state.CorrelationID == "" || work.CorrelationID != state.CorrelationID {
			return fmt.Errorf("experiment %s crosses its Work correlation boundary", id)
		}
		intent := graph.Intents[work.Value.IntentID]
		if experiment.OrganizationID != intent.Value.OrganizationID || experiment.Objective != work.Value.Objective {
			return fmt.Errorf("experiment %s crosses its Work organization or objective boundary", id)
		}
		// A terminal Work may briefly coexist with a RUNNING experiment after a
		// crash; startup reconciliation closes it from the durable Work event.
		if experiment.Status != ExperimentRunning && !ValidTerminalExperimentWorkStatus(experiment, work.Value) {
			return fmt.Errorf("experiment %s conflicts with its Work lifecycle", id)
		}
	}
	for id, state := range graph.PromotionCandidates {
		candidate := state.Value
		if !ValidPromotionCandidate(candidate) {
			return fmt.Errorf("promotion candidate %s is incomplete or unsafe", id)
		}
		experiment, found := graph.Experiments[candidate.ExperimentID]
		if !found || experiment.Value.Status != ExperimentCompleted || candidate.OrganizationID != experiment.Value.OrganizationID ||
			candidate.ExperimentVersion != experiment.Version || state.CorrelationID == "" || state.CorrelationID != experiment.CorrelationID ||
			!slices.Equal(candidate.ExperimentResultEventRefs, experiment.Value.ResultEventRefs) {
			return fmt.Errorf("promotion candidate %s lacks its exact completed experiment", id)
		}
		work := graph.Works[experiment.Value.WorkID]
		intent := graph.Intents[work.Value.IntentID]
		if candidate.NominatedBy != intent.Value.SourcePrincipalID {
			return fmt.Errorf("promotion candidate %s does not preserve its commissioning actor", id)
		}
	}
	for id, state := range graph.Knowledge {
		knowledge := state.Value
		if !ValidKnowledgeRecord(knowledge) || knowledge.Version != state.Version {
			return fmt.Errorf("knowledge %s is incomplete or has an invalid version", id)
		}
		switch knowledge.Scope {
		case KnowledgeScopeOrganization:
			if knowledge.ScopeID != knowledge.OrganizationID {
				return fmt.Errorf("knowledge %s crosses its Organization scope", id)
			}
		case KnowledgeScopeAgent:
			agent, found := graph.Agents[knowledge.ScopeID]
			if !found || agent.Value.OrganizationID != knowledge.OrganizationID {
				return fmt.Errorf("knowledge %s references an invalid Agent scope", id)
			}
		case KnowledgeScopeTeam:
			team, found := graph.Teams[knowledge.ScopeID]
			if !found || team.Value.OrganizationID != knowledge.OrganizationID {
				return fmt.Errorf("knowledge %s references an invalid Team scope", id)
			}
		default:
			return fmt.Errorf("knowledge %s has an unsupported scope", id)
		}
	}
	return nil
}

// ValidateTaskAssignment proves that a Task's assignee and pinned execution
// configuration are durable within the Task's organization boundary.
func ValidateTaskAssignment(task Task, organizationID ID, graph DurableGraph) error {
	switch task.AssigneeType {
	case "":
		if task.AssigneeID != "" || task.AgentConfig != nil {
			return fmt.Errorf("task %s has assignment details without an assignee type", task.ID)
		}
	case "AGENT":
		agent, ok := graph.Agents[task.AssigneeID]
		if !ok || agent.Value.OrganizationID != organizationID {
			return fmt.Errorf("task %s references invalid assignee agent %s", task.ID, task.AssigneeID)
		}
		if err := validateDurableTaskAgentConfig(task.ID, task.AgentConfig, organizationID, graph); err != nil {
			return err
		}
	case "TEAM":
		if task.AgentConfig != nil {
			return fmt.Errorf("task %s has Agent configuration for a Team assignment", task.ID)
		}
		team, ok := graph.Teams[task.AssigneeID]
		if !ok || team.Value.OrganizationID != organizationID {
			return fmt.Errorf("task %s references invalid assignee team %s", task.ID, task.AssigneeID)
		}
	default:
		return fmt.Errorf("task %s has unsupported assignee type %s", task.ID, task.AssigneeType)
	}
	return nil
}

func validateDurableTaskAgentConfig(taskID ID, config *AgentConfig, organizationID ID, graph DurableGraph) error {
	if config == nil || config.BlueprintID == "" || config.BlueprintVersion == "" || config.ProfileID == "" || config.ProfileVersion == "" || config.RuntimeAdapter == "" {
		return fmt.Errorf("task %s has incomplete pinned Agent configuration", taskID)
	}
	blueprint, ok := graph.AgentBlueprints[config.BlueprintID]
	if !ok || blueprint.Value.OrganizationID != organizationID || blueprint.Value.Version != config.BlueprintVersion {
		return fmt.Errorf("task %s references invalid pinned blueprint %s", taskID, config.BlueprintID)
	}
	profile, ok := graph.ExecutionProfiles[config.ProfileID]
	if !ok || profile.Value.OrganizationID != organizationID || profile.Value.Version != config.ProfileVersion {
		return fmt.Errorf("task %s references invalid pinned execution profile %s", taskID, config.ProfileID)
	}
	return nil
}

func validateDurableRoster(graph DurableGraph) error {
	for id, state := range graph.AgentBlueprints {
		blueprint := state.Value
		if !ValidAgentBlueprint(blueprint) {
			return fmt.Errorf("agent blueprint %s is incomplete", id)
		}
	}
	for id, state := range graph.ExecutionProfiles {
		profile := state.Value
		if !ValidExecutionProfile(profile) {
			return fmt.Errorf("execution profile %s is incomplete", id)
		}
	}
	for id, state := range graph.Agents {
		agent := state.Value
		if !ValidAgent(agent) {
			return fmt.Errorf("agent %s is incomplete", id)
		}
		blueprint, blueprintFound := graph.AgentBlueprints[agent.BlueprintID]
		profile, profileFound := graph.ExecutionProfiles[agent.ExecutionProfileID]
		if !blueprintFound || !profileFound || !ValidAgentConfigurationBinding(agent, blueprint.Value, profile.Value) {
			return fmt.Errorf("agent %s references invalid blueprint %s", id, agent.BlueprintID)
		}
	}
	return nil
}

func validDurableRosterStatus(status string) bool {
	return status == "ACTIVE" || status == "INACTIVE"
}

type durableOrganizedIdentity struct {
	kind           string
	recordID       ID
	valueID        ID
	organizationID ID
}

func validateDurableIdentity(kind string, recordID, valueID ID) error {
	if recordID == "" || valueID != recordID {
		return fmt.Errorf("%s record %s has mismatched identity %s", kind, recordID, valueID)
	}
	return nil
}

func validateDurableOrganizedIdentity(record durableOrganizedIdentity, organizations map[ID]DurableState[Organization]) error {
	if err := validateDurableIdentity(record.kind, record.recordID, record.valueID); err != nil {
		return err
	}
	if _, ok := organizations[record.organizationID]; !ok {
		return fmt.Errorf("%s %s references missing organization %s", record.kind, record.recordID, record.organizationID)
	}
	return nil
}
