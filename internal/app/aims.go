package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	aimsEvidenceSchemaVersion = "agentos.aims.evidence.v1"
	maximumAIMSEvidenceBytes  = 2 << 20
)

// AIMSEvidencePackage is a bounded, tenant-scoped readiness artifact. It is
// not a conformity assessment or certification record and deliberately omits
// raw events, prompts, results, artifacts, credentials, and authority state.
type AIMSEvidencePackage struct {
	SchemaVersion string                 `json:"schema_version"`
	GeneratedAt   time.Time              `json:"generated_at"`
	Claim         AIMSEvidenceClaim      `json:"claim"`
	Organization  AIMSOrganizationRecord `json:"organization"`
	Inventory     AIMSInventory          `json:"inventory"`
	EvidenceIndex []AIMSEvidenceIndex    `json:"evidence_index"`
	OpenGaps      []AIMSOpenGap          `json:"open_gaps"`
}

type AIMSEvidenceClaim struct {
	Status    string `json:"status"`
	Certified bool   `json:"certified"`
	Scope     string `json:"scope"`
}

type AIMSOrganizationRecord struct {
	ID            core.ID `json:"id"`
	Name          string  `json:"name"`
	PolicyVersion string  `json:"policy_version"`
	Version       int     `json:"version"`
}

type AIMSInventory struct {
	AISystems  []AIMSAISystemRecord `json:"ai_systems"`
	Direction  AIMSDirectionSummary `json:"direction"`
	Operations AIMSOperationSummary `json:"operations"`
}

type AIMSAISystemRecord struct {
	AgentID                core.ID `json:"agent_id"`
	Role                   string  `json:"role"`
	LifecycleStatus        string  `json:"lifecycle_status"`
	Available              bool    `json:"available"`
	BlueprintStatus        string  `json:"blueprint_status"`
	ExecutionProfileStatus string  `json:"execution_profile_status"`
	RuntimeAdapter         string  `json:"runtime_adapter"`
	ModelProvider          string  `json:"model_provider"`
	Model                  string  `json:"model"`
	Version                int     `json:"version"`
}

type AIMSDirectionSummary struct {
	Missions      int         `json:"missions"`
	Goals         int         `json:"goals"`
	GoalModes     []AIMSCount `json:"goal_modes"`
	GoalStates    []AIMSCount `json:"goal_states"`
	MissionStates []AIMSCount `json:"mission_states"`
}

type AIMSOperationSummary struct {
	Teams             int         `json:"teams"`
	Agents            int         `json:"agents"`
	Works             int         `json:"works"`
	Tasks             int         `json:"tasks"`
	Experiments       int         `json:"experiments"`
	WorkModes         []AIMSCount `json:"work_modes"`
	WorkStates        []AIMSCount `json:"work_states"`
	TaskKinds         []AIMSCount `json:"task_kinds"`
	TaskStates        []AIMSCount `json:"task_states"`
	InferencePolicies []AIMSCount `json:"inference_policies"`
}

type AIMSCount struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type AIMSEvidenceIndex struct {
	Control         string   `json:"control"`
	State           string   `json:"state"`
	RecordCount     int      `json:"record_count"`
	Projection      string   `json:"projection"`
	SourceContracts []string `json:"source_contracts"`
}

type AIMSOpenGap struct {
	Area   string `json:"area"`
	Reason string `json:"reason"`
}

// AIMSEvidence returns a current public-projection export for one organization.
// The timestamp marks export creation only; it does not become ledger authority.
func (s *Service) AIMSEvidence(ctx context.Context, organizationID core.ID, generatedAt time.Time) (AIMSEvidencePackage, bool, error) {
	view, found, err := s.OrganizationState(ctx, organizationID)
	if err != nil || !found {
		return AIMSEvidencePackage{}, found, err
	}
	export, err := buildAIMSEvidence(view, generatedAt)
	return export, true, err
}

func buildAIMSEvidence(view OrganizationSnapshot, generatedAt time.Time) (AIMSEvidencePackage, error) {
	if generatedAt.IsZero() {
		return AIMSEvidencePackage{}, fmt.Errorf("AIMS evidence generation time is required")
	}
	export := AIMSEvidencePackage{
		SchemaVersion: aimsEvidenceSchemaVersion,
		GeneratedAt:   generatedAt.UTC(),
		Claim: AIMSEvidenceClaim{
			Status: "READINESS_WORK_IN_PROGRESS", Certified: false,
			Scope: "tenant-scoped technical-control inventory and evidence index",
		},
		Organization: AIMSOrganizationRecord{
			ID: view.Organization.ID, Name: view.Organization.Name,
			PolicyVersion: view.Organization.PolicyVersion, Version: view.Organization.Version,
		},
		Inventory: AIMSInventory{
			AISystems:  make([]AIMSAISystemRecord, 0, len(view.Agents)),
			Direction:  AIMSDirectionSummary{Missions: len(view.Missions), Goals: len(view.Goals)},
			Operations: AIMSOperationSummary{Teams: len(view.Teams), Agents: len(view.Agents), Works: len(view.Works), Tasks: len(view.Tasks)},
		},
		OpenGaps: []AIMSOpenGap{
			{Area: "accountability", Reason: "accountable AIMS owners and delegated management roles are not recorded by this export"},
			{Area: "data_and_information", Reason: "data categories, provenance, retention, deletion, and privacy assessments require operator-owned records"},
			{Area: "purpose_users_and_deployment", Reason: "the Agent role is included, but intended purpose, users, deployment context, and operating environment require operator-owned records"},
			{Area: "impact_and_risk", Reason: "impact assessments, risk acceptance, treatment ownership, and residual-risk decisions require explicit operator records"},
			{Area: "management_system", Reason: "policy approval, competence, audit, management review, corrective action, and control applicability remain separate management-system evidence"},
		},
	}
	missionStates := map[string]int{}
	goalModes, goalStates := map[string]int{}, map[string]int{}
	workModes, workStates := map[string]int{}, map[string]int{}
	taskKinds, taskStates, inferencePolicies := map[string]int{}, map[string]int{}, map[string]int{}
	for _, mission := range view.Missions {
		missionStates[string(mission.Status)]++
	}
	for _, goal := range view.Goals {
		goalModes[string(goal.Mode)]++
		goalStates[string(goal.Status)]++
	}
	for _, work := range view.Works {
		workModes[string(work.Mode)]++
		workStates[string(work.Status)]++
		if work.Mode == core.IntentModeExperiment {
			export.Inventory.Operations.Experiments++
		}
	}
	for _, task := range view.Tasks {
		taskKinds[string(task.ExecutionKind)]++
		taskStates[string(task.Status)]++
		inferencePolicies[string(task.ModelInferencePolicy)]++
	}
	for _, agent := range view.Agents {
		export.Inventory.AISystems = append(export.Inventory.AISystems, AIMSAISystemRecord{
			AgentID: agent.ID, Role: agent.Role, LifecycleStatus: agent.Status,
			Available: agent.Available, BlueprintStatus: agent.BlueprintStatus,
			ExecutionProfileStatus: agent.ExecutionProfileStatus, RuntimeAdapter: agent.RuntimeAdapter,
			ModelProvider: agent.ModelProvider, Model: agent.Model, Version: agent.Version,
		})
	}
	export.Inventory.Direction.MissionStates = sortedAIMSCounts(missionStates)
	export.Inventory.Direction.GoalModes = sortedAIMSCounts(goalModes)
	export.Inventory.Direction.GoalStates = sortedAIMSCounts(goalStates)
	export.Inventory.Operations.WorkModes = sortedAIMSCounts(workModes)
	export.Inventory.Operations.WorkStates = sortedAIMSCounts(workStates)
	export.Inventory.Operations.TaskKinds = sortedAIMSCounts(taskKinds)
	export.Inventory.Operations.TaskStates = sortedAIMSCounts(taskStates)
	export.Inventory.Operations.InferencePolicies = sortedAIMSCounts(inferencePolicies)
	export.EvidenceIndex = []AIMSEvidenceIndex{
		aimsEvidenceIndex("organization_identity_and_policy", 1, "organization", events.ProjectionLifecycleEventTypes("organization")...),
		aimsEvidenceIndex("durable_organizational_direction", len(view.Missions)+len(view.Goals), "organization.missions+goals", projectionLifecycleEventTypes("mission", "goal")...),
		aimsEvidenceIndex("reviewed_work_lifecycle", len(view.Works)+len(view.Tasks), "organization.works+tasks", projectionLifecycleEventTypes("intent", "work", "task")...),
		aimsEvidenceIndex("task_lifecycle_projection", len(view.Tasks), "organization.tasks", events.ProjectionLifecycleEventTypes("task")...),
		aimsEvidenceIndex("reviewed_ai_system_configuration", len(view.Agents), "organization.agents", projectionLifecycleEventTypes("agent_blueprint", "execution_profile", "agent")...),
		aimsEvidenceIndex("organizational_team_roster", len(view.Teams), "organization.teams", events.ProjectionLifecycleEventTypes("team")...),
		aimsEvidenceIndex("governed_experimentation", export.Inventory.Operations.Experiments, "organization.works[mode=EXPERIMENT]", projectionLifecycleEventTypes("work", "lab_experiment")...),
	}
	encoded, err := json.Marshal(export)
	if err != nil {
		return AIMSEvidencePackage{}, fmt.Errorf("encode AIMS evidence package: %w", err)
	}
	if len(encoded)+1 > maximumAIMSEvidenceBytes {
		return AIMSEvidencePackage{}, fmt.Errorf("AIMS evidence package exceeds its byte limit")
	}
	return export, nil
}

func projectionLifecycleEventTypes(kinds ...string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0)
	for _, kind := range kinds {
		for _, eventType := range events.ProjectionLifecycleEventTypes(kind) {
			if _, duplicate := seen[eventType]; duplicate {
				continue
			}
			seen[eventType] = struct{}{}
			result = append(result, eventType)
		}
	}
	return result
}

func sortedAIMSCounts(values map[string]int) []AIMSCount {
	keys := make([]string, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Strings(keys)
	result := make([]AIMSCount, 0, len(keys))
	for _, value := range keys {
		result = append(result, AIMSCount{Value: value, Count: values[value]})
	}
	return result
}

func aimsEvidenceIndex(control string, recordCount int, projection string, contracts ...string) AIMSEvidenceIndex {
	state := "NO_CURRENT_RECORDS"
	if recordCount > 0 {
		state = "PROJECTION_AVAILABLE"
	}
	return AIMSEvidenceIndex{Control: control, State: state, RecordCount: recordCount, Projection: projection, SourceContracts: contracts}
}
