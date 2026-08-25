package app

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/projections"
)

const (
	maximumOrganizationSnapshotRecords = 10_000
	maximumOrganizationSnapshotBytes   = 8 << 20
)

// OrganizationSnapshot is a bounded, read-only projection of one durable
// organization. It deliberately excludes instructions, credentials, tool
// references, model prompts, event payloads, results, and authority records.
type OrganizationSnapshot struct {
	Organization OrganizationSummary `json:"organization"`
	Missions     []MissionSummary    `json:"missions"`
	Goals        []GoalSummary       `json:"goals"`
	Works        []WorkSummary       `json:"works"`
	Tasks        []TaskSummary       `json:"tasks"`
	Teams        []TeamSummary       `json:"teams"`
	Agents       []AgentSummary      `json:"agents"`
}

type OrganizationSummary struct {
	ID            core.ID   `json:"id"`
	Name          string    `json:"name"`
	PolicyVersion string    `json:"policy_version"`
	Version       int       `json:"version"`
	CreatedAt     time.Time `json:"created_at"`
}

type MissionSummary struct {
	ID        core.ID            `json:"id"`
	Statement string             `json:"statement"`
	Status    core.MissionStatus `json:"status"`
	Version   int                `json:"version"`
	CreatedAt time.Time          `json:"created_at"`
}

type GoalSummary struct {
	ID              core.ID         `json:"id"`
	MissionID       core.ID         `json:"mission_id"`
	Objective       string          `json:"objective"`
	Mode            core.GoalMode   `json:"mode"`
	SuccessCriteria []string        `json:"success_criteria"`
	Status          core.GoalStatus `json:"status"`
	Version         int             `json:"version"`
	CreatedAt       time.Time       `json:"created_at"`
}

type WorkSummary struct {
	ID               core.ID               `json:"id"`
	GoalID           core.ID               `json:"goal_id,omitempty"`
	ReplacesWorkID   core.ID               `json:"replaces_work_id,omitempty"`
	Objective        string                `json:"objective"`
	Mode             core.IntentMode       `json:"mode"`
	ExperimentStatus core.ExperimentStatus `json:"experiment_status,omitempty"`
	TrustLabel       string                `json:"trust_label,omitempty"`
	Status           core.WorkStatus       `json:"status"`
	Version          int                   `json:"version"`
	CreatedAt        time.Time             `json:"created_at"`
}

type TaskSummary struct {
	ID                   core.ID                   `json:"id"`
	WorkID               core.ID                   `json:"work_id"`
	ParentID             core.ID                   `json:"parent_id,omitempty"`
	Description          string                    `json:"description"`
	ExecutionKind        core.ExecutionKind        `json:"execution_kind"`
	ModelInferencePolicy core.ModelInferencePolicy `json:"model_inference_policy"`
	DependsOn            []core.ID                 `json:"depends_on"`
	AssigneeType         string                    `json:"assignee_type,omitempty"`
	AssigneeID           core.ID                   `json:"assignee_id,omitempty"`
	Status               core.TaskStatus           `json:"status"`
	Version              int                       `json:"version"`
}

type AgentSummary struct {
	ID                     core.ID `json:"id"`
	Role                   string  `json:"role"`
	Status                 string  `json:"status"`
	BlueprintStatus        string  `json:"blueprint_status"`
	ExecutionProfileStatus string  `json:"execution_profile_status"`
	Available              bool    `json:"available"`
	RuntimeAdapter         string  `json:"runtime_adapter"`
	ModelProvider          string  `json:"model_provider"`
	Model                  string  `json:"model"`
	Version                int     `json:"version"`
}

type TeamSummary struct {
	ID             core.ID   `json:"id"`
	Name           string    `json:"name"`
	Mission        string    `json:"mission,omitempty"`
	MemberAgentIDs []core.ID `json:"member_agent_ids"`
	Status         string    `json:"status"`
	Version        int       `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
}

// OrganizationState returns the current validated projection for exactly one
// tenant. A caller never receives another organization's identity or work.
func (s *Service) OrganizationState(ctx context.Context, organizationID core.ID) (OrganizationSnapshot, bool, error) {
	if s == nil || s.state == nil || organizationID == "" {
		return OrganizationSnapshot{}, false, fmt.Errorf("organization state boundary is required")
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return OrganizationSnapshot{}, false, err
	}
	return organizationSnapshot(snapshot, organizationID)
}

func organizationSnapshot(snapshot projections.Snapshot, organizationID core.ID) (OrganizationSnapshot, bool, error) {
	organization, found := snapshot.Organizations[organizationID]
	if !found {
		return OrganizationSnapshot{}, false, nil
	}
	view := OrganizationSnapshot{
		Organization: OrganizationSummary{
			ID: organization.Value.ID, Name: organization.Value.Name, PolicyVersion: organization.Value.PolicyVersion,
			Version: organization.Version, CreatedAt: organization.Value.CreatedAt,
		},
		Missions: make([]MissionSummary, 0), Goals: make([]GoalSummary, 0), Works: make([]WorkSummary, 0),
		Tasks: make([]TaskSummary, 0), Teams: make([]TeamSummary, 0), Agents: make([]AgentSummary, 0),
	}
	for _, state := range snapshot.Missions {
		if state.Value.OrganizationID == organizationID {
			view.Missions = append(view.Missions, MissionSummary{ID: state.Value.ID, Statement: state.Value.Statement, Status: state.Value.Status, Version: state.Version, CreatedAt: state.Value.CreatedAt})
		}
	}
	for _, state := range snapshot.Goals {
		if state.Value.OrganizationID != organizationID {
			continue
		}
		criteria := make([]string, 0, len(state.Value.SuccessCriteria))
		for _, criterion := range state.Value.SuccessCriteria {
			criteria = append(criteria, criterion.Value)
		}
		view.Goals = append(view.Goals, GoalSummary{
			ID: state.Value.ID, MissionID: state.Value.MissionID, Objective: state.Value.Objective, Mode: state.Value.Mode,
			SuccessCriteria: criteria, Status: state.Value.Status, Version: state.Version, CreatedAt: state.Value.CreatedAt,
		})
	}
	experimentsByWork := make(map[core.ID]core.Experiment)
	for _, state := range snapshot.Experiments {
		if state.Value.OrganizationID != organizationID {
			continue
		}
		if _, duplicate := experimentsByWork[state.Value.WorkID]; duplicate {
			return OrganizationSnapshot{}, false, fmt.Errorf("work %s has multiple durable experiments", state.Value.WorkID)
		}
		experimentsByWork[state.Value.WorkID] = state.Value
	}
	workIDs := make(map[core.ID]struct{})
	for _, state := range snapshot.Works {
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return OrganizationSnapshot{}, false, fmt.Errorf("work %s has no durable intent", state.Value.ID)
		}
		if intent.Value.OrganizationID != organizationID {
			continue
		}
		workIDs[state.Value.ID] = struct{}{}
		work := WorkSummary{
			ID: state.Value.ID, GoalID: state.Value.GoalID, ReplacesWorkID: state.Value.ReplacesWorkID,
			Objective: state.Value.Objective, Mode: core.IntentModeStandard, Status: state.Value.Status, Version: state.Version, CreatedAt: state.Value.CreatedAt,
		}
		if experiment, experimental := experimentsByWork[state.Value.ID]; experimental {
			work.Mode = core.IntentModeExperiment
			work.ExperimentStatus = experiment.Status
			work.TrustLabel = experiment.TrustLabel
		}
		view.Works = append(view.Works, work)
	}
	for _, state := range snapshot.Tasks {
		if _, ok := workIDs[state.Value.WorkID]; !ok {
			continue
		}
		view.Tasks = append(view.Tasks, TaskSummary{
			ID: state.Value.ID, WorkID: state.Value.WorkID, ParentID: state.Value.ParentID, Description: state.Value.Description,
			ExecutionKind: state.Value.ExecutionKind, ModelInferencePolicy: state.Value.ModelInferencePolicy,
			DependsOn: append([]core.ID{}, state.Value.DependsOn...), AssigneeType: state.Value.AssigneeType,
			AssigneeID: state.Value.AssigneeID, Status: state.Value.Status, Version: state.Version,
		})
	}
	for _, state := range snapshot.Teams {
		if state.Value.OrganizationID != organizationID {
			continue
		}
		view.Teams = append(view.Teams, TeamSummary{
			ID: state.Value.ID, Name: state.Value.Name, Mission: state.Value.Mission,
			MemberAgentIDs: append([]core.ID{}, state.Value.MemberAgentIDs...), Status: state.Value.Status,
			Version: state.Version, CreatedAt: state.Value.CreatedAt,
		})
	}
	for _, state := range snapshot.Agents {
		if state.Value.OrganizationID != organizationID {
			continue
		}
		blueprint, blueprintFound := snapshot.AgentBlueprints[state.Value.BlueprintID]
		profile, profileFound := snapshot.ExecutionProfiles[state.Value.ExecutionProfileID]
		if !blueprintFound || !profileFound || !core.ValidAgentConfigurationBinding(state.Value, blueprint.Value, profile.Value) {
			return OrganizationSnapshot{}, false, fmt.Errorf("agent %s has no exact durable configuration", state.Value.ID)
		}
		view.Agents = append(view.Agents, AgentSummary{
			ID: state.Value.ID, Role: blueprint.Value.Role, Status: state.Value.Status,
			BlueprintStatus: blueprint.Value.Status, ExecutionProfileStatus: profile.Value.Status,
			Available:      state.Value.Status == "ACTIVE" && blueprint.Value.Status == "ACTIVE" && profile.Value.Status == "ACTIVE",
			RuntimeAdapter: state.Value.RuntimeAdapter,
			ModelProvider:  profile.Value.ModelProvider, Model: profile.Value.Model, Version: state.Version,
		})
	}
	sort.Slice(view.Missions, func(i, j int) bool { return view.Missions[i].ID < view.Missions[j].ID })
	sort.Slice(view.Goals, func(i, j int) bool { return view.Goals[i].ID < view.Goals[j].ID })
	sort.Slice(view.Works, func(i, j int) bool { return view.Works[i].ID < view.Works[j].ID })
	sort.Slice(view.Tasks, func(i, j int) bool { return view.Tasks[i].ID < view.Tasks[j].ID })
	sort.Slice(view.Teams, func(i, j int) bool { return view.Teams[i].ID < view.Teams[j].ID })
	sort.Slice(view.Agents, func(i, j int) bool { return view.Agents[i].ID < view.Agents[j].ID })
	if err := validateOrganizationSnapshotBounds(view); err != nil {
		return OrganizationSnapshot{}, false, err
	}
	return view, true, nil
}

func validateOrganizationSnapshotBounds(view OrganizationSnapshot) error {
	if len(view.Missions)+len(view.Goals)+len(view.Works)+len(view.Tasks)+len(view.Teams)+len(view.Agents) > maximumOrganizationSnapshotRecords {
		return fmt.Errorf("organization state exceeds the bounded dashboard view")
	}
	return validateOrganizationSnapshotSize(view)
}

func validateOrganizationSnapshotSize(view OrganizationSnapshot) error {
	encoded, err := json.Marshal(view)
	if err != nil {
		return fmt.Errorf("encode bounded organization state: %w", err)
	}
	if len(encoded) > maximumOrganizationSnapshotBytes {
		return fmt.Errorf("organization state exceeds the bounded dashboard response")
	}
	return nil
}
