package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/projections"
)

const (
	maximumStrategyTextBytes      = 16 << 10
	maximumStrategyCriteria       = 32
	maximumStrategyCriterionBytes = 4 << 10
	maximumStrategyCriteriaBytes  = 64 << 10
)

var (
	ErrStrategyInvalid     = errors.New("invalid strategy bootstrap")
	ErrStrategyConflict    = errors.New("strategy bootstrap conflicts with durable state")
	ErrStrategyUnavailable = errors.New("strategy bootstrap unavailable")
)

// StrategyBootstrapInput is an authenticated local-user request to establish
// durable organizational direction. It carries no effect, approval, policy,
// model, or capability authority.
type StrategyBootstrapInput struct {
	OrganizationID   core.ID
	RequestID        string
	RequestedByID    core.ID
	RequestedByKind  core.PrincipalKind
	SourceChannel    string
	MissionID        core.ID
	MissionStatement string
	GoalID           core.ID
	GoalObjective    string
	GoalMode         core.GoalMode
	SuccessCriteria  []string
}

// BootstrapStrategy atomically creates a Mission and measurable Goal, and
// creates the Organization projection when this is the installation's first
// durable organizational action. Exact retries are idempotent.
func (s *Service) BootstrapStrategy(ctx context.Context, input StrategyBootstrapInput) (OrganizationSnapshot, error) {
	input.MissionStatement = strings.TrimSpace(input.MissionStatement)
	input.GoalObjective = strings.TrimSpace(input.GoalObjective)
	criteria := make([]string, 0, len(input.SuccessCriteria))
	for _, criterion := range input.SuccessCriteria {
		criteria = append(criteria, strings.TrimSpace(criterion))
	}
	input.SuccessCriteria = criteria
	if err := validateStrategyBootstrap(input); err != nil {
		return OrganizationSnapshot{}, err
	}
	if err := s.acquire(ctx); err != nil {
		return OrganizationSnapshot{}, fmt.Errorf("%w: acquire runtime", ErrStrategyUnavailable)
	}
	defer s.release()

	matched, found, err := s.strategyBootstrapAdmission(ctx, input)
	if err != nil {
		return OrganizationSnapshot{}, fmt.Errorf("%w: resolve durable admission", ErrStrategyUnavailable)
	}
	if matched {
		return s.strategySnapshot(ctx, input.OrganizationID)
	}
	if found {
		return OrganizationSnapshot{}, ErrStrategyConflict
	}

	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return OrganizationSnapshot{}, fmt.Errorf("%w: load durable state", ErrStrategyUnavailable)
	}
	if strategyBootstrapCollides(snapshot, input) {
		return OrganizationSnapshot{}, ErrStrategyConflict
	}

	now := time.Now().UTC()
	mission := core.Mission{
		ID: input.MissionID, OrganizationID: input.OrganizationID, Statement: input.MissionStatement,
		Status: core.MissionActive, CreatedAt: now,
	}
	goalCriteria := make([]core.IntentValue, 0, len(input.SuccessCriteria))
	for _, criterion := range input.SuccessCriteria {
		goalCriteria = append(goalCriteria, core.IntentValue{Value: criterion, Origin: "USER_CONFIRMED"})
	}
	goal := core.Goal{
		ID: input.GoalID, OrganizationID: input.OrganizationID, MissionID: input.MissionID,
		Objective: input.GoalObjective, Mode: input.GoalMode, SuccessCriteria: goalCriteria,
		Status: core.GoalActive, CreatedAt: now,
	}
	var organization *core.Organization
	if _, exists := snapshot.Organizations[input.OrganizationID]; !exists {
		organization = &core.Organization{
			ID: input.OrganizationID, Name: string(input.OrganizationID), PolicyVersion: "v1", CreatedAt: now,
		}
	}
	detail := events.StrategyBootstrapDetail{
		RequestID: input.RequestID, RequestedByID: input.RequestedByID,
		RequestedByKind: input.RequestedByKind, SourceChannel: input.SourceChannel,
	}
	proposedView, err := preflightStrategySnapshot(snapshot, organization, mission, goal)
	if err != nil {
		return OrganizationSnapshot{}, fmt.Errorf("%w: preflight bounded organization view", ErrStrategyUnavailable)
	}
	if err := s.state.SaveStrategyBootstrap(ctx, organization, mission, goal, input.RequestID, detail); err != nil {
		matched, found, resolveErr := s.strategyBootstrapAdmission(ctx, input)
		if resolveErr != nil {
			return OrganizationSnapshot{}, fmt.Errorf("%w: verify durable admission", ErrStrategyUnavailable)
		}
		if matched {
			return proposedView, nil
		}
		if found {
			return OrganizationSnapshot{}, ErrStrategyConflict
		}
		return OrganizationSnapshot{}, fmt.Errorf("%w: persist durable direction", ErrStrategyUnavailable)
	}
	return proposedView, nil
}

func (s *Service) strategySnapshot(ctx context.Context, organizationID core.ID) (OrganizationSnapshot, error) {
	view, found, err := s.OrganizationState(ctx, organizationID)
	if err != nil || !found {
		return OrganizationSnapshot{}, fmt.Errorf("%w: project durable direction", ErrStrategyUnavailable)
	}
	return view, nil
}

func validateStrategyBootstrap(input StrategyBootstrapInput) error {
	if input.OrganizationID == "" || input.RequestedByKind != core.PrincipalHuman || input.SourceChannel != "HUMAN_DIRECT" || !validStrategyIdentifier(string(input.RequestedByID)) ||
		!validStrategyIdentifier(input.RequestID) || !strings.HasPrefix(string(input.MissionID), "mission-") || !validStrategyIdentifier(string(input.MissionID)) ||
		!strings.HasPrefix(string(input.GoalID), "goal-") || !validStrategyIdentifier(string(input.GoalID)) || input.MissionID == input.GoalID ||
		!validBoundedStrategyText(input.MissionStatement, maximumStrategyTextBytes) || !validBoundedStrategyText(input.GoalObjective, maximumStrategyTextBytes) ||
		(input.GoalMode != core.GoalTarget && input.GoalMode != core.GoalContinuous) || len(input.SuccessCriteria) == 0 || len(input.SuccessCriteria) > maximumStrategyCriteria {
		return ErrStrategyInvalid
	}
	total := 0
	uniqueCriteria := make(map[string]struct{}, len(input.SuccessCriteria))
	for _, criterion := range input.SuccessCriteria {
		if !validBoundedStrategyText(criterion, maximumStrategyCriterionBytes) {
			return ErrStrategyInvalid
		}
		if _, duplicate := uniqueCriteria[criterion]; duplicate {
			return ErrStrategyInvalid
		}
		uniqueCriteria[criterion] = struct{}{}
		total += len(criterion)
		if total > maximumStrategyCriteriaBytes {
			return ErrStrategyInvalid
		}
	}
	return nil
}

func validStrategyIdentifier(value string) bool {
	return core.ValidGoalReferenceID(value)
}

func validBoundedStrategyText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func (s *Service) strategyBootstrapAdmission(ctx context.Context, input StrategyBootstrapInput) (bool, bool, error) {
	stream, err := s.gateway.Events(ctx, input.RequestID)
	if err != nil {
		return false, false, err
	}
	if len(stream) == 0 {
		return false, false, nil
	}
	missionSeen := false
	goalSeen := false
	organizationSeen := false
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return false, true, err
		}
		if !present {
			continue
		}
		switch event.EventType {
		case "ORGANIZATION_CREATED":
			if organizationSeen || !strategyCreationEnvelopeMatches(event, input) || !strategyCreationDetailMatches(payload.Detail, input) ||
				payload.Projection.ProjectionKind != projections.KindOrganization || payload.Projection.RecordID != string(input.OrganizationID) || payload.Projection.Version != 1 {
				return false, true, nil
			}
			var organization core.Organization
			if json.Unmarshal(payload.Projection.Value, &organization) != nil || organization.ID != input.OrganizationID || organization.Name != string(input.OrganizationID) || organization.PolicyVersion != "v1" {
				return false, true, nil
			}
			organizationSeen = true
		case "MISSION_CREATED":
			if missionSeen || !strategyCreationEnvelopeMatches(event, input) || !strategyCreationDetailMatches(payload.Detail, input) ||
				payload.Projection.ProjectionKind != projections.KindMission || payload.Projection.RecordID != string(input.MissionID) || payload.Projection.Version != 1 {
				return false, true, nil
			}
			var mission core.Mission
			if json.Unmarshal(payload.Projection.Value, &mission) != nil || mission.ID != input.MissionID || mission.OrganizationID != input.OrganizationID || mission.Statement != input.MissionStatement || mission.Status != core.MissionActive {
				return false, true, nil
			}
			missionSeen = true
		case "GOAL_CREATED":
			if goalSeen || !strategyCreationEnvelopeMatches(event, input) || !strategyCreationDetailMatches(payload.Detail, input) ||
				payload.Projection.ProjectionKind != projections.KindGoal || payload.Projection.RecordID != string(input.GoalID) || payload.Projection.Version != 1 {
				return false, true, nil
			}
			var goal core.Goal
			if json.Unmarshal(payload.Projection.Value, &goal) != nil || !strategyCreationGoalMatches(goal, input) {
				return false, true, nil
			}
			goalSeen = true
		}
	}
	return missionSeen && goalSeen, true, nil
}

func strategyCreationEnvelopeMatches(event events.Event, input StrategyBootstrapInput) bool {
	return event.OrganizationID == string(input.OrganizationID) && event.CorrelationID == input.RequestID && event.SourceActorID == "runtime" &&
		event.SourceExecutionID == "" && event.RecipientScope == "" && event.RecipientID == "" && event.TaskID == "" &&
		len(event.AuthorizationRefs) == 0 && len(event.ArtifactRefs) == 0
}

func strategyCreationDetailMatches(encoded json.RawMessage, input StrategyBootstrapInput) bool {
	var detail events.StrategyBootstrapDetail
	if json.Unmarshal(encoded, &detail) != nil {
		return false
	}
	canonical, err := json.Marshal(detail)
	return err == nil && bytes.Equal(encoded, canonical) && detail.Valid() && detail.RequestID == input.RequestID &&
		detail.RequestedByID == input.RequestedByID && detail.RequestedByKind == input.RequestedByKind && detail.SourceChannel == input.SourceChannel
}

func strategyCreationGoalMatches(goal core.Goal, input StrategyBootstrapInput) bool {
	if goal.ID != input.GoalID || goal.OrganizationID != input.OrganizationID || goal.MissionID != input.MissionID || goal.Objective != input.GoalObjective ||
		goal.Mode != input.GoalMode || goal.Status != core.GoalActive || len(goal.SuccessCriteria) != len(input.SuccessCriteria) {
		return false
	}
	for index, criterion := range goal.SuccessCriteria {
		if criterion.Value != input.SuccessCriteria[index] || criterion.Origin != "USER_CONFIRMED" || criterion.SourceMessageID != "" {
			return false
		}
	}
	return true
}

func preflightStrategySnapshot(snapshot projections.Snapshot, organization *core.Organization, mission core.Mission, goal core.Goal) (OrganizationSnapshot, error) {
	view, found, err := organizationSnapshot(snapshot, mission.OrganizationID)
	if err != nil {
		return OrganizationSnapshot{}, err
	}
	if !found {
		if organization == nil {
			return OrganizationSnapshot{}, fmt.Errorf("strategy organization is unavailable")
		}
		view = OrganizationSnapshot{
			Organization: OrganizationSummary{ID: organization.ID, Name: organization.Name, PolicyVersion: organization.PolicyVersion, Version: 1, CreatedAt: organization.CreatedAt},
			Missions:     make([]MissionSummary, 0, 1), Goals: make([]GoalSummary, 0, 1), Works: make([]WorkSummary, 0),
			Tasks: make([]TaskSummary, 0), Teams: make([]TeamSummary, 0), Agents: make([]AgentSummary, 0),
		}
	}
	criteria := make([]string, 0, len(goal.SuccessCriteria))
	for _, criterion := range goal.SuccessCriteria {
		criteria = append(criteria, criterion.Value)
	}
	view.Missions = append(view.Missions, MissionSummary{ID: mission.ID, Statement: mission.Statement, Status: mission.Status, Version: 1, CreatedAt: mission.CreatedAt})
	view.Goals = append(view.Goals, GoalSummary{
		ID: goal.ID, MissionID: goal.MissionID, Objective: goal.Objective, Mode: goal.Mode,
		SuccessCriteria: criteria, Status: goal.Status, Version: 1, CreatedAt: goal.CreatedAt,
	})
	sort.Slice(view.Missions, func(i, j int) bool { return view.Missions[i].ID < view.Missions[j].ID })
	sort.Slice(view.Goals, func(i, j int) bool { return view.Goals[i].ID < view.Goals[j].ID })
	if err := validateOrganizationSnapshotBounds(view); err != nil {
		return OrganizationSnapshot{}, err
	}
	return view, nil
}

func strategyBootstrapCollides(snapshot projections.Snapshot, input StrategyBootstrapInput) bool {
	if _, exists := snapshot.Missions[input.MissionID]; exists {
		return true
	}
	if _, exists := snapshot.Goals[input.GoalID]; exists {
		return true
	}
	for _, state := range snapshot.Missions {
		if state.CorrelationID == input.RequestID {
			return true
		}
	}
	for _, state := range snapshot.Goals {
		if state.CorrelationID == input.RequestID {
			return true
		}
	}
	return false
}
