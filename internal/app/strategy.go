package app

import (
	"context"
	"errors"
	"fmt"
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

	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return OrganizationSnapshot{}, fmt.Errorf("%w: load durable state", ErrStrategyUnavailable)
	}
	if strategyBootstrapMatches(snapshot, input) {
		return s.strategySnapshot(ctx, input.OrganizationID)
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
	if err := s.state.SaveStrategyBootstrap(ctx, organization, mission, goal, input.RequestID, detail); err != nil {
		reloaded, loadErr := s.state.Load(ctx)
		if loadErr != nil {
			return OrganizationSnapshot{}, fmt.Errorf("%w: verify durable admission", ErrStrategyUnavailable)
		}
		if strategyBootstrapMatches(reloaded, input) {
			return s.strategySnapshot(ctx, input.OrganizationID)
		}
		if strategyBootstrapCollides(reloaded, input) {
			return OrganizationSnapshot{}, ErrStrategyConflict
		}
		return OrganizationSnapshot{}, fmt.Errorf("%w: persist durable direction", ErrStrategyUnavailable)
	}
	return s.strategySnapshot(ctx, input.OrganizationID)
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

func strategyBootstrapMatches(snapshot projections.Snapshot, input StrategyBootstrapInput) bool {
	mission, missionFound := snapshot.Missions[input.MissionID]
	goal, goalFound := snapshot.Goals[input.GoalID]
	if !missionFound || !goalFound || mission.CorrelationID != input.RequestID || goal.CorrelationID != input.RequestID ||
		mission.Value.OrganizationID != input.OrganizationID || mission.Value.Statement != input.MissionStatement || mission.Value.Status != core.MissionActive ||
		goal.Value.OrganizationID != input.OrganizationID || goal.Value.MissionID != input.MissionID || goal.Value.Objective != input.GoalObjective ||
		goal.Value.Mode != input.GoalMode || goal.Value.Status != core.GoalActive || len(goal.Value.SuccessCriteria) != len(input.SuccessCriteria) {
		return false
	}
	for index, criterion := range goal.Value.SuccessCriteria {
		if criterion.Value != input.SuccessCriteria[index] || criterion.Origin != "USER_CONFIRMED" || criterion.SourceMessageID != "" {
			return false
		}
	}
	return true
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
