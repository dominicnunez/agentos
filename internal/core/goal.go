package core

import (
	"reflect"
	"strings"
)

// ValidGoal reports whether a Goal contains the complete runtime-owned shape
// required for durable admission. Achievement is deliberately not a Goal
// status; it is established by a separate evidence evaluation.
func ValidGoal(goal Goal) bool {
	validMode := goal.Mode == GoalTarget || goal.Mode == GoalContinuous
	validStatus := goal.Status == GoalActive || goal.Status == GoalPaused || goal.Status == GoalRetired
	if goal.ID == "" || goal.OrganizationID == "" || goal.MissionID == "" || strings.TrimSpace(goal.Objective) == "" || len(goal.SuccessCriteria) == 0 || len(goal.SuccessCriteria) > 256 || !validMode || !validStatus {
		return false
	}
	for _, criterion := range goal.SuccessCriteria {
		if strings.TrimSpace(criterion.Value) == "" || strings.TrimSpace(criterion.Origin) == "" {
			return false
		}
	}
	return true
}

// ValidGoalRevision enforces immutable identity and one-way lifecycle
// transitions. Direction may be refined while status is stable; a lifecycle
// transition cannot simultaneously rewrite that direction.
func ValidGoalRevision(previous, next Goal) bool {
	if !ValidGoal(previous) || !ValidGoal(next) || previous.ID != next.ID || previous.OrganizationID != next.OrganizationID || previous.MissionID != next.MissionID || !previous.CreatedAt.Equal(next.CreatedAt) {
		return false
	}
	if previous.Status == GoalRetired {
		return reflect.DeepEqual(previous, next)
	}
	if previous.Status == next.Status {
		return true
	}
	transition := previous.Status == GoalActive && (next.Status == GoalPaused || next.Status == GoalRetired) ||
		previous.Status == GoalPaused && (next.Status == GoalActive || next.Status == GoalRetired)
	next.Status = previous.Status
	return transition && reflect.DeepEqual(previous, next)
}
