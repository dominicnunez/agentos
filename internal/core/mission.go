package core

import (
	"reflect"
	"strings"
)

// ValidMission reports whether a Mission contains the complete durable shape
// accepted by the organizational hierarchy.
func ValidMission(mission Mission) bool {
	return mission.ID != "" && mission.OrganizationID != "" && strings.TrimSpace(mission.Statement) != "" &&
		(mission.Status == MissionActive || mission.Status == MissionRetired)
}

// ValidMissionRevision preserves identity and permits active direction to be
// refined or retired. Retirement is terminal and cannot rewrite direction.
func ValidMissionRevision(previous, next Mission) bool {
	if !ValidMission(previous) || !ValidMission(next) || previous.ID != next.ID || previous.OrganizationID != next.OrganizationID || !previous.CreatedAt.Equal(next.CreatedAt) {
		return false
	}
	if previous.Status == MissionRetired {
		return reflect.DeepEqual(previous, next)
	}
	if next.Status == MissionActive {
		return true
	}
	transition := next.Status == MissionRetired
	next.Status = previous.Status
	return transition && reflect.DeepEqual(previous, next)
}
