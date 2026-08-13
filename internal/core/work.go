package core

import (
	"reflect"
	"strings"
)

// ValidWork reports whether a bounded Work projection has the complete shape
// required for durable admission.
func ValidWork(work Work) bool {
	validStatus := work.Status == WorkActive || work.Status == WorkCompleted || work.Status == WorkFailed
	return work.ID != "" && work.IntentID != "" && strings.TrimSpace(work.Objective) != "" && validStatus
}

// ValidWorkRevision preserves the accepted Intent, optional Goal binding, and
// objective while allowing only one-way terminal lifecycle transitions.
func ValidWorkRevision(previous, next Work) bool {
	if !ValidWork(previous) || !ValidWork(next) {
		return false
	}
	transition := previous.Status == next.Status || previous.Status == WorkActive && (next.Status == WorkCompleted || next.Status == WorkFailed)
	next.Status = previous.Status
	return transition && reflect.DeepEqual(previous, next)
}
