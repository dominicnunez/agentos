package core

import (
	"reflect"
	"strings"
)

// ValidTask reports whether a Task has the complete identity and bounded
// execution contract required for durable lifecycle admission.
func ValidTask(task Task) bool {
	if task.ID == "" || task.WorkID == "" || strings.TrimSpace(task.Description) == "" || strings.TrimSpace(task.TaskContractVersion) == "" {
		return false
	}
	switch task.ExecutionKind {
	case ExecutionDeterministic, ExecutionTool, ExecutionAgent, ExecutionTeam, ExecutionHuman, ExecutionMixed:
	default:
		return false
	}
	switch task.ModelInferencePolicy {
	case InferenceForbidden, InferenceAllowed, InferenceRequired:
	default:
		return false
	}
	switch task.Status {
	case TaskPending, TaskRunning, TaskCompleted, TaskFailed, TaskBlocked:
	default:
		return false
	}
	seenDependencies := make(map[ID]struct{}, len(task.DependsOn))
	for _, dependencyID := range task.DependsOn {
		if dependencyID == "" || dependencyID == task.ID {
			return false
		}
		if _, duplicate := seenDependencies[dependencyID]; duplicate {
			return false
		}
		seenDependencies[dependencyID] = struct{}{}
	}
	if task.ParentID == task.ID {
		return false
	}
	if task.AgentConfig != nil && (task.AgentConfig.BlueprintID == "" || task.AgentConfig.BlueprintVersion == "" || task.AgentConfig.ProfileID == "" || task.AgentConfig.ProfileVersion == "" || task.AgentConfig.RuntimeAdapter == "") {
		return false
	}
	if task.CompletionContract != nil && task.CompletionContract.TaskID != task.ID {
		return false
	}
	return true
}

// ValidTaskRevision preserves the complete planned and assigned Task contract.
// Status is the only field that a lifecycle transition may change.
func ValidTaskRevision(previous, next Task) bool {
	if !ValidTask(previous) || !ValidTask(next) {
		return false
	}
	next.Status = previous.Status
	return reflect.DeepEqual(previous, next)
}
