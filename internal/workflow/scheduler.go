// Package workflow contains deterministic Task-DAG scheduling policy.
package workflow

import (
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

// Scheduler finds pending tasks whose dependencies are verified complete.
// It does not choose a model or mutate task state.
type Scheduler struct{}

func (Scheduler) Ready(tasks map[core.ID]core.Task) ([]core.Task, error) {
	if err := validate(tasks); err != nil {
		return nil, err
	}
	ready := make([]core.Task, 0)
	for _, task := range tasks {
		if task.Status == core.TaskPending && task.Ready(tasks) {
			ready = append(ready, task)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
	return ready, nil
}

// RemediationReady finds pending tasks whose ordinary dependency path is
// blocked. Every other dependency must be complete. ParentID expresses the
// accountability route for a block, while DependsOn remains the authoritative
// execution graph; requiring both edges to match can strand deeper DAGs.
func (Scheduler) RemediationReady(tasks map[core.ID]core.Task) ([]core.Task, error) {
	if err := validate(tasks); err != nil {
		return nil, err
	}
	ready := make([]core.Task, 0)
	for _, task := range tasks {
		if task.Status != core.TaskPending {
			continue
		}
		blockedDependency := false
		eligible := len(task.DependsOn) > 0
		for _, dependencyID := range task.DependsOn {
			dependency := tasks[dependencyID]
			switch dependency.Status {
			case core.TaskCompleted:
				continue
			case core.TaskBlocked:
				blockedDependency = true
			case core.TaskPending, core.TaskRunning, core.TaskFailed:
				eligible = false
			default:
				// Unknown durable states are never eligible for execution.
				eligible = false
			}
		}
		if eligible && blockedDependency {
			ready = append(ready, task)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
	return ready, nil
}

// FailedDependencyBlocked returns non-terminal tasks that can no longer run
// because at least one declared dependency failed. The application records
// the terminal transition; the scheduler remains deterministic and read-only.
func (Scheduler) FailedDependencyBlocked(tasks map[core.ID]core.Task) ([]core.Task, error) {
	if err := validate(tasks); err != nil {
		return nil, err
	}
	blocked := make([]core.Task, 0)
	for _, task := range tasks {
		if task.Status != core.TaskPending && task.Status != core.TaskBlocked {
			continue
		}
		for _, dependencyID := range task.DependsOn {
			if tasks[dependencyID].Status == core.TaskFailed {
				blocked = append(blocked, task)
				break
			}
		}
	}
	sort.Slice(blocked, func(i, j int) bool { return blocked[i].ID < blocked[j].ID })
	return blocked, nil
}

func validate(tasks map[core.ID]core.Task) error {
	return core.ValidateTaskDAG(tasks)
}
