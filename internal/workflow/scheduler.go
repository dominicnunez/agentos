// Package workflow contains deterministic Task-DAG scheduling policy.
package workflow

import (
	"fmt"
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

// RemediationReady finds pending parent tasks whose ordinary dependency path
// is blocked by one or more direct children. Every other dependency must be
// complete. The runtime uses this separate path to let the parent observe the
// blocked-work contract without pretending the child completed.
func (Scheduler) RemediationReady(tasks map[core.ID]core.Task) ([]core.Task, error) {
	if err := validate(tasks); err != nil {
		return nil, err
	}
	ready := make([]core.Task, 0)
	for _, task := range tasks {
		if task.Status != core.TaskPending {
			continue
		}
		blockedChild := false
		eligible := len(task.DependsOn) > 0
		for _, dependencyID := range task.DependsOn {
			dependency := tasks[dependencyID]
			switch {
			case dependency.Status == core.TaskCompleted:
				continue
			case dependency.Status == core.TaskBlocked && dependency.ParentID == task.ID:
				blockedChild = true
			default:
				eligible = false
			}
		}
		if eligible && blockedChild {
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
	for id, task := range tasks {
		for _, dependency := range task.DependsOn {
			if _, ok := tasks[dependency]; !ok {
				return fmt.Errorf("task %s has missing dependency %s", id, dependency)
			}
		}
	}
	visiting := make(map[core.ID]bool)
	visited := make(map[core.ID]bool)
	var visit func(core.ID) error
	visit = func(id core.ID) error {
		if visiting[id] {
			return fmt.Errorf("task dependency cycle at %s", id)
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		for _, dependency := range tasks[id].DependsOn {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	for id := range tasks {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}
