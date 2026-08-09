package workflow

import (
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestReadyReturnsOnlySatisfiedPendingTasks(t *testing.T) {
	tasks := map[core.ID]core.Task{
		"a": {ID: "a", Status: core.TaskCompleted},
		"b": {ID: "b", Status: core.TaskPending, DependsOn: []core.ID{"a"}},
		"c": {ID: "c", Status: core.TaskPending, DependsOn: []core.ID{"b"}},
	}
	ready, err := (Scheduler{}).Ready(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].ID != "b" {
		t.Fatalf("ready=%v", ready)
	}
}

func TestReadyRejectsCycle(t *testing.T) {
	tasks := map[core.ID]core.Task{
		"a": {ID: "a", Status: core.TaskPending, DependsOn: []core.ID{"b"}},
		"b": {ID: "b", Status: core.TaskPending, DependsOn: []core.ID{"a"}},
	}
	if _, err := (Scheduler{}).Ready(tasks); err == nil {
		t.Fatal("dependency cycle accepted")
	}
}
