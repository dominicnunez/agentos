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

func TestRemediationReadyReturnsParentOfBlockedChild(t *testing.T) {
	tasks := map[core.ID]core.Task{
		"done":   {ID: "done", Status: core.TaskCompleted},
		"child":  {ID: "child", ParentID: "parent", Status: core.TaskBlocked},
		"parent": {ID: "parent", Status: core.TaskPending, DependsOn: []core.ID{"done", "child"}},
	}
	ready, err := (Scheduler{}).RemediationReady(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].ID != "parent" {
		t.Fatalf("remediation ready=%v", ready)
	}
}

func TestRemediationReadyFollowsDependencyGraphBeyondParentEdge(t *testing.T) {
	tasks := map[core.ID]core.Task{
		"blocked": {ID: "blocked", ParentID: "root", Status: core.TaskBlocked},
		"middle":  {ID: "middle", ParentID: "root", Status: core.TaskPending, DependsOn: []core.ID{"blocked"}},
		"root":    {ID: "root", Status: core.TaskPending, DependsOn: []core.ID{"middle"}},
	}
	ready, err := (Scheduler{}).RemediationReady(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 1 || ready[0].ID != "middle" {
		t.Fatalf("deep remediation ready=%v", ready)
	}
}

func TestRemediationReadyRejectsUnrelatedOrUnfinishedDependencies(t *testing.T) {
	tasks := map[core.ID]core.Task{
		"blocked": {ID: "blocked", Status: core.TaskBlocked},
		"pending": {ID: "pending", Status: core.TaskPending},
		"parent":  {ID: "parent", Status: core.TaskPending, DependsOn: []core.ID{"blocked", "pending"}},
	}
	ready, err := (Scheduler{}).RemediationReady(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(ready) != 0 {
		t.Fatalf("unexpected remediation ready=%v", ready)
	}
}

func TestFailedDependencyBlockedReturnsPendingAndRemediationTasks(t *testing.T) {
	tasks := map[core.ID]core.Task{
		"failed":  {ID: "failed", Status: core.TaskFailed},
		"pending": {ID: "pending", Status: core.TaskPending, DependsOn: []core.ID{"failed"}},
		"blocked": {ID: "blocked", Status: core.TaskBlocked, DependsOn: []core.ID{"failed"}},
		"done":    {ID: "done", Status: core.TaskCompleted, DependsOn: []core.ID{"failed"}},
	}
	blocked, err := (Scheduler{}).FailedDependencyBlocked(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked) != 2 || blocked[0].ID != "blocked" || blocked[1].ID != "pending" {
		t.Fatalf("failed-dependency tasks=%v", blocked)
	}
}
