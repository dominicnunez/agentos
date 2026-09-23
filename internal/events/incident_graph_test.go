package events

import (
	"github.com/dominicnunez/agentos/internal/core"
	"testing"
	"time"
)

func TestIncidentTaskGraph(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func([]core.Task)
		invalid bool
	}{
		{name: "forward dependency", change: func(tasks []core.Task) { tasks[0].DependsOn = []core.ID{tasks[1].ID} }},
		{name: "forward parent", change: func(tasks []core.Task) { tasks[0].ParentID = tasks[1].ID }},
		{name: "parent is not execution dependency", change: func(tasks []core.Task) { tasks[0].ParentID = tasks[1].ID; tasks[1].DependsOn = []core.ID{tasks[0].ID} }},
		{name: "missing dependency", invalid: true, change: func(tasks []core.Task) { tasks[0].DependsOn = []core.ID{"absent"} }},
		{name: "dependency cycle", invalid: true, change: func(tasks []core.Task) {
			tasks[0].DependsOn = []core.ID{tasks[1].ID}
			tasks[1].DependsOn = []core.ID{tasks[0].ID}
		}},
		{name: "missing parent", invalid: true, change: func(tasks []core.Task) { tasks[0].ParentID = "absent" }},
		{name: "cross Work dependency", invalid: true, change: func(tasks []core.Task) { tasks[0].DependsOn = []core.ID{tasks[1].ID}; tasks[1].WorkID = "work-2" }},
		{name: "cross Work parent", invalid: true, change: func(tasks []core.Task) { tasks[0].ParentID = tasks[1].ID; tasks[1].WorkID = "work-2" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().UTC()
			intent := core.Intent{ID: "intent-1", OrganizationID: "org-1", OriginalInstruction: "test", NormalizedObjective: "test", CreatedAt: now}
			work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "test", Status: core.WorkActive, CreatedAt: now}
			other := work
			other.ID = "work-2"
			tasks := []core.Task{
				{ID: "task-1", WorkID: work.ID, Description: "first", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending},
				{ID: "task-2", WorkID: work.ID, Description: "second", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending},
			}
			test.change(tasks)
			stream := []Event{
				incidentProjectionEvent(t, 1, "ORGANIZATION_CREATED", "organization", "org-1", 1, "", core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "1", CreatedAt: now}),
				incidentProjectionEvent(t, 2, "INTENT_CREATED", "intent", string(intent.ID), 1, "", intent),
				incidentProjectionEvent(t, 3, "WORK_CREATED", "work", string(work.ID), 1, "", work),
				incidentProjectionEvent(t, 4, "WORK_CREATED", "work", string(other.ID), 1, "", other),
				incidentProjectionEvent(t, 5, "TASK_CREATED", "task", string(tasks[0].ID), 1, string(tasks[0].ID), tasks[0]),
				incidentProjectionEvent(t, 6, "TASK_CREATED", "task", string(tasks[1].ID), 1, string(tasks[1].ID), tasks[1]),
			}
			admitted, err := IncidentTaskAdmissions(stream)
			if test.invalid {
				if err == nil {
					t.Fatal("accepted invalid final Task graph")
				}
			} else if err != nil || admitted["task-1"] != 5 || admitted["task-2"] != 6 {
				t.Fatalf("valid graph admissions = %v, error = %v", admitted, err)
			}
		})
	}
}
