package events

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestResolveExecutionCoordinationSelectsExactLatestPeerRevisions(t *testing.T) {
	own := coordinationTask("task-own", "work-1", core.TaskPending)
	peerA := coordinationTask("task-a", "work-1", core.TaskPending)
	peerB := coordinationTask("task-b", "work-1", core.TaskBlocked)
	peerARunning := peerA
	peerARunning.Status = core.TaskRunning
	peerACompleted := peerA
	peerACompleted.Status = core.TaskCompleted
	stream := []Event{
		executionCoordinationProjection(t, 1, "TASK_CREATED", 1, own),
		executionCoordinationProjection(t, 2, "TASK_CREATED", 1, peerA),
		executionCoordinationProjection(t, 3, "TASK_BLOCKED", 1, peerB),
		executionCoordinationProjection(t, 4, "EXECUTION_STARTED", 2, peerARunning),
		executionCoordinationProjection(t, 7, "TASK_VERIFIED_COMPLETE", 3, peerACompleted),
	}
	selected, err := ResolveExecutionCoordination("org-1", "run-1", "work-1", own.ID, 6, stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].Task.ID != peerA.ID || selected[0].Version != 2 || selected[0].Task.Status != core.TaskRunning || selected[0].EventRef != "task-event-task-a-4" ||
		selected[1].Task.ID != peerB.ID || selected[1].Version != 1 || selected[1].Task.Status != core.TaskBlocked {
		t.Fatalf("unexpected peer coordination selection: %+v", selected)
	}
}

func TestResolveExecutionCoordinationFailsClosedOnInvalidHistory(t *testing.T) {
	base := coordinationTask("task-peer", "work-1", core.TaskPending)
	running := base
	running.Status = core.TaskRunning
	changed := running
	changed.Description = "substituted scope"
	crossWork := coordinationTask("task-cross-work", "work-2", core.TaskPending)
	for name, stream := range map[string][]Event{
		"missing first revision": {executionCoordinationProjection(t, 2, "EXECUTION_STARTED", 2, running)},
		"changed immutable contract": {
			executionCoordinationProjection(t, 1, "TASK_CREATED", 1, base),
			executionCoordinationProjection(t, 2, "EXECUTION_STARTED", 2, changed),
		},
		"cross Work projection": {executionCoordinationProjection(t, 1, "TASK_CREATED", 1, crossWork)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ResolveExecutionCoordination("org-1", "run-1", "work-1", "task-own", 5, stream); err == nil {
				t.Fatal("invalid coordination history was accepted")
			}
		})
	}
}

func TestResolveExecutionCoordinationRejectsUnboundedPeerSet(t *testing.T) {
	stream := make([]Event, 0, maximumExecutionPeerTasks+1)
	for index := 0; index <= maximumExecutionPeerTasks; index++ {
		task := coordinationTask(core.ID(fmt.Sprintf("task-%02d", index)), "work-1", core.TaskPending)
		stream = append(stream, executionCoordinationProjection(t, int64(index+1), "TASK_CREATED", 1, task))
	}
	if _, err := ResolveExecutionCoordination("org-1", "run-1", "work-1", "task-own", 100, stream); err == nil {
		t.Fatal("unbounded peer coordination set was accepted")
	}
}

func coordinationTask(id, workID core.ID, status core.TaskStatus) core.Task {
	return core.Task{
		ID: id, WorkID: workID, Description: "coordinate " + string(id),
		ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden,
		TaskContractVersion: "1", Status: status,
	}
}

func executionCoordinationProjection(t *testing.T, sequence int64, eventType string, version int, task core.Task) Event {
	t.Helper()
	body, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		EventID: fmt.Sprintf("task-event-%s-%d", task.ID, sequence), Sequence: sequence, OrganizationID: "org-1",
		EventType: eventType, SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "run-1",
		CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion,
	}
	sealed, err := SealProjectionEvent(event, ProjectionRecord{
		ProjectionKind: "task", RecordID: string(task.ID), Version: version, CorrelationID: event.CorrelationID, Value: body,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
