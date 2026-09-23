package events

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestIncidentTaskAdmissionsRequiresActiveWork(t *testing.T) {
	now := time.Now().UTC()
	intent := core.Intent{ID: "intent-1", OrganizationID: "org-1", OriginalInstruction: "test", NormalizedObjective: "test", CreatedAt: now}
	active := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	failed := active
	failed.Status = core.WorkFailed
	task := core.Task{ID: "task-1", WorkID: active.ID, Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	stream := []Event{
		incidentProjectionEvent(t, 1, "ORGANIZATION_CREATED", "organization", "org-1", 1, "", core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "1", CreatedAt: now}),
		incidentProjectionEvent(t, 2, "INTENT_CREATED", "intent", string(intent.ID), 1, "", intent),
		incidentProjectionEvent(t, 3, "WORK_CREATED", "work", string(active.ID), 1, "", active),
		incidentProjectionEvent(t, 4, "WORK_FAILED", "work", string(failed.ID), 2, "", failed),
		incidentProjectionEvent(t, 5, "TASK_CREATED", "task", string(task.ID), 1, string(task.ID), task),
	}
	if _, err := IncidentTaskAdmissions(stream); err == nil {
		t.Fatal("accepted Task admission after its Work became terminal")
	}
}

func incidentProjectionEvent(t *testing.T, sequence int64, eventType, kind, recordID string, version int, taskID string, value any) Event {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{EventID: fmt.Sprintf("event-%d", sequence), Sequence: sequence, OrganizationID: "org-1", EventType: eventType, SourceActorID: "runtime", TaskID: taskID, CorrelationID: "incident", CreatedAt: time.Now().UTC(), SchemaVersion: SchemaVersion}
	record := ProjectionRecord{ProjectionKind: kind, RecordID: recordID, Version: version, CorrelationID: event.CorrelationID, Value: encoded}
	payload, err := SealProjectionEvent(event, record, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return event
}
