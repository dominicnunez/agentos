package events

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestProjectionHistoryReturnsAdmittedGraph(t *testing.T) {
	stream := historyTestEvents(t, core.WorkActive, "")
	// Input order is not authority; durable sequence determines prior state.
	stream[0], stream[3] = stream[3], stream[0]
	before := append([]Event(nil), stream...)
	graph, err := ValidateProjectionHistory(stream, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := graph.Tasks["task-1"]; got.Version != 1 || got.CorrelationID != "incident" || got.Value.WorkID != "work-1" || got.Value.Status != core.TaskPending {
		t.Fatalf("unexpected admitted Task: %+v", got)
	}
	if len(graph.Organizations) != 1 || len(graph.Intents) != 1 || len(graph.Works) != 1 || len(graph.Tasks) != 1 {
		t.Fatalf("incomplete admitted graph: %+v", graph)
	}
	if !reflect.DeepEqual(stream, before) {
		t.Fatal("history validator mutated the caller's event order")
	}
}

func TestProjectionHistoryRejectsInvalidPredecessors(t *testing.T) {
	cases := []struct {
		name   string
		events func(*testing.T) []Event
		want   string
	}{
		{"missing parent Organization", func(t *testing.T) []Event { return historyTestEvents(t, core.WorkActive, "")[1:] }, "durable parent Organization"},
		{"missing assigned Agent", func(t *testing.T) []Event { return historyTestEvents(t, core.WorkActive, "agent-missing") }, "invalid assignee agent"},
		{"Task after terminal Work", func(t *testing.T) []Event { return historyTestEvents(t, core.WorkFailed, "") }, "exact active Work"},
		{"duplicate sequence", func(t *testing.T) []Event {
			stream := historyTestEvents(t, core.WorkActive, "")
			stream = append(stream, Event{EventID: "ordinary", Sequence: 1, SchemaVersion: SchemaVersion, CreatedAt: time.Now().UTC(), EventType: "NOTE_RECORDED"})
			return stream
		}, "duplicate sequence"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			graph, err := ValidateProjectionHistory(tc.events(t), nil, nil, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v; want %q", err, tc.want)
			}
			if !reflect.DeepEqual(graph, core.DurableGraph{}) {
				t.Fatal("failed history exposed a partially admitted graph")
			}
		})
	}
}

func TestProjectionHistoryChecksAbandonment(t *testing.T) {
	stream := historyTestEvents(t, core.WorkActive, "")
	stream = append(stream, Event{EventID: "abandon", Sequence: 5, OrganizationID: "org-1", CorrelationID: "incident", EventType: "INTAKE_ABANDONED", SourceActorID: "runtime", SchemaVersion: SchemaVersion, CreatedAt: time.Now().UTC(), Payload: []byte(`{}`)})
	if _, err := ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
		t.Fatal("accepted malformed standalone intake abandonment")
	}
}

func historyTestEvents(t *testing.T, status core.WorkStatus, assignee core.ID) []Event {
	t.Helper()
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "test", NormalizedObjective: "test", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if assignee != "" {
		task.AssigneeType, task.AssigneeID = "AGENT", assignee
	}
	stream := []Event{
		incidentProjectionEvent(t, 1, "ORGANIZATION_CREATED", "organization", string(organization.ID), 1, "", organization),
		incidentProjectionEvent(t, 2, "INTENT_CREATED", "intent", string(intent.ID), 1, "", intent),
		incidentProjectionEvent(t, 3, "WORK_CREATED", "work", string(work.ID), 1, "", work),
	}
	if status == core.WorkFailed {
		work.Status = status
		stream = append(stream, incidentProjectionEvent(t, 4, "WORK_FAILED", "work", string(work.ID), 2, "", work))
	}
	return append(stream, incidentProjectionEvent(t, int64(len(stream)+1), "TASK_CREATED", "task", string(task.ID), 1, string(task.ID), task))
}
