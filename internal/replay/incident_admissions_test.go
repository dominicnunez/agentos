package replay

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestProjectIncidentRejectsHiddenWorkEvent(t *testing.T) {
	for _, destination := range []string{"dependency", "related"} {
		t.Run(destination, func(t *testing.T) {
			snapshot := incidentFixture(t)
			if _, err := ProjectIncident(snapshot, "conversation"); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			event := snapshot.Work.Events[len(snapshot.Work.Events)-1]
			snapshot.Work.Events = snapshot.Work.Events[:len(snapshot.Work.Events)-1]
			if destination == "dependency" {
				snapshot.DependencyEvents = append(snapshot.DependencyEvents, event)
			} else {
				snapshot.RelatedEvents = append(snapshot.RelatedEvents, event)
			}
			report, err := ProjectIncident(snapshot, "conversation")
			if err == nil {
				t.Fatal("accepted selected Work event hidden in private evidence")
			}
			if !reflect.DeepEqual(report, Report{}) {
				t.Fatal("incomplete Work timeline returned a partial report")
			}
		})
	}
}

func TestProjectIncidentBoundsAdmissions(t *testing.T) {
	for _, name := range []string{"items", "bytes"} {
		t.Run(name, func(t *testing.T) {
			snapshot := incidentFixture(t)
			if name == "items" {
				snapshot.Admissions = make([]events.IncidentAdmission, events.MaximumIncidentEvidence+1)
			} else {
				snapshot.Admissions[0].ExecutionID = strings.Repeat("x", events.MaximumIncidentEvidenceBytes+1)
			}
			report, err := ProjectIncident(snapshot, "conversation")
			if err == nil || !strings.Contains(err.Error(), "bound") {
				t.Fatalf("expected evidence bound before admission projection, got %v", err)
			}
			if !reflect.DeepEqual(report, Report{}) {
				t.Fatal("oversized evidence returned a partial report")
			}
		})
	}
}

func TestProjectIncidentRequiresEveryAdmission(t *testing.T) {
	for _, name := range []string{"inference", "effect"} {
		t.Run(name, func(t *testing.T) {
			var snapshot events.IncidentSnapshot
			if name == "inference" {
				snapshot = incidentFixture(t)
			} else {
				snapshot = admittedEffectFixture(t)
				snapshot.Admissions = []events.IncidentAdmission{{EventRef: "effect", Kind: "EFFECT_ATTEMPT", TaskID: "task-id"}}
			}
			if _, err := ProjectIncident(snapshot, "conversation"); err != nil {
				t.Fatalf("complete snapshot rejected: %v", err)
			}
			snapshot.Admissions = nil
			if _, err := ProjectIncident(snapshot, "conversation"); err == nil {
				t.Fatal("accepted snapshot missing a durable admission")
			}
		})
	}
}

func TestCollectAdmissionsRequiresExecutionStart(t *testing.T) {
	snapshot := admittedEffectFixture(t)
	start := snapshot.Work.Events[2]
	payload, present, err := events.AdmittedProjection(start)
	if err != nil || !present {
		t.Fatalf("missing fixture projection: %v", err)
	}
	var task map[string]any
	if err := json.Unmarshal(payload.Projection.Value, &task); err != nil {
		t.Fatal(err)
	}
	task["execution_kind"] = "TOOL"
	task["status"] = "RUNNING"
	payload.Projection.Value, _ = json.Marshal(task)
	start.EventType = "EXECUTION_STARTED"
	payload, err = events.SealProjectionEvent(start, payload.Projection, nil)
	if err != nil {
		t.Fatal(err)
	}
	start.Payload, _ = json.Marshal(payload)
	admission := events.IncidentAdmission{EventRef: start.EventID, Kind: "EXECUTION_START", TaskID: "task-id", ExecutionID: "execution-task-id-v1"}
	if err := collectAdmissions(&Containment{}, []events.IncidentAdmission{admission}, []events.Event{start}); err != nil {
		t.Fatalf("complete admission rejected: %v", err)
	}
	if err := collectAdmissions(&Containment{}, nil, []events.Event{start}); err == nil {
		t.Fatal("accepted missing execution-start annotation")
	}
}

func TestProjectIncidentRequiresEarlierAdmission(t *testing.T) {
	snapshot := incidentFixture(t)
	later := snapshot.Work.Events[0]
	later.EventID, later.Sequence = "second-reservation", 5
	snapshot.Work.Events = append(snapshot.Work.Events, later)
	snapshot.Work.LedgerEvents++
	snapshot.Admissions = append(snapshot.Admissions, events.IncidentAdmission{EventRef: later.EventID, Kind: "INFERENCE_RESERVATION", TaskID: later.TaskID, ExecutionID: later.SourceExecutionID})
	if _, err := ProjectIncident(snapshot, "conversation"); err != nil {
		t.Fatalf("complete snapshot rejected: %v", err)
	}
	snapshot.Admissions = snapshot.Admissions[1:]
	if _, err := ProjectIncident(snapshot, "conversation"); err == nil {
		t.Fatal("later annotation concealed an omitted earlier admission")
	}
}

func TestProjectIncidentRejectsAdmissionDuringHold(t *testing.T) {
	snapshot := incidentFixture(t)
	snapshot.Work.Events[0].Sequence = 2
	snapshot.RelatedEvents[0].Sequence = 1
	if _, err := ProjectIncident(snapshot, "conversation"); err == nil {
		t.Fatal("accepted reservation admitted during an active hold")
	}
}
