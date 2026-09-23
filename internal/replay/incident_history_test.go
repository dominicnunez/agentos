package replay

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestProjectIncidentRejectsIncompleteGraphAdmission(t *testing.T) {
	for _, name := range []string{"unreviewed human Intent", "missing Task assignee"} {
		t.Run(name, func(t *testing.T) {
			snapshot := admittedEffectFixture(t)
			if _, err := ProjectIncident(snapshot, "conversation"); err != nil {
				t.Fatalf("valid fixture rejected: %v", err)
			}
			index := 0
			if name == "missing Task assignee" {
				index = 2
			}
			event := snapshot.Work.Events[index]
			payload, present, err := events.AdmittedProjection(event)
			if err != nil || !present {
				t.Fatalf("fixture lacks projection: %v", err)
			}
			var value map[string]any
			if err := json.Unmarshal(payload.Projection.Value, &value); err != nil {
				t.Fatal(err)
			}
			if name == "unreviewed human Intent" {
				value["source_channel"] = "HUMAN_DIRECT"
			} else {
				value["assignee_type"], value["assignee_id"] = "AGENT", "absent-agent"
			}
			payload.Projection.Value, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			payload, err = events.SealProjectionEvent(event, payload.Projection, nil)
			if err != nil {
				t.Fatal(err)
			}
			snapshot.Work.Events[index].Payload, err = json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ProjectIncident(snapshot, "conversation"); err == nil {
				t.Fatal("accepted incomplete graph admission")
			}
		})
	}
}

func TestProjectIncidentChecksOtherProjectionHistory(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("retired-earlier-%t", invalid), func(t *testing.T) {
			snapshot := admittedEffectFixture(t)
			for version := 1; version <= 3; version++ {
				label, status, statement := "MISSION_REVISED", "ACTIVE", fmt.Sprintf("direction %d", version)
				if version == 1 {
					label = "MISSION_CREATED"
				}
				if invalid && version == 2 {
					label, status, statement = "MISSION_RETIRED", "RETIRED", "direction 1"
				}
				event := events.Event{EventID: fmt.Sprintf("mission-%d", version), Sequence: int64(6 + version), OrganizationID: "org", CorrelationID: "work", SourceActorID: "runtime", EventType: label, CreatedAt: snapshot.Work.Events[0].CreatedAt, SchemaVersion: events.SchemaVersion}
				value, err := json.Marshal(map[string]any{"id": "mission", "organization_id": "org", "statement": statement, "status": status, "created_at": event.CreatedAt})
				if err != nil {
					t.Fatal(err)
				}
				sealed, err := events.SealProjectionEvent(event, events.ProjectionRecord{ProjectionKind: "mission", RecordID: "mission", Version: version, CorrelationID: "work", Value: value}, nil)
				if err != nil {
					t.Fatal(err)
				}
				event.Payload, err = json.Marshal(sealed)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.Work.Events = append(snapshot.Work.Events, event)
			}
			snapshot.Work.LedgerEvents, snapshot.Work.LedgerSequence = 9, 9
			_, err := ProjectIncident(snapshot, "conversation")
			if invalid && err == nil {
				t.Fatal("accepted Mission revival after an earlier terminal revision")
			}
			if !invalid && err != nil {
				t.Fatalf("valid Mission refinement rejected: %v", err)
			}
		})
	}
}

func TestProjectIncidentKeepsDependenciesPrivate(t *testing.T) {
	snapshot := admittedEffectFixture(t)
	report, err := ProjectIncident(snapshot, "conversation")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Entries) != 5 {
		t.Fatalf("supporting Organization appeared as a displayed action: %+v", report.Entries)
	}
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), `"event_id":"org"`) || strings.Contains(string(body), "ORGANIZATION_CREATED") {
		t.Fatal("private dependency appeared in report JSON")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil || string(encoded) != "{}" {
		t.Fatalf("private snapshot evidence serialized: %s (%v)", encoded, err)
	}
}

func TestProjectIncidentRejectsBrokenDependencies(t *testing.T) {
	for _, name := range []string{"missing", "other tenant", "future", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			snapshot := admittedEffectFixture(t)
			switch name {
			case "missing":
				snapshot.DependencyEvents = nil
			case "other tenant":
				snapshot.DependencyEvents[0].OrganizationID = "other"
			case "future":
				snapshot.DependencyEvents[0].Sequence = snapshot.Work.LedgerSequence + 1
			case "duplicate":
				snapshot.DependencyEvents = append(snapshot.DependencyEvents, snapshot.DependencyEvents[0])
			}
			if _, err := ProjectIncident(snapshot, "conversation"); err == nil {
				t.Fatal("accepted invalid dependency evidence")
			}
		})
	}
}
