package replay

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

func incidentFixture(t *testing.T) events.IncidentSnapshot {
	t.Helper()
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	reserve := events.Event{EventID: "reserve", Sequence: 1, OrganizationID: "org", CorrelationID: "work", TaskID: "task-work", SourceActorID: "runtime", SourceExecutionID: "call", EventType: "INFERENCE_RESERVED", CreatedAt: now.Add(time.Hour), SchemaVersion: events.SchemaVersion, Payload: []byte(`{"private":"never disclose"}`)}
	usage := reserve
	usage.EventID, usage.EventType, usage.Sequence = "usage", "INFERENCE_USAGE_RECORDED", 4
	snapshot := events.IncidentSnapshot{Work: events.VerifiedEventSnapshot{OrganizationID: "org", CorrelationID: "work", Algorithm: "SHA-256", LedgerEvents: 4, LedgerSequence: 40, LedgerEventID: "private-global-head", LedgerSHA256: strings.Repeat("a", 64), Events: []events.Event{reserve, usage}}, Admissions: []events.IncidentAdmission{{EventRef: "reserve", Kind: "INFERENCE_RESERVATION", TaskID: "task-work", ExecutionID: "call"}}}
	for i, frozen := range []bool{true, false} {
		id, prior := "hold", ""
		if i == 1 {
			id, prior = "release", "hold"
		}
		body, err := json.Marshal(map[string]any{"organization_id": "org", "frozen": frozen, "reason": "private operator reason", "updated_at": now.Add(time.Duration(i) * time.Second), "control": map[string]any{"actor_id": "owner", "actor_kind": "HUMAN", "prior_event_ref": prior, "prior_version": i}})
		if err != nil {
			t.Fatal(err)
		}
		event := events.Event{EventID: id, Sequence: int64(i + 2), OrganizationID: "org", SourceActorID: "owner", EventType: "FREEZE_SET", CreatedAt: now.Add(time.Duration(i) * time.Second), SchemaVersion: events.SchemaVersion, Payload: body}
		snapshot.RelatedEvents = append(snapshot.RelatedEvents, event)
		snapshot.FreezeRecords = append(snapshot.FreezeRecords, events.AuthorityRecord{Kind: "organization_freeze", RecordID: "org", Version: i + 1, Body: body, AdmissionEventID: id})
	}
	return snapshot
}

func TestProjectIncidentUsesExactHoldOrder(t *testing.T) {
	snapshot := incidentFixture(t)
	report, err := ProjectIncident(snapshot, "public-conversation")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Containment.Holds) != 2 || len(report.Entries) != 4 {
		t.Fatalf("incomplete report: %+v", report)
	}
	hold := report.Containment.Holds[0]
	want := []Admission{{EventRef: "reserve", Kind: "INFERENCE_RESERVATION", TaskID: "task-work", ExecutionID: "call"}}
	if hold.EventRef != "hold" || !hold.Frozen || !reflect.DeepEqual(hold.LastAdmissions, want) || report.Containment.Holds[1].PriorEventRef != "hold" {
		t.Fatalf("hold boundary lost exact admission or predecessor: %+v", report.Containment.Holds)
	}
	if report.Entries[0].EventID != "reserve" || report.Entries[1].EventID != "hold" || report.Entries[3].EventID != "usage" {
		t.Fatal("timeline used wall clock or omitted late accounting")
	}
	repeated, err := ProjectIncident(snapshot, "public-conversation")
	if err != nil || !reflect.DeepEqual(report, repeated) {
		t.Fatal("incident projection was not deterministic")
	}
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"never disclose", "private operator reason", "private-global-head", `"sequence":`, `"ledger_events":`, `"ledger_sha256":`, `"payload":`} {
		if strings.Contains(string(body), private) {
			t.Fatalf("incident exposed %q", private)
		}
	}
}

func TestProjectIncidentRejectsBrokenEvidence(t *testing.T) {
	for name, mutate := range map[string]func(*events.IncidentSnapshot){
		"other tenant":    func(s *events.IncidentSnapshot) { s.RelatedEvents[0].OrganizationID = "other" },
		"unrelated event": func(s *events.IncidentSnapshot) { s.RelatedEvents[0].EventType = "SECRET_OTHER_WORK" },
		"other task effect": func(s *events.IncidentSnapshot) {
			s.RelatedEvents[0].EventType = "EFFECT_OBLIGATION_TRANSITIONED"
			s.RelatedEvents[0].TaskID = "other-task"
		},
		"duplicate":                   func(s *events.IncidentSnapshot) { s.RelatedEvents = append(s.RelatedEvents, s.Work.Events[0]) },
		"orphan freeze":               func(s *events.IncidentSnapshot) { s.FreezeRecords = s.FreezeRecords[1:] },
		"earlier invalid later valid": func(s *events.IncidentSnapshot) { s.FreezeRecords[0].Body = []byte(`{}`) },
		"outside admission":           func(s *events.IncidentSnapshot) { s.Admissions[0].EventRef = "unselected" },
		"wrong admission class":       func(s *events.IncidentSnapshot) { s.Admissions[0].Kind = "EFFECT_ATTEMPT" },
		"duplicate admission":         func(s *events.IncidentSnapshot) { s.Admissions = append(s.Admissions, s.Admissions[0]) },
		"wrong admission task":        func(s *events.IncidentSnapshot) { s.Admissions[0].TaskID = "other-task" },
		"wrong admission execution":   func(s *events.IncidentSnapshot) { s.Admissions[0].ExecutionID = "invented" },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := incidentFixture(t)
			mutate(&snapshot)
			if _, err := ProjectIncident(snapshot, "public-conversation"); err == nil {
				t.Fatal("accepted inconsistent incident evidence")
			}
		})
	}
}

func TestProjectIncidentRejectsTaskMentionAsAdmission(t *testing.T) {
	snapshot := incidentFixture(t)
	snapshot.RelatedEvents = append(snapshot.RelatedEvents, events.Event{
		EventID: "effect", Sequence: 5, OrganizationID: "org", TaskID: "task-work",
		EventType: "EFFECT_OBLIGATION_TRANSITIONED", CreatedAt: snapshot.Work.Events[0].CreatedAt,
		SchemaVersion: events.SchemaVersion,
		Payload:       []byte(`{"effect_obligation_id":"effect-1","task_id":"task-work","status":"ATTEMPTED","attempt_count":1}`),
	})
	snapshot.Work.LedgerEvents++
	if _, err := ProjectIncident(snapshot, "public-conversation"); err == nil {
		t.Fatal("ordinary event TaskID was accepted as a Task admission")
	}
}
