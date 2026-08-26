package replay

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestProjectBuildsDeterministicPayloadFreeTimeline(t *testing.T) {
	now := time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)
	snapshot := events.VerifiedEventSnapshot{
		OrganizationID: "org-1", CorrelationID: "work-1", Algorithm: "SHA-256",
		LedgerEvents: 4, LedgerSequence: 8, LedgerEventID: "ledger-head", LedgerSHA256: strings.Repeat("a", 64),
		Events: []events.Event{
			{EventID: "event-1", Sequence: 2, OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: "task-1", AuthorizationRefs: []string{}, ArtifactRefs: []string{}, CreatedAt: now, SchemaVersion: events.SchemaVersion, Payload: json.RawMessage(`{"secret":"must-not-leak"}`), CorrelationID: "work-1"},
			{EventID: "event-2", Sequence: 4, OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", SourceExecutionID: "execution-2", TaskID: "task-2", AuthorizationRefs: []string{"lease-1"}, ArtifactRefs: []string{}, CreatedAt: now.Add(time.Second), SchemaVersion: events.SchemaVersion, Payload: json.RawMessage(`{"input":"private"}`), CorrelationID: "work-1"},
			{EventID: "event-3", Sequence: 7, OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: "execution-1", TaskID: "task-1", AuthorizationRefs: []string{}, ArtifactRefs: []string{"artifact-1"}, CreatedAt: now.Add(2 * time.Second), SchemaVersion: events.SchemaVersion, Payload: json.RawMessage(`{"result":"private"}`), CorrelationID: "work-1"},
			{EventID: "event-4", Sequence: 8, OrganizationID: "org-1", EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", SourceExecutionID: "execution-1", TaskID: "task-1", AuthorizationRefs: []string{}, ArtifactRefs: []string{"artifact-1"}, CreatedAt: now.Add(3 * time.Second), SchemaVersion: events.SchemaVersion, Payload: json.RawMessage(`{"evidence":"private"}`), CorrelationID: "work-1"},
		},
	}
	sealedSnapshot, err := json.Marshal(snapshot)
	if err != nil || string(sealedSnapshot) != "{}" {
		t.Fatalf("internal replay snapshot is wire-visible: %s err=%v", sealedSnapshot, err)
	}
	first, err := Project(snapshot, "conversation-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := Project(snapshot, "conversation-1")
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("replay is not deterministic: first=%+v second=%+v err=%v", first, second, err)
	}
	if first.EventCount != 4 || len(first.Entries) != 4 || first.Entries[0].Index != 1 || first.Entries[3].Index != 4 || first.Entries[0].PayloadSHA256 == "" || len(first.Links) != 6 {
		t.Fatalf("unexpected replay report: %+v", first)
	}
	expectedLinks := []Link{
		{FromEventID: "event-1", ToEventID: "event-2", Kind: "STREAM_PREDECESSOR"},
		{FromEventID: "event-2", ToEventID: "event-3", Kind: "STREAM_PREDECESSOR"},
		{FromEventID: "event-1", ToEventID: "event-3", Kind: "TASK_PREDECESSOR"},
		{FromEventID: "event-3", ToEventID: "event-4", Kind: "STREAM_PREDECESSOR"},
		{FromEventID: "event-3", ToEventID: "event-4", Kind: "TASK_PREDECESSOR"},
		{FromEventID: "event-3", ToEventID: "event-4", Kind: "EXECUTION_PREDECESSOR"},
	}
	if !reflect.DeepEqual(first.Links, expectedLinks) {
		t.Fatalf("replay predecessor links=%+v want %+v", first.Links, expectedLinks)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"must-not-leak", `"input":"private"`, `"result":"private"`, `"evidence":"private"`} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("replay exposed private payload %q: %s", private, encoded)
		}
	}
}

func TestProjectRejectsUnverifiedOrCrossBoundaryStreams(t *testing.T) {
	now := time.Now().UTC()
	base := events.VerifiedEventSnapshot{
		OrganizationID: "org-1", CorrelationID: "work-1", Algorithm: "SHA-256",
		LedgerEvents: 2, LedgerSequence: 2, LedgerEventID: "event-2", LedgerSHA256: strings.Repeat("b", 64),
		Events: []events.Event{
			{EventID: "event-1", Sequence: 1, OrganizationID: "org-1", EventType: "TASK_CREATED", CreatedAt: now, SchemaVersion: events.SchemaVersion, CorrelationID: "work-1", Payload: json.RawMessage(`{}`)},
			{EventID: "event-2", Sequence: 2, OrganizationID: "org-1", EventType: "TASK_BLOCKED", CreatedAt: now.Add(time.Second), SchemaVersion: events.SchemaVersion, CorrelationID: "work-1", Payload: json.RawMessage(`{}`)},
		},
	}
	tests := map[string]func(*events.VerifiedEventSnapshot){
		"invalid head":       func(snapshot *events.VerifiedEventSnapshot) { snapshot.LedgerSHA256 = "forged" },
		"other organization": func(snapshot *events.VerifiedEventSnapshot) { snapshot.Events[1].OrganizationID = "org-2" },
		"other correlation":  func(snapshot *events.VerifiedEventSnapshot) { snapshot.Events[1].CorrelationID = "work-2" },
		"duplicate event":    func(snapshot *events.VerifiedEventSnapshot) { snapshot.Events[1].EventID = "event-1" },
		"reordered stream":   func(snapshot *events.VerifiedEventSnapshot) { snapshot.Events[1].Sequence = 1 },
		"future schema":      func(snapshot *events.VerifiedEventSnapshot) { snapshot.Events[1].SchemaVersion++ },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			snapshot := base
			snapshot.Events = append([]events.Event(nil), base.Events...)
			mutate(&snapshot)
			if _, err := Project(snapshot, "conversation-1"); err == nil {
				t.Fatal("invalid replay snapshot was accepted")
			}
		})
	}
}

func TestProjectRejectsOversizedPublicMetadata(t *testing.T) {
	now := time.Now().UTC()
	base := events.VerifiedEventSnapshot{
		OrganizationID: "org-1", CorrelationID: "work-1", Algorithm: "SHA-256",
		LedgerEvents: 1, LedgerSequence: 1, LedgerEventID: "event-1", LedgerSHA256: strings.Repeat("c", 64),
		Events: []events.Event{{
			EventID: "event-1", Sequence: 1, OrganizationID: "org-1", EventType: "TASK_CREATED",
			CreatedAt: now, SchemaVersion: events.SchemaVersion, CorrelationID: "work-1", Payload: json.RawMessage(`{}`),
		}},
	}

	tooLong := base
	tooLong.Events = append([]events.Event(nil), base.Events...)
	tooLong.Events[0].ArtifactRefs = []string{strings.Repeat("x", MaximumEnvelopeFieldBytes+1)}
	if _, err := Project(tooLong, "conversation-1"); err == nil || !strings.Contains(err.Error(), "public bounds") {
		t.Fatalf("oversized reference was not rejected: %v", err)
	}

	tooMany := base
	tooMany.Events = append([]events.Event(nil), base.Events...)
	tooMany.Events[0].AuthorizationRefs = make([]string, MaximumReferencesPerEvent+1)
	for index := range tooMany.Events[0].AuthorizationRefs {
		tooMany.Events[0].AuthorizationRefs[index] = "lease"
	}
	if _, err := Project(tooMany, "conversation-1"); err == nil || !strings.Contains(err.Error(), "public bounds") {
		t.Fatalf("oversized reference collection was not rejected: %v", err)
	}

	escaped := base
	escaped.Events = append([]events.Event(nil), base.Events...)
	escaped.Events[0].ArtifactRefs = make([]string, 400)
	for index := range escaped.Events[0].ArtifactRefs {
		escaped.Events[0].ArtifactRefs[index] = strings.Repeat("\\", MaximumEnvelopeFieldBytes)
	}
	if _, err := Project(escaped, "conversation-1"); err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("oversized encoded report was not rejected: %v", err)
	}
}
