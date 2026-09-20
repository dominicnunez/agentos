package replay

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func admittedEffectFixture(t *testing.T) events.IncidentSnapshot {
	t.Helper()
	now := time.Now().UTC()
	seal := func(kind, id, label, taskID string, sequence int64, value any) events.Event {
		event := events.Event{EventID: id, Sequence: sequence, OrganizationID: "org", CorrelationID: "work", SourceActorID: "runtime", TaskID: taskID, EventType: label, CreatedAt: now, SchemaVersion: events.SchemaVersion}
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := events.SealProjectionEvent(event, events.ProjectionRecord{ProjectionKind: kind, RecordID: id, Version: 1, CorrelationID: "work", Value: body}, nil)
		if err != nil {
			t.Fatal(err)
		}
		event.Payload, err = json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	work := seal("work", "work-id", "WORK_CREATED", "", 1, core.Work{ID: "work-id", IntentID: "intent", Objective: "objective", Status: core.WorkActive, CreatedAt: now})
	task := seal("task", "task-id", "TASK_CREATED", "task-id", 2, core.Task{ID: "task-id", WorkID: "work-id", Description: "task", TaskContractVersion: "1", ExecutionKind: core.ExecutionHuman, ModelInferencePolicy: core.InferenceForbidden, Status: core.TaskPending})
	value := core.EffectObligation{ID: "effect-id", OrganizationID: "org", TaskID: "task-id", ActorID: "owner", Action: "send", Resource: "destination", Scope: "org", IdempotencyKey: "key", EffectFingerprint: "legacy", AuthorizationRefs: []string{"lease"}, Status: core.EffectAttempted, AttemptCount: 1}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	effect := events.Event{EventID: "effect", Sequence: 3, OrganizationID: "org", TaskID: "task-id", EventType: "EFFECT_OBLIGATION_TRANSITIONED", AuthorizationRefs: value.AuthorizationRefs, Payload: body, CreatedAt: now, SchemaVersion: events.SchemaVersion}
	return events.IncidentSnapshot{Work: events.VerifiedEventSnapshot{OrganizationID: "org", CorrelationID: "work", Algorithm: "SHA-256", LedgerEvents: 3, LedgerSequence: 3, LedgerEventID: "effect", LedgerSHA256: strings.Repeat("a", 64), Events: []events.Event{work, task}}, RelatedEvents: []events.Event{effect}}
}

func TestProjectIncidentValidatesEffectAdmission(t *testing.T) {
	for _, variant := range []string{"valid", "invalid-state", "effect-in-work-without-task", "pending-labelled-attempt"} {
		t.Run(variant, func(t *testing.T) {
			snapshot := admittedEffectFixture(t)
			switch variant {
			case "pending-labelled-attempt":
				var value core.EffectObligation
				if err := json.Unmarshal(snapshot.RelatedEvents[0].Payload, &value); err != nil {
					t.Fatal(err)
				}
				value.Status, value.AttemptCount = core.EffectPending, 0
				snapshot.RelatedEvents[0].Payload, _ = json.Marshal(value)
				snapshot.Admissions = []events.IncidentAdmission{{EventRef: "effect", Kind: "EFFECT_ATTEMPT", TaskID: "task-id"}}
			case "invalid-state":
				var value core.EffectObligation
				if err := json.Unmarshal(snapshot.RelatedEvents[0].Payload, &value); err != nil {
					t.Fatal(err)
				}
				value.Status = core.EffectConfirmed
				value.ConfirmationEvidenceRefs = []string{"receipt"}
				snapshot.RelatedEvents[0].Payload, _ = json.Marshal(value)
				snapshot.RelatedEvents[0].ArtifactRefs = value.ConfirmationEvidenceRefs
			case "effect-in-work-without-task":
				effect := snapshot.RelatedEvents[0]
				effect.CorrelationID = "work"
				snapshot.Work.Events = []events.Event{snapshot.Work.Events[0], effect}
				snapshot.RelatedEvents = nil
			}
			report, err := ProjectIncident(snapshot, "conversation")
			if variant == "valid" {
				if err != nil || len(report.Containment.Effects) != 1 {
					t.Fatalf("valid effect rejected: %v", err)
				}
			} else if err == nil {
				t.Fatal("accepted invalid direct effect evidence")
			}
		})
	}
}
