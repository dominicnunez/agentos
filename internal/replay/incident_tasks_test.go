package replay

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

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
	intent := seal("intent", "intent", "INTENT_CREATED", "", 1, map[string]any{"id": "intent", "organization_id": "org", "normalized_objective": "objective", "created_at": now})
	work := seal("work", "work-id", "WORK_CREATED", "", 2, map[string]any{"id": "work-id", "intent_id": "intent", "objective": "objective", "status": "ACTIVE", "created_at": now})
	task := seal("task", "task-id", "TASK_CREATED", "task-id", 3, map[string]any{"id": "task-id", "work_id": "work-id", "description": "task", "task_contract_version": "1", "execution_kind": "HUMAN", "model_inference_policy": "DISALLOWED", "status": "PENDING"})
	value := map[string]any{"effect_obligation_id": "effect-id", "organization_id": "org", "task_id": "task-id", "actor_id": "owner", "action": "send", "resource": "destination", "scope": "org", "idempotency_key": "key", "effect_fingerprint": "legacy", "authorization_refs": []string{"lease"}, "status": "ATTEMPTED", "attempt_count": 1}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	effect := events.Event{EventID: "effect", Sequence: 4, OrganizationID: "org", TaskID: "task-id", EventType: "EFFECT_OBLIGATION_TRANSITIONED", AuthorizationRefs: []string{"lease"}, Payload: body, CreatedAt: now, SchemaVersion: events.SchemaVersion}
	return events.IncidentSnapshot{Work: events.VerifiedEventSnapshot{OrganizationID: "org", CorrelationID: "work", Algorithm: "SHA-256", LedgerEvents: 4, LedgerSequence: 4, LedgerEventID: "effect", LedgerSHA256: strings.Repeat("a", 64), Events: []events.Event{intent, work, task}}, RelatedEvents: []events.Event{effect}}
}

func TestProjectIncidentValidatesEffectAdmission(t *testing.T) {
	for _, variant := range []string{"valid", "invalid-state", "effect-in-work-without-task", "pending-labelled-attempt"} {
		t.Run(variant, func(t *testing.T) {
			snapshot := admittedEffectFixture(t)
			switch variant {
			case "pending-labelled-attempt":
				var value map[string]any
				if err := json.Unmarshal(snapshot.RelatedEvents[0].Payload, &value); err != nil {
					t.Fatal(err)
				}
				value["status"], value["attempt_count"] = "PENDING", 0
				snapshot.RelatedEvents[0].Payload, _ = json.Marshal(value)
				snapshot.Admissions = []events.IncidentAdmission{{EventRef: "effect", Kind: "EFFECT_ATTEMPT", TaskID: "task-id"}}
			case "invalid-state":
				var value map[string]any
				if err := json.Unmarshal(snapshot.RelatedEvents[0].Payload, &value); err != nil {
					t.Fatal(err)
				}
				value["status"] = "CONFIRMED"
				value["confirmation_evidence_refs"] = []string{"receipt"}
				snapshot.RelatedEvents[0].Payload, _ = json.Marshal(value)
				snapshot.RelatedEvents[0].ArtifactRefs = []string{"receipt"}
			case "effect-in-work-without-task":
				effect := snapshot.RelatedEvents[0]
				effect.CorrelationID = "work"
				snapshot.Work.Events = []events.Event{snapshot.Work.Events[0], snapshot.Work.Events[1], effect}
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
