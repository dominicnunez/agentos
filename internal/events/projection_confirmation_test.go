package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestProjectionHistoryCountsLateConfirmation(t *testing.T) {
	now := time.Now().UTC()
	draft := core.IntentDraft{ID: "intent-incident", OrganizationID: "org-1", Version: 1, Status: core.IntentStatusReadyForReview, Mode: core.IntentModeStandard, RequestedExecutionKind: core.ExecutionDeterministic, Objective: "Prepare bounded work.", Deliverables: []core.IntentValue{{Value: "result", Origin: "USER"}}, CompletionCriteria: []core.IntentValue{{Value: "verified result", Origin: "USER"}}, CreatedAt: now}
	var err error
	draft.Fingerprint, err = core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := func(id, label, actor string, seq int64, payload any) Event {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return Event{EventID: id, Sequence: seq, OrganizationID: "org-1", CorrelationID: "incident", EventType: label, SourceActorID: actor, TaskID: "task-incident", CreatedAt: now, SchemaVersion: SchemaVersion, Payload: body}
	}
	intake := ordinary("intake", "INTAKE_MESSAGE_RECORDED", "user-1", 2, IntakeMessageRecordedPayload{MessageID: "message-1", Text: draft.Objective, SourcePrincipalID: "user-1", SourcePrincipalKind: "HUMAN", SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: core.ExecutionDeterministic})
	review := ordinary("draft", "INTENT_DRAFTED", "runtime", 3, IntentDraftedPayload{SourceMessageID: "message-1", Draft: draft, Reply: "Review bounded work."})
	confirmed := ordinary("confirm", "INTENT_CONFIRMED", "user-1", 4, IntentConfirmedPayload{IntentID: string(draft.ID), Version: 1, Fingerprint: draft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: "HUMAN", SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1"})
	intent := core.Intent{ID: draft.ID, OrganizationID: "org-1", OriginalInstruction: draft.Objective, NormalizedObjective: draft.Objective, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "message-1", AcceptedFingerprint: draft.Fingerprint, CreatedAt: now}
	stream := []Event{
		incidentProjectionEvent(t, 1, "ORGANIZATION_CREATED", "organization", "org-1", 1, "", core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "1", CreatedAt: now}),
		intake, review, confirmed,
		incidentProjectionEvent(t, 5, "INTENT_CREATED", "intent", string(intent.ID), 1, "", intent),
		incidentProjectionEvent(t, 6, "WORK_CREATED", "work", "work-1", 1, "", core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}),
	}
	if _, err := ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("valid reviewed Work rejected: %v", err)
	}
	confirmed.EventID, confirmed.Sequence = "late-confirmation", 7
	stream = append(stream, confirmed)
	if _, err := ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
		t.Fatal("accepted duplicate confirmation after the final Work admission")
	}
}
