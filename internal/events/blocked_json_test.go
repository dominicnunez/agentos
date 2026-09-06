package events

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestTaskBlockedRejectsNoncanonicalFieldsBeforeAppend(t *testing.T) {
	for _, payload := range []string{
		`{"REASON":"missing access","missing":"input","why_needed":"analysis","work_completed":"none"}`,
		`{"reason":"first","reason":"missing access","missing":"input","why_needed":"analysis","work_completed":"none"}`,
		`{"reason":"missing access","missing":"input","why_needed":"analysis","work_completed":"none","extra":true}`,
	} {
		ledger := &memoryLedger{}
		gateway := NewGateway(ledger)
		gateway.SetRouteValidator(routeValidatorFunc(func(context.Context, AddressedRoute) error { return nil }))
		_, err := gateway.PublishAgentDraft(t.Context(), "org", "agent", "execution", "correlation", Draft{
			EventType: "TASK_BLOCKED", TaskID: "child", RecipientScope: RecipientTask, RecipientID: "parent", Payload: json.RawMessage(payload),
		})
		if err == nil || len(ledger.events) != 0 {
			t.Errorf("noncanonical blocked contract reached ledger append: %s", payload)
		}
	}
}

func TestTaskBlockedPreservesCanonicalOptionalFields(t *testing.T) {
	ledger := &blockedPayloadLedger{}
	gateway := NewGateway(ledger)
	gateway.SetRouteValidator(routeValidatorFunc(func(context.Context, AddressedRoute) error { return nil }))
	payload := TaskBlockedPayload{
		Code: "INPUT_MISSING", Reason: "missing access", Missing: "input", WhyNeeded: "analysis",
		WorkCompleted: "validated available inputs", RemainingWork: "finish analysis",
		EvidenceRefs: []string{"evidence-1"}, Urgency: "normal",
	}
	event, err := gateway.PublishAgentDraft(t.Context(), "org", "agent", "execution", "correlation", Draft{
		EventType: "TASK_BLOCKED", TaskID: "child", RecipientScope: RecipientTask, RecipientID: "parent", Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	var actual TaskBlockedPayload
	if err := json.Unmarshal(event.Payload, &actual); err != nil {
		t.Fatal(err)
	}
	if len(ledger.events) != 1 || !reflect.DeepEqual(actual, payload) {
		t.Fatalf("canonical blocked contract changed: %+v", actual)
	}
}

type blockedPayloadLedger struct{ memoryLedger }

func (l *blockedPayloadLedger) Append(ctx context.Context, draft TrustedDraft) (Event, error) {
	payload, err := json.Marshal(draft.Payload)
	if err != nil {
		return Event{}, err
	}
	event, err := l.memoryLedger.Append(ctx, draft)
	event.Payload = payload
	return event, err
}
