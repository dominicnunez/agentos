package events

import (
	"context"
	"testing"
)

type memoryLedger struct{ events []Event }

func (m *memoryLedger) Append(_ context.Context, d TrustedDraft) (Event, error) {
	e := Event{EventID: "1", EventType: d.EventType, OrganizationID: d.OrganizationID}
	m.events = append(m.events, e)
	return e, nil
}
func (m *memoryLedger) Events(context.Context, string) ([]Event, error) { return m.events, nil }

func TestAgentCannotMintTrustedControlEvent(t *testing.T) {
	g := NewGateway(&memoryLedger{})
	_, err := g.PublishAgentDraft(context.Background(), "org", "agent", "execution", "correlation", Draft{EventType: "APPROVAL_DECIDED", Payload: map[string]any{}})
	if err == nil {
		t.Fatal("trusted control event accepted from agent draft")
	}
}
