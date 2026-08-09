package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/events"
)

type noopLedger struct{}

func (noopLedger) Append(_ context.Context, d events.TrustedDraft) (events.Event, error) {
	return events.Event{EventID: "e", EventType: d.EventType}, nil
}
func (noopLedger) Events(context.Context, string) ([]events.Event, error) { return nil, nil }

func TestAgentCardIsPublic(t *testing.T) {
	h := NewA2A(app.New(events.NewGateway(noopLedger{})), ExternalActor{BearerToken: "secret", OrganizationID: "o"})
	r := httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
}
func TestSubmissionFailsClosedWithoutActorCredential(t *testing.T) {
	h := NewA2A(app.New(events.NewGateway(noopLedger{})), ExternalActor{BearerToken: "secret", OrganizationID: "o"})
	r := httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/send", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", w.Code)
	}
}
