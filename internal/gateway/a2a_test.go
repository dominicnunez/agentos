package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
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
	if !strings.Contains(w.Body.String(), `"version":"1.0.0-dev"`) || !strings.Contains(w.Body.String(), `"description":"Inbound work-level gateway for Agent OS V1"`) {
		t.Fatalf("agent card does not identify Agent OS V1: %s", w.Body.String())
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

func TestSubmissionFailsClosedWithoutCapabilityMapping(t *testing.T) {
	h := NewA2A(app.New(events.NewGateway(noopLedger{})), ExternalActor{BearerToken: "secret", OrganizationID: "o"})
	r := httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/send", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d", w.Code)
	}
}

func TestA2AStatusAndInputContinuation(t *testing.T) {
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	service := app.New(events.NewGateway(l))
	h := NewA2A(service, ExternalActor{ID: "hermes", OrganizationID: "o", BearerToken: "token", Capabilities: []string{"submit_work", "read_status", "provide_input"}})
	body := `{"id":"r1","message":{"role":"user","parts":[{"type":"text","text":"echo hello"}]}}`
	r := httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/send", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("submit=%d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodGet, "/a2a/v1/tasks/r1", nil)
	r.Header.Set("Authorization", "Bearer token")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"state":"completed"`) {
		t.Fatalf("status=%d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"events"`) || strings.Contains(w.Body.String(), `"payload"`) {
		t.Fatalf("status leaked raw ledger data: %s", w.Body.String())
	}
	body = `{"id":"r2","message":{"role":"user","parts":[{"type":"text","text":"human decision"}]},"metadata":{"execution_kind":"HUMAN"}}`
	r = httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/send", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer token")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"BLOCKED"`) {
		t.Fatalf("blocked submit=%d %s", w.Code, w.Body.String())
	}
	unauthorized := NewA2A(service, ExternalActor{ID: "observer", OrganizationID: "o", BearerToken: "observer-token", Capabilities: []string{"read_status"}})
	r = httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/r2/input", strings.NewReader(`{"task_id":"task-r2","text":"forged"}`))
	r.Header.Set("Authorization", "Bearer observer-token")
	w = httptest.NewRecorder()
	unauthorized.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("unauthorized input=%d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/r2/input", strings.NewReader(`{"task_id":"task-r2","text":"detail"}`))
	r.Header.Set("Authorization", "Bearer token")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"state":"completed"`) {
		t.Fatalf("input=%d %s", w.Code, w.Body.String())
	}
	es, err := l.Events(context.Background(), "r2")
	if err != nil {
		t.Fatal(err)
	}
	assertA2AEventOrder(t, es, "A2A_INPUT_RECEIVED", "TASK_RESUMED", "EXECUTION_STARTED", "TOOL_OUTCOME_RECORDED", "EXECUTION_FINISHED", "CANDIDATE_COMPLETE", "COMPLETION_VERIFIED", "TASK_VERIFIED_COMPLETE")
	for _, event := range es {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("external input crossed a governance boundary: %+v", event)
		}
	}
	r = httptest.NewRequest(http.MethodPost, "/a2a/v1/tasks/r2/input", strings.NewReader(`{"task_id":"task-r2","text":"duplicate"}`))
	r.Header.Set("Authorization", "Bearer token")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("duplicate input=%d %s", w.Code, w.Body.String())
	}
}

func assertA2AEventOrder(t *testing.T, stream []events.Event, expected ...string) {
	t.Helper()
	next := 0
	for _, event := range stream {
		if next < len(expected) && event.EventType == expected[next] {
			next++
		}
	}
	if next != len(expected) {
		t.Fatalf("event order missing %q after %d matches: %+v", expected[next], next, stream)
	}
}
