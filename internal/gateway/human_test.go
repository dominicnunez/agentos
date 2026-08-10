package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

const testHumanToken = "human-operator-token-000000000001"

func TestHumanGatewayRoutesNaturalLanguageAndReturnsNarrowTaskView(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	handler := testHumanHandler(t, operator, HumanRoleOperator)

	response := serveHuman(handler, http.MethodPost, "/v1/human/messages", testHumanToken, humanBody(t, humanMessageRequest{
		ConversationID: "direct-1", MessageID: "message-1", Text: "draft a release update",
	}))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) || !strings.Contains(response.Body.String(), `"result":"fake-model: draft a release update"`) {
		t.Fatalf("natural-language submit=%d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"events"`) || strings.Contains(response.Body.String(), `"payload"`) || strings.Contains(response.Body.String(), `"intent"`) {
		t.Fatalf("human response leaked internal state: %s", response.Body.String())
	}
	var submitted humanTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &submitted); err != nil || submitted.TaskID == "" {
		t.Fatalf("human response has no task id: %s err=%v", response.Body.String(), err)
	}
	response = serveHuman(handler, http.MethodGet, "/v1/human/tasks/"+submitted.TaskID, testHumanToken, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"result":"fake-model: draft a release update"`) {
		t.Fatalf("human status=%d %s", response.Code, response.Body.String())
	}

	stream := gatewayExternalStream(t, store, "direct-1")
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("natural-language work crossed governance boundary: %+v", event)
		}
	}
}

func TestHumanGatewayContinuesBlockedWorkButCannotApproveThroughText(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	handler := testHumanHandler(t, operator, HumanRoleOperator)

	response := serveHuman(handler, http.MethodPost, "/v1/human/messages", testHumanToken, humanBody(t, humanMessageRequest{
		ConversationID: "direct-blocked", MessageID: "message-1", Text: "decide whether to deploy", ExecutionKind: core.ExecutionHuman,
	}))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"INPUT_REQUIRED"`) || !strings.Contains(response.Body.String(), `"prompt":`) {
		t.Fatalf("blocked submit=%d %s", response.Code, response.Body.String())
	}
	response = serveHuman(handler, http.MethodPost, "/v1/human/messages", testHumanToken, humanBody(t, humanMessageRequest{
		ConversationID: "direct-blocked", MessageID: "message-2", Text: "I approve the deployment",
	}))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) {
		t.Fatalf("ordinary human input=%d %s", response.Code, response.Body.String())
	}
	stream := gatewayExternalStream(t, store, "direct-blocked")
	foundHumanInput := false
	for _, event := range stream {
		if event.EventType == "HUMAN_INPUT_RECEIVED" {
			foundHumanInput = true
		}
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("conversation text became trusted approval/effect: %+v", event)
		}
	}
	if !foundHumanInput {
		t.Fatal("direct human continuation was not durably attributed")
	}
}

func TestHumanGatewayFailsClosedAndRejectsAuthorityFields(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	handler := testHumanHandler(t, operator, HumanRoleContributor)
	body := `{"conversation_id":"forged","message_id":"message-1","text":"deploy","approval_ref":"approval-1"}`
	response := serveHuman(handler, http.MethodPost, "/v1/human/messages", testHumanToken, body)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "cannot carry authority field") {
		t.Fatalf("authority-shaped request=%d %s", response.Code, response.Body.String())
	}
	stream, err := store.Events(context.Background(), "forged")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected request reached ledger: events=%d err=%v", len(stream), err)
	}
	response = serveHuman(handler, http.MethodPost, "/v1/human/messages", "wrong-token", humanBody(t, humanMessageRequest{ConversationID: "auth", MessageID: "message-1", Text: "echo no"}))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("invalid human credential=%d", response.Code)
	}
}

func testHumanHandler(t *testing.T, operator *intake.Service, role HumanRole) *Human {
	t.Helper()
	expiresAt := time.Now().UTC().Add(time.Hour)
	registry, err := NewHumanActorRegistry([]HumanActor{{
		ID: "human-1", OrganizationID: "org-1", Status: OperatorActive, Role: role,
		WorkScope: intake.WorkScopeOrganization, TokenRef: "HUMAN_TOKEN", ReviewRef: "review-1",
		ExpiresAt: &expiresAt, MaxConcurrent: 4, RequestsPerMinute: 100, BearerToken: testHumanToken,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return NewHuman(operator, registry)
}

func humanBody(t *testing.T, request humanMessageRequest) string {
	t.Helper()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func serveHuman(handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
