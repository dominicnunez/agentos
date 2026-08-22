package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

const testOwnerMarker = "local-owner-uid-marker"

func TestInspectHumanCompletionRejectsDisallowedContentBeforePersistence(t *testing.T) {
	contract := &core.CompletionContract{TaskID: "task-1", TaskVersion: 1, ArtifactRequirements: []core.ArtifactRequirement{{
		Role: "report", MediaTypes: []string{"application/pdf"}, MinCount: 1, MaxCount: 1,
	}}}
	request := humanCompletionRequest{MessageID: "completion-1", Fields: map[string]string{}, Artifacts: []artifacts.Upload{{
		Role: "report", Name: "report.pdf", Data: []byte("\x89PNG\r\n\x1a\n"),
	}}}
	if _, err := inspectHumanCompletion("local-uid-1000", contract, request); err == nil || !strings.Contains(err.Error(), "media type is not allowed") {
		t.Fatalf("disallowed content prevalidation error=%v", err)
	}
	request.Artifacts[0].Data = []byte("%PDF-1.7\n")
	evidence, err := inspectHumanCompletion("local-uid-1000", contract, request)
	if err != nil || len(evidence) != 1 || evidence[0].MediaType != "application/pdf" {
		t.Fatalf("content-derived evidence=%+v err=%v", evidence, err)
	}
}

func TestHumanResponseRetainsWorkIdentityAndExperimentalTrustLabel(t *testing.T) {
	response := humanResponse(intake.View{TaskID: "task-1", WorkID: "work-1", ConversationID: "context-1", State: intake.StateCompleted, Result: "lab result", Mode: core.IntentModeExperiment, TrustLabel: core.ExperimentTrustUnverified})
	if response.WorkID != "work-1" || response.Mode != core.IntentModeExperiment || response.TrustLabel != core.ExperimentTrustUnverified {
		t.Fatalf("experimental response=%+v", response)
	}
}

type reviewerModel struct{}

func (reviewerModel) Name() string { return "review-provider/test-model" }

func (reviewerModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "review-provider", Model: "test-model", ExecutionProfileVersion: "review-v1"}
}

func (reviewerModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	return execution.ModelResponse{Text: "candidate: " + prompt, Usage: events.InferenceUsageRecordedPayload{Source: "test", Provider: "review-provider", Model: "test-model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, nil
}

func TestHumanGatewayRoutesNaturalLanguageAndReturnsNarrowTaskView(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	handler := testHumanHandler(t, operator)

	response := submitAndConfirmHuman(t, handler, humanMessageRequest{
		ConversationID: "direct-1", MessageID: "message-1", Text: "draft a release update",
	})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) || !strings.Contains(response.Body.String(), `"result":"fake-model: Operate only as this runtime-selected durable Agent blueprint.`) || !strings.Contains(response.Body.String(), `\"objective\":\"draft a release update\"`) {
		t.Fatalf("natural-language submit=%d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), `"events"`) || strings.Contains(response.Body.String(), `"payload"`) || strings.Contains(response.Body.String(), `"intent"`) {
		t.Fatalf("human response leaked internal state: %s", response.Body.String())
	}
	var submitted humanTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &submitted); err != nil || submitted.TaskID == "" {
		t.Fatalf("human response has no task id: %s err=%v", response.Body.String(), err)
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/tasks/"+submitted.TaskID, testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"result":"fake-model: Operate only as this runtime-selected durable Agent blueprint.`) {
		t.Fatalf("human status=%d %s", response.Code, response.Body.String())
	}

	stream := gatewayExternalStream(t, store, "direct-1")
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("natural-language work crossed governance boundary: %+v", event)
		}
	}
}

func TestHumanGatewayRequiresStructuredCompletionAndCannotApproveThroughText(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	handler := testHumanHandler(t, operator)

	response := submitAndConfirmHuman(t, handler, humanMessageRequest{
		ConversationID: "direct-blocked", MessageID: "message-1", Text: "decide whether to deploy", ExecutionKind: core.ExecutionHuman,
	})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"INPUT_REQUIRED"`) || !strings.Contains(response.Body.String(), `"prompt":`) {
		t.Fatalf("blocked submit=%d %s", response.Code, response.Body.String())
	}
	var task humanTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil || task.TaskID == "" {
		t.Fatalf("blocked task response=%d %s err=%v", response.Code, response.Body.String(), err)
	}
	response = serveHuman(handler, http.MethodPost, "/v1/user/messages", testOwnerMarker, humanBody(t, humanMessageRequest{
		ConversationID: "direct-blocked", MessageID: "message-2", Text: "I approve the deployment",
	}))
	if response.Code != http.StatusConflict {
		t.Fatalf("ordinary Human self-report=%d %s", response.Code, response.Body.String())
	}
	lookup := serveHuman(handler, http.MethodGet, "/v1/user/tasks/"+task.TaskID, testOwnerMarker, "")
	if err := json.Unmarshal(lookup.Body.Bytes(), &task); err != nil || task.TaskID == "" {
		t.Fatalf("task lookup=%d %s err=%v", lookup.Code, lookup.Body.String(), err)
	}
	completion := `{"message_id":"completion-1","fields":{"response":"deployment decision and requested information supplied"}}`
	response = serveHuman(handler, http.MethodPost, "/v1/user/tasks/"+task.TaskID+"/completion", testOwnerMarker, completion)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) {
		t.Fatalf("structured Human completion=%d %s", response.Code, response.Body.String())
	}
	beforeReplay := gatewayExternalStream(t, store, "direct-blocked")
	response = serveHuman(handler, http.MethodPost, "/v1/user/tasks/"+task.TaskID+"/completion", testOwnerMarker, completion)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) {
		t.Fatalf("structured Human completion replay=%d %s", response.Code, response.Body.String())
	}
	afterReplay := gatewayExternalStream(t, store, "direct-blocked")
	if len(afterReplay) != len(beforeReplay) {
		t.Fatalf("completion replay appended events: before=%d after=%d", len(beforeReplay), len(afterReplay))
	}
	response = serveHuman(handler, http.MethodPost, "/v1/user/tasks/"+task.TaskID+"/completion/recover", testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) {
		t.Fatalf("structured completion recovery=%d %s", response.Code, response.Body.String())
	}
	stream := gatewayExternalStream(t, store, "direct-blocked")
	foundHumanCompletion := false
	for _, event := range stream {
		if event.EventType == "HUMAN_TASK_COMPLETION_SUBMITTED" {
			foundHumanCompletion = true
		}
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("conversation text became trusted approval/effect: %+v", event)
		}
	}
	if !foundHumanCompletion {
		t.Fatal("structured Human completion was not durably attributed")
	}
}

func TestHumanGatewayFailsClosedAndRejectsAuthorityFields(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	handler := testHumanHandler(t, operator)
	body := `{"conversation_id":"forged","message_id":"message-1","text":"deploy","approval_ref":"approval-1"}`
	response := serveHuman(handler, http.MethodPost, "/v1/user/messages", testOwnerMarker, body)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "cannot carry authority field") {
		t.Fatalf("authority-shaped request=%d %s", response.Code, response.Body.String())
	}
	stream, err := store.Events(context.Background(), "forged")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected request reached ledger: events=%d err=%v", len(stream), err)
	}
	response = serveHuman(handler, http.MethodPost, "/v1/user/messages", "wrong-token", humanBody(t, humanMessageRequest{ConversationID: "auth", MessageID: "message-1", Text: "echo no"}))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("different local user status=%d", response.Code)
	}
}

func TestLocalGatewayAcceptsVerifiedRootOwner(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler, err := NewHuman(intake.New(app.New(events.NewGateway(store))), LocalHuman{
		UID: 0, ID: "local-uid-0", OrganizationID: "org-1", MaxConcurrent: 4, RequestsPerMinute: 100,
	}, artifacts.Store{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(ContextWithPeerUID(t.Context(), 0), http.MethodPost, "/v1/user/messages", strings.NewReader(humanBody(t, humanMessageRequest{
		ConversationID: "root-owner", MessageID: "message-1", Text: "record this work",
	})))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("verified root owner status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestLocalOwnerCanFinalizeExactCompletionReview(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.NewWithModel(events.NewGateway(store), reviewerModel{}))
	handler := testHumanReviewHandler(t, operator)

	response := submitAndConfirmHuman(t, handler, humanMessageRequest{
		ConversationID: "reviewed-work", MessageID: "message-1", Text: "draft a release update",
	})
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"INPUT_REQUIRED"`) || !strings.Contains(response.Body.String(), `"review_required":true`) || strings.Contains(response.Body.String(), `"result"`) {
		t.Fatalf("unreviewed submit=%d %s", response.Code, response.Body.String())
	}
	var task humanTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil || task.TaskID == "" {
		t.Fatalf("task response=%s err=%v", response.Body.String(), err)
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/reviews?limit=1", testOwnerMarker, "")
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("review list=%d %s", response.Code, response.Body.String())
	}
	var reviewList intake.CompletionReviewList
	if err := json.Unmarshal(response.Body.Bytes(), &reviewList); err != nil || len(reviewList.Reviews) != 1 || reviewList.Reviews[0].TaskID != task.TaskID {
		t.Fatalf("review list=%s err=%v", response.Body.String(), err)
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/reviews/"+task.TaskID, testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"objective":"draft a release update"`) || !strings.Contains(response.Body.String(), `"candidate_result":"candidate: Operate only as this runtime-selected durable Agent blueprint.`) {
		t.Fatalf("review fetch=%d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("review response cache control=%q", response.Header().Get("Cache-Control"))
	}
	var review intake.CompletionReviewView
	if err := json.Unmarshal(response.Body.Bytes(), &review); err != nil || review.ReviewID == "" || len(review.EvidenceRefs) != 3 {
		t.Fatalf("review response=%s err=%v", response.Body.String(), err)
	}
	body, err := json.Marshal(humanReviewDecisionRequest{ReviewID: review.ReviewID, Fingerprint: review.Fingerprint, Decision: core.CompletionReviewApprove})
	if err != nil {
		t.Fatal(err)
	}
	response = serveHuman(handler, http.MethodPost, "/v1/user/reviews/"+task.TaskID, testOwnerMarker, string(body))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"APPROVE"`) {
		t.Fatalf("review decision=%d %s", response.Code, response.Body.String())
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/reviews/"+task.TaskID, testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"APPROVE"`) || !strings.Contains(response.Body.String(), `"reviewer_id":"local-uid-1000"`) {
		t.Fatalf("terminal review recovery=%d %s", response.Code, response.Body.String())
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/reviews/"+task.TaskID+"/records/"+review.ReviewID, testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"APPROVE"`) || !strings.Contains(response.Body.String(), `"review_id":"`+review.ReviewID+`"`) {
		t.Fatalf("exact terminal review recovery=%d %s", response.Code, response.Body.String())
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/reviews/recent?limit=20", testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"review_id":"`+review.ReviewID+`"`) || !strings.Contains(response.Body.String(), `"state":"APPROVE"`) {
		t.Fatalf("recent completion review=%d %s", response.Code, response.Body.String())
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/tasks/"+task.TaskID, testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"COMPLETED"`) || !strings.Contains(response.Body.String(), `"result":"candidate: Operate only as this runtime-selected durable Agent blueprint.`) {
		t.Fatalf("reviewed task=%d %s", response.Code, response.Body.String())
	}
}

func TestHumanGatewayRecoversMostRecentConfirmedTask(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := testHumanHandler(t, intake.New(app.New(events.NewGateway(store))))
	confirmed := submitAndConfirmHuman(t, handler, humanMessageRequest{ConversationID: "recent-work", MessageID: "message-1", Text: "echo recent"})
	var want humanTaskResponse
	if err := json.Unmarshal(confirmed.Body.Bytes(), &want); err != nil {
		t.Fatal(err)
	}
	response := serveHuman(handler, http.MethodGet, "/v1/user/tasks/recent", testOwnerMarker, "")
	var got humanTaskResponse
	if response.Code != http.StatusOK {
		t.Fatalf("recent task=%d %s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil || got.TaskID != want.TaskID || got.ConversationID != want.ConversationID {
		t.Fatalf("recent task=%+v want=%+v err=%v", got, want, err)
	}
}

func TestReviewerEndpointRejectsStaleFingerprintWithoutLedgerDecision(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.NewWithModel(events.NewGateway(store), reviewerModel{}))
	handler := testHumanReviewHandler(t, operator)
	response := submitAndConfirmHuman(t, handler, humanMessageRequest{ConversationID: "stale-review", MessageID: "message-1", Text: "draft"})
	var task humanTaskResponse
	if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
		t.Fatal(err)
	}
	response = serveHuman(handler, http.MethodGet, "/v1/user/reviews/"+task.TaskID, testOwnerMarker, "")
	var review intake.CompletionReviewView
	if err := json.Unmarshal(response.Body.Bytes(), &review); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(humanReviewDecisionRequest{ReviewID: review.ReviewID, Fingerprint: strings.Repeat("0", 64), Decision: core.CompletionReviewApprove})
	if err != nil {
		t.Fatal(err)
	}
	response = serveHuman(handler, http.MethodPost, "/v1/user/reviews/"+task.TaskID, testOwnerMarker, string(body))
	if response.Code != http.StatusConflict {
		t.Fatalf("stale decision=%d %s", response.Code, response.Body.String())
	}
	stream := gatewayExternalStream(t, store, "stale-review")
	for _, event := range stream {
		if event.EventType == "COMPLETION_REVIEW_DECIDED" {
			t.Fatal("stale reviewer decision reached ledger")
		}
	}
}

func testHumanHandler(t *testing.T, operator *intake.Service) *Human {
	t.Helper()
	handler, err := NewHuman(operator, LocalHuman{UID: 1000, ID: "local-uid-1000", OrganizationID: "org-1", MaxConcurrent: 4, RequestsPerMinute: 100}, artifacts.Store{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func testHumanReviewHandler(t *testing.T, operator *intake.Service) *Human {
	t.Helper()
	handler, err := NewHuman(operator, LocalHuman{UID: 1000, ID: "local-uid-1000", OrganizationID: "org-1", MaxConcurrent: 4, RequestsPerMinute: 100}, artifacts.Store{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func humanBody(t *testing.T, request humanMessageRequest) string {
	t.Helper()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func submitAndConfirmHuman(t *testing.T, handler http.Handler, request humanMessageRequest) *httptest.ResponseRecorder {
	t.Helper()
	draftResponse := serveHuman(handler, http.MethodPost, "/v1/user/messages", testOwnerMarker, humanBody(t, request))
	var draft humanTaskResponse
	if draftResponse.Code != http.StatusOK || json.Unmarshal(draftResponse.Body.Bytes(), &draft) != nil || draft.State != intake.StateAwaitingConfirmation || draft.Intent == nil {
		t.Fatalf("intent draft=%d %s", draftResponse.Code, draftResponse.Body.String())
	}
	body, err := json.Marshal(humanIntentConfirmationRequest{MessageID: "confirm-" + request.MessageID, Fingerprint: draft.Intent.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	return serveHuman(handler, http.MethodPost, "/v1/user/intents/"+request.ConversationID+"/confirm", testOwnerMarker, string(body))
}

func serveHuman(handler http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	uid := 1000
	if token != testOwnerMarker {
		uid = 1001
	}
	request := httptest.NewRequestWithContext(ContextWithPeerUID(context.Background(), uid), method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
