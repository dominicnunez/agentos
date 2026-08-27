package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestHumanOrganizationViewIsReadOnlyAndTenantScoped(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	first := testHumanHandler(t, operator)
	second, err := NewHuman(operator, LocalHuman{UID: 1000, ID: "local-uid-1000", OrganizationID: "org-2", MaxConcurrent: 4, RequestsPerMinute: 100}, artifacts.Store{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	firstResult := submitAndConfirmHuman(t, first, humanMessageRequest{ConversationID: "organization-first", MessageID: "message-first", Text: "echo first tenant objective"})
	secondResult := submitAndConfirmHuman(t, second, humanMessageRequest{ConversationID: "organization-second", MessageID: "message-second", Text: "echo second tenant objective"})
	if firstResult.Code != http.StatusOK || secondResult.Code != http.StatusOK {
		t.Fatalf("seed organization views first=%d second=%d", firstResult.Code, secondResult.Code)
	}

	response := serveHuman(first, http.MethodGet, "/v1/user/organization", testOwnerMarker, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"id":"org-1"`) || !strings.Contains(response.Body.String(), "first tenant objective") {
		t.Fatalf("organization view=%d %s", response.Code, response.Body.String())
	}
	for _, forbidden := range []string{"org-2", "second tenant objective", "operating_instructions", "tool_refs", "event_type", "payload", "authorization_refs"} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("organization view leaked %q: %s", forbidden, response.Body.String())
		}
	}
	if response := serveHuman(first, http.MethodGet, "/v1/user/organization?scope=all", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("organization query expansion=%d %s", response.Code, response.Body.String())
	}
	if response := serveHuman(first, http.MethodPost, "/v1/user/organization", testOwnerMarker, `{}`); response.Code != http.StatusNotFound {
		t.Fatalf("organization mutation=%d %s", response.Code, response.Body.String())
	}
	evidence := serveHuman(first, http.MethodGet, "/v1/user/aims/evidence", testOwnerMarker, "")
	if evidence.Code != http.StatusOK || evidence.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(evidence.Header().Get("Content-Disposition"), "agentos-aims-evidence.json") ||
		!strings.Contains(evidence.Body.String(), `"status":"READINESS_WORK_IN_PROGRESS"`) ||
		!strings.Contains(evidence.Body.String(), `"certified":false`) {
		t.Fatalf("AIMS evidence=%d headers=%v body=%s", evidence.Code, evidence.Header(), evidence.Body.String())
	}
	digest := sha256.Sum256(evidence.Body.Bytes())
	if got, want := evidence.Header().Get("X-AgentOS-SHA256"), hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("AIMS evidence checksum=%q want=%q", got, want)
	}
	for _, forbidden := range []string{"org-2", "second tenant objective", "first tenant objective", "operating_instructions", "tool_refs", "event_type", "payload", "authorization_refs"} {
		if strings.Contains(evidence.Body.String(), forbidden) {
			t.Fatalf("AIMS evidence leaked %q: %s", forbidden, evidence.Body.String())
		}
	}
	if response := serveHuman(first, http.MethodGet, "/v1/user/aims/evidence?scope=all", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("AIMS query expansion=%d %s", response.Code, response.Body.String())
	}
	if response := serveHuman(first, http.MethodPost, "/v1/user/aims/evidence", testOwnerMarker, `{}`); response.Code != http.StatusNotFound {
		t.Fatalf("AIMS mutation=%d %s", response.Code, response.Body.String())
	}
	inspection := serveHuman(first, http.MethodGet, "/v1/user/governance/inspection", testOwnerMarker, "")
	if inspection.Code != http.StatusOK || inspection.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(inspection.Body.String(), `"schema_version":"agentos.governance.inspection.v1"`) ||
		!strings.Contains(inspection.Body.String(), `"assessment":"RUNTIME_GOVERNANCE_INSPECTION_ONLY"`) ||
		!strings.Contains(inspection.Body.String(), `"certified":false`) ||
		!strings.Contains(inspection.Body.String(), `"verification":"COMPLETE_LEDGER_CHAIN"`) {
		t.Fatalf("governance inspection=%d headers=%v body=%s", inspection.Code, inspection.Header(), inspection.Body.String())
	}
	inspectionDigest := sha256.Sum256(inspection.Body.Bytes())
	if got, want := inspection.Header().Get("X-AgentOS-SHA256"), hex.EncodeToString(inspectionDigest[:]); got != want {
		t.Fatalf("governance inspection checksum=%q want=%q", got, want)
	}
	for _, forbidden := range []string{"org-2", "second tenant objective", "first tenant objective", "operating_instructions", "tool_refs", "payload", "authorization_refs"} {
		if strings.Contains(inspection.Body.String(), forbidden) {
			t.Fatalf("governance inspection leaked %q: %s", forbidden, inspection.Body.String())
		}
	}
	if response := serveHuman(first, http.MethodGet, "/v1/user/governance/inspection?scope=all", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("governance inspection query expansion=%d %s", response.Code, response.Body.String())
	}
	if response := serveHuman(first, http.MethodPost, "/v1/user/governance/inspection", testOwnerMarker, `{}`); response.Code != http.StatusNotFound {
		t.Fatalf("governance inspection mutation=%d %s", response.Code, response.Body.String())
	}
}

func TestHumanIncidentReplayIsVerifiedPayloadFreeAndTenantScoped(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operator := intake.New(app.New(events.NewGateway(store)))
	first := testHumanHandler(t, operator)
	second, err := NewHuman(operator, LocalHuman{UID: 1000, ID: "local-uid-1000", OrganizationID: "org-2", MaxConcurrent: 4, RequestsPerMinute: 100}, artifacts.Store{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	privateText := "echo private incident content"
	if response := submitAndConfirmHuman(t, first, humanMessageRequest{ConversationID: "incident-first", MessageID: "message-first", Text: privateText}); response.Code != http.StatusOK {
		t.Fatalf("seed first incident=%d %s", response.Code, response.Body.String())
	}
	if response := submitAndConfirmHuman(t, second, humanMessageRequest{ConversationID: "incident-second", MessageID: "message-second", Text: "echo other tenant secret"}); response.Code != http.StatusOK {
		t.Fatalf("seed second incident=%d %s", response.Code, response.Body.String())
	}

	response := serveHuman(first, http.MethodGet, "/v1/user/incidents/replay?conversation_id=incident-first", testOwnerMarker, "")
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(response.Body.String(), `"conversation_id":"incident-first"`) ||
		!strings.Contains(response.Body.String(), `"algorithm":"SHA-256"`) ||
		!strings.Contains(response.Body.String(), `"verification":"COMPLETE_LEDGER_CHAIN"`) ||
		!strings.Contains(response.Body.String(), `"payload_sha256"`) ||
		!strings.Contains(response.Body.String(), `"kind":"STREAM_PREDECESSOR"`) {
		t.Fatalf("incident replay=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	for _, forbidden := range []string{privateText, "other tenant secret", `"payload":`, `"organization_id":"org-2"`, `"ledger_events":`, `"ledger_event_id":`, `"ledger_sha256":`, `"sequence":`} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("incident replay leaked %q: %s", forbidden, response.Body.String())
		}
	}
	if response := serveHuman(first, http.MethodGet, "/v1/user/incidents/replay?conversation_id=incident-second", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant incident replay=%d %s", response.Code, response.Body.String())
	}
	if response := serveHuman(first, http.MethodGet, "/v1/user/incidents/replay?conversation_id=incident-first&scope=all", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("incident query expansion=%d %s", response.Code, response.Body.String())
	}
	if response := serveHuman(first, http.MethodGet, "/v1/user/incidents/replay?conversation_id=incident-first&conversation_id=incident-second", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("duplicate incident query=%d %s", response.Code, response.Body.String())
	}
	if response := serveHuman(first, http.MethodPost, "/v1/user/incidents/replay?conversation_id=incident-first", testOwnerMarker, `{}`); response.Code != http.StatusNotFound {
		t.Fatalf("incident replay mutation=%d %s", response.Code, response.Body.String())
	}
}

func TestHumanGatewayBootstrapsStrategyWithoutCreatingAuthority(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := testHumanHandler(t, intake.New(app.New(events.NewGateway(store))))
	request := userStrategyBootstrapRequest{
		RequestID: "strategy-1", MissionID: "mission-1", MissionStatement: "Build a governed artificial organization",
		GoalID: "goal-1", GoalObjective: "Complete a verified workflow", GoalMode: core.GoalTarget,
		SuccessCriteria: []string{"The Work is complete", "Completion evidence is durable"},
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if response := serveHuman(handler, http.MethodPost, "/v1/user/strategy/bootstrap", "wrong-owner", string(body)); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-owner strategy=%d %s", response.Code, response.Body.String())
	}
	response := serveHuman(handler, http.MethodPost, "/v1/user/strategy/bootstrap", testOwnerMarker, string(body))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"id":"mission-1"`) || !strings.Contains(response.Body.String(), `"id":"goal-1"`) {
		t.Fatalf("strategy bootstrap=%d %s", response.Code, response.Body.String())
	}
	if replay := serveHuman(handler, http.MethodPost, "/v1/user/strategy/bootstrap", testOwnerMarker, string(body)); replay.Code != http.StatusOK {
		t.Fatalf("strategy replay=%d %s", replay.Code, replay.Body.String())
	}
	if response := serveHuman(handler, http.MethodGet, "/v1/user/strategy/bootstrap", testOwnerMarker, ""); response.Code != http.StatusNotFound {
		t.Fatalf("strategy GET=%d %s", response.Code, response.Body.String())
	}
	unknown := strings.TrimSuffix(string(body), "}") + `,"approval_authority":true}`
	if response := serveHuman(handler, http.MethodPost, "/v1/user/strategy/bootstrap", testOwnerMarker, unknown); response.Code != http.StatusBadRequest {
		t.Fatalf("authority-shaped strategy=%d %s", response.Code, response.Body.String())
	}
	stream, err := store.Events(t.Context(), request.RequestID)
	if err != nil || len(stream) != 3 {
		t.Fatalf("strategy stream=%+v err=%v", stream, err)
	}
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("strategy created authority: %+v", event)
		}
	}
}

func TestHumanGatewayAcceptsWorstCaseEncodedValidStrategy(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := testHumanHandler(t, intake.New(app.New(events.NewGateway(store))))
	criteria := make([]string, 0, 16)
	for index := 0; index < 16; index++ {
		criteria = append(criteria, strings.Repeat("\"", (4<<10)-1)+string(rune('A'+index)))
	}
	body, err := json.Marshal(userStrategyBootstrapRequest{
		RequestID: "strategy-escaped", MissionID: "mission-escaped", MissionStatement: strings.Repeat("<", 16<<10),
		GoalID: "goal-escaped", GoalObjective: strings.Repeat("<", 16<<10), GoalMode: core.GoalTarget,
		SuccessCriteria: criteria,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) <= 256<<10 || len(body) > MaximumStrategyRequestBytes {
		t.Fatalf("encoded valid strategy bytes=%d", len(body))
	}
	response := serveHuman(handler, http.MethodPost, "/v1/user/strategy/bootstrap", testOwnerMarker, string(body))
	if response.Code != http.StatusOK {
		t.Fatalf("encoded valid strategy status=%d", response.Code)
	}
}

func TestHumanGatewayReportsStrategyCapacityAsTerminal(t *testing.T) {
	handler := &Human{}
	response := httptest.NewRecorder()
	handler.writeIntakeError(response, intake.ErrCapacity)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("strategy capacity status=%d body=%s", response.Code, response.Body.String())
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

func TestHumanGatewayBindsSelectedActiveGoalWithoutGrantingAuthority(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	mission := core.Mission{ID: "mission-1", OrganizationID: organization.ID, Statement: "Deliver governed work", Status: core.MissionActive, CreatedAt: now}
	goal := core.Goal{ID: "goal-1", OrganizationID: organization.ID, MissionID: mission.ID, Objective: "Produce verified outcomes", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "Verified", Origin: "RUNTIME_TEST"}}, Status: core.GoalActive, CreatedAt: now}
	for _, projection := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "seed-org"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "seed-mission"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "seed-goal"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
	} {
		if _, err := gateway.PublishProjection(ctx, projection); err != nil {
			t.Fatal(err)
		}
	}
	runtime := app.New(gateway)
	handler := testHumanHandler(t, intake.New(runtime))

	request := humanMessageRequest{ConversationID: "goal-bound", MessageID: "message-1", Text: "echo a verified result", GoalID: goal.ID}
	draftResponse := serveHuman(handler, http.MethodPost, "/v1/user/messages", testOwnerMarker, humanBody(t, request))
	var draft humanTaskResponse
	if draftResponse.Code != http.StatusOK || json.Unmarshal(draftResponse.Body.Bytes(), &draft) != nil || draft.Intent == nil || draft.SelectedGoalID != goal.ID {
		t.Fatalf("goal-bound draft=%d %s", draftResponse.Code, draftResponse.Body.String())
	}
	confirmationBody, err := json.Marshal(humanIntentConfirmationRequest{MessageID: "confirmation-1", Fingerprint: draft.Intent.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	response := serveHuman(handler, http.MethodPost, "/v1/user/intents/"+request.ConversationID+"/confirm", testOwnerMarker, string(confirmationBody))
	if response.Code != http.StatusOK {
		t.Fatalf("goal-bound submit=%d %s", response.Code, response.Body.String())
	}
	snapshot, found, err := runtime.OrganizationState(ctx, organization.ID)
	if err != nil || !found || len(snapshot.Works) != 1 || snapshot.Works[0].GoalID != goal.ID {
		t.Fatalf("goal-bound organization=%+v found=%t err=%v", snapshot, found, err)
	}
	stream := gatewayExternalStream(t, store, request.ConversationID)
	for _, event := range stream {
		if strings.HasPrefix(event.EventType, "APPROVAL_") || strings.HasPrefix(event.EventType, "CAPABILITY_") || strings.HasPrefix(event.EventType, "EFFECT_") {
			t.Fatalf("Goal selection crossed an authority boundary: %+v", event)
		}
	}
}

func TestHumanGatewayAbandonsOnlyOwnedUnconfirmedIntake(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := testHumanHandler(t, intake.New(app.New(events.NewGateway(store))))
	request := humanMessageRequest{ConversationID: "abandon-intake", MessageID: "message-1", Text: "prepare bounded work"}
	draft := serveHuman(handler, http.MethodPost, "/v1/user/messages", testOwnerMarker, humanBody(t, request))
	if draft.Code != http.StatusOK {
		t.Fatalf("draft=%d %s", draft.Code, draft.Body.String())
	}
	body, err := json.Marshal(humanIntentAbandonmentRequest{MessageID: "abandon-1"})
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner := serveHuman(handler, http.MethodPost, "/v1/user/intents/abandon-intake/abandon", "wrong-token", string(body))
	if wrongOwner.Code != http.StatusUnauthorized {
		t.Fatalf("wrong owner abandonment=%d %s", wrongOwner.Code, wrongOwner.Body.String())
	}
	response := serveHuman(handler, http.MethodPost, "/v1/user/intents/abandon-intake/abandon", testOwnerMarker, string(body))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"ABANDONED"`) {
		t.Fatalf("abandonment=%d %s", response.Code, response.Body.String())
	}
	replay := serveHuman(handler, http.MethodPost, "/v1/user/intents/abandon-intake/abandon", testOwnerMarker, string(body))
	if replay.Code != http.StatusOK {
		t.Fatalf("exact abandonment replay=%d %s", replay.Code, replay.Body.String())
	}
	active := serveHuman(handler, http.MethodGet, "/v1/user/intents/active", testOwnerMarker, "")
	if active.Code != http.StatusNotFound {
		t.Fatalf("abandoned intake remained active=%d %s", active.Code, active.Body.String())
	}
	unknown := serveHuman(handler, http.MethodPost, "/v1/user/intents/abandon-intake/abandon", testOwnerMarker, `{"message_id":"abandon-1","authority":"admin"}`)
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("authority-shaped abandonment field=%d %s", unknown.Code, unknown.Body.String())
	}
	correlationID, found, err := store.ResolveExternalWork(t.Context(), "org-1", request.ConversationID)
	if err != nil || !found {
		t.Fatalf("resolve abandoned intake: found=%t err=%v", found, err)
	}
	stream, err := store.Events(t.Context(), correlationID)
	if err != nil {
		t.Fatal(err)
	}
	if countGatewayEvents(stream, "INTAKE_ABANDONED") != 1 || countGatewayEvents(stream, "INTENT_CONFIRMED") != 0 {
		t.Fatalf("abandonment stream=%+v", stream)
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

func countGatewayEvents(stream []events.Event, eventType string) int {
	count := 0
	for _, event := range stream {
		if event.EventType == eventType {
			count++
		}
	}
	return count
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
