package intake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestRouterUsesLeastNondeterministicAvailableMechanism(t *testing.T) {
	router := Router{}
	tests := []struct {
		name    string
		message Message
		want    core.ExecutionKind
		wantErr error
	}{
		{name: "registered deterministic handler", message: Message{Text: "echo hello"}, want: core.ExecutionDeterministic},
		{name: "unstructured natural language", message: Message{Text: "draft a concise release update"}, want: core.ExecutionAgent},
		{name: "explicit human work", message: Message{Text: "choose a launch date", RequestedKind: core.ExecutionHuman}, want: core.ExecutionHuman},
		{name: "unavailable tool routing", message: Message{Text: "call a tool", RequestedKind: core.ExecutionTool}, wantErr: ErrInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := router.Route(test.message)
			if !errors.Is(err, test.wantErr) || got != test.want {
				t.Fatalf("route=%s err=%v want=%s err=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestIntakeKeepsSourceProvenance(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runtime := app.New(events.NewGateway(store))
	service := New(runtime)
	human := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	externalAgent := testPrincipal("external-agent-1", core.PrincipalExternalAgent, ChannelA2A)
	externalAgent.WorkScope = WorkScopeOrganization

	view, err := service.Handle(ctx, human, Message{ConversationID: "human-work", MessageID: "human-message-1", Text: "draft a concise release update"})
	if err != nil || view.State != StateCompleted || view.Result != "fake-model: draft a concise release update" {
		t.Fatalf("human view=%+v err=%v", view, err)
	}
	intent, task, stream := projectedWork(t, store, "human-work")
	if intent.SourcePrincipalID != "human-1" || intent.SourcePrincipalKind != core.PrincipalHuman || intent.SourceChannel != ChannelHumanDirect || intent.SourceMessageID != "human-message-1" || intent.SourceHumanID != "human-1" {
		t.Fatalf("human provenance=%+v", intent)
	}
	if task.ExecutionKind != core.ExecutionAgent || task.ModelInferencePolicy != core.InferenceAllowed {
		t.Fatalf("natural-language route=%+v", task)
	}
	if !containsEvent(stream, "HUMAN_WORK_ACCEPTED") || !containsEvent(stream, "EXECUTION_CONTEXT_MANIFESTED") || !containsEvent(stream, "INFERENCE_USAGE_RECORDED") {
		t.Fatalf("agent route lacks manifested inference evidence: %+v", stream)
	}

	view, err = service.Handle(ctx, externalAgent, Message{ConversationID: "agent-work", MessageID: "agent-message-1", Text: "echo hello"})
	if err != nil || view.State != StateCompleted || view.Result != "hello" {
		t.Fatalf("external-Agent view=%+v err=%v", view, err)
	}
	intent, task, stream = projectedWork(t, store, "agent-work")
	if intent.SourcePrincipalID != "external-agent-1" || intent.SourcePrincipalKind != core.PrincipalExternalAgent || intent.SourceChannel != ChannelA2A || intent.SourceMessageID != "agent-message-1" || intent.SourceHumanID != "" {
		t.Fatalf("external-Agent provenance=%+v", intent)
	}
	if task.ExecutionKind != core.ExecutionDeterministic || !containsEvent(stream, "A2A_WORK_ACCEPTED") || containsEvent(stream, "INFERENCE_USAGE_RECORDED") {
		t.Fatalf("deterministic route used model inference: task=%+v events=%+v", task, stream)
	}

	shared, err := service.Get(ctx, human, view.TaskID)
	if err != nil || shared.Result != "hello" {
		t.Fatalf("authorized human could not observe external-Agent work: view=%+v err=%v", shared, err)
	}
}

func TestChannelsShareWorkNotIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	human := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	externalAgent := testPrincipal("external-agent-1", core.PrincipalExternalAgent, ChannelA2A)
	externalAgent.WorkScope = WorkScopeOrganization

	view, err := service.Handle(ctx, human, Message{ConversationID: "shared", MessageID: "human-message-1", Text: "choose the launch date", RequestedKind: core.ExecutionHuman})
	if err != nil || view.State != StateInputRequired || view.Prompt == "" {
		t.Fatalf("blocked human work=%+v err=%v", view, err)
	}
	taskID := view.TaskID
	view, err = service.Handle(ctx, externalAgent, Message{ConversationID: "shared", TaskID: taskID, MessageID: "agent-message-1", Text: "Use September 15", RequestedKind: core.ExecutionHuman})
	if err != nil || view.State != StateCompleted || view.Result != "authorized external input persisted" {
		t.Fatalf("external-Agent continuation=%+v err=%v", view, err)
	}
	stream := externalStream(t, store, "shared")
	var input events.OperatorInputReceivedPayload
	found := false
	for _, event := range stream {
		if event.EventType != "A2A_INPUT_RECEIVED" {
			continue
		}
		if err := json.Unmarshal(event.Payload, &input); err != nil {
			t.Fatal(err)
		}
		found = true
	}
	if !found || input.SourcePrincipalID != "external-agent-1" || input.SourcePrincipalKind != string(core.PrincipalExternalAgent) || input.SourceChannel != ChannelA2A || input.MessageID != "agent-message-1" {
		t.Fatalf("continuation provenance=%+v found=%t", input, found)
	}
	eventCount := len(stream)
	view, err = service.Handle(ctx, externalAgent, Message{ConversationID: "shared", TaskID: taskID, MessageID: "agent-message-1", Text: "Use September 15"})
	if err != nil || view.State != StateCompleted {
		t.Fatalf("idempotent retry=%+v err=%v", view, err)
	}
	stream = externalStream(t, store, "shared")
	if len(stream) != eventCount {
		t.Fatalf("retry appended events=%d want=%d", len(stream), eventCount)
	}

	noInput := externalAgent
	noInput.Capabilities = []string{CapabilityReadStatus}
	_, err = service.Handle(ctx, noInput, Message{ConversationID: "new-block", MessageID: "message-1", Text: "decision", RequestedKind: core.ExecutionHuman})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("principal without submit capability err=%v", err)
	}
}

func TestConversationIdentifiersAreTenantScopedAndTaskIDsAreOpaque(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	first := testPrincipal("agent-1", core.PrincipalExternalAgent, ChannelA2A)
	second := testPrincipal("agent-2", core.PrincipalExternalAgent, ChannelA2A)
	second.OrganizationID = "org-2"

	firstView, err := service.Handle(context.Background(), first, Message{ConversationID: "shared-public-id", MessageID: "message-1", Text: "echo first"})
	if err != nil {
		t.Fatal(err)
	}
	secondView, err := service.Handle(context.Background(), second, Message{ConversationID: "shared-public-id", MessageID: "message-1", Text: "echo second"})
	if err != nil {
		t.Fatal(err)
	}
	if firstView.TaskID == secondView.TaskID || strings.Contains(firstView.TaskID, "shared-public-id") || strings.Contains(secondView.TaskID, "shared-public-id") {
		t.Fatalf("task identifiers are not opaque and tenant-scoped: first=%q second=%q", firstView.TaskID, secondView.TaskID)
	}
	if _, err := service.Get(context.Background(), first, secondView.TaskID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant task lookup err=%v", err)
	}
}

func TestIntakeBoundsUntrustedIdentifiersAndText(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	principal := testPrincipal("agent-1", core.PrincipalExternalAgent, ChannelA2A)
	for _, message := range []Message{
		{ConversationID: strings.Repeat("c", 257), MessageID: "message", Text: "echo safe"},
		{ConversationID: "conversation\nforged", MessageID: "message", Text: "echo safe"},
		{ConversationID: "conversation", MessageID: strings.Repeat("m", 257), Text: "echo safe"},
		{ConversationID: "conversation", MessageID: "message", Text: strings.Repeat("x", (64<<10)+1)},
	} {
		if _, err := service.Handle(context.Background(), principal, message); !errors.Is(err, ErrInvalid) {
			t.Fatalf("unbounded message err=%v", err)
		}
	}
}

func TestIntakeGrandfathersOnlyDurablyBoundLegacyConversationIDs(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-binding.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	principal := testPrincipal("agent-1", core.PrincipalExternalAgent, ChannelA2A)
	message := Message{ConversationID: "canonical-conversation", MessageID: "message-1", Text: "echo legacy"}
	view, err := service.Handle(ctx, principal, message)
	if err != nil || view.State != StateCompleted {
		t.Fatalf("initial work=%+v err=%v", view, err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE external_work SET request_id = ? WHERE organization_id = ? AND request_id = ?`, "case 123", "org-1", message.ConversationID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	message.ConversationID = "case 123"
	view, err = service.Handle(ctx, principal, message)
	if err != nil || view.State != StateCompleted || view.ConversationID != message.ConversationID {
		t.Fatalf("durably bound legacy conversation=%+v err=%v", view, err)
	}
	message.ConversationID = "case 456"
	if _, err := service.Handle(ctx, principal, message); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbound legacy-shaped conversation err=%v", err)
	}
}

func TestIntakeUsesDurableMessageIDsForContinuationAndReplayAuthorization(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	human := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	externalAgent := testPrincipal("external-agent-1", core.PrincipalExternalAgent, ChannelA2A)
	externalAgent.WorkScope = WorkScopeOrganization

	const repeatedText = "choose the launch date"
	view, err := service.Handle(ctx, human, Message{ConversationID: "same-text", MessageID: "initial-message", Text: repeatedText, RequestedKind: core.ExecutionHuman})
	if err != nil || view.State != StateInputRequired {
		t.Fatalf("blocked work=%+v err=%v", view, err)
	}
	view, err = service.Handle(ctx, externalAgent, Message{ConversationID: "same-text", TaskID: view.TaskID, MessageID: "continuation-message", Text: repeatedText})
	if err != nil || view.State != StateCompleted {
		t.Fatalf("same-text continuation=%+v err=%v", view, err)
	}

	submitOnly := human
	submitOnly.Capabilities = []string{CapabilitySubmitWork}
	message := Message{ConversationID: "read-guard", MessageID: "submission", Text: "echo private status"}
	receipt, err := service.Handle(ctx, submitOnly, message)
	if err != nil || receipt.TaskID == "" || receipt.State != StateWorking || receipt.Result != "" || receipt.Prompt != "" {
		t.Fatalf("initial submission failed: %v", err)
	}
	retry, err := service.Handle(ctx, submitOnly, message)
	if err != nil || retry != receipt {
		t.Fatalf("submission receipt retry=%+v want=%+v err=%v", retry, receipt, err)
	}
	otherSubmitter := submitOnly
	otherSubmitter.ID = "human-2"
	if _, err := service.Handle(ctx, otherSubmitter, message); !errors.Is(err, ErrForbidden) {
		t.Fatalf("different submitter replay err=%v", err)
	}
}

func testPrincipal(id string, kind core.PrincipalKind, channel string) Principal {
	scope := WorkScopeOwn
	if kind == core.PrincipalHuman {
		scope = WorkScopeOrganization
	}
	return Principal{
		ID: id, Kind: kind, OrganizationID: "org-1", Channel: channel,
		Capabilities: []string{CapabilitySubmitWork, CapabilityReadStatus, CapabilityReadResult, CapabilityProvideInput}, WorkScope: scope,
	}
}

func projectedWork(t *testing.T, store *ledger.SQLite, correlationID string) (core.Intent, core.Task, []events.Event) {
	t.Helper()
	stream := externalStream(t, store, correlationID)
	var intent core.Intent
	var task core.Task
	for _, event := range stream {
		var payload events.ProjectionEventPayload
		if json.Unmarshal(event.Payload, &payload) != nil {
			continue
		}
		switch event.EventType {
		case "INTENT_CREATED":
			if err := json.Unmarshal(payload.Projection.Value, &intent); err != nil {
				t.Fatal(err)
			}
		case "TASK_CREATED":
			if err := json.Unmarshal(payload.Projection.Value, &task); err != nil {
				t.Fatal(err)
			}
		}
	}
	return intent, task, stream
}

func externalStream(t *testing.T, store *ledger.SQLite, externalRequestID string) []events.Event {
	t.Helper()
	all, err := store.Events(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	correlationID := ""
	for _, event := range all {
		if event.EventType != "INTENT_CREATED" {
			continue
		}
		var payload events.ProjectionEventPayload
		var intent core.Intent
		if json.Unmarshal(event.Payload, &payload) == nil && json.Unmarshal(payload.Projection.Value, &intent) == nil && intent.ExternalRequestID == externalRequestID {
			correlationID = event.CorrelationID
			break
		}
	}
	if correlationID == "" {
		return nil
	}
	stream, err := store.Events(context.Background(), correlationID)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func containsEvent(stream []events.Event, eventType string) bool {
	for _, event := range stream {
		if event.EventType == eventType {
			return true
		}
	}
	return false
}
