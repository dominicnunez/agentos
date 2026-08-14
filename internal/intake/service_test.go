package intake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestExternalViewProjectsOnlyRuntimeRootTask(t *testing.T) {
	rootID := "task-root"
	childID := "task-root-child"
	started := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	stream := []events.Event{
		{EventType: "INTAKE_MESSAGE_RECORDED", TaskID: rootID, CreatedAt: started},
		taskProjectionEvent(t, "TASK_CREATED", core.Task{ID: core.ID(rootID), Status: core.TaskPending}, started.Add(time.Second)),
		taskProjectionEvent(t, "TASK_CREATED", core.Task{ID: core.ID(childID), ParentID: core.ID(rootID), Status: core.TaskPending}, started.Add(2*time.Second)),
		taskProjectionEvent(t, "TASK_BLOCKED", core.Task{ID: core.ID(childID), ParentID: core.ID(rootID), Status: core.TaskBlocked}, started.Add(3*time.Second)),
		{EventType: "RESULT_PUBLISHED", TaskID: childID, Payload: json.RawMessage(`{"summary":"internal result"}`), CreatedAt: started.Add(4 * time.Second)},
	}
	view := projectView("work-1", stream, true)
	if view.TaskID != rootID || view.State != StateWorking || view.Result != "" || !view.UpdatedAt.Equal(started.Add(time.Second)) {
		t.Fatalf("child state leaked through root view: %+v", view)
	}
	stream = append(stream,
		events.Event{EventType: "RESULT_PUBLISHED", TaskID: rootID, Payload: json.RawMessage(`{"summary":"public root result"}`), CreatedAt: started.Add(5 * time.Second)},
		taskProjectionEvent(t, "TASK_VERIFIED_COMPLETE", core.Task{ID: core.ID(rootID), Status: core.TaskCompleted}, started.Add(6*time.Second)),
	)
	view = projectView("work-1", stream, true)
	if view.State != StateCompleted || view.Result != "public root result" || !view.UpdatedAt.Equal(started.Add(6*time.Second)) {
		t.Fatalf("root result projection=%+v", view)
	}
}

func TestExternalViewProjectsRootDependencyFailure(t *testing.T) {
	started := time.Now().UTC()
	rootID := "task-work-1"
	stream := []events.Event{
		{EventType: "INTAKE_MESSAGE_RECORDED", TaskID: rootID, CreatedAt: started},
		taskProjectionEvent(t, "TASK_CREATED", core.Task{ID: core.ID(rootID), Status: core.TaskPending}, started.Add(time.Second)),
		taskProjectionEvent(t, "TASK_DEPENDENCY_FAILED", core.Task{ID: core.ID(rootID), Status: core.TaskFailed}, started.Add(2*time.Second)),
	}
	view := projectView("work-1", stream, true)
	if view.State != StateFailed || !view.UpdatedAt.Equal(started.Add(2*time.Second)) {
		t.Fatalf("dependency failure view=%+v", view)
	}
}

func TestExternalViewProjectsPlanningAndRemediationFailures(t *testing.T) {
	started := time.Now().UTC()
	rootID := "task-work-1"
	base := []events.Event{{EventType: "INTAKE_MESSAGE_RECORDED", TaskID: rootID, CreatedAt: started}}

	planning := append([]events.Event(nil), base...)
	planning = append(planning, events.Event{EventType: "WORK_PLANNING_FAILED", CreatedAt: started.Add(time.Second)})
	view := projectView("work-1", planning, true)
	if view.State != StateFailed || !view.UpdatedAt.Equal(started.Add(time.Second)) {
		t.Fatalf("planning failure view=%+v", view)
	}

	remediation := append([]events.Event(nil), base...)
	remediation = append(remediation,
		taskProjectionEvent(t, "TASK_CREATED", core.Task{ID: core.ID(rootID), Status: core.TaskPending}, started.Add(time.Second)),
		taskProjectionEvent(t, "TASK_REMEDIATION_FAILED", core.Task{ID: core.ID(rootID), Status: core.TaskFailed}, started.Add(2*time.Second)),
	)
	view = projectView("work-1", remediation, true)
	if view.State != StateFailed || !view.UpdatedAt.Equal(started.Add(2*time.Second)) {
		t.Fatalf("remediation failure view=%+v", view)
	}
}

func taskProjectionEvent(t *testing.T, eventType string, task core.Task, at time.Time) events.Event {
	t.Helper()
	value, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(events.ProjectionEventPayload{Projection: events.ProjectionRecord{
		ProjectionKind: "task", RecordID: string(task.ID), Version: 1, CorrelationID: "work-1", Value: value,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return events.Event{EventType: eventType, TaskID: string(task.ID), Payload: payload, CreatedAt: at}
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

	view, err := submitAndConfirm(t, ctx, service, human, Message{ConversationID: "human-work", MessageID: "human-message-1", Text: "draft a concise release update"})
	if err != nil || view.State != StateCompleted || !strings.HasPrefix(view.Result, "fake-model: Operate only as this runtime-selected durable Agent blueprint.") || !strings.Contains(view.Result, `"objective":"draft a concise release update"`) {
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

	view, err = submitAndConfirm(t, ctx, service, externalAgent, Message{ConversationID: "agent-work", MessageID: "agent-message-1", Text: "echo hello"})
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

func TestChannelsShareWorkButAgentCannotCompleteUserTask(t *testing.T) {
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

	view, err := submitAndConfirm(t, ctx, service, human, Message{ConversationID: "shared", MessageID: "human-message-1", Text: "choose the launch date", RequestedKind: core.ExecutionHuman})
	if err != nil || view.State != StateInputRequired || view.Prompt == "" {
		t.Fatalf("blocked human work=%+v err=%v", view, err)
	}
	taskID := view.TaskID
	view, err = service.Handle(ctx, externalAgent, Message{ConversationID: "shared", TaskID: taskID, MessageID: "agent-message-1", Text: "Use September 15", RequestedKind: core.ExecutionHuman})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("external Agent completed a structured user task: view=%+v err=%v", view, err)
	}
	stream := externalStream(t, store, "shared")
	for _, event := range stream {
		if event.EventType == "A2A_INPUT_RECEIVED" {
			t.Fatalf("rejected Agent continuation reached the ledger: %+v", event)
		}
	}
	shared, err := service.Get(ctx, externalAgent, taskID)
	if err != nil || shared.State != StateInputRequired {
		t.Fatalf("authorized Agent could not observe blocked shared work: view=%+v err=%v", shared, err)
	}
	completed, err := service.CompleteHumanTask(ctx, human, taskID, core.HumanTaskSubmission{
		MessageID: "human-completion-1",
		Fields:    map[string]string{"response": "Use September 15"},
	})
	if err != nil || completed.State != StateCompleted || completed.Result != "structured user completion persisted" {
		t.Fatalf("structured user completion did not finish its durable Work: view=%+v err=%v", completed, err)
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
	view, err := submitAndConfirm(t, ctx, service, principal, message)
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
	view, err := submitAndConfirm(t, ctx, service, human, Message{ConversationID: "same-text", MessageID: "initial-message", Text: repeatedText, RequestedKind: core.ExecutionHuman})
	if err != nil || view.State != StateInputRequired {
		t.Fatalf("blocked work=%+v err=%v", view, err)
	}
	if _, err := service.Handle(ctx, human, Message{ConversationID: "same-text", MessageID: "initial-message", Text: repeatedText, RequestedKind: core.ExecutionAgent}); !errors.Is(err, ErrConflict) {
		t.Fatalf("confirmed initial replay changed its requested execution kind: %v", err)
	}
	view, err = service.Handle(ctx, externalAgent, Message{ConversationID: "same-text", TaskID: view.TaskID, MessageID: "continuation-message", Text: repeatedText})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("same-text Agent continuation completed a user task: view=%+v err=%v", view, err)
	}

	submitOnly := human
	submitOnly.Capabilities = []string{CapabilitySubmitWork}
	message := Message{ConversationID: "read-guard", MessageID: "submission", Text: "echo private status"}
	receipt, err := service.Handle(ctx, submitOnly, message)
	if err != nil || receipt.TaskID == "" || receipt.State != StateAwaitingConfirmation || receipt.Result != "" || receipt.Prompt == "" || receipt.Intent == nil {
		t.Fatalf("initial submission failed: %v", err)
	}
	retry, err := service.Handle(ctx, submitOnly, message)
	if err != nil || retry.TaskID != receipt.TaskID || retry.State != receipt.State || retry.Intent == nil || retry.Intent.Fingerprint != receipt.Intent.Fingerprint {
		t.Fatalf("submission receipt retry=%+v want=%+v err=%v", retry, receipt, err)
	}
	if _, err := service.ConfirmIntent(ctx, submitOnly, IntentConfirmation{ConversationID: message.ConversationID, MessageID: "confirm-submission", Fingerprint: receipt.Intent.Fingerprint}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("submit-only principal confirmed intent: %v", err)
	}
	otherSubmitter := submitOnly
	otherSubmitter.ID = "human-2"
	if _, err := service.Handle(ctx, otherSubmitter, message); !errors.Is(err, ErrForbidden) {
		t.Fatalf("different submitter replay err=%v", err)
	}
}

func TestUnconfirmedIntentResumesAfterServiceRestart(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	first := New(app.New(events.NewGateway(store)))
	draft, err := first.Handle(ctx, principal, Message{ConversationID: "resumable", MessageID: "message-1", Text: "prepare a Linux release"})
	if err != nil || draft.State != StateAwaitingConfirmation || draft.Intent == nil {
		t.Fatalf("draft=%+v err=%v", draft, err)
	}
	restarted := New(app.New(events.NewGateway(store)))
	resumed, err := restarted.ActiveIntent(ctx, principal)
	if err != nil || resumed.ConversationID != draft.ConversationID || resumed.TaskID != draft.TaskID || resumed.Intent == nil || resumed.Intent.Fingerprint != draft.Intent.Fingerprint {
		t.Fatalf("resumed=%+v err=%v", resumed, err)
	}
	confirmed, err := restarted.ConfirmIntent(ctx, principal, IntentConfirmation{ConversationID: resumed.ConversationID, TaskID: resumed.TaskID, MessageID: "confirmation-1", Fingerprint: resumed.Intent.Fingerprint})
	if err != nil || confirmed.TaskID != draft.TaskID {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
	if _, err := restarted.ActiveIntent(ctx, principal); !errors.Is(err, ErrNotFound) {
		t.Fatalf("confirmed intake remained active: %v", err)
	}
}

func TestOwnScopeBindsCompleteAuthenticatedPrincipalIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	user := testPrincipal("shared-id", core.PrincipalHuman, ChannelHumanDirect)
	draft, err := service.Handle(ctx, user, Message{ConversationID: "identity-bound", MessageID: "message-1", Text: "echo private"})
	if err != nil || draft.Intent == nil {
		t.Fatalf("user draft=%+v err=%v", draft, err)
	}

	agent := testPrincipal("shared-id", core.PrincipalExternalAgent, ChannelA2A)
	if _, err := service.ActiveIntent(ctx, agent); !errors.Is(err, ErrNotFound) {
		t.Fatalf("same-id agent resumed user intake: %v", err)
	}
	if _, err := service.Handle(ctx, agent, Message{ConversationID: draft.ConversationID, TaskID: draft.TaskID, MessageID: "message-2", Text: "continue"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("same-id agent continued user intake: %v", err)
	}
	if _, err := service.ConfirmIntent(ctx, agent, IntentConfirmation{ConversationID: draft.ConversationID, TaskID: draft.TaskID, MessageID: "confirmation-agent", Fingerprint: draft.Intent.Fingerprint}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("same-id agent confirmed user intent: %v", err)
	}

	confirmed, err := service.ConfirmIntent(ctx, user, IntentConfirmation{ConversationID: draft.ConversationID, MessageID: "confirmation-user", Fingerprint: draft.Intent.Fingerprint})
	if err != nil || confirmed.TaskID == "" {
		t.Fatalf("user confirmation=%+v err=%v", confirmed, err)
	}
	if _, err := service.Get(ctx, agent, confirmed.TaskID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("same-id agent read user task: %v", err)
	}
}

func TestIntentNormalizationManifestsModelUseAndReplaysWithoutInference(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ready := `{"state":"READY_FOR_REVIEW","reply":"Review this intent.","intent":{"objective":"Prepare a Linux release","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Binary passes verification","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	normalizer, err := NewModelNormalizer(normalizationModel{response: ready})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithNormalizer(app.New(events.NewGateway(store)), normalizer)
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	message := Message{ConversationID: "audited-intake", MessageID: "message-1", Text: "Prepare a Linux release"}

	first, err := service.Handle(ctx, principal, message)
	if err != nil || first.State != StateAwaitingConfirmation {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	stream := externalStream(t, store, message.ConversationID)
	if countEvents(stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") != 1 || countEvents(stream, "INFERENCE_USAGE_RECORDED") != 1 || countEvents(stream, "INTENT_DRAFTED") != 1 {
		t.Fatalf("normalization audit events=%+v", stream)
	}
	var contextPayload events.IntentNormalizationContextPayload
	for _, event := range stream {
		if event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" {
			if err := json.Unmarshal(event.Payload, &contextPayload); err != nil {
				t.Fatal(err)
			}
			if event.SourceExecutionID == "" || contextPayload.SourceMessageID != message.MessageID || contextPayload.PromptVersion != intentNormalizationPromptVersion || len(contextPayload.InputEventRefs) != 1 {
				t.Fatalf("normalization context event=%+v payload=%+v", event, contextPayload)
			}
		}
	}
	replayed, err := service.Handle(ctx, principal, message)
	if err != nil || replayed.Intent == nil || replayed.Intent.Fingerprint != first.Intent.Fingerprint {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	changedRoute := message
	changedRoute.RequestedKind = core.ExecutionHuman
	if _, err := service.Handle(ctx, principal, changedRoute); !errors.Is(err, ErrConflict) {
		t.Fatalf("same message id changed its requested execution kind: %v", err)
	}
	stream = externalStream(t, store, message.ConversationID)
	if countEvents(stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") != 1 || countEvents(stream, "INFERENCE_USAGE_RECORDED") != 1 || countEvents(stream, "INTENT_DRAFTED") != 1 {
		t.Fatalf("replay repeated model work: %+v", stream)
	}
	confirmed, err := service.ConfirmIntent(ctx, principal, IntentConfirmation{ConversationID: message.ConversationID, MessageID: "confirmation-1", Fingerprint: first.Intent.Fingerprint})
	if err != nil || confirmed.TaskID == "" {
		t.Fatalf("confirmation=%+v err=%v", confirmed, err)
	}
	_, task, _ := projectedWork(t, store, message.ConversationID)
	if len(task.AcceptanceCriteria) != 1 || task.AcceptanceCriteria[0].Value != "Binary passes verification" {
		t.Fatalf("accepted completion criteria did not reach task contract input: %+v", task.AcceptanceCriteria)
	}
}

type retryNormalizationModel struct {
	response string
	calls    int
}

func (*retryNormalizationModel) Descriptor() NormalizerDescriptor {
	return NormalizerDescriptor{Provider: "test", Model: "test-model", ExecutionProfileVersion: "test-profile"}
}

func (m *retryNormalizationModel) CompleteText(context.Context, string) (TextCompletion, error) {
	m.calls++
	if m.calls == 1 {
		return TextCompletion{}, errors.New("temporary provider failure")
	}
	return TextCompletion{Text: m.response, Usage: events.InferenceUsageRecordedPayload{Source: "test", Provider: "test", Model: "test-model"}}, nil
}

func TestIntentNormalizationRetryCompletesAnInterruptedDraftOnce(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ready := `{"state":"READY_FOR_REVIEW","reply":"Review this intent.","intent":{"objective":"Prepare a Linux release","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Binary passes verification","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	model := &retryNormalizationModel{response: ready}
	normalizer, err := NewModelNormalizer(model)
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithNormalizer(app.New(events.NewGateway(store)), normalizer)
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	message := Message{ConversationID: "retry-intake", MessageID: "message-1", Text: "Prepare a Linux release"}

	if _, err := service.Handle(ctx, principal, message); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("first normalization err=%v", err)
	}
	view, err := service.Handle(ctx, principal, message)
	if err != nil || view.State != StateAwaitingConfirmation || model.calls != 2 {
		t.Fatalf("retried view=%+v calls=%d err=%v", view, model.calls, err)
	}
	stream := externalStream(t, store, message.ConversationID)
	if countEvents(stream, "INTAKE_MESSAGE_RECORDED") != 1 || countEvents(stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") != 2 || countEvents(stream, "INFERENCE_USAGE_RECORDED") != 1 || countEvents(stream, "INTENT_DRAFTED") != 1 {
		t.Fatalf("interrupted retry did not preserve distinct attempts: %+v", stream)
	}
}

func TestInvalidNormalizationStillRecordsProviderUsage(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	normalizer, err := NewModelNormalizer(normalizationModel{response: `{"not":"the contract"}`})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithNormalizer(app.New(events.NewGateway(store)), normalizer)
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	message := Message{ConversationID: "invalid-normalization", MessageID: "message-1", Text: "Prepare a release"}
	if _, err := service.Handle(ctx, principal, message); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("invalid normalization err=%v", err)
	}
	stream := externalStream(t, store, message.ConversationID)
	if countEvents(stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") != 1 || countEvents(stream, "INFERENCE_USAGE_RECORDED") != 1 || countEvents(stream, "INTENT_DRAFTED") != 0 {
		t.Fatalf("invalid normalization audit events=%+v", stream)
	}
}

func TestIntentConversationLimitsRejectBeforeAppending(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := New(app.New(events.NewGateway(store)))
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	conversationID := "bounded-intake"
	var taskID string
	for index := 0; index < MaximumIntentTurns; index++ {
		view, handleErr := service.Handle(ctx, principal, Message{ConversationID: conversationID, TaskID: taskID, MessageID: fmt.Sprintf("message-%d", index), Text: "Add context"})
		if handleErr != nil {
			t.Fatalf("turn %d: %v", index, handleErr)
		}
		taskID = view.TaskID
	}
	if _, err := service.Handle(ctx, principal, Message{ConversationID: conversationID, TaskID: taskID, MessageID: "message-overflow", Text: "One more detail"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("turn overflow err=%v", err)
	}
	stream := externalStream(t, store, conversationID)
	if countEvents(stream, "INTAKE_MESSAGE_RECORDED") != MaximumIntentTurns {
		t.Fatalf("rejected turn reached ledger: %d", countEvents(stream, "INTAKE_MESSAGE_RECORDED"))
	}
}

type fixedNormalizer struct{}

func (fixedNormalizer) Descriptor() (NormalizerDescriptor, bool) {
	return NormalizerDescriptor{}, false
}

type goalNormalizer struct{ goalID string }

func (goalNormalizer) Descriptor() (NormalizerDescriptor, bool) {
	return NormalizerDescriptor{}, false
}

func (n goalNormalizer) Normalize(_ context.Context, turns []ConversationTurn) (Normalization, error) {
	latest := turns[len(turns)-1]
	value := core.IntentValue{Value: latest.Text, Origin: "EXPLICIT", SourceMessageID: latest.MessageID}
	goal := core.IntentValue{Value: n.goalID, Origin: "EXPLICIT", SourceMessageID: latest.MessageID}
	return Normalization{
		State: normalizationReady, Reply: "Review this Goal-bound intent.",
		Candidate: IntentCandidate{Goal: &goal, Objective: "Advance " + n.goalID, Deliverables: []core.IntentValue{value}, CompletionCriteria: []core.IntentValue{value}},
	}, nil
}

func TestInvalidGoalCannotStrandIntentConfirmation(t *testing.T) {
	tests := map[string]func(*testing.T, context.Context, *events.Gateway){
		"missing": func(*testing.T, context.Context, *events.Gateway) {},
		"paused": func(t *testing.T, ctx context.Context, gateway *events.Gateway) {
			seedIntakeGoal(t, ctx, gateway, "org-1", "goal-invalid", core.GoalPaused)
		},
		"cross organization": func(t *testing.T, ctx context.Context, gateway *events.Gateway) {
			seedIntakeGoal(t, ctx, gateway, "org-2", "goal-invalid", core.GoalActive)
		},
	}
	for name, seed := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			gateway := events.NewGateway(store)
			seed(t, ctx, gateway)
			service := NewWithNormalizer(app.New(gateway), goalNormalizer{goalID: "goal-invalid"})
			principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
			draft, err := service.Handle(ctx, principal, Message{ConversationID: "invalid-goal", MessageID: "message-1", Text: "Use goal-invalid for this work"})
			if err != nil || draft.Intent == nil {
				t.Fatalf("draft=%+v err=%v", draft, err)
			}
			if _, err := service.ConfirmIntent(ctx, principal, IntentConfirmation{ConversationID: "invalid-goal", MessageID: "confirm-1", Fingerprint: draft.Intent.Fingerprint}); !errors.Is(err, ErrConflict) {
				t.Fatalf("invalid Goal confirmation err=%v", err)
			}
			stream := externalStream(t, store, "invalid-goal")
			if containsEvent(stream, "INTENT_CONFIRMED") {
				t.Fatal("invalid Goal was persisted as a confirmed Intent")
			}
			continued, err := service.Handle(ctx, principal, Message{ConversationID: "invalid-goal", MessageID: "message-2", Text: "Remove goal-invalid from the request"})
			if err != nil || continued.Intent == nil {
				t.Fatalf("unconfirmed conversation could not continue: view=%+v err=%v", continued, err)
			}
		})
	}
}

func TestGoalBoundIntentPreservesSubmissionAndConfirmationMessageIdentities(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	seedIntakeGoal(t, ctx, gateway, "org-1", "goal-1", core.GoalActive)
	service := NewWithNormalizer(app.New(gateway), goalNormalizer{goalID: "goal-1"})
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	draft, err := service.Handle(ctx, principal, Message{ConversationID: "goal-work", MessageID: "message-1", Text: "Use goal-1 for this work"})
	if err != nil || draft.Intent == nil {
		t.Fatalf("draft=%+v err=%v", draft, err)
	}
	confirmed, err := service.ConfirmIntent(ctx, principal, IntentConfirmation{ConversationID: "goal-work", MessageID: "confirmation-1", Fingerprint: draft.Intent.Fingerprint})
	if err != nil || confirmed.TaskID == "" {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
	intent, _, stream := projectedWork(t, store, "goal-work")
	if intent.GoalID != "goal-1" || intent.SourceMessageID != "message-1" {
		t.Fatalf("accepted Intent lost its Goal or submission identity: %+v", intent)
	}
	var confirmation events.IntentConfirmedPayload
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" {
			if err := json.Unmarshal(event.Payload, &confirmation); err != nil {
				t.Fatal(err)
			}
		}
	}
	if confirmation.MessageID != "confirmation-1" || confirmation.MessageID == intent.SourceMessageID {
		t.Fatalf("submission and confirmation identities were conflated: intent=%+v confirmation=%+v", intent, confirmation)
	}
}

func TestGoalBoundIntentConcurrentConfirmationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	seedIntakeGoal(t, ctx, gateway, "org-1", "goal-1", core.GoalActive)
	service := NewWithNormalizer(app.New(gateway), goalNormalizer{goalID: "goal-1"})
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	draft, err := service.Handle(ctx, principal, Message{ConversationID: "goal-race", MessageID: "message-1", Text: "Use goal-1 for this work"})
	if err != nil || draft.Intent == nil {
		t.Fatalf("draft=%+v err=%v", draft, err)
	}
	confirmation := IntentConfirmation{ConversationID: "goal-race", MessageID: "confirmation-1", Fingerprint: draft.Intent.Fingerprint}
	start := make(chan struct{})
	results := make(chan View, 2)
	errorsOut := make(chan error, 2)
	var callers sync.WaitGroup
	for range 2 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			result, err := service.ConfirmIntent(ctx, principal, confirmation)
			results <- result
			errorsOut <- err
		}()
	}
	close(start)
	callers.Wait()
	close(results)
	close(errorsOut)
	var taskID string
	for err := range errorsOut {
		if err != nil {
			t.Fatalf("concurrent confirmation failed: %v", err)
		}
	}
	for result := range results {
		if taskID == "" {
			taskID = result.TaskID
		}
		if result.TaskID == "" || result.TaskID != taskID {
			t.Fatalf("concurrent confirmation diverged: first=%s result=%+v", taskID, result)
		}
	}
	_, _, stream := projectedWork(t, store, "goal-race")
	confirmations := 0
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" {
			confirmations++
		}
	}
	if confirmations != 1 {
		t.Fatalf("concurrent replay persisted %d intent confirmations", confirmations)
	}
	confirmation.MessageID = "confirmation-2"
	if _, err := service.ConfirmIntent(ctx, principal, confirmation); err == nil {
		t.Fatal("conflicting confirmation identity was accepted")
	}
}

func seedIntakeGoal(t *testing.T, ctx context.Context, gateway *events.Gateway, organizationID, goalID core.ID, status core.GoalStatus) {
	t.Helper()
	now := time.Now().UTC()
	organization := core.Organization{ID: organizationID, Name: string(organizationID), PolicyVersion: "v1", CreatedAt: now}
	if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: string(organizationID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "seed-" + string(organizationID)}, ProjectionKind: "organization", RecordID: string(organizationID), Version: 1, Value: organization}); err != nil {
		t.Fatal(err)
	}
	mission := core.Mission{ID: core.ID("mission-" + string(organizationID)), OrganizationID: organizationID, Statement: "test direction", Status: core.MissionActive, CreatedAt: now}
	if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: string(organizationID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "seed-" + string(mission.ID)}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission}); err != nil {
		t.Fatal(err)
	}
	goal := core.Goal{ID: goalID, OrganizationID: organizationID, MissionID: mission.ID, Objective: "test outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified", Origin: "RUNTIME_TEST"}}, Status: core.GoalActive, CreatedAt: now}
	if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: string(organizationID), EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "seed-" + string(goalID)}, ProjectionKind: "goal", RecordID: string(goalID), Version: 1, Value: goal}); err != nil {
		t.Fatal(err)
	}
	if status != core.GoalActive {
		goal.Status = status
		if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: string(organizationID), EventType: "GOAL_PAUSED", SourceActorID: "runtime", CorrelationID: "seed-" + string(goalID)}, ProjectionKind: "goal", RecordID: string(goalID), Version: 2, Value: goal}); err != nil {
			t.Fatal(err)
		}
	}
}

func (fixedNormalizer) Normalize(_ context.Context, turns []ConversationTurn) (Normalization, error) {
	latest := turns[len(turns)-1]
	value := core.IntentValue{Value: "Bounded result", Origin: "DEFAULT", SourceMessageID: latest.MessageID}
	return Normalization{
		State: normalizationReady, Reply: "Review this intent.",
		Candidate: IntentCandidate{Objective: "Retain the supplied context", Deliverables: []core.IntentValue{value}, CompletionCriteria: []core.IntentValue{value}},
	}, nil
}

func TestIntentConversationByteLimitRejectsBeforeAppending(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := NewWithNormalizer(app.New(events.NewGateway(store)), fixedNormalizer{})
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	conversationID := "byte-bounded-intake"
	first, err := service.Handle(ctx, principal, Message{ConversationID: conversationID, MessageID: "message-1", Text: strings.Repeat("a", 64<<10)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Handle(ctx, principal, Message{ConversationID: conversationID, TaskID: first.TaskID, MessageID: "message-2", Text: strings.Repeat("b", 64<<10)}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Handle(ctx, principal, Message{ConversationID: conversationID, TaskID: first.TaskID, MessageID: "message-overflow", Text: "c"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("byte overflow err=%v", err)
	}
	stream := externalStream(t, store, conversationID)
	if countEvents(stream, "INTAKE_MESSAGE_RECORDED") != 2 {
		t.Fatalf("rejected bytes reached ledger: %d", countEvents(stream, "INTAKE_MESSAGE_RECORDED"))
	}
}

type clarificationRoutingNormalizer struct{}

func (clarificationRoutingNormalizer) Descriptor() (NormalizerDescriptor, bool) {
	return NormalizerDescriptor{}, false
}

func (clarificationRoutingNormalizer) Normalize(_ context.Context, turns []ConversationTurn) (Normalization, error) {
	latest := turns[len(turns)-1]
	value := core.IntentValue{Value: latest.Text, Origin: "EXPLICIT", SourceMessageID: latest.MessageID}
	if len(turns) == 1 {
		return Normalization{
			State: normalizationNeedsInput, Reply: "Provide the final objective.",
			Candidate: IntentCandidate{Objective: "echo provisional", MissingUserInputs: []core.IntentValue{{Value: "final objective", Origin: "DEFAULT"}}},
		}, nil
	}
	return Normalization{
		State: normalizationReady, Reply: "Review this intent.",
		Candidate: IntentCandidate{Objective: latest.Text, Deliverables: []core.IntentValue{value}, CompletionCriteria: []core.IntentValue{value}},
	}, nil
}

func TestClarificationReroutesOnlyInferredExecutionKind(t *testing.T) {
	tests := []struct {
		name      string
		explicit  core.ExecutionKind
		objective string
		want      core.ExecutionKind
	}{
		{name: "inferred route follows revised objective", objective: "draft the final report", want: core.ExecutionAgent},
		{name: "explicit route remains operator selected", explicit: core.ExecutionHuman, objective: "echo final answer", want: core.ExecutionHuman},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			service := NewWithNormalizer(app.New(events.NewGateway(store)), clarificationRoutingNormalizer{})
			principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
			first, err := service.Handle(ctx, principal, Message{ConversationID: "reroute-kind", MessageID: "message-1", Text: "provisional request", RequestedKind: test.explicit})
			if err != nil || first.State != StateInputRequired {
				t.Fatalf("first=%+v err=%v", first, err)
			}
			second, err := service.Handle(ctx, principal, Message{ConversationID: "reroute-kind", TaskID: first.TaskID, MessageID: "message-2", Text: test.objective})
			if err != nil || second.State != StateAwaitingConfirmation || second.Intent == nil || second.Intent.RequestedExecutionKind != test.want {
				t.Fatalf("second=%+v err=%v want kind=%s", second, err, test.want)
			}
		})
	}
}

type stagedNormalizer struct {
	calls         int
	secondStarted chan struct{}
	releaseSecond chan struct{}
}

func (*stagedNormalizer) Descriptor() (NormalizerDescriptor, bool) {
	return NormalizerDescriptor{}, false
}

func (n *stagedNormalizer) Normalize(_ context.Context, turns []ConversationTurn) (Normalization, error) {
	n.calls++
	if n.calls == 2 {
		close(n.secondStarted)
		<-n.releaseSecond
	}
	latest := turns[len(turns)-1]
	value := core.IntentValue{Value: "Deliver " + latest.MessageID, Origin: "EXPLICIT", SourceMessageID: latest.MessageID}
	return Normalization{
		State: normalizationReady, Reply: "Review this intent.",
		Candidate: IntentCandidate{Objective: "Objective " + latest.MessageID, Deliverables: []core.IntentValue{value}, CompletionCriteria: []core.IntentValue{value}},
	}, nil
}

func TestIntentConfirmationCannotRacePastANewerMessage(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	normalizer := &stagedNormalizer{secondStarted: make(chan struct{}), releaseSecond: make(chan struct{})}
	service := NewWithNormalizer(app.New(events.NewGateway(store)), normalizer)
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	first, err := service.Handle(ctx, principal, Message{ConversationID: "serialized-intake", MessageID: "message-1", Text: "First request"})
	if err != nil || first.Intent == nil {
		t.Fatalf("first=%+v err=%v", first, err)
	}

	continued := make(chan struct {
		view View
		err  error
	}, 1)
	go func() {
		view, handleErr := service.Handle(ctx, principal, Message{ConversationID: "serialized-intake", MessageID: "message-2", Text: "Important correction"})
		continued <- struct {
			view View
			err  error
		}{view: view, err: handleErr}
	}()
	<-normalizer.secondStarted

	confirmed := make(chan error, 1)
	go func() {
		_, confirmErr := service.ConfirmIntent(ctx, principal, IntentConfirmation{ConversationID: "serialized-intake", MessageID: "confirm-1", Fingerprint: first.Intent.Fingerprint})
		confirmed <- confirmErr
	}()
	select {
	case confirmErr := <-confirmed:
		t.Fatalf("stale confirmation crossed active normalization: %v", confirmErr)
	case <-time.After(50 * time.Millisecond):
	}
	close(normalizer.releaseSecond)
	result := <-continued
	if result.err != nil || result.view.Intent == nil || result.view.Intent.Fingerprint == first.Intent.Fingerprint {
		t.Fatalf("continued=%+v err=%v", result.view, result.err)
	}
	if confirmErr := <-confirmed; !errors.Is(confirmErr, ErrConflict) {
		t.Fatalf("stale confirmation err=%v", confirmErr)
	}
}

func testPrincipal(id string, kind core.PrincipalKind, channel string) Principal {
	scope := WorkScopeOwn
	if kind == core.PrincipalHuman {
		scope = WorkScopeOrganization
	}
	return Principal{
		ID: id, Kind: kind, OrganizationID: "org-1", Channel: channel,
		Capabilities: []string{CapabilitySubmitWork, CapabilityConfirmIntent, CapabilityReadStatus, CapabilityReadResult, CapabilityProvideInput}, WorkScope: scope,
	}
}

func submitAndConfirm(t *testing.T, ctx context.Context, service *Service, principal Principal, message Message) (View, error) {
	t.Helper()
	draft, err := service.Handle(ctx, principal, message)
	if err != nil {
		return draft, err
	}
	if draft.State != StateAwaitingConfirmation || draft.Intent == nil {
		t.Fatalf("intent was not ready for confirmation: %+v", draft)
	}
	return service.ConfirmIntent(ctx, principal, IntentConfirmation{ConversationID: message.ConversationID, MessageID: "confirm-" + message.MessageID, Fingerprint: draft.Intent.Fingerprint})
}

func TestDurableInputReplayRejectsNonCanonicalContract(t *testing.T) {
	principal := testPrincipal("external-agent", core.PrincipalExternalAgent, ChannelA2A)
	message := Message{MessageID: "message-1", Text: "bounded continuation"}
	payload := events.OperatorInputReceivedPayload{
		MessageID: message.MessageID, Text: message.Text, SourcePrincipalID: principal.ID,
		SourcePrincipalKind: string(principal.Kind), SourceChannel: principal.Channel,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	event := events.Event{
		EventID: "input-1", Sequence: 1, OrganizationID: principal.OrganizationID, EventType: "A2A_INPUT_RECEIVED",
		SourceActorID: principal.ID, TaskID: "task-1", CorrelationID: "work-1", Payload: body,
		CreatedAt: time.Unix(1, 0).UTC(), SchemaVersion: events.SchemaVersion,
	}
	matched, err := matchesDurableInput([]events.Event{event}, principal, message)
	if err != nil || !matched {
		t.Fatalf("canonical replay was rejected: matched=%t err=%v", matched, err)
	}
	event.Payload = []byte(`{"message_id":"message-1","text":"bounded continuation","source_principal_id":"external-agent","source_principal_kind":"EXTERNAL_AGENT","source_channel":"A2A","source_external_actor":"external-agent"}`)
	if matched, err := matchesDurableInput([]events.Event{event}, principal, message); err == nil || matched {
		t.Fatalf("non-canonical replay was accepted: matched=%t err=%v", matched, err)
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
	if correlationID, found, err := store.ResolveExternalWork(context.Background(), "org-1", externalRequestID); err != nil {
		t.Fatal(err)
	} else if found {
		stream, err := store.Events(context.Background(), correlationID)
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
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

func countEvents(stream []events.Event, eventType string) int {
	count := 0
	for _, event := range stream {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}
