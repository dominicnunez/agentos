package intake

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type intakeExecutionModel struct {
	response string
	calls    int
}

func (*intakeExecutionModel) Name() string { return "test/test-model" }
func (*intakeExecutionModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "test", Model: "test-model", ExecutionProfileVersion: "test-profile"}
}
func (m *intakeExecutionModel) Complete(_ context.Context, _ string) (execution.ModelResponse, error) {
	m.calls++
	return execution.ModelResponse{Text: m.response, Usage: events.InferenceUsageRecordedPayload{Source: "test", Provider: "test", Model: "test-model"}}, nil
}

type guardedIntakeModel struct {
	adapter   execution.StructuredModelAdapter
	lastError error
}

func (m guardedIntakeModel) Descriptor() NormalizerDescriptor {
	descriptor := m.adapter.Descriptor()
	return NormalizerDescriptor{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
}
func (m *guardedIntakeModel) CompleteRequest(ctx context.Context, request modelinput.Request) (TextCompletion, error) {
	response, err := m.adapter.CompleteRequest(ctx, request)
	m.lastError = err
	return TextCompletion{Text: response.Text, Usage: response.Usage}, err
}

type initialIntakeDenialLedger struct {
	*ledger.SQLite
	deny           bool
	loseProof      bool
	loseSuspension bool
}

func (l *initialIntakeDenialLedger) BeginInferenceContext(ctx context.Context, organization string) (context.Context, func(), error) {
	if l.deny {
		l.deny = false
		return nil, nil, core.ErrContainmentUnavailable
	}
	return l.SQLite.BeginInferenceContext(ctx, organization)
}

func (l *initialIntakeDenialLedger) RecordInferenceNotSent(ctx context.Context, request inference.InferenceRequest) error {
	if l.loseProof {
		return errors.New("injected loss of non-dispatch evidence")
	}
	return l.SQLite.RecordInferenceNotSent(ctx, request)
}

func (l *initialIntakeDenialLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if l.loseSuspension && draft.EventType == "INTENT_NORMALIZATION_SUSPENDED" {
		return events.Event{}, errors.New("injected crash before suspension")
	}
	return l.SQLite.Append(ctx, draft)
}

func TestIntentNormalizationUsesDurableInferenceScope(t *testing.T) {
	for _, mode := range []string{"normal", "initial-denial", "lost-suspension", "lost-proof"} {
		t.Run(mode, func(t *testing.T) { testIntentNormalizationUsesDurableInferenceScope(t, mode) })
	}
}

func testIntentNormalizationUsesDurableInferenceScope(t *testing.T, mode string) {
	store, err := ledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	if err := store.ActivateInferencePolicy(t.Context(), inference.Policy{
		Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: "test", Model: "test-model",
		ExecutionProfileVersion: "test-profile", Mode: inference.Local,
		MaxInputTokensPerRequest: 262_144, MaxOutputTokensPerRequest: 262_144, MaxTokensPerWindow: 1_000_000,
		ContinuityReserveTokens: 100_000, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
		AuthorizedBy: "local-uid-1000", AuthorizedAt: now.Add(-time.Minute), AuthorizationExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	ready := `{"state":"READY_FOR_REVIEW","reply":"Review this intent.","intent":{"mode":"STANDARD","objective":"Prepare a Linux release","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Binary passes verification","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	writer := &initialIntakeDenialLedger{SQLite: store, deny: mode != "normal", loseProof: mode == "lost-proof", loseSuspension: mode == "lost-suspension"}
	model := &intakeExecutionModel{response: ready}
	guarded, err := inference.NewGuardedAdapter(writer, model)
	if err != nil {
		t.Fatal(err)
	}
	guardModel := &guardedIntakeModel{adapter: guarded}
	normalizer, err := NewModelNormalizer(guardModel)
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithNormalizer(app.New(events.NewGateway(writer)), normalizer)
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	message := Message{ConversationID: "guarded-intake", MessageID: "message-1", Text: "Prepare a Linux release"}
	if mode != "normal" {
		if _, err := service.Handle(t.Context(), principal, message); err == nil || model.calls != 0 {
			t.Fatalf("initial denial dispatched: calls=%d err=%v", model.calls, err)
		}
		service = NewWithNormalizer(app.New(events.NewGateway(store)), normalizer)
		if mode == "lost-proof" {
			if _, err := service.Handle(t.Context(), principal, message); err == nil || model.calls != 0 {
				t.Fatalf("unproven retry dispatched: calls=%d err=%v", model.calls, err)
			}
			return
		}
	}
	view, err := service.Handle(t.Context(), principal, message)
	if err != nil || view.State != StateAwaitingConfirmation {
		t.Fatalf("guarded intake=%+v err=%v guard=%v unavailable=%t not-sent=%t provider-calls=%d", view, err, guardModel.lastError, errors.Is(guardModel.lastError, core.ErrContainmentUnavailable), execution.WasRequestNotSent(guardModel.lastError), model.calls)
	}
	stream := externalStream(t, store, message.ConversationID)
	wantAttempts, wantProof := 1, 0
	if mode != "normal" {
		wantAttempts, wantProof = 2, 1
	}
	if model.calls != 1 || countEvents(stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") != wantAttempts || countEvents(stream, "INFERENCE_NOT_SENT") != wantProof {
		t.Fatal("normalization retry lost its exact non-dispatch boundary")
	}
	if countEvents(stream, "INFERENCE_RESERVED") != 1 || countEvents(stream, "INFERENCE_RECONCILED") != 1 || countEvents(stream, "INFERENCE_USAGE_RECORDED") != 1 {
		t.Fatalf("normalization did not cross the exact inference boundary: %+v", stream)
	}
}
