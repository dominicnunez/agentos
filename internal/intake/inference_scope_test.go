package intake

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type intakeExecutionModel struct{ response string }

func (*intakeExecutionModel) Name() string { return "test/test-model" }
func (*intakeExecutionModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "test", Model: "test-model", ExecutionProfileVersion: "test-profile"}
}
func (m *intakeExecutionModel) Complete(context.Context, string) (execution.ModelResponse, error) {
	return execution.ModelResponse{Text: m.response, Usage: events.InferenceUsageRecordedPayload{Source: "test", Provider: "test", Model: "test-model"}}, nil
}

type guardedIntakeModel struct{ adapter execution.ModelAdapter }

func (m guardedIntakeModel) Descriptor() NormalizerDescriptor {
	descriptor := m.adapter.Descriptor()
	return NormalizerDescriptor{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
}
func (m guardedIntakeModel) CompleteText(ctx context.Context, prompt string) (TextCompletion, error) {
	response, err := m.adapter.Complete(ctx, prompt)
	return TextCompletion{Text: response.Text, Usage: response.Usage}, err
}

func TestIntentNormalizationUsesDurableInferenceScope(t *testing.T) {
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
	ready := `{"state":"READY_FOR_REVIEW","reply":"Review this intent.","intent":{"objective":"Prepare a Linux release","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Binary passes verification","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	guarded, err := inference.NewGuardedAdapter(store, &intakeExecutionModel{response: ready})
	if err != nil {
		t.Fatal(err)
	}
	normalizer, err := NewModelNormalizer(guardedIntakeModel{adapter: guarded})
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithNormalizer(app.New(events.NewGateway(store)), normalizer)
	principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
	message := Message{ConversationID: "guarded-intake", MessageID: "message-1", Text: "Prepare a Linux release"}
	view, err := service.Handle(t.Context(), principal, message)
	if err != nil || view.State != StateAwaitingConfirmation {
		t.Fatalf("guarded intake=%+v err=%v", view, err)
	}
	stream := externalStream(t, store, message.ConversationID)
	if countEvents(stream, "INFERENCE_RESERVED") != 1 || countEvents(stream, "INFERENCE_RECONCILED") != 1 || countEvents(stream, "INFERENCE_USAGE_RECORDED") != 1 {
		t.Fatalf("normalization did not cross the exact inference boundary: %+v", stream)
	}
}
