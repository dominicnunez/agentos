package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestRequestNotSentMarkerSurvivesWrapping(t *testing.T) {
	sentinel := errors.New("local rejection")
	marked := RequestNotSent(sentinel)
	if !WasRequestNotSent(fmt.Errorf("adapter: %w", marked)) || !errors.Is(marked, sentinel) {
		t.Fatal("definite pre-send failure lost its marker or cause")
	}
	if WasRequestNotSent(sentinel) || RequestNotSent(nil) != nil {
		t.Fatal("ordinary or nil error was classified as a definite pre-send failure")
	}
}

func TestAgentExecutionReturnsSeparateUsageContract(t *testing.T) {
	executor := NewAgentExecution(FakeModel{})
	descriptor := executor.Descriptor()
	if descriptor.Provider != "fake" || descriptor.Model != "fake-model/v1" || descriptor.ExecutionProfileVersion != "v1-fake" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	result, err := executor.Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.ObservedEffect != "fake-model: work" || result.InferenceUsage == nil || !result.InferenceUsage.Valid() || result.InferenceUsage.CostUSD == nil || *result.InferenceUsage.CostUSD != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestReviewFakeModelIsASeparateNonNetworkProfile(t *testing.T) {
	executor := NewAgentExecution(ReviewFakeModel{})
	descriptor := executor.Descriptor()
	if descriptor.Provider != "fake-review" || descriptor.Model != "fake-review-model/v1" || descriptor.ExecutionProfileVersion != "v1-fake-review" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	result, err := executor.Execute(context.Background(), core.Task{ID: "task-1", Description: "review work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome.ObservedEffect != "fake-review-model: review work" || result.Outcome.ToolID != "fake-review-model/v1" || result.InferenceUsage == nil || result.InferenceUsage.Provider != "fake-review" || !result.InferenceUsage.Valid() {
		t.Fatalf("result=%+v", result)
	}
}

type mismatchedUsageModel struct{}

func (mismatchedUsageModel) Name() string { return "configured/model" }
func (mismatchedUsageModel) Descriptor() ModelDescriptor {
	return ModelDescriptor{Provider: "configured", Model: "model", ExecutionProfileVersion: "profile-v1"}
}
func (mismatchedUsageModel) Complete(context.Context, string) (ModelResponse, error) {
	return ModelResponse{Text: "untrusted response", Usage: events.InferenceUsageRecordedPayload{Source: "provider_response", Provider: "different", Model: "model", InputTokens: 1, TotalTokens: 1}}, nil
}

func TestAgentExecutionRejectsMismatchedProviderUsageIdentity(t *testing.T) {
	executor := NewAgentExecution(mismatchedUsageModel{})
	descriptor := executor.Descriptor()
	result, err := executor.Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err == nil || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ErrorClass != "provider_contract" || result.InferenceUsage != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

type manifestTrackingModel struct{ called bool }

func (*manifestTrackingModel) Name() string { return "fake-model/v1" }
func (*manifestTrackingModel) Descriptor() ModelDescriptor {
	return FakeModel{}.Descriptor()
}
func (m *manifestTrackingModel) Complete(context.Context, string) (ModelResponse, error) {
	m.called = true
	return FakeModel{}.Complete(context.Background(), "work")
}

func TestAgentExecutionRejectsMismatchedManifestIdentityBeforeCallingProvider(t *testing.T) {
	model := &manifestTrackingModel{}
	result, err := NewAgentExecution(model).Execute(context.Background(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: "other", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"})
	if err == nil || model.called || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ErrorClass != "provider_contract" || result.InferenceUsage != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
