package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type failedUsageModel struct {
	FakeModel
	usage events.InferenceUsageRecordedPayload
}

func (m failedUsageModel) Complete(context.Context, string) (ModelResponse, error) {
	return ModelResponse{Text: "suppressed"}, WithReconciledUsage(errors.New("interrupted"), m.usage)
}

func TestAgentExecutionRetainsOnlyMatchingReconciledUsage(t *testing.T) {
	response, _ := (FakeModel{}).Complete(t.Context(), "work")
	for _, mismatch := range []string{"none", "provider", "model", "connection"} {
		t.Run(mismatch, func(t *testing.T) {
			usage := response.Usage
			switch mismatch {
			case "none":
			case "provider":
				usage.Provider = "other"
			case "model":
				usage.Model = "other"
			case "connection":
				usage.ConnectionID = "other"
			}
			executor := NewAgentExecution(failedUsageModel{usage: usage})
			d := executor.Descriptor()
			result, err := executor.Execute(t.Context(), core.Task{ID: "task-1", Description: "work", ModelInferencePolicy: core.InferenceAllowed}, core.ExecutionContextManifest{Provider: d.Provider, Model: d.Model, ExecutionProfileVersion: d.ExecutionProfileVersion})
			if err == nil || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ObservedEffect != nil || (result.InferenceUsage != nil) != (mismatch == "none") {
				t.Fatalf("failed-call evidence mishandled: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestReconciledUsageSurvivesWrappingWithoutAliasing(t *testing.T) {
	response, _ := (FakeModel{}).Complete(t.Context(), "work")
	cause := errors.New("interrupted")
	err := fmt.Errorf("guard: %w", WithReconciledUsage(cause, response.Usage))
	*response.Usage.CostUSD = 9
	usage, ok := ReconciledUsage(err)
	if !ok || !errors.Is(err, cause) || *usage.CostUSD != 0 {
		t.Fatal("usage lost or aliased its source")
	}
	*usage.CostUSD = 7
	again, _ := ReconciledUsage(err)
	if *again.CostUSD != 0 {
		t.Fatal("usage aliases its returned copy")
	}
}
