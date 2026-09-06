package execution

import (
	"context"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type countingStructuredModel struct {
	FakeModel
	calls int
}

func (m *countingStructuredModel) CompleteRequest(ctx context.Context, request modelinput.Request) (ModelResponse, error) {
	m.calls++
	return m.FakeModel.CompleteRequest(ctx, request)
}

func TestStructuredExecutionRequiresExactManifestedSourceBinding(t *testing.T) {
	for _, version := range []string{"v4", "v5"} {
		t.Run(version, func(t *testing.T) { testStructuredExecutionRequiresExactManifestedSourceBinding(t, version) })
	}
}

func testStructuredExecutionRequiresExactManifestedSourceBinding(t *testing.T, version string) {
	t.Helper()
	request := modelinput.Request{Version: modelinput.Version, Messages: []modelinput.Message{{Role: modelinput.User, Text: "work", Source: modelinput.Source{Kind: modelinput.TaskContext, Reference: "task-1", Digest: modelinput.TextDigest("work")}}}}
	body, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := request.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	model := &countingStructuredModel{}
	adapter := NewAgentExecution(model)
	descriptor := model.Descriptor()
	manifest := core.ExecutionContextManifest{ContextBuilderVersion: version, ExecutionInputSHA256: fingerprint, Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
	task := core.Task{ID: "task-1", ModelInferencePolicy: core.InferenceAllowed, ExecutionBrief: string(body)}
	if _, err := adapter.Execute(t.Context(), task, manifest); err != nil || model.calls != 1 {
		t.Fatalf("valid manifested input rejected: %v", err)
	}
	// Keep the content and its digest valid, but substitute the runtime source.
	request.Messages[0].Source.Reference = "different-task"
	body, err = request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	task.ExecutionBrief = string(body)
	if _, err := adapter.Execute(t.Context(), task, manifest); err == nil || model.calls != 1 {
		t.Fatal("substituted source crossed the provider boundary")
	}
	task.ExecutionBrief = `{"version":"model-input-v1","Version":"model-input-v1","messages":[]}`
	if _, err := adapter.Execute(t.Context(), task, manifest); err == nil || model.calls != 1 {
		t.Fatal("ambiguous structured input crossed the provider boundary")
	}
}
