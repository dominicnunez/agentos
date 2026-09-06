package execution

import (
	"context"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"testing"
)

type accountModel struct {
	countingStructuredModel
	account string
}

func (m *accountModel) ConnectionID() string { return m.account }

func TestExecutionRejectsSameModelAccountSubstitution(t *testing.T) {
	request := modelinput.Request{Version: modelinput.Version, Messages: []modelinput.Message{{Role: modelinput.User, Text: "work", Source: modelinput.Source{Kind: modelinput.TaskContext, Reference: "task-1", Digest: modelinput.TextDigest("work")}}}}
	body, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := request.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	task := core.Task{ID: "task-1", ModelInferencePolicy: core.InferenceAllowed, ExecutionBrief: string(body)}
	model := &accountModel{account: "account-a"}
	descriptor := model.Descriptor()
	manifest := core.ExecutionContextManifest{ConnectionID: "account-a", ContextBuilderVersion: "v5", ExecutionInputSHA256: fingerprint, Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion}
	executor := NewAgentExecution(model)
	if _, err := executor.Execute(t.Context(), task, manifest); err != nil || model.calls != 1 {
		t.Fatalf("approved account rejected: %v", err)
	}
	for _, account := range []string{"account-b", "", "../account-a"} {
		manifest.ConnectionID = account
		if _, err := executor.Execute(t.Context(), task, manifest); err == nil || model.calls != 1 {
			t.Fatalf("substituted account %q reached provider", account)
		}
	}
	manifest.ConnectionID = "account-a"
	manifest.ContextBuilderVersion = "v4"
	if _, err := executor.Execute(t.Context(), task, manifest); err == nil || model.calls != 1 {
		t.Fatal("named connection accepted legacy context")
	}
	manifest.ContextBuilderVersion = "v5"
	legacy := &countingStructuredModel{}
	if _, err := NewAgentExecution(legacy).Execute(t.Context(), task, manifest); err == nil || legacy.calls != 0 {
		t.Fatal("legacy adapter accepted named account")
	}
}

func (m *accountModel) CompleteRequest(ctx context.Context, request modelinput.Request) (ModelResponse, error) {
	response, err := m.countingStructuredModel.CompleteRequest(ctx, request)
	response.Usage.ConnectionID = m.account
	return response, err
}
