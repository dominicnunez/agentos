package execution

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

func (a *AgentExecution) completeStructured(ctx context.Context, input string, manifest core.ExecutionContextManifest) (ModelResponse, error) {
	if len(input) > modelinput.MaximumBytes {
		return ModelResponse{}, RequestNotSent(modelinput.ErrLimit)
	}
	request, err := modelinput.Decode([]byte(input))
	if err != nil {
		return ModelResponse{}, RequestNotSent(err)
	}
	fingerprint, err := request.Fingerprint()
	if err != nil || fingerprint != manifest.ExecutionInputSHA256 {
		return ModelResponse{}, RequestNotSent(fmt.Errorf("structured execution input does not match its manifest"))
	}
	adapter, ok := a.model.(StructuredModelAdapter)
	if !ok {
		return ModelResponse{}, RequestNotSent(fmt.Errorf("model adapter does not support structured input"))
	}
	return adapter.CompleteRequest(ctx, request)
}

func (m FakeModel) CompleteRequest(ctx context.Context, request modelinput.Request) (ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return ModelResponse{}, RequestNotSent(err)
	}
	return m.Complete(ctx, string(body))
}

func (m ReviewFakeModel) CompleteRequest(ctx context.Context, request modelinput.Request) (ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return ModelResponse{}, RequestNotSent(err)
	}
	return m.Complete(ctx, string(body))
}
