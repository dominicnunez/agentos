package inference

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

// CompleteRequest binds the roles and source evidence into the same durable
// reservation used for billing and identity admission. There is no string
// fallback for a provider that cannot preserve this contract.
func (a *GuardedAdapter) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	if len(request.Messages) > modelinput.MaximumMessages {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, modelinput.ErrLimit)
	}
	request.Messages = append([]modelinput.Message(nil), request.Messages...)
	scope, err := scopeFromContext(ctx)
	if err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, err)
	}
	invocation, err := modelinput.InvocationScope(scope.OrganizationID, scope.ExecutionID)
	if err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, err)
	}
	if err := modelinput.ValidateBinding(invocation, request); err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, err)
	}
	fingerprint, err := request.Fingerprint()
	if err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, err)
	}
	adapter, ok := a.adapter.(execution.StructuredModelAdapter)
	if !ok {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, fmt.Errorf("adapter does not support structured input"))
	}
	return a.complete(ctx, fingerprint, func() (execution.ModelResponse, error) {
		return adapter.CompleteRequest(ctx, request)
	})
}
