package execution

import (
	"context"

	"github.com/dominicnunez/agentos/internal/modelinput"
)

type openAIInputMessage struct {
	Type    string          `json:"type"`
	Role    modelinput.Role `json:"role"`
	Content string          `json:"content"`
}

// CompleteRequest uses Responses message items so trusted instructions never
// share a role with untrusted work context. Source evidence remains bound in
// the guarded reservation rather than being sent as provider metadata.
func (a *OpenAIAPI) CompleteRequest(ctx context.Context, request modelinput.Request) (ModelResponse, error) {
	if len(request.Messages) > modelinput.MaximumMessages {
		return ModelResponse{}, RequestNotSent(modelinput.ErrLimit)
	}
	request.Messages = append([]modelinput.Message(nil), request.Messages...)
	if _, err := request.Canonical(); err != nil {
		return ModelResponse{}, RequestNotSent(err)
	}
	input := make([]openAIInputMessage, len(request.Messages))
	for i, message := range request.Messages {
		content, err := modelinput.WireText(message)
		if err != nil {
			return ModelResponse{}, RequestNotSent(err)
		}
		if message.Role == modelinput.Data {
			input[i] = openAIInputMessage{Type: "message", Role: modelinput.User, Content: content}
			continue
		}
		input[i] = openAIInputMessage{Type: "message", Role: message.Role, Content: content}
	}
	return a.completeInput(ctx, input)
}
