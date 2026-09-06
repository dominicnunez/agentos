package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/modelinput"
)

// CompleteRequest maps trusted messages to the app-server's developer
// instructions and user messages to one bounded JSON array in the fresh user
// turn. This transport has no assistant-history input seam; reject it rather
// than relabeling assistant messages as user or trusted instructions.
func (a *CodexSubscription) CompleteRequest(ctx context.Context, request modelinput.Request) (ModelResponse, error) {
	if len(request.Messages) > modelinput.MaximumMessages {
		return ModelResponse{}, RequestNotSent(modelinput.ErrLimit)
	}
	request.Messages = append([]modelinput.Message(nil), request.Messages...)
	if _, err := request.Canonical(); err != nil {
		return ModelResponse{}, RequestNotSent(err)
	}
	var trusted []string
	var data []json.RawMessage
	for _, message := range request.Messages {
		switch message.Role {
		case modelinput.System:
			if len(data) != 0 {
				return ModelResponse{}, RequestNotSent(fmt.Errorf("codex requires instructions before user messages"))
			}
			trusted = append(trusted, message.Text)
		case modelinput.User, modelinput.Data:
			quoted, err := modelinput.WireText(message)
			if err != nil {
				return ModelResponse{}, RequestNotSent(err)
			}
			encoded := json.RawMessage(quoted)
			if message.Source.Handle == "" && message.Role == modelinput.User {
				// Unbound user text has no runtime envelope. Encode it as a
				// string; never interpret payload JSON as envelope metadata.
				encoded, err = json.Marshal(quoted)
				if err != nil {
					return ModelResponse{}, RequestNotSent(modelinput.ErrInvalid)
				}
			}
			data = append(data, encoded)
		case modelinput.Assistant:
			return ModelResponse{}, RequestNotSent(fmt.Errorf("codex structured input does not support assistant history"))
		default:
			return ModelResponse{}, RequestNotSent(fmt.Errorf("codex structured input does not support this message role"))
		}
	}
	if len(data) == 0 {
		return ModelResponse{}, RequestNotSent(fmt.Errorf("codex structured input requires user data"))
	}
	body, err := json.Marshal(data)
	if err != nil {
		return ModelResponse{}, RequestNotSent(modelinput.ErrInvalid)
	}
	instructions := strings.Join(trusted, "\n\n")
	return a.completeInput(ctx, string(body), &instructions)
}
