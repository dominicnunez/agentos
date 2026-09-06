package intake

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

func normalizationInput(ctx context.Context, contract string, turns []ConversationTurn) (*modelinput.Binding, error) {
	if len(turns) == 0 || len(turns) >= modelinput.MaximumMessages {
		return nil, modelinput.ErrLimit
	}
	request := modelinput.Request{Version: modelinput.Version, Messages: []modelinput.Message{{
		Role: modelinput.System, Text: contract,
		Source: modelinput.Source{Kind: modelinput.RuntimeContract, Reference: intentNormalizationPromptVersion, Digest: modelinput.TextDigest(contract)},
	}}}
	seen := make(map[string]bool, len(turns))
	for _, turn := range turns {
		if ValidateIdentifier("source message", turn.MessageID) != nil || !validIntentText(turn.Text) || seen[turn.MessageID] {
			return nil, fmt.Errorf("invalid or duplicate normalization source")
		}
		seen[turn.MessageID] = true
		request.Messages = append(request.Messages, modelinput.Message{
			Role: modelinput.User, Text: turn.Text,
			Source: modelinput.Source{Kind: modelinput.OperatorMessage, Reference: turn.MessageID, Digest: modelinput.TextDigest(turn.Text)},
		})
	}
	return modelinput.BindContext(ctx, request)
}

// Resolve only handles issued for operator messages in this invocation. The
// durable Intent continues to store original message IDs after this check.
func resolveNormalizationSources(ctx context.Context, binding *modelinput.Binding, result *Normalization) error {
	scope, err := modelinput.Invocation(ctx)
	if err != nil {
		return err
	}
	resolve := func(reference *string) error {
		if *reference == "" {
			return nil
		}
		source, err := binding.Resolve(scope, *reference)
		if err != nil || source.Kind != modelinput.OperatorMessage {
			return fmt.Errorf("intent provenance references an unknown operator source handle")
		}
		*reference = source.Reference
		return nil
	}
	candidate := &result.Candidate
	for _, group := range [][]core.IntentValue{candidate.Context, candidate.Deliverables, candidate.CompletionCriteria, candidate.Constraints, candidate.MissingUserInputs} {
		for i := range group {
			if err := resolve(&group[i].SourceMessageID); err != nil {
				return err
			}
		}
	}
	for i := range candidate.ResolvedDecisions {
		if err := resolve(&candidate.ResolvedDecisions[i].SourceMessageID); err != nil {
			return err
		}
	}
	for _, value := range []*core.IntentValue{candidate.Goal, candidate.ReplacesWork} {
		if value != nil {
			if err := resolve(&value.SourceMessageID); err != nil {
				return err
			}
		}
	}
	return nil
}
