package intake

import (
	"context"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

type normalizationModel struct{ response string }

func (normalizationModel) Descriptor() NormalizerDescriptor {
	return NormalizerDescriptor{Provider: "test", Model: "test-model", ExecutionProfileVersion: "test-profile"}
}

func (m normalizationModel) CompleteText(context.Context, string) (TextCompletion, error) {
	return TextCompletion{Text: m.response, Usage: events.InferenceUsageRecordedPayload{Source: "test", Provider: "test", Model: "test-model"}}, nil
}

func TestModelNormalizerRequiresCompleteStrictIntent(t *testing.T) {
	ready := `{"state":"READY_FOR_REVIEW","reply":"Please review this interpretation.","intent":{"objective":"Release version 1","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"The verified binary is public","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":["PUBLIC_EXTERNAL"],"missing_user_inputs":[]}}`
	normalizer, err := NewModelNormalizer(normalizationModel{response: ready})
	if err != nil {
		t.Fatal(err)
	}
	result, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: "Release version 1 for Linux"}})
	if err != nil || result.State != normalizationReady || result.Candidate.Objective != "Release version 1" || result.Usage == nil {
		t.Fatalf("normalization=%+v err=%v", result, err)
	}

	for _, malformed := range []string{
		ready + `{}`,
		strings.Replace(ready, `"missing_user_inputs":[]`, `"missing_user_inputs":[{"value":"version","origin":"INFERRED"}]`, 1),
		strings.Replace(ready, `"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}]`, `"deliverables":[]`, 1),
		strings.Replace(ready, `"consequence_candidates":["PUBLIC_EXTERNAL"]`, `"consequence_candidates":["NO_APPROVAL_NEEDED"]`, 1),
		strings.Replace(ready, `"source_message_id":"message-1"`, `"source_message_id":"unknown-message"`, 1),
		strings.Replace(ready, `,"source_message_id":"message-1"`, ``, 1),
	} {
		normalizer, err := NewModelNormalizer(normalizationModel{response: malformed})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: "release"}}); err == nil {
			t.Fatalf("malformed normalization was accepted: %s", malformed)
		}
	}
}

func TestModelNormalizerAllowsOnlyExplicitMissingUserInputState(t *testing.T) {
	response := `{"state":"NEEDS_USER_INPUT","reply":"Which release version should be used?","intent":{"objective":"Publish a release","context":[],"deliverables":[],"completion_criteria":[],"constraints":[],"resolved_decisions":[],"consequence_candidates":["PUBLIC_EXTERNAL"],"missing_user_inputs":[{"value":"Release version","origin":"INFERRED"}]}}`
	normalizer, err := NewModelNormalizer(normalizationModel{response: response})
	if err != nil {
		t.Fatal(err)
	}
	result, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: "Publish a release"}})
	if err != nil || result.State != normalizationNeedsInput || len(result.Candidate.MissingUserInputs) != 1 {
		t.Fatalf("normalization=%+v err=%v", result, err)
	}
}
