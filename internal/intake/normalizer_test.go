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
	ready := `{"state":"READY_FOR_REVIEW","reply":"Please review this interpretation.","intent":{"mode":"STANDARD","objective":"Release version 1","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"The verified binary is public","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":["PUBLIC_EXTERNAL"],"missing_user_inputs":[]}}`
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
		strings.Replace(ready, `"mode":"STANDARD"`, `"mode":""`, 1),
		strings.Replace(ready, `"mode":"STANDARD"`, `"mode":"ADMIN"`, 1),
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
	response := `{"state":"NEEDS_USER_INPUT","reply":"Which release version should be used?","intent":{"mode":"STANDARD","objective":"Publish a release","context":[],"deliverables":[],"completion_criteria":[],"constraints":[],"resolved_decisions":[],"consequence_candidates":["PUBLIC_EXTERNAL"],"missing_user_inputs":[{"value":"Release version","origin":"INFERRED"}]}}`
	normalizer, err := NewModelNormalizer(normalizationModel{response: response})
	if err != nil {
		t.Fatal(err)
	}
	result, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: "Publish a release"}})
	if err != nil || result.State != normalizationNeedsInput || len(result.Candidate.MissingUserInputs) != 1 {
		t.Fatalf("normalization=%+v err=%v", result, err)
	}
}

func TestModelNormalizerBindsOnlyExplicitGoalReference(t *testing.T) {
	response := `{"state":"READY_FOR_REVIEW","reply":"Review the Goal-bound work.","intent":{"mode":"STANDARD","objective":"Advance the Goal","goal":{"value":"goal-123","origin":"EXPLICIT","source_message_id":"message-1"},"context":[],"deliverables":[{"value":"Result","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Verified","origin":"DEFAULT"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	normalizer, err := NewModelNormalizer(normalizationModel{response: response})
	if err != nil {
		t.Fatal(err)
	}
	result, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: "Use goal-123 for this work"}})
	if err != nil || result.Candidate.Goal == nil || result.Candidate.Goal.Value != "goal-123" {
		t.Fatalf("explicit Goal normalization=%+v err=%v", result, err)
	}
	for _, invalid := range []string{
		strings.Replace(response, `"origin":"EXPLICIT"`, `"origin":"INFERRED"`, 1),
		strings.Replace(response, `goal-123`, `goal-invented`, 1),
		strings.Replace(response, `goal-123`, `goal-12`, 1),
		strings.Replace(response, `"source_message_id":"message-1"`, `"source_message_id":"unknown"`, 1),
	} {
		normalizer, err := NewModelNormalizer(normalizationModel{response: invalid})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: "Use goal-123 for this work"}}); err == nil {
			t.Fatalf("untrusted Goal binding was accepted: %s", invalid)
		}
	}
}

func TestModelNormalizerTreatsOnlyUnambiguousPunctuationAsGoalBoundary(t *testing.T) {
	response := `{"state":"READY_FOR_REVIEW","reply":"Review the Goal-bound work.","intent":{"mode":"STANDARD","objective":"Advance the Goal","goal":{"value":"goal-123","origin":"EXPLICIT","source_message_id":"message-1"},"context":[],"deliverables":[{"value":"Result","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Verified","origin":"DEFAULT"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	for _, text := range []string{"Use goal-123, then continue", "Use goal-123!", "Use (goal-123)."} {
		normalizer, err := NewModelNormalizer(normalizationModel{response: response})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: text}}); err != nil {
			t.Fatalf("ordinary Goal punctuation was rejected for %q: %v", text, err)
		}
	}
	for _, text := range []string{"Use goal-123.", "Use goal-123: complete the work", "Use goal-123.4.", "Use goal-123:child."} {
		normalizer, err := NewModelNormalizer(normalizationModel{response: response})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := normalizer.Normalize(context.Background(), []ConversationTurn{{MessageID: "message-1", Text: text}}); err == nil {
			t.Fatalf("Goal prefix was accepted as an exact reference in %q", text)
		}
	}
}

func TestModelNormalizerBindsExplicitReplacementWorkProvenance(t *testing.T) {
	response := `{"state":"READY_FOR_REVIEW","reply":"Review the replacement.","intent":{"mode":"STANDARD","objective":"Retry with a bounded approach","goal":null,"replaces_work":{"value":"work-failed-1","origin":"EXPLICIT","source_message_id":"message-1"},"context":[],"deliverables":[{"value":"Verified replacement result","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Result passes verification","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	normalizer, err := NewModelNormalizer(normalizationModel{response: response})
	if err != nil {
		t.Fatal(err)
	}
	turns := []ConversationTurn{{MessageID: "message-1", Text: "Replace work-failed-1 with a bounded approach and verify the result."}}
	result, err := normalizer.Normalize(context.Background(), turns)
	if err != nil || result.Candidate.ReplacesWork == nil || result.Candidate.ReplacesWork.Value != "work-failed-1" {
		t.Fatalf("replacement normalization=%+v err=%v", result, err)
	}

	for name, changed := range map[string]string{
		"invented provenance": strings.Replace(response, `"origin":"EXPLICIT","source_message_id":"message-1"`, `"origin":"INFERRED","source_message_id":"message-1"`, 1),
		"absent reference":    strings.Replace(response, `"value":"work-failed-1"`, `"value":"work-other"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			normalizer, err := NewModelNormalizer(normalizationModel{response: changed})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := normalizer.Normalize(context.Background(), turns); err == nil {
				t.Fatal("untrusted replacement provenance was accepted")
			}
		})
	}
}
