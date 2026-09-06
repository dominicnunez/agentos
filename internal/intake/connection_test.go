package intake

import (
	"context"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"testing"
)

type accountNormalizationModel struct {
	normalizationModel
	account string
}

func (m accountNormalizationModel) Descriptor() NormalizerDescriptor {
	descriptor := m.normalizationModel.Descriptor()
	descriptor.ConnectionID = "configured"
	return descriptor
}
func (m accountNormalizationModel) CompleteRequest(ctx context.Context, request modelinput.Request) (TextCompletion, error) {
	result, err := m.normalizationModel.CompleteRequest(ctx, request)
	result.Usage.ConnectionID = m.account
	return result, err
}
func TestNormalizerRequiresConfiguredUsageAccount(t *testing.T) {
	const ready = `{"state":"READY_FOR_REVIEW","reply":"Review the note.","intent":{"mode":"STANDARD","objective":"Draft note","context":[],"deliverables":[{"value":"Note","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Note drafted","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
	for _, account := range []string{"configured", "other", ""} {
		normalizer, err := NewModelNormalizer(accountNormalizationModel{normalizationModel: normalizationModel{response: ready}, account: account})
		if err != nil {
			t.Fatal(err)
		}
		result, err := normalizer.Normalize(normalizationTestContext(t), []ConversationTurn{{MessageID: "message-1", Text: "Draft note"}})
		if (err == nil) != (account == "configured") {
			t.Fatalf("account=%q err=%v", account, err)
		}
		if err == nil && (result.Usage == nil || result.Usage.ConnectionID != "configured") {
			t.Fatal("normalization account attribution lost")
		}
	}
}
