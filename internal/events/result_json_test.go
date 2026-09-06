package events

import (
	"encoding/json"
	"testing"
)

func TestResultPublicationRejectsNoncanonicalFieldsBeforeAppend(t *testing.T) {
	for _, payload := range []string{
		`{"SUMMARY":"result","artifact_refs":["artifact-1"]}`,
		`{"summary":"result","artifact_refs":["artifact-1"],"extra":true}`,
		`{"summary":"first","summary":"result","artifact_refs":["artifact-1"]}`,
	} {
		ledger := &memoryLedger{}
		_, err := NewGateway(ledger).PublishAgentDraft(t.Context(), "org", "agent", "execution", "correlation", Draft{
			EventType: "RESULT_PUBLISHED", TaskID: "task-1", ArtifactRefs: []string{"artifact-1"}, Payload: json.RawMessage(payload),
		})
		if err == nil || len(ledger.events) != 0 {
			t.Errorf("noncanonical result reached ledger append: %s", payload)
		}
	}
}
