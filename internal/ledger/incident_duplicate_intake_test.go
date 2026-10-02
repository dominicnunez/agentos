package ledger

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDuplicateIntakeIdentity(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{"message_id", "source_message_id", "escaped-message"} {
		t.Run(field, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendIncidentReplacements(t, store, 2)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "replacement-1", 256); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			body := []byte(`{"message_id":"unrelated","source_message_id":"unrelated","source_principal_id":"user-1","source_principal_kind":"HUMAN","source_channel":"HUMAN_DIRECT"}`)
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				event, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTAKE_ABANDONED", SourceActorID: "runtime", CorrelationID: "hidden-intake", TaskID: "hidden-task", Payload: json.RawMessage(body)})
				if err != nil {
					return err
				}
				member := field
				if field == "escaped-message" {
					member = "message_id"
				}
				body = duplicateJSONMember(t, body, member, []byte(`"source-replacement-1"`))
				if field == "escaped-message" {
					body = bytes.ReplaceAll(body, []byte(`"message_id"`), []byte(`"message\u005fid"`))
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
				t.Fatal("full owner accepted duplicate intake claim")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "replacement-1", 256)
			if err == nil {
				t.Fatal("incident omitted later duplicate selected message identity")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("duplicate intake claim returned partial evidence")
			}
		})
	}
}
