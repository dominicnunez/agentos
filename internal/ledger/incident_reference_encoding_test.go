package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentReferenceEncoding(t *testing.T) {
	for _, sample := range []struct {
		name    string
		raw     []byte
		invalid bool
	}{
		{"unicode", []byte(`["caf\u00e9"]`), false},
		{"nul", []byte(`["before\u0000after"]`), false},
		{"surrogate-pair", []byte(`["\ud83d\ude00"]`), false},
		{"unpaired-surrogate", []byte(`["\ud800"]`), false},
		{"invalid-utf8", append(append([]byte(`["`), 0xff), []byte(`"]`)...), true},
		{"truncated-utf8", append(append([]byte(`["`), 0xe2, 0x82), []byte(`"]`)...), true},
	} {
		t.Run(sample.name, func(t *testing.T) {
			store := sharedBudgetStore(t)
			event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "encoding", ArtifactRefs: []string{"original"}, Payload: map[string]string{"text": "bounded evidence"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET artifact_refs=CAST(? AS TEXT) WHERE event_id=?`, sample.raw, event.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			// The chain binds this exact raw source; a parser disagreement must
			// not undercount the reference bytes retained by the public reader.
			if _, err := ValidateEventIntegrity(t.Context(), store.db); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "encoding", 256)
			if sample.invalid {
				if err == nil {
					t.Fatal("accepted reference JSON with invalid UTF-8")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid reference source returned partial evidence")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			if err := json.Unmarshal(sample.raw, &want); err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Work.Events) != 1 || !reflect.DeepEqual(snapshot.Work.Events[0].ArtifactRefs, want) {
				t.Fatal("valid reference encoding changed")
			}
		})
	}
}
