package ledger

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEffectRequiresEarlierTask(t *testing.T) {
	for _, laterConfirmation := range []bool{false, true} {
		t.Run(map[bool]string{false: "attempt", true: "later-confirmation"}[laterConfirmation], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "effects.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			earlier, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", Payload: map[string]string{"reason": "before Task"}})
			if err != nil {
				t.Fatal(err)
			}
			attempt := incidentEffectFixture(t, store)
			if laterConfirmation {
				confirmed := attempt
				confirmed.Status = core.EffectConfirmed
				confirmed.ConfirmationEvidenceRefs = []string{"receipt"}
				if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(attempt.TaskID), attempt.AuthorizationRefs, confirmed.ConfirmationEvidenceRefs, "effect", string(attempt.ID), 2, confirmed); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var originalSequence int64
				if err := tx.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' ORDER BY sequence LIMIT 1`).Scan(&originalSequence); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET sequence=(SELECT MAX(sequence)+1 FROM events) WHERE event_id=?`, earlier.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET sequence=? WHERE event_id=(SELECT event_id FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' ORDER BY sequence LIMIT 1)`, earlier.Sequence); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET sequence=? WHERE event_id=?`, originalSequence, earlier.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
				t.Fatal("accepted effect before its Task existed")
			}
		})
	}
}
