package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// ChangeLabEvidenceForTest reseals one Lab projection and its retained record.
func ChangeLabEvidenceForTest(t *testing.T, store *SQLite, id, field, target, side string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil || !found {
			return fmt.Errorf("read Lab event: %v", err)
		}
		payload, _, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		var value map[string]json.RawMessage
		if err := json.Unmarshal(payload.Projection.Value, &value); err != nil {
			return err
		}
		value[field], err = json.Marshal([]string{target})
		if err != nil {
			return err
		}
		payload.Projection.Value, err = json.Marshal(value)
		if err != nil {
			return err
		}
		sealed, err := events.SealProjectionEvent(event, payload.Projection, payload.Detail)
		if err != nil {
			return err
		}
		body, err := json.Marshal(sealed)
		if err != nil {
			return err
		}
		if side != "record" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, id); err != nil {
				return err
			}
		}
		body, err = json.Marshal(payload.Projection)
		if err != nil {
			return err
		}
		if side != "event" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, id); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}
