package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentExecutionAdmission(t *testing.T) {
	for _, mutation := range []string{"valid", "missing-detail", "wrong-dispatch"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "admission.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			incidentTestExecution(t, store)
			if mutation != "valid" {
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					stream, err := collectEvents(tx.QueryContext(t.Context(), `SELECT `+incidentEventColumns+` FROM events WHERE event_type='EXECUTION_STARTED'`))
					if err != nil {
						return err
					}
					if len(stream) != 1 {
						return fmt.Errorf("expected one start")
					}
					event := stream[0]
					payload, _, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					var detail events.ExecutionStartDetail
					if mutation == "wrong-dispatch" {
						if err = json.Unmarshal(payload.Detail, &detail); err != nil {
							return err
						}
						if detail.DispatchBinding == nil {
							return fmt.Errorf("fixture lacks dispatch")
						}
						detail.DispatchBinding.AgentRecordVersion++
					}
					body, err := json.Marshal(detail)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(event, payload.Projection, body)
					if err != nil {
						return err
					}
					body, err = json.Marshal(sealed)
					if err != nil {
						return err
					}
					if _, err = tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
						return err
					}
					if _, err = tx.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint=? WHERE admission_event_id=?`, sealed.Admission.Fingerprint, event.EventID); err != nil {
						return err
					}
					if _, err = tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			_, err = store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if mutation == "valid" && err != nil {
				t.Fatal(err)
			}
			if mutation != "valid" && err == nil {
				t.Fatal("sealed invalid execution admission accepted")
			}
		})
	}
}
