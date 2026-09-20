package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentRejectsRepeatedTerminalWork(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "work.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident", "work-1")
	var body []byte
	if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='work'`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	var record events.ProjectionRecord
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	var work core.Work
	if err := json.Unmarshal(record.Value, &work); err != nil {
		t.Fatal(err)
	}
	work.Status = core.WorkFailed
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: "incident"}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: draft, ProjectionKind: "work", RecordID: "work-1", Version: 2, Value: work}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err != nil {
		t.Fatal(err)
	}
	// Retained, correctly sealed history can still contain an illegal transition.
	draft.Payload = map[string]string{}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, err := appendEvent(t.Context(), tx, draft)
		if err != nil {
			return err
		}
		record.Version = 3
		record.Value, err = json.Marshal(work)
		if err != nil {
			return err
		}
		payload, err := events.SealProjectionEvent(event, record, nil)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, encoded, event.EventID); err != nil {
			return err
		}
		body, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at) VALUES('work','work-1',3,?,?,?,?)`, body, event.EventID, payload.Admission.Fingerprint, event.CreatedAt.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err == nil {
		t.Fatal("accepted repeated terminal Work transition")
	}
}

func TestIncidentRequiresExactSelectedRecords(t *testing.T) {
	for _, mutation := range []string{"work-padding", "task-padding", "missing-intent"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incident.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			stopTestExecution(t, store)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
				t.Fatal(err)
			}
			if mutation == "missing-intent" {
				_, err = store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='intent'`)
			} else {
				kind := "work"
				if mutation == "task-padding" {
					kind = "task"
				}
				_, err = store.db.ExecContext(t.Context(), `UPDATE records SET body=CAST(body AS TEXT)||' ' WHERE kind=?`, kind)
			}
			if err != nil {
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
				t.Fatal("accepted selected projection without exact canonical record")
			}
		})
	}
}
