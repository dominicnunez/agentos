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

func TestIncidentRequiresSelectedProjectionAdmissions(t *testing.T) {
	for _, mutation := range []string{"missing-admission", "missing-event", "moved-event", "moved-event-missing-fingerprint", "all-events-moved"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incident.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			stopTestExecution(t, store)
			if mutation == "missing-event" {
				now := time.Now().UTC()
				intent := core.Intent{ID: "orphan-intent", OrganizationID: "org-1", OriginalInstruction: "orphan", NormalizedObjective: "orphan", CreatedAt: now}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "stop-work"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent}); err != nil {
					t.Fatal(err)
				}
			}
			if mutation == "all-events-moved" {
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "stop-work", EventType: "AUDIT_NOTE", Payload: map[string]string{"reason": "retain selected stream"}}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
				t.Fatalf("valid incident was rejected: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				statement := `UPDATE events SET payload=CAST('{}' AS BLOB) WHERE event_type='INTENT_CREATED'`
				switch mutation {
				case "missing-event":
					statement = `DELETE FROM events WHERE event_id=(SELECT admission_event_id FROM records WHERE kind='intent' AND record_id='orphan-intent')`
				case "moved-event", "moved-event-missing-fingerprint":
					statement = `UPDATE events SET correlation_id='other-run' WHERE event_type='INTENT_CREATED'`
				case "all-events-moved":
					statement = `UPDATE events SET correlation_id='other-run' WHERE correlation_id='stop-work' AND event_type IN ('INTENT_CREATED','WORK_CREATED','TASK_CREATED','EXECUTION_STARTED')`
				}
				if _, err := tx.ExecContext(t.Context(), statement); err != nil {
					return err
				}
				if mutation == "moved-event-missing-fingerprint" {
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET admission_fingerprint='' WHERE kind='intent'`); err != nil {
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
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
				t.Fatalf("accepted selected projection with %s", mutation)
			}
		})
	}
}

func TestIncidentProjectionRecordTenantIsolation(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	stopTestExecution(t, store)
	appendTaskProjectionParents(t, t.Context(), store, "org-2", "stop-work", "other-work")
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err != nil {
		t.Fatalf("other tenant's projection records affected incident: %v", err)
	}
}
