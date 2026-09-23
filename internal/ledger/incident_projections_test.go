package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
			incidentTestExecution(t, store)
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
			incidentTestExecution(t, store)
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

func TestIncidentDiscoversWorkHistoryThroughSelectedIntent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incident.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident", "work-1")
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "incident"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind IN ('work','task') ORDER BY kind,version`)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		type admission struct {
			body    []byte
			eventID string
		}
		var admissions []admission
		for rows.Next() {
			var value admission
			if err := rows.Scan(&value.body, &value.eventID); err != nil {
				return err
			}
			admissions = append(admissions, value)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, admission := range admissions {
			event, found, err := eventByID(t.Context(), tx, admission.eventID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("missing Work/Task admission event %s", admission.eventID)
			}
			payload, present, err := events.AdmittedProjection(event)
			if err != nil {
				return err
			}
			if !present {
				return fmt.Errorf("Work/Task event %s lacks projection admission", admission.eventID)
			}
			var record events.ProjectionRecord
			if err := json.Unmarshal(admission.body, &record); err != nil {
				return err
			}
			event.CorrelationID = "other-run"
			record.CorrelationID = "other-run"
			resealed, err := events.SealProjectionEvent(event, record, payload.Detail)
			if err != nil {
				return err
			}
			eventBody, err := json.Marshal(resealed)
			if err != nil {
				return err
			}
			recordBody, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id=?,payload=? WHERE event_id=?`, event.CorrelationID, eventBody, event.EventID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, resealed.Admission.Fingerprint, event.EventID); err != nil {
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err == nil {
		t.Fatal("accepted Work and Task history detached from its selected Intent")
	}
}

func TestIncidentDiscoversTaskHistoryThroughSelectedWork(t *testing.T) {
	for _, mutation := range []string{"moved", "moved-masked-record-link", "missing-event"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incident.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident", "work-1")
			task := core.Task{ID: "task-1", WorkID: "work-1", Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "incident"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
				t.Fatal(err)
			}
			task.Status = core.TaskBlocked
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_BLOCKED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "incident"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err != nil {
				t.Fatalf("valid Task history was rejected: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				rows, err := tx.QueryContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='task' AND record_id='task-1' ORDER BY version`)
				if err != nil {
					return err
				}
				defer func() { _ = rows.Close() }()
				type admission struct {
					body    []byte
					eventID string
				}
				var admissions []admission
				for rows.Next() {
					var value admission
					if err := rows.Scan(&value.body, &value.eventID); err != nil {
						_ = rows.Close()
						return err
					}
					admissions = append(admissions, value)
				}
				if err := rows.Err(); err != nil {
					_ = rows.Close()
					return err
				}
				if err := rows.Close(); err != nil {
					return err
				}
				for _, admission := range admissions {
					if mutation == "missing-event" {
						if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id=?`, admission.eventID); err != nil {
							return err
						}
						continue
					}
					event, found, err := eventByID(t.Context(), tx, admission.eventID)
					if err != nil {
						return err
					}
					if !found {
						return fmt.Errorf("missing Task admission event %s", admission.eventID)
					}
					payload, present, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					if !present {
						return fmt.Errorf("Task event %s lacks projection admission", admission.eventID)
					}
					var record events.ProjectionRecord
					if err := json.Unmarshal(admission.body, &record); err != nil {
						return err
					}
					event.CorrelationID = "other-run"
					record.CorrelationID = "other-run"
					resealed, err := events.SealProjectionEvent(event, record, payload.Detail)
					if err != nil {
						return err
					}
					if mutation == "moved-masked-record-link" {
						var masked core.Task
						if err := json.Unmarshal(record.Value, &masked); err != nil {
							return err
						}
						masked.WorkID = "masked-work"
						record.Value, err = json.Marshal(masked)
						if err != nil {
							return err
						}
					}
					eventBody, err := json.Marshal(resealed)
					if err != nil {
						return err
					}
					recordBody, err := json.Marshal(record)
					if err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id=?,payload=? WHERE event_id=?`, event.CorrelationID, eventBody, event.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, resealed.Admission.Fingerprint, event.EventID); err != nil {
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
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err == nil {
				t.Fatalf("accepted %s Task history detached from its selected Work", mutation)
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
	incidentTestExecution(t, store)
	appendTaskProjectionParents(t, t.Context(), store, "org-2", "stop-work", "other-work")
	foreign := core.Task{ID: "foreign-task", WorkID: "other-work", Description: "foreign claim", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(foreign.ID), CorrelationID: "stop-work"}, ProjectionKind: "task", RecordID: string(foreign.ID), Version: 1, Value: foreign}); err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var body []byte
		var eventID string
		if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='task' AND record_id=?`, foreign.ID).Scan(&body, &eventID); err != nil {
			return err
		}
		event, found, err := eventByID(t.Context(), tx, eventID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("missing foreign Task event %s", eventID)
		}
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("foreign Task event %s lacks projection admission", eventID)
		}
		var record events.ProjectionRecord
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		foreign.WorkID = "work-1"
		record.Value, err = json.Marshal(foreign)
		if err != nil {
			return err
		}
		event.CorrelationID = "other-run"
		record.CorrelationID = "other-run"
		resealed, err := events.SealProjectionEvent(event, record, payload.Detail)
		if err != nil {
			return err
		}
		eventBody, err := json.Marshal(resealed)
		if err != nil {
			return err
		}
		recordBody, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET correlation_id=?,payload=? WHERE event_id=?`, event.CorrelationID, eventBody, event.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, resealed.Admission.Fingerprint, event.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
		t.Fatal("foreign Task referring to selected global Work was omitted")
	}
}
