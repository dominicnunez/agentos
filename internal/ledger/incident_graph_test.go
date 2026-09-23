package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentRejectsInvalidTaskGraph(t *testing.T) {
	for _, mutation := range []string{"missing dependency", "missing parent", "cycle"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "graph.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident", "work-1")
			tasks := []core.Task{
				{ID: "task-1", WorkID: "work-1", Description: "first", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending},
				{ID: "task-2", WorkID: "work-1", Description: "second", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending},
			}
			for _, task := range tasks {
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "incident"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "missing dependency":
				tasks[0].DependsOn = []core.ID{"absent"}
			case "missing parent":
				tasks[0].ParentID = "absent"
			case "cycle":
				tasks[0].DependsOn = []core.ID{tasks[1].ID}
				tasks[1].DependsOn = []core.ID{tasks[0].ID}
			}
			// Preserve valid seals, backing records and integrity so only the graph is invalid.
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				for _, task := range tasks {
					var body []byte
					var eventID string
					if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='task' AND record_id=?`, task.ID).Scan(&body, &eventID); err != nil {
						return err
					}
					var record events.ProjectionRecord
					if err := json.Unmarshal(body, &record); err != nil {
						return err
					}
					record.Value, err = json.Marshal(task)
					if err != nil {
						return err
					}
					event, _, err := eventByID(t.Context(), tx, eventID)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(event, record, nil)
					if err != nil {
						return err
					}
					encoded, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					body, err = json.Marshal(record)
					if err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, encoded, eventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, eventID); err != nil {
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
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident", 256); err == nil {
				t.Fatal("accepted invalid retained Task graph after reopen")
			}
		})
	}
}
