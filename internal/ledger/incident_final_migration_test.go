package ledger

import (
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// These raw grammar fixtures test persistence/backfill, not valid admission.
// Admission validity is covered by the writer-derived identity regressions.
func TestIncidentFinalLinkMigration(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		name := "backfill"
		if rollback {
			name = "rollback-reopen"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "final-links.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					_ = store.Close()
				}
			})
			fixtures := []struct {
				event, payload string
				targets        [][2]string
			}{
				{"EXECUTION_CONTEXT_MANIFESTED", `{"task_id":"manifest-task","agent_id":"manifest-agent","knowledge_refs":[{"id":"manifest-knowledge"}],"coordination_refs":[{"id":"manifest-peer"}],"additional_context_refs":[{"id":"mission/manifest-mission"},{"id":"goal/manifest-goal"}],"event_refs":["manifest-evidence"]}`, [][2]string{{"task", "manifest-task"}, {"agent", "manifest-agent"}, {"knowledge", "manifest-knowledge"}, {"task", "manifest-peer"}, {"mission", "manifest-mission"}, {"goal", "manifest-goal"}, {"event", "manifest-evidence"}}},
				{"EXECUTION_STARTED", `{"projection":{"projection_kind":"task","record_id":"start-task","value":{}},"detail":{"strategic_context_refs":[{"id":"mission/start-mission"},{"id":"goal/start-goal"}],"strategic_event_refs":["start-evidence"]}}`, [][2]string{{"mission", "start-mission"}, {"goal", "start-goal"}, {"event", "start-evidence"}}},
				{"WORK_COMPLETION_EVALUATED", `{"work_id":"aggregate-work","intent_id":"aggregate-intent","tasks":[{"task_id":"aggregate-task","verification_event_ref":"aggregate-verification","completion_event_ref":"aggregate-completion"}]}`, [][2]string{{"work", "aggregate-work"}, {"intent", "aggregate-intent"}, {"task", "aggregate-task"}, {"event", "aggregate-verification"}, {"event", "aggregate-completion"}}},
				{"GOAL_PROGRESS_EVALUATED", `{"mission_id":"aggregate-mission","work_evidence_refs":["goal-evidence"],"criteria":[{"work_evidence_refs":["criterion-evidence"]}]}`, [][2]string{{"mission", "aggregate-mission"}, {"event", "goal-evidence"}, {"event", "criterion-evidence"}}},
				{"COMPLETION_REVIEW_REQUESTED", `{"task_id":"request-task","evidence_refs":["request-evidence"]}`, [][2]string{{"task", "request-task"}, {"event", "request-evidence"}}},
				{"COMPLETION_REVIEW_DECIDED", `{"task_id":"decision-task","evidence_refs":["decision-evidence"]}`, [][2]string{{"task", "decision-task"}, {"event", "decision-evidence"}}},
			}
			ids := make([]string, len(fixtures))
			for i, fixture := range fixtures {
				event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "migration", Payload: map[string]string{"note": "raw grammar fixture"}})
				if err != nil {
					t.Fatal(err)
				}
				ids[i] = event.EventID
				if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type=?,payload=? WHERE event_id=?`, fixture.event, []byte(fixture.payload), event.EventID); err != nil {
					t.Fatal(err)
				}
			}
			recordBody := []byte(`{"value":{"provenance_event_refs":["record-provenance"],"occurrence_event_refs":["record-occurrence"],"validation_refs":["record-validation"],"derived_knowledge_refs":[{"id":"old-derived"}]}}`)
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,admission_event_id,created_at) VALUES('knowledge','migration-record',1,?,'raw-admission','2026-09-01T00:00:00Z')`, recordBody); err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				if err := rebuildEventIntegrity(t.Context(), tx); err != nil {
					return err
				}
				for _, object := range incidentLinkObjects {
					if object.kind == "trigger" {
						if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+object.name); err != nil {
							return err
						}
					}
				}
				// Rebuild every old index row from immutable v1, including non-event kinds.
				if _, err := tx.ExecContext(t.Context(), `DROP TABLE incident_event_links; DROP TABLE incident_record_links`); err != nil {
					return err
				}
				if err := createIncidentLinks(t.Context(), tx, 1); err != nil {
					return err
				}
				if err := validateIncidentLinkGrammar(t.Context(), tx, 1); err != nil {
					return err
				}
				if err := validateIncidentLinkVersion(t.Context(), tx, 1); err != nil {
					return err
				}
				fingerprint, err := storageSchemaFingerprint(t.Context(), tx)
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=13,schema_fingerprint=?; PRAGMA user_version=13`, fingerprint)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			before, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			checkNew := func(want int) {
				t.Helper()
				var retained int
				if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_record_links WHERE record_id='migration-record' AND target_kind='knowledge' AND target_id='old-derived'`).Scan(&retained); err != nil {
					t.Fatal(err)
				}
				if retained != 1 {
					t.Fatalf("immutable v1 relationship was not retained: %d", retained)
				}
				for i, fixture := range fixtures {
					for _, target := range fixture.targets {
						var count int
						if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE event_id=? AND target_kind=? AND target_id=?`, ids[i], target[0], target[1]).Scan(&count); err != nil {
							t.Fatal(err)
						}
						if count != want {
							t.Fatalf("%s %s/%s: got %d links, want %d", fixture.event, target[0], target[1], count, want)
						}
					}
				}
				for _, target := range []string{"record-provenance", "record-occurrence", "record-validation"} {
					var count int
					if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_record_links WHERE record_id='migration-record' AND target_kind='event' AND target_id=?`, target).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != want {
						t.Fatalf("record %s: got %d links, want %d", target, count, want)
					}
				}
			}
			checkNew(0)
			if rollback {
				interrupted := errors.New("interrupt final link migration")
				err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if err := migrateIncidentEvidenceLinks(t.Context(), tx); err != nil {
						return err
					}
					return interrupted
				})
				if !errors.Is(err, interrupted) {
					t.Fatalf("migration did not reach rollback: %v", err)
				}
				if err := validateIncidentLinkGrammar(t.Context(), store.db, 1); err != nil {
					t.Fatal(err)
				}
				if err := validateIncidentLinkVersion(t.Context(), store.db, 1); err != nil {
					t.Fatal(err)
				}
				var version int
				if err := store.db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&version); err != nil {
					t.Fatal(err)
				}
				if version != 13 {
					t.Fatalf("rollback changed version to %d", version)
				}
				checkNew(0)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			checkNew(1)
			if err := validateIncidentLinkGrammar(t.Context(), store.db, 2); err != nil {
				t.Fatal(err)
			}
			if err := validateIncidentLinkContents(t.Context(), store.db); err != nil {
				t.Fatal(err)
			}
			after, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("migration changed retained event bytes or metadata")
			}
			var afterBody []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='knowledge' AND record_id='migration-record'`).Scan(&afterBody); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(recordBody, afterBody) {
				t.Fatal("migration changed retained record bytes")
			}
		})
	}
}
