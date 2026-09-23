package ledger

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIndexQueryPlans(t *testing.T) {
	for _, mode := range []string{"fresh", "upgrade-v12"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "replacement-index.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					_ = store.Close()
				}
			})
			if mode == "upgrade-v12" {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				if _, err := db.ExecContext(t.Context(), `DROP INDEX IF EXISTS records_replaced_work_idx; DROP INDEX IF EXISTS events_incident_execution_idx; DROP INDEX IF EXISTS events_message_idx; DROP INDEX IF EXISTS events_source_message_idx;
INSERT INTO events(event_id,organization_id,event_type,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('malformed-intake','org-1','INTAKE_MESSAGE_RECORDED','[]','[]','{','2026-09-22T00:00:00Z',:schema);
INSERT INTO records(kind,record_id,version,body,created_at) VALUES('retained','malformed',1,:body,'2026-09-22T00:00:00Z');`, sql.Named("schema", events.SchemaVersion), sql.Named("body", []byte(`{`))); err != nil {
					t.Fatal(err)
				}
				fingerprint, err := storageSchemaFingerprint(t.Context(), db)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=12,schema_fingerprint=?; PRAGMA user_version=12`, fingerprint); err != nil {
					t.Fatal(err)
				}
				if contract, err := ValidateStorageContract(t.Context(), db); err != nil || contract.StorageVersion != 12 {
					t.Fatalf("pre-upgrade contract=%+v err=%v", contract, err)
				}
				tx, err := db.BeginTx(t.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := rebuildEventIntegrity(t.Context(), tx); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				var intakeBody string
				if err := store.db.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_id='malformed-intake'`).Scan(&intakeBody); err != nil || intakeBody != "{" {
					t.Fatalf("migration changed retained intake bytes: %q %v", intakeBody, err)
				}
				var body []byte
				if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='retained' AND record_id='malformed'`).Scan(&body); err != nil || string(body) != "{" {
					t.Fatalf("migration changed retained bytes: %q %v", body, err)
				}
			}
			contract, err := ValidateStorageContract(t.Context(), store.db)
			if err != nil || contract.StorageVersion != CurrentStorageVersion {
				t.Fatalf("current contract=%+v err=%v", contract, err)
			}
			for _, kind := range []string{"work", "intent"} {
				for field, index := range map[string]string{"message_id": "events_message_idx", "source_message_id": "events_source_message_idx"} {
					plan := incidentIndexPlan(t, store.db, `SELECT event_id FROM events WHERE organization_id=? AND event_type IN (`+incidentIntakeTypes+`) AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.`+field+`') END=?`, "org-1", "message")
					if !strings.Contains(plan, index) || !strings.Contains(plan, "<expr>=?") {
						t.Fatalf("intake lookup does not seek message: %s", plan)
					}
				}
				joined := incidentIndexPlan(t, store.db, `SELECT record_id FROM records WHERE kind=? AND CASE WHEN json_valid(body) THEN json_extract(body,'$.value.replaces_work_id') END=? AND version=1`, kind, "predecessor")
				if !strings.Contains(joined, "records_replaced_work_idx") || !strings.Contains(joined, "<expr>=?") {
					t.Fatalf("%s reverse lookup does not use replacement expression index: %s", kind, joined)
				}
			}
			reverse := incidentIndexPlan(t, store.db, `SELECT record_id FROM records WHERE kind IN ('work','intent') AND CASE WHEN json_valid(body) THEN json_extract(body,'$.value.replaces_work_id') END=?`, "predecessor")
			if !strings.Contains(reverse, "records_replaced_work_idx") || !strings.Contains(reverse, "<expr>=?") {
				t.Fatalf("combined reverse lookup does not seek replacement identity: %s", reverse)
			}
			joined := incidentIndexPlan(t, store.db, `SELECT event_id FROM events WHERE organization_id=? AND source_execution_id=? ORDER BY sequence`, "org-1", "execution-1")
			if !strings.Contains(joined, "events_incident_execution_idx") || !strings.Contains(joined, "organization_id=? AND source_execution_id=?") {
				t.Fatalf("execution lookup does not seek the complete execution: %s", joined)
			}
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES('retained','malformed-new',1,?,'2026-09-22T00:00:00Z')`, []byte(`{`)); err != nil {
				t.Fatalf("index rejects retained malformed JSON before validation: %v", err)
			}
		})
	}
}

func incidentIndexPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(plan, "; ")
}

func TestIncidentIndexesRequired(t *testing.T) {
	for _, index := range []string{"records_replaced_work_idx", "events_incident_execution_idx", "events_message_idx", "events_source_message_idx"} {
		t.Run(index, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing-index.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), "DROP INDEX "+index); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err == nil {
				_ = store.Close()
				t.Fatal("current storage accepted missing required incident index")
			}
			if !strings.Contains(err.Error(), "lacks incident index "+index) {
				t.Fatalf("unexpected rejection: %v", err)
			}
		})
	}
}
