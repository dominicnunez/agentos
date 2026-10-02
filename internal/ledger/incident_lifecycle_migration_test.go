package ledger

import (
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestIncidentLifecycleMigration(t *testing.T) {
	for _, mode := range []string{"backfill", "rollback", "old-index-omission", "old-index-extra"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "lifecycle-v14.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					_ = store.Close()
				}
			})
			incidentStopRequest(t, store, "task")
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var started string
			for _, event := range stream {
				if event.EventType == "EXECUTION_STARTED" {
					started = event.EventID
				}
			}
			if started == "" {
				t.Fatal("writer omitted execution start")
			}
			RemoveIncidentAdmissionForTest(t, store, started)
			before, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				for _, object := range incidentLinkObjects {
					if object.kind == "trigger" {
						if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+object.name); err != nil {
							return err
						}
					}
				}
				if _, err := tx.ExecContext(t.Context(), `DROP TABLE incident_event_links; DROP TABLE incident_record_links`); err != nil {
					return err
				}
				if err := createIncidentLinks(t.Context(), tx, 2); err != nil {
					return err
				}
				if err := validateIncidentLinkVersion(t.Context(), tx, 2); err != nil {
					return err
				}
				var guard string
				switch mode {
				case "old-index-omission":
					guard = "incident_event_links_delete_guard"
				case "old-index-extra":
					guard = "incident_event_links_insert_guard"
				}
				if guard != "" {
					if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+guard); err != nil {
						return err
					}
					statement := `DELETE FROM incident_event_links WHERE target_kind='task'`
					if mode == "old-index-extra" {
						statement = `INSERT INTO incident_event_links VALUES('event','invented',1,'invented')`
					}
					if _, err := tx.ExecContext(t.Context(), statement); err != nil {
						return err
					}
					for _, object := range incidentLinkObjects {
						if object.name == guard {
							if _, err := tx.ExecContext(t.Context(), incidentLinkGrammar(object.sql, 2)); err != nil {
								return err
							}
						}
					}
				}
				fingerprint, err := storageSchemaFingerprint(t.Context(), tx)
				if err != nil {
					return err
				}
				_, err = tx.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=14,schema_fingerprint=?; PRAGMA user_version=14`, fingerprint)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if mode == "rollback" {
				interrupted := errors.New("rollback lifecycle migration")
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if err := migrateIncidentLifecycleLinks(t.Context(), tx); err != nil {
						return err
					}
					return interrupted
				}); !errors.Is(err, interrupted) {
					t.Fatalf("migration did not reach rollback: %v", err)
				}
				if err := validateIncidentLinkGrammar(t.Context(), store.db, 2); err != nil {
					t.Fatal(err)
				}
				if err := validateIncidentLinkVersion(t.Context(), store.db, 2); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if mode == "old-index-omission" || mode == "old-index-extra" {
				if err == nil {
					t.Fatal("migration repaired corrupt historical index")
				}
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				var version int
				if err := db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&version); err != nil || version != 14 {
					t.Fatalf("failed migration changed version: %d, %v", version, err)
				}
				if err := validateIncidentLinkGrammar(t.Context(), db, 2); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			after, err := store.Events(t.Context(), "")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("migration changed source history: %v", err)
			}
			if err := validateIncidentLinkContents(t.Context(), store.db); err != nil {
				t.Fatal(err)
			}
			var links int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE event_id=? AND target_kind='event'`, started).Scan(&links); err != nil || links < 3 {
				t.Fatalf("lost dispatch detail links: %d, %v", links, err)
			}
			if _, err := store.db.ExecContext(t.Context(), `DELETE FROM incident_event_links WHERE event_id=? AND target_kind='event'`, started); err == nil {
				t.Fatal("migrated detail links are unguarded")
			}
		})
	}
}
