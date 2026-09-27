package ledger

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEvidenceMigration(t *testing.T) {
	for _, mode := range []string{"backfill", "old-index-omission", "old-index-extra", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			corrupt := strings.HasPrefix(mode, "old-index-")
			path := filepath.Join(t.TempDir(), "evidence-v13.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					_ = store.Close()
				}
			})
			correlation := appendDerivedIncidentChain(t, store, 1)
			before, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				for _, object := range incidentLinkObjects {
					if object.kind != "trigger" {
						continue
					}
					if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+object.name); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), incidentLinkGrammar(object.sql, 1)); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM incident_event_links WHERE target_kind='event'; DELETE FROM incident_record_links WHERE target_kind='event'`); err != nil {
					return err
				}
				if err := validateIncidentLinkVersion(t.Context(), tx, 1); err != nil {
					return err
				}
				if mode == "old-index-omission" {
					if _, err := tx.ExecContext(t.Context(), `DROP TRIGGER incident_event_links_delete_guard; DELETE FROM incident_event_links WHERE target_kind='knowledge' AND target_id='derived-0'`); err != nil {
						return err
					}
					for _, object := range incidentLinkObjects {
						if object.name == "incident_event_links_delete_guard" {
							if _, err := tx.ExecContext(t.Context(), incidentLinkGrammar(object.sql, 1)); err != nil {
								return err
							}
						}
					}
				}
				if mode == "old-index-extra" {
					if _, err := tx.ExecContext(t.Context(), `DROP TRIGGER incident_event_links_insert_guard; INSERT INTO incident_event_links VALUES('knowledge','invented',1,'invented')`); err != nil {
						return err
					}
					for _, object := range incidentLinkObjects {
						if object.name == "incident_event_links_insert_guard" {
							if _, err := tx.ExecContext(t.Context(), incidentLinkGrammar(object.sql, 1)); err != nil {
								return err
							}
						}
					}
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
			if mode == "rollback" {
				interrupted := errors.New("rollback evidence migration")
				err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if err := migrateIncidentEvidenceLinks(t.Context(), tx); err != nil {
						return err
					}
					return interrupted
				})
				if !errors.Is(err, interrupted) {
					t.Fatalf("migration did not reach transactional rollback: %v", err)
				}
				if err := validateIncidentLinkGrammar(t.Context(), store.db, 1); err != nil {
					t.Fatal(err)
				}
				if err := validateIncidentLinkVersion(t.Context(), store.db, 1); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if corrupt {
				if err == nil || !strings.Contains(err.Error(), "incident link contents") {
					t.Fatalf("migration repaired corrupt v13 index: %v", err)
				}
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				var version, links int
				if err := db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&version); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE target_kind='event'`).Scan(&links); err != nil {
					t.Fatal(err)
				}
				if version != 13 || links != 0 {
					t.Fatalf("failed migration changed state: version %d, new links %d", version, links)
				}
				if err := validateIncidentLinkGrammar(t.Context(), db, 1); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			after, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("migration changed authoritative event history")
			}
			if err := validateIncidentLinkContents(t.Context(), store.db); err != nil {
				t.Fatal(err)
			}
			var eventLinks, recordLinks int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE target_kind='event'`).Scan(&eventLinks); err != nil {
				t.Fatal(err)
			}
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_record_links WHERE target_kind='event'`).Scan(&recordLinks); err != nil {
				t.Fatal(err)
			}
			if eventLinks != 4 || recordLinks != 4 {
				t.Fatalf("incomplete evidence backfill: events %d, records %d", eventLinks, recordLinks)
			}
			if _, err := store.db.ExecContext(t.Context(), `DELETE FROM incident_record_links WHERE target_kind='event'`); err == nil {
				t.Fatal("migrated evidence links are unguarded")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestIncidentDetailLinkMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "detail-v13.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	request := incidentStopRequest(t, store, "task")
	before, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var started events.Event
	var detail events.ExecutionStartDetail
	for _, event := range before {
		if event.EventType != "EXECUTION_STARTED" {
			continue
		}
		started = event
		payload, present, err := events.AdmittedProjection(event)
		if err != nil || !present || json.Unmarshal(payload.Detail, &detail) != nil || detail.DispatchBinding == nil {
			t.Fatalf("invalid fixture execution start: %v", err)
		}
	}
	if started.EventID == "" {
		t.Fatal("fixture lacks Agent execution start")
	}
	refs := []string{detail.DispatchBinding.AgentEventRef, detail.DispatchBinding.BlueprintEventRef, detail.DispatchBinding.ExecutionProfileEventRef}
	for _, ref := range refs {
		if ref == "" {
			t.Fatal("fixture lacks exact dispatch admission reference")
		}
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		for _, object := range incidentLinkObjects {
			if object.kind != "trigger" {
				continue
			}
			if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+object.name); err != nil {
				return err
			}
			if _, err := tx.ExecContext(t.Context(), incidentLinkGrammar(object.sql, 1)); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM incident_event_links WHERE target_kind='event'; DELETE FROM incident_record_links WHERE target_kind='event'`); err != nil {
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("detail-link migration changed authoritative event history")
	}
	for _, ref := range refs {
		var count int
		if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE target_kind='event' AND target_id=? AND event_id=?`, ref, started.EventID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("missing migrated Agent dispatch event link %s: %d", ref, count)
		}
	}
	var stopLinks int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE target_kind='event' AND target_id=? AND event_id=?`, started.EventID, request.EventID).Scan(&stopLinks); err != nil {
		t.Fatal(err)
	}
	if stopLinks != 1 {
		t.Fatalf("missing migrated stop request reference: %d", stopLinks)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", started.CorrelationID, 256); err != nil {
		t.Fatalf("migrated Agent execution incident: %v", err)
	}
}
