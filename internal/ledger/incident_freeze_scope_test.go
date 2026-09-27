package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentFreezePayloadScope(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "owner"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "freeze.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			modelStopManifest(t, store, false, "planning-1")
			if legacy {
				appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
			} else {
				appendInferenceFreeze(t, store, "org-1", 1, true)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err != nil {
				t.Fatalf("baseline: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='organization_freeze'`); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id='foreign-org',correlation_id='model-stop' WHERE event_type='FREEZE_SET'`); err != nil {
					return err
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
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := events.ResolveAuthorityAdmissions(stream, nil); err == nil {
				t.Fatal("full recovery accepted orphan hold")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
			if err == nil {
				t.Fatal("incident omitted payload-linked orphan hold")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("returned partial evidence")
			}
		})
	}
}

func TestIncidentFreezeClaimVariants(t *testing.T) {
	for _, mode := range []string{"record", "duplicate-event", "duplicate-record", "unrelated-event", "unrelated-record"} {
		t.Run(mode, func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "freeze.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			modelStopManifest(t, store, false, "planning-1")
			appendInferenceFreeze(t, store, "org-1", 1, true)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if strings.Contains(mode, "record") {
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_type='FREEZE_SET'`); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET record_id='foreign-org' WHERE kind='organization_freeze'`); err != nil {
						return err
					}
					if mode == "duplicate-record" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body='{"organization_id":"foreign-org","organization_id":"org-1"}' WHERE kind='organization_freeze'`); err != nil {
							return err
						}
					}
					if mode == "unrelated-record" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=json_set(body,'$.organization_id','foreign-org') WHERE kind='organization_freeze'`); err != nil {
							return err
						}
					}
				} else {
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='organization_freeze'`); err != nil {
						return err
					}
					payload := `{"organization_id":"foreign-org","organization_id":"org-1"}`
					if mode == "unrelated-event" {
						payload = `{"organization_id":"foreign-org"}`
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id='foreign-org',payload=? WHERE event_type='FREEZE_SET'`, []byte(payload)); err != nil {
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
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
			if strings.HasPrefix(mode, "unrelated") {
				if err != nil {
					t.Fatalf("unrelated foreign evidence affected selected incident: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("omitted selected organization claim")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("returned partial evidence")
			}
		})
	}
}
