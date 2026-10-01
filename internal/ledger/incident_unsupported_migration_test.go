package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

// Historical v1 indexed arbitrary projection kinds. Current discovery accepts
// only the closed admitted kinds, so an unsupported raw source holds upgrade
// rather than silently dropping its verified old index row or rewriting it.
func TestIncidentUnsupportedV13Rollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsupported-v13.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	source, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "legacy", Payload: map[string]string{"note": "raw legacy source"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user", CorrelationID: "intake", TaskID: "intake-task", Payload: events.IntakeMessageRecordedPayload{MessageID: "retained-intake", Text: "Preserve the source bytes", SourcePrincipalID: "user", SourcePrincipalKind: "HUMAN", SourceChannel: "HUMAN_DIRECT"}}); err != nil {
		t.Fatal(err)
	}
	var fingerprint string
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		for _, object := range incidentLinkObjects {
			if object.kind == "trigger" {
				if _, err := tx.ExecContext(t.Context(), "DROP TRIGGER "+object.name); err != nil {
					return err
				}
			}
		}
		raw := []byte(` {"projection":{"projection_kind":"unsupported_legacy_kind","record_id":"retained-id","value":{}}} `)
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, raw, source.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		if err := rebuildEventIntegrity(t.Context(), tx); err != nil {
			return err
		}
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
		var err error
		fingerprint, err = storageSchemaFingerprint(t.Context(), tx)
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path)
	if opened != nil {
		_ = opened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "incident link contents") {
		t.Fatalf("unsupported v13 source did not hold migration: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	after, err := collectEvents(db.QueryContext(t.Context(), `SELECT `+incidentEventColumns+` FROM events ORDER BY sequence`))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("failed upgrade changed retained source bytes or event metadata")
	}
	var pragma, version, links, oldLinks, records int
	var storedFingerprint string
	if err := db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&pragma); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT storage_version,schema_fingerprint FROM agentos_storage`).Scan(&version, &storedFingerprint); err != nil {
		t.Fatal(err)
	}
	if pragma != 13 || version != 13 || storedFingerprint != fingerprint {
		t.Fatalf("failed upgrade changed storage contract: pragma=%d version=%d fingerprint=%s", pragma, version, storedFingerprint)
	}
	actual, err := storageSchemaFingerprint(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if actual != fingerprint {
		t.Fatal("failed upgrade changed schema objects")
	}
	if err := validateIncidentLinkGrammar(t.Context(), db, 1); err != nil {
		t.Fatal(err)
	}
	if err := validateIncidentLinkVersion(t.Context(), db, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links`).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_event_links WHERE target_kind='unsupported_legacy_kind' AND target_id='retained-id' AND event_id=? AND event_sequence=?`, source.EventID, source.Sequence).Scan(&oldLinks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM incident_record_links`).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if links != 1 || oldLinks != 1 || records != 0 {
		t.Fatalf("failed upgrade changed verified v1 index or retained new intake backfill: events=%d old=%d records=%d", links, oldLinks, records)
	}
	if _, err := ValidateEventIntegrity(t.Context(), db); err != nil {
		t.Fatal(err)
	}
}
