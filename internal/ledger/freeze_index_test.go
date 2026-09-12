package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func freezeV10Fixture(t *testing.T) (string, []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "storage-v10.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body, err := json.Marshal(core.FreezeState{OrganizationID: "org-1", Frozen: true, UpdatedAt: time.Unix(1, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		return appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "old-runtime", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", 1, body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DROP INDEX records_freeze_control_idx`); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := storageSchemaFingerprint(t.Context(), store.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=10,schema_fingerprint=?; PRAGMA user_version=10`, fingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStorageContract(t.Context(), store.db); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return path, body
}

func TestFreezeIndexMigration(t *testing.T) {
	path, body := freezeV10Fixture(t)
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	contract, err := ValidateStorageContract(t.Context(), store.db)
	if err != nil || contract.StorageVersion != 11 {
		t.Fatalf("migration contract: %+v %v", contract, err)
	}
	rows, err := store.Records(t.Context(), "organization_freeze", "org-1")
	if err != nil || len(rows) != 1 || string(rows[0]) != string(body) {
		t.Fatalf("migration rewrote legacy authority: %v", err)
	}
	state, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || !state.State.Frozen || state.State.Control != nil {
		t.Fatalf("legacy hold changed: %+v %v", state, err)
	}
	for _, query := range []string{
		`SELECT MIN(version) FROM records WHERE kind='organization_freeze' AND record_id=? AND ` + freezeControlPresent + `=1 AND version<=?`,
		`SELECT MAX(version) FROM records WHERE kind='organization_freeze' AND record_id=? AND ` + freezeControlPresent + `=0 AND version<=?`,
	} {
		var id, parent, unused int
		var plan string
		if err := store.db.QueryRowContext(t.Context(), "EXPLAIN QUERY PLAN "+query, "org-1", 99999).Scan(&id, &parent, &unused, &plan); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(plan, "records_freeze_control_idx") || !strings.Contains(plan, "<expr>=?") {
			t.Fatalf("control lookup scans history: %s", plan)
		}
	}
}

func TestFreezeIndexMigrationFailsAtomically(t *testing.T) {
	path, _ := freezeV10Fixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), `UPDATE records SET body='invalid-json' WHERE kind='organization_freeze'`); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(path); err == nil {
		_ = store.Close()
		t.Fatal("migration accepted corrupt freeze history")
	}
	var version, indexCount int
	if err := db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema WHERE name='records_freeze_control_idx'`).Scan(&indexCount); err != nil {
		t.Fatal(err)
	}
	if version != 10 || indexCount != 0 {
		t.Fatalf("failed migration changed storage contract: version=%d index=%d", version, indexCount)
	}
}
