package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func removeConnectionColumnsForLegacyFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `DROP TRIGGER IF EXISTS freeze_events_delete_change;
DROP TRIGGER IF EXISTS freeze_events_insert_change;
DROP TRIGGER IF EXISTS freeze_events_insert_conflict;
DROP TRIGGER IF EXISTS freeze_events_update_change;
DROP TRIGGER IF EXISTS freeze_records_delete_change;
DROP TRIGGER IF EXISTS freeze_records_insert_change;
DROP TRIGGER IF EXISTS freeze_records_update_change;
DROP TABLE IF EXISTS freeze_changes;
DROP INDEX inference_policies_active_idx;
ALTER TABLE inference_policies DROP COLUMN connection_id;
ALTER TABLE inference_reservations DROP COLUMN connection_id;
CREATE UNIQUE INDEX inference_policies_active_idx ON inference_policies(organization_id) WHERE active=1;`); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceConnectionMigrationPreservesLegacyReservation(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "legacy-inference.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if err := store.ActivateInferencePolicy(ctx, testInferencePolicy(now)); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(ctx, testInferenceRequest("legacy"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Events(ctx, "work-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the exact v9 layout; do not pretend that relabeling v10 is v9.
	removeConnectionColumnsForLegacyFixture(t, db)
	fingerprint, err := storageSchemaFingerprint(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agentos_storage SET storage_version=9,schema_fingerprint=?; PRAGMA user_version=9`, fingerprint); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateStorageContract(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopened.now = func() time.Time { return now }
	after, err := reopened.Events(ctx, "work-1")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("migration changed legacy events: %v", err)
	}
	var connection string
	if err := reopened.db.QueryRowContext(ctx, `SELECT connection_id FROM inference_reservations WHERE reservation_id=?`, reservation.ID).Scan(&connection); err != nil || connection != "" {
		t.Fatalf("legacy connection was invented: %q %v", connection, err)
	}
	if err := reopened.ValidateInferenceAdmissions(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.db.ExecContext(ctx, `UPDATE inference_reservations SET connection_id='substituted' WHERE reservation_id=?`, reservation.ID); err != nil {
		t.Fatal(err)
	}
	if err := reopened.ValidateInferenceAdmissions(ctx); err == nil {
		t.Fatal("substituted stored connection accepted")
	}
	if _, err := reopened.ReconcileInference(ctx, reservation, nil, inference.ReconciliationNotSent); err == nil {
		t.Fatal("substituted stored connection reconciled")
	}
	if _, err := reopened.db.ExecContext(ctx, `UPDATE inference_reservations SET connection_id='' WHERE reservation_id=?`, reservation.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ReconcileInference(ctx, reservation, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	if err := reopened.ValidateInferenceAdmissions(ctx); err != nil {
		t.Fatal(err)
	}
}
