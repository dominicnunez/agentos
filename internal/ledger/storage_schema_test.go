package ledger

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestOpenBootstrapsCurrentStorageContract(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	contract, err := ValidateStorageContract(t.Context(), store.db)
	if err != nil {
		t.Fatal(err)
	}
	if contract.StorageVersion != CurrentStorageVersion || contract.EventSchemaVersion != events.SchemaVersion {
		t.Fatalf("storage contract=%+v", contract)
	}
}

func TestStorageV1FixtureMatchesFrozenFingerprint(t *testing.T) {
	db := createStorageV1Fixture(t, filepath.Join(t.TempDir(), "storage-v1.db"))
	defer func() { _ = db.Close() }()
	fingerprint, err := storageSchemaFingerprint(t.Context(), db)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint != storageSchemaV1Fingerprint {
		t.Fatalf("storage v1 fingerprint=%s", fingerprint)
	}
}

func TestOpenMigratesStorageV1FixtureWithoutRewritingLedger(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "storage-v1.db")
	legacy := createStorageV1Fixture(t, path)
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "policy-v1", CreatedAt: time.Now().UTC()}
	if err := insertLegacyProjection(ctx, &SQLite{db: legacy}, "ORGANIZATION_CREATED", "organization", string(organization.ID), "", organization); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	var beforePayload []byte
	if err := legacy.QueryRowContext(ctx, `SELECT payload FROM events WHERE sequence=1`).Scan(&beforePayload); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := ValidateStorageContract(ctx, store.db)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if contract.StorageVersion != CurrentStorageVersion || contract.EventSchemaVersion != events.SchemaVersion {
		_ = store.Close()
		t.Fatalf("migrated contract=%+v", contract)
	}
	var afterPayload []byte
	if err := store.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE sequence=1`).Scan(&afterPayload); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if string(afterPayload) != string(beforePayload) {
		_ = store.Close()
		t.Fatal("storage migration rewrote the authoritative Event payload")
	}
	records, err := store.Records(ctx, "organization", string(organization.ID))
	if err != nil || len(records) != 1 {
		_ = store.Close()
		t.Fatalf("migrated projection records=%d err=%v", len(records), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	stream, err := restarted.Events(ctx, "legacy-request")
	if err != nil || len(stream) != 1 || stream[0].OrganizationID != "org-1" {
		t.Fatalf("restarted migrated stream=%+v err=%v", stream, err)
	}
}

func TestStorageMigrationFailsAtomicallyOnAmbiguousV1Layout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt-v1.db")
	legacy := createStorageV1Fixture(t, path)
	if _, err := legacy.ExecContext(t.Context(), `DROP INDEX records_admission_event_idx`); err != nil {
		_ = legacy.Close()
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "lacks exact index") {
		t.Fatalf("ambiguous v1 layout was not rejected: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var version, metadataTables int
	if err := db.QueryRowContext(t.Context(), `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='agentos_storage'`).Scan(&metadataTables); err != nil {
		t.Fatal(err)
	}
	if version != 1 || metadataTables != 0 {
		t.Fatalf("failed migration partially mutated storage: version=%d metadata=%d", version, metadataTables)
	}
}

func TestOpenRejectsUnsupportedOrMismatchedStorageContract(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB)
		want   string
	}{
		{
			name: "wrong application id",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `PRAGMA application_id=7`); err != nil {
					t.Fatal(err)
				}
			},
			want: "is not Agent OS",
		},
		{
			name: "future storage version",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `PRAGMA user_version=99`); err != nil {
					t.Fatal(err)
				}
			},
			want: "newer than supported",
		},
		{
			name: "event contract mismatch",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `UPDATE agentos_storage SET event_schema_version=event_schema_version+1`); err != nil {
					t.Fatal(err)
				}
			},
			want: "metadata does not match",
		},
		{
			name: "schema fingerprint drift",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `CREATE INDEX unreviewed_events_type_idx ON events(event_type)`); err != nil {
					t.Fatal(err)
				}
			},
			want: "schema fingerprint does not match",
		},
		{
			name: "durable event schema mismatch",
			mutate: func(t *testing.T, db *sql.DB) {
				if _, err := db.ExecContext(t.Context(), `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('future-event','org-1','AUDIT_NOTE','runtime',?,?,?,'2026-08-13T12:00:00Z',?)`, []byte("[]"), []byte("[]"), []byte("{}"), events.SchemaVersion+1); err != nil {
					t.Fatal(err)
				}
			},
			want: "outside supported Event Contract",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "agentos.db")
			store, err := Open(path)
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
			test.mutate(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("mismatched storage contract error=%v", err)
			}
		})
	}
}

func createStorageV1Fixture(t *testing.T, path string) *sql.DB {
	t.Helper()
	script, err := os.ReadFile(filepath.Join("testdata", "storage-v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), string(script)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}
