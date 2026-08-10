package recovery

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestBackupAndRestorePreserveSnapshotWithoutOverwriting(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	source := filepath.Join(directory, "live.db")
	live := createTestLedger(t, source)
	t.Cleanup(func() { _ = live.Close() })
	if _, err := live.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err := live.ExecContext(ctx, `INSERT INTO events(event_id,payload) VALUES('event-1','first')`); err != nil {
		t.Fatal(err)
	}

	backupPath := filepath.Join(directory, "backup.db")
	backup, err := Backup(ctx, source, backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if backup.Path != backupPath || backup.EventCount != 1 || backup.MaxSequence != 1 || backup.SHA256 == "" || backup.SizeBytes == 0 {
		t.Fatalf("backup=%+v", backup)
	}
	if _, err := live.ExecContext(ctx, `INSERT INTO events(event_id,payload) VALUES('event-2','second')`); err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(ctx, backupPath)
	if err != nil || verified.EventCount != 1 || verified.MaxSequence != 1 {
		t.Fatalf("verified=%+v err=%v", verified, err)
	}

	protected := filepath.Join(directory, "protected.db")
	if err := os.WriteFile(protected, []byte("do-not-replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(ctx, backupPath, protected); err == nil {
		t.Fatal("restore overwrote an existing destination")
	}
	content, err := os.ReadFile(protected)
	if err != nil || string(content) != "do-not-replace" {
		t.Fatalf("protected destination changed: content=%q err=%v", content, err)
	}

	restoredPath := filepath.Join(directory, "restored.db")
	restored, err := Restore(ctx, backupPath, restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Path != restoredPath || restored.EventCount != 1 || restored.MaxSequence != 1 {
		t.Fatalf("restored=%+v", restored)
	}
	restoredDB, err := sql.Open("sqlite", restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restoredDB.Close() }()
	var payload string
	if err := restoredDB.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_id='event-1'`).Scan(&payload); err != nil || payload != "first" {
		t.Fatalf("restored payload=%q err=%v", payload, err)
	}
	var eventCount int
	if err := restoredDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("restored event count=%d err=%v", eventCount, err)
	}
}

func TestRecoveryRejectsCorruptionWrongSchemaAndCancelledPublication(t *testing.T) {
	directory := t.TempDir()
	corrupt := filepath.Join(directory, "corrupt.db")
	if err := os.WriteFile(corrupt, []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), corrupt); err == nil {
		t.Fatal("corrupt database passed verification")
	}

	wrongSchema := filepath.Join(directory, "wrong-schema.db")
	db, err := sql.Open("sqlite", wrongSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE unrelated(value TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), wrongSchema); err == nil {
		t.Fatal("non-Agent OS SQLite database passed verification")
	}

	incompleteSchema := filepath.Join(directory, "incomplete-schema.db")
	incompleteDB := createTestLedger(t, incompleteSchema)
	if _, err := incompleteDB.Exec(`ALTER TABLE inbox DROP COLUMN organization_id`); err != nil {
		t.Fatal(err)
	}
	if err := incompleteDB.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(context.Background(), incompleteSchema); err == nil {
		t.Fatal("Agent OS ledger with a missing required column passed verification")
	}

	source := filepath.Join(directory, "source.db")
	sourceDB := createTestLedger(t, source)
	if err := sourceDB.Close(); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	destination := filepath.Join(directory, "cancelled.db")
	if _, err := Backup(cancelled, source, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled backup error=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled backup published destination: %v", err)
	}
}

func TestBackupRefusesExistingDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	db := createTestLedger(t, source)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "existing.db")
	if err := os.WriteFile(destination, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Backup(context.Background(), source, destination); err == nil {
		t.Fatal("backup overwrote an existing destination")
	}
	content, err := os.ReadFile(destination)
	if err != nil || string(content) != "keep" {
		t.Fatalf("existing destination changed: content=%q err=%v", content, err)
	}
}

func createTestLedger(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE events(
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL DEFAULT '',
event_type TEXT NOT NULL DEFAULT '', source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '',
recipient_scope TEXT NOT NULL DEFAULT '', recipient_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '',
authorization_refs BLOB NOT NULL DEFAULT '[]', artifact_refs BLOB NOT NULL DEFAULT '[]', payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '', schema_version INTEGER NOT NULL DEFAULT 1);
CREATE TABLE records(kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL, created_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(kind, record_id, version));
CREATE TABLE inbox(recipient_scope TEXT NOT NULL, recipient_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', available_at TEXT NOT NULL DEFAULT '', observed_at TEXT NOT NULL DEFAULT '', observation_event_id TEXT NOT NULL DEFAULT '');
CREATE TABLE consumed_approvals(approval_id TEXT PRIMARY KEY, effect_fingerprint TEXT NOT NULL, consumed_at TEXT NOT NULL);
CREATE TABLE external_work(organization_id TEXT NOT NULL, request_id TEXT NOT NULL, correlation_id TEXT NOT NULL, intent_id TEXT NOT NULL, PRIMARY KEY(organization_id, request_id));
CREATE TABLE external_tasks(organization_id TEXT NOT NULL, task_id TEXT NOT NULL, correlation_id TEXT NOT NULL, PRIMARY KEY(organization_id, task_id));`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}
