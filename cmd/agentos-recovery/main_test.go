package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/ledger/recovery"
	_ "modernc.org/sqlite"
)

func TestRunRequiresKnownCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"verify", "extra"}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("run(%v) succeeded", args)
		}
	}
}

func TestRunPrintsVersionWithoutOpeningDatabase(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"version"}} {
		var output bytes.Buffer
		if err := run(context.Background(), args, &output); err != nil {
			t.Fatal(err)
		}
		if output.String() != version+"\n" {
			t.Fatalf("run(%v) output=%q", args, output.String())
		}
	}
}

func TestRunVerifyReturnsStructuredResult(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.db")
	store, err := recoveryFixture(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(context.Background(), []string{"verify", "--database", source}, &output); err != nil {
		t.Fatal(err)
	}
	var result recovery.Result
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Path != source || result.SHA256 == "" || result.SizeBytes == 0 {
		t.Fatalf("result=%+v", result)
	}
}

func recoveryFixture(ctx context.Context, path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE events(
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
		return nil, err
	}
	return db, nil
}
