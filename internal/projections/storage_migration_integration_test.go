package projections_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
	_ "modernc.org/sqlite"
)

func TestStorageV1FixtureMigratesAndRebuildsFromEvents(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "storage-v1.db")
	script, err := os.ReadFile(filepath.Join("..", "ledger", "testdata", "storage-v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, string(script)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	organization := core.Organization{ID: "org-v1", Name: "Migrated Organization", PolicyVersion: "policy-v1", CreatedAt: now}
	value, err := json.Marshal(organization)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	record := events.ProjectionRecord{ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, CorrelationID: "fixture-v1", Value: value}
	event := events.Event{
		EventID: "event-v1-organization", Sequence: 1, OrganizationID: string(organization.ID),
		EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: record.CorrelationID,
		AuthorizationRefs: []string{}, ArtifactRefs: []string{}, CreatedAt: now, SchemaVersion: events.SchemaVersion,
	}
	sealed, err := events.SealProjectionEvent(event, record, nil)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	payload, err := json.Marshal(sealed)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	body, err := json.Marshal(record)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events(sequence,event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, event.Sequence, event.EventID, event.OrganizationID, event.EventType, event.SourceActorID, []byte("[]"), []byte("[]"), payload, event.CorrelationID, now.Format(time.RFC3339Nano), event.SchemaVersion); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at) VALUES(?,?,?,?,?,?,?)`, record.ProjectionKind, record.RecordID, record.Version, body, event.EventID, sealed.Admission.Fingerprint, now.Format(time.RFC3339Nano)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rebuilt, err := projections.New(events.NewGateway(store)).Rebuild(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := rebuilt.Organizations[organization.ID]
	if !ok || got.Version != 1 || got.Value != organization {
		t.Fatalf("rebuilt organization=%+v present=%t", got, ok)
	}
}

