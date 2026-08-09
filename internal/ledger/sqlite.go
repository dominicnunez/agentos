package ledger

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	_ "modernc.org/sqlite"
)

type SQLite struct{ db *sql.DB }

func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	l := &SQLite{db: db}
	if err := l.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return l, nil
}
func (l *SQLite) Close() error { return l.db.Close() }
func (l *SQLite) migrate(ctx context.Context) error {
	_, err := l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL,
event_type TEXT NOT NULL, source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', authorization_refs BLOB NOT NULL, artifact_refs BLOB NOT NULL, payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, schema_version INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS events_correlation_idx ON events(correlation_id, sequence);`)
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS records (
kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL,
created_at TEXT NOT NULL, PRIMARY KEY(kind, record_id, version));
CREATE INDEX IF NOT EXISTS records_kind_idx ON records(kind, created_at);`)
	return err
}

// PutRecord appends a versioned durable object. The primary key prevents
// history from being overwritten and makes promotion/version races fail closed.
func (l *SQLite) PutRecord(ctx context.Context, kind, id string, version int, value any) error {
	if kind == "" || id == "" || version < 1 {
		return fmt.Errorf("kind, id, and positive version are required")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	_, err = l.db.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,?,?,?)`, kind, id, version, body, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("append record: %w", err)
	}
	return nil
}

func (l *SQLite) Records(ctx context.Context, kind, id string) ([][]byte, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT body FROM records WHERE kind=? AND (?='' OR record_id=?) ORDER BY record_id,version`, kind, id, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, rows.Err()
}
func (l *SQLite) Append(ctx context.Context, d events.TrustedDraft) (events.Event, error) {
	data, err := json.Marshal(d.Payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("encode event: %w", err)
	}
	auth, _ := json.Marshal(d.AuthorizationRefs)
	artifacts, _ := json.Marshal(d.ArtifactRefs)
	now := time.Now().UTC()
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return events.Event{}, fmt.Errorf("generate event id: %w", err)
	}
	id := "evt-" + hex.EncodeToString(random[:])
	r, err := l.db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,source_execution_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, id, d.OrganizationID, d.EventType, d.SourceActorID, d.SourceExecutionID, d.TaskID, auth, artifacts, data, d.CorrelationID, now.Format(time.RFC3339Nano), events.SchemaVersion)
	if err != nil {
		return events.Event{}, fmt.Errorf("append event: %w", err)
	}
	seq, err := r.LastInsertId()
	if err != nil {
		return events.Event{}, err
	}
	return events.Event{EventID: id, Sequence: seq, OrganizationID: d.OrganizationID, EventType: d.EventType, SourceActorID: d.SourceActorID, SourceExecutionID: d.SourceExecutionID, TaskID: d.TaskID, AuthorizationRefs: d.AuthorizationRefs, ArtifactRefs: d.ArtifactRefs, CreatedAt: now, SchemaVersion: events.SchemaVersion, Payload: data, CorrelationID: d.CorrelationID}, nil
}
func (l *SQLite) Events(ctx context.Context, correlationID string) ([]events.Event, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE (?='' OR correlation_id=?) ORDER BY sequence`, correlationID, correlationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.Event
	for rows.Next() {
		var e events.Event
		var at string
		var auth, artifacts []byte
		if err := rows.Scan(&e.EventID, &e.Sequence, &e.OrganizationID, &e.EventType, &e.SourceActorID, &e.SourceExecutionID, &e.TaskID, &auth, &artifacts, &e.Payload, &e.CorrelationID, &at, &e.SchemaVersion); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(auth, &e.AuthorizationRefs); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(artifacts, &e.ArtifactRefs); err != nil {
			return nil, err
		}
		e.CreatedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
