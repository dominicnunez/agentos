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
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS consumed_approvals (
approval_id TEXT PRIMARY KEY, effect_fingerprint TEXT NOT NULL, consumed_at TEXT NOT NULL);`)
	return err
}

// AppendRecord appends the authoritative transition event and updates its
// rebuildable record projection in one transaction. The event is inserted
// first so durable object state can never exist without ledger evidence.
func (l *SQLite) AppendRecord(ctx context.Context, organizationID, eventType, actorID, taskID string, authorizationRefs, artifactRefs []string, kind, id string, version int, value any) error {
	if kind == "" || id == "" || version < 1 {
		return fmt.Errorf("kind, id, and positive version are required")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		draft := events.TrustedDraft{OrganizationID: organizationID, EventType: eventType, SourceActorID: actorID, TaskID: taskID, AuthorizationRefs: authorizationRefs, ArtifactRefs: artifactRefs, Payload: value}
		if _, err := appendEvent(ctx, tx, draft); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,?,?,?)`, kind, id, version, body, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("append record: %w", err)
		}
		return nil
	})
}

// AppendProjection atomically appends a trusted transition and its versioned,
// rebuildable projection record. The event payload includes the full record so
// the records table can be regenerated from the append-only ledger.
func (l *SQLite) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if draft.Event.EventType == "" || draft.ProjectionKind == "" || draft.RecordID == "" || draft.Version < 1 {
		return events.Event{}, fmt.Errorf("event type, projection kind, record id, and positive version are required")
	}
	value, err := json.Marshal(draft.Value)
	if err != nil {
		return events.Event{}, fmt.Errorf("encode projection value: %w", err)
	}
	record := events.ProjectionRecord{
		ProjectionKind: draft.ProjectionKind,
		RecordID:       draft.RecordID,
		Version:        draft.Version,
		CorrelationID:  draft.Event.CorrelationID,
		Value:          value,
	}
	body, err := json.Marshal(record)
	if err != nil {
		return events.Event{}, fmt.Errorf("encode projection record: %w", err)
	}
	detail, err := json.Marshal(draft.Event.Payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("encode projection event detail: %w", err)
	}
	eventDraft := draft.Event
	eventDraft.Payload = events.ProjectionEventPayload{Projection: record, Detail: detail}
	var event events.Event
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		event, err = appendEvent(ctx, tx, eventDraft)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,?,?,?)`, draft.ProjectionKind, draft.RecordID, draft.Version, body, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("append projection: %w", err)
		}
		return nil
	})
	return event, err
}

// ConsumeApproval durably and atomically claims a single-use approval. A
// duplicate approval ID fails before an external adapter can be called.
func (l *SQLite) ConsumeApproval(ctx context.Context, organizationID, taskID, approvalID, fingerprint, effectID string) error {
	if approvalID == "" || fingerprint == "" {
		return fmt.Errorf("approval id and fingerprint are required")
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		draft := events.TrustedDraft{OrganizationID: organizationID, EventType: "APPROVAL_CONSUMED", TaskID: taskID, Payload: map[string]string{"approval_id": approvalID, "effect_fingerprint": fingerprint, "effect_obligation_id": effectID}}
		if _, err := appendEvent(ctx, tx, draft); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO consumed_approvals(approval_id,effect_fingerprint,consumed_at) VALUES(?,?,?)`, approvalID, fingerprint, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("consume approval: %w", err)
		}
		return nil
	})
}

func (l *SQLite) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
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
	return appendEvent(ctx, l.db, d)
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func appendEvent(ctx context.Context, db sqlExecutor, d events.TrustedDraft) (events.Event, error) {
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
	r, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,source_execution_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, id, d.OrganizationID, d.EventType, d.SourceActorID, d.SourceExecutionID, d.TaskID, auth, artifacts, data, d.CorrelationID, now.Format(time.RFC3339Nano), events.SchemaVersion)
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
