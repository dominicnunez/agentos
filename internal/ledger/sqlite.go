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
event_type TEXT NOT NULL, source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '', recipient_scope TEXT NOT NULL DEFAULT '', recipient_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', authorization_refs BLOB NOT NULL, artifact_refs BLOB NOT NULL, payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, schema_version INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS events_correlation_idx ON events(correlation_id, sequence);`)
	if err != nil {
		return err
	}
	if err := l.ensureEventRoutingColumns(ctx); err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS records (
kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL,
created_at TEXT NOT NULL, PRIMARY KEY(kind, record_id, version));
CREATE INDEX IF NOT EXISTS records_kind_idx ON records(kind, created_at);`)
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS inbox (
recipient_scope TEXT NOT NULL, recipient_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE,
organization_id TEXT NOT NULL, task_id TEXT NOT NULL DEFAULT '', available_at TEXT NOT NULL,
observed_at TEXT NOT NULL DEFAULT '', observation_event_id TEXT NOT NULL DEFAULT '',
PRIMARY KEY(recipient_scope, recipient_id, event_id));
CREATE INDEX IF NOT EXISTS inbox_available_idx ON inbox(recipient_scope, recipient_id, observed_at, available_at);`)
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS consumed_approvals (
approval_id TEXT PRIMARY KEY, effect_fingerprint TEXT NOT NULL, consumed_at TEXT NOT NULL);`)
	return err
}

func (l *SQLite) ensureEventRoutingColumns(ctx context.Context) error {
	columns := map[string]bool{}
	rows, err := l.db.QueryContext(ctx, `PRAGMA table_info(events)`)
	if err != nil {
		return fmt.Errorf("inspect event schema: %w", err)
	}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read event schema: %w", err)
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for name, ddl := range map[string]string{
		"recipient_scope": `ALTER TABLE events ADD COLUMN recipient_scope TEXT NOT NULL DEFAULT ''`,
		"recipient_id":    `ALTER TABLE events ADD COLUMN recipient_id TEXT NOT NULL DEFAULT ''`,
	} {
		if columns[name] {
			continue
		}
		if _, err := l.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("add event routing column %s: %w", name, err)
		}
	}
	return nil
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
	if d.EventType == "MESSAGE" {
		return l.appendMessage(ctx, d)
	}
	return appendEvent(ctx, l.db, d)
}

func (l *SQLite) appendMessage(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.RecipientScope == "" || draft.RecipientID == "" {
		return events.Event{}, fmt.Errorf("message recipient is required")
	}
	return l.appendWithProjection(ctx, draft, func(tx *sql.Tx, event events.Event) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO inbox(recipient_scope,recipient_id,event_id,organization_id,task_id,available_at) VALUES(?,?,?,?,?,?)`, draft.RecipientScope, draft.RecipientID, event.EventID, draft.OrganizationID, draft.TaskID, event.CreatedAt.Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("project message to inbox: %w", err)
		}
		return nil
	})
}

func (l *SQLite) ObserveInbox(ctx context.Context, draft events.TrustedDraft, recipientScope, recipientID string, eventIDs []string) (events.Event, error) {
	if draft.OrganizationID == "" || draft.RecipientScope != recipientScope || draft.RecipientID != recipientID || len(eventIDs) == 0 {
		return events.Event{}, fmt.Errorf("observation organization and matching recipient are required")
	}
	return l.appendWithProjection(ctx, draft, func(tx *sql.Tx, observation events.Event) error {
		now := observation.CreatedAt.Format(time.RFC3339Nano)
		for _, eventID := range eventIDs {
			result, err := tx.ExecContext(ctx, `UPDATE inbox SET observed_at=?,observation_event_id=? WHERE organization_id=? AND recipient_scope=? AND recipient_id=? AND event_id=? AND observed_at=''`, now, observation.EventID, draft.OrganizationID, recipientScope, recipientID, eventID)
			if err != nil {
				return fmt.Errorf("observe inbox event %s: %w", eventID, err)
			}
			changed, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if changed != 1 {
				return fmt.Errorf("inbox event %s is not available to recipient", eventID)
			}
		}
		return nil
	})
}

// appendWithProjection commits the authoritative event before its derived
// availability/state rows inside the same transaction. Any projection failure
// rolls the event back as well.
func (l *SQLite) appendWithProjection(ctx context.Context, draft events.TrustedDraft, project func(*sql.Tx, events.Event) error) (events.Event, error) {
	var event events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		event, err = appendEvent(ctx, tx, draft)
		if err != nil {
			return err
		}
		return project(tx, event)
	})
	return event, err
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
	r, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, d.OrganizationID, d.EventType, d.SourceActorID, d.SourceExecutionID, d.RecipientScope, d.RecipientID, d.TaskID, auth, artifacts, data, d.CorrelationID, now.Format(time.RFC3339Nano), events.SchemaVersion)
	if err != nil {
		return events.Event{}, fmt.Errorf("append event: %w", err)
	}
	seq, err := r.LastInsertId()
	if err != nil {
		return events.Event{}, err
	}
	return events.Event{EventID: id, Sequence: seq, OrganizationID: d.OrganizationID, EventType: d.EventType, SourceActorID: d.SourceActorID, SourceExecutionID: d.SourceExecutionID, RecipientScope: d.RecipientScope, RecipientID: d.RecipientID, TaskID: d.TaskID, AuthorizationRefs: d.AuthorizationRefs, ArtifactRefs: d.ArtifactRefs, CreatedAt: now, SchemaVersion: events.SchemaVersion, Payload: data, CorrelationID: d.CorrelationID}, nil
}
func (l *SQLite) Events(ctx context.Context, correlationID string) ([]events.Event, error) {
	return collectEvents(l.db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE (?='' OR correlation_id=?) ORDER BY sequence`, correlationID, correlationID))
}

func (l *SQLite) Inbox(ctx context.Context, recipientScope, recipientID string) ([]events.Event, error) {
	return collectEvents(l.db.QueryContext(ctx, `SELECT e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM inbox i JOIN events e ON e.event_id=i.event_id
WHERE i.recipient_scope=? AND i.recipient_id=? AND i.observed_at=''
ORDER BY e.sequence`, recipientScope, recipientID))
}

func collectEvents(rows *sql.Rows, err error) ([]events.Event, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []events.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(...any) error
}

func scanEvent(row rowScanner) (events.Event, error) {
	var event events.Event
	var createdAt string
	var authorizationRefs, artifactRefs []byte
	if err := row.Scan(&event.EventID, &event.Sequence, &event.OrganizationID, &event.EventType, &event.SourceActorID, &event.SourceExecutionID, &event.RecipientScope, &event.RecipientID, &event.TaskID, &authorizationRefs, &artifactRefs, &event.Payload, &event.CorrelationID, &createdAt, &event.SchemaVersion); err != nil {
		return events.Event{}, err
	}
	if err := json.Unmarshal(authorizationRefs, &event.AuthorizationRefs); err != nil {
		return events.Event{}, err
	}
	if err := json.Unmarshal(artifactRefs, &event.ArtifactRefs); err != nil {
		return events.Event{}, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return events.Event{}, err
	}
	event.CreatedAt = parsed
	return event, nil
}
