package ledger

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
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
		return nil, errors.Join(err, db.Close())
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
	if err := l.migrateExternalWorkIndex(ctx); err != nil {
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

func (l *SQLite) migrateExternalWorkIndex(ctx context.Context) error {
	if _, err := l.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS external_work (
organization_id TEXT NOT NULL, request_id TEXT NOT NULL, correlation_id TEXT NOT NULL, intent_id TEXT NOT NULL,
PRIMARY KEY(organization_id, request_id), UNIQUE(organization_id, correlation_id), UNIQUE(intent_id));`); err != nil {
		return fmt.Errorf("create external work index: %w", err)
	}
	rows, err := l.db.QueryContext(ctx, `SELECT body FROM records WHERE kind='intent' ORDER BY record_id, version`)
	if err != nil {
		return fmt.Errorf("scan intents for external work migration: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type workBinding struct{ organizationID, requestID, correlationID, intentID string }
	var bindings []workBinding
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return fmt.Errorf("read intent for external work migration: %w", err)
		}
		var record events.ProjectionRecord
		var intent core.Intent
		if err := json.Unmarshal(body, &record); err != nil {
			return fmt.Errorf("decode intent record for external work migration: %w", err)
		}
		if err := json.Unmarshal(record.Value, &intent); err != nil {
			return fmt.Errorf("decode intent value for external work migration: %w", err)
		}
		if intent.SourceChannel != "A2A" && intent.SourceChannel != "HUMAN_DIRECT" {
			continue
		}
		requestID := intent.ExternalRequestID
		if requestID == "" {
			requestID = record.CorrelationID
		}
		if intent.OrganizationID == "" || requestID == "" || record.CorrelationID == "" || intent.ID == "" {
			return fmt.Errorf("external intent %q lacks migration identity", intent.ID)
		}
		bindings = append(bindings, workBinding{string(intent.OrganizationID), requestID, record.CorrelationID, string(intent.ID)})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate intents for external work migration: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close intent migration rows: %w", err)
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		for _, binding := range bindings {
			if err := registerExternalWork(ctx, tx, binding.organizationID, binding.requestID, binding.correlationID, binding.intentID); err != nil {
				return fmt.Errorf("migrate external work %s/%s: %w", binding.organizationID, binding.requestID, err)
			}
		}
		return nil
	})
}

func (l *SQLite) ensureEventRoutingColumns(ctx context.Context) error {
	columns := map[string]bool{}
	rows, err := l.db.QueryContext(ctx, `PRAGMA table_info(events)`)
	if err != nil {
		return fmt.Errorf("inspect event schema: %w", err)
	}
	defer func() {
		_ = rows.Close() // Iteration and close failures are reported by rows.Err.
	}()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return fmt.Errorf("read event schema: %w", err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate event schema: %w", err)
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
		return appendRecord(ctx, tx, draft, kind, id, version, body)
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
		if draft.ProjectionKind == "intent" {
			var intent core.Intent
			if err := json.Unmarshal(value, &intent); err != nil {
				return fmt.Errorf("decode intent for external work index: %w", err)
			}
			if intent.SourceChannel == "A2A" || intent.SourceChannel == "HUMAN_DIRECT" {
				if err := registerExternalWork(ctx, tx, string(intent.OrganizationID), intent.ExternalRequestID, draft.Event.CorrelationID, string(intent.ID)); err != nil {
					return err
				}
			}
		}
		if eventDraft.RecipientScope != "" || eventDraft.RecipientID != "" {
			if eventDraft.RecipientScope == "" || eventDraft.RecipientID == "" {
				return fmt.Errorf("addressed projection recipient is required")
			}
			if err := projectInbox(ctx, tx, event); err != nil {
				return err
			}
		}
		return nil
	})
	return event, err
}

func registerExternalWork(ctx context.Context, tx *sql.Tx, organizationID, requestID, correlationID, intentID string) error {
	if organizationID == "" || requestID == "" || correlationID == "" || intentID == "" {
		return fmt.Errorf("complete external work identity is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO external_work(organization_id,request_id,correlation_id,intent_id) VALUES(?,?,?,?)`, organizationID, requestID, correlationID, intentID); err != nil {
		return fmt.Errorf("register external work: %w", err)
	}
	var storedCorrelationID, storedIntentID string
	if err := tx.QueryRowContext(ctx, `SELECT correlation_id,intent_id FROM external_work WHERE organization_id=? AND request_id=?`, organizationID, requestID).Scan(&storedCorrelationID, &storedIntentID); err != nil {
		return fmt.Errorf("verify external work registration: %w", err)
	}
	if storedCorrelationID != correlationID || storedIntentID != intentID {
		return fmt.Errorf("external request is already bound to different work")
	}
	return nil
}

func (l *SQLite) ResolveExternalWork(ctx context.Context, organizationID, requestID string) (string, bool, error) {
	var correlationID string
	err := l.db.QueryRowContext(ctx, `SELECT correlation_id FROM external_work WHERE organization_id=? AND request_id=?`, organizationID, requestID).Scan(&correlationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return correlationID, err == nil, err
}

func (l *SQLite) ResolveExternalRequest(ctx context.Context, organizationID, correlationID string) (string, bool, error) {
	var requestID string
	err := l.db.QueryRowContext(ctx, `SELECT request_id FROM external_work WHERE organization_id=? AND correlation_id=?`, organizationID, correlationID).Scan(&requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return requestID, err == nil, err
}

// AuthorizeAndAppendEffectAttempt serializes the latest durable approval,
// freeze, and lease state with the action's ATTEMPTED transition. Authorization
// that changes before this transaction blocks the effect; authorization that
// changes afterward orders after the effect has durably begun.
func (l *SQLite) AuthorizeAndAppendEffectAttempt(ctx context.Context, obligation core.EffectObligation, version int, value any) (core.AuthorizationTrace, error) {
	if obligation.OrganizationID == "" || obligation.TaskID == "" || obligation.ActorID == "" || obligation.Action == "" || obligation.Resource == "" || obligation.Scope == "" || len(obligation.AuthorizationRefs) == 0 || obligation.ID == "" || version < 1 {
		return core.AuthorizationTrace{}, fmt.Errorf("effect authority, record identity, and positive version are required")
	}
	requiresApproval, err := approvals.RequiresHumanApproval(obligation.ConsequenceBoundary)
	if err != nil {
		return core.AuthorizationTrace{}, err
	}
	if requiresApproval && obligation.ApprovalRef == "" {
		return core.AuthorizationTrace{}, fmt.Errorf("%w: durable approval is required", approvals.ErrApprovalPending)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return core.AuthorizationTrace{}, fmt.Errorf("encode authorized effect record: %w", err)
	}
	var trace core.AuthorizationTrace
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		var approval core.HumanApproval
		if obligation.ApprovalRef != "" {
			approvalBody, found, err := latestRecordBody(ctx, tx, "approval", obligation.ApprovalRef)
			if err != nil {
				return err
			}
			if !found {
				return approvals.ErrApprovalPending
			}
			if err := json.Unmarshal(approvalBody, &approval); err != nil {
				return fmt.Errorf("decode approval %s: %w", obligation.ApprovalRef, err)
			}
		}
		freezeBody, found, err := latestRecordBody(ctx, tx, "organization_freeze", string(obligation.OrganizationID))
		if err != nil {
			return err
		}
		frozen := false
		if found {
			var state authority.FreezeState
			if err := json.Unmarshal(freezeBody, &state); err != nil {
				return fmt.Errorf("decode organization freeze: %w", err)
			}
			if state.OrganizationID != obligation.OrganizationID {
				return fmt.Errorf("organization freeze identity mismatch")
			}
			frozen = state.Frozen
		}
		leases := make([]core.CapabilityLease, 0, len(obligation.AuthorizationRefs))
		for _, ref := range obligation.AuthorizationRefs {
			leaseBody, found, err := latestRecordBody(ctx, tx, "capability_lease", ref)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			var lease core.CapabilityLease
			if err := json.Unmarshal(leaseBody, &lease); err != nil {
				return fmt.Errorf("decode capability lease %s: %w", ref, err)
			}
			if string(lease.ID) != ref {
				return fmt.Errorf("capability lease identity mismatch for %s", ref)
			}
			leases = append(leases, lease)
		}
		authorizedAt := time.Now().UTC()
		if obligation.ApprovalRef != "" {
			if err := approvals.ValidateForEffect(approval, obligation, authorizedAt); err != nil {
				return err
			}
		}
		trace = authority.Check(authorizedAt, obligation.ActorID, obligation.TaskID, obligation.Action, obligation.Resource, obligation.Scope, leases, frozen)
		traceBody, err := json.Marshal(trace)
		if err != nil {
			return err
		}
		traceVersion, err := nextRecordVersion(ctx, tx, "authorization_trace", string(obligation.ID))
		if err != nil {
			return err
		}
		eventType := "CAPABILITY_DENIED"
		if trace.Allowed {
			eventType = "CAPABILITY_CHECKED"
		}
		traceDraft := events.TrustedDraft{OrganizationID: string(obligation.OrganizationID), EventType: eventType, SourceActorID: string(obligation.ActorID), TaskID: string(obligation.TaskID), AuthorizationRefs: obligation.AuthorizationRefs, Payload: trace}
		if err := appendRecord(ctx, tx, traceDraft, "authorization_trace", string(obligation.ID), traceVersion, traceBody); err != nil {
			return err
		}
		if !trace.Allowed {
			return nil
		}
		if approval.SingleUse {
			draft := events.TrustedDraft{OrganizationID: string(obligation.OrganizationID), EventType: "APPROVAL_CONSUMED", TaskID: string(obligation.TaskID), Payload: map[string]string{"approval_id": obligation.ApprovalRef, "effect_fingerprint": obligation.EffectFingerprint, "effect_obligation_id": string(obligation.ID)}}
			if _, err := appendEvent(ctx, tx, draft); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO consumed_approvals(approval_id,effect_fingerprint,consumed_at) VALUES(?,?,?)`, obligation.ApprovalRef, obligation.EffectFingerprint, authorizedAt.Format(time.RFC3339Nano)); err != nil {
				return fmt.Errorf("consume approval: %w", err)
			}
		}
		effectDraft := events.TrustedDraft{OrganizationID: string(obligation.OrganizationID), EventType: "EFFECT_OBLIGATION_TRANSITIONED", TaskID: string(obligation.TaskID), AuthorizationRefs: obligation.AuthorizationRefs, ArtifactRefs: obligation.ConfirmationEvidenceRefs, Payload: value}
		return appendRecord(ctx, tx, effectDraft, "effect", string(obligation.ID), version, body)
	})
	return trace, err
}

func latestRecordBody(ctx context.Context, tx *sql.Tx, kind, id string) ([]byte, bool, error) {
	var body []byte
	err := tx.QueryRowContext(ctx, `SELECT body FROM records WHERE kind=? AND record_id=? ORDER BY version DESC LIMIT 1`, kind, id).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

func nextRecordVersion(ctx context.Context, tx *sql.Tx, kind, id string) (int, error) {
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM records WHERE kind=? AND record_id=?`, kind, id).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func appendRecord(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft, kind, id string, version int, body []byte) error {
	if _, err := appendEvent(ctx, tx, draft); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,?,?,?)`, kind, id, version, body, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("append record: %w", err)
	}
	return nil
}

func (l *SQLite) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback() // Best-effort cleanup; Commit makes this return sql.ErrTxDone.
	}()
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
	defer func() {
		_ = rows.Close() // Iteration and close failures are reported by rows.Err.
	}()
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

// LatestRecords returns the current durable version of every record of a kind,
// ordered by record identity. Callers still re-read a specific record before a
// state transition so discovery never becomes a stale write authorization.
func (l *SQLite) LatestRecords(ctx context.Context, kind string) ([][]byte, error) {
	if kind == "" {
		return nil, fmt.Errorf("record kind is required")
	}
	rows, err := l.db.QueryContext(ctx, `SELECT current.body
FROM records AS current
JOIN (
	SELECT record_id, MAX(version) AS version
	FROM records
	WHERE kind=?
	GROUP BY record_id
) AS latest ON latest.record_id=current.record_id AND latest.version=current.version
WHERE current.kind=?
ORDER BY current.record_id`, kind, kind)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close() // Iteration and close failures are reported by rows.Err.
	}()
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
	if d.EventType == "MESSAGE" || d.RecipientScope != "" || d.RecipientID != "" {
		return l.appendAddressed(ctx, d)
	}
	return appendEvent(ctx, l.db, d)
}

func (l *SQLite) appendAddressed(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.RecipientScope == "" || draft.RecipientID == "" {
		return events.Event{}, fmt.Errorf("addressed event recipient is required")
	}
	return l.appendWithProjection(ctx, draft, func(tx *sql.Tx, event events.Event) error {
		return projectInbox(ctx, tx, event)
	})
}

func projectInbox(ctx context.Context, tx *sql.Tx, event events.Event) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO inbox(recipient_scope,recipient_id,event_id,organization_id,task_id,available_at) VALUES(?,?,?,?,?,?)`, event.RecipientScope, event.RecipientID, event.EventID, event.OrganizationID, event.TaskID, event.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("project addressed event to inbox: %w", err)
	}
	return nil
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
	defer func() {
		_ = rows.Close() // Iteration and close failures are reported by rows.Err.
	}()
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
