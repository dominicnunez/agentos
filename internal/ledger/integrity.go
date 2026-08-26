package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger/anchor"
)

const (
	EventIntegrityAlgorithm      = "SHA-256"
	EventIntegrityStorageVersion = 6
	eventIntegrityDomain         = "agentos.event-integrity.v1"
	authorityIntegrityDomain     = "agentos.authority-integrity.v1"
)

// EventIntegrityHead identifies the verified tip of the ledger's
// cryptographic event chain. It is evidence about this SQLite snapshot, not a
// signature, external timestamp, or certification claim.
type EventIntegrityHead struct {
	Algorithm  string `json:"algorithm"`
	EventCount int64  `json:"event_count"`
	Sequence   int64  `json:"sequence"`
	EventID    string `json:"event_id,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
}

// AuthorityIntegrityState identifies the exact durable authority-bearing
// state bound into an external checkpoint. These rows remain subordinate to
// the chained event stream; the digest prevents an offline database rewrite
// from changing approval, capability, or inference admission decisions.
type AuthorityIntegrityState struct {
	Algorithm string `json:"algorithm"`
	Count     int64  `json:"count"`
	SHA256    string `json:"sha256"`
}

// ValidateAuthorityIntegrity hashes the exact ordered authority-bearing state
// stored in one SQLite snapshot.
func ValidateAuthorityIntegrity(ctx context.Context, db *sql.DB) (AuthorityIntegrityState, error) {
	if ctx == nil || db == nil {
		return AuthorityIntegrityState{}, fmt.Errorf("authority integrity context and database are required")
	}
	return authorityIntegrityState(ctx, db)
}

func authorityIntegrityState(ctx context.Context, db storageQueryer) (AuthorityIntegrityState, error) {
	digest := sha256.New()
	writeIntegrityField(digest, []byte(authorityIntegrityDomain))
	state := AuthorityIntegrityState{Algorithm: EventIntegrityAlgorithm}
	tables := []struct {
		name  string
		query string
	}{
		{name: "records", query: `SELECT CAST(kind AS BLOB),CAST(record_id AS BLOB),CAST(version AS BLOB),CAST(body AS BLOB),CAST(admission_event_id AS BLOB),CAST(admission_fingerprint AS BLOB),CAST(created_at AS BLOB) FROM records ORDER BY kind,record_id,version`},
		{name: "consumed_approvals", query: `SELECT CAST(approval_id AS BLOB),CAST(effect_fingerprint AS BLOB),CAST(consumed_at AS BLOB) FROM consumed_approvals ORDER BY approval_id`},
		{name: "inference_policies", query: `SELECT CAST(organization_id AS BLOB),CAST(policy_fingerprint AS BLOB),CAST(body AS BLOB),CAST(activation_event_id AS BLOB),CAST(activated_at AS BLOB),CAST(active AS BLOB) FROM inference_policies ORDER BY organization_id,policy_fingerprint`},
		{name: "inference_reservations", query: `SELECT CAST(reservation_id AS BLOB),CAST(request_id AS BLOB),CAST(organization_id AS BLOB),CAST(purpose AS BLOB),CAST(intent_id AS BLOB),CAST(task_id AS BLOB),CAST(execution_id AS BLOB),CAST(correlation_id AS BLOB),CAST(prompt_sha256 AS BLOB),CAST(provider AS BLOB),CAST(model AS BLOB),CAST(execution_profile_version AS BLOB),CAST(policy_fingerprint AS BLOB),CAST(state AS BLOB),CAST(reserved_input_tokens AS BLOB),CAST(reserved_output_tokens AS BLOB),CAST(reserved_cost_nano_usd AS BLOB),CAST(charged_input_tokens AS BLOB),CAST(charged_output_tokens AS BLOB),CAST(charged_cost_nano_usd AS BLOB),CAST(window_started_at AS BLOB),CAST(window_expires_at AS BLOB),CAST(created_at AS BLOB),CAST(updated_at AS BLOB) FROM inference_reservations ORDER BY reservation_id`},
	}
	for _, table := range tables {
		count, err := hashAuthorityTable(ctx, db, digest, table.name, table.query)
		if err != nil {
			return AuthorityIntegrityState{}, err
		}
		state.Count += count
	}
	state.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return state, nil
}

func hashAuthorityTable(ctx context.Context, db storageQueryer, digest hash.Hash, name, query string) (int64, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("read %s for authority integrity binding: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil || len(columns) == 0 {
		return 0, fmt.Errorf("inspect %s authority integrity columns: %w", name, err)
	}
	writeIntegrityField(digest, []byte(name))
	writeIntegrityInt(digest, int64(len(columns)))
	var count int64
	for rows.Next() {
		values := make([][]byte, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return 0, fmt.Errorf("decode %s authority integrity row: %w", name, err)
		}
		for _, value := range values {
			writeIntegrityField(digest, value)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scan %s authority integrity rows: %w", name, err)
	}
	return count, nil
}

type storedIntegrityEvent struct {
	sequence         int64
	eventID          string
	organizationID   string
	eventType        string
	sourceActorID    string
	sourceExecution  string
	recipientScope   string
	recipientID      string
	taskID           string
	authorizationRaw []byte
	artifactsRaw     []byte
	payloadRaw       []byte
	correlationID    string
	createdAt        string
	schemaVersion    int64
}

type integrityRowScanner interface {
	Scan(...any) error
}

func scanStoredIntegrityEvent(row integrityRowScanner) (storedIntegrityEvent, error) {
	var event storedIntegrityEvent
	err := row.Scan(
		&event.sequence, &event.eventID, &event.organizationID, &event.eventType,
		&event.sourceActorID, &event.sourceExecution, &event.recipientScope,
		&event.recipientID, &event.taskID, &event.authorizationRaw,
		&event.artifactsRaw, &event.payloadRaw, &event.correlationID,
		&event.createdAt, &event.schemaVersion,
	)
	return event, err
}

func eventIntegritySHA256(previous string, event storedIntegrityEvent) string {
	digest := sha256.New()
	writeIntegrityField(digest, []byte(eventIntegrityDomain))
	writeIntegrityField(digest, []byte(EventIntegrityAlgorithm))
	writeIntegrityInt(digest, event.sequence)
	writeIntegrityField(digest, []byte(previous))
	writeIntegrityField(digest, []byte(event.eventID))
	writeIntegrityField(digest, []byte(event.organizationID))
	writeIntegrityField(digest, []byte(event.eventType))
	writeIntegrityField(digest, []byte(event.sourceActorID))
	writeIntegrityField(digest, []byte(event.sourceExecution))
	writeIntegrityField(digest, []byte(event.recipientScope))
	writeIntegrityField(digest, []byte(event.recipientID))
	writeIntegrityField(digest, []byte(event.taskID))
	writeIntegrityField(digest, event.authorizationRaw)
	writeIntegrityField(digest, event.artifactsRaw)
	writeIntegrityField(digest, event.payloadRaw)
	writeIntegrityField(digest, []byte(event.correlationID))
	writeIntegrityField(digest, []byte(event.createdAt))
	writeIntegrityInt(digest, event.schemaVersion)
	return hex.EncodeToString(digest.Sum(nil))
}

func sealEventIntegrity(ctx context.Context, db sqlExecutor, sequence int64) error {
	if sequence < 1 {
		return fmt.Errorf("event integrity requires a positive sequence")
	}
	var integrityTables, storageVersion int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='event_integrity'`).Scan(&integrityTables); err != nil {
		return fmt.Errorf("inspect event integrity storage: %w", err)
	}
	if integrityTables == 0 {
		if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&storageVersion); err != nil {
			return fmt.Errorf("inspect event integrity storage version: %w", err)
		}
		if storageVersion < EventIntegrityStorageVersion {
			return nil
		}
		return fmt.Errorf("current storage lacks event integrity records")
	}
	event, err := scanStoredIntegrityEvent(db.QueryRowContext(ctx, `SELECT sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,CAST(authorization_refs AS BLOB),CAST(artifact_refs AS BLOB),CAST(payload AS BLOB),correlation_id,created_at,schema_version FROM events WHERE sequence=?`, sequence))
	if err != nil {
		return fmt.Errorf("read event %d for integrity seal: %w", sequence, err)
	}
	var previousSequence int64
	var previousHash string
	err = db.QueryRowContext(ctx, `SELECT sequence,event_hash FROM event_integrity ORDER BY sequence DESC LIMIT 1`).Scan(&previousSequence, &previousHash)
	switch {
	case errors.Is(err, sql.ErrNoRows) && sequence != 1:
		return fmt.Errorf("event integrity chain starts at sequence %d", sequence)
	case errors.Is(err, sql.ErrNoRows):
		previousSequence, previousHash = 0, ""
	case err != nil:
		return fmt.Errorf("read prior event integrity head: %w", err)
	case previousSequence+1 != sequence:
		return fmt.Errorf("event integrity sequence %d does not follow %d", sequence, previousSequence)
	case !validEventIntegrityHash(previousHash):
		return fmt.Errorf("prior event integrity hash is invalid")
	}
	eventHash := eventIntegritySHA256(previousHash, event)
	if _, err := db.ExecContext(ctx, `INSERT INTO event_integrity(sequence,event_id,algorithm,previous_hash,event_hash) VALUES(?,?,?,?,?)`, event.sequence, event.eventID, EventIntegrityAlgorithm, previousHash, eventHash); err != nil {
		return fmt.Errorf("seal event %d integrity: %w", sequence, err)
	}
	return nil
}

// ValidateEventIntegrity verifies complete one-to-one coverage and the exact
// stored-byte hash chain for every event in sequence order.
func ValidateEventIntegrity(ctx context.Context, db storageQueryer) (EventIntegrityHead, error) {
	head := EventIntegrityHead{Algorithm: EventIntegrityAlgorithm}
	rows, err := db.QueryContext(ctx, `SELECT e.sequence,e.event_id,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,CAST(e.authorization_refs AS BLOB),CAST(e.artifact_refs AS BLOB),CAST(e.payload AS BLOB),e.correlation_id,e.created_at,e.schema_version,i.event_id,i.algorithm,i.previous_hash,i.event_hash FROM events e LEFT JOIN event_integrity i ON i.sequence=e.sequence ORDER BY e.sequence`)
	if err != nil {
		return EventIntegrityHead{}, fmt.Errorf("read event integrity chain: %w", err)
	}
	defer func() { _ = rows.Close() }()
	previousHash := ""
	for rows.Next() {
		var event storedIntegrityEvent
		var integrityEventID, algorithm, recordedPrevious, recordedHash sql.NullString
		if err := rows.Scan(
			&event.sequence, &event.eventID, &event.organizationID, &event.eventType,
			&event.sourceActorID, &event.sourceExecution, &event.recipientScope,
			&event.recipientID, &event.taskID, &event.authorizationRaw,
			&event.artifactsRaw, &event.payloadRaw, &event.correlationID,
			&event.createdAt, &event.schemaVersion, &integrityEventID, &algorithm,
			&recordedPrevious, &recordedHash,
		); err != nil {
			return EventIntegrityHead{}, fmt.Errorf("decode event integrity chain: %w", err)
		}
		head.EventCount++
		if event.sequence != head.EventCount {
			return EventIntegrityHead{}, fmt.Errorf("event integrity sequence %d is not contiguous", event.sequence)
		}
		if !integrityEventID.Valid || integrityEventID.String != event.eventID || !algorithm.Valid || algorithm.String != EventIntegrityAlgorithm || !recordedPrevious.Valid || recordedPrevious.String != previousHash || !recordedHash.Valid || !validEventIntegrityHash(recordedHash.String) {
			return EventIntegrityHead{}, fmt.Errorf("event %d lacks its exact integrity record", event.sequence)
		}
		expected := eventIntegritySHA256(previousHash, event)
		if recordedHash.String != expected {
			return EventIntegrityHead{}, fmt.Errorf("event %d integrity hash does not match stored content", event.sequence)
		}
		previousHash = recordedHash.String
		head.Sequence, head.EventID, head.SHA256 = event.sequence, event.eventID, recordedHash.String
	}
	if err := rows.Err(); err != nil {
		return EventIntegrityHead{}, fmt.Errorf("iterate event integrity chain: %w", err)
	}
	var orphaned int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_integrity i LEFT JOIN events e ON e.sequence=i.sequence AND e.event_id=i.event_id WHERE e.sequence IS NULL`).Scan(&orphaned); err != nil {
		return EventIntegrityHead{}, fmt.Errorf("inspect orphaned event integrity records: %w", err)
	}
	if orphaned != 0 {
		return EventIntegrityHead{}, fmt.Errorf("event integrity chain contains %d orphaned records", orphaned)
	}
	return head, nil
}

func validEventIntegrityHash(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func rebuildEventIntegrity(ctx context.Context, tx *sql.Tx) error {
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_integrity`).Scan(&existing); err != nil {
		return fmt.Errorf("inspect event integrity migration target: %w", err)
	}
	if existing != 0 {
		return fmt.Errorf("event integrity migration target is not empty")
	}
	rows, err := tx.QueryContext(ctx, `SELECT sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,CAST(authorization_refs AS BLOB),CAST(artifact_refs AS BLOB),CAST(payload AS BLOB),correlation_id,created_at,schema_version FROM events ORDER BY sequence`)
	if err != nil {
		return fmt.Errorf("read events for integrity migration: %w", err)
	}
	defer func() { _ = rows.Close() }()
	previousHash := ""
	var expectedSequence int64
	for rows.Next() {
		event, err := scanStoredIntegrityEvent(rows)
		if err != nil {
			return fmt.Errorf("decode event for integrity migration: %w", err)
		}
		expectedSequence++
		if event.sequence != expectedSequence || event.eventID == "" {
			return fmt.Errorf("event integrity migration requires a contiguous complete stream")
		}
		eventHash := eventIntegritySHA256(previousHash, event)
		if _, err := tx.ExecContext(ctx, `INSERT INTO event_integrity(sequence,event_id,algorithm,previous_hash,event_hash) VALUES(?,?,?,?,?)`, event.sequence, event.eventID, EventIntegrityAlgorithm, previousHash, eventHash); err != nil {
			return fmt.Errorf("backfill event %d integrity: %w", event.sequence, err)
		}
		previousHash = eventHash
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan events for integrity migration: %w", err)
	}
	return nil
}

func (l *SQLite) Integrity(ctx context.Context) (EventIntegrityHead, error) {
	return ValidateEventIntegrity(ctx, l.db)
}

// IntegrityAnchorState performs the complete storage and event-chain
// validation needed before an external checkpoint is trusted or attached.
func (l *SQLite) IntegrityAnchorState(ctx context.Context) (anchor.LedgerState, error) {
	contract, err := ValidateStorageContract(ctx, l.db)
	if err != nil {
		return anchor.LedgerState{}, fmt.Errorf("validate storage before ledger anchoring: %w", err)
	}
	head, err := ValidateEventIntegrity(ctx, l.db)
	if err != nil {
		return anchor.LedgerState{}, fmt.Errorf("validate event chain before ledger anchoring: %w", err)
	}
	authority, err := authorityIntegrityState(ctx, l.db)
	if err != nil {
		return anchor.LedgerState{}, fmt.Errorf("validate records before ledger anchoring: %w", err)
	}
	return anchor.LedgerState{
		ApplicationID: StorageApplicationID, StorageVersion: contract.StorageVersion,
		EventSchemaVersion: contract.EventSchemaVersion, EventCount: head.EventCount,
		Sequence: head.Sequence, EventID: head.EventID,
		ChainAlgorithm: head.Algorithm, ChainHead: head.SHA256,
		AuthorityCount: authority.Count, AuthorityAlgorithm: authority.Algorithm, AuthoritySHA256: authority.SHA256,
	}, nil
}

// integrityAnchorState reads the already-validated tip inside the writer's
// SQLite transaction. Startup validated the complete chain, and every later
// event and integrity row is appended atomically by this package.
func integrityAnchorState(ctx context.Context, db storageQueryer) (anchor.LedgerState, error) {
	head := EventIntegrityHead{Algorithm: EventIntegrityAlgorithm}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&head.EventCount); err != nil {
		return anchor.LedgerState{}, fmt.Errorf("count events for ledger anchor: %w", err)
	}
	if head.EventCount > 0 {
		var integrityEventID, algorithm, eventHash string
		if err := db.QueryRowContext(ctx, `SELECT e.sequence,e.event_id,i.event_id,i.algorithm,i.event_hash FROM events e JOIN event_integrity i ON i.sequence=e.sequence ORDER BY e.sequence DESC LIMIT 1`).Scan(&head.Sequence, &head.EventID, &integrityEventID, &algorithm, &eventHash); err != nil {
			return anchor.LedgerState{}, fmt.Errorf("read event head for ledger anchor: %w", err)
		}
		if head.Sequence != head.EventCount || integrityEventID != head.EventID || algorithm != EventIntegrityAlgorithm || !validEventIntegrityHash(eventHash) {
			return anchor.LedgerState{}, fmt.Errorf("event head is invalid for external anchoring")
		}
		head.SHA256 = eventHash
	}
	authority, err := authorityIntegrityState(ctx, db)
	if err != nil {
		return anchor.LedgerState{}, err
	}
	return anchor.LedgerState{
		ApplicationID: StorageApplicationID, StorageVersion: CurrentStorageVersion,
		EventSchemaVersion: events.SchemaVersion, EventCount: head.EventCount,
		Sequence: head.Sequence, EventID: head.EventID,
		ChainAlgorithm: head.Algorithm, ChainHead: head.SHA256,
		AuthorityCount: authority.Count, AuthorityAlgorithm: authority.Algorithm, AuthoritySHA256: authority.SHA256,
	}, nil
}

func writeIntegrityField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func writeIntegrityInt(digest hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = digest.Write(encoded[:])
}
