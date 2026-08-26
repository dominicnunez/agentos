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
)

const (
	EventIntegrityAlgorithm      = "SHA-256"
	EventIntegrityStorageVersion = 6
	eventIntegrityDomain         = "agentos.event-integrity.v1"
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
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
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
