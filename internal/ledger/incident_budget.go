package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/events"
)

// Support belongs to the whole read transaction, including evidence which is
// checked but omitted from the returned timeline. Charge identity never replaces
// an owning validator or changes which rows a selector must examine.
type incidentSupport struct {
	items  int
	bytes  int64
	events map[int64]incidentEventCharge
	rows   map[incidentRow]bool
}

type incidentEventCharge struct {
	public   bool
	size     int64
	refBytes int64
}

type incidentRow struct {
	table string
	id    int64
}

func newIncidentSupport() *incidentSupport {
	return &incidentSupport{items: events.MaximumIncidentEvidence, bytes: events.MaximumIncidentEvidenceBytes,
		events: map[int64]incidentEventCharge{}, rows: map[incidentRow]bool{}}
}

func (s *incidentSupport) reserve(items int, size int64) error {
	if items < 0 || size < 0 || items > s.items || size > s.bytes {
		return fmt.Errorf("incident evidence exceeds shared support limit")
	}
	s.items -= items
	s.bytes -= size
	return nil
}

const incidentInboxBytes = `length(CAST(i.observation_event_id AS BLOB))+length(CAST(i.event_id AS BLOB))+length(CAST(i.organization_id AS BLOB))+length(CAST(i.recipient_scope AS BLOB))+length(CAST(i.recipient_id AS BLOB))`

// Envelope reference columns encode string arrays or null for empty slices.
// Reject other shapes before decoding and count every reference occurrence.
const incidentRefCount = `CASE WHEN json_valid(authorization_refs) THEN CASE WHEN json_type(authorization_refs)='array' THEN json_array_length(authorization_refs) WHEN json_type(authorization_refs)='null' THEN 0 ELSE -1 END ELSE -1 END`
const incidentArtifactCount = `CASE WHEN json_valid(artifact_refs) THEN CASE WHEN json_type(artifact_refs)='array' THEN json_array_length(artifact_refs) WHEN json_type(artifact_refs)='null' THEN 0 ELSE -1 END ELSE -1 END`
const incidentRefBytes = `COALESCE((SELECT SUM(length(CAST(value AS BLOB))) FROM json_each(CASE WHEN json_valid(authorization_refs) THEN authorization_refs ELSE '[]' END)),0)+COALESCE((SELECT SUM(length(CAST(value AS BLOB))) FROM json_each(CASE WHEN json_valid(artifact_refs) THEN artifact_refs ELSE '[]' END)),0)`

// The caller has bounded the exact selected rows' raw bytes and identities.
// Reference JSON is bounded raw metadata, checked before decoding arrays. Its
// UTF-8 must be valid: Go's replacement of invalid bytes can grow strings beyond
// SQLite's decoded byte measurement. Payloads are not fetched by this preflight.
func (s *incidentSupport) eventRows(ctx context.Context, tx *sql.Tx, public bool, where string, args ...any) error {
	rows, err := tx.QueryContext(ctx, `SELECT CASE WHEN typeof(sequence)='integer' THEN sequence END,`+incidentEventBytes+`,`+incidentRefCount+`,`+incidentArtifactCount+`,`+incidentRefBytes+`,CAST(authorization_refs AS BLOB),CAST(artifact_refs AS BLOB) FROM events WHERE `+where, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sequence, size, auth, artifacts, refBytes int64
		var authorization, artifact []byte
		if err := rows.Scan(&sequence, &size, &auth, &artifacts, &refBytes, &authorization, &artifact); err != nil {
			return err
		}
		if !utf8.Valid(authorization) || !utf8.Valid(artifact) {
			return fmt.Errorf("incident reference JSON has invalid UTF-8")
		}
		if auth < 0 || artifacts < 0 || auth > int64(events.MaximumIncidentEvidence) || artifacts > int64(events.MaximumIncidentEvidence) {
			return fmt.Errorf("incident references are invalid or exceed support limit")
		}
		if prior, exists := s.events[sequence]; exists {
			if public && !prior.public {
				// A later Task effect can already be private dependency evidence.
				// Its references stay charged; its own raw bytes move to public.
				s.items++
				s.bytes += prior.size - prior.refBytes
				prior.public = true
				s.events[sequence] = prior
			}
			continue
		}
		items, bytes := int(auth+artifacts), refBytes
		if !public {
			items++
			bytes = size
		}
		if err := s.reserve(items, bytes); err != nil {
			return err
		}
		s.events[sequence] = incidentEventCharge{public: public, size: size, refBytes: refBytes}
	}
	return rows.Err()
}

// query returns integer SQLite rowid and complete raw source bytes. Its caller
// first bounds row count and bytes, so collecting charge identities is bounded.
func (s *incidentSupport) sourceRows(ctx context.Context, tx *sql.Tx, table, query string, args ...any) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id, size int64
		if err := rows.Scan(&id, &size); err != nil {
			return err
		}
		key := incidentRow{table: table, id: id}
		if s.rows[key] {
			continue
		}
		if err := s.reserve(1, size); err != nil {
			return err
		}
		s.rows[key] = true
	}
	return rows.Err()
}

// Standalone callers consume this budget directly. Request readers retain
// per-load safety caps while consuming the transaction-wide support allowance.
func (b *incidentBudget) consume(items int, size int64) error {
	if b.support != nil {
		return b.support.reserve(items, size)
	}
	if items > b.events || size > b.bytes {
		return fmt.Errorf("incident support exceeds limit")
	}
	b.events -= items
	b.bytes -= size
	return nil
}

func (s *incidentSupport) admissions(admissions []events.IncidentAdmission) error {
	for _, admission := range admissions {
		if err := s.reserve(1, int64(len(admission.EventRef)+len(admission.Kind)+len(admission.TaskID)+len(admission.ExecutionID))); err != nil {
			return err
		}
	}
	return nil
}
