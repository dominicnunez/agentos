package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type freezeRevision struct {
	record events.AuthorityRecord
	event  events.Event
	state  authority.FreezeState
}

// freezeHistory is a complete, validated organization freeze chain from one
// database snapshot. Its revisions are ordered by both version and admission
// sequence, as proven by ResolveAuthorityAdmissions.
type freezeHistory struct {
	organization string
	revisions    []freezeRevision
	byEvent      map[string]int
}

const freezeHistorySQL = `SELECT r.kind,r.record_id,r.version,r.body,r.admission_event_id,
e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM events e
LEFT JOIN records r ON r.admission_event_id<>'' AND r.admission_event_id=e.event_id AND r.kind='organization_freeze' AND r.record_id=?
WHERE e.event_type='FREEZE_SET' AND e.organization_id=?
UNION ALL
SELECT r.kind,r.record_id,r.version,r.body,r.admission_event_id,
e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM records r
LEFT JOIN events e ON e.event_id=r.admission_event_id
WHERE r.kind='organization_freeze' AND r.record_id=?
AND (e.event_id IS NULL OR e.event_type<>'FREEZE_SET' OR e.organization_id<>?)
ORDER BY 3,7`

// loadFreezeHistory reads one organization's records and Event Contracts in a
// single query. The event-led arm retains orphan FREEZE_SET events; the second
// arm retains records whose event is missing or crosses the expected envelope.
// This lets the shared full resolver reject either half of a broken binding.
func loadFreezeHistory(ctx context.Context, tx *sql.Tx, organization string) (freezeHistory, error) {
	history := freezeHistory{organization: organization}
	if organization == "" {
		return history, fmt.Errorf("freeze history organization is required")
	}
	rows, err := tx.QueryContext(ctx, freezeHistorySQL, organization, organization, organization, organization)
	if err != nil {
		return history, fmt.Errorf("read organization freeze history: %w", err)
	}
	defer func() { _ = rows.Close() }()
	records := make([]events.AuthorityRecord, 0)
	stream := make([]events.Event, 0)
	for rows.Next() {
		record, event, hasRecord, hasEvent, err := scanFreezeHistoryRow(rows)
		if err != nil {
			return history, fmt.Errorf("scan organization freeze history: %w", err)
		}
		if hasRecord {
			records = append(records, record)
		}
		if hasEvent {
			stream = append(stream, event)
		}
	}
	if err := rows.Err(); err != nil {
		return history, fmt.Errorf("iterate organization freeze history: %w", err)
	}
	_, admissions, err := events.ResolveAuthorityAdmissions(stream, records)
	if err != nil {
		return history, fmt.Errorf("resolve organization freeze history: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return history, err
	}
	if len(admissions) != len(records) {
		return history, fmt.Errorf("organization freeze history has incomplete admissions")
	}
	eventsByID := make(map[string]events.Event, len(stream))
	for _, event := range stream {
		if _, duplicate := eventsByID[event.EventID]; duplicate {
			return history, fmt.Errorf("organization freeze history repeats admission event %s", event.EventID)
		}
		eventsByID[event.EventID] = event
	}
	history.revisions = make([]freezeRevision, 0, len(records))
	history.byEvent = make(map[string]int, len(records))
	for i, record := range records {
		admission := admissions[i]
		event, found := eventsByID[admission.EventRef]
		var state authority.FreezeState
		if !found || admission.Version != record.Version || admission.OrganizationID != core.ID(organization) ||
			event.OrganizationID != organization || event.EventID != record.AdmissionEventID ||
			decodeExactJSONBytes(record.Body, &state) != nil || state.OrganizationID != core.ID(organization) {
			return history, fmt.Errorf("organization freeze history admission %d is inconsistent", record.Version)
		}
		record.Body = append([]byte(nil), record.Body...)
		index := len(history.revisions)
		history.revisions = append(history.revisions, freezeRevision{record: record, event: event, state: state})
		history.byEvent[event.EventID] = index
	}
	return history, ctx.Err()
}

func scanFreezeHistoryRow(row rowScanner) (events.AuthorityRecord, events.Event, bool, bool, error) {
	var recordKind, recordID, admissionEventID sql.NullString
	var recordVersion sql.NullInt64
	var recordBody []byte
	var eventID, eventOrganization, eventType, actorID, executionID sql.NullString
	var recipientScope, recipientID, taskID, correlationID, createdAt sql.NullString
	var eventSequence, schemaVersion sql.NullInt64
	var authorizationRefs, artifactRefs, payload []byte
	if err := row.Scan(&recordKind, &recordID, &recordVersion, &recordBody, &admissionEventID,
		&eventID, &eventSequence, &eventOrganization, &eventType, &actorID, &executionID,
		&recipientScope, &recipientID, &taskID, &authorizationRefs, &artifactRefs,
		&payload, &correlationID, &createdAt, &schemaVersion); err != nil {
		return events.AuthorityRecord{}, events.Event{}, false, false, err
	}
	var record events.AuthorityRecord
	if recordKind.Valid {
		if !recordID.Valid || !recordVersion.Valid || !admissionEventID.Valid {
			return record, events.Event{}, false, false, fmt.Errorf("freeze record row is incomplete")
		}
		record = events.AuthorityRecord{Kind: recordKind.String, RecordID: recordID.String, Version: int(recordVersion.Int64), Body: append([]byte(nil), recordBody...), AdmissionEventID: admissionEventID.String}
	}
	if !eventID.Valid {
		return record, events.Event{}, recordKind.Valid, false, nil
	}
	if !eventSequence.Valid || !eventOrganization.Valid || !eventType.Valid || !actorID.Valid || !executionID.Valid ||
		!recipientScope.Valid || !recipientID.Valid || !taskID.Valid || !correlationID.Valid || !createdAt.Valid || !schemaVersion.Valid {
		return record, events.Event{}, recordKind.Valid, false, fmt.Errorf("freeze admission event row is incomplete")
	}
	var event events.Event
	if err := json.Unmarshal(authorizationRefs, &event.AuthorizationRefs); err != nil {
		return record, events.Event{}, recordKind.Valid, false, err
	}
	if err := json.Unmarshal(artifactRefs, &event.ArtifactRefs); err != nil {
		return record, events.Event{}, recordKind.Valid, false, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, createdAt.String)
	if err != nil {
		return record, events.Event{}, recordKind.Valid, false, err
	}
	event.EventID = eventID.String
	event.Sequence = eventSequence.Int64
	event.OrganizationID = eventOrganization.String
	event.EventType = eventType.String
	event.SourceActorID = actorID.String
	event.SourceExecutionID = executionID.String
	event.RecipientScope = recipientScope.String
	event.RecipientID = recipientID.String
	event.TaskID = taskID.String
	event.Payload = append([]byte(nil), payload...)
	event.CorrelationID = correlationID.String
	event.CreatedAt = parsed
	event.SchemaVersion = int(schemaVersion.Int64)
	return record, event, recordKind.Valid, true, nil
}

func (history freezeHistory) latest() (freezeRevision, bool) {
	if len(history.revisions) == 0 {
		return freezeRevision{}, false
	}
	return history.revisions[len(history.revisions)-1], true
}

func (history freezeHistory) before(sequence int64) (freezeRevision, bool) {
	for i := len(history.revisions) - 1; i >= 0; i-- {
		if history.revisions[i].event.Sequence < sequence {
			return history.revisions[i], true
		}
	}
	return freezeRevision{}, false
}

func (history freezeHistory) version(version int) (freezeRevision, bool) {
	if version < 1 || version > len(history.revisions) {
		return freezeRevision{}, false
	}
	revision := history.revisions[version-1]
	return revision, revision.record.Version == version
}

func (history freezeHistory) event(eventRef string) (freezeRevision, bool) {
	index, found := history.byEvent[eventRef]
	if !found {
		return freezeRevision{}, false
	}
	return history.revisions[index], true
}
