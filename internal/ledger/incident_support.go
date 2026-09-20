package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/dominicnunez/agentos/internal/events"
)

func (d *incidentDependencies) loadAuthorities(ctx context.Context, tx *sql.Tx) error {
	var ids []string
	for key := range d.keys {
		if key.kind == "capability_lease" && !d.records[key.kind+":"+key.id] {
			ids = append(ids, key.id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	query := `SELECT kind,record_id,version,body,admission_event_id FROM records WHERE kind='capability_lease' AND record_id IN (` + incidentMarks(len(ids)) + `) ORDER BY record_id,version LIMIT ?`
	args = append(args, d.budget.events+1)
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(record_id AS BLOB))+length(CAST(admission_event_id AS BLOB))),0) FROM (`+query+`)`, args...).Scan(&count, &size); err != nil {
		return err
	}
	if count > d.budget.events || size > d.budget.bytes {
		return fmt.Errorf("incident authority exceeds support limit")
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var record events.AuthorityRecord
		if err := rows.Scan(&record.Kind, &record.RecordID, &record.Version, &record.Body, &record.AdmissionEventID); err != nil {
			return err
		}
		d.authority = append(d.authority, record)
		d.ref(record.AdmissionEventID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	d.budget.events -= count
	d.budget.bytes -= size
	for _, id := range ids {
		d.records["capability_lease:"+id] = true
	}
	return nil
}

func (d *incidentDependencies) loadSupportingRows(ctx context.Context, tx *sql.Tx, snapshot *events.IncidentSnapshot) error {
	var ids []string
	for id, event := range d.stream {
		if event.EventType == "INBOX_EVENTS_OBSERVED" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	// Include unmatched rows in the bound; the exact joined recipient/tenant
	// checks below must not make malformed backing disappear from accounting.
	where := ` WHERE i.observation_event_id IN (` + incidentMarks(len(ids)) + `)`
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(size),0) FROM (SELECT length(CAST(i.observation_event_id AS BLOB))+length(CAST(i.event_id AS BLOB))+length(CAST(i.organization_id AS BLOB))+length(CAST(i.recipient_scope AS BLOB))+length(CAST(i.recipient_id AS BLOB)) AS size FROM inbox i`+where+` LIMIT ?)`, append(args, d.budget.events+1)...).Scan(&count, &size); err != nil {
		return err
	}
	if count > d.budget.events || size > d.budget.bytes {
		return fmt.Errorf("incident inbox exceeds support limit")
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.observation_event_id,i.event_id,i.organization_id,i.recipient_scope,i.recipient_id FROM inbox i LEFT JOIN events e ON e.event_id=i.event_id`+where+` ORDER BY i.observation_event_id,e.sequence`, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	snapshot.InboxObservations = map[string]events.InboxObservationBinding{}
	for rows.Next() {
		var id, eventID, organization, scope, recipient string
		if err := rows.Scan(&id, &eventID, &organization, &scope, &recipient); err != nil {
			return err
		}
		observation, ok := d.stream[id]
		addressed, found := d.stream[eventID]
		if !ok || !found || organization != d.organization || observation.RecipientScope != scope || observation.RecipientID != recipient || addressed.RecipientScope != scope || addressed.RecipientID != recipient {
			return fmt.Errorf("incident inbox observation lacks exact recipient binding")
		}
		binding := snapshot.InboxObservations[id]
		binding.EventIDs = append(binding.EventIDs, eventID)
		snapshot.InboxObservations[id] = binding
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		var payload events.InboxEventsObservedPayload
		if err := json.Unmarshal(d.stream[id].Payload, &payload); err != nil {
			return err
		}
		binding := snapshot.InboxObservations[id]
		if payload.ExecutionStartEventRef == "" || len(payload.EventIDs) == 0 || !slices.Equal(payload.EventIDs, binding.EventIDs) {
			return fmt.Errorf("incident inbox observation lacks complete backing")
		}
		binding.ExecutionStartEventRef = payload.ExecutionStartEventRef
		snapshot.InboxObservations[id] = binding
	}
	d.budget.events -= count
	d.budget.bytes -= size
	return nil
}
