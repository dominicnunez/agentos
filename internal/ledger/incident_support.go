package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
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

// loadInboxCandidates reconstructs the input boundary from durable execution
// starts, independently of the manifest's claimed input references.
func (d *incidentDependencies) loadInboxCandidates(ctx context.Context, tx *sql.Tx) error {
	type boundary struct {
		Task   string `json:"task"`
		Agent  string `json:"agent"`
		Start  int64  `json:"start"`
		Cutoff int64  `json:"cutoff"`
	}
	if d.inboxStarts == nil {
		d.inboxStarts = map[string]bool{}
	}
	var boundaries []boundary
	for _, event := range d.stream {
		if event.EventType != "EXECUTION_STARTED" || d.inboxStarts[event.EventID] {
			continue
		}
		d.inboxStarts[event.EventID] = true
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("incident start lacks projection")
		}
		var task core.Task
		var detail events.ExecutionStartDetail
		if json.Unmarshal(payload.Projection.Value, &task) != nil || json.Unmarshal(payload.Detail, &detail) != nil {
			return fmt.Errorf("invalid incident start inbox boundary")
		}
		if task.ExecutionKind == core.ExecutionAgent {
			boundaries = append(boundaries, boundary{string(task.ID), string(task.AssigneeID), event.Sequence, detail.InboxCutoffSequence})
		}
	}
	if len(boundaries) == 0 {
		return nil
	}
	body, err := json.Marshal(boundaries)
	if err != nil {
		return err
	}
	// Team membership is evaluated at the start, while candidate events are
	// bounded by its earlier persisted inbox cutoff. Neither uses correlation.
	where := `organization_id=? AND EXISTS (
 SELECT 1 FROM json_each(?) b WHERE events.sequence<=json_extract(b.value,'$.cutoff') AND (
 (events.recipient_scope='TASK' AND events.recipient_id=json_extract(b.value,'$.task')) OR
 (events.recipient_scope='AGENT' AND events.recipient_id=json_extract(b.value,'$.agent')) OR
 (events.recipient_scope='TEAM' AND EXISTS (
 SELECT 1 FROM events team, json_each(CASE WHEN json_valid(team.payload) THEN json_extract(team.payload,'$.projection.value.member_agent_ids') END) member
 WHERE team.organization_id=events.organization_id AND team.sequence<json_extract(b.value,'$.start')
 AND CASE WHEN json_valid(team.payload) THEN json_extract(team.payload,'$.projection.projection_kind') END='team'
 AND json_extract(team.payload,'$.projection.record_id')=events.recipient_id
 AND member.value=json_extract(b.value,'$.agent')
 AND NOT EXISTS (SELECT 1 FROM events newer WHERE newer.organization_id=team.organization_id AND newer.sequence>team.sequence AND newer.sequence<json_extract(b.value,'$.start') AND CASE WHEN json_valid(newer.payload) THEN json_extract(newer.payload,'$.projection.projection_kind') END='team' AND json_extract(newer.payload,'$.projection.record_id')=events.recipient_id)
 ))))`
	args := []any{d.organization, string(body)}
	if err := d.validateInboxCandidates(ctx, tx, where, args); err != nil {
		return err
	}
	var ids []string
	for id := range d.stream {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > 0 {
		where += ` AND event_id NOT IN (` + incidentMarks(len(ids)) + `)`
		for _, id := range ids {
			args = append(args, id)
		}
	}
	loaded, err := incidentEvents(ctx, tx, &d.budget, where, args...)
	if err != nil {
		return fmt.Errorf("incident inbox candidates: %w", err)
	}
	for _, event := range loaded {
		if event.RecipientScope == events.RecipientTeam {
			d.key("team", event.RecipientID)
		}
		if err := d.add(event); err != nil {
			return err
		}
	}
	return nil
}

func validateIncidentManifests(ctx context.Context, tx *sql.Tx, stream []events.Event, inbox map[string]events.InboxObservationBinding) error {
	history := newInferenceExecutionHistory()
	// A non-nil map prevents the shared validator from querying global inbox rows.
	if inbox == nil {
		inbox = map[string]events.InboxObservationBinding{}
	}
	for _, event := range stream {
		if event.EventType == "INFERENCE_RESERVED" {
			var payload events.InferenceReservedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			if err := history.validateReservation(ctx, tx, event, payload, &inbox); err != nil {
				return err
			}
		}
		if err := history.observe(event); err != nil {
			return err
		}
	}
	return nil
}

// validateInboxCandidates checks both durable rows and event envelopes before
// the selected manifest can rely on an apparently empty recipient route.
func (d *incidentDependencies) validateInboxCandidates(ctx context.Context, tx *sql.Tx, where string, args []any) error {
	rowWhere := strings.ReplaceAll(where, "events.recipient_scope", "i.recipient_scope")
	rowWhere = strings.ReplaceAll(rowWhere, "events.recipient_id", "i.recipient_id")
	rowWhere = strings.ReplaceAll(rowWhere, "events.organization_id", "i.organization_id")
	rowWhere = strings.Replace(rowWhere, "organization_id=?", "i.organization_id=?", 1)
	rowWhere = strings.ReplaceAll(rowWhere, "events.sequence<=json_extract(b.value,'$.cutoff')", "(events.sequence IS NULL OR events.sequence<=json_extract(b.value,'$.cutoff'))")
	query := `SELECT i.event_id,i.organization_id,i.recipient_scope,i.recipient_id,events.event_id AS backing,events.organization_id AS event_org,events.recipient_scope AS event_scope,events.recipient_id AS event_recipient FROM inbox i LEFT JOIN events ON events.event_id=i.event_id WHERE ` + rowWhere + ` LIMIT ?`
	if d.inboxRows == nil {
		d.inboxRows = map[string]bool{}
	}
	seen := make([]string, 0, len(d.inboxRows))
	for id := range d.inboxRows {
		seen = append(seen, id)
	}
	seenBody, err := json.Marshal(seen)
	if err != nil {
		return err
	}
	query = strings.Replace(query, " LIMIT ?", ` AND i.event_id NOT IN (SELECT value FROM json_each(?)) LIMIT ?`, 1)
	bounded := append(append([]any{}, args...), string(seenBody), d.budget.events+1)
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(length(CAST(event_id AS BLOB))+length(CAST(organization_id AS BLOB))+length(CAST(recipient_scope AS BLOB))+length(CAST(recipient_id AS BLOB))),0) FROM (`+query+`)`, bounded...).Scan(&count, &size); err != nil {
		return err
	}
	if count > d.budget.events || size > d.budget.bytes {
		return fmt.Errorf("incident inbox candidates exceed support limit")
	}
	var invalid bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM (`+query+`) WHERE backing IS NULL OR organization_id<>event_org OR recipient_scope<>event_scope OR recipient_id<>event_recipient)`, bounded...).Scan(&invalid); err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("incident inbox candidate lacks exact event binding")
	}
	// The inverse check includes selected/public events too, not just newly loaded
	// dependencies. An addressed event cannot disappear by losing its inbox row.
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE `+where+` AND event_type<>'INBOX_EVENTS_OBSERVED' AND NOT EXISTS(SELECT 1 FROM inbox i WHERE i.event_id=events.event_id AND i.organization_id=events.organization_id AND i.recipient_scope=events.recipient_scope AND i.recipient_id=events.recipient_id))`, args...).Scan(&invalid); err != nil {
		return err
	}
	if invalid {
		return fmt.Errorf("incident addressed event lacks exact inbox backing")
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM (`+query+`)`, bounded...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		d.inboxRows[id] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	d.budget.events -= count
	d.budget.bytes -= size
	return nil
}
