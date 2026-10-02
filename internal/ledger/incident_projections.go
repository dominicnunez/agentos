package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

const incidentProjectionRecordBytes = `length(CAST(r.body AS BLOB))+length(CAST(r.kind AS BLOB))+length(CAST(r.record_id AS BLOB))+length(CAST(r.version AS BLOB))+length(CAST(r.admission_event_id AS BLOB))+length(CAST(r.admission_fingerprint AS BLOB))`
const incidentProjectionEventBytes = `COALESCE(length(CAST(e.event_id AS BLOB)),0)+COALESCE(length(CAST(e.organization_id AS BLOB)),0)+COALESCE(length(CAST(e.event_type AS BLOB)),0)+COALESCE(length(CAST(e.source_actor_id AS BLOB)),0)+COALESCE(length(CAST(e.source_execution_id AS BLOB)),0)+COALESCE(length(CAST(e.recipient_scope AS BLOB)),0)+COALESCE(length(CAST(e.recipient_id AS BLOB)),0)+COALESCE(length(CAST(e.task_id AS BLOB)),0)+COALESCE(length(CAST(e.authorization_refs AS BLOB)),0)+COALESCE(length(CAST(e.artifact_refs AS BLOB)),0)+COALESCE(length(CAST(e.payload AS BLOB)),0)+COALESCE(length(CAST(e.correlation_id AS BLOB)),0)+COALESCE(length(CAST(e.created_at AS BLOB)),0)+COALESCE(length(CAST(e.schema_version AS BLOB)),0)`
const incidentProjectionKindsSQL = `'organization','mission','goal','team','agent_blueprint','execution_profile','agent','intent','work','lab_experiment','lab_promotion_candidate','knowledge','task'`
const incidentProjectionOwnedOrganization = `CASE WHEN json_valid(r.body) THEN CASE WHEN r.kind='organization' THEN json_extract(r.body,'$.value.id') WHEN r.kind NOT IN ('work','task') THEN json_extract(r.body,'$.value.organization_id') END END`

// Validate every selected projection against the shared exact admission reader,
// including projection kinds not needed to discover this incident's Tasks.
func validateIncidentRecords(ctx context.Context, tx *sql.Tx, stream []events.Event, support *incidentSupport) error {
	if len(stream) == 0 {
		return nil
	}
	args := []any{}
	wanted := map[string]bool{}
	intentIDs := map[string]bool{}
	workIDs := map[string]bool{}
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			if events.RequiresProjectionAdmission(event.EventType, event.SourceActorID) {
				return fmt.Errorf("incident projection lifecycle event lacks its required admission")
			}
			continue
		}
		if !events.ProjectionKindRequiresAdmission(payload.Projection.ProjectionKind) {
			return fmt.Errorf("incident projection kind is unsupported")
		}
		if err := events.ValidateProjectionEventBoundary(event, payload); err != nil {
			return err
		}
		if payload.Projection.ProjectionKind == "intent" {
			intentIDs[payload.Projection.RecordID] = true
		}
		if payload.Projection.ProjectionKind == "work" {
			workIDs[payload.Projection.RecordID] = true
		}
		args = append(args, event.EventID)
		wanted[event.EventID] = true
	}
	selected := `0`
	if len(args) != 0 {
		marks := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
		selected = `r.admission_event_id IN (` + marks + `)`
	}
	organization, correlation := stream[0].OrganizationID, stream[0].CorrelationID
	args = append(args, correlation, organization, organization)
	where := `WHERE (` + selected + `) OR (r.kind IN (` + incidentProjectionKindsSQL + `) AND CASE WHEN json_valid(r.body) THEN json_extract(r.body,'$.correlation_id') END=? AND (e.organization_id=? OR (e.event_id IS NULL AND ` + incidentProjectionOwnedOrganization + `=?)))`
	for _, link := range []struct {
		kind, field string
		ids         map[string]bool
	}{{"work", "intent_id", intentIDs}, {"task", "work_id", workIDs}} {
		if len(link.ids) == 0 {
			continue
		}
		ids := make([]string, 0, len(link.ids))
		for id := range link.ids {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
		// SQL identifiers come only from the two fixed relationship definitions.
		where += fmt.Sprintf(` OR (r.kind='%s' AND (e.organization_id=? OR e.event_id IS NULL) AND (CASE WHEN json_valid(r.body) THEN json_extract(r.body,'$.value.%s') END IN (%s) OR CASE WHEN json_valid(e.payload) THEN json_extract(e.payload,'$.projection.value.%s') END IN (%s)))`, link.kind, link.field, marks, link.field, marks)
		args = append(args, organization)
		for _, id := range ids {
			args = append(args, id)
		}
		for _, id := range ids {
			args = append(args, id)
		}
	}
	var count int
	var recordBytes, eventBytes int64
	preflight := `SELECT COUNT(*),COALESCE(SUM(record_bytes),0),COALESCE(SUM(event_bytes),0) FROM (SELECT ` + incidentProjectionRecordBytes + ` AS record_bytes,` + incidentProjectionEventBytes + ` AS event_bytes FROM records AS r LEFT JOIN events AS e ON e.event_id=r.admission_event_id ` + where + ` LIMIT 257)`
	if err := tx.QueryRowContext(ctx, preflight, args...).Scan(&count, &recordBytes, &eventBytes); err != nil {
		return err
	}
	if count != len(wanted) || recordBytes > 2<<20 || eventBytes > 2<<20 {
		return fmt.Errorf("incident projection records are missing or exceed bounded snapshot")
	}
	if err := support.sourceRows(ctx, tx, "records", `SELECT r.rowid,`+incidentProjectionRecordBytes+` FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id `+where+` LIMIT 257`, args...); err != nil {
		return err
	}
	// Selected event bytes were already bounded to 2 MiB before allocation;
	// reverse-discovered same-organization admissions and their exact backing
	// records above have separate aggregate bounds before either is allocated.
	records, err := admittedProjectionRecordsBounded(ctx, tx, 4<<20, where+` ORDER BY e.sequence LIMIT 257`, args...)
	if err != nil {
		return err
	}
	for _, record := range records {
		if !wanted[record.event.EventID] {
			return fmt.Errorf("incident projection admission is duplicated or unexpected")
		}
		delete(wanted, record.event.EventID)
	}
	if len(wanted) != 0 {
		return fmt.Errorf("incident projection lacks its exact record")
	}
	return nil
}
