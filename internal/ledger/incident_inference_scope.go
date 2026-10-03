package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// Materialized Task identities are global; auxiliary planning identities are
// organization-scoped. Derive the distinction from selected admitted projections,
// never from the purpose claimed by a candidate reservation or accounting row.
// Request and execution strings remain organization-scoped even when an auxiliary
// request reuses the spelling of a materialized Task's execution identifier.
func validateIncidentInferenceScope(ctx context.Context, tx *sql.Tx, stream []events.Event) error {
	tasks := map[string]bool{}
	for _, event := range stream {
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if present && projection.Projection.ProjectionKind == "task" {
			tasks[projection.Projection.RecordID] = true
		}
	}
	if len(tasks) == 0 {
		return nil
	}
	manifests := map[string]bool{}
	for _, event := range stream {
		if !tasks[event.TaskID] {
			continue
		}
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
			manifests[event.EventID] = true
		}
	}
	// Both halves are independent: an event may have lost its accounting row,
	// and a row may have lost its event. Payload keys are enumerated so a duplicate
	// key cannot hide a selected identity behind json_extract's first match.
	args := []any{stream[0].OrganizationID}
	var rowParts, eventParts []string
	add := func(parts *[]string, column string, ids map[string]bool) {
		if len(ids) == 0 {
			return
		}
		ordered := make([]string, 0, len(ids))
		for id := range ids {
			ordered = append(ordered, id)
		}
		sort.Strings(ordered)
		*parts = append(*parts, column+` IN (`+incidentMarks(len(ordered))+`)`)
		for _, id := range ordered {
			args = append(args, id)
		}
	}
	add(&rowParts, "task_id", tasks)
	rowQuery := `SELECT 1 FROM inference_reservations WHERE organization_id<>? AND (` + strings.Join(rowParts, " OR ") + `)`
	args = append(args, stream[0].OrganizationID)
	add(&eventParts, "task_id", tasks)
	var payloadParts []string
	if len(manifests) != 0 {
		add(&payloadParts, "value", manifests)
		last := len(payloadParts) - 1
		payloadParts[last] = `(key='execution_manifest_ref' AND ` + payloadParts[last] + `)`
	}
	if len(payloadParts) != 0 {
		eventParts = append(eventParts, `EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END) WHERE `+strings.Join(payloadParts, " OR ")+`)`)
	}
	eventQuery := `SELECT 1 FROM events WHERE organization_id<>? AND event_type IN ('INFERENCE_RESERVED','INFERENCE_RECONCILED','INFERENCE_NOT_SENT','INFERENCE_USAGE_RECORDED') AND (` + strings.Join(eventParts, " OR ") + `)`
	var found bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(`+rowQuery+` UNION ALL `+eventQuery+`)`, args...).Scan(&found); err != nil {
		return err
	}
	if found {
		return fmt.Errorf("incident inference accounting crosses its materialized Task organization")
	}
	return nil
}
