package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// Validate every selected projection against the shared exact admission reader,
// including projection kinds not needed to discover this incident's Tasks.
func validateIncidentRecords(ctx context.Context, tx *sql.Tx, stream []events.Event) error {
	args := []any{}
	wanted := map[string]bool{}
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		if err := events.ValidateProjectionEventBoundary(event, payload); err != nil {
			return err
		}
		args = append(args, event.EventID)
		wanted[event.EventID] = true
	}
	if len(args) == 0 {
		return nil
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
	where := ` WHERE admission_event_id IN (` + marks + `)`
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(length(CAST(body AS BLOB))+length(CAST(kind AS BLOB))+length(CAST(record_id AS BLOB))+length(CAST(admission_event_id AS BLOB))+length(CAST(admission_fingerprint AS BLOB))),0) FROM (SELECT * FROM records`+where+` LIMIT 257)`, args...).Scan(&count, &size); err != nil {
		return err
	}
	if count != len(wanted) || size > 2<<20 {
		return fmt.Errorf("incident projection records are missing or exceed bounded snapshot")
	}
	// Selected event bytes were already bounded to 2 MiB before allocation;
	// their exact backing records above have a separate 2 MiB aggregate bound.
	records, err := admittedProjectionRecordsBounded(ctx, tx, 4<<20, `WHERE r.admission_event_id IN (`+marks+`) ORDER BY e.sequence LIMIT 257`, args...)
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
