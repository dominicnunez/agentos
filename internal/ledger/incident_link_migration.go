package ledger

import (
	"context"
	"database/sql"
	"fmt"
)

// Expand only a complete, guarded v13 index. Missing old links are corruption,
// not a reason to repair an unverified index while adding the new grammar.
func migrateIncidentEvidenceLinks(ctx context.Context, tx *sql.Tx) error {
	return migrateIncidentLinkVersion(ctx, tx, 1, 2)
}

func migrateIncidentLifecycleLinks(ctx context.Context, tx *sql.Tx) error {
	return migrateIncidentLinkVersion(ctx, tx, 2, 3)
}

func migrateIncidentLinkVersion(ctx context.Context, tx *sql.Tx, previous, next int) error {
	if err := validateIncidentLinkGrammar(ctx, tx, previous); err != nil {
		return err
	}
	if err := validateIncidentLinkVersion(ctx, tx, previous); err != nil {
		return err
	}
	for _, object := range incidentLinkObjects {
		if object.kind != "trigger" {
			continue
		}
		if _, err := tx.ExecContext(ctx, "DROP TRIGGER "+object.name); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, incidentLinkGrammar(object.sql, next)); err != nil {
			return err
		}
	}
	for _, record := range []bool{false, true} {
		table, source, alias, identity := "incident_event_links", "events", "e", "e.sequence,e.event_id"
		if record {
			table, source, alias, identity = "incident_record_links", "records", "r", "r.kind,r.record_id,r.version,r.admission_event_id"
		}
		statement := "INSERT OR IGNORE INTO " + table + " SELECT json_extract(link.value,'$.kind'),json_extract(link.value,'$.id')," + identity + " FROM " + source + " " + alias + " JOIN json_each(" + incidentLinkJSON(record, alias) + ") link"
		if _, err := tx.ExecContext(ctx, incidentLinkGrammar(statement, next)); err != nil {
			return fmt.Errorf("backfill incident evidence links: %w", err)
		}
	}
	if err := validateIncidentLinkGrammar(ctx, tx, next); err != nil {
		return err
	}
	return validateIncidentLinkVersion(ctx, tx, next)
}
