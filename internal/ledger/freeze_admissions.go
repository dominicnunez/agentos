package ledger

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dominicnunez/agentos/internal/events"
)

// scopedAuthorityAdmissions resolves capability history directly and reuses
// each organization's complete freeze proof from the exact transaction scope.
func scopedAuthorityAdmissions(ctx context.Context, tx *sql.Tx, scope *freezeReadScope) ([]events.CapabilityLeaseAdmission, []events.OrganizationFreezeAdmission, error) {
	if tx == nil || scope == nil || scope.tx != tx {
		return nil, nil, fmt.Errorf("authority admission scope does not match its transaction")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE event_type IN ('CAPABILITY_GRANTED','CAPABILITY_REVOKED') ORDER BY sequence`))
	if err != nil {
		return nil, nil, fmt.Errorf("read capability Event Contracts: %w", err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT kind,record_id,version,body,admission_event_id FROM records
WHERE kind='capability_lease' ORDER BY record_id,version`)
	if err != nil {
		return nil, nil, fmt.Errorf("read capability records: %w", err)
	}
	var records []events.AuthorityRecord
	for rows.Next() {
		var record events.AuthorityRecord
		if err := rows.Scan(&record.Kind, &record.RecordID, &record.Version, &record.Body, &record.AdmissionEventID); err != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("scan capability record: %w", err)
		}
		record.Body = append([]byte(nil), record.Body...)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, fmt.Errorf("iterate capability records: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("close capability records: %w", err)
	}
	leasing, _, err := events.ResolveAuthorityAdmissions(stream, records)
	if err != nil {
		return nil, nil, err
	}

	organizations, err := tx.QueryContext(ctx, `SELECT organization_id FROM (
	SELECT record_id AS organization_id FROM records WHERE kind='organization_freeze'
	UNION
	SELECT organization_id FROM events WHERE event_type='FREEZE_SET'
) ORDER BY organization_id`)
	if err != nil {
		return nil, nil, fmt.Errorf("read freeze organizations: %w", err)
	}
	var organizationIDs []string
	for organizations.Next() {
		var organization string
		if err := organizations.Scan(&organization); err != nil {
			_ = organizations.Close()
			return nil, nil, fmt.Errorf("scan freeze organization: %w", err)
		}
		organizationIDs = append(organizationIDs, organization)
	}
	if err := organizations.Err(); err != nil {
		_ = organizations.Close()
		return nil, nil, fmt.Errorf("iterate freeze organizations: %w", err)
	}
	if err := organizations.Close(); err != nil {
		return nil, nil, fmt.Errorf("close freeze organizations: %w", err)
	}

	freezes := make([]events.OrganizationFreezeAdmission, 0)
	for _, organization := range organizationIDs {
		history, err := scope.read(ctx, organization)
		if err != nil {
			return nil, nil, err
		}
		for _, revision := range history.revisions {
			control := revision.state.Control
			if control != nil {
				copy := *control
				control = &copy
			}
			freezes = append(freezes, events.OrganizationFreezeAdmission{
				OrganizationID: revision.state.OrganizationID,
				EventRef:       revision.event.EventID,
				Frozen:         revision.state.Frozen,
				Sequence:       revision.event.Sequence,
				Version:        revision.record.Version,
				Control:        control,
			})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return leasing, freezes, nil
}
