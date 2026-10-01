package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/events"
)

// Validate owned global identity claims after dependency closure has determined
// the required keys. A different source can claim those keys in its value while
// its projection/storage key remains unrelated. Reject the contradiction in a
// scalar preflight, retaining the public/private item and byte bounds.
func validateIncidentProjectionIDs(ctx context.Context, tx *sql.Tx, keys map[incidentKey]bool) error {
	type selectedIdentity struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	}
	selected := make([]selectedIdentity, 0, len(keys))
	for key := range keys {
		if events.ProjectionKindRequiresAdmission(key.kind) || key.kind == "capability_lease" || key.kind == "event" {
			selected = append(selected, selectedIdentity{key.kind, key.id})
		}
	}
	if len(selected) == 0 {
		return nil
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].Kind != selected[j].Kind {
			return selected[i].Kind < selected[j].Kind
		}
		return selected[i].ID < selected[j].ID
	})
	body, err := json.Marshal(selected)
	if err != nil {
		return err
	}
	query := `WITH selected AS MATERIALIZED (SELECT json_extract(value,'$.kind') AS kind,json_extract(value,'$.id') AS identity FROM json_each(?1))
		SELECT EXISTS(SELECT 1 FROM events e WHERE ` + projectionIdentityClaims("event") + `)
		OR EXISTS(SELECT 1 FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id
		WHERE (r.kind IN (` + incidentProjectionKindsSQL + `,'capability_lease') OR r.admission_event_id<>'' OR r.admission_fingerprint<>'')
		AND ` + projectionIdentityClaims("record") + `)`
	var conflict bool
	if err := tx.QueryRowContext(ctx, query, body).Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("incident retained source claims a conflicting selected identity")
	}
	return nil
}

func projectionIdentityClaims(source string) string {
	physical := `OR (c.kind='organization' AND e.organization_id<>s.identity) OR (c.kind='task' AND e.task_id<>s.identity)
		OR EXISTS(SELECT 1 FROM records backing WHERE backing.admission_event_id=e.event_id AND backing.admission_event_id<>'' AND (backing.kind<>c.kind OR backing.record_id<>s.identity))`
	leaseOwner, leaseConflict := `(e.event_type IN ('CAPABILITY_GRANTED','CAPABILITY_REVOKED') OR EXISTS(SELECT 1 FROM records backing WHERE backing.admission_event_id=e.event_id AND backing.admission_event_id<>'' AND backing.kind='capability_lease'))`, `EXISTS(SELECT 1 FROM records backing WHERE backing.admission_event_id=e.event_id AND backing.admission_event_id<>'' AND (backing.kind<>'capability_lease' OR backing.record_id<>s.identity))`
	admission := `UNION ALL SELECT 1 FROM nodes a JOIN nodes l ON l.parent=a.id JOIN selected s ON s.kind='event' AND s.identity=l.value
		WHERE a.parent=0 AND a.key='admission' AND a.type='object' AND l.key='event_ref' AND l.type='text' AND e.event_id<>s.identity`
	if source == "record" {
		physical = `OR r.kind<>c.kind OR r.record_id<>s.identity`
		leaseOwner, leaseConflict = `(r.kind='capability_lease' OR e.event_type IN ('CAPABILITY_GRANTED','CAPABILITY_REVOKED'))`, `(r.kind<>'capability_lease' OR r.record_id<>s.identity)`
		admission = ""
	}
	return `EXISTS(WITH ` + projectionClaimNodes(source) + `,
		owned_ids AS (
			SELECT o.id,o.kind,l.value AS identity FROM owners o JOIN nodes v ON v.parent=o.id AND v.key='value' AND v.type='object'
			JOIN nodes l ON l.parent=v.id AND l.type='text' AND l.key=CASE WHEN o.kind='knowledge' THEN 'knowledge_id' ELSE 'id' END
			UNION ALL SELECT o.id,o.kind,l.value FROM owners o JOIN nodes l ON l.parent=o.id AND l.key='record_id' AND l.type='text'
		)
		SELECT 1 FROM owned_ids c JOIN selected s ON s.kind=c.kind AND s.identity=c.identity WHERE
			NOT EXISTS(SELECT 1 FROM nodes l WHERE l.parent=c.id AND l.key='record_id' AND l.type='text' AND l.value=s.identity)
			OR NOT EXISTS(SELECT 1 FROM nodes v JOIN nodes l ON l.parent=v.id WHERE v.parent=c.id AND v.key='value' AND v.type='object' AND l.type='text' AND l.key=CASE WHEN c.kind='knowledge' THEN 'knowledge_id' ELSE 'id' END AND l.value=s.identity)
			OR EXISTS(SELECT 1 FROM owned_ids other WHERE other.id=c.id AND other.kind=c.kind AND other.identity<>s.identity)
			` + physical + `
		UNION ALL
		SELECT 1 FROM nodes l JOIN selected s ON s.kind='capability_lease' AND s.identity=l.value
		WHERE l.parent=0 AND l.key='id' AND l.type='text' AND ` + leaseOwner + ` AND (` + leaseConflict + `)
		` + admission + `)`
}
