package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// Organization ownership is not an incident dependency. Before scoped reads,
// reject retained projections whose owned identity contradicts the selected
// Organization. Inspect both sources independently: either can retain a claim
// after the other source is removed or corrupted. The scalar preflight scans
// retained source JSON in SQLite without allocating those payloads in Go.
func validateIncidentProjectionScope(ctx context.Context, tx *sql.Tx, organization string) error {
	var conflict bool
	query := `SELECT EXISTS(SELECT 1 FROM events e WHERE ` + projectionScopeClaims("event") + `)
		OR EXISTS(SELECT 1 FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id
		WHERE (r.kind IN (` + incidentProjectionKindsSQL + `) OR r.admission_event_id<>'' OR r.admission_fingerprint<>'')
		AND ` + projectionScopeClaims("record") + `)`
	if err := tx.QueryRowContext(ctx, query, organization).Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("incident projection organization claim conflicts with its retained source")
	}
	return nil
}

// These kinds are the closed admitted projection family. Work has no direct
// organization claim; Task owns only routing.organization_id. Event lifecycle
// labels and physical record kinds remain independent applicability channels
// when a payload discriminator is missing or corrupted.
var projectionScopeKinds = []string{
	"organization", "mission", "goal", "team", "agent_blueprint",
	"execution_profile", "agent", "intent", "work", "lab_experiment",
	"lab_promotion_candidate", "knowledge", "task",
}

func projectionScopeClaims(source string) string {
	conflict := `(e.organization_id<>c.organization AND (e.organization_id=s.organization OR c.organization=s.organization))`
	if source == "record" {
		// Only Organization has a globally keyed Organization identity. A Team
		// orphan's organization ownership alone does not establish a conflicting
		// envelope, so it remains subject to normal incident applicability.
		conflict = `(e.event_id IS NOT NULL AND ` + conflict + `)
			OR (c.kind='organization' AND r.record_id<>c.organization AND (r.record_id=s.organization OR c.organization=s.organization))`
	}
	return `EXISTS(WITH ` + projectionClaimNodes(source) + `,
		claims AS (
			SELECT o.kind,l.value AS organization FROM owners o JOIN nodes v ON v.parent=o.id AND v.key='value' AND v.type='object'
			JOIN nodes l ON l.parent=v.id AND l.type='text'
			WHERE (o.kind='organization' AND l.key='id') OR (o.kind NOT IN ('organization','work','task') AND l.key='organization_id')
			UNION ALL
			SELECT o.kind,l.value FROM owners o JOIN nodes v ON v.parent=o.id AND v.key='value' AND v.type='object'
			JOIN nodes routing ON routing.parent=v.id AND routing.key='routing' AND routing.type='object'
			JOIN nodes l ON l.parent=routing.id AND l.key='organization_id' AND l.type='text' WHERE o.kind='task'
			UNION ALL
			SELECT o.kind,l.value FROM owners o JOIN nodes l ON l.parent=o.id AND l.key='record_id' AND l.type='text' WHERE o.kind='organization'
		),
		s AS (SELECT ?1 AS organization)
		SELECT 1 FROM claims c,s WHERE ` + conflict + `)`
}

// Shared source occurrence tree for the two independent identity preflights.
func projectionClaimNodes(source string) string {
	body, projectionNodes := "e.payload", `SELECT id FROM nodes WHERE parent=0 AND key='projection' AND type='object'`
	if source == "record" {
		body, projectionNodes = "r.body", `SELECT id FROM nodes WHERE parent IS NULL AND type='object'`
	}
	owners := `SELECT p.id, k.value AS kind FROM projection_nodes p JOIN nodes k ON k.parent=p.id
		WHERE k.key='projection_kind' AND k.type='text' AND k.value IN (` + incidentProjectionKindsSQL + `)`
	if source == "record" {
		owners += ` UNION SELECT p.id,r.kind FROM projection_nodes p WHERE r.kind IN (` + incidentProjectionKindsSQL + `)`
	} else {
		owners += ` UNION SELECT p.id,r.kind FROM projection_nodes p JOIN records r ON r.admission_event_id=e.event_id AND r.admission_event_id<>'' WHERE r.kind IN (` + incidentProjectionKindsSQL + `)`
	}
	for _, kind := range projectionScopeKinds {
		labels := events.ProjectionLifecycleEventTypes(kind)
		if len(labels) == 0 {
			continue
		}
		quoted := make([]string, len(labels))
		for i, label := range labels {
			// All identifiers come from the owning closed lifecycle contracts.
			quoted[i] = "'" + strings.ReplaceAll(label, "'", "''") + "'"
		}
		owners += ` UNION SELECT p.id,'` + kind + `' FROM projection_nodes p WHERE e.event_type IN (` + strings.Join(quoted, ",") + `)`
	}
	return `nodes AS MATERIALIZED (SELECT id,parent,key,type,value FROM json_tree(CASE WHEN json_valid(` + body + `) THEN ` + body + ` ELSE '{}' END)),
		projection_nodes AS (` + projectionNodes + `),
		owners AS (` + owners + `)`
}
