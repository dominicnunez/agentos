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
	query := `SELECT EXISTS(SELECT 1 FROM events e WHERE CASE WHEN ` + projectionEventContainerBytes("e.payload") + ` THEN
		(e.organization_id=?1 OR ` + projectionSourceIdentityBytes("e.payload", "?1") + `) AND ` + projectionScopeClaims("event") + ` ELSE 0 END)
		OR EXISTS(SELECT 1 FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id
		WHERE (r.kind IN (` + incidentProjectionKindsSQL + `) OR r.admission_event_id<>'' OR r.admission_fingerprint<>'')
		AND (e.organization_id=?1 OR r.record_id=?1 OR ` + projectionSourceIdentityBytes("r.body", "?1") + `)
		AND ` + projectionScopeClaims("record") + `)`
	if err := tx.QueryRowContext(ctx, query, organization).Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("incident projection organization claim conflicts with its retained source")
	}
	return nil
}

// A decoded selected string occurs literally in an unescaped JSON source, or
// its source contains an escape. BLOB operations preserve bytes after raw NUL.
// This only avoids an occurrence tree for definitely unrelated source bytes;
// the unchanged full scalar owner guard still decides every retained candidate.
func projectionSourceIdentityBytes(body, identity string) string {
	return `(instr(CAST(` + body + ` AS BLOB),CAST(` + identity + ` AS BLOB))>0 OR instr(CAST(` + body + ` AS BLOB),X'5C')>0)`
}

// Event claims need a root projection object; an envelope or counterpart owner
// cannot create one. Its exact decoded key is literal quoted source bytes when
// unescaped. Keep every source escape, including escaped container keys.
func projectionEventContainerBytes(body string) string {
	return `(instr(CAST(` + body + ` AS BLOB),X'5C')>0 OR ` + projectionSourceKeyBytes(body, "projection") + `)`
}

func projectionSourceKeyBytes(body, key string) string {
	return `instr(CAST(` + body + ` AS BLOB),X'` + fmt.Sprintf("%x", []byte(`"`+key+`"`)) + `')>0`
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
	lifecycle := []string{}
	for _, kind := range projectionScopeKinds {
		labels := events.ProjectionLifecycleEventTypes(kind)
		for _, label := range labels {
			// All identifiers come from the owning closed lifecycle contracts.
			lifecycle = append(lifecycle, "('"+strings.ReplaceAll(label, "'", "''")+"','"+kind+"')")
		}
	}
	// The event label is one independent owner channel. A closed relation
	// avoids rebuilding one correlated UNION branch for every family and row.
	owners += ` UNION SELECT p.id,l.kind FROM projection_nodes p JOIN lifecycle l ON l.label=e.event_type`
	return `nodes AS MATERIALIZED (SELECT id,parent,key,type,value FROM json_tree(CASE WHEN json_valid(` + body + `) THEN ` + body + ` ELSE '{}' END)),
		projection_nodes AS (` + projectionNodes + `),
		lifecycle(label,kind) AS (VALUES ` + strings.Join(lifecycle, ",") + `),
		owners AS (` + owners + `)`
}
