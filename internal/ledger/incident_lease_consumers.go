package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// Selected lease admissions already pass through add's organization check:
// loadAuthorities selects each admission event by its global event identity.
// Their successful exact authority validation therefore binds them to this
// organization. A foreign governed Knowledge claim against those identities
// must fail even when its capability check is otherwise absent from the incident.
func validateIncidentLeaseConsumers(ctx context.Context, tx *sql.Tx, organization string, keys map[incidentKey]bool) error {
	var ids []string
	for key := range keys {
		if key.kind == "capability_lease" {
			ids = append(ids, key.id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	args := []any{organization}
	for _, id := range ids {
		args = append(args, id)
	}
	var conflict bool
	if err := tx.QueryRowContext(ctx, incidentLeaseConsumerSQL(len(ids)), args...).Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("incident capability lease has a cross-organization Knowledge consumer")
	}
	return nil
}

// Enumerate consuming Knowledge sources once, then probe capability checks by
// global event ID. This does not walk every user of shared authority or repeat
// a history scan for every selected lease. The boolean preflight allocates no
// source payload in Go and shares the incident's read transaction and context.
func incidentLeaseConsumerSQL(leases int) string {
	organization := `(SELECT organization FROM selected_scope)`
	// Only governed judgments accept CAPABILITY_CHECKED in validation_refs.
	// Valid candidates have no validation refs and terminal revisions preserve
	// their admitted validation. Consumption itself retains applicability when
	// malformed metadata removes both the method and principal discriminators.
	claim := `(` + incidentScalarClaim("check_event.payload", "$.lease_id", ` IN (SELECT id FROM selected_leases)`) + ` OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(check_event.authorization_refs) THEN check_event.authorization_refs ELSE '[]' END) authorization WHERE authorization.type='text' AND authorization.value IN (SELECT id FROM selected_leases)))`
	refs := func(body, path string) string {
		return `(SELECT json_group_array(value) FROM (` + incidentClaimQuery(body, path+".validation_refs", true, "") + `))`
	}
	foreign := func(body, path, envelope string) string {
		return `(check_event.organization_id<>` + organization + ` OR ` + envelope + `<>` + organization + ` OR ` + incidentScalarClaim(body, path+".organization_id", `<>`+organization) + `)`
	}
	var labels []string
	for _, label := range events.ProjectionLifecycleEventTypes("knowledge") {
		labels = append(labels, "'"+strings.ReplaceAll(label, "'", "''")+"'")
	}
	labelSQL := strings.Join(labels, ",")
	// The counterpart supplies an independent owner channel, never its refs:
	// corrupting both discriminators on the claiming source cannot hide the
	// occurrence still owned by its retained projection admission relationship.
	eventOwner := `(consumer.event_type IN (` + labelSQL + `) OR ` + incidentScalarClaim("consumer.payload", "$.projection.projection_kind", `='knowledge'`) + ` OR EXISTS (
SELECT 1 FROM records backing WHERE backing.admission_event_id<>'' AND backing.admission_event_id=consumer.event_id AND
(backing.kind='knowledge' OR ` + incidentScalarClaim("backing.body", "$.projection_kind", `='knowledge'`) + `)))`
	// Generic records without admission metadata remain opaque even when their
	// note body quotes a Knowledge projection. Admission metadata makes a moved
	// body discriminator an attempted authority claim subject to exact recovery.
	recordOwner := `(consumer.kind='knowledge' OR ((consumer.admission_event_id<>'' OR consumer.admission_fingerprint<>'') AND
(` + incidentScalarClaim("consumer.body", "$.projection_kind", `='knowledge'`) + ` OR admission.event_type IN (` + labelSQL + `) OR ` + incidentScalarClaim("admission.payload", "$.projection.projection_kind", `='knowledge'`) + `)))`
	values := strings.TrimSuffix(strings.Repeat("(?),", leases), ",")
	// Keep references before check lookup. Reordering the check table ahead of
	// json_each can repeat each consumer's JSON expansion for every check.
	return `WITH selected_scope(organization) AS (VALUES (?)), selected_leases(id) AS (VALUES ` + values + `)
SELECT EXISTS (SELECT 1 FROM events consumer JOIN json_each(` + refs("consumer.payload", "$.projection.value") + `) reference
CROSS JOIN events check_event ON check_event.event_id=reference.value AND check_event.event_type='CAPABILITY_CHECKED'
WHERE ` + eventOwner + ` AND ` + foreign("consumer.payload", "$.projection.value", "consumer.organization_id") + ` AND ` + claim + `)
OR EXISTS (SELECT 1 FROM records consumer LEFT JOIN events admission ON admission.event_id=consumer.admission_event_id
JOIN json_each(` + refs("consumer.body", "$.value") + `) reference
CROSS JOIN events check_event ON check_event.event_id=reference.value AND check_event.event_type='CAPABILITY_CHECKED'
WHERE ` + recordOwner + ` AND ` + foreign("consumer.body", "$.value", "admission.organization_id") + ` AND ` + claim + `)`
}
