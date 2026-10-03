package ledger

import (
	"database/sql/driver"
	"encoding/json"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
	"modernc.org/sqlite"
)

// An admitted physical record remains an owner when both the event label and
// discriminator are damaged. Only malformed owner metadata uses this guard;
// ordinary typed sources continue through the durable incoming index.
func init() {
	for _, record := range []bool{false, true} {
		name := "agentos_incident_counterpart_ref_v1"
		if record {
			name = "agentos_incident_record_owner_ref_v1"
		}
		sqlite.MustRegisterDeterministicScalarFunction(name, 3, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			kind, _ := args[1].(string)
			if !events.ProjectionKindRequiresAdmission(kind) {
				return false, nil
			}
			var encoded []byte
			switch value := args[2].(type) {
			case string:
				encoded = []byte(value)
			case []byte:
				encoded = value
			}
			var selected []incidentSelector
			if err := json.Unmarshal(encoded, &selected); err != nil {
				return nil, err
			}
			keys := make(map[incidentKey]bool, len(selected))
			for _, key := range selected {
				keys[incidentKey{key.Kind, key.ID}] = true
			}
			rules := append(append([]incidentLinkRule(nil), incidentLinkRules...), incidentEvidenceLinkRules...)
			rules = append(rules, incidentDetailLinkRules...)
			rules = append(rules, incidentManifestRecordLinks...)
			var owned []incidentLinkRule
			for _, rule := range rules {
				if rule.payload || record && rule.detail || !incidentLinkSource(rule.sources, kind) {
					continue
				}
				// The physical owner survives the missing event label. Retain only
				// this owner's named detail paths, including their requires gates.
				if rule.detail {
					rule.eventTypes = ""
				}
				owned = append(owned, rule)
			}
			flag, sourceKind, counterpart := int64(0), "", kind
			if record {
				flag, sourceKind, counterpart = 1, kind, ""
			}
			found := visitIncidentLinkSources([]driver.Value{flag, sourceKind, args[0]}, "", owned, false, counterpart, func(target, id string) bool {
				return keys[incidentKey{target, id}]
			})
			return found, nil
		})
	}
}

func incidentCounterpartClaims() string {
	var labels []string
	var owners []string
	for _, kind := range projectionScopeKinds {
		var quoted []string
		for _, label := range events.ProjectionLifecycleEventTypes(kind) {
			quoted = append(quoted, "'"+strings.ReplaceAll(label, "'", "''")+"'")
			owners = append(owners, "('"+kind+"','"+strings.ReplaceAll(label, "'", "''")+"')")
		}
		labels = append(labels, `(r.kind='`+kind+`' AND e.event_type IN (`+strings.Join(quoted, ",")+`))`)
	}
	valid := `((` + strings.Join(labels, " OR ") + `) AND ` + incidentScalarClaim("e.payload", "$.projection.projection_kind", `=r.kind`) + `)`
	return `EXISTS(SELECT 1 FROM records r JOIN events e ON e.event_id=r.admission_event_id
WHERE r.admission_event_id<>'' AND r.kind IN (` + incidentProjectionKindsSQL + `) AND
CASE WHEN ` + projectionSelectedSourceBytes("e.payload") + ` THEN CASE WHEN ` + valid + ` THEN 0 ELSE
agentos_incident_counterpart_ref_v1(e.payload,r.kind,?1) END ELSE 0 END)
OR EXISTS(WITH ownership(kind,label) AS (VALUES ` + strings.Join(owners, ",") + `)
SELECT 1 FROM records r JOIN events e ON e.event_id=r.admission_event_id
WHERE r.admission_event_id<>'' AND CASE WHEN ` + projectionSelectedSourceBytes("r.body") + ` THEN EXISTS(
SELECT 1 FROM (SELECT DISTINCT kind FROM ownership) owner
WHERE CASE WHEN r.kind=owner.kind AND ` + incidentScalarClaim("r.body", "$.projection_kind", `=owner.kind`) + ` THEN 0
ELSE CASE WHEN
(EXISTS(SELECT 1 FROM ownership o WHERE o.kind=owner.kind AND o.label=e.event_type) OR ` + incidentScalarClaim("e.payload", "$.projection.projection_kind", `=owner.kind`) + `)
THEN agentos_incident_record_owner_ref_v1(r.body,owner.kind,?1) ELSE 0 END END) ELSE 0 END)`
}
