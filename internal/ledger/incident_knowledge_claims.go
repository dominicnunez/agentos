package ledger

import (
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
)

// Raw Knowledge statements acquire evidence semantics when a durable Knowledge
// admission consumes them. Keep event and record consumers independent so a
// missing or displaced counterpart cannot hide the remaining claim.
func incidentKnowledgeClaims(membership string) string {
	equals := func(field, value string) func(string, string) string {
		return func(source, path string) string {
			return incidentScalarClaim(source, path+"."+field, `='`+value+`'`)
		}
	}
	proposals := incidentKnowledgeConsumer("provenance_event_refs", equals("created_by_kind", string(core.PrincipalAgent)), false)
	validations := incidentKnowledgeConsumer("validation_refs", equals("validation_method", string(core.KnowledgeValidationDeterministic)), true)
	judgments := incidentKnowledgeConsumer("validation_refs", func(source, path string) string {
		// Method validation consumes statements; governed principals also
		// consume their exact capability binding at activation.
		return `(` + incidentScalarClaim(source, path+".validation_method", ` IN ('`+string(core.KnowledgeValidationHuman)+`','`+string(core.KnowledgeValidationIndependentAgent)+`')`) + ` OR ` + incidentScalarClaim(source, path+".validated_by_kind", ` IN ('`+string(core.PrincipalHuman)+`','`+string(core.PrincipalAgent)+`','`+string(core.PrincipalExternalAgent)+`')`) + `)`
	}, true)
	return `(event_type='KNOWLEDGE_PROPOSED' AND ` + incidentArrayClaim("payload", "$.occurrence_event_refs", ` IN (`+membership+`)`) + ` AND (` + proposals + `)) OR
(event_type='KNOWLEDGE_VALIDATION_RECORDED' AND ` + incidentScalarClaim("payload", "$.outcome_event_ref", ` IN (`+membership+`)`) + ` AND (` + validations + `)) OR
(event_type IN ('HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED','KNOWLEDGE_JUDGMENT_PUBLISHED') AND ` + incidentScalarClaim("payload", "$.capability_check_event_id", ` IN (`+membership+`)`) + ` AND (` + judgments + `))`
}

func incidentKnowledgeConsumer(field string, predicate func(string, string) string, activation bool) string {
	return incidentKnowledgeConsumerFor("events", field, predicate, activation)
}

func incidentKnowledgeConsumerFor(source, field string, predicate func(string, string) string, activation bool) string {
	eventCondition, recordCondition := "", ""
	if activation {
		eventCondition = ` AND consumer.event_type='KNOWLEDGE_ACTIVATED'`
		recordCondition = ` AND ` + incidentScalarClaim("consumer.body", "$.value.status", `='ACTIVE'`)
	}
	return `EXISTS (SELECT 1 FROM incident_event_links link JOIN events consumer ON consumer.sequence=link.event_sequence AND consumer.event_id=link.event_id
WHERE link.target_kind='event' AND link.target_id=` + source + `.event_id AND
` + incidentScalarClaim("consumer.payload", "$.projection.projection_kind", `='knowledge'`) + ` AND ` + predicate("consumer.payload", "$.projection.value") + eventCondition + ` AND
` + incidentArrayClaim("consumer.payload", "$.projection.value."+field, `=`+source+`.event_id`) + `) OR
EXISTS (SELECT 1 FROM incident_record_links link JOIN records consumer ON consumer.kind=link.record_kind AND consumer.record_id=link.record_id AND consumer.version=link.record_version
WHERE link.target_kind='event' AND link.target_id=` + source + `.event_id AND consumer.kind='knowledge' AND
` + predicate("consumer.body", "$.value") + recordCondition + ` AND
` + incidentArrayClaim("consumer.body", "$.value."+field, `=`+source+`.event_id`) + `)`
}

func incidentKnowledgeIncoming(source string) string {
	proposal := incidentKnowledgeConsumerFor(source, "provenance_event_refs", func(body, path string) string {
		return incidentScalarClaim(body, path+".created_by_kind", `='AGENT'`)
	}, false)
	validation := incidentKnowledgeConsumerFor(source, "validation_refs", func(body, path string) string {
		return incidentScalarClaim(body, path+".validation_method", `='`+string(core.KnowledgeValidationDeterministic)+`'`)
	}, true)
	judgment := incidentKnowledgeConsumerFor(source, "validation_refs", func(body, path string) string {
		return `(` + incidentScalarClaim(body, path+".validation_method", ` IN ('`+string(core.KnowledgeValidationHuman)+`','`+string(core.KnowledgeValidationIndependentAgent)+`')`) + ` OR ` + incidentScalarClaim(body, path+".validated_by_kind", ` IN ('HUMAN','AGENT','EXTERNAL_AGENT')`) + `)`
	}, true)
	return `(CASE WHEN ` + source + `.event_type NOT IN ('KNOWLEDGE_PROPOSED','KNOWLEDGE_VALIDATION_RECORDED','KNOWLEDGE_JUDGMENT_PUBLISHED','HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED') THEN 1
WHEN ` + incidentScalarClaim(source+".payload", "$.projection.projection_kind", `='knowledge'`) + ` THEN 1
WHEN ` + source + `.event_type='KNOWLEDGE_PROPOSED' THEN (` + proposal + `)
WHEN ` + source + `.event_type='KNOWLEDGE_VALIDATION_RECORDED' THEN (` + validation + `)
ELSE (` + judgment + `) END)`
}

// json_tree decodes member keys while fullkey retains their source spelling.
// Match each owned object ancestor by key and parent, including duplicate and
// escaped aliases. Arrays contribute only their immediate string elements.
// Paths and comparisons are internal SQL; identities remain bound arguments.
func incidentScalarClaim(source, path, comparison string) string {
	return `EXISTS (` + incidentClaimQuery(source, path, false, comparison) + `)`
}

func incidentArrayClaim(source, path, comparison string) string {
	return `EXISTS (` + incidentClaimQuery(source, path, true, comparison) + `)`
}

// A correlated JSON array lets identity readers enumerate one source-sized
// tree once, rather than reparsing the document for every matching leaf.
func incidentClaimJSON(source, path string) string {
	return `(SELECT json_group_array(value) FROM (` + incidentClaimQuery(source, path, false, "") + `))`
}

func incidentClaimQuery(source, path string, array bool, comparison string) string {
	fields := strings.Split(strings.TrimPrefix(path, "$."), ".")
	tree := `json_tree(CASE WHEN json_valid(` + source + `) THEN ` + source + ` ELSE '{}' END)`
	nodes := `WITH claims AS MATERIALIZED (SELECT id,parent,key,type,value FROM ` + tree + `) `
	from, leaf, value, typed := `claims claim`, "claim", "claim.value", `claim.type='text'`
	if array {
		from = `claims ref JOIN claims claim ON claim.id=ref.parent`
		value = "ref.value"
		typed = `ref.type='text' AND claim.type='array'`
	}
	if !array && len(fields) == 1 {
		// Root scalar members need no ancestor self-join or materialized copy.
		nodes, from = "", tree+` claim`
	}
	predicate := typed + ` AND claim.key='` + fields[len(fields)-1] + `'`
	for index := len(fields) - 2; index >= 0; index-- {
		parent := fmt.Sprintf("parent%d", index)
		from += ` JOIN claims ` + parent + ` ON ` + parent + `.id=` + leaf + `.parent`
		predicate += ` AND ` + parent + `.type='object' AND ` + parent + `.key='` + fields[index] + `'`
		leaf = parent
	}
	predicate += ` AND ` + leaf + `.parent=0`
	if comparison != "" {
		predicate += ` AND ` + value + comparison
	}
	return nodes + `SELECT ` + value + ` AS value FROM ` + from + ` WHERE ` + predicate
}
