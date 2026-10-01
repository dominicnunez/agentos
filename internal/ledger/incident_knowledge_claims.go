package ledger

import "github.com/dominicnunez/agentos/internal/core"

// Raw Knowledge statements acquire evidence semantics when a durable Knowledge
// admission consumes them. Keep event and record consumers independent so a
// missing or displaced counterpart cannot hide the remaining claim.
func incidentKnowledgeClaims(membership string) string {
	equals := func(field, value string) func(string, string) string {
		return func(source, path string) string {
			return `json_extract(` + source + `,'` + path + `.` + field + `')='` + value + `'`
		}
	}
	proposals := incidentKnowledgeConsumer("provenance_event_refs", equals("created_by_kind", string(core.PrincipalAgent)), false)
	validations := incidentKnowledgeConsumer("validation_refs", equals("validation_method", string(core.KnowledgeValidationDeterministic)), true)
	judgments := incidentKnowledgeConsumer("validation_refs", func(source, path string) string {
		// Method validation consumes statements; governed principals also
		// consume their exact capability binding at activation.
		return `(json_extract(` + source + `,'` + path + `.validation_method') IN ('` + string(core.KnowledgeValidationHuman) + `','` + string(core.KnowledgeValidationIndependentAgent) + `') OR json_extract(` + source + `,'` + path + `.validated_by_kind') IN ('` + string(core.PrincipalHuman) + `','` + string(core.PrincipalAgent) + `','` + string(core.PrincipalExternalAgent) + `'))`
	}, true)
	return `(event_type='KNOWLEDGE_PROPOSED' AND EXISTS (
SELECT 1 FROM json_each(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END,'$.occurrence_event_refs') ref
WHERE ref.type='text' AND ref.value IN (` + membership + `)) AND (` + proposals + `)) OR
(event_type='KNOWLEDGE_VALIDATION_RECORDED' AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.outcome_event_ref') END IN (` + membership + `) AND (` + validations + `)) OR
(event_type IN ('HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED','KNOWLEDGE_JUDGMENT_PUBLISHED') AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.capability_check_event_id') END IN (` + membership + `) AND (` + judgments + `))`
}

func incidentKnowledgeConsumer(field string, predicate func(string, string) string, activation bool) string {
	eventCondition, recordCondition := "", ""
	if activation {
		eventCondition = ` AND consumer.event_type='KNOWLEDGE_ACTIVATED'`
		recordCondition = ` AND json_extract(consumer.body,'$.value.status')='ACTIVE'`
	}
	return `EXISTS (SELECT 1 FROM incident_event_links link JOIN events consumer ON consumer.sequence=link.event_sequence AND consumer.event_id=link.event_id
WHERE link.target_kind='event' AND link.target_id=events.event_id AND CASE WHEN json_valid(consumer.payload) THEN
json_extract(consumer.payload,'$.projection.projection_kind')='knowledge' AND ` + predicate("consumer.payload", "$.projection.value") + eventCondition + ` AND
EXISTS (SELECT 1 FROM json_each(consumer.payload,'$.projection.value.` + field + `') ref WHERE ref.type='text' AND ref.value=events.event_id) END) OR
EXISTS (SELECT 1 FROM incident_record_links link JOIN records consumer ON consumer.kind=link.record_kind AND consumer.record_id=link.record_id AND consumer.version=link.record_version
WHERE link.target_kind='event' AND link.target_id=events.event_id AND consumer.kind='knowledge' AND CASE WHEN json_valid(consumer.body) THEN
` + predicate("consumer.body", "$.value") + recordCondition + ` AND
EXISTS (SELECT 1 FROM json_each(consumer.body,'$.value.` + field + `') ref WHERE ref.type='text' AND ref.value=events.event_id) END)`
}
