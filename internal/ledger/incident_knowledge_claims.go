package ledger

import (
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
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
	validations := incidentKnowledgeConsumer("validation_refs", nil, true)
	judgments := validations
	return `(event_type='KNOWLEDGE_PROPOSED' AND ` + incidentArrayClaim("payload", "$.occurrence_event_refs", ` IN (`+membership+`)`) + ` AND (` + proposals + `)) OR
(event_type='KNOWLEDGE_VALIDATION_RECORDED' AND ` + incidentScalarClaim("payload", "$.outcome_event_ref", ` IN (`+membership+`)`) + ` AND (` + validations + `)) OR
(event_type IN ('HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED','KNOWLEDGE_JUDGMENT_PUBLISHED') AND ` + incidentScalarClaim("payload", "$.capability_check_event_id", ` IN (`+membership+`)`) + ` AND (` + judgments + `))`
}

func incidentKnowledgeConsumer(field string, predicate func(string, string) string, validation bool) string {
	return incidentKnowledgeConsumerFor("events", field, predicate, validation)
}

func incidentKnowledgeConsumerFor(source, field string, predicate func(string, string) string, validation bool) string {
	if validation {
		// Every retained validation reference is owned. Activation checks each
		// statement against the method; candidates forbid validation refs.
		// Invalid status, method, or principal cannot establish an exclusion.
		predicate = func(string, string) string { return "1" }
	}
	return `EXISTS (SELECT 1 FROM incident_event_links link JOIN events consumer ON consumer.sequence=link.event_sequence AND consumer.event_id=link.event_id
WHERE link.target_kind='event' AND link.target_id=` + source + `.event_id AND
		` + incidentKnowledgeOwner("consumer") + ` AND ` + predicate("consumer.payload", "$.projection.value") + ` AND
` + incidentArrayClaim("consumer.payload", "$.projection.value."+field, `=`+source+`.event_id`) + `) OR
EXISTS (SELECT 1 FROM incident_record_links link JOIN records consumer ON consumer.kind=link.record_kind AND consumer.record_id=link.record_id AND consumer.version=link.record_version
WHERE link.target_kind='event' AND link.target_id=` + source + `.event_id AND ` + incidentKnowledgeRecordOwner("consumer") + ` AND
` + predicate("consumer.body", "$.value") + ` AND
` + incidentArrayClaim("consumer.body", "$.value."+field, `=`+source+`.event_id`) + `) OR ` + incidentKnowledgeCounterpart(source, field, predicate)
}

// A damaged source can lose every discriminator used by its durable link
// index. The retained counterpart still owns its reference-bearing value;
// discover that consumption before filtering the raw statement it names.
func incidentKnowledgeCounterpart(source, field string, predicate func(string, string) string) string {
	eventPredicate := predicate("consumer.payload", "$.projection.value")
	recordPredicate := predicate("consumer.body", "$.value")
	eventOwned := `(` + incidentKnowledgeOwner("consumer") + ` AND ` + eventPredicate + `)`
	recordOwned := `(` + incidentKnowledgeRecordOwner("consumer") + ` AND ` + recordPredicate + `)`
	return `EXISTS(SELECT 1 FROM records backing JOIN events consumer ON consumer.event_id=backing.admission_event_id
WHERE backing.admission_event_id<>'' AND ` + incidentKnowledgeRecordOwner("backing") + ` AND
CASE WHEN ` + eventOwned + ` THEN 0 ELSE CASE WHEN ` + projectionSourceIdentityBytes("consumer.payload", source+".event_id") + ` THEN
(` + eventPredicate + ` OR ` + predicate("backing.body", "$.value") + `) AND ` + incidentArrayClaim("consumer.payload", "$.projection.value."+field, `=`+source+`.event_id`) + ` ELSE 0 END END)
OR EXISTS(SELECT 1 FROM records consumer JOIN events backing ON backing.event_id=consumer.admission_event_id
WHERE consumer.admission_event_id<>'' AND CASE WHEN ` + recordOwned + ` THEN 0 ELSE CASE WHEN
` + projectionSourceIdentityBytes("consumer.body", source+".event_id") + ` AND ` + incidentKnowledgeOwner("backing") + ` THEN
(` + recordPredicate + ` OR ` + predicate("backing.payload", "$.projection.value") + `) AND ` + incidentArrayClaim("consumer.body", "$.value."+field, `=`+source+`.event_id`) + ` ELSE 0 END END)`
}

// Admission metadata makes the typed body authoritative even when the physical
// record kind is damaged. An ordinary generic body remains opaque.
func incidentKnowledgeRecordOwner(source string) string {
	return `(` + source + `.kind='knowledge' OR ((` + source + `.admission_event_id<>'' OR ` + source + `.admission_fingerprint<>'') AND ` + incidentScalarClaim(source+".body", "$.projection_kind", `='knowledge'`) + `))`
}

func incidentKnowledgeIncoming(source string) string {
	proposal := incidentKnowledgeConsumerFor(source, "provenance_event_refs", func(body, path string) string {
		return incidentScalarClaim(body, path+".created_by_kind", `='AGENT'`)
	}, false)
	validation := incidentKnowledgeConsumerFor(source, "validation_refs", nil, true)
	return `(CASE WHEN ` + source + `.event_type NOT IN ('KNOWLEDGE_PROPOSED','KNOWLEDGE_VALIDATION_RECORDED','KNOWLEDGE_JUDGMENT_PUBLISHED','HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED') THEN 1
WHEN ` + incidentScalarClaim(source+".payload", "$.projection.projection_kind", `='knowledge'`) + ` THEN 1
WHEN ` + source + `.event_type='KNOWLEDGE_PROPOSED' AND ` + incidentKnowledgeOwner(source) + ` THEN 1
WHEN ` + source + `.event_type='KNOWLEDGE_PROPOSED' THEN (` + proposal + `)
ELSE (` + validation + `) END)`
}

// Runtime labels and reserved admission fields retain ownership independently
// of a valid discriminator. Bare non-runtime proposals remain consumer-gated.
func incidentKnowledgeOwner(source string) string {
	labels := events.ProjectionLifecycleEventTypes("knowledge")
	for index, label := range labels {
		labels[index] = "'" + strings.ReplaceAll(label, "'", "''") + "'"
	}
	reserved := `EXISTS(SELECT 1 FROM json_each(CASE WHEN json_valid(` + source + `.payload) THEN ` + source + `.payload ELSE '{}' END) field WHERE field.key IN ('projection','admission'))`
	return `(` + incidentScalarClaim(source+".payload", "$.projection.projection_kind", `='knowledge'`) + ` OR (` + source + `.event_type IN (` + strings.Join(labels, ",") + `) AND (` + source + `.event_type<>'KNOWLEDGE_PROPOSED' OR ` + source + `.source_actor_id='runtime' OR ` + reserved + `)))`
}

// Traverse each owned object member by its decoded key, retaining duplicate
// and escaped aliases at every ancestor. Arrays contribute only their
// immediate string elements.
// Paths and comparisons are internal SQL; identities remain bound arguments.
func incidentScalarClaim(source, path, comparison string) string {
	return `EXISTS (` + incidentClaimQuery(source, path, false, comparison) + `)`
}

func incidentArrayClaim(source, path, comparison string) string {
	return `EXISTS (` + incidentClaimQuery(source, path, true, comparison) + `)`
}

// A correlated JSON array lets identity readers enumerate each source once,
// retaining every matching scalar occurrence before downstream joins.
func incidentClaimJSON(source, path string) string {
	return `(SELECT json_group_array(value) FROM (` + incidentClaimQuery(source, path, false, "") + `))`
}

func incidentClaimQuery(source, path string, array bool, comparison string) string {
	fields := strings.Split(strings.TrimPrefix(path, "$."), ".")
	from := `json_each(CASE WHEN json_valid(` + source + `) THEN ` + source + ` ELSE '{}' END)`
	for index := 0; index < len(fields)-1; index++ {
		parent := fmt.Sprintf("parent%d", index)
		from += ` ` + parent + ` JOIN json_each(CASE WHEN ` + parent + `.key='` + fields[index] + `' AND ` + parent + `.type='object' THEN ` + parent + `.value ELSE '{}' END)`
	}
	from += ` claim`
	value, typed := "claim.value", `claim.type='text'`
	if array {
		from += ` JOIN json_each(CASE WHEN claim.key='` + fields[len(fields)-1] + `' AND claim.type='array' THEN claim.value ELSE '[]' END) ref`
		value = "ref.value"
		typed = `ref.type='text' AND claim.type='array'`
	}
	predicate := typed + ` AND claim.key='` + fields[len(fields)-1] + `'`
	if comparison != "" {
		predicate += ` AND ` + value + comparison
	}
	return `SELECT ` + value + ` AS value FROM ` + from + ` WHERE ` + predicate
}
