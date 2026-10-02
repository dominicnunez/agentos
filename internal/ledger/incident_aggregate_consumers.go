package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/events"
)

// Raw aggregate evaluations are statements until a terminal transition consumes
// their exact event identity. The completion owners follow the Work/Goal
// transition's detail, not every evaluation naming a Work, Goal, or witness.
// Use this condition for incoming selection and outgoing claim discovery. An
// exact declared reference still selects its target independently, including
// invalid foreign evidence.
func incidentAggregateIncoming(source string) string {
	// The merged incoming index supplies identities, not their source fields.
	// Check the owning terminal source and detail independently. Oversized
	// consumers remain candidates without JSON expansion; ordinary metadata
	// preflight then fails before their payload can be decoded.
	// The terminal label owns this consumption even when its projection
	// discriminator is damaged. Exact admission validation rejects that source.
	consumed := incidentScalarClaim("consumer.payload", "$.detail.evidence_event_ref", `=`+source+`.event_id`)
	return `(` + source + `.event_type NOT IN ('WORK_COMPLETION_EVALUATED','GOAL_PROGRESS_EVALUATED') OR EXISTS (
SELECT 1 FROM incident_event_links aggregate_use JOIN events consumer ON consumer.sequence=aggregate_use.event_sequence AND consumer.event_id=aggregate_use.event_id
WHERE aggregate_use.target_kind='event' AND aggregate_use.target_id=` + source + `.event_id AND
((` + source + `.event_type='WORK_COMPLETION_EVALUATED' AND consumer.event_type='WORK_COMPLETED') OR
 (` + source + `.event_type='GOAL_PROGRESS_EVALUATED' AND consumer.event_type='GOAL_ACHIEVED')) AND
CASE WHEN length(CAST(consumer.payload AS BLOB))>` + fmt.Sprint(events.MaximumIncidentEvidenceBytes) + ` THEN 1 ELSE (` + consumed + `) END))`
}

func incidentRawClaimApplicable(source string) string {
	// CASE evaluates only the selected family. Boolean AND/OR expression
	// evaluation otherwise needlessly inspects unrelated source documents.
	return `(CASE WHEN ` + source + `.event_type IN ('WORK_COMPLETION_EVALUATED','GOAL_PROGRESS_EVALUATED') THEN ` + incidentAggregateIncoming(source) + `
WHEN ` + source + `.event_type IN ('KNOWLEDGE_PROPOSED','KNOWLEDGE_VALIDATION_RECORDED','KNOWLEDGE_JUDGMENT_PUBLISHED','HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED') THEN ` + incidentKnowledgeIncoming(source) + ` ELSE 1 END)`
}

func incidentDeferredClaim(kind string) bool {
	switch kind {
	case "WORK_COMPLETION_EVALUATED", "GOAL_PROGRESS_EVALUATED", "KNOWLEDGE_PROPOSED", "KNOWLEDGE_VALIDATION_RECORDED", "KNOWLEDGE_JUDGMENT_PUBLISHED", "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", "A2A_KNOWLEDGE_JUDGMENT_RECEIVED":
		return true
	}
	return false
}

// Keep directly retained statements visible while deferring their outgoing
// evidence semantics until the same authoritative snapshot contains a consumer.
func (d *incidentDependencies) loadAggregateClaims(ctx context.Context, tx *sql.Tx) error {
	if d.aggregateClaims == nil {
		d.aggregateClaims = map[string]bool{}
	}
	var ids []string
	for id, event := range d.stream {
		if d.aggregateClaims[id] || !incidentDeferredClaim(event.EventType) {
			continue
		}
		_, projection, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !projection {
			ids = append(ids, id)
			d.aggregateClaims[id] = true
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_id FROM events WHERE event_id IN (`+incidentMarks(len(ids))+`) AND `+incidentRawClaimApplicable("events"), args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var consumed []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		consumed = append(consumed, id)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for _, id := range consumed {
		if err := d.discoverEvent(d.stream[id], events.ProjectionEventPayload{}, false); err != nil {
			return err
		}
	}
	return nil
}
