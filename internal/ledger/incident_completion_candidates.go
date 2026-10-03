package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type incidentCompletionBoundary struct {
	Task         string `json:"task"`
	Correlation  string `json:"correlation"`
	Organization string `json:"organization"`
	Terminal     int64  `json:"terminal"`
	Outcome      string `json:"outcome"`
	Agent        bool   `json:"agent"`
}

// Full completion validation matches Task, correlation and sequence before
// checking organization or execution. Reconstruct those candidate windows from
// selected terminal admissions independently of the declared evidence list.
// Invalid or missing canonical evidence still fails the owning validator; it
// cannot justify accepting the selected terminal transition.
const incidentCompletionCandidatesSQL = `WITH boundaries AS MATERIALIZED (
 SELECT json_extract(value,'$.task') AS task,json_extract(value,'$.correlation') AS correlation,
 json_extract(value,'$.organization') AS organization,json_extract(value,'$.terminal') AS terminal,
 outcome.sequence AS outcome, outcome.source_execution_id AS execution, json_extract(boundary.value,'$.agent') AS agent FROM json_each(?) boundary
 LEFT JOIN events outcome ON outcome.event_id=json_extract(boundary.value,'$.outcome')
), verified AS MATERIALIZED (
 SELECT b.*, (SELECT MIN(v.sequence) FROM events v WHERE v.correlation_id=b.correlation AND v.task_id=b.task
 AND v.organization_id=b.organization AND v.event_type='COMPLETION_VERIFIED' AND v.sequence<b.terminal) AS verification
 FROM boundaries b
), results AS MATERIALIZED (
 SELECT b.*, (SELECT MIN(r.sequence) FROM events r WHERE r.correlation_id=b.correlation AND r.task_id=b.task
 AND r.organization_id=b.organization AND r.event_type='RESULT_PUBLISHED' AND r.sequence>b.outcome AND r.sequence<b.verification) AS result
 FROM verified b
)
SELECT DISTINCT sequence FROM (
 SELECT e.sequence,e.event_id FROM results b CROSS JOIN events e ON e.correlation_id=b.correlation AND e.task_id=b.task AND e.sequence<b.terminal
 WHERE (e.event_type='COMPLETION_VERIFIED') OR
 (e.event_type='RESULT_PUBLISHED' AND e.sequence>b.outcome AND e.sequence<b.verification) OR
 (e.event_type='CANDIDATE_COMPLETE' AND e.sequence>b.result AND e.sequence<b.verification)
 UNION ALL
 SELECT review.sequence,review.event_id FROM (
 SELECT b.correlation,MAX(manifest.sequence) AS manifest FROM boundaries b CROSS JOIN events manifest
 ON manifest.correlation_id=b.correlation AND manifest.task_id=b.task AND manifest.organization_id=b.organization
 AND manifest.event_type='EXECUTION_CONTEXT_MANIFESTED' AND manifest.source_execution_id=b.execution AND manifest.sequence<b.outcome
 WHERE b.agent GROUP BY b.correlation
 ) consumed CROSS JOIN events review ON review.correlation_id=consumed.correlation AND review.sequence<consumed.manifest
 WHERE review.event_type IN ('COMPLETION_REVIEW_REQUESTED','COMPLETION_REVIEW_DECIDED')
) candidates WHERE event_id NOT IN (SELECT value FROM json_each(?)) LIMIT ?`

func (d *incidentDependencies) loadCompletionCandidates(ctx context.Context, tx *sql.Tx) error {
	if d.completionEnds == nil {
		d.completionEnds = map[string]bool{}
	}
	var boundaries []incidentCompletionBoundary
	for _, event := range d.stream {
		if event.EventType != "TASK_VERIFIED_COMPLETE" || d.completionEnds[event.EventID] {
			continue
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("incident completion lacks terminal admission")
		}
		var decision events.CompletionDecisionPayload
		if err := json.Unmarshal(projection.Detail, &decision); err != nil {
			return err
		}
		var task core.Task
		if err := json.Unmarshal(projection.Projection.Value, &task); err != nil {
			return err
		}
		boundaries = append(boundaries, incidentCompletionBoundary{Task: projection.Projection.RecordID,
			Correlation: event.CorrelationID, Organization: event.OrganizationID, Terminal: event.Sequence, Outcome: decision.OutcomeEventRef, Agent: task.ExecutionKind == core.ExecutionAgent})
		d.completionEnds[event.EventID] = true
	}
	if len(boundaries) == 0 {
		return nil
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Terminal < boundaries[j].Terminal })
	body, err := json.Marshal(boundaries)
	if err != nil {
		return err
	}
	known := make([]string, 0, len(d.stream))
	for id := range d.stream {
		known = append(known, id)
	}
	sort.Strings(known)
	retained, err := json.Marshal(known)
	if err != nil {
		return err
	}
	loaded, err := incidentEvents(ctx, tx, &d.budget, `sequence IN (`+incidentCompletionCandidatesSQL+`)`, string(body), string(retained), d.budget.events+1)
	if err != nil {
		return err
	}
	for _, event := range loaded {
		if err := d.add(event); err != nil {
			return err
		}
	}
	return nil
}
