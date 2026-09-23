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

type incidentFactBoundary struct {
	Start int64    `json:"start"`
	Agent string   `json:"agent"`
	Teams []string `json:"teams"`
}

// Discover candidate inputs independently of the context manifest. Both sides
// of the durable projection pair participate; exact pairing and complete
// histories are validated by the ordinary dependency closure. Any previously
// eligible revision selects its identity: an unvalidated later revision cannot
// hide it by removing eligibility. Actual relevance and latest state remain
// decisions of the shared replay validator.
func (d *incidentDependencies) loadFactualCandidates(ctx context.Context, tx *sql.Tx) error {
	if d.factualStarts == nil {
		d.factualStarts = map[string]bool{}
	}
	var boundaries []incidentFactBoundary
	for _, event := range d.stream {
		if event.EventType != "EXECUTION_STARTED" || d.factualStarts[event.EventID] {
			continue
		}
		d.factualStarts[event.EventID] = true
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("incident factual start lacks admission")
		}
		var task core.Task
		if err := json.Unmarshal(projection.Projection.Value, &task); err != nil {
			return err
		}
		if task.ExecutionKind == core.ExecutionAgent {
			boundaries = append(boundaries, incidentFactBoundary{Start: event.Sequence, Agent: string(task.AssigneeID)})
		}
	}
	if len(boundaries) == 0 {
		return nil
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i].Start < boundaries[j].Start })
	for _, kind := range []string{"team", "knowledge"} {
		body, err := json.Marshal(boundaries)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, incidentFactualQuery(kind), d.organization, string(body), d.budget.events+1)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		count := 0
		bytes := int64(0)
		for rows.Next() {
			var start int64
			var id, ref string
			var current bool
			if err := rows.Scan(&start, &id, &ref, &current); err != nil {
				_ = rows.Close()
				return err
			}
			count++
			bytes += int64(len(id) + len(ref))
			if count > d.budget.events || bytes > d.budget.bytes {
				_ = rows.Close()
				return fmt.Errorf("incident factual candidates exceed support limit")
			}
			d.key(kind, id)
			d.ref(ref)
			if kind == "team" && current {
				for i := range boundaries {
					if boundaries[i].Start == start {
						boundaries[i].Teams = append(boundaries[i].Teams, id)
					}
				}
			}
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if d.tooManyKeys {
			return fmt.Errorf("incident factual identities exceed support limit")
		}
	}
	return nil
}

func incidentFactualQuery(kind string) string {
	// Each branch is bounded before UNION retains candidate identities. Full
	// documents are loaded only after the normal metadata budget preflight.
	branch := func(records bool) string {
		from := "events e"
		doc := "e.payload"
		root := "$.projection"
		id := "json_extract(e.payload,'$.projection.record_id')"
		prior := "e.sequence<json_extract(b.value,'$.start')"
		newer := `NOT EXISTS (SELECT 1 FROM events n WHERE n.organization_id=e.organization_id AND n.sequence>e.sequence AND n.sequence<json_extract(b.value,'$.start') AND CASE WHEN json_valid(n.payload) THEN json_extract(n.payload,'$.projection.projection_kind') END='` + kind + `' AND json_extract(n.payload,'$.projection.record_id')=` + id + `)`
		if records {
			from = "records r LEFT JOIN events e ON e.event_id=r.admission_event_id"
			doc = "r.body"
			root = "$"
			id = "r.record_id"
			prior = "(e.sequence IS NULL OR e.sequence<json_extract(b.value,'$.start'))"
			newer = `NOT EXISTS (SELECT 1 FROM records n JOIN events ne ON ne.event_id=n.admission_event_id WHERE n.kind=r.kind AND n.record_id=r.record_id AND n.version>r.version AND ne.sequence<json_extract(b.value,'$.start'))`
		}
		value := func(field string) string { return "json_extract(" + doc + ",'" + root + ".value." + field + "')" }
		typed := "json_extract(" + doc + ",'" + root + ".projection_kind')='" + kind + "'"
		if records {
			typed = "r.kind='" + kind + "'"
		}
		match := `EXISTS (SELECT 1 FROM json_each(` + value("member_agent_ids") + `) member WHERE member.value=json_extract(b.value,'$.agent'))`
		if kind == "knowledge" {
			match = value("status") + `='ACTIVE' AND ` + value("context_use") + `='FACTUAL_REFERENCE' AND ((` + value("scope") + `='ORGANIZATION' AND ` + value("scope_id") + `=(SELECT organization FROM config)) OR (` + value("scope") + `='AGENT' AND ` + value("scope_id") + `=json_extract(b.value,'$.agent')) OR (` + value("scope") + `='TEAM' AND ` + value("scope_id") + ` IN (SELECT value FROM json_each(json_extract(b.value,'$.teams')))))`
		}
		// Retain former member Team histories too, but only current membership
		// expands the factual scope at this start. Invalid removals then fail
		// history validation without granting the manifest authority to omit it.
		current := "0"
		if kind == "team" {
			current = "CASE WHEN " + newer + " THEN 1 ELSE 0 END"
		}
		return `SELECT start,id,ref,current FROM (SELECT json_extract(b.value,'$.start') AS start,` + id + ` AS id,COALESCE(e.event_id,'') AS ref,` + current + ` AS current FROM ` + from + ` JOIN json_each((SELECT boundaries FROM config)) b WHERE ` + prior + ` AND CASE WHEN json_valid(` + doc + `) THEN ` + typed + ` AND ` + value("organization_id") + `=(SELECT organization FROM config) AND ` + match + ` END LIMIT ` + fmt.Sprint(events.MaximumIncidentEvidence+1) + `)`
	}
	return `WITH config(organization,boundaries) AS (VALUES (?,?)) ` + branch(false) + ` UNION ` + branch(true) + ` LIMIT ?`
}
