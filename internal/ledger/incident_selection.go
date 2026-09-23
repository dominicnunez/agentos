package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
	"modernc.org/sqlite"
)

type incidentSelector struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func init() {
	// The function has no connection, request, or cache state. It only discovers
	// selectors in one already budgeted document using the reader's grammar.
	sqlite.MustRegisterDeterministicScalarFunction("agentos_incident_edges_v1", 3, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		kind, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("invalid incident event type")
		}
		var body []byte
		switch value := args[1].(type) {
		case []byte:
			body = value
		case string:
			body = []byte(value)
		default:
			return nil, fmt.Errorf("invalid incident document")
		}
		envelope, ok := args[2].(string)
		if !ok || len(envelope) > 2*events.MaximumIncidentEvidenceBytes {
			return nil, fmt.Errorf("invalid incident envelope")
		}
		var metadata struct {
			events.Event
			Correlation string `json:"correlation_id"`
		}
		if err := json.Unmarshal([]byte(envelope), &metadata); err != nil {
			return nil, err
		}
		event := metadata.Event
		event.CorrelationID = metadata.Correlation
		event.EventType, event.Payload = kind, body
		selected, err := incidentDocumentSelectors(event)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(selected)
		return string(encoded), err
	})
}

// Candidate discovery shares the live reader's grammar. Exact admission seals
// and retained record pairs are still required after the metadata preflight.
func incidentDocumentSelectors(event events.Event) ([]incidentSelector, error) {
	if len(event.Payload) > events.MaximumIncidentEvidenceBytes {
		return nil, fmt.Errorf("incident document exceeds support limit")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &object); err != nil {
		return nil, err
	}
	present := false
	for key := range object {
		if strings.EqualFold(key, "projection") || strings.EqualFold(key, "admission") {
			present = true
		}
	}
	var projection events.ProjectionEventPayload
	if present {
		if err := json.Unmarshal(event.Payload, &projection); err != nil {
			return nil, err
		}
	} else if !incidentOwnedContract(event.EventType) {
		return []incidentSelector{}, nil
	}
	d := incidentDependencies{keys: map[incidentKey]bool{}, refs: map[string]bool{}, reverse: map[incidentKey]bool{}, correlations: map[string]bool{}, executions: map[string]bool{}}
	if err := d.discoverEvent(event, projection, present); err != nil {
		return nil, err
	}
	selected := make([]incidentSelector, 0, len(d.keys)+len(d.refs)+len(d.correlations)+len(d.executions))
	for key := range d.keys {
		selected = append(selected, incidentSelector{key.kind, key.id})
	}
	for id := range d.refs {
		selected = append(selected, incidentSelector{"event", id})
	}
	for id := range d.correlations {
		selected = append(selected, incidentSelector{"correlation", id})
	}
	for id := range d.executions {
		selected = append(selected, incidentSelector{"execution", id})
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].Kind != selected[j].Kind {
			return selected[i].Kind < selected[j].Kind
		}
		return selected[i].ID < selected[j].ID
	})
	return selected, nil
}
func incidentReferenceSQL(seeds int) string {
	// Phase zero emits physical rows before phase one selectors and phase two
	// document expansion. The caller charges metadata before SQLite can expand
	// the next document. UNION deduplicates canonical physical/selector tokens,
	// including cycles, without retaining full documents in the recursive queue.
	// Each lookup branch is capped before UNION builds its distinct temp set.
	envelope := `json_object('organization_id',e.organization_id,'correlation_id',e.correlation_id,'source_execution_id',e.source_execution_id,'task_id',e.task_id,'recipient_scope',e.recipient_scope,'recipient_id',e.recipient_id,'authorization_refs',json(CAST(e.authorization_refs AS TEXT)))`
	recordTargets := fmt.Sprintf(`SELECT locator FROM (SELECT rowid AS locator FROM records WHERE kind=walk.kind AND record_id=walk.identity LIMIT %[1]d)
 UNION SELECT locator FROM (SELECT r.rowid AS locator FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id WHERE walk.kind='work' AND r.kind IN ('work','intent') AND CASE WHEN json_valid(r.body) THEN json_extract(r.body,'$.value.replaces_work_id') END=walk.identity LIMIT %[1]d) LIMIT %[1]d`, events.MaximumIncidentEvidence+1)
	eventTargets := fmt.Sprintf(`SELECT locator FROM (SELECT sequence AS locator FROM events WHERE walk.kind='correlation' AND organization_id=(SELECT organization FROM config) AND correlation_id=walk.identity LIMIT %[1]d)
 UNION SELECT locator FROM (SELECT sequence AS locator FROM events WHERE walk.kind='execution' AND organization_id=(SELECT organization FROM config) AND source_execution_id=walk.identity LIMIT %[1]d)
 UNION SELECT locator FROM (SELECT sequence AS locator FROM events WHERE walk.kind='intake_message' AND organization_id=(SELECT organization FROM config) AND event_type IN (`+incidentIntakeTypes+`) AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.message_id') END=walk.identity LIMIT %[1]d)
 UNION SELECT locator FROM (SELECT sequence AS locator FROM events WHERE walk.kind='intake_message' AND organization_id=(SELECT organization FROM config) AND event_type IN (`+incidentIntakeTypes+`) AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.source_message_id') END=walk.identity LIMIT %[1]d) LIMIT %[1]d`, events.MaximumIncidentEvidence+1)
	bounded := func(query string) string {
		return fmt.Sprintf(`(SELECT CASE WHEN count(*)>%d THEN json_array(NULL) ELSE json_group_array(locator) END FROM (%s))`, events.MaximumIncidentEvidence, query)
	}
	return `WITH RECURSIVE config(organization) AS (VALUES (?)), walk(sequence,phase,kind,identity) AS (
 SELECT sequence,0,'','' FROM events WHERE event_id IN (` + incidentMarks(seeds) + `)
 UNION SELECT sequence,2,kind,'' FROM walk WHERE phase=0
 UNION SELECT CASE WHEN json_extract(edge.value,'$.kind')='event' THEN target.sequence ELSE 0 END,
 CASE WHEN json_extract(edge.value,'$.kind')='event' THEN 0 ELSE 1 END,
 CASE WHEN json_extract(edge.value,'$.kind')='event' THEN '' ELSE json_extract(edge.value,'$.kind') END,
 CASE WHEN json_extract(edge.value,'$.kind')='event' THEN '' ELSE json_extract(edge.value,'$.id') END
 FROM walk JOIN events e ON e.sequence=walk.sequence AND walk.kind=''
 JOIN json_each(agentos_incident_edges_v1(e.event_type,e.payload,` + envelope + `)) edge
 LEFT JOIN events target ON target.event_id=json_extract(edge.value,'$.id') AND json_extract(edge.value,'$.kind')='event'
 WHERE walk.phase=2 AND (json_extract(edge.value,'$.kind')<>'event' OR target.sequence IS NOT NULL)
 UNION SELECT COALESCE(edge.value,0),CASE WHEN edge.type='null' THEN -1 ELSE 0 END,'@record',''
 FROM walk JOIN json_each(` + bounded(recordTargets) + `) edge WHERE phase=1 AND kind NOT IN ('correlation','execution','intake_message')
 UNION SELECT COALESCE(edge.value,0),CASE WHEN edge.type='null' THEN -1 ELSE 0 END,'',''
 FROM walk JOIN json_each(` + bounded(eventTargets) + `) edge WHERE phase=1 AND kind IN ('correlation','execution','intake_message')
 UNION SELECT e.sequence,0,'','' FROM walk JOIN records r ON r.rowid=walk.sequence JOIN events e ON e.event_id=r.admission_event_id WHERE walk.phase=2 AND walk.kind='@record'
 ORDER BY 2)
 SELECT walk.sequence,walk.phase,walk.kind,walk.identity,e.event_id,e.organization_id,CASE WHEN walk.kind='@record' THEN ` + incidentProjectionRecordBytes + ` ELSE ` + incidentProjectionEventBytes + ` END
 FROM walk LEFT JOIN events e ON e.sequence=walk.sequence AND walk.kind='' LEFT JOIN records r ON r.rowid=walk.sequence AND walk.kind='@record' WHERE phase<=1`
}

func (d *incidentDependencies) expandReferences(ctx context.Context, tx *sql.Tx, seed []events.Event) error {
	if len(seed) == 0 {
		return nil
	}
	args := make([]any, 0, len(seed)+1)
	args = append(args, d.organization)
	for _, event := range seed {
		args = append(args, event.EventID)
	}
	rows, err := tx.QueryContext(ctx, incidentReferenceSQL(len(seed)), args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var sequences []any
	var size int64
	physical := 0
	selectors := map[string]map[string]bool{"key": {}, "correlation": {}, "execution": {}}
	for key := range d.keys {
		selectors["key"][key.kind+"\x00"+key.id] = true
	}
	for id := range d.correlations {
		selectors["correlation"][id] = true
	}
	for id := range d.executions {
		selectors["execution"][id] = true
	}
	if d.selectedRecords == nil {
		d.selectedRecords = map[int64]bool{}
	}
	for rows.Next() {
		var sequence, phase int64
		var kind, identity string
		var id, organization sql.NullString
		var bytes sql.NullInt64
		if err := rows.Scan(&sequence, &phase, &kind, &identity, &id, &organization, &bytes); err != nil {
			_ = rows.Close()
			return err
		}
		if phase < 0 {
			_ = rows.Close()
			return fmt.Errorf("incident selected history exceeds support limit")
		}
		if phase == 1 {
			category, key := "key", kind+"\x00"+identity
			if kind == "correlation" || kind == "execution" {
				category, key = kind, identity
			}
			selectors[category][key] = true
			if len(selectors[category]) > events.MaximumIncidentEvidence {
				_ = rows.Close()
				return fmt.Errorf("incident identity frontier exceeds support limit")
			}
			continue
		}
		if kind == "@record" {
			if d.selectedRecords[sequence] {
				continue
			}
			d.selectedRecords[sequence] = true
		} else {
			if organization.String != d.organization {
				_ = rows.Close()
				return fmt.Errorf("incident dependency crosses organization")
			}
			if _, seen := d.stream[id.String]; seen {
				continue
			}
			sequences = append(sequences, sequence)
		}
		physical++
		size += bytes.Int64
		if physical > d.budget.events || size > d.budget.bytes {
			_ = rows.Close()
			return fmt.Errorf("incident reference evidence exceeds support limit")
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
	if len(sequences) == 0 {
		return nil
	}
	loaded, err := incidentEvents(ctx, tx, &d.budget, `sequence IN (`+incidentMarks(len(sequences))+`)`, sequences...)
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
