package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// An identity selects every retained revision, not merely a referenced version.
// Reverse keys select complete sibling sets needed by temporal graph validation.
type incidentKey struct{ kind, id string }

type incidentDependencies struct {
	organization       string
	budget             incidentBudget
	stream             map[string]events.Event
	public             map[string]bool
	keys               map[incidentKey]bool
	refs               map[string]bool
	correlations       map[string]bool
	executions         map[string]bool
	reverse            map[incidentKey]bool
	records            map[string]bool
	recordCorrelations map[string]bool
	backed             map[string]bool
	authority          []events.AuthorityRecord
	tooManyKeys        bool
}

func loadIncidentDependencies(ctx context.Context, tx *sql.Tx, snapshot *events.IncidentSnapshot, freezes []events.OrganizationFreezeAdmission) error {
	d := incidentDependencies{organization: snapshot.Work.OrganizationID,
		budget: incidentBudget{events: events.MaximumIncidentEvidence, bytes: events.MaximumIncidentEvidenceBytes},
		stream: map[string]events.Event{}, public: map[string]bool{}, keys: map[incidentKey]bool{}, refs: map[string]bool{},
		correlations: map[string]bool{}, executions: map[string]bool{}, reverse: map[incidentKey]bool{}, records: map[string]bool{}, recordCorrelations: map[string]bool{}, backed: map[string]bool{}}
	for _, record := range snapshot.FreezeRecords {
		d.budget.events--
		d.budget.bytes -= int64(len(record.Body) + len(record.RecordID) + len(record.Kind) + len(record.AdmissionEventID))
	}
	for _, stream := range [][]events.Event{snapshot.Work.Events, snapshot.RelatedEvents} {
		for _, event := range stream {
			d.public[event.EventID] = true
			if err := d.add(event); err != nil {
				return err
			}
		}
	}
	for {
		where, args := d.frontier()
		if where == "" {
			break
		}
		// Select both directions. A moved record still selects its event; a moved
		// event still selects the complete identity, and exact backing is checked.
		loaded, err := incidentEvents(ctx, tx, &d.budget, where, args...)
		if err != nil {
			return fmt.Errorf("incident dependency events: %w", err)
		}
		for _, event := range loaded {
			if err := d.add(event); err != nil {
				return err
			}
		}
		if err := d.loadRecords(ctx, tx); err != nil {
			return err
		}
		if err := d.loadAuthorities(ctx, tx); err != nil {
			return err
		}
	}
	for _, event := range d.stream {
		_, present, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		if present && !d.backed[event.EventID] {
			return fmt.Errorf("incident dependency projection lacks exact record")
		}
	}
	snapshot.AuthorityRecords = d.authority
	for id, event := range d.stream {
		if !d.public[id] {
			snapshot.DependencyEvents = append(snapshot.DependencyEvents, event)
		}
	}
	sort.Slice(snapshot.DependencyEvents, func(i, j int) bool {
		return snapshot.DependencyEvents[i].Sequence < snapshot.DependencyEvents[j].Sequence
	})
	if err := d.loadSupportingRows(ctx, tx, snapshot); err != nil {
		return err
	}
	stream := make([]events.Event, 0, len(d.stream))
	for _, event := range d.stream {
		stream = append(stream, event)
	}
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	correlations := make([]string, 0, len(d.correlations))
	for correlation := range d.correlations {
		correlations = append(correlations, correlation)
	}
	sort.Strings(correlations)
	if err := validateIncidentInferenceBudget(ctx, tx, stream, freezes, correlations, &d.budget); err != nil {
		return err
	}
	return validateIncidentExecutionRows(ctx, tx, stream)
}

func (d *incidentDependencies) add(event events.Event) error {
	if event.OrganizationID != d.organization {
		return fmt.Errorf("incident dependency crosses organization")
	}
	if _, exists := d.stream[event.EventID]; exists {
		return nil
	}
	d.stream[event.EventID] = event
	projection, present, err := events.AdmittedProjection(event)
	if err != nil {
		return err
	}
	if !present && !incidentOwnedContract(event.EventType) {
		return nil
	}
	switch event.EventType {
	case "PLAN_CREATED", "INTENT_CONFIRMED", "INTAKE_RECEIVED", "INTENT_DRAFTED":
		d.correlation(event.CorrelationID)
	}
	d.key("organization", event.OrganizationID)
	if event.TaskID != "" {
		d.key("task", event.TaskID)
	}
	switch event.RecipientScope {
	case "AGENT":
		d.key("agent", event.RecipientID)
	case "TEAM":
		d.key("team", event.RecipientID)
	}
	if event.SourceExecutionID != "" {
		if _, ok := d.executions[event.SourceExecutionID]; !ok {
			if len(d.executions) >= events.MaximumIncidentEvidence {
				return fmt.Errorf("incident execution frontier exceeds support limit")
			}
			d.executions[event.SourceExecutionID] = false
		}
	}
	for _, ref := range event.AuthorizationRefs {
		d.key("capability_lease", ref)
	}
	if event.EventType == "CAPABILITY_GRANTED" || event.EventType == "CAPABILITY_REVOKED" {
		var lease struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(event.Payload, &lease) != nil {
			return fmt.Errorf("invalid incident capability")
		}
		d.key("capability_lease", lease.ID)
	}
	if present {
		d.key(projection.Projection.ProjectionKind, projection.Projection.RecordID)
		switch projection.Projection.ProjectionKind {
		case "intent", "work", "task", "lab_experiment", "lab_promotion_candidate":
			d.correlation(event.CorrelationID)
		}
	}
	bodies := []json.RawMessage{event.Payload}
	if present {
		bodies = []json.RawMessage{projection.Projection.Value, projection.Detail}
	}
	for _, body := range bodies {
		if len(body) == 0 {
			continue
		}
		var payload any
		if json.Unmarshal(body, &payload) != nil {
			return fmt.Errorf("incident dependency has invalid JSON")
		}
		d.discover(payload, "")
	}
	if d.tooManyKeys {
		return fmt.Errorf("incident dependency frontier exceeds support limit")
	}
	return nil
}

func incidentExecutionContract(kind string) bool {
	for _, token := range strings.Split(incidentExecutionTypes, ",") {
		if strings.Trim(strings.TrimSpace(token), "'") == kind {
			return true
		}
	}
	return false
}

// Ordinary notes and result text cannot introduce graph edges merely by
// spelling an identity field name; only owned semantic contracts expand scope.
func incidentOwnedContract(kind string) bool {
	if incidentExecutionContract(kind) {
		return true
	}
	switch kind {
	case "INTENT_CONFIRMED", "INTAKE_MESSAGE_RECORDED", "HUMAN_INPUT_RECEIVED", "A2A_INPUT_RECEIVED",
		"WORK_COMPLETION_EVALUATED", "GOAL_PROGRESS_EVALUATED", "EVIDENCE_PUBLISHED", "COMPLETION_REVIEW_REQUESTED", "COMPLETION_REVIEW_DECIDED", "INBOX_EVENTS_OBSERVED",
		"CAPABILITY_GRANTED", "CAPABILITY_REVOKED", "CAPABILITY_CHECKED", "FREEZE_SET",
		"HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", "A2A_KNOWLEDGE_JUDGMENT_RECEIVED", "KNOWLEDGE_JUDGMENT_PUBLISHED", "KNOWLEDGE_VALIDATION_RECORDED", "KNOWLEDGE_PROPOSED":
		return true
	}
	return false
}

func (d *incidentDependencies) key(kind, id string) {
	if id == "" {
		return
	}
	key := incidentKey{kind, id}
	if _, ok := d.keys[key]; !ok {
		if len(d.keys) >= events.MaximumIncidentEvidence {
			d.tooManyKeys = true
			return
		}
		d.keys[key] = false
	}
	switch kind {
	case "work", "intent", "goal", "knowledge", "lab_experiment":
		if _, ok := d.reverse[key]; !ok {
			d.reverse[key] = false
		}
	}
}
func (d *incidentDependencies) correlation(id string) {
	if id != "" {
		if _, ok := d.correlations[id]; !ok {
			if len(d.correlations) >= events.MaximumIncidentEvidence {
				d.tooManyKeys = true
				return
			}
			d.correlations[id] = false
		}
	}
}
func (d *incidentDependencies) ref(id string) {
	if id != "" {
		if _, ok := d.refs[id]; !ok {
			if len(d.refs) >= events.MaximumIncidentEvidence {
				d.tooManyKeys = true
				return
			}
			d.refs[id] = false
		}
	}
}

// Only contract identity/reference keys are followed. Arbitrary text and
// artifact locators do not expand the selected graph.
func (d *incidentDependencies) discover(value any, field string) {
	if d.tooManyKeys {
		return
	}
	switch value := value.(type) {
	case map[string]any:
		if field == "derived_knowledge_refs" || field == "knowledge_refs" {
			if id, ok := value["id"].(string); ok {
				d.key("knowledge", id)
			}
		}
		if field == "coordination_refs" {
			if id, ok := value["id"].(string); ok {
				d.key("task", id)
			}
		}
		if field == "strategic_context_refs" {
			if id, ok := value["id"].(string); ok {
				kind, id, found := strings.Cut(id, "/")
				if found && (kind == "mission" || kind == "goal") {
					d.key(kind, id)
				}
			}
		}
		if kind, ok := value["created_by_kind"].(string); ok && kind == "AGENT" {
			if id, ok := value["created_by"].(string); ok {
				d.key("agent", id)
			}
		}
		if kind, ok := value["assignee_type"].(string); ok {
			if id, ok := value["assignee_id"].(string); ok {
				switch kind {
				case "AGENT":
					d.key("agent", id)
				case "TEAM":
					d.key("team", id)
				}
			}
		}
		if scope, ok := value["scope"].(string); ok {
			if id, ok := value["scope_id"].(string); ok {
				switch scope {
				case "ORGANIZATION":
					d.key("organization", id)
				case "TEAM":
					d.key("team", id)
				case "AGENT":
					d.key("agent", id)
				case "TASK":
					d.key("task", id)
				case "WORK":
					d.key("work", id)
				}
			}
		}
		if kind, ok := value["kind"].(string); ok {
			if id, ok := value["id"].(string); ok {
				switch strings.ToLower(kind) {
				case "mission", "goal", "team", "agent", "knowledge":
					d.key(strings.ToLower(kind), id)
				}
			}
		}
		for key, child := range value {
			if key == "observed_effect" {
				if object, ok := child.(map[string]any); ok {
					d.discover(object["stop_request_ref"], "stop_request_ref")
				}
				continue
			}
			switch key {
			case "fields", "text", "content", "replay_context", "recovery_result":
				continue
			}
			d.discover(child, key)
		}
	case []any:
		for _, child := range value {
			d.discover(child, field)
		}
	case string:
		kind := ""
		switch field {
		case "organization_id":
			kind = "organization"
		case "mission_id":
			kind = "mission"
		case "goal_id", "selected_goal_id":
			kind = "goal"
		case "team_id":
			kind = "team"
		case "agent_id", "member_agent_ids":
			kind = "agent"
		case "blueprint_id":
			kind = "agent_blueprint"
		case "profile_id", "execution_profile_id":
			kind = "execution_profile"
		case "intent_id":
			kind = "intent"
		case "work_id", "replaces_work_id":
			kind = "work"
		case "task_id", "origin_task_id", "parent_id":
			kind = "task"
		case "experiment_id":
			kind = "lab_experiment"
		case "knowledge_id":
			kind = "knowledge"
		case "lease_id":
			kind = "capability_lease"
		}
		if kind != "" {
			d.key(kind, value)
		}
		if strings.HasSuffix(field, "event_ref") || strings.HasSuffix(field, "event_refs") || strings.HasSuffix(field, "event_id") || field == "event_ids" || field == "input_event_refs" || field == "work_evidence_refs" || field == "reproduction_evidence_refs" || field == "validation_refs" || field == "evidence_refs" || field == "execution_manifest_ref" || field == "judgment_ref" || field == "stop_request_ref" || field == "execution_start_ref" {
			d.ref(value)
		}
	}
}

func incidentMarks(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

func (d *incidentDependencies) frontier() (string, []any) {
	var parts []string
	var args []any
	var identities []incidentKey
	for key, done := range d.keys {
		if !done {
			identities = append(identities, key)
			d.keys[key] = true
		}
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].kind != identities[j].kind {
			return identities[i].kind < identities[j].kind
		}
		return identities[i].id < identities[j].id
	})
	if len(identities) > 0 {
		var pairs []string
		var leaseIDs []string
		for _, key := range identities {
			pairs = append(pairs, "(?,?)")
			args = append(args, key.kind, key.id)
			if key.kind == "capability_lease" {
				leaseIDs = append(leaseIDs, key.id)
			}
		}
		marks := strings.Join(pairs, ",")
		parts = append(parts, `(CASE WHEN json_valid(payload) THEN json_extract(payload,'$.projection.projection_kind') END,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.projection.record_id') END) IN (`+marks+`)`)
		parts = append(parts, `event_id IN (SELECT admission_event_id FROM records WHERE (kind,record_id) IN (`+marks+`))`)
		for _, key := range identities {
			args = append(args, key.kind, key.id)
		}
		// Lease events carry their identity directly, not in a projection.
		// Select the full lifecycle even when its backing record was removed.
		// Do not hide a tenant mismatch; add rejects foreign selected evidence.
		if len(leaseIDs) > 0 {
			parts = append(parts, `(event_type IN ('CAPABILITY_GRANTED','CAPABILITY_REVOKED') AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.id') END IN (`+incidentMarks(len(leaseIDs))+`))`)
			for _, id := range leaseIDs {
				args = append(args, id)
			}
		}
	}
	addSet := func(set map[string]bool, column string) {
		var ids []string
		for id, done := range set {
			if !done {
				ids = append(ids, id)
				set[id] = true
			}
		}
		sort.Strings(ids)
		if len(ids) > 0 {
			part := column + ` IN (` + incidentMarks(len(ids)) + `)`
			if column != "event_id" {
				part = `(organization_id=? AND ` + part + `)`
				args = append(args, d.organization)
			}
			parts = append(parts, part)
			for _, id := range ids {
				args = append(args, id)
			}
		}
	}
	addSet(d.refs, "event_id")
	addSet(d.correlations, "correlation_id")
	addSet(d.executions, "source_execution_id")
	for key, done := range d.reverse {
		if done {
			continue
		}
		d.reverse[key] = true
		var condition string
		switch key.kind {
		case "work":
			condition = `((json_extract(payload,'$.projection.projection_kind') IN ('task','lab_experiment') AND json_extract(payload,'$.projection.value.work_id')=?) OR (event_type='INTENT_CONFIRMED' AND json_extract(payload,'$.replaces_work_id')=?) OR (json_extract(payload,'$.projection.projection_kind') IN ('work','intent') AND json_extract(payload,'$.projection.value.replaces_work_id')=?))`
			args = append(args, key.id, key.id, key.id)
		case "intent":
			condition = `((json_extract(payload,'$.projection.projection_kind')='work' AND json_extract(payload,'$.projection.value.intent_id')=?) OR (event_type='INTENT_CONFIRMED' AND json_extract(payload,'$.intent_id')=?))`
			args = append(args, key.id, key.id)
		case "goal":
			condition = `((json_extract(payload,'$.projection.projection_kind') IN ('work','intent') AND json_extract(payload,'$.projection.value.goal_id')=?) OR (event_type IN ('INTENT_CONFIRMED','WORK_COMPLETION_EVALUATED','GOAL_PROGRESS_EVALUATED') AND json_extract(payload,'$.goal_id')=?))`
			args = append(args, key.id, key.id)
		case "knowledge":
			condition = `(event_type IN ('KNOWLEDGE_PROPOSED','KNOWLEDGE_VALIDATION_RECORDED','KNOWLEDGE_JUDGMENT_PUBLISHED','HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED') AND json_extract(payload,'$.knowledge_id')=?)`
			args = append(args, key.id)
		case "lab_experiment":
			condition = `(json_extract(payload,'$.projection.projection_kind')='lab_promotion_candidate' AND json_extract(payload,'$.projection.value.experiment_id')=?)`
			args = append(args, key.id)
		}
		if condition != "" {
			parts = append(parts, `(CASE WHEN json_valid(payload) THEN `+condition+` END AND organization_id=?)`)
			args = append(args, d.organization)
		}
	}
	if len(parts) == 0 {
		return "", nil
	}
	where := `(` + strings.Join(parts, " OR ") + `)`
	if len(d.stream) > 0 {
		var ids []string
		for id := range d.stream {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		where += ` AND event_id NOT IN (` + incidentMarks(len(ids)) + `)`
		for _, id := range ids {
			args = append(args, id)
		}
	}
	return where, args
}

func (d *incidentDependencies) loadRecords(ctx context.Context, tx *sql.Tx) error {
	var keys []incidentKey
	for key := range d.keys {
		if key.kind != "capability_lease" && !d.records[key.kind+":"+key.id] {
			keys = append(keys, key)
		}
	}
	var correlations []string
	for id := range d.correlations {
		if !d.recordCorrelations[id] {
			correlations = append(correlations, id)
		}
	}
	sort.Strings(correlations)
	if len(keys) == 0 && len(correlations) == 0 {
		return nil
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].kind+keys[i].id < keys[j].kind+keys[j].id })
	var pairs []string
	var args []any
	for _, key := range keys {
		pairs = append(pairs, "(?,?)")
		args = append(args, key.kind, key.id)
	}
	where := `WHERE 0`
	if len(pairs) > 0 {
		where += ` OR (r.kind,r.record_id) IN (` + strings.Join(pairs, ",") + `)`
	}
	if len(correlations) > 0 {
		where += ` OR (r.kind IN (` + incidentProjectionKindsSQL + `) AND CASE WHEN json_valid(r.body) THEN json_extract(r.body,'$.correlation_id') END IN (` + incidentMarks(len(correlations)) + `) AND (e.organization_id=? OR (e.event_id IS NULL AND ` + incidentProjectionOwnedOrganization + `=?)))`
		for _, id := range correlations {
			args = append(args, id)
		}
		args = append(args, d.organization, d.organization)
	}
	for _, key := range keys {
		var predicate string
		switch key.kind {
		case "work":
			predicate = `(r.kind IN ('task','lab_experiment') AND json_extract(r.body,'$.value.work_id')=?) OR (r.kind IN ('work','intent') AND json_extract(r.body,'$.value.replaces_work_id')=?)`
			args = append(args, key.id, key.id)
		case "intent":
			predicate = `r.kind='work' AND json_extract(r.body,'$.value.intent_id')=?`
			args = append(args, key.id)
		case "lab_experiment":
			predicate = `r.kind='lab_promotion_candidate' AND json_extract(r.body,'$.value.experiment_id')=?`
			args = append(args, key.id)
		case "goal":
			predicate = `r.kind IN ('work','intent') AND json_extract(r.body,'$.value.goal_id')=?`
			args = append(args, key.id)
		}
		if predicate != "" {
			where += ` OR (CASE WHEN json_valid(r.body) THEN (` + predicate + `) END AND (e.organization_id=? OR e.event_id IS NULL))`
			args = append(args, d.organization)
		}
	}
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(size),0) FROM (SELECT `+incidentProjectionRecordBytes+`+`+incidentProjectionEventBytes+` AS size FROM records r LEFT JOIN events e ON e.event_id=r.admission_event_id `+where+` LIMIT ?)`, append(args, d.budget.events+1)...).Scan(&count, &size); err != nil {
		return err
	}
	if count*2 > d.budget.events || size > d.budget.bytes {
		return fmt.Errorf("incident dependency records exceed support limit")
	}
	records, err := admittedProjectionRecordsBounded(ctx, tx, int(d.budget.bytes), where+` ORDER BY e.sequence LIMIT ?`, append(args, d.budget.events+1)...)
	if err != nil {
		return err
	}
	d.budget.events -= count * 2
	d.budget.bytes -= size
	for _, key := range keys {
		d.records[key.kind+":"+key.id] = true
	}
	for _, id := range correlations {
		d.recordCorrelations[id] = true
	}
	for _, record := range records {
		d.backed[record.event.EventID] = true
		if err := d.add(record.event); err != nil {
			return err
		}
	}
	return nil
}
