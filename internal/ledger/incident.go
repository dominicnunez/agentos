package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const incidentEventColumns = `event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version`
const incidentEventBytes = `length(event_id)+length(organization_id)+length(event_type)+length(source_actor_id)+length(source_execution_id)+length(recipient_scope)+length(recipient_id)+length(task_id)+length(authorization_refs)+length(artifact_refs)+length(payload)+length(correlation_id)+length(created_at)+length(schema_version)`

type incidentBudget struct {
	events int
	bytes  int64
}

// VerifiedIncidentEvents does not promote its verified snapshot to a writer.
// All related evidence is selected and validated inside the same transaction.
func (l *SQLite) VerifiedIncidentEvents(ctx context.Context, organization, correlation string, limit int) (events.IncidentSnapshot, error) {
	if organization == "" || correlation == "" || limit < 1 || limit > 256 {
		return events.IncidentSnapshot{}, fmt.Errorf("incident requires organization, correlation and limit from 1 to 256")
	}
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()
	snapshot, err := readIncident(ctx, tx, organization, correlation, limit)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return events.IncidentSnapshot{}, err
	}
	return snapshot, nil
}

func readIncident(ctx context.Context, tx *sql.Tx, organization, correlation string, limit int) (events.IncidentSnapshot, error) {
	head, err := ValidateEventIntegrity(ctx, tx)
	if err != nil {
		return events.IncidentSnapshot{}, fmt.Errorf("verify incident ledger: %w", err)
	}
	budget := incidentBudget{events: limit, bytes: 2 << 20}
	work, err := incidentEvents(ctx, tx, &budget, `organization_id=? AND correlation_id=?`, organization, correlation)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	snapshot := events.IncidentSnapshot{Work: events.VerifiedEventSnapshot{OrganizationID: organization, CorrelationID: correlation, Algorithm: head.Algorithm, LedgerEvents: head.EventCount, LedgerSequence: head.Sequence, LedgerEventID: head.EventID, LedgerSHA256: head.SHA256, Events: work}}
	if len(work) == 0 {
		return snapshot, nil
	}
	tasks, err := incidentTasks(ctx, tx, work)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	// Bound the joined record/event rows before the existing complete-chain
	// resolver allocates its evidence. Orphans count toward the same bound.
	var freezeCount int
	var freezeBytes int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(COALESCE(length(body),0)+COALESCE(`+incidentEventBytes+`,0)),0) FROM (`+freezeHistorySQL+` LIMIT ?)`, organization, organization, organization, organization, limit+1).Scan(&freezeCount, &freezeBytes); err != nil {
		return events.IncidentSnapshot{}, err
	}
	if freezeCount > limit || freezeBytes > 2<<20 {
		return events.IncidentSnapshot{}, fmt.Errorf("incident freeze evidence exceeds bounded snapshot")
	}
	history, err := readFreezeHistory(ctx, tx, organization)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	freezes := make([]events.OrganizationFreezeAdmission, 0, len(history.revisions))
	for _, revision := range history.revisions {
		snapshot.FreezeRecords = append(snapshot.FreezeRecords, revision.record)
		freezes = append(freezes, events.OrganizationFreezeAdmission{OrganizationID: revision.state.OrganizationID, EventRef: revision.event.EventID, Sequence: revision.event.Sequence, Frozen: revision.state.Frozen, Version: revision.record.Version, Control: revision.state.Control})
	}
	related, err := incidentEvents(ctx, tx, &budget, `organization_id=? AND event_type='FREEZE_SET' AND correlation_id<>?`, organization, correlation)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	snapshot.RelatedEvents = related
	if err := events.ValidateExecutionStops(work, freezes); err != nil {
		return events.IncidentSnapshot{}, err
	}
	if err := events.ValidateModelStops(work, freezes); err != nil {
		return events.IncidentSnapshot{}, err
	}
	if err := events.ValidateSecurityHoldOutcomes(work, freezes); err != nil {
		return events.IncidentSnapshot{}, err
	}
	if err := validateIncidentInference(ctx, tx, work, freezes); err != nil {
		return events.IncidentSnapshot{}, err
	}
	effects, err := incidentEffects(ctx, tx, &budget, organization, correlation, tasks, work)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	snapshot.RelatedEvents = append(snapshot.RelatedEvents, effects...)
	sort.Slice(snapshot.RelatedEvents, func(i, j int) bool { return snapshot.RelatedEvents[i].Sequence < snapshot.RelatedEvents[j].Sequence })
	snapshot.Admissions, err = incidentAdmissions(work, snapshot.RelatedEvents, freezes)
	if err != nil {
		return events.IncidentSnapshot{}, err
	}
	return snapshot, nil
}

func incidentAdmissions(work, related []events.Event, freezes []events.OrganizationFreezeAdmission) ([]events.IncidentAdmission, error) {
	stream := append(append([]events.Event(nil), work...), related...)
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	suspended := map[string]bool{}
	stopped := map[string]bool{}
	var admissions []events.IncidentAdmission
	for _, event := range stream {
		if event.EventType == "TASK_EXECUTION_SUSPENDED" {
			suspended[event.TaskID] = true
		}
		if event.EventType == "EXECUTION_STOP_REQUESTED" || event.EventType == "MODEL_STOP_REQUESTED" {
			stopped[event.SourceExecutionID] = true
		}
		kind, execution := "", event.SourceExecutionID
		switch event.EventType {
		case "EXECUTION_STARTED":
			var err error
			execution, err = events.ContainmentExecutionID(event)
			if err != nil {
				return nil, err
			}
			kind = "EXECUTION_START"
		case "INFERENCE_RESERVED":
			kind = "INFERENCE_RESERVATION"
		case "EFFECT_OBLIGATION_TRANSITIONED":
			value, err := core.DecodeEffectObligation(event.Payload)
			if err != nil {
				return nil, err
			}
			if value.Status == core.EffectAttempted {
				kind = "EFFECT_ATTEMPT"
			}
		}
		if kind == "" {
			continue
		}
		frozen := false
		for _, freeze := range freezes {
			if freeze.Sequence < event.Sequence {
				frozen = freeze.Frozen
			}
		}
		if frozen || stopped[execution] || suspended[event.TaskID] {
			return nil, fmt.Errorf("incident admission follows containment boundary")
		}
		admissions = append(admissions, events.IncidentAdmission{EventRef: event.EventID, Kind: kind, TaskID: event.TaskID, ExecutionID: execution})
	}
	return admissions, nil
}

func incidentEvents(ctx context.Context, tx *sql.Tx, budget *incidentBudget, where string, args ...any) ([]events.Event, error) {
	query := `SELECT ` + incidentEventColumns + ` FROM events WHERE ` + where + ` ORDER BY sequence LIMIT ?`
	args = append(args, budget.events+1)
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(`+incidentEventBytes+`),0) FROM (`+query+`)`, args...).Scan(&count, &size); err != nil {
		return nil, err
	}
	if count > budget.events || size > budget.bytes {
		return nil, fmt.Errorf("incident evidence exceeds event or byte limit")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, query, args...))
	if err != nil {
		return nil, err
	}
	budget.events -= count
	budget.bytes -= size
	return stream, nil
}

func incidentTasks(ctx context.Context, tx *sql.Tx, stream []events.Event) (map[string]bool, error) {
	works := map[core.ID]core.Work{}
	tasks := map[string]bool{}
	priorTasks := map[string]core.Task{}
	versions := map[string]int{}
	identities := map[string][2]string{}
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		if err := events.ValidateProjectionEventBoundary(event, payload); err != nil {
			return nil, err
		}
		projection := payload.Projection
		if projection.ProjectionKind != "work" && projection.ProjectionKind != "task" {
			continue
		}
		var body []byte
		var eventID, fingerprint string
		var size int64
		if err := tx.QueryRowContext(ctx, `SELECT length(body) FROM records WHERE kind=? AND record_id=? AND version=?`, projection.ProjectionKind, projection.RecordID, projection.Version).Scan(&size); err != nil || size > 2<<20 {
			return nil, fmt.Errorf("incident projection record is missing or exceeds byte limit")
		}
		if err := tx.QueryRowContext(ctx, `SELECT body,admission_event_id,admission_fingerprint FROM records WHERE kind=? AND record_id=? AND version=?`, projection.ProjectionKind, projection.RecordID, projection.Version).Scan(&body, &eventID, &fingerprint); err != nil {
			return nil, fmt.Errorf("incident projection lacks exact record: %w", err)
		}
		var record events.ProjectionRecord
		if decodeExactJSONBytes(body, &record) != nil || !reflect.DeepEqual(record, projection) || eventID != event.EventID || fingerprint != payload.Admission.Fingerprint {
			return nil, fmt.Errorf("incident projection record differs from admitted event")
		}
		key := projection.ProjectionKind + ":" + projection.RecordID
		if projection.Version != versions[key]+1 {
			return nil, fmt.Errorf("incident projection history is incomplete")
		}
		versions[key] = projection.Version
		identities[key] = [2]string{projection.ProjectionKind, projection.RecordID}
		if projection.ProjectionKind == "work" {
			var work core.Work
			if decodeExactJSONBytes(projection.Value, &work) != nil || string(work.ID) != projection.RecordID {
				return nil, fmt.Errorf("incident Work identity is invalid")
			}
			if prior, ok := works[work.ID]; ok && !core.ValidWorkRevision(prior, work) {
				return nil, fmt.Errorf("incident Work revision is invalid")
			}
			works[work.ID] = work
			continue
		}
		var task core.Task
		if decodeExactJSONBytes(projection.Value, &task) != nil || string(task.ID) != projection.RecordID || works[task.WorkID].ID == "" {
			return nil, fmt.Errorf("incident Task lacks its exact Work")
		}
		var prior *core.Task
		if value, ok := priorTasks[projection.RecordID]; ok {
			prior = &value
		}
		if err := events.ValidateTaskProjectionTransition(event.EventType, projection.Version, prior, task); err != nil {
			return nil, err
		}
		priorTasks[projection.RecordID] = task
		tasks[projection.RecordID] = true
	}
	for key, identity := range identities {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM records WHERE kind=? AND record_id=? LIMIT 257)`, identity[0], identity[1]).Scan(&count); err != nil {
			return nil, err
		}
		if count != versions[key] {
			return nil, fmt.Errorf("incident projection record/event history is incomplete")
		}
	}
	return tasks, nil
}

func incidentEffects(ctx context.Context, tx *sql.Tx, budget *incidentBudget, organization, correlation string, tasks map[string]bool, work []events.Event) ([]events.Event, error) {
	if len(tasks) == 0 {
		for _, event := range work {
			if event.EventType == "EFFECT_OBLIGATION_TRANSITIONED" {
				return nil, fmt.Errorf("incident effect lacks an admitted Task")
			}
		}
		return nil, nil
	}
	ids := make([]string, 0, len(tasks))
	for id := range tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := []any{organization}
	for _, id := range ids {
		args = append(args, id)
	}
	// Discover by both durable records and event envelopes. Neither side may
	// conceal an orphan or later cross-tenant revision of a selected effect.
	idQuery := `SELECT record_id AS id FROM records WHERE kind='effect' AND json_valid(body) AND json_extract(body,'$.organization_id')=? AND json_extract(body,'$.task_id') IN (` + marks + `) UNION SELECT json_extract(payload,'$.effect_obligation_id') AS id FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND organization_id=? AND task_id IN (` + marks + `) UNION SELECT json_extract(payload,'$.effect_obligation_id') AS id FROM events WHERE event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_valid(payload) AND json_extract(payload,'$.organization_id')=? AND json_extract(payload,'$.task_id') IN (` + marks + `)`
	idArgs := append(append(append([]any(nil), args...), args...), args...)
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(length(id)),0) FROM (`+idQuery+` LIMIT 257)`, idArgs...).Scan(&count, &size); err != nil {
		return nil, err
	}
	if count > 256 || size > budget.bytes {
		return nil, fmt.Errorf("incident effect identities exceed limit")
	}
	rows, err := tx.QueryContext(ctx, idQuery+` ORDER BY id LIMIT 257`, idArgs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	effectIDs := make([]string, 0, count)
	for rows.Next() {
		var id sql.NullString
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if !id.Valid || id.String == "" {
			return nil, fmt.Errorf("incident effect lacks identity")
		}
		effectIDs = append(effectIDs, id.String)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(effectIDs) == 0 {
		return nil, nil
	}
	effectMarks := strings.TrimSuffix(strings.Repeat("?,", len(effectIDs)), ",")
	effectArgs := make([]any, 0, len(effectIDs)+1)
	for _, id := range effectIDs {
		effectArgs = append(effectArgs, id)
	}
	selected, err := incidentEvents(ctx, tx, budget, `event_type='EFFECT_OBLIGATION_TRANSITIONED' AND json_extract(payload,'$.effect_obligation_id') IN (`+effectMarks+`) AND NOT (organization_id=? AND correlation_id=?)`, append(effectArgs, organization, correlation)...)
	if err != nil {
		return nil, err
	}
	all := append([]events.Event(nil), selected...)
	for _, event := range work {
		if event.EventType == "EFFECT_OBLIGATION_TRANSITIONED" {
			all = append(all, event)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Sequence < all[j].Sequence })
	if err := validateIncidentEffects(ctx, tx, organization, tasks, effectIDs, all); err != nil {
		return nil, err
	}
	return selected, nil
}

func validateIncidentEffects(ctx context.Context, tx *sql.Tx, organization string, tasks map[string]bool, ids []string, stream []events.Event) error {
	histories := map[string][]events.Event{}
	for _, event := range stream {
		value, err := core.DecodeEffectObligation(event.Payload)
		if err != nil || string(value.OrganizationID) != organization || string(value.TaskID) != event.TaskID || !tasks[event.TaskID] || event.OrganizationID != organization || event.SchemaVersion != events.SchemaVersion || event.SourceExecutionID != "" || event.RecipientID != "" || event.RecipientScope != "" {
			return fmt.Errorf("incident effect crosses its Task identity")
		}
		histories[string(value.ID)] = append(histories[string(value.ID)], event)
	}
	for _, id := range ids {
		var count int
		var size int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(length(body)),0) FROM (SELECT body FROM records WHERE kind='effect' AND record_id=? ORDER BY version LIMIT 257)`, id).Scan(&count, &size); err != nil {
			return err
		}
		if count != len(histories[id]) || count == 0 || count > 256 || size > 2<<20 {
			return fmt.Errorf("incident effect record/event history is incomplete or exceeds limit")
		}
		rows, err := tx.QueryContext(ctx, `SELECT version,body,admission_event_id FROM records WHERE kind='effect' AND record_id=? ORDER BY version`, id)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		var previous core.EffectObligation
		index := 0
		for rows.Next() {
			var version int
			var body []byte
			var admission string
			if err := rows.Scan(&version, &body, &admission); err != nil {
				_ = rows.Close()
				return err
			}
			value, err := core.DecodeEffectObligation(body)
			event := histories[id][index]
			var recorded core.EffectObligation
			if err != nil || json.Unmarshal(event.Payload, &recorded) != nil || !reflect.DeepEqual(recorded, value) || version != index+1 || string(value.ID) != id || admission != "" && admission != event.EventID {
				_ = rows.Close()
				return fmt.Errorf("incident effect record differs from its ordered event")
			}
			if err := validateIncidentEffect(value, previous, index == 0); err != nil {
				_ = rows.Close()
				return err
			}
			previous = value
			index++
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Effects have no shared replay validator or sealed record admission. This
// read-only check proves ordered identity/state consistency, not permission to
// dispatch or independent confirmation by an external destination.
func validateIncidentEffect(value, previous core.EffectObligation, first bool) error {
	if value.ID == "" || value.OrganizationID == "" || value.TaskID == "" || value.ActorID == "" || value.Action == "" || value.Resource == "" || value.Scope == "" || value.IdempotencyKey == "" || value.EffectFingerprint == "" || len(value.AuthorizationRefs) == 0 || len(value.AuthorizationRefs) > core.MaximumEffectAuthorizationRefs {
		return fmt.Errorf("incident effect identity is incomplete")
	}
	if err := core.ValidateExecutionAuthorityEffect(value); err != nil {
		return err
	}
	if value.ActorKind != "" {
		fingerprint, err := core.FingerprintEffect(value)
		if err != nil || fingerprint != value.EffectFingerprint || !core.ValidPrincipalKind(value.ActorKind) {
			return fmt.Errorf("incident effect fingerprint is invalid")
		}
	}
	if !first {
		before, err := core.FingerprintEffect(previous)
		after, nextErr := core.FingerprintEffect(value)
		if err != nil || nextErr != nil || before != after {
			return fmt.Errorf("incident effect changes immutable intent")
		}
	}
	switch value.Status {
	case core.EffectPending:
		if !first || value.AttemptCount != 0 || value.LastAttemptAt != nil {
			return fmt.Errorf("incident effect pending history is invalid")
		}
	case core.EffectAttempted:
		if value.AttemptCount != previous.AttemptCount+1 || !first && previous.Status != core.EffectPending {
			return fmt.Errorf("incident effect attempt history is invalid")
		}
	case core.EffectConfirmed, core.EffectFailed:
		if first || previous.Status != core.EffectAttempted || value.AttemptCount != previous.AttemptCount || !reflect.DeepEqual(value.LastAttemptAt, previous.LastAttemptAt) {
			return fmt.Errorf("incident effect terminal history lacks its attempt")
		}
		if value.Status == core.EffectConfirmed && len(value.ConfirmationEvidenceRefs) == 0 {
			return fmt.Errorf("incident effect confirmation lacks evidence")
		}
		if value.ReconciledAt != nil && len(value.ReconciliationEvidenceRefs) == 0 {
			return fmt.Errorf("incident effect reconciliation lacks evidence")
		}
	case core.EffectCancelled:
		if first || previous.Status != core.EffectPending || value.AttemptCount != 0 {
			return fmt.Errorf("incident cancelled effect was not pending")
		}
	default:
		return fmt.Errorf("incident effect status is invalid")
	}
	return nil
}
