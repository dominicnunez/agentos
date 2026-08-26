package ledger

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	_ "modernc.org/sqlite"
)

type SQLite struct {
	db                 *sql.DB
	newWorkCorrelation func() (string, error)
	now                func() time.Time
}

func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	l := &SQLite{db: db, newWorkCorrelation: randomWorkCorrelation, now: time.Now}
	if err := l.migrate(context.Background()); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return l, nil
}
func (l *SQLite) Close() error { return l.db.Close() }
func (l *SQLite) migrate(ctx context.Context) error {
	if err := migrateStorage(ctx, l.db); err != nil {
		return err
	}
	if _, err := ValidateEventIntegrity(ctx, l.db); err != nil {
		return fmt.Errorf("validate event integrity at startup: %w", err)
	}
	return l.rebuildExternalWorkIndex(ctx)
}

func (l *SQLite) nowUTC() time.Time {
	if l.now == nil {
		return time.Now().UTC()
	}
	return l.now().UTC()
}

func (l *SQLite) rebuildExternalWorkIndex(ctx context.Context) error {
	intentBodies, err := l.Records(ctx, "intent", "")
	if err != nil {
		return fmt.Errorf("scan intents for external work index: %w", err)
	}
	type workBinding struct{ organizationID, requestID, correlationID, intentID string }
	var bindings []workBinding
	for _, body := range intentBodies {
		var record events.ProjectionRecord
		var intent core.Intent
		if err := json.Unmarshal(body, &record); err != nil {
			return fmt.Errorf("decode intent record for external work migration: %w", err)
		}
		if err := json.Unmarshal(record.Value, &intent); err != nil {
			return fmt.Errorf("decode intent value for external work migration: %w", err)
		}
		if intent.SourceChannel != "A2A" && intent.SourceChannel != "HUMAN_DIRECT" {
			continue
		}
		if intent.OrganizationID == "" || intent.ExternalRequestID == "" || record.CorrelationID == "" || intent.ID == "" {
			return fmt.Errorf("external intent %q lacks exact durable index identity", intent.ID)
		}
		bindings = append(bindings, workBinding{string(intent.OrganizationID), intent.ExternalRequestID, record.CorrelationID, string(intent.ID)})
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM external_tasks`); err != nil {
			return fmt.Errorf("clear external task index: %w", err)
		}
		for _, binding := range bindings {
			if err := registerExternalWork(ctx, tx, binding.organizationID, binding.requestID, binding.correlationID, binding.intentID); err != nil {
				return fmt.Errorf("rebuild external work %s/%s: %w", binding.organizationID, binding.requestID, err)
			}
		}
		// Only runtime-owned roots cross the external lookup boundary.
		if _, err := tx.ExecContext(ctx, `INSERT INTO external_tasks(organization_id,task_id,correlation_id)
SELECT organization_id,'task-' || correlation_id,correlation_id FROM external_work`); err != nil {
			return fmt.Errorf("rebuild external root task index: %w", err)
		}
		var conflictingTask bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
SELECT 1 FROM events e
JOIN external_work w ON w.organization_id=e.organization_id AND w.correlation_id=e.correlation_id
JOIN external_tasks t ON t.organization_id=e.organization_id AND t.task_id=e.task_id
WHERE e.task_id<>'' AND t.correlation_id<>e.correlation_id)`).Scan(&conflictingTask); err != nil {
			return fmt.Errorf("verify external task index: %w", err)
		}
		if conflictingTask {
			return fmt.Errorf("external task is bound to multiple work streams")
		}
		return nil
	})
}

type preparedProjection struct {
	draft              events.ProjectionDraft
	eventDraft         events.TrustedDraft
	record             events.ProjectionRecord
	detail             json.RawMessage
	body               []byte
	mission            *core.Mission
	goal               *core.Goal
	team               *core.Team
	blueprint          *core.AgentBlueprint
	profile            *core.ExecutionProfile
	agent              *core.Agent
	intent             *core.Intent
	task               *core.Task
	work               *core.Work
	experiment         *core.Experiment
	promotionCandidate *core.PromotionCandidate
	knowledge          *core.KnowledgeRecord
}

// AppendRecord is retained for non-projection domain records whose bounded
// services own their validation. Every organizational projection namespace is
// reserved for the typed, event-coupled admission paths below.
func (l *SQLite) AppendRecord(ctx context.Context, organizationID, eventType, actorID, taskID string, authorizationRefs, artifactRefs []string, kind, id string, version int, value any) error {
	if kind == "" || id == "" || version < 1 {
		return fmt.Errorf("kind, id, and positive version are required")
	}
	if !genericRecordKindAllowed(kind) {
		return fmt.Errorf("record kind is not admitted by the generic writer")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode record: %w", err)
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		draft := events.TrustedDraft{OrganizationID: organizationID, EventType: eventType, SourceActorID: actorID, TaskID: taskID, AuthorizationRefs: authorizationRefs, ArtifactRefs: artifactRefs, Payload: value}
		return appendRecord(ctx, tx, draft, kind, id, version, body)
	})
}

func genericRecordKindAllowed(kind string) bool {
	switch kind {
	case "approval", "authorization_trace", "capability_lease", "effect", "organization_freeze":
		return true
	default:
		return false
	}
}

// AppendProjection atomically appends a trusted transition and its versioned,
// rebuildable projection record. The event payload includes the full record so
// the records table can be regenerated from the append-only ledger.
func (l *SQLite) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	appended, err := l.AppendProjections(ctx, []events.ProjectionDraft{draft})
	if err != nil {
		return events.Event{}, err
	}
	return appended[0], nil
}

// AppendProjections commits a closed projection set and all authoritative
// events in one SQLite transaction.
func (l *SQLite) AppendProjections(ctx context.Context, drafts []events.ProjectionDraft) ([]events.Event, error) {
	if len(drafts) == 0 {
		return nil, fmt.Errorf("at least one projection is required")
	}
	prepared := make([]preparedProjection, 0, len(drafts))
	for _, draft := range drafts {
		item, err := prepareProjection(draft, false, false)
		if err != nil {
			return nil, err
		}
		if item.task != nil && item.draft.Event.EventType == "EXECUTION_STARTED" && item.task.ExecutionKind == core.ExecutionAgent {
			return nil, fmt.Errorf("agent execution start requires atomic inbox selection")
		}
		prepared = append(prepared, item)
	}
	return l.appendPreparedProjections(ctx, prepared)
}

// AppendWorkCompletion is the only ledger path that admits a completed Work.
// It serializes the prior active projection, the exact durable evidence event,
// and the terminal transition in one transaction.
func (l *SQLite) AppendWorkCompletion(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if draft.Event.EventType != "WORK_COMPLETED" || draft.Event.OrganizationID == "" || draft.Event.SourceActorID != "runtime" || draft.Event.SourceExecutionID != "" || draft.Event.TaskID != "" || draft.Event.CorrelationID == "" || draft.ProjectionKind != "work" || draft.RecordID == "" || draft.Version < 2 {
		return events.Event{}, fmt.Errorf("complete work transition identity is required")
	}
	item, err := prepareProjection(draft, true, false)
	if err != nil {
		return events.Event{}, err
	}
	if item.work == nil || item.work.ID != core.ID(draft.RecordID) || item.work.Status != core.WorkCompleted {
		return events.Event{}, fmt.Errorf("completed work projection is invalid")
	}
	var detail events.WorkCompletionTransitionPayload
	if err := decodeExactJSON(draft.Event.Payload, &detail); err != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" {
		return events.Event{}, fmt.Errorf("completed work requires exact durable evidence")
	}

	var appended events.Event
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		if err := validatePriorActiveWork(ctx, tx, item); err != nil {
			return err
		}
		if err := validateWorkCompletionEvidence(ctx, tx, item, detail); err != nil {
			return err
		}
		var err error
		appended, err = appendPreparedProjection(ctx, tx, item)
		return err
	})
	return appended, err
}

// AppendWorkCompletionEvidence validates the runtime-owned aggregate against
// current durable Work and its complete Task evidence before persisting it.
// A crash may leave admitted evidence without the terminal transition; the
// normal recovery path can safely reuse that immutable event.
func (l *SQLite) AppendWorkCompletionEvidence(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	var evidence events.WorkCompletionEvidencePayload
	if draft.OrganizationID == "" || draft.EventType != "WORK_COMPLETION_EVALUATED" || draft.SourceActorID != "runtime" || draft.SourceExecutionID != "" || draft.RecipientScope != "" || draft.RecipientID != "" || draft.TaskID != "" || len(draft.AuthorizationRefs) != 0 || draft.CorrelationID == "" || decodeExactJSON(draft.Payload, &evidence) != nil || !evidence.Valid() || !slices.Equal(draft.ArtifactRefs, evidence.ArtifactRefs) {
		return events.Event{}, fmt.Errorf("work completion evidence requires a complete typed runtime admission")
	}
	var appended events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		body, found, err := latestRecordBody(ctx, tx, "work", string(evidence.WorkID))
		if err != nil {
			return fmt.Errorf("read Work completion evidence projection: %w", err)
		}
		var record events.ProjectionRecord
		var work core.Work
		if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &work) != nil || record.ProjectionKind != "work" || record.RecordID != string(evidence.WorkID) || record.Version+1 != evidence.WorkVersion || record.CorrelationID != draft.CorrelationID || work.ID != evidence.WorkID || work.Status != core.WorkActive {
			return fmt.Errorf("work completion evidence requires its exact active Work revision")
		}
		completed := work
		completed.Status = core.WorkCompleted
		detail := events.WorkCompletionTransitionPayload{Fingerprint: evidence.Fingerprint}
		item, err := prepareProjection(events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: draft.OrganizationID, EventType: "WORK_COMPLETED", SourceActorID: "runtime",
				CorrelationID: draft.CorrelationID, Payload: detail,
			},
			ProjectionKind: "work", RecordID: string(work.ID), Version: evidence.WorkVersion, Value: completed,
		}, true, false)
		if err != nil {
			return err
		}
		existing, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND event_type='WORK_COMPLETION_EVALUATED' AND correlation_id=? ORDER BY sequence LIMIT 2`, draft.OrganizationID, draft.CorrelationID))
		if err != nil {
			return fmt.Errorf("read existing Work completion evidence: %w", err)
		}
		if len(existing) > 1 {
			return fmt.Errorf("multiple Work completion evidence records")
		}
		if len(existing) == 1 {
			var recorded events.WorkCompletionEvidencePayload
			candidate := existing[0]
			if decodeExactJSONBytes(candidate.Payload, &recorded) != nil || !reflect.DeepEqual(recorded, evidence) || candidate.SourceActorID != "runtime" || candidate.SourceExecutionID != "" || candidate.RecipientScope != "" || candidate.RecipientID != "" || candidate.TaskID != "" || len(candidate.AuthorizationRefs) != 0 || candidate.CorrelationID != draft.CorrelationID || candidate.SchemaVersion != events.SchemaVersion || !slices.Equal(candidate.ArtifactRefs, evidence.ArtifactRefs) {
				return fmt.Errorf("durable Work completion evidence conflicts with current state")
			}
			appended = candidate
		} else {
			appended, err = appendEvent(ctx, tx, draft)
			if err != nil {
				return err
			}
		}
		detail.EvidenceEventRef = appended.EventID
		if err := validateWorkCompletionEvidence(ctx, tx, item, detail); err != nil {
			return fmt.Errorf("admit Work completion evidence: %w", err)
		}
		return nil
	})
	return appended, err
}

// AppendGoalProgress is the only path that may append Goal progress evidence
// or admit GOAL_ACHIEVED. Evidence selection, deterministic evaluation, and
// the optional terminal projection occur under the same SQLite transaction.
func (l *SQLite) AppendGoalProgress(ctx context.Context, organizationID string, goalID core.ID) (events.GoalProgressAdmission, error) {
	if organizationID == "" || goalID == "" {
		return events.GoalProgressAdmission{}, fmt.Errorf("goal progress organization and identity are required")
	}
	var admitted events.GoalProgressAdmission
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		record, goal, err := currentGoalRevision(ctx, tx, organizationID, goalID)
		if err != nil {
			return err
		}
		if goal.Status == core.GoalAchieved {
			admitted, err = validateAchievedGoal(ctx, tx, organizationID, record, goal)
			return err
		}
		if goal.Status != core.GoalActive {
			return fmt.Errorf("goal progress requires an active Goal")
		}
		if err := validateGoalMissionForProgress(ctx, tx, organizationID, goal, true); err != nil {
			return err
		}
		workEvidence, err := authoritativeGoalWorkEvidence(ctx, tx, organizationID, goal, nil, 0)
		if err != nil {
			return err
		}
		evaluation, err := events.NewGoalProgressEvaluation(goal, record.Version, workEvidence)
		if err != nil {
			return err
		}
		existing, found, err := existingGoalProgressEvaluation(ctx, tx, organizationID, record, goal, evaluation)
		if err != nil {
			return err
		}
		if found {
			admitted = existing
			return nil
		}
		evaluationEvent, err := appendEvent(ctx, tx, events.TrustedDraft{
			OrganizationID: organizationID, EventType: "GOAL_PROGRESS_EVALUATED", SourceActorID: "runtime",
			CorrelationID: record.CorrelationID, Payload: evaluation,
		})
		if err != nil {
			return err
		}
		admitted = events.GoalProgressAdmission{Evaluation: evaluation, EvaluationEvent: evaluationEvent}
		if evaluation.Result != events.GoalProgressTargetAchieved {
			return nil
		}
		achieved := goal
		achieved.Status = core.GoalAchieved
		detail := events.GoalAchievementTransitionPayload{EvidenceEventRef: evaluationEvent.EventID, Fingerprint: evaluation.Fingerprint}
		item, err := prepareProjection(events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: organizationID, EventType: "GOAL_ACHIEVED", SourceActorID: "runtime",
				CorrelationID: record.CorrelationID, Payload: detail,
			},
			ProjectionKind: "goal", RecordID: string(goalID), Version: record.Version + 1, Value: achieved,
		}, false, true)
		if err != nil {
			return err
		}
		transition, err := appendPreparedProjection(ctx, tx, item)
		if err != nil {
			return err
		}
		admitted.GoalTransition = &transition
		return nil
	})
	return admitted, err
}

func (l *SQLite) ValidateGoalAchievement(ctx context.Context, organizationID string, goalID core.ID) error {
	if organizationID == "" || goalID == "" {
		return fmt.Errorf("goal achievement organization and identity are required")
	}
	return l.withTx(ctx, func(tx *sql.Tx) error {
		record, goal, err := currentGoalRevision(ctx, tx, organizationID, goalID)
		if err != nil {
			return err
		}
		if goal.Status != core.GoalAchieved {
			return fmt.Errorf("goal is not achieved")
		}
		_, err = validateAchievedGoal(ctx, tx, organizationID, record, goal)
		return err
	})
}

// ValidateGoalAchievementAdmissions reuses the authoritative ledger evidence
// validator to certify every current achieved Goal in a read-only database.
// Backup verification calls this after proving projection/event admission.
func ValidateGoalAchievementAdmissions(ctx context.Context, db *sql.DB) error {
	return validateReadOnlyAdmissions(ctx, db, "Goal achievement", func(tx *sql.Tx) error {
		current, err := currentProjectionAdmissions[core.Goal](ctx, tx, "goal", "Goal", func(goal core.Goal) core.ID { return goal.ID })
		if err != nil {
			return err
		}
		for _, candidate := range current {
			if candidate.value.Status != core.GoalAchieved {
				continue
			}
			if _, err := validateAchievedGoal(ctx, tx, string(candidate.value.OrganizationID), candidate.record, candidate.value); err != nil {
				return fmt.Errorf("achieved Goal %s lacks exact durable evidence: %w", candidate.value.ID, err)
			}
		}
		return nil
	})
}

// ValidateWorkCompletionAdmissions certifies that every current completed
// Work projection retains one exact transition and its authoritative evidence
// chain. Recovery calls this against its read-only SQLite connection.
func ValidateWorkCompletionAdmissions(ctx context.Context, db *sql.DB) error {
	return validateReadOnlyAdmissions(ctx, db, "Work completion", func(tx *sql.Tx) error {
		current, err := currentProjectionAdmissions[core.Work](ctx, tx, "work", "Work", func(work core.Work) core.ID { return work.ID })
		if err != nil {
			return err
		}
		for _, candidate := range current {
			if candidate.value.Status != core.WorkCompleted {
				continue
			}
			transition, err := exactProjectionTransition(ctx, tx, "WORK_COMPLETED", candidate.record)
			if err != nil {
				return fmt.Errorf("completed Work %s transition is invalid: %w", candidate.value.ID, err)
			}
			payload, _, _ := events.AdmittedProjection(transition)
			var detail events.WorkCompletionTransitionPayload
			if decodeExactJSONBytes(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" {
				return fmt.Errorf("completed Work %s transition is invalid", candidate.value.ID)
			}
			item, err := prepareProjection(events.ProjectionDraft{
				Event: events.TrustedDraft{
					OrganizationID: transition.OrganizationID, EventType: transition.EventType, SourceActorID: transition.SourceActorID,
					SourceExecutionID: transition.SourceExecutionID, RecipientScope: transition.RecipientScope, RecipientID: transition.RecipientID,
					TaskID: transition.TaskID, AuthorizationRefs: transition.AuthorizationRefs, ArtifactRefs: transition.ArtifactRefs,
					CorrelationID: transition.CorrelationID, Payload: detail,
				},
				ProjectionKind: "work", RecordID: string(candidate.value.ID), Version: candidate.record.Version, Value: candidate.value,
			}, true, false)
			if err != nil {
				return fmt.Errorf("completed Work %s transition cannot be prepared: %w", candidate.value.ID, err)
			}
			if err := validateHistoricalPriorActiveWork(ctx, tx, item); err != nil {
				return fmt.Errorf("completed Work %s lacks its exact prior state: %w", candidate.value.ID, err)
			}
			if err := validateWorkCompletionEvidence(ctx, tx, item, detail); err != nil {
				return fmt.Errorf("completed Work %s lacks exact durable evidence: %w", candidate.value.ID, err)
			}
			evidenceEvent, err := scanEvent(tx.QueryRowContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_id=?`, detail.EvidenceEventRef))
			if err != nil || evidenceEvent.Sequence >= transition.Sequence {
				return fmt.Errorf("completed Work %s evidence does not precede its transition", candidate.value.ID)
			}
		}
		return nil
	})
}

// ValidateTaskCompletionAdmissions certifies every current completed Task
// projection against its exact verification decision, outcome, execution, and
// result evidence. Recovery must not infer completion from status alone.
func ValidateTaskCompletionAdmissions(ctx context.Context, db *sql.DB) error {
	return validateReadOnlyAdmissions(ctx, db, "Task completion", func(tx *sql.Tx) error {
		current, err := currentProjectionAdmissions[core.Task](ctx, tx, "task", "Task", func(task core.Task) core.ID { return task.ID })
		if err != nil {
			return err
		}
		teamBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='team' ORDER BY r.record_id,r.version`)
		if err != nil {
			return fmt.Errorf("read Task completion Team history: %w", err)
		}
		inboxObservations, err := inboxObservationBindings(ctx, tx)
		if err != nil {
			return fmt.Errorf("read Task completion inbox observations: %w", err)
		}
		streams := make(map[string][]events.Event)
		teamRevisions := make(map[string]map[core.ID][]events.TeamRevisionBinding)
		for _, candidate := range current {
			if candidate.value.Status != core.TaskCompleted {
				continue
			}
			transition, err := exactProjectionTransition(ctx, tx, "TASK_VERIFIED_COMPLETE", candidate.record)
			if err != nil {
				return fmt.Errorf("completed Task %s transition is invalid: %w", candidate.value.ID, err)
			}
			stream, found := streams[transition.OrganizationID]
			if !found {
				stream, err = collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? ORDER BY sequence`, transition.OrganizationID))
				if err != nil {
					return fmt.Errorf("read completed Task %s event chain: %w", candidate.value.ID, err)
				}
				streams[transition.OrganizationID] = stream
			}
			revisions, found := teamRevisions[transition.OrganizationID]
			if !found {
				revisions, err = events.ResolveTeamRevisionBindings(transition.OrganizationID, teamBodies, stream)
				if err != nil {
					return fmt.Errorf("resolve completed Task %s Team history: %w", candidate.value.ID, err)
				}
				teamRevisions[transition.OrganizationID] = revisions
			}
			binding, err := taskCompletionBinding(ctx, tx, transition.OrganizationID, candidate.record.CorrelationID, candidate.value.WorkID, transition.Sequence, current, revisions, inboxObservations)
			if err != nil {
				return fmt.Errorf("completed Task %s binding: %w", candidate.value.ID, err)
			}
			if _, err := events.ValidateTaskCompletionEvidenceChain(binding, events.WorkCompletionTaskBinding{Task: candidate.value, Version: candidate.record.Version, CorrelationID: candidate.record.CorrelationID}, transition, stream); err != nil {
				return fmt.Errorf("completed Task %s lacks exact durable evidence: %w", candidate.value.ID, err)
			}
		}
		return nil
	})
}

func exactProjectionTransition(ctx context.Context, tx *sql.Tx, eventType string, record events.ProjectionRecord) (events.Event, error) {
	matches, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE event_type=? AND json_extract(payload,'$.projection.projection_kind')=? AND json_extract(payload,'$.projection.record_id')=? AND json_extract(payload,'$.projection.version')=? ORDER BY sequence LIMIT 2`, eventType, record.ProjectionKind, record.RecordID, record.Version))
	if err != nil {
		return events.Event{}, err
	}
	if len(matches) != 1 {
		return events.Event{}, fmt.Errorf("requires one exact transition")
	}
	payload, present, err := events.AdmittedProjection(matches[0])
	if err != nil || !present || !reflect.DeepEqual(payload.Projection, record) {
		return events.Event{}, fmt.Errorf("transition does not match its projection")
	}
	return matches[0], nil
}

func taskCompletionBinding(ctx context.Context, tx *sql.Tx, organizationID, correlationID string, workID core.ID, completionSequence int64, current []currentProjectionAdmission[core.Task], teamRevisions map[core.ID][]events.TeamRevisionBinding, inboxObservations map[string]events.InboxObservationBinding) (events.WorkCompletionBinding, error) {
	workBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='work' AND r.record_id=? AND e.sequence<? ORDER BY r.version DESC LIMIT 1`, string(workID), completionSequence)
	if err != nil {
		return events.WorkCompletionBinding{}, err
	}
	var workRecord events.ProjectionRecord
	var work core.Work
	if len(workBodies) != 1 || decodeExactJSONBytes(workBodies[0], &workRecord) != nil || decodeExactJSONBytes(workRecord.Value, &work) != nil || work.ID != workID || work.Status != core.WorkActive || workRecord.CorrelationID != correlationID {
		return events.WorkCompletionBinding{}, fmt.Errorf("durable Work is invalid")
	}
	intentBody, found, err := latestRecordBody(ctx, tx, "intent", string(work.IntentID))
	if err != nil {
		return events.WorkCompletionBinding{}, err
	}
	var intentRecord events.ProjectionRecord
	var intent core.Intent
	if !found || decodeExactJSONBytes(intentBody, &intentRecord) != nil || decodeExactJSONBytes(intentRecord.Value, &intent) != nil || intent.ID != work.IntentID || intentRecord.CorrelationID != correlationID || string(intent.OrganizationID) != organizationID {
		return events.WorkCompletionBinding{}, fmt.Errorf("durable Intent is invalid")
	}
	tasks := make([]events.WorkCompletionTaskBinding, 0)
	blueprints := make(map[core.ID]core.AgentBlueprint)
	profiles := make(map[core.ID]core.ExecutionProfile)
	for _, candidate := range current {
		if candidate.value.WorkID != workID {
			continue
		}
		tasks = append(tasks, events.WorkCompletionTaskBinding{Task: candidate.value, Version: candidate.record.Version, CorrelationID: candidate.record.CorrelationID})
		if candidate.value.ExecutionKind != core.ExecutionAgent || candidate.value.AgentConfig == nil {
			continue
		}
		config := candidate.value.AgentConfig
		blueprint, err := semanticProjectionRevision[core.AgentBlueprint](ctx, tx, "agent_blueprint", config.BlueprintID, config.BlueprintVersion, func(value core.AgentBlueprint) core.ID { return value.ID }, func(value core.AgentBlueprint) string { return value.Version })
		if err != nil {
			return events.WorkCompletionBinding{}, err
		}
		profile, err := semanticProjectionRevision[core.ExecutionProfile](ctx, tx, "execution_profile", config.ProfileID, config.ProfileVersion, func(value core.ExecutionProfile) core.ID { return value.ID }, func(value core.ExecutionProfile) string { return value.Version })
		if err != nil {
			return events.WorkCompletionBinding{}, err
		}
		blueprints[config.BlueprintID] = blueprint
		profiles[config.ProfileID] = profile
	}
	return events.WorkCompletionBinding{
		OrganizationID: organizationID, CorrelationID: correlationID, Work: work, WorkVersion: workRecord.Version, Intent: intent, Tasks: tasks,
		TeamRevisions: teamRevisions, InboxObservations: inboxObservations, AgentBlueprints: blueprints, ExecutionProfiles: profiles,
	}, nil
}

func semanticProjectionRevision[T any](ctx context.Context, tx *sql.Tx, kind string, id core.ID, version string, identity func(T) core.ID, semanticVersion func(T) string) (T, error) {
	var zero T
	bodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind=? AND r.record_id=? ORDER BY r.version`, kind, string(id))
	if err != nil {
		return zero, err
	}
	var selected T
	for _, body := range bodies {
		var record events.ProjectionRecord
		var candidate T
		if decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &candidate) != nil {
			return zero, fmt.Errorf("decode %s revision", kind)
		}
		if semanticVersion(candidate) == version {
			selected = candidate
		}
	}
	if identity(selected) != id {
		return zero, fmt.Errorf("pinned %s revision is unavailable", kind)
	}
	return selected, nil
}

type currentProjectionAdmission[T any] struct {
	record events.ProjectionRecord
	// Generic instantiations are read by both evidence validators, but Gallow
	// cannot currently connect those reads to this generic declaration.
	// gallow-ignore-next-line unused-field
	value T
}

func currentProjectionAdmissions[T any](ctx context.Context, tx *sql.Tx, kind, label string, identity func(T) core.ID) (final []currentProjectionAdmission[T], finalErr error) {
	rows, err := tx.QueryContext(ctx, `SELECT body FROM records AS current
WHERE kind=? AND version=(SELECT MAX(version) FROM records AS latest WHERE latest.kind=current.kind AND latest.record_id=current.record_id)
ORDER BY record_id`, kind)
	if err != nil {
		return nil, fmt.Errorf("read current %s admissions: %w", label, err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, rows.Close())
	}()
	for rows.Next() {
		// This current-set scan and the single prior-Work check share decoding
		// mechanics but enforce different completeness and lifecycle invariants.
		// gallow-ignore-next-line duplicate-code
		var body []byte
		var candidate currentProjectionAdmission[T]
		if err := rows.Scan(&body); err != nil || decodeExactJSONBytes(body, &candidate.record) != nil || decodeExactJSONBytes(candidate.record.Value, &candidate.value) != nil || candidate.record.ProjectionKind != kind || candidate.record.RecordID != string(identity(candidate.value)) {
			return nil, fmt.Errorf("current %s admission is invalid", label)
		}
		final = append(final, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate current %s admissions: %w", label, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close current %s admissions: %w", label, err)
	}
	return final, nil
}

func validateReadOnlyAdmissions(ctx context.Context, db *sql.DB, label string, validate func(*sql.Tx) error) (finalErr error) {
	if ctx == nil || db == nil {
		return fmt.Errorf("%s validation requires a database and context", label)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin %s validation: %w", label, err)
	}
	defer func() {
		if tx != nil {
			finalErr = errors.Join(finalErr, tx.Rollback())
		}
	}()
	if err := validate(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("complete %s validation: %w", label, err)
	}
	tx = nil
	return nil
}

func validateHistoricalPriorActiveWork(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	if item.work == nil || item.draft.Version < 2 {
		return fmt.Errorf("completed Work requires a prior revision")
	}
	var body []byte
	if err := tx.QueryRowContext(ctx, `SELECT body FROM records WHERE kind='work' AND record_id=? AND version=?`, item.draft.RecordID, item.draft.Version-1).Scan(&body); err != nil {
		return fmt.Errorf("read prior completed Work revision: %w", err)
	}
	var record events.ProjectionRecord
	var prior core.Work
	if decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &prior) != nil {
		return fmt.Errorf("prior completed Work revision is malformed")
	}
	if record.ProjectionKind != "work" || record.RecordID != item.draft.RecordID || record.Version != item.draft.Version-1 {
		return fmt.Errorf("prior completed Work revision has mismatched identity")
	}
	if record.CorrelationID != item.draft.Event.CorrelationID {
		return fmt.Errorf("prior completed Work revision crosses its correlation boundary")
	}
	if prior.Status != core.WorkActive {
		return fmt.Errorf("prior completed Work revision is not active")
	}
	if err := events.ValidateWorkProjectionTransition("WORK_COMPLETED", item.draft.Version, &prior, *item.work); err != nil {
		return fmt.Errorf("completed Work transition is invalid: %w", err)
	}
	return nil
}

func currentGoalRevision(ctx context.Context, tx *sql.Tx, organizationID string, goalID core.ID) (events.ProjectionRecord, core.Goal, error) {
	body, found, err := latestRecordBody(ctx, tx, "goal", string(goalID))
	if err != nil {
		return events.ProjectionRecord{}, core.Goal{}, fmt.Errorf("read Goal progress projection: %w", err)
	}
	var record events.ProjectionRecord
	var goal core.Goal
	if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &goal) != nil || record.ProjectionKind != "goal" || record.RecordID != string(goalID) || record.Version < 1 || record.CorrelationID == "" || goal.ID != goalID || string(goal.OrganizationID) != organizationID || !core.ValidGoal(goal) {
		return events.ProjectionRecord{}, core.Goal{}, fmt.Errorf("goal progress requires an exact durable Goal revision")
	}
	return record, goal, nil
}

func validateGoalMissionForProgress(ctx context.Context, tx *sql.Tx, organizationID string, goal core.Goal, requireActive bool) error {
	body, found, err := latestRecordBody(ctx, tx, "mission", string(goal.MissionID))
	if err != nil {
		return fmt.Errorf("read Goal progress Mission: %w", err)
	}
	var record events.ProjectionRecord
	var mission core.Mission
	if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &mission) != nil || record.ProjectionKind != "mission" || record.RecordID != string(goal.MissionID) || mission.ID != goal.MissionID || string(mission.OrganizationID) != organizationID || !core.ValidMission(mission) || requireActive && mission.Status != core.MissionActive {
		return fmt.Errorf("goal progress requires its valid Mission in the same organization")
	}
	return nil
}

func existingGoalProgressEvaluation(ctx context.Context, tx *sql.Tx, organizationID string, goalRecord events.ProjectionRecord, goal core.Goal, expected events.GoalProgressEvaluatedPayload) (events.GoalProgressAdmission, bool, error) {
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND event_type='GOAL_PROGRESS_EVALUATED' AND json_extract(payload,'$.goal_id')=? AND json_extract(payload,'$.goal_version')=? ORDER BY sequence LIMIT 4097`, organizationID, string(goal.ID), goalRecord.Version))
	if err != nil {
		return events.GoalProgressAdmission{}, false, fmt.Errorf("read Goal progress evaluations: %w", err)
	}
	if len(stream) > 4096 {
		return events.GoalProgressAdmission{}, false, fmt.Errorf("goal progress evaluation history exceeds its admission bound")
	}
	var matched events.Event
	for _, event := range stream {
		var recorded events.GoalProgressEvaluatedPayload
		if decodeExactJSONBytes(event.Payload, &recorded) != nil || event.OrganizationID != organizationID || event.EventType != "GOAL_PROGRESS_EVALUATED" || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != goalRecord.CorrelationID || event.SchemaVersion != events.SchemaVersion || !recorded.Valid() {
			return events.GoalProgressAdmission{}, false, fmt.Errorf("goal progress evaluation crosses its runtime-owned boundary")
		}
		if err := validateGoalRevisionBeforeEvaluation(ctx, tx, organizationID, goalRecord, event.Sequence); err != nil {
			return events.GoalProgressAdmission{}, false, fmt.Errorf("goal progress evaluation predates its Goal revision: %w", err)
		}
		if err := validateActiveMissionAt(ctx, tx, organizationID, goal.MissionID, event.Sequence); err != nil {
			return events.GoalProgressAdmission{}, false, fmt.Errorf("goal progress evaluation lacks its active mission: %w", err)
		}
		selected, err := authoritativeGoalWorkEvidence(ctx, tx, organizationID, goal, recorded.WorkEvidenceRefs, event.Sequence)
		if err != nil || events.ValidateGoalProgressEvaluation(goal, goalRecord.Version, selected, recorded) != nil {
			return events.GoalProgressAdmission{}, false, fmt.Errorf("goal progress evaluation lacks authoritative Work evidence")
		}
		if recorded.Fingerprint != expected.Fingerprint {
			continue
		}
		if !reflect.DeepEqual(recorded, expected) || matched.EventID != "" {
			return events.GoalProgressAdmission{}, false, fmt.Errorf("goal progress evaluation conflicts with durable state")
		}
		matched = event
	}
	if matched.EventID == "" {
		return events.GoalProgressAdmission{}, false, nil
	}
	if expected.Result == events.GoalProgressTargetAchieved {
		return events.GoalProgressAdmission{}, false, fmt.Errorf("achieved Goal evaluation lacks its atomic terminal transition")
	}
	return events.GoalProgressAdmission{Evaluation: expected, EvaluationEvent: matched}, true, nil
}

func validateAchievedGoal(ctx context.Context, tx *sql.Tx, organizationID string, record events.ProjectionRecord, goal core.Goal) (events.GoalProgressAdmission, error) {
	if record.Version < 2 || goal.Mode != core.GoalTarget || goal.Status != core.GoalAchieved {
		return events.GoalProgressAdmission{}, fmt.Errorf("achieved Goal projection is invalid")
	}
	if err := validateGoalMissionForProgress(ctx, tx, organizationID, goal, false); err != nil {
		return events.GoalProgressAdmission{}, err
	}
	priorBody, found, err := recordBodyAtVersion(ctx, tx, "goal", string(goal.ID), record.Version-1)
	if err != nil || !found {
		return events.GoalProgressAdmission{}, fmt.Errorf("read pre-achievement Goal revision: %w", err)
	}
	var priorRecord events.ProjectionRecord
	var prior core.Goal
	if json.Unmarshal(priorBody, &priorRecord) != nil || json.Unmarshal(priorRecord.Value, &prior) != nil || priorRecord.ProjectionKind != "goal" || priorRecord.RecordID != string(goal.ID) || priorRecord.Version != record.Version-1 || priorRecord.CorrelationID != record.CorrelationID || prior.Status != core.GoalActive || !core.ValidGoalRevision(prior, goal) {
		return events.GoalProgressAdmission{}, fmt.Errorf("achieved Goal does not exactly follow its active revision")
	}
	transitions, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND event_type='GOAL_ACHIEVED' AND json_extract(payload,'$.projection.record_id')=? AND json_extract(payload,'$.projection.version')=? ORDER BY sequence LIMIT 2`, organizationID, string(goal.ID), record.Version))
	if err != nil || len(transitions) != 1 {
		return events.GoalProgressAdmission{}, fmt.Errorf("achieved Goal requires one authoritative transition")
	}
	transition := transitions[0]
	var projection events.ProjectionEventPayload
	var detail events.GoalAchievementTransitionPayload
	if decodeExactJSONBytes(transition.Payload, &projection) != nil || !reflect.DeepEqual(projection.Projection, record) || decodeExactJSONBytes(projection.Detail, &detail) != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" || transition.SourceActorID != "runtime" || transition.SourceExecutionID != "" || transition.RecipientScope != "" || transition.RecipientID != "" || transition.TaskID != "" || len(transition.AuthorizationRefs) != 0 || len(transition.ArtifactRefs) != 0 || transition.CorrelationID != record.CorrelationID || transition.SchemaVersion != events.SchemaVersion {
		return events.GoalProgressAdmission{}, fmt.Errorf("achieved Goal transition crosses its runtime-owned boundary")
	}
	row := tx.QueryRowContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_id=?`, detail.EvidenceEventRef)
	evaluationEvent, err := scanEvent(row)
	if err != nil {
		return events.GoalProgressAdmission{}, fmt.Errorf("read Goal achievement evidence: %w", err)
	}
	var evaluation events.GoalProgressEvaluatedPayload
	if decodeExactJSONBytes(evaluationEvent.Payload, &evaluation) != nil || evaluationEvent.Sequence >= transition.Sequence || evaluationEvent.OrganizationID != organizationID || evaluationEvent.EventType != "GOAL_PROGRESS_EVALUATED" || evaluationEvent.SourceActorID != "runtime" || evaluationEvent.SourceExecutionID != "" || evaluationEvent.RecipientScope != "" || evaluationEvent.RecipientID != "" || evaluationEvent.TaskID != "" || len(evaluationEvent.AuthorizationRefs) != 0 || len(evaluationEvent.ArtifactRefs) != 0 || evaluationEvent.CorrelationID != record.CorrelationID || evaluationEvent.SchemaVersion != events.SchemaVersion || evaluation.Result != events.GoalProgressTargetAchieved || evaluation.Fingerprint != detail.Fingerprint {
		return events.GoalProgressAdmission{}, fmt.Errorf("goal achievement evidence crosses its runtime-owned boundary")
	}
	if err := validateGoalRevisionBeforeEvaluation(ctx, tx, organizationID, priorRecord, evaluationEvent.Sequence); err != nil {
		return events.GoalProgressAdmission{}, fmt.Errorf("goal achievement evaluation predates its active revision: %w", err)
	}
	if err := validateActiveMissionAt(ctx, tx, organizationID, prior.MissionID, evaluationEvent.Sequence); err != nil {
		return events.GoalProgressAdmission{}, fmt.Errorf("goal achievement evaluation lacks its active mission: %w", err)
	}
	selected, err := authoritativeGoalWorkEvidence(ctx, tx, organizationID, prior, evaluation.WorkEvidenceRefs, evaluationEvent.Sequence)
	if err != nil {
		return events.GoalProgressAdmission{}, fmt.Errorf("goal achievement lacks authoritative completed-Work evidence: %w", err)
	}
	if err := events.ValidateGoalProgressEvaluation(prior, priorRecord.Version, selected, evaluation); err != nil {
		return events.GoalProgressAdmission{}, fmt.Errorf("goal achievement does not match authoritative completed-Work evidence: %w", err)
	}
	return events.GoalProgressAdmission{Evaluation: evaluation, EvaluationEvent: evaluationEvent, GoalTransition: &transition}, nil
}

func validateGoalRevisionBeforeEvaluation(ctx context.Context, tx *sql.Tx, organizationID string, record events.ProjectionRecord, evaluationSequence int64) error {
	if evaluationSequence < 1 {
		return fmt.Errorf("evaluation sequence is invalid")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND json_extract(payload,'$.projection.projection_kind')='goal' AND json_extract(payload,'$.projection.record_id')=? AND json_extract(payload,'$.projection.version')=? ORDER BY sequence LIMIT 2`, organizationID, record.RecordID, record.Version))
	if err != nil || len(stream) != 1 {
		return fmt.Errorf("goal revision requires one authoritative transition")
	}
	event := stream[0]
	var payload events.ProjectionEventPayload
	validType := record.Version == 1 && event.EventType == "GOAL_CREATED" || record.Version > 1 && (event.EventType == "GOAL_REFINED" || event.EventType == "GOAL_RESUMED")
	if event.Sequence >= evaluationSequence || !validType || decodeExactJSONBytes(event.Payload, &payload) != nil || !reflect.DeepEqual(payload.Projection, record) || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != record.CorrelationID || event.SchemaVersion != events.SchemaVersion {
		return fmt.Errorf("goal revision does not precede its evaluation on the runtime-owned boundary")
	}
	return nil
}

func validateActiveMissionAt(ctx context.Context, tx *sql.Tx, organizationID string, missionID core.ID, evaluationSequence int64) error {
	if organizationID == "" || missionID == "" || evaluationSequence < 1 {
		return fmt.Errorf("mission evaluation boundary is invalid")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND sequence<? AND json_extract(payload,'$.projection.projection_kind')='mission' AND json_extract(payload,'$.projection.record_id')=? ORDER BY sequence DESC LIMIT 1`, organizationID, evaluationSequence, string(missionID)))
	if err != nil || len(stream) != 1 {
		return fmt.Errorf("active mission revision is unavailable")
	}
	event := stream[0]
	var payload events.ProjectionEventPayload
	var mission core.Mission
	if decodeExactJSONBytes(event.Payload, &payload) != nil || decodeExactJSONBytes(payload.Projection.Value, &mission) != nil ||
		payload.Projection.ProjectionKind != "mission" || payload.Projection.RecordID != string(missionID) || payload.Projection.Version < 1 || payload.Projection.CorrelationID == "" ||
		mission.ID != missionID || string(mission.OrganizationID) != organizationID || mission.Status != core.MissionActive || !core.ValidMission(mission) ||
		event.EventType != activeMissionEvent(payload.Projection.Version) || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "" || len(event.AuthorizationRefs) != 0 || len(event.ArtifactRefs) != 0 || event.CorrelationID != payload.Projection.CorrelationID || event.SchemaVersion != events.SchemaVersion {
		return fmt.Errorf("mission was not active on its runtime-owned evaluation boundary")
	}
	body, found, err := recordBodyAtVersion(ctx, tx, "mission", string(missionID), payload.Projection.Version)
	if err != nil || !found {
		return fmt.Errorf("read mission revision at evaluation: %w", err)
	}
	var record events.ProjectionRecord
	if decodeExactJSONBytes(body, &record) != nil || !reflect.DeepEqual(record, payload.Projection) {
		return fmt.Errorf("mission evaluation revision is not authoritative")
	}
	return nil
}

func activeMissionEvent(version int) string {
	if version == 1 {
		return "MISSION_CREATED"
	}
	return "MISSION_REVISED"
}

func authoritativeGoalWorkEvidence(ctx context.Context, tx *sql.Tx, organizationID string, goal core.Goal, selectedRefs []string, beforeSequence int64) ([]events.GoalWorkEvidence, error) {
	if beforeSequence < 0 {
		return nil, fmt.Errorf("goal evidence sequence boundary is invalid")
	}
	if len(selectedRefs) > 4096 {
		return nil, fmt.Errorf("goal completed-Work evidence exceeds its admission bound")
	}
	selected := make(map[string]struct{}, len(selectedRefs))
	for _, ref := range selectedRefs {
		if ref == "" {
			return nil, fmt.Errorf("goal Work evidence reference is empty")
		}
		if _, duplicate := selected[ref]; duplicate {
			return nil, fmt.Errorf("goal Work evidence reference is duplicated")
		}
		selected[ref] = struct{}{}
	}
	var transitions []events.Event
	if len(selected) == 0 {
		var err error
		transitions, err = goalProgressWitnessTransitions(ctx, tx, organizationID, goal)
		if err != nil {
			return nil, err
		}
	} else {
		orderedRefs := append([]string(nil), selectedRefs...)
		sort.Strings(orderedRefs)
		for _, ref := range orderedRefs {
			matches, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND event_type='WORK_COMPLETED' AND json_extract(payload,'$.projection.value.goal_id')=? AND json_extract(payload,'$.detail.evidence_event_ref')=? ORDER BY sequence LIMIT 2`, organizationID, string(goal.ID), ref))
			if err != nil {
				return nil, fmt.Errorf("read selected Goal Work transition: %w", err)
			}
			if len(matches) != 1 {
				return nil, fmt.Errorf("goal progress references missing, duplicated, or cross-tenant Work evidence")
			}
			transitions = append(transitions, matches[0])
		}
	}
	result := make([]events.GoalWorkEvidence, 0, len(transitions))
	seenWorks := make(map[core.ID]struct{}, len(transitions))
	foundRefs := make(map[string]struct{}, len(selected))
	for _, transition := range transitions {
		if beforeSequence > 0 && transition.Sequence >= beforeSequence {
			return nil, fmt.Errorf("goal Work completion does not precede its progress evaluation")
		}
		var payload events.ProjectionEventPayload
		var detail events.WorkCompletionTransitionPayload
		var work core.Work
		if decodeExactJSONBytes(transition.Payload, &payload) != nil || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID == "" || payload.Projection.Version < 2 || payload.Projection.CorrelationID == "" || decodeExactJSONBytes(payload.Projection.Value, &work) != nil || decodeExactJSONBytes(payload.Detail, &detail) != nil || work.ID != core.ID(payload.Projection.RecordID) || work.GoalID != goal.ID || work.Status != core.WorkCompleted || detail.EvidenceEventRef == "" || detail.Fingerprint == "" || transition.SourceActorID != "runtime" || transition.SourceExecutionID != "" || transition.RecipientScope != "" || transition.RecipientID != "" || transition.TaskID != "" || len(transition.AuthorizationRefs) != 0 || len(transition.ArtifactRefs) != 0 || transition.CorrelationID != payload.Projection.CorrelationID || transition.SchemaVersion != events.SchemaVersion {
			return nil, fmt.Errorf("goal completed-Work transition is invalid")
		}
		if _, duplicate := seenWorks[work.ID]; duplicate {
			return nil, fmt.Errorf("goal Work has multiple completion transitions")
		}
		seenWorks[work.ID] = struct{}{}
		if body, found, err := latestRecordBody(ctx, tx, "lab_experiment", "experiment-"+string(work.ID)); err != nil {
			return nil, fmt.Errorf("read Goal Work experiment binding: %w", err)
		} else if found {
			var record events.ProjectionRecord
			var experiment core.Experiment
			if decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &experiment) != nil || experiment.ID != core.ID(record.RecordID) || experiment.WorkID != work.ID || string(experiment.OrganizationID) != organizationID {
				return nil, fmt.Errorf("goal Work experiment binding is invalid")
			}
			return nil, fmt.Errorf("experimental Work cannot provide authoritative Goal progress evidence")
		}
		currentBody, found, err := latestRecordBody(ctx, tx, "work", string(work.ID))
		if err != nil || !found {
			return nil, fmt.Errorf("read current Goal Work projection")
		}
		var current events.ProjectionRecord
		if json.Unmarshal(currentBody, &current) != nil || !reflect.DeepEqual(current, payload.Projection) {
			return nil, fmt.Errorf("goal Work completion is stale or superseded")
		}
		if len(selected) > 0 {
			if _, wanted := selected[detail.EvidenceEventRef]; !wanted {
				continue
			}
		}
		item, err := prepareProjection(events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: organizationID, EventType: "WORK_COMPLETED", SourceActorID: "runtime",
				CorrelationID: payload.Projection.CorrelationID, Payload: detail,
			},
			ProjectionKind: "work", RecordID: string(work.ID), Version: payload.Projection.Version, Value: work,
		}, true, false)
		if err != nil || validateWorkCompletionEvidence(ctx, tx, item, detail) != nil {
			return nil, fmt.Errorf("goal Work completion lacks authoritative evidence")
		}
		row := tx.QueryRowContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_id=?`, detail.EvidenceEventRef)
		evidenceEvent, err := scanEvent(row)
		var evidence events.WorkCompletionEvidencePayload
		if err != nil || decodeExactJSONBytes(evidenceEvent.Payload, &evidence) != nil || evidenceEvent.Sequence >= transition.Sequence || evidence.Fingerprint != detail.Fingerprint {
			return nil, fmt.Errorf("goal Work evidence event is invalid")
		}
		result = append(result, events.GoalWorkEvidence{EventRef: evidenceEvent.EventID, EventAt: evidenceEvent.CreatedAt, Evidence: evidence})
		foundRefs[evidenceEvent.EventID] = struct{}{}
	}
	if len(selected) > 0 && len(foundRefs) != len(selected) {
		return nil, fmt.Errorf("goal progress references missing, stale, or cross-tenant Work evidence")
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("goal progress requires completed-Work evidence")
	}
	return result, nil
}

// goalProgressWitnessTransitions selects a stable, bounded proof set instead
// of embedding every completed Work in each evaluation. The earliest current
// completion that exactly matches each criterion is retained. Before any
// criterion is satisfied, the earliest completion is retained as evidence of
// the evaluation. A Goal revision therefore creates no more than one initial
// evaluation plus one material evaluation per criterion.
func goalProgressWitnessTransitions(ctx context.Context, tx *sql.Tx, organizationID string, goal core.Goal) ([]events.Event, error) {
	byID := make(map[string]events.Event, len(goal.SuccessCriteria))
	for _, criterion := range goal.SuccessCriteria {
		matches, err := collectEvents(tx.QueryContext(ctx, `SELECT transition.event_id,transition.sequence,transition.organization_id,transition.event_type,transition.source_actor_id,transition.source_execution_id,transition.recipient_scope,transition.recipient_id,transition.task_id,transition.authorization_refs,transition.artifact_refs,transition.payload,transition.correlation_id,transition.created_at,transition.schema_version
FROM events AS transition
JOIN events AS evidence ON evidence.event_id=json_extract(transition.payload,'$.detail.evidence_event_ref')
JOIN json_each(evidence.payload,'$.criteria') AS criterion
WHERE transition.organization_id=? AND transition.event_type='WORK_COMPLETED'
  AND json_extract(transition.payload,'$.projection.value.goal_id')=?
	AND NOT EXISTS (
		SELECT 1 FROM records AS experiment
		WHERE experiment.kind='lab_experiment'
		  AND experiment.record_id='experiment-' || json_extract(transition.payload,'$.projection.value.id')
		  AND json_extract(experiment.body,'$.value.organization_id')=?
	)
  AND evidence.organization_id=? AND evidence.event_type='WORK_COMPLETION_EVALUATED'
  AND json_extract(criterion.value,'$.value')=?
  AND json_extract(criterion.value,'$.origin')=?
  AND COALESCE(json_extract(criterion.value,'$.source_message_id'),'')=?
ORDER BY transition.sequence,transition.event_id LIMIT 1`, organizationID, string(goal.ID), organizationID, organizationID, criterion.Value, criterion.Origin, criterion.SourceMessageID))
		if err != nil {
			return nil, fmt.Errorf("read Goal criterion witness: %w", err)
		}
		if len(matches) == 1 {
			byID[matches[0].EventID] = matches[0]
		}
	}
	if len(byID) == 0 {
		matches, err := collectEvents(tx.QueryContext(ctx, `SELECT transition.event_id,transition.sequence,transition.organization_id,transition.event_type,transition.source_actor_id,transition.source_execution_id,transition.recipient_scope,transition.recipient_id,transition.task_id,transition.authorization_refs,transition.artifact_refs,transition.payload,transition.correlation_id,transition.created_at,transition.schema_version
FROM events AS transition
WHERE transition.organization_id=? AND transition.event_type='WORK_COMPLETED'
  AND json_extract(transition.payload,'$.projection.value.goal_id')=?
	AND NOT EXISTS (
		SELECT 1 FROM records AS experiment
		WHERE experiment.kind='lab_experiment'
		  AND experiment.record_id='experiment-' || json_extract(transition.payload,'$.projection.value.id')
		  AND json_extract(experiment.body,'$.value.organization_id')=?
	)
ORDER BY transition.sequence,transition.event_id LIMIT 1`, organizationID, string(goal.ID), organizationID))
		if err != nil {
			return nil, fmt.Errorf("read initial Goal Work witness: %w", err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("goal progress requires completed-Work evidence")
		}
		byID[matches[0].EventID] = matches[0]
	}
	transitions := make([]events.Event, 0, len(byID))
	for _, transition := range byID {
		transitions = append(transitions, transition)
	}
	sort.Slice(transitions, func(i, j int) bool {
		if transitions[i].Sequence == transitions[j].Sequence {
			return transitions[i].EventID < transitions[j].EventID
		}
		return transitions[i].Sequence < transitions[j].Sequence
	})
	return transitions, nil
}

func (l *SQLite) AppendExecutionStart(ctx context.Context, draft events.ProjectionDraft, routes []events.InboxRoute, validate events.ExecutionStartValidator) (events.Event, []events.InboxSelection, error) {
	var requested events.ExecutionStartDetail
	if draft.Event.EventType != "EXECUTION_STARTED" || draft.Event.OrganizationID == "" || draft.Event.SourceActorID != "runtime" || draft.Event.SourceExecutionID != "" || draft.Event.TaskID == "" || draft.Event.TaskID != draft.RecordID || draft.Event.CorrelationID == "" || draft.ProjectionKind != "task" || draft.RecordID == "" || draft.Version < 2 || decodeExactJSON(draft.Event.Payload, &requested) != nil || requested.InboxCutoffSequence != 0 || requested.DispatchBinding != nil {
		return events.Event{}, nil, fmt.Errorf("complete execution-start boundary is required")
	}
	value, err := json.Marshal(draft.Value)
	if err != nil {
		return events.Event{}, nil, fmt.Errorf("encode execution-start Task: %w", err)
	}
	var task core.Task
	if json.Unmarshal(value, &task) != nil || task.ID != core.ID(draft.RecordID) || task.Status != core.TaskRunning {
		return events.Event{}, nil, fmt.Errorf("execution-start task is invalid")
	}
	switch task.ExecutionKind {
	case core.ExecutionAgent:
		if task.AssigneeType != "AGENT" || task.AssigneeID == "" || len(routes) < 2 || validate == nil || requested.InputEventRef != "" || requested.Mode != "" && requested.Mode != "BLOCKED_DEPENDENCY_REMEDIATION" {
			return events.Event{}, nil, fmt.Errorf("agent execution-start boundary is invalid")
		}
	case core.ExecutionDeterministic:
		if len(routes) != 0 || validate != nil || requested.Mode != "" || requested.InputEventRef != "" {
			return events.Event{}, nil, fmt.Errorf("deterministic execution-start boundary is invalid")
		}
	case core.ExecutionHuman:
		if len(routes) != 0 || validate != nil || requested.Mode != "OPERATOR_HUMAN_INPUT" && requested.Mode != "STRUCTURED_HUMAN_COMPLETION" || requested.InputEventRef == "" {
			return events.Event{}, nil, fmt.Errorf("user execution-start boundary is invalid")
		}
	case core.ExecutionTool, core.ExecutionTeam, core.ExecutionMixed:
		return events.Event{}, nil, fmt.Errorf("execution-start kind is unavailable")
	default:
		return events.Event{}, nil, fmt.Errorf("execution-start kind is unavailable")
	}
	var started events.Event
	var selections []events.InboxSelection
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		if err := validatePriorExecutionTask(ctx, tx, draft, task); err != nil {
			return err
		}
		if task.ExecutionKind == core.ExecutionHuman {
			inputEvents, err := exactEventsByID(ctx, tx, draft.Event.OrganizationID, requested.InputEventRef)
			if err != nil || len(inputEvents) != 1 || inputEvents[0].CorrelationID != draft.Event.CorrelationID || inputEvents[0].TaskID != string(task.ID) {
				return fmt.Errorf("user execution start lacks its exact durable input event")
			}
			if requested.Mode == "OPERATOR_HUMAN_INPUT" {
				if _, err := events.DecodeDurableOperatorInput(inputEvents[0]); err != nil {
					return fmt.Errorf("user execution start input event is invalid: %w", err)
				}
			} else if _, err := events.DecodeHumanTaskCompletion(inputEvents[0]); err != nil {
				return fmt.Errorf("structured user completion start event is invalid: %w", err)
			}
		}
		var dispatch *events.AgentDispatchBinding
		var cutoff int64
		if task.ExecutionKind == core.ExecutionAgent {
			bound, err := bindAgentDispatch(ctx, tx, draft, task)
			if err != nil {
				return err
			}
			dispatch = &bound
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0) FROM events`).Scan(&cutoff); err != nil {
				return fmt.Errorf("read Agent execution inbox cutoff: %w", err)
			}
		}
		stream, err := validateStrategicExecutionStart(ctx, tx, draft, task, requested)
		if err != nil {
			return err
		}
		if dispatch != nil {
			roster, err := exactEventsByID(ctx, tx, draft.Event.OrganizationID, dispatch.AgentEventRef, dispatch.BlueprintEventRef, dispatch.ExecutionProfileEventRef)
			if err != nil {
				return fmt.Errorf("read Agent execution roster boundary: %w", err)
			}
			stream = append(stream, roster...)
		}
		draft.Event.Payload = events.ExecutionStartDetail{
			Mode: requested.Mode, InputEventRef: requested.InputEventRef, InboxCutoffSequence: cutoff, DispatchBinding: dispatch,
			StrategicEventRefs: append([]string(nil), requested.StrategicEventRefs...), StrategicContextRefs: append([]core.VersionedRef(nil), requested.StrategicContextRefs...),
		}
		item, err := prepareProjection(draft, false, false)
		if err != nil {
			return err
		}
		started, err = appendPreparedProjection(ctx, tx, item)
		if err != nil {
			return err
		}
		stream = append(stream, started)
		if task.ExecutionKind != core.ExecutionAgent {
			return nil
		}
		if err := events.ValidateAgentDispatchStart(started, task, draft.Version, stream); err != nil {
			return fmt.Errorf("validate Agent dispatch admission: %w", err)
		}
		teamBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='team' ORDER BY r.record_id,r.version`)
		if err != nil {
			return fmt.Errorf("read Agent execution Team history: %w", err)
		}
		teamEvents, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND json_extract(payload,'$.projection.projection_kind')='team' ORDER BY sequence`, draft.Event.OrganizationID))
		if err != nil {
			return fmt.Errorf("read Agent execution Team event history: %w", err)
		}
		teamRevisions, err := events.ResolveTeamRevisionBindings(draft.Event.OrganizationID, teamBodies, teamEvents)
		if err != nil {
			return fmt.Errorf("resolve Agent execution Team history: %w", err)
		}
		expectedRoutes := events.AgentExecutionRoutes(teamRevisions, task, started.Sequence)
		if !reflect.DeepEqual(routes, expectedRoutes) {
			return fmt.Errorf("agent execution inbox routes do not match admitted assignment and team membership")
		}
		selections = make([]events.InboxSelection, 0, len(routes))
		for _, route := range routes {
			selected, err := collectEvents(tx.QueryContext(ctx, `SELECT e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM inbox i JOIN events e ON e.event_id=i.event_id AND e.organization_id=i.organization_id AND e.recipient_scope=i.recipient_scope AND e.recipient_id=i.recipient_id
WHERE i.organization_id=? AND i.recipient_scope=? AND i.recipient_id=? AND i.observed_at='' AND e.sequence<=?
ORDER BY e.sequence`, draft.Event.OrganizationID, route.Scope, route.ID, cutoff))
			if err != nil {
				return fmt.Errorf("select Agent execution inbox %s/%s: %w", route.Scope, route.ID, err)
			}
			selections = append(selections, events.InboxSelection{Route: route, Events: selected})
		}
		if err := validate(selections); err != nil {
			return fmt.Errorf("validate bounded Agent execution input: %w", err)
		}
		return nil
	})
	return started, selections, err
}

func validateStrategicExecutionStart(ctx context.Context, tx *sql.Tx, draft events.ProjectionDraft, task core.Task, requested events.ExecutionStartDetail) ([]events.Event, error) {
	body, found, err := latestRecordBody(ctx, tx, "work", string(task.WorkID))
	if err != nil {
		return nil, fmt.Errorf("read execution Work: %w", err)
	}
	var record events.ProjectionRecord
	var work core.Work
	if !found || decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &work) != nil || work.ID != task.WorkID || work.IntentID == "" || work.Status != core.WorkActive || record.CorrelationID != draft.Event.CorrelationID {
		return nil, fmt.Errorf("execution strategic Work is invalid")
	}
	if work.GoalID == "" {
		if len(requested.StrategicEventRefs) != 0 || len(requested.StrategicContextRefs) != 0 {
			return nil, fmt.Errorf("ad hoc execution cannot bind strategic context")
		}
		return nil, nil
	}
	stream, err := boundedStrategicExecutionEvents(ctx, tx, draft.Event.OrganizationID, draft.Event.CorrelationID, work, requested)
	if err != nil {
		return nil, err
	}
	intentBody, intentFound, err := latestRecordBody(ctx, tx, "intent", string(work.IntentID))
	if err != nil {
		return nil, fmt.Errorf("read execution strategic Intent: %w", err)
	}
	var intentRecord events.ProjectionRecord
	var intent core.Intent
	if !intentFound || decodeExactJSONBytes(intentBody, &intentRecord) != nil || decodeExactJSONBytes(intentRecord.Value, &intent) != nil ||
		intent.ID != work.IntentID || intent.OrganizationID != core.ID(draft.Event.OrganizationID) || intent.AcceptedFingerprint == "" || intentRecord.CorrelationID != draft.Event.CorrelationID {
		return nil, fmt.Errorf("execution strategic Intent is invalid")
	}
	plan, strategy, err := events.ResolvePlanStrategicContext(draft.Event.OrganizationID, draft.Event.CorrelationID, work, intent, stream)
	if err != nil || strategy == nil || strategy.Mission.Status != core.MissionActive || strategy.Goal.Status != core.GoalActive {
		return nil, fmt.Errorf("execution strategic Plan is stale or invalid")
	}
	_, currentEventRefs, currentContextRefs, err := events.ResolveStrategicContext(draft.Event.OrganizationID, work, stream, 0)
	if err != nil || !slices.Equal(plan.StrategicEventRefs, requested.StrategicEventRefs) || !slices.Equal(plan.StrategicContextRefs, requested.StrategicContextRefs) ||
		!slices.Equal(currentEventRefs, requested.StrategicEventRefs) || !slices.Equal(currentContextRefs, requested.StrategicContextRefs) {
		return nil, fmt.Errorf("%w before transactional execution start", events.ErrStrategicContextChanged)
	}
	return stream, nil
}

func boundedStrategicExecutionEvents(ctx context.Context, tx *sql.Tx, organizationID, correlationID string, work core.Work, requested events.ExecutionStartDetail) ([]events.Event, error) {
	if organizationID == "" || correlationID == "" || work.GoalID == "" || len(requested.StrategicEventRefs) != 2 || len(requested.StrategicContextRefs) != 2 {
		return nil, fmt.Errorf("execution strategic references are incomplete")
	}
	plans, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND correlation_id=? AND event_type='PLAN_CREATED' ORDER BY sequence LIMIT 2`, organizationID, correlationID))
	if err != nil || len(plans) != 1 {
		return nil, fmt.Errorf("read exact execution strategic Plan")
	}
	goalBody, found, err := latestRecordBody(ctx, tx, "goal", string(work.GoalID))
	if err != nil || !found {
		return nil, fmt.Errorf("read current execution Goal")
	}
	var goalRecord events.ProjectionRecord
	var goal core.Goal
	if decodeExactJSONBytes(goalBody, &goalRecord) != nil || decodeExactJSONBytes(goalRecord.Value, &goal) != nil || goal.ID != work.GoalID || goal.MissionID == "" || string(goal.OrganizationID) != organizationID {
		return nil, fmt.Errorf("current execution Goal is invalid")
	}
	goalEventRef, err := latestAdmissionEventRef(ctx, tx, "goal", string(goal.ID))
	if err != nil {
		return nil, err
	}
	missionEventRef, err := latestAdmissionEventRef(ctx, tx, "mission", string(goal.MissionID))
	if err != nil {
		return nil, err
	}
	selected, err := exactEventsByID(ctx, tx, organizationID,
		requested.StrategicEventRefs[0], requested.StrategicEventRefs[1], missionEventRef, goalEventRef)
	if err != nil {
		return nil, fmt.Errorf("read bounded execution strategic events: %w", err)
	}
	return append(plans, selected...), nil
}

func latestAdmissionEventRef(ctx context.Context, tx *sql.Tx, kind, recordID string) (string, error) {
	var eventRef string
	if err := tx.QueryRowContext(ctx, `SELECT admission_event_id FROM records WHERE kind=? AND record_id=? ORDER BY version DESC LIMIT 1`, kind, recordID).Scan(&eventRef); err != nil || eventRef == "" {
		return "", fmt.Errorf("read current %s admission reference", kind)
	}
	return eventRef, nil
}

func exactEventsByID(ctx context.Context, tx *sql.Tx, organizationID string, eventRefs ...string) ([]events.Event, error) {
	seen := make(map[string]struct{}, len(eventRefs))
	selected := make([]events.Event, 0, len(eventRefs))
	for _, eventRef := range eventRefs {
		if eventRef == "" {
			return nil, fmt.Errorf("event reference is empty")
		}
		if _, duplicate := seen[eventRef]; duplicate {
			continue
		}
		seen[eventRef] = struct{}{}
		row := tx.QueryRowContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_id=?`, eventRef)
		event, err := scanEvent(row)
		if err != nil || event.OrganizationID != organizationID {
			return nil, fmt.Errorf("event reference %s is missing or crosses its organization", eventRef)
		}
		selected = append(selected, event)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Sequence < selected[j].Sequence })
	return selected, nil
}

func validatePriorExecutionTask(ctx context.Context, tx *sql.Tx, draft events.ProjectionDraft, task core.Task) error {
	body, found, err := latestRecordBody(ctx, tx, "task", draft.RecordID)
	if err != nil {
		return fmt.Errorf("read prior execution Task: %w", err)
	}
	var record events.ProjectionRecord
	var prior core.Task
	if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &prior) != nil || record.ProjectionKind != "task" || record.RecordID != draft.RecordID || record.Version < 1 || record.CorrelationID != draft.Event.CorrelationID || draft.Version != record.Version+1 || prior.ID != task.ID || prior.Status != core.TaskPending {
		return fmt.Errorf("execution start does not follow an eligible exact task revision")
	}
	prior.Status = task.Status
	if !reflect.DeepEqual(prior, task) {
		return fmt.Errorf("execution start changes the immutable task contract")
	}
	return nil
}

type dispatchRosterRevision[T any] struct {
	record events.ProjectionRecord
	// Generic instantiations are read by dispatch admission, but Gallow cannot
	// currently connect those reads to this generic declaration.
	// gallow-ignore-next-line unused-field
	value    T
	eventRef string
}

func bindAgentDispatch(ctx context.Context, tx *sql.Tx, draft events.ProjectionDraft, task core.Task) (events.AgentDispatchBinding, error) {
	if task.AgentConfig == nil {
		return events.AgentDispatchBinding{}, fmt.Errorf("agent dispatch requires an exact pinned Task configuration")
	}
	agent, err := latestDispatchRosterRevision[core.Agent](ctx, tx, "agent", task.AssigneeID)
	if err != nil {
		return events.AgentDispatchBinding{}, fmt.Errorf("bind Agent dispatch assignee: %w", err)
	}
	config := task.AgentConfig
	blueprint, err := latestDispatchRosterRevision[core.AgentBlueprint](ctx, tx, "agent_blueprint", config.BlueprintID)
	if err != nil {
		return events.AgentDispatchBinding{}, fmt.Errorf("bind Agent dispatch blueprint: %w", err)
	}
	profile, err := latestDispatchRosterRevision[core.ExecutionProfile](ctx, tx, "execution_profile", config.ProfileID)
	if err != nil {
		return events.AgentDispatchBinding{}, fmt.Errorf("bind Agent dispatch execution profile: %w", err)
	}
	if !core.ValidAgent(agent.value) || agent.value.ID != task.AssigneeID || agent.value.OrganizationID != core.ID(draft.Event.OrganizationID) || agent.value.Status != "ACTIVE" || blueprint.value.Status != "ACTIVE" || profile.value.Status != "ACTIVE" ||
		blueprint.value.ID != config.BlueprintID || blueprint.value.OrganizationID != agent.value.OrganizationID || blueprint.value.Version != config.BlueprintVersion || profile.value.ID != config.ProfileID || profile.value.OrganizationID != agent.value.OrganizationID || profile.value.Version != config.ProfileVersion {
		return events.AgentDispatchBinding{}, fmt.Errorf("agent dispatch requires its active exact assigned roster configuration")
	}
	return events.AgentDispatchBinding{
		DispatchID:                    core.ID(fmt.Sprintf("execution-%s-v%d", task.ID, draft.Version)),
		OrganizationID:                core.ID(draft.Event.OrganizationID),
		TaskID:                        task.ID,
		TaskVersion:                   draft.Version,
		AgentID:                       agent.value.ID,
		AgentRecordVersion:            agent.record.Version,
		AgentEventRef:                 agent.eventRef,
		BlueprintID:                   blueprint.value.ID,
		BlueprintRecordVersion:        blueprint.record.Version,
		BlueprintVersion:              blueprint.value.Version,
		BlueprintEventRef:             blueprint.eventRef,
		ExecutionProfileID:            profile.value.ID,
		ExecutionProfileRecordVersion: profile.record.Version,
		ExecutionProfileVersion:       profile.value.Version,
		ExecutionProfileEventRef:      profile.eventRef,
		RuntimeAdapter:                config.RuntimeAdapter,
	}, nil
}

func latestDispatchRosterRevision[T any](ctx context.Context, tx *sql.Tx, kind string, id core.ID) (dispatchRosterRevision[T], error) {
	var result dispatchRosterRevision[T]
	body, found, err := latestRecordBody(ctx, tx, kind, string(id))
	if err != nil {
		return result, err
	}
	if !found || decodeExactJSONBytes(body, &result.record) != nil || decodeExactJSONBytes(result.record.Value, &result.value) != nil || result.record.ProjectionKind != kind || result.record.RecordID != string(id) || result.record.Version < 1 {
		return dispatchRosterRevision[T]{}, fmt.Errorf("active exact %s revision %s is unavailable", kind, id)
	}
	if err := tx.QueryRowContext(ctx, `SELECT admission_event_id FROM records WHERE kind=? AND record_id=? AND version=?`, kind, string(id), result.record.Version).Scan(&result.eventRef); err != nil {
		return dispatchRosterRevision[T]{}, fmt.Errorf("read %s dispatch admission reference: %w", kind, err)
	}
	if result.eventRef == "" {
		return dispatchRosterRevision[T]{}, fmt.Errorf("%s dispatch admission reference is empty", kind)
	}
	return result, nil
}

func prepareProjection(draft events.ProjectionDraft, allowWorkCompletion, allowGoalAchievement bool) (preparedProjection, error) {
	if draft.Event.EventType == "" || draft.ProjectionKind == "" || draft.RecordID == "" || draft.Version < 1 {
		return preparedProjection{}, fmt.Errorf("event type, projection kind, record id, and positive version are required")
	}
	value, err := json.Marshal(draft.Value)
	if err != nil {
		return preparedProjection{}, fmt.Errorf("encode projection value: %w", err)
	}
	record := events.ProjectionRecord{ProjectionKind: draft.ProjectionKind, RecordID: draft.RecordID, Version: draft.Version, CorrelationID: draft.Event.CorrelationID, Value: value}
	body, err := json.Marshal(record)
	if err != nil {
		return preparedProjection{}, fmt.Errorf("encode projection record: %w", err)
	}
	detail, err := json.Marshal(draft.Event.Payload)
	if err != nil {
		return preparedProjection{}, fmt.Errorf("encode projection event detail: %w", err)
	}
	eventDraft := draft.Event
	item := preparedProjection{draft: draft, eventDraft: eventDraft, record: record, detail: detail, body: body}
	boundary := events.Event{
		OrganizationID: draft.Event.OrganizationID, EventType: draft.Event.EventType,
		SourceActorID: draft.Event.SourceActorID, SourceExecutionID: draft.Event.SourceExecutionID,
		RecipientScope: draft.Event.RecipientScope, RecipientID: draft.Event.RecipientID, TaskID: draft.Event.TaskID,
		AuthorizationRefs: draft.Event.AuthorizationRefs, ArtifactRefs: draft.Event.ArtifactRefs,
		CorrelationID: draft.Event.CorrelationID, SchemaVersion: events.SchemaVersion,
	}
	if err := events.ValidateProjectionEventBoundary(boundary, events.ProjectionEventPayload{Projection: record}); err != nil {
		return preparedProjection{}, fmt.Errorf("projection boundary: %w", err)
	}
	switch draft.ProjectionKind {
	case "organization":
		var organization core.Organization
		if err := decodeExactJSONBytes(value, &organization); err != nil {
			return preparedProjection{}, fmt.Errorf("decode organization projection: %w", err)
		}
	case "mission":
		var mission core.Mission
		if err := decodeExactJSONBytes(value, &mission); err != nil {
			return preparedProjection{}, fmt.Errorf("decode mission projection: %w", err)
		}
		item.mission = &mission
	case "goal":
		var goal core.Goal
		if err := decodeExactJSONBytes(value, &goal); err != nil {
			return preparedProjection{}, fmt.Errorf("decode goal projection: %w", err)
		}
		// Goal and Work terminal gates are deliberately separate typed checks;
		// Gallow's token window crosses the unrelated switch cases between them.
		// gallow-ignore-next-line duplicate-code
		if !allowGoalAchievement && (goal.Status == core.GoalAchieved || draft.Event.EventType == "GOAL_ACHIEVED") {
			return preparedProjection{}, fmt.Errorf("achieved Goal requires evidence-backed admission")
		}
		item.goal = &goal
	case "team":
		var team core.Team
		if err := decodeExactJSONBytes(value, &team); err != nil {
			return preparedProjection{}, fmt.Errorf("decode Team projection: %w", err)
		}
		item.team = &team
	case "agent_blueprint":
		var blueprint core.AgentBlueprint
		if err := decodeExactJSONBytes(value, &blueprint); err != nil {
			return preparedProjection{}, fmt.Errorf("decode Agent blueprint projection: %w", err)
		}
		item.blueprint = &blueprint
	case "execution_profile":
		var profile core.ExecutionProfile
		if err := decodeExactJSONBytes(value, &profile); err != nil {
			return preparedProjection{}, fmt.Errorf("decode execution profile projection: %w", err)
		}
		item.profile = &profile
	case "agent":
		var agent core.Agent
		if err := decodeExactJSONBytes(value, &agent); err != nil {
			return preparedProjection{}, fmt.Errorf("decode Agent projection: %w", err)
		}
		item.agent = &agent
	case "intent":
		var intent core.Intent
		if err := decodeExactJSONBytes(value, &intent); err != nil {
			return preparedProjection{}, fmt.Errorf("decode intent for external work index: %w", err)
		}
		item.intent = &intent
	case "task":
		var task core.Task
		if err := decodeExactJSONBytes(value, &task); err != nil {
			return preparedProjection{}, fmt.Errorf("decode task for external work index: %w", err)
		}
		item.task = &task
	case "work":
		var work core.Work
		if err := decodeExactJSONBytes(value, &work); err != nil {
			return preparedProjection{}, fmt.Errorf("decode work projection: %w", err)
		}
		if !allowWorkCompletion && (work.Status == core.WorkCompleted || draft.Event.EventType == "WORK_COMPLETED") {
			return preparedProjection{}, fmt.Errorf("completed work requires evidence-backed admission")
		}
		item.work = &work
	case "lab_experiment":
		var experiment core.Experiment
		if err := decodeExactJSONBytes(value, &experiment); err != nil {
			return preparedProjection{}, fmt.Errorf("decode Lab experiment projection: %w", err)
		}
		item.experiment = &experiment
	case "lab_promotion_candidate":
		var candidate core.PromotionCandidate
		if err := decodeExactJSONBytes(value, &candidate); err != nil {
			return preparedProjection{}, fmt.Errorf("decode Lab promotion-candidate projection: %w", err)
		}
		item.promotionCandidate = &candidate
	case "knowledge":
		var knowledge core.KnowledgeRecord
		if err := decodeExactJSONBytes(value, &knowledge); err != nil {
			return preparedProjection{}, fmt.Errorf("decode knowledge projection: %w", err)
		}
		item.knowledge = &knowledge
	}
	return item, nil
}

func (l *SQLite) appendPreparedProjections(ctx context.Context, prepared []preparedProjection) ([]events.Event, error) {
	appended := make([]events.Event, 0, len(prepared))
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		containsTask := false
		for _, item := range prepared {
			event, err := appendPreparedProjection(ctx, tx, item)
			if err != nil {
				return err
			}
			appended = append(appended, event)
			containsTask = containsTask || item.task != nil
		}
		if containsTask {
			if err := validateClosedTaskGraph(ctx, tx); err != nil {
				return fmt.Errorf("validate closed Task graph: %w", err)
			}
		}
		return nil
	})
	return appended, err
}

func validateClosedTaskGraph(ctx context.Context, tx *sql.Tx) (finalErr error) {
	rows, err := tx.QueryContext(ctx, `SELECT body FROM records AS current
WHERE kind='task' AND version=(SELECT MAX(version) FROM records AS latest WHERE latest.kind=current.kind AND latest.record_id=current.record_id)
ORDER BY record_id`)
	if err != nil {
		return fmt.Errorf("read current Task graph: %w", err)
	}
	defer func() {
		finalErr = errors.Join(finalErr, rows.Close())
	}()
	type admittedTask struct {
		task          core.Task
		correlationID string
	}
	admitted := make(map[core.ID]admittedTask)
	tasks := make(map[core.ID]core.Task)
	for rows.Next() {
		var body []byte
		var record events.ProjectionRecord
		var task core.Task
		if err := rows.Scan(&body); err != nil || decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &task) != nil || record.ProjectionKind != "task" || record.RecordID != string(task.ID) || record.Version < 1 || record.CorrelationID == "" || !core.ValidTask(task) {
			return fmt.Errorf("current Task graph contains an invalid admission")
		}
		admitted[task.ID] = admittedTask{task: task, correlationID: record.CorrelationID}
		tasks[task.ID] = task
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate current Task graph: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close current Task graph: %w", err)
	}
	if err := core.ValidateTaskDAG(tasks); err != nil {
		return err
	}
	for id, state := range admitted {
		if state.task.ParentID != "" {
			parent, ok := admitted[state.task.ParentID]
			if !ok || parent.task.WorkID != state.task.WorkID || parent.correlationID != state.correlationID {
				return fmt.Errorf("task %s references invalid parent %s", id, state.task.ParentID)
			}
		}
		for _, dependencyID := range state.task.DependsOn {
			dependency := admitted[dependencyID]
			if dependency.task.WorkID != state.task.WorkID || dependency.correlationID != state.correlationID {
				return fmt.Errorf("task %s references cross-boundary dependency %s", id, dependencyID)
			}
		}
		if err := validateClosedTaskAssignment(ctx, tx, state.task, state.correlationID); err != nil {
			return err
		}
	}
	return nil
}

func validateClosedTaskAssignment(ctx context.Context, tx *sql.Tx, task core.Task, correlationID string) error {
	workRecord, work, found, err := latestProjectionRevision[core.Work](ctx, tx, "work", string(task.WorkID))
	if err != nil || !found || work.ID != task.WorkID || workRecord.CorrelationID != correlationID {
		return fmt.Errorf("task %s lacks its exact Work boundary", task.ID)
	}
	intentRecord, intent, found, err := latestProjectionRevision[core.Intent](ctx, tx, "intent", string(work.IntentID))
	if err != nil || !found || intent.ID != work.IntentID || intentRecord.CorrelationID != correlationID || intent.OrganizationID == "" {
		return fmt.Errorf("task %s lacks its exact Intent boundary", task.ID)
	}
	graph := core.DurableGraph{
		Teams:             map[core.ID]core.DurableState[core.Team]{},
		AgentBlueprints:   map[core.ID]core.DurableState[core.AgentBlueprint]{},
		ExecutionProfiles: map[core.ID]core.DurableState[core.ExecutionProfile]{},
		Agents:            map[core.ID]core.DurableState[core.Agent]{},
	}
	switch task.AssigneeType {
	case "AGENT":
		agentRecord, agent, agentFound, agentErr := latestProjectionRevision[core.Agent](ctx, tx, "agent", string(task.AssigneeID))
		if agentErr != nil || !agentFound || agent.ID != task.AssigneeID || !core.ValidAgent(agent) {
			return fmt.Errorf("task %s references invalid assignee agent %s", task.ID, task.AssigneeID)
		}
		graph.Agents[agent.ID] = core.DurableState[core.Agent]{Version: agentRecord.Version, CorrelationID: agentRecord.CorrelationID, Value: agent}
		if err := loadTaskAssignmentBlueprint(ctx, tx, graph.AgentBlueprints, agent.BlueprintID); err != nil {
			return fmt.Errorf("task %s assignee: %w", task.ID, err)
		}
		if err := loadTaskAssignmentProfile(ctx, tx, graph.ExecutionProfiles, agent.ExecutionProfileID); err != nil {
			return fmt.Errorf("task %s assignee: %w", task.ID, err)
		}
		if !core.ValidAgentConfigurationBinding(agent, graph.AgentBlueprints[agent.BlueprintID].Value, graph.ExecutionProfiles[agent.ExecutionProfileID].Value) {
			return fmt.Errorf("task %s assignee Agent has invalid pinned configuration", task.ID)
		}
		if task.AgentConfig != nil {
			if err := loadTaskAssignmentBlueprint(ctx, tx, graph.AgentBlueprints, task.AgentConfig.BlueprintID); err != nil {
				return fmt.Errorf("task %s: %w", task.ID, err)
			}
			if err := loadTaskAssignmentProfile(ctx, tx, graph.ExecutionProfiles, task.AgentConfig.ProfileID); err != nil {
				return fmt.Errorf("task %s: %w", task.ID, err)
			}
		}
	case "TEAM":
		teamRecord, team, teamFound, teamErr := latestProjectionRevision[core.Team](ctx, tx, "team", string(task.AssigneeID))
		if teamErr != nil || !teamFound || team.ID != task.AssigneeID {
			return fmt.Errorf("task %s references invalid assignee team %s", task.ID, task.AssigneeID)
		}
		graph.Teams[team.ID] = core.DurableState[core.Team]{Version: teamRecord.Version, CorrelationID: teamRecord.CorrelationID, Value: team}
	}
	return core.ValidateTaskAssignment(task, intent.OrganizationID, graph)
}

func loadTaskAssignmentBlueprint(ctx context.Context, tx *sql.Tx, target map[core.ID]core.DurableState[core.AgentBlueprint], id core.ID) error {
	record, value, found, err := latestProjectionRevision[core.AgentBlueprint](ctx, tx, "agent_blueprint", string(id))
	if err != nil || !found || value.ID != id {
		return fmt.Errorf("invalid pinned blueprint %s", id)
	}
	target[id] = core.DurableState[core.AgentBlueprint]{Version: record.Version, CorrelationID: record.CorrelationID, Value: value}
	return nil
}

func loadTaskAssignmentProfile(ctx context.Context, tx *sql.Tx, target map[core.ID]core.DurableState[core.ExecutionProfile], id core.ID) error {
	record, value, found, err := latestProjectionRevision[core.ExecutionProfile](ctx, tx, "execution_profile", string(id))
	if err != nil || !found || value.ID != id {
		return fmt.Errorf("invalid pinned execution profile %s", id)
	}
	target[id] = core.DurableState[core.ExecutionProfile]{Version: record.Version, CorrelationID: record.CorrelationID, Value: value}
	return nil
}

func appendPreparedProjection(ctx context.Context, tx *sql.Tx, item preparedProjection) (events.Event, error) {
	if err := validatePreparedProjectionRevision(ctx, tx, item); err != nil {
		return events.Event{}, err
	}
	event, payload, err := appendProjectionEvent(ctx, tx, item.eventDraft, item.record, item.detail)
	if err != nil {
		return events.Event{}, err
	}
	if item.knowledge != nil && (item.knowledge.CreatedAt.After(event.CreatedAt) ||
		(item.knowledge.LastVerifiedAt != nil && item.knowledge.LastVerifiedAt.After(event.CreatedAt))) {
		return events.Event{}, fmt.Errorf("knowledge timestamps postdate their admitting event")
	}
	if item.task != nil && item.draft.Event.EventType == "TASK_VERIFIED_COMPLETE" {
		if err := validateTaskCompletionTransition(ctx, tx, item, event); err != nil {
			return events.Event{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at) VALUES(?,?,?,?,?,?,?)`, item.draft.ProjectionKind, item.draft.RecordID, item.draft.Version, item.body, event.EventID, payload.Admission.Fingerprint, event.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		return events.Event{}, fmt.Errorf("append projection: %w", err)
	}
	if item.intent != nil && item.intent.ExternalRequestID != "" {
		if err := registerExternalWork(ctx, tx, string(item.intent.OrganizationID), item.intent.ExternalRequestID, item.draft.Event.CorrelationID, string(item.intent.ID)); err != nil {
			return events.Event{}, err
		}
	}
	// Only the runtime-owned root is an externally addressable A2A Task.
	// Internal DAG nodes never cross the gateway boundary.
	if item.task != nil && item.task.ParentID == "" {
		registered, err := externalWorkRegistered(ctx, tx, item.draft.Event.OrganizationID, item.draft.Event.CorrelationID)
		if err != nil {
			return events.Event{}, err
		}
		if registered {
			if err := registerExternalTask(ctx, tx, item.draft.Event.OrganizationID, item.draft.RecordID, item.draft.Event.CorrelationID); err != nil {
				return events.Event{}, err
			}
		}
	}
	if item.eventDraft.RecipientScope != "" || item.eventDraft.RecipientID != "" {
		if item.eventDraft.RecipientScope == "" || item.eventDraft.RecipientID == "" {
			return events.Event{}, fmt.Errorf("addressed projection recipient is required")
		}
		if err := projectInbox(ctx, tx, event); err != nil {
			return events.Event{}, err
		}
	}
	return event, nil
}

func validatePreparedProjectionRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	switch item.draft.ProjectionKind {
	case "organization":
		return nil
	case "intent":
		return validateIntentRevision(ctx, tx, item)
	case "mission":
		return validateMissionRevision(ctx, tx, item)
	case "goal":
		return validateGoalRevision(ctx, tx, item)
	case "team":
		return validateTeamRevision(ctx, tx, item)
	case "agent_blueprint":
		return validateAgentBlueprintRevision(ctx, tx, item)
	case "execution_profile":
		return validateExecutionProfileRevision(ctx, tx, item)
	case "agent":
		return validateAgentRevision(ctx, tx, item)
	case "task":
		if err := validateTaskRevision(ctx, tx, item); err != nil {
			return err
		}
		return validateTaskWorkBinding(ctx, tx, item)
	case "work":
		if err := validateWorkRevision(ctx, tx, item); err != nil {
			return err
		}
		return validateWorkIntentBinding(ctx, tx, item)
	case "lab_experiment":
		return validateLabExperimentRevision(ctx, tx, item)
	case "lab_promotion_candidate":
		return validateLabPromotionCandidateRevision(ctx, tx, item)
	case "knowledge":
		return validateKnowledgeRevision(ctx, tx, item)
	default:
		return fmt.Errorf("projection kind has no typed revision validator")
	}
}

func validateTaskCompletionTransition(ctx context.Context, tx *sql.Tx, item preparedProjection, transition events.Event) error {
	current, err := currentProjectionAdmissions[core.Task](ctx, tx, "task", "Task", func(task core.Task) core.ID { return task.ID })
	if err != nil {
		return err
	}
	replaced := false
	for index := range current {
		if current[index].value.ID != item.task.ID {
			continue
		}
		current[index] = currentProjectionAdmission[core.Task]{record: item.record, value: *item.task}
		replaced = true
		break
	}
	if !replaced {
		return fmt.Errorf("completed Task lacks its prior durable projection")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? ORDER BY sequence`, transition.OrganizationID))
	if err != nil {
		return fmt.Errorf("read Task completion event chain: %w", err)
	}
	teamBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='team' ORDER BY r.record_id,r.version`)
	if err != nil {
		return fmt.Errorf("read Task completion Team history: %w", err)
	}
	teamRevisions, err := events.ResolveTeamRevisionBindings(transition.OrganizationID, teamBodies, stream)
	if err != nil {
		return fmt.Errorf("resolve Task completion Team history: %w", err)
	}
	inboxObservations, err := inboxObservationBindings(ctx, tx)
	if err != nil {
		return fmt.Errorf("read Task completion inbox observations: %w", err)
	}
	binding, err := taskCompletionBinding(ctx, tx, transition.OrganizationID, item.record.CorrelationID, item.task.WorkID, transition.Sequence, current, teamRevisions, inboxObservations)
	if err != nil {
		return fmt.Errorf("bind Task completion: %w", err)
	}
	if _, err := events.ValidateTaskCompletionEvidenceChain(binding, events.WorkCompletionTaskBinding{Task: *item.task, Version: item.record.Version, CorrelationID: item.record.CorrelationID}, transition, stream); err != nil {
		return fmt.Errorf("validate Task completion evidence: %w", err)
	}
	return nil
}

func validateAgentRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	agent := *item.agent
	if agent.ID != core.ID(item.draft.RecordID) || string(agent.OrganizationID) != item.draft.Event.OrganizationID || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" || item.draft.Event.RecipientScope != "" || item.draft.Event.RecipientID != "" {
		return fmt.Errorf("agent projection crosses its runtime-owned lifecycle boundary")
	}
	if err := validateRosterParentOrganization(ctx, tx, agent.OrganizationID); err != nil {
		return fmt.Errorf("agent: %w", err)
	}
	if err := validateAgentConfigurationBinding(ctx, tx, agent); err != nil {
		return err
	}
	record, previous, found, err := latestProjectionRevision[core.Agent](ctx, tx, "agent", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		if err := events.ValidateAgentProjectionTransition(item.draft.Event.EventType, item.draft.Version, nil, agent); err != nil {
			return fmt.Errorf("agent creation: %w", err)
		}
		return nil
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("agent version %d follows %d", item.draft.Version, record.Version)
	}
	if err := events.ValidateAgentProjectionTransition(item.draft.Event.EventType, item.draft.Version, &previous, agent); err != nil {
		return fmt.Errorf("agent revision: %w", err)
	}
	return nil
}

func validateIntentRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	intent := *item.intent
	if intent.ID != core.ID(item.draft.RecordID) || string(intent.OrganizationID) != item.draft.Event.OrganizationID || item.draft.Event.CorrelationID == "" {
		return fmt.Errorf("intent projection crosses its durable identity or correlation boundary")
	}
	if err := validateRosterParentOrganization(ctx, tx, intent.OrganizationID); err != nil {
		return fmt.Errorf("intent: %w", err)
	}
	return nil
}

func validateAgentConfigurationBinding(ctx context.Context, tx *sql.Tx, agent core.Agent) error {
	blueprintBody, blueprintFound, err := latestRecordBody(ctx, tx, "agent_blueprint", string(agent.BlueprintID))
	if err != nil {
		return fmt.Errorf("read Agent blueprint binding: %w", err)
	}
	var blueprintRecord events.ProjectionRecord
	var blueprint core.AgentBlueprint
	if !blueprintFound || decodeExactJSONBytes(blueprintBody, &blueprintRecord) != nil || decodeExactJSONBytes(blueprintRecord.Value, &blueprint) != nil || blueprintRecord.ProjectionKind != "agent_blueprint" {
		return fmt.Errorf("agent references an invalid pinned blueprint")
	}
	profileBody, profileFound, err := latestRecordBody(ctx, tx, "execution_profile", string(agent.ExecutionProfileID))
	if err != nil {
		return fmt.Errorf("read Agent execution profile binding: %w", err)
	}
	var profileRecord events.ProjectionRecord
	var profile core.ExecutionProfile
	if !profileFound || decodeExactJSONBytes(profileBody, &profileRecord) != nil || decodeExactJSONBytes(profileRecord.Value, &profile) != nil || profileRecord.ProjectionKind != "execution_profile" || !core.ValidAgentConfigurationBinding(agent, blueprint, profile) {
		return fmt.Errorf("agent references an invalid pinned execution profile")
	}
	return nil
}

func validateTaskRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	task := *item.task
	if task.ID != core.ID(item.draft.RecordID) || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != item.draft.RecordID {
		return fmt.Errorf("task projection crosses its runtime-owned lifecycle boundary")
	}
	record, previous, found, err := latestProjectionRevision[core.Task](ctx, tx, "task", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		if err := events.ValidateTaskProjectionTransition(item.draft.Event.EventType, item.draft.Version, nil, task); err != nil {
			return fmt.Errorf("task creation: %w", err)
		}
		return nil
	}
	if item.draft.Version != record.Version+1 || record.CorrelationID == "" || record.CorrelationID != item.draft.Event.CorrelationID {
		return fmt.Errorf("task revision is noncontiguous or crosses its correlation boundary")
	}
	if err := events.ValidateTaskProjectionTransition(item.draft.Event.EventType, item.draft.Version, &previous, task); err != nil {
		return fmt.Errorf("task revision: %w", err)
	}
	return nil
}

func validateTaskWorkBinding(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	task := *item.task
	body, found, err := latestRecordBody(ctx, tx, "work", string(task.WorkID))
	if err != nil {
		return fmt.Errorf("read task parent Work: %w", err)
	}
	var record events.ProjectionRecord
	var work core.Work
	if !found || decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &work) != nil || record.ProjectionKind != "work" || record.RecordID != string(task.WorkID) || record.CorrelationID != item.draft.Event.CorrelationID || work.ID != task.WorkID || work.Status != core.WorkActive {
		return fmt.Errorf("task requires its exact active Work on the same correlation boundary")
	}
	intentBody, intentFound, err := latestRecordBody(ctx, tx, "intent", string(work.IntentID))
	if err != nil {
		return fmt.Errorf("read task parent Intent: %w", err)
	}
	var intentRecord events.ProjectionRecord
	var intent core.Intent
	if !intentFound || decodeExactJSONBytes(intentBody, &intentRecord) != nil || decodeExactJSONBytes(intentRecord.Value, &intent) != nil || intentRecord.ProjectionKind != "intent" || intentRecord.RecordID != string(work.IntentID) || intentRecord.CorrelationID != item.draft.Event.CorrelationID || intent.ID != work.IntentID || string(intent.OrganizationID) != item.draft.Event.OrganizationID {
		return fmt.Errorf("task requires its exact Intent organization and correlation boundary")
	}
	return nil
}

func validateWorkRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	work := *item.work
	if work.ID != core.ID(item.draft.RecordID) || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" || item.draft.Event.RecipientScope != "" || item.draft.Event.RecipientID != "" {
		return fmt.Errorf("work projection is incomplete or crosses its runtime-owned lifecycle boundary")
	}
	record, previous, found, err := latestProjectionRevision[core.Work](ctx, tx, "work", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		return events.ValidateWorkProjectionTransition(item.draft.Event.EventType, item.draft.Version, nil, work)
	}
	if record.CorrelationID == "" || record.CorrelationID != item.draft.Event.CorrelationID {
		return fmt.Errorf("prior work revision is invalid or crosses its correlation boundary")
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("work version %d follows %d", item.draft.Version, record.Version)
	}
	return events.ValidateWorkProjectionTransition(item.draft.Event.EventType, item.draft.Version, &previous, work)
}

func validateLabExperimentRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	experiment := *item.experiment
	if experiment.ID != core.ID(item.draft.RecordID) || string(experiment.OrganizationID) != item.draft.Event.OrganizationID || item.draft.Event.CorrelationID == "" {
		return fmt.Errorf("lab experiment crosses its durable identity boundary")
	}
	record, previous, found, err := latestProjectionRevision[core.Experiment](ctx, tx, "lab_experiment", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		if err := events.ValidateExperimentProjectionTransition(item.draft.Event.EventType, item.draft.Version, nil, experiment); err != nil {
			return err
		}
	} else {
		if item.draft.Version != record.Version+1 || record.CorrelationID != item.draft.Event.CorrelationID {
			return fmt.Errorf("lab experiment revision is noncontiguous or crosses its correlation boundary")
		}
		if err := events.ValidateExperimentProjectionTransition(item.draft.Event.EventType, item.draft.Version, &previous, experiment); err != nil {
			return err
		}
	}
	workRecord, work, workFound, err := latestProjectionRevision[core.Work](ctx, tx, "work", string(experiment.WorkID))
	if err != nil || !workFound || workRecord.CorrelationID != item.draft.Event.CorrelationID || experiment.Objective != work.Objective {
		return fmt.Errorf("lab experiment requires its exact bounded Work")
	}
	intentRecord, intent, intentFound, err := latestProjectionRevision[core.Intent](ctx, tx, "intent", string(work.IntentID))
	if err != nil || !intentFound || intentRecord.CorrelationID != item.draft.Event.CorrelationID || intent.OrganizationID != experiment.OrganizationID {
		return fmt.Errorf("lab experiment crosses its Work organization boundary")
	}
	switch experiment.Status {
	case core.ExperimentRunning:
		if work.Status != core.WorkActive {
			return fmt.Errorf("running Lab experiment requires active Work")
		}
	case core.ExperimentCompleted:
		if !core.ValidTerminalExperimentWorkStatus(experiment, work) || len(experiment.ResultEventRefs) != 1 {
			return fmt.Errorf("completed Lab experiment requires one exact completed Work transition")
		}
		completion, found, err := eventByID(ctx, tx, experiment.ResultEventRefs[0])
		if err != nil || !found || completion.OrganizationID != item.draft.Event.OrganizationID || completion.CorrelationID != item.draft.Event.CorrelationID || completion.EventType != "WORK_COMPLETED" {
			return fmt.Errorf("completed Lab experiment result is not its exact Work completion")
		}
		payload, admitted, err := events.AdmittedProjection(completion)
		var detail events.WorkCompletionTransitionPayload
		if err != nil || !admitted || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID != string(experiment.WorkID) || decodeExactJSONBytes(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" {
			return fmt.Errorf("completed Lab experiment result lacks evidence-backed Work admission")
		}
		evidence, found, err := eventByID(ctx, tx, detail.EvidenceEventRef)
		if err != nil || !found || evidence.EventType != "WORK_COMPLETION_EVALUATED" || evidence.OrganizationID != item.draft.Event.OrganizationID || evidence.CorrelationID != item.draft.Event.CorrelationID || !slices.Equal(experiment.ArtifactRefs, evidence.ArtifactRefs) {
			return fmt.Errorf("completed Lab experiment artifacts do not match Work evidence")
		}
	case core.ExperimentFailed:
		if !core.ValidTerminalExperimentWorkStatus(experiment, work) {
			return fmt.Errorf("failed Lab experiment conflicts with its Work outcome")
		}
	}
	return nil
}

func validateLabPromotionCandidateRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	candidate := *item.promotionCandidate
	if candidate.ID != core.ID(item.draft.RecordID) || string(candidate.OrganizationID) != item.draft.Event.OrganizationID || item.draft.Event.CorrelationID == "" || events.ValidatePromotionCandidateProjectionTarget(item.draft.Event.EventType, item.draft.Version, candidate) != nil {
		return fmt.Errorf("lab promotion candidate crosses its runtime-owned nomination boundary")
	}
	if _, _, found, err := latestProjectionRevision[core.PromotionCandidate](ctx, tx, "lab_promotion_candidate", item.draft.RecordID); err != nil || found {
		return fmt.Errorf("lab promotion candidate identity is already admitted")
	}
	experimentRecord, experiment, found, err := latestProjectionRevision[core.Experiment](ctx, tx, "lab_experiment", string(candidate.ExperimentID))
	if err != nil || !found || experiment.Status != core.ExperimentCompleted || experiment.OrganizationID != candidate.OrganizationID ||
		experimentRecord.Version != candidate.ExperimentVersion || experimentRecord.CorrelationID != item.draft.Event.CorrelationID ||
		!slices.Equal(experiment.ResultEventRefs, candidate.ExperimentResultEventRefs) || candidate.CreatedAt.Before(*experiment.FinishedAt) {
		return fmt.Errorf("lab promotion candidate lacks its exact completed experiment")
	}
	_, work, found, err := latestProjectionRevision[core.Work](ctx, tx, "work", string(experiment.WorkID))
	if err != nil || !found {
		return fmt.Errorf("lab promotion candidate lacks its experimental Work")
	}
	_, intent, found, err := latestProjectionRevision[core.Intent](ctx, tx, "intent", string(work.IntentID))
	if err != nil || !found || candidate.NominatedBy != intent.SourcePrincipalID {
		return fmt.Errorf("lab promotion candidate does not preserve its commissioning actor")
	}
	for _, ref := range candidate.ReproductionEvidenceRefs {
		reproduction, found, err := eventByID(ctx, tx, ref)
		if err != nil || !found || reproduction.OrganizationID != item.draft.Event.OrganizationID || reproduction.CorrelationID == item.draft.Event.CorrelationID || reproduction.EventType != "WORK_COMPLETED" {
			return fmt.Errorf("lab promotion candidate lacks independent same-organization reproduction evidence")
		}
		payload, admitted, err := events.AdmittedProjection(reproduction)
		if err != nil || !admitted || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID == string(experiment.WorkID) {
			return fmt.Errorf("lab promotion reproduction is not a distinct admitted Work completion")
		}
	}
	return nil
}

func eventByID(ctx context.Context, queryer rowsQueryer, eventID string) (events.Event, bool, error) {
	if eventID == "" {
		return events.Event{}, false, fmt.Errorf("event id is required")
	}
	rows, err := collectEvents(queryer.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE event_id=? LIMIT 2`, eventID))
	if err != nil {
		return events.Event{}, false, err
	}
	if len(rows) == 0 {
		return events.Event{}, false, nil
	}
	if len(rows) != 1 {
		return events.Event{}, false, fmt.Errorf("event id %s is not unique", eventID)
	}
	return rows[0], true, nil
}

func validateMissionRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	mission := *item.mission
	if mission.ID != core.ID(item.draft.RecordID) || string(mission.OrganizationID) != item.draft.Event.OrganizationID || !core.ValidMission(mission) || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" || item.draft.Event.RecipientScope != "" || item.draft.Event.RecipientID != "" {
		return fmt.Errorf("mission projection is incomplete or crosses its runtime-owned lifecycle boundary")
	}
	organizationBody, organizationFound, err := latestRecordBody(ctx, tx, "organization", string(mission.OrganizationID))
	if err != nil {
		return fmt.Errorf("read mission parent organization: %w", err)
	}
	var organizationRecord events.ProjectionRecord
	var organization core.Organization
	if !organizationFound || json.Unmarshal(organizationBody, &organizationRecord) != nil || json.Unmarshal(organizationRecord.Value, &organization) != nil || organizationRecord.ProjectionKind != "organization" || organizationRecord.RecordID != string(mission.OrganizationID) || organization.ID != mission.OrganizationID {
		return fmt.Errorf("mission requires its durable parent organization")
	}
	record, previous, found, err := latestProjectionRevision[core.Mission](ctx, tx, "mission", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		return events.ValidateMissionProjectionTransition(item.draft.Event.EventType, item.draft.Version, nil, mission)
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("mission version %d follows %d", item.draft.Version, record.Version)
	}
	return events.ValidateMissionProjectionTransition(item.draft.Event.EventType, item.draft.Version, &previous, mission)
}

func validateGoalRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	goal := *item.goal
	if goal.ID != core.ID(item.draft.RecordID) || string(goal.OrganizationID) != item.draft.Event.OrganizationID || !core.ValidGoal(goal) || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" || item.draft.Event.RecipientScope != "" || item.draft.Event.RecipientID != "" {
		return fmt.Errorf("goal projection is incomplete or crosses its runtime-owned lifecycle boundary")
	}
	missionBody, missionFound, err := latestRecordBody(ctx, tx, "mission", string(goal.MissionID))
	if err != nil {
		return fmt.Errorf("read goal parent mission: %w", err)
	}
	var missionRecord events.ProjectionRecord
	var mission core.Mission
	if !missionFound || json.Unmarshal(missionBody, &missionRecord) != nil || json.Unmarshal(missionRecord.Value, &mission) != nil || missionRecord.ProjectionKind != "mission" || missionRecord.RecordID != string(goal.MissionID) || mission.ID != goal.MissionID || mission.OrganizationID != goal.OrganizationID || !core.ValidMission(mission) {
		return fmt.Errorf("goal requires a valid parent mission in its organization")
	}
	record, previous, found, err := latestProjectionRevision[core.Goal](ctx, tx, "goal", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		return events.ValidateGoalProjectionTransition(item.draft.Event.EventType, item.draft.Version, nil, goal)
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("goal version %d follows %d", item.draft.Version, record.Version)
	}
	return events.ValidateGoalProjectionTransition(item.draft.Event.EventType, item.draft.Version, &previous, goal)
}

func validateKnowledgeRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	knowledge := *item.knowledge
	if knowledge.KnowledgeID != core.ID(item.draft.RecordID) || string(knowledge.OrganizationID) != item.draft.Event.OrganizationID ||
		item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" ||
		item.draft.Event.RecipientScope != "" || item.draft.Event.RecipientID != "" || item.draft.Event.CorrelationID == "" ||
		!slices.Equal(item.draft.Event.ArtifactRefs, knowledge.EvidenceArtifactRefs) {
		return fmt.Errorf("knowledge projection crosses its runtime-owned identity, tenant, route, or evidence boundary")
	}
	if err := validateRosterParentOrganization(ctx, tx, knowledge.OrganizationID); err != nil {
		return fmt.Errorf("knowledge: %w", err)
	}
	if err := validateKnowledgeScopeBinding(ctx, tx, knowledge); err != nil {
		return err
	}
	for _, refs := range [][]string{knowledge.ProvenanceEventRefs, knowledge.OccurrenceEventRefs, knowledge.ValidationRefs} {
		for _, eventRef := range refs {
			if err := validateKnowledgeEventReference(ctx, tx, knowledge.OrganizationID, eventRef); err != nil {
				return err
			}
		}
	}
	for _, ref := range knowledge.DerivedKnowledgeRefs {
		version, err := strconv.Atoi(ref.Version)
		if err != nil || version < 1 || strconv.Itoa(version) != ref.Version || ref.ID == item.draft.RecordID {
			return fmt.Errorf("knowledge derived reference is invalid")
		}
		body, found, err := recordBodyAtVersion(ctx, tx, "knowledge", ref.ID, version)
		if err != nil {
			return fmt.Errorf("read derived knowledge reference: %w", err)
		}
		var record events.ProjectionRecord
		var derived core.KnowledgeRecord
		if !found || decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &derived) != nil ||
			record.ProjectionKind != "knowledge" || record.RecordID != ref.ID || record.Version != version ||
			derived.OrganizationID != knowledge.OrganizationID || derived.Status != core.KnowledgeActive {
			return fmt.Errorf("derived knowledge must reference an exact active revision in the same organization")
		}
		currentRecord, current, currentFound, err := latestProjectionRevision[core.KnowledgeRecord](ctx, tx, "knowledge", ref.ID)
		if err != nil || !currentFound || currentRecord.Version != version || current.Status != core.KnowledgeActive {
			return fmt.Errorf("derived knowledge must reference the current active revision")
		}
	}
	record, previous, found, err := latestProjectionRevision[core.KnowledgeRecord](ctx, tx, "knowledge", item.draft.RecordID)
	if err != nil {
		return err
	}
	if !found {
		if item.draft.Event.CorrelationID != "knowledge-"+item.draft.RecordID {
			return fmt.Errorf("knowledge proposal correlation is not deterministic")
		}
		return nil
	}
	if record.CorrelationID != item.draft.Event.CorrelationID || item.draft.Version != record.Version+1 {
		return fmt.Errorf("knowledge revision crosses its correlation or version boundary")
	}
	if err := core.ValidateKnowledgeTransition(item.draft.Event.EventType, previous, knowledge); err != nil {
		return fmt.Errorf("knowledge revision: %w", err)
	}
	if item.draft.Event.EventType == "KNOWLEDGE_ACTIVATED" && knowledge.Basis == core.KnowledgeBasisRepeatedPattern &&
		!containsNovelKnowledgeValidationRef(knowledge.OccurrenceEventRefs, knowledge.ValidationRefs) {
		return fmt.Errorf("repeated-pattern activation requires validation evidence beyond the proposal occurrences")
	}
	return nil
}

func validateKnowledgeScopeBinding(ctx context.Context, tx *sql.Tx, knowledge core.KnowledgeRecord) error {
	if knowledge.Scope == core.KnowledgeScopeOrganization {
		return nil
	}
	kind := "agent"
	if knowledge.Scope == core.KnowledgeScopeTeam {
		kind = "team"
	}
	body, found, err := latestRecordBody(ctx, tx, kind, string(knowledge.ScopeID))
	if err != nil {
		return fmt.Errorf("read knowledge scope binding: %w", err)
	}
	if !found {
		return fmt.Errorf("knowledge scope is not a durable %s", kind)
	}
	var record events.ProjectionRecord
	if decodeExactJSONBytes(body, &record) != nil || record.ProjectionKind != kind || record.RecordID != string(knowledge.ScopeID) {
		return fmt.Errorf("knowledge scope projection is invalid")
	}
	var organizationID core.ID
	if kind == "agent" {
		var agent core.Agent
		if decodeExactJSONBytes(record.Value, &agent) != nil || agent.ID != knowledge.ScopeID {
			return fmt.Errorf("knowledge Agent scope projection is invalid")
		}
		organizationID = agent.OrganizationID
	} else {
		var team core.Team
		if decodeExactJSONBytes(record.Value, &team) != nil || team.ID != knowledge.ScopeID {
			return fmt.Errorf("knowledge Team scope projection is invalid")
		}
		organizationID = team.OrganizationID
	}
	if organizationID != knowledge.OrganizationID {
		return fmt.Errorf("knowledge scope crosses its organization boundary")
	}
	return nil
}

func validateKnowledgeEventReference(ctx context.Context, tx *sql.Tx, organizationID core.ID, eventRef string) error {
	var actualOrganization string
	if err := tx.QueryRowContext(ctx, `SELECT organization_id FROM events WHERE event_id=?`, eventRef).Scan(&actualOrganization); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("knowledge references an unavailable event")
		}
		return fmt.Errorf("read knowledge event reference: %w", err)
	}
	if actualOrganization != string(organizationID) {
		return fmt.Errorf("knowledge event reference crosses its organization boundary")
	}
	return nil
}

func containsNovelKnowledgeValidationRef(occurrences, validation []string) bool {
	seen := make(map[string]struct{}, len(occurrences))
	for _, ref := range occurrences {
		seen[ref] = struct{}{}
	}
	for _, ref := range validation {
		if _, repeated := seen[ref]; !repeated {
			return true
		}
	}
	return false
}

func latestProjectionRevision[T any](ctx context.Context, tx *sql.Tx, kind, recordID string) (events.ProjectionRecord, T, bool, error) {
	var value T
	body, found, err := latestRecordBody(ctx, tx, kind, recordID)
	if err != nil {
		return events.ProjectionRecord{}, value, false, fmt.Errorf("read prior %s revision: %w", kind, err)
	}
	if !found {
		return events.ProjectionRecord{}, value, false, nil
	}
	var record events.ProjectionRecord
	if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &value) != nil || record.ProjectionKind != kind || record.RecordID != recordID || record.Version < 1 {
		return events.ProjectionRecord{}, value, false, fmt.Errorf("prior %s revision is invalid", kind)
	}
	return record, value, true, nil
}

func validateTeamRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	team := *item.team
	expectedEventType := "TEAM_REVISED"
	if item.draft.Version == 1 {
		expectedEventType = "TEAM_CREATED"
	}
	if team.ID != core.ID(item.draft.RecordID) || string(team.OrganizationID) != item.draft.Event.OrganizationID || strings.TrimSpace(team.Name) == "" || strings.TrimSpace(team.Status) == "" || item.draft.Event.EventType != expectedEventType || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" {
		return fmt.Errorf("team projection is incomplete or crosses its runtime-owned lifecycle boundary")
	}
	organizationBody, organizationFound, err := latestRecordBody(ctx, tx, "organization", string(team.OrganizationID))
	if err != nil {
		return fmt.Errorf("read Team parent organization: %w", err)
	}
	var organizationRecord events.ProjectionRecord
	var organization core.Organization
	if !organizationFound || json.Unmarshal(organizationBody, &organizationRecord) != nil || json.Unmarshal(organizationRecord.Value, &organization) != nil || organizationRecord.ProjectionKind != "organization" || organizationRecord.RecordID != string(team.OrganizationID) || organization.ID != team.OrganizationID {
		return fmt.Errorf("team requires its durable parent organization")
	}
	members := make(map[core.ID]struct{}, len(team.MemberAgentIDs))
	for _, memberID := range team.MemberAgentIDs {
		if memberID == "" {
			return fmt.Errorf("team projection contains an empty member identity")
		}
		if _, duplicate := members[memberID]; duplicate {
			return fmt.Errorf("team projection contains a duplicate member identity")
		}
		members[memberID] = struct{}{}
		body, found, err := latestRecordBody(ctx, tx, "agent", string(memberID))
		if err != nil {
			return fmt.Errorf("read Team member Agent: %w", err)
		}
		var record events.ProjectionRecord
		var agent core.Agent
		if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &agent) != nil || record.ProjectionKind != "agent" || record.RecordID != string(memberID) || agent.ID != memberID || agent.OrganizationID != team.OrganizationID {
			return fmt.Errorf("team projection references an invalid member Agent")
		}
	}
	body, found, err := latestRecordBody(ctx, tx, "team", item.draft.RecordID)
	if err != nil {
		return fmt.Errorf("read prior Team revision: %w", err)
	}
	if !found {
		if item.draft.Version != 1 {
			return fmt.Errorf("team creation must start at version one")
		}
		return nil
	}
	var record events.ProjectionRecord
	var previous core.Team
	if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &previous) != nil || record.ProjectionKind != "team" || record.RecordID != item.draft.RecordID || record.Version < 1 {
		return fmt.Errorf("prior Team revision is invalid")
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("team version %d follows %d", item.draft.Version, record.Version)
	}
	if !core.ValidTeamRevision(previous, team) {
		return fmt.Errorf("team revision changes immutable identity")
	}
	return nil
}

func validateAgentBlueprintRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	blueprint := *item.blueprint
	expectedEventType := "AGENT_BLUEPRINT_UPDATED"
	if item.draft.Version == 1 {
		expectedEventType = "AGENT_BLUEPRINT_CREATED"
	}
	if blueprint.ID != core.ID(item.draft.RecordID) || string(blueprint.OrganizationID) != item.draft.Event.OrganizationID || !core.ValidAgentBlueprint(blueprint) || item.draft.Event.EventType != expectedEventType || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" {
		return fmt.Errorf("agent blueprint projection is incomplete or crosses its runtime-owned lifecycle boundary")
	}
	if err := validateRosterParentOrganization(ctx, tx, blueprint.OrganizationID); err != nil {
		return fmt.Errorf("agent blueprint: %w", err)
	}
	body, found, err := latestRecordBody(ctx, tx, "agent_blueprint", item.draft.RecordID)
	if err != nil {
		return fmt.Errorf("read prior Agent blueprint revision: %w", err)
	}
	if !found {
		if item.draft.Version != 1 {
			return fmt.Errorf("agent blueprint creation must start at version one")
		}
		return nil
	}
	var record events.ProjectionRecord
	var previous core.AgentBlueprint
	if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &previous) != nil || record.ProjectionKind != "agent_blueprint" || record.RecordID != item.draft.RecordID || record.Version < 1 {
		return fmt.Errorf("prior Agent blueprint revision is invalid")
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("agent blueprint version %d follows %d", item.draft.Version, record.Version)
	}
	blueprint.Status = previous.Status
	if !reflect.DeepEqual(previous, blueprint) {
		return fmt.Errorf("agent blueprint revision changes immutable configuration")
	}
	return nil
}

func validateExecutionProfileRevision(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	profile := *item.profile
	expectedEventType := "EXECUTION_PROFILE_UPDATED"
	if item.draft.Version == 1 {
		expectedEventType = "EXECUTION_PROFILE_CREATED"
	}
	if profile.ID != core.ID(item.draft.RecordID) || string(profile.OrganizationID) != item.draft.Event.OrganizationID || !core.ValidExecutionProfile(profile) || item.draft.Event.EventType != expectedEventType || item.draft.Event.SourceActorID != "runtime" || item.draft.Event.SourceExecutionID != "" || item.draft.Event.TaskID != "" {
		return fmt.Errorf("execution profile projection is incomplete or crosses its runtime-owned lifecycle boundary")
	}
	if err := validateRosterParentOrganization(ctx, tx, profile.OrganizationID); err != nil {
		return fmt.Errorf("execution profile: %w", err)
	}
	body, found, err := latestRecordBody(ctx, tx, "execution_profile", item.draft.RecordID)
	if err != nil {
		return fmt.Errorf("read prior execution profile revision: %w", err)
	}
	if !found {
		if item.draft.Version != 1 {
			return fmt.Errorf("execution profile creation must start at version one")
		}
		return nil
	}
	var record events.ProjectionRecord
	var previous core.ExecutionProfile
	if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &previous) != nil || record.ProjectionKind != "execution_profile" || record.RecordID != item.draft.RecordID || record.Version < 1 {
		return fmt.Errorf("prior execution profile revision is invalid")
	}
	if item.draft.Version != record.Version+1 {
		return fmt.Errorf("execution profile version %d follows %d", item.draft.Version, record.Version)
	}
	profile.Status = previous.Status
	if !reflect.DeepEqual(previous, profile) {
		return fmt.Errorf("execution profile revision changes immutable configuration")
	}
	return nil
}

func validateRosterParentOrganization(ctx context.Context, tx *sql.Tx, organizationID core.ID) error {
	body, found, err := latestRecordBody(ctx, tx, "organization", string(organizationID))
	if err != nil {
		return fmt.Errorf("read parent organization: %w", err)
	}
	var record events.ProjectionRecord
	var organization core.Organization
	if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &organization) != nil || record.ProjectionKind != "organization" || record.RecordID != string(organizationID) || organization.ID != organizationID {
		return fmt.Errorf("durable parent organization is unavailable")
	}
	return nil
}

func validateWorkIntentBinding(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	body, found, err := latestRecordBody(ctx, tx, "intent", string(item.work.IntentID))
	if err != nil {
		return fmt.Errorf("read work intent binding: %w", err)
	}
	if !found {
		return fmt.Errorf("work requires its durable intent")
	}
	var record events.ProjectionRecord
	var intent core.Intent
	if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &intent) != nil || record.RecordID != string(item.work.IntentID) || record.CorrelationID != item.draft.Event.CorrelationID || intent.ID != item.work.IntentID || intent.GoalID != item.work.GoalID || intent.ReplacesWorkID != item.work.ReplacesWorkID || intent.NormalizedObjective != item.work.Objective || string(intent.OrganizationID) != item.draft.Event.OrganizationID {
		return fmt.Errorf("work does not match its accepted intent boundary")
	}
	if intentRequiresConfirmation(intent) {
		if err := validateExternalIntentConfirmation(ctx, tx, item, intent); err != nil {
			return err
		}
	}
	if err := validateWorkReplacementBinding(ctx, tx, item, intent); err != nil {
		return err
	}
	return nil
}

func validateWorkReplacementBinding(ctx context.Context, tx *sql.Tx, item preparedProjection, intent core.Intent) error {
	predecessorID := item.work.ReplacesWorkID
	if predecessorID == "" {
		return nil
	}
	body, found, err := latestRecordBody(ctx, tx, "work", string(predecessorID))
	if err != nil || !found {
		return fmt.Errorf("replacement Work predecessor is unavailable")
	}
	var predecessorRecord events.ProjectionRecord
	var predecessor core.Work
	if decodeExactJSONBytes(body, &predecessorRecord) != nil || decodeExactJSONBytes(predecessorRecord.Value, &predecessor) != nil || predecessor.ID != predecessorID || predecessor.Status != core.WorkFailed || predecessor.GoalID != item.work.GoalID {
		return fmt.Errorf("replacement Work requires a failed predecessor with the same Goal binding")
	}
	intentBody, intentFound, err := latestRecordBody(ctx, tx, "intent", string(predecessor.IntentID))
	if err != nil || !intentFound {
		return fmt.Errorf("replacement Work predecessor lacks its durable Intent")
	}
	var predecessorIntentRecord events.ProjectionRecord
	var predecessorIntent core.Intent
	if decodeExactJSONBytes(intentBody, &predecessorIntentRecord) != nil || decodeExactJSONBytes(predecessorIntentRecord.Value, &predecessorIntent) != nil || predecessorIntent.ID != predecessor.IntentID || predecessorIntent.OrganizationID != intent.OrganizationID || predecessorIntentRecord.CorrelationID != predecessorRecord.CorrelationID {
		return fmt.Errorf("replacement Work predecessor crosses its organization or Intent boundary")
	}
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT record_id) FROM records WHERE kind='work' AND record_id<>? AND json_extract(body,'$.value.replaces_work_id')=?`, item.draft.RecordID, string(predecessorID)).Scan(&existing); err != nil {
		return fmt.Errorf("read replacement Work uniqueness: %w", err)
	}
	if existing != 0 {
		return fmt.Errorf("failed Work already has a durable replacement")
	}
	return nil
}

func intentRequiresConfirmation(intent core.Intent) bool {
	return intent.GoalID != "" || intent.ReplacesWorkID != "" || intent.SourceChannel == "HUMAN_DIRECT" || intent.SourceChannel == "A2A" || intent.SourcePrincipalKind == core.PrincipalHuman || intent.SourcePrincipalKind == core.PrincipalExternalAgent
}

func validateExternalIntentConfirmation(ctx context.Context, tx *sql.Tx, item preparedProjection, intent core.Intent) error {
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE correlation_id=? AND event_type IN ('INTAKE_MESSAGE_RECORDED','INTENT_DRAFTED','INTAKE_ABANDONED','INTENT_CONFIRMED') ORDER BY sequence LIMIT ?`, item.draft.Event.CorrelationID, events.ReviewedIntentEvidenceLimit+1))
	if err != nil {
		return fmt.Errorf("read reviewed intent confirmation: %w", err)
	}
	if len(stream) > events.ReviewedIntentEvidenceLimit {
		return fmt.Errorf("external Work intent review evidence exceeds its admission bound")
	}
	var confirmations []events.Event
	for _, event := range stream {
		if event.EventType == "INTENT_CONFIRMED" {
			confirmations = append(confirmations, event)
		}
	}
	if len(confirmations) != 1 {
		return fmt.Errorf("external Work requires one atomic intent confirmation")
	}
	if confirmations[0].CorrelationID != item.draft.Event.CorrelationID {
		return fmt.Errorf("work intent confirmation crosses its correlation boundary")
	}
	return events.ValidateIntentConfirmation(stream, confirmations[0], intent)
}

func validatePriorActiveWork(ctx context.Context, tx *sql.Tx, item preparedProjection) error {
	body, found, err := latestRecordBody(ctx, tx, "work", item.draft.RecordID)
	if err != nil {
		return fmt.Errorf("read prior work projection: %w", err)
	}
	if !found {
		return fmt.Errorf("completed work requires a prior active projection")
	}
	var record events.ProjectionRecord
	var prior core.Work
	if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &prior) != nil || record.RecordID != item.draft.RecordID || record.Version != item.draft.Version-1 || record.CorrelationID != item.draft.Event.CorrelationID || prior.ID != item.work.ID || prior.IntentID != item.work.IntentID || prior.GoalID != item.work.GoalID || prior.Objective != item.work.Objective || !prior.CreatedAt.Equal(item.work.CreatedAt) || prior.Status != core.WorkActive {
		return fmt.Errorf("completed work does not exactly follow its active projection")
	}
	return nil
}

func validateWorkCompletionEvidence(ctx context.Context, tx *sql.Tx, item preparedProjection, detail events.WorkCompletionTransitionPayload) error {
	row := tx.QueryRowContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE event_id=?`, detail.EvidenceEventRef)
	evidenceEvent, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("work completion evidence event is missing")
		}
		return fmt.Errorf("read work completion evidence: %w", err)
	}
	intentBody, found, err := latestRecordBody(ctx, tx, "intent", string(item.work.IntentID))
	if err != nil {
		return fmt.Errorf("read work completion intent: %w", err)
	}
	if !found {
		return fmt.Errorf("work completion intent projection is missing")
	}
	var intentRecord events.ProjectionRecord
	var intent core.Intent
	if json.Unmarshal(intentBody, &intentRecord) != nil || json.Unmarshal(intentRecord.Value, &intent) != nil || intentRecord.RecordID != string(item.work.IntentID) || intentRecord.CorrelationID != item.draft.Event.CorrelationID || intent.ID != item.work.IntentID {
		return fmt.Errorf("work completion intent projection is invalid")
	}
	taskBodies, err := latestRecordBodies(ctx, tx, "task")
	if err != nil {
		return fmt.Errorf("read work completion tasks: %w", err)
	}
	tasks := make([]events.WorkCompletionTaskBinding, 0)
	blueprints := make(map[core.ID]core.AgentBlueprint)
	profiles := make(map[core.ID]core.ExecutionProfile)
	for _, body := range taskBodies {
		var record events.ProjectionRecord
		var task core.Task
		if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &task) != nil {
			return fmt.Errorf("decode work completion task projection")
		}
		if task.WorkID != item.work.ID {
			continue
		}
		if record.RecordID != string(task.ID) {
			return fmt.Errorf("work completion task projection identity is invalid")
		}
		tasks = append(tasks, events.WorkCompletionTaskBinding{Task: task, Version: record.Version, CorrelationID: record.CorrelationID})
		if task.ExecutionKind != core.ExecutionAgent || task.AgentConfig == nil {
			continue
		}
		blueprintID := task.AgentConfig.BlueprintID
		blueprintBody, blueprintFound, err := latestRecordBody(ctx, tx, "agent_blueprint", string(blueprintID))
		if err != nil {
			return fmt.Errorf("read work completion Agent blueprint: %w", err)
		}
		var blueprintRecord events.ProjectionRecord
		var blueprint core.AgentBlueprint
		if !blueprintFound || json.Unmarshal(blueprintBody, &blueprintRecord) != nil || json.Unmarshal(blueprintRecord.Value, &blueprint) != nil || blueprintRecord.ProjectionKind != "agent_blueprint" || blueprintRecord.RecordID != string(blueprintID) || blueprint.ID != blueprintID || string(blueprint.OrganizationID) != item.draft.Event.OrganizationID || blueprint.Version != task.AgentConfig.BlueprintVersion {
			return fmt.Errorf("work completion Agent blueprint is invalid")
		}
		blueprints[blueprintID] = blueprint
		profileID := task.AgentConfig.ProfileID
		profileBody, profileFound, err := latestRecordBody(ctx, tx, "execution_profile", string(profileID))
		if err != nil {
			return fmt.Errorf("read work completion execution profile: %w", err)
		}
		var profileRecord events.ProjectionRecord
		var profile core.ExecutionProfile
		if !profileFound || json.Unmarshal(profileBody, &profileRecord) != nil || json.Unmarshal(profileRecord.Value, &profile) != nil || profileRecord.ProjectionKind != "execution_profile" || profileRecord.RecordID != string(profileID) || profile.ID != profileID || string(profile.OrganizationID) != item.draft.Event.OrganizationID || profile.Version != task.AgentConfig.ProfileVersion {
			return fmt.Errorf("work completion execution profile is invalid")
		}
		profiles[profileID] = profile
	}
	teamBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='team' ORDER BY r.record_id,r.version`)
	if err != nil {
		return fmt.Errorf("read work completion Teams: %w", err)
	}
	// Execution inputs may include addressed inbox events from another Work
	// correlation. Completion admission therefore receives the complete
	// organization event boundary while every Work claim remains correlation-
	// bound by the event-contract validator.
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? ORDER BY sequence`, item.draft.Event.OrganizationID))
	if err != nil {
		return fmt.Errorf("read work completion event chain: %w", err)
	}
	teamRevisions, err := events.ResolveTeamRevisionBindings(item.draft.Event.OrganizationID, teamBodies, stream)
	if err != nil {
		return fmt.Errorf("resolve work completion Team history: %w", err)
	}
	inboxObservations, err := inboxObservationBindings(ctx, tx)
	if err != nil {
		return fmt.Errorf("read work completion inbox observations: %w", err)
	}
	binding := events.WorkCompletionBinding{
		OrganizationID: item.draft.Event.OrganizationID, CorrelationID: item.draft.Event.CorrelationID,
		Work: *item.work, WorkVersion: item.draft.Version, Intent: intent, Tasks: tasks,
		TeamRevisions: teamRevisions, InboxObservations: inboxObservations, AgentBlueprints: blueprints, ExecutionProfiles: profiles,
	}
	evidence, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, stream)
	if err != nil {
		return err
	}
	if evidence.Fingerprint != detail.Fingerprint {
		return fmt.Errorf("work completion evidence fingerprint does not match the transition")
	}
	return nil
}

func decodeExactJSON(value any, target any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func decodeExactJSONBytes(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func registerExternalWork(ctx context.Context, tx *sql.Tx, organizationID, requestID, correlationID, intentID string) error {
	if organizationID == "" || requestID == "" || correlationID == "" || intentID == "" {
		return fmt.Errorf("complete external work identity is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO external_work(organization_id,request_id,correlation_id,intent_id) VALUES(?,?,?,?)`, organizationID, requestID, correlationID, intentID); err != nil {
		return fmt.Errorf("register external work: %w", err)
	}
	var storedCorrelationID, storedIntentID string
	if err := tx.QueryRowContext(ctx, `SELECT correlation_id,intent_id FROM external_work WHERE organization_id=? AND request_id=?`, organizationID, requestID).Scan(&storedCorrelationID, &storedIntentID); err != nil {
		return fmt.Errorf("verify external work registration: %w", err)
	}
	if storedCorrelationID != correlationID || storedIntentID != intentID {
		return fmt.Errorf("external request is already bound to different work")
	}
	return nil
}

func externalWorkRegistered(ctx context.Context, tx *sql.Tx, organizationID, correlationID string) (bool, error) {
	var registered bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM external_work WHERE organization_id=? AND correlation_id=?)`, organizationID, correlationID).Scan(&registered); err != nil {
		return false, fmt.Errorf("resolve task work binding: %w", err)
	}
	return registered, nil
}

func registerExternalTask(ctx context.Context, tx *sql.Tx, organizationID, taskID, correlationID string) error {
	if organizationID == "" || taskID == "" || correlationID == "" {
		return fmt.Errorf("complete external task identity is required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO external_tasks(organization_id,task_id,correlation_id) VALUES(?,?,?)`, organizationID, taskID, correlationID); err != nil {
		return fmt.Errorf("register external task: %w", err)
	}
	var storedCorrelationID string
	if err := tx.QueryRowContext(ctx, `SELECT correlation_id FROM external_tasks WHERE organization_id=? AND task_id=?`, organizationID, taskID).Scan(&storedCorrelationID); err != nil {
		return fmt.Errorf("verify external task registration: %w", err)
	}
	if storedCorrelationID != correlationID {
		return fmt.Errorf("external task is already bound to different work")
	}
	return nil
}

func (l *SQLite) ResolveExternalWork(ctx context.Context, organizationID, requestID string) (string, bool, error) {
	var correlationID string
	err := l.db.QueryRowContext(ctx, `SELECT correlation_id FROM external_work WHERE organization_id=? AND request_id=?`, organizationID, requestID).Scan(&correlationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return correlationID, err == nil, err
}

func (l *SQLite) ResolveExternalRequest(ctx context.Context, organizationID, correlationID string) (string, bool, error) {
	var requestID string
	err := l.db.QueryRowContext(ctx, `SELECT request_id FROM external_work WHERE organization_id=? AND correlation_id=?`, organizationID, correlationID).Scan(&requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return requestID, err == nil, err
}

func (l *SQLite) ResolveExternalTask(ctx context.Context, organizationID, taskID string) (string, string, bool, error) {
	var requestID, correlationID string
	err := l.db.QueryRowContext(ctx, `SELECT w.request_id,t.correlation_id
FROM external_tasks t JOIN external_work w ON w.organization_id=t.organization_id AND w.correlation_id=t.correlation_id
WHERE t.organization_id=? AND t.task_id=?`, organizationID, taskID).Scan(&requestID, &correlationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return requestID, correlationID, err == nil, err
}

func (l *SQLite) ResolveActiveIntake(ctx context.Context, organizationID, principalID, principalKind, sourceChannel string) (string, string, bool, error) {
	var requestID, correlationID string
	err := l.db.QueryRowContext(ctx, `SELECT w.request_id,e.correlation_id
FROM events e JOIN external_work w ON w.organization_id=e.organization_id AND w.correlation_id=e.correlation_id
WHERE e.organization_id=? AND e.event_type='INTAKE_MESSAGE_RECORDED' AND e.source_actor_id=?
AND json_extract(CAST(e.payload AS TEXT),'$.source_principal_kind')=?
AND json_extract(CAST(e.payload AS TEXT),'$.source_channel')=?
AND NOT EXISTS (
  SELECT 1 FROM events confirmed
  WHERE confirmed.organization_id=e.organization_id
    AND confirmed.correlation_id=e.correlation_id
    AND confirmed.event_type='INTENT_CONFIRMED'
)
AND NOT EXISTS (
  SELECT 1 FROM events abandoned
  WHERE abandoned.organization_id=e.organization_id
    AND abandoned.correlation_id=e.correlation_id
    AND abandoned.event_type='INTAKE_ABANDONED'
)
ORDER BY e.sequence DESC LIMIT 1`, organizationID, principalID, principalKind, sourceChannel).Scan(&requestID, &correlationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return requestID, correlationID, err == nil, err
}

func (l *SQLite) ResolveLatestConfirmedIntake(ctx context.Context, organizationID, principalID, principalKind, sourceChannel string) (string, string, bool, error) {
	var requestID, correlationID string
	err := l.db.QueryRowContext(ctx, `SELECT w.request_id,e.correlation_id
FROM events e JOIN external_work w ON w.organization_id=e.organization_id AND w.correlation_id=e.correlation_id
JOIN events confirmed ON confirmed.organization_id=e.organization_id
  AND confirmed.correlation_id=e.correlation_id AND confirmed.event_type='INTENT_CONFIRMED'
WHERE e.organization_id=? AND e.event_type='INTAKE_MESSAGE_RECORDED' AND e.source_actor_id=?
AND json_extract(CAST(e.payload AS TEXT),'$.source_principal_kind')=?
AND json_extract(CAST(e.payload AS TEXT),'$.source_channel')=?
ORDER BY confirmed.sequence DESC LIMIT 1`, organizationID, principalID, principalKind, sourceChannel).Scan(&requestID, &correlationID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return requestID, correlationID, err == nil, err
}

// ReserveExternalWork returns the durable correlation for one tenant/request.
// New correlations are random and checked against both migrated caller-owned
// streams and prior reservations before they enter the shared ledger namespace.
func (l *SQLite) ReserveExternalWork(ctx context.Context, organizationID, requestID string) (string, error) {
	if organizationID == "" || requestID == "" {
		return "", fmt.Errorf("organization and request are required")
	}
	var correlationID string
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT correlation_id FROM external_work WHERE organization_id=? AND request_id=?`, organizationID, requestID).Scan(&correlationID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("resolve external work reservation: %w", err)
		}
		for range 16 {
			candidate, err := l.newWorkCorrelation()
			if err != nil {
				return err
			}
			intentID := "intent-" + candidate
			var occupied bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
SELECT 1 FROM events WHERE correlation_id=?
UNION ALL SELECT 1 FROM external_work WHERE correlation_id=?
UNION ALL SELECT 1 FROM records WHERE (kind='intent' AND record_id=?) OR (kind='work' AND record_id=?) OR (kind='task' AND record_id=?))`, candidate, candidate, intentID, "work-"+candidate, "task-"+candidate).Scan(&occupied); err != nil {
				return fmt.Errorf("check external work namespace: %w", err)
			}
			if occupied {
				continue
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO external_work(organization_id,request_id,correlation_id,intent_id) VALUES(?,?,?,?)`, organizationID, requestID, candidate, intentID); err != nil {
				return fmt.Errorf("reserve external work: %w", err)
			}
			if err := registerExternalTask(ctx, tx, organizationID, "task-"+candidate, candidate); err != nil {
				return fmt.Errorf("reserve external task identity: %w", err)
			}
			correlationID = candidate
			return nil
		}
		return fmt.Errorf("allocate collision-free external work identity")
	})
	return correlationID, err
}

func randomWorkCorrelation() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate external work identity: %w", err)
	}
	return "w-" + hex.EncodeToString(random[:]), nil
}

// AuthorizeAndAppendEffectAttempt serializes the latest durable approval,
// freeze, and lease state with the action's ATTEMPTED transition. Authorization
// that changes before this transaction blocks the effect; authorization that
// changes afterward orders after the effect has durably begun.
func (l *SQLite) AuthorizeAndAppendEffectAttempt(ctx context.Context, obligation core.EffectObligation, version int, value any) (core.AuthorizationTrace, error) {
	if obligation.OrganizationID == "" || obligation.TaskID == "" || obligation.ActorID == "" || obligation.Action == "" || obligation.Resource == "" || obligation.Scope == "" || len(obligation.AuthorizationRefs) == 0 || obligation.ID == "" || version < 1 {
		return core.AuthorizationTrace{}, fmt.Errorf("effect authority, record identity, and positive version are required")
	}
	requiresApproval, err := approvals.RequiresHumanApproval(obligation.ConsequenceBoundary)
	if err != nil {
		return core.AuthorizationTrace{}, err
	}
	if requiresApproval && obligation.ApprovalRef == "" {
		return core.AuthorizationTrace{}, fmt.Errorf("%w: durable approval is required", approvals.ErrApprovalPending)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return core.AuthorizationTrace{}, fmt.Errorf("encode authorized effect record: %w", err)
	}
	var trace core.AuthorizationTrace
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		var approval core.HumanApproval
		if obligation.ApprovalRef != "" {
			approvalBody, found, err := latestRecordBody(ctx, tx, "approval", obligation.ApprovalRef)
			if err != nil {
				return err
			}
			if !found {
				return approvals.ErrApprovalPending
			}
			if err := json.Unmarshal(approvalBody, &approval); err != nil {
				return fmt.Errorf("decode approval %s: %w", obligation.ApprovalRef, err)
			}
		}
		freezeBody, found, err := latestRecordBody(ctx, tx, "organization_freeze", string(obligation.OrganizationID))
		if err != nil {
			return err
		}
		frozen := false
		if found {
			var state authority.FreezeState
			if err := json.Unmarshal(freezeBody, &state); err != nil {
				return fmt.Errorf("decode organization freeze: %w", err)
			}
			if state.OrganizationID != obligation.OrganizationID {
				return fmt.Errorf("organization freeze identity mismatch")
			}
			frozen = state.Frozen
		}
		leases := make([]core.CapabilityLease, 0, len(obligation.AuthorizationRefs))
		for _, ref := range obligation.AuthorizationRefs {
			leaseBody, found, err := latestRecordBody(ctx, tx, "capability_lease", ref)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			var lease core.CapabilityLease
			if err := json.Unmarshal(leaseBody, &lease); err != nil {
				return fmt.Errorf("decode capability lease %s: %w", ref, err)
			}
			if string(lease.ID) != ref {
				return fmt.Errorf("capability lease identity mismatch for %s", ref)
			}
			leases = append(leases, lease)
		}
		authorizedAt := time.Now().UTC()
		if obligation.ApprovalRef != "" {
			if err := approvals.ValidateForEffect(approval, obligation, authorizedAt); err != nil {
				return err
			}
		}
		trace = authority.Check(authorizedAt, obligation.ActorID, obligation.TaskID, obligation.Action, obligation.Resource, obligation.Scope, leases, frozen)
		traceBody, err := json.Marshal(trace)
		if err != nil {
			return err
		}
		traceVersion, err := nextRecordVersion(ctx, tx, "authorization_trace", string(obligation.ID))
		if err != nil {
			return err
		}
		eventType := "CAPABILITY_DENIED"
		if trace.Allowed {
			eventType = "CAPABILITY_CHECKED"
		}
		traceDraft := events.TrustedDraft{OrganizationID: string(obligation.OrganizationID), EventType: eventType, SourceActorID: string(obligation.ActorID), TaskID: string(obligation.TaskID), AuthorizationRefs: obligation.AuthorizationRefs, Payload: trace}
		if err := appendRecord(ctx, tx, traceDraft, "authorization_trace", string(obligation.ID), traceVersion, traceBody); err != nil {
			return err
		}
		if !trace.Allowed {
			return nil
		}
		if approval.SingleUse {
			draft := events.TrustedDraft{OrganizationID: string(obligation.OrganizationID), EventType: "APPROVAL_CONSUMED", TaskID: string(obligation.TaskID), Payload: map[string]string{"approval_id": obligation.ApprovalRef, "effect_fingerprint": obligation.EffectFingerprint, "effect_obligation_id": string(obligation.ID)}}
			if _, err := appendEvent(ctx, tx, draft); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO consumed_approvals(approval_id,effect_fingerprint,consumed_at) VALUES(?,?,?)`, obligation.ApprovalRef, obligation.EffectFingerprint, authorizedAt.Format(time.RFC3339Nano)); err != nil {
				return fmt.Errorf("consume approval: %w", err)
			}
		}
		effectDraft := events.TrustedDraft{OrganizationID: string(obligation.OrganizationID), EventType: "EFFECT_OBLIGATION_TRANSITIONED", TaskID: string(obligation.TaskID), AuthorizationRefs: obligation.AuthorizationRefs, ArtifactRefs: obligation.ConfirmationEvidenceRefs, Payload: value}
		return appendRecord(ctx, tx, effectDraft, "effect", string(obligation.ID), version, body)
	})
	return trace, err
}

func latestRecordBody(ctx context.Context, tx *sql.Tx, kind, id string) ([]byte, bool, error) {
	if !genericRecordKindAllowed(kind) {
		bodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind=? AND r.record_id=? ORDER BY r.version DESC LIMIT 1`, kind, id)
		if err != nil {
			return nil, false, err
		}
		if len(bodies) == 0 {
			return nil, false, nil
		}
		return bodies[0], true, nil
	}
	var body []byte
	err := tx.QueryRowContext(ctx, `SELECT body FROM records WHERE kind=? AND record_id=? ORDER BY version DESC LIMIT 1`, kind, id).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

func recordBodyAtVersion(ctx context.Context, queryer rowsQueryer, kind, id string, version int) ([]byte, bool, error) {
	if kind == "" || id == "" || version < 1 {
		return nil, false, fmt.Errorf("complete record version identity is required")
	}
	if !genericRecordKindAllowed(kind) {
		bodies, err := admittedProjectionRecordBodies(ctx, queryer, `WHERE r.kind=? AND r.record_id=? AND r.version=?`, kind, id, version)
		if err != nil {
			return nil, false, err
		}
		if len(bodies) == 0 {
			return nil, false, nil
		}
		return bodies[0], true, nil
	}
	rows, err := collectRecordBodies(queryer.QueryContext(ctx, `SELECT body FROM records WHERE kind=? AND record_id=? AND version=?`, kind, id, version))
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	return rows[0], true, nil
}

func nextRecordVersion(ctx context.Context, tx *sql.Tx, kind, id string) (int, error) {
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM records WHERE kind=? AND record_id=?`, kind, id).Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func appendRecord(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft, kind, id string, version int, body []byte) error {
	if _, err := appendEvent(ctx, tx, draft); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,?,?,?)`, kind, id, version, body, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("append record: %w", err)
	}
	if kind == "approval" {
		if err := syncPendingApproval(ctx, tx, id, version, body); err != nil {
			return err
		}
	}
	return nil
}

func syncPendingApproval(ctx context.Context, tx *sql.Tx, id string, version int, body []byte) error {
	var approval core.HumanApproval
	if err := decodeExactJSONBytes(body, &approval); err != nil || approval.ID == "" || string(approval.ID) != id || approval.OrganizationID == "" || version < 1 {
		return fmt.Errorf("pending approval projection identity is invalid")
	}
	switch approval.Status {
	case core.ApprovalPending, core.ApprovalNotified, core.ApprovalAcknowledged, core.ApprovalPendingDecision:
		var expiresAt int64
		if approval.ExpiresAt != nil {
			expiresAt = approval.ExpiresAt.UnixNano()
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO pending_approvals(organization_id,approval_id,record_version,body,expires_at,updated_at)
VALUES(?,?,?,?,?,?)
ON CONFLICT(approval_id) DO UPDATE SET organization_id=excluded.organization_id,record_version=excluded.record_version,body=excluded.body,expires_at=excluded.expires_at,updated_at=excluded.updated_at
WHERE pending_approvals.organization_id=excluded.organization_id AND pending_approvals.record_version<excluded.record_version`, approval.OrganizationID, approval.ID, version, body, expiresAt, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("update pending approval projection: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("pending approval projection version or tenant conflicts with durable record")
		}
	case core.ApprovalApproved, core.ApprovalDenied:
		result, err := tx.ExecContext(ctx, `DELETE FROM pending_approvals WHERE organization_id=? AND approval_id=? AND record_version<?`, approval.OrganizationID, approval.ID, version)
		if err != nil {
			return fmt.Errorf("remove terminal pending approval: %w", err)
		}
		if _, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("inspect terminal pending-approval removal: %w", err)
		}
	default:
		return fmt.Errorf("approval status is invalid for pending projection")
	}
	return nil
}

func (l *SQLite) PendingApprovalRecords(ctx context.Context, organizationID string, now time.Time, limit int) ([][]byte, error) {
	if organizationID == "" || now.IsZero() || limit < 1 || limit > 1001 {
		return nil, fmt.Errorf("organization, current time, and bounded pending-approval limit are required")
	}
	var bodies [][]byte
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM pending_approvals WHERE organization_id=? AND expires_at<>0 AND expires_at<=?`, organizationID, now.UnixNano()); err != nil {
			return fmt.Errorf("purge expired pending approvals: %w", err)
		}
		var err error
		bodies, err = collectRecordBodies(tx.QueryContext(ctx, `SELECT body FROM pending_approvals WHERE organization_id=? ORDER BY approval_id LIMIT ?`, organizationID, limit))
		return err
	})
	return bodies, err
}

func (l *SQLite) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback() // Best-effort cleanup; Commit makes this return sql.ErrTxDone.
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (l *SQLite) Records(ctx context.Context, kind, id string) ([][]byte, error) {
	if !genericRecordKindAllowed(kind) {
		return admittedProjectionRecordBodies(ctx, l.db, `WHERE r.kind=? AND (?='' OR r.record_id=?) ORDER BY r.record_id,r.version`, kind, id, id)
	}
	return collectRecordBodies(l.db.QueryContext(ctx, `SELECT body FROM records WHERE kind=? AND (?='' OR record_id=?) ORDER BY record_id,version`, kind, id, id))
}

// ActiveKnowledgeRecords returns only current, event-admitted knowledge rows
// from one organization. The caller applies the remaining scope and text
// filters, but can never turn this into an unbounded or cross-tenant scan.
func (l *SQLite) ActiveKnowledgeRecords(ctx context.Context, organizationID, scope, scopeID string, limit int) ([][]byte, error) {
	if organizationID == "" || scopeID == "" || limit < 1 || limit > 256 ||
		(scope != string(core.KnowledgeScopeAgent) && scope != string(core.KnowledgeScopeTeam) && scope != string(core.KnowledgeScopeOrganization)) {
		return nil, fmt.Errorf("organization, closed knowledge scope, scope identity, and limit from 1 through 256 are required")
	}
	bodies, err := admittedProjectionRecordBodies(ctx, l.db, `JOIN (
	SELECT record_id, MAX(version) AS version
	FROM records
	WHERE kind='knowledge'
	GROUP BY record_id
) AS latest ON latest.record_id=r.record_id AND latest.version=r.version
WHERE r.kind='knowledge' AND e.organization_id=? AND json_extract(r.body,'$.value.status')=?
AND json_extract(r.body,'$.value.scope')=? AND json_extract(r.body,'$.value.scope_id')=?
ORDER BY r.record_id
LIMIT ?`, organizationID, string(core.KnowledgeActive), scope, scopeID, limit)
	if err != nil {
		return nil, err
	}
	return bodies, nil
}

// LatestRecords returns the current durable version of every record of a kind,
// ordered by record identity. Callers still re-read a specific record before a
// state transition so discovery never becomes a stale write authorization.
func (l *SQLite) LatestRecords(ctx context.Context, kind string) ([][]byte, error) {
	return latestRecordBodies(ctx, l.db, kind)
}

type rowsQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func latestRecordBodies(ctx context.Context, queryer rowsQueryer, kind string) ([][]byte, error) {
	if kind == "" {
		return nil, fmt.Errorf("record kind is required")
	}
	if !genericRecordKindAllowed(kind) {
		return admittedProjectionRecordBodies(ctx, queryer, `JOIN (
	SELECT record_id, MAX(version) AS version
	FROM records
	WHERE kind=?
	GROUP BY record_id
) AS latest ON latest.record_id=r.record_id AND latest.version=r.version
WHERE r.kind=?
ORDER BY r.record_id`, kind, kind)
	}
	return collectRecordBodies(queryer.QueryContext(ctx, `SELECT current.body
FROM records AS current
JOIN (
	SELECT record_id, MAX(version) AS version
	FROM records
	WHERE kind=?
	GROUP BY record_id
) AS latest ON latest.record_id=current.record_id AND latest.version=current.version
WHERE current.kind=?
ORDER BY current.record_id`, kind, kind))
}

func admittedProjectionRecordBodies(ctx context.Context, queryer rowsQueryer, suffix string, args ...any) ([][]byte, error) {
	query := `SELECT r.body,r.kind,r.record_id,r.version,r.admission_event_id,r.admission_fingerprint,
e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM records AS r
LEFT JOIN events AS e ON e.event_id=r.admission_event_id ` + suffix
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var bodies [][]byte
	for rows.Next() {
		var body []byte
		var kind, recordID, admissionEventID, admissionFingerprint string
		var version int
		var event events.Event
		var authorizationRefs, artifactRefs []byte
		var createdAt string
		if err := rows.Scan(&body, &kind, &recordID, &version, &admissionEventID, &admissionFingerprint,
			&event.EventID, &event.Sequence, &event.OrganizationID, &event.EventType, &event.SourceActorID, &event.SourceExecutionID, &event.RecipientScope, &event.RecipientID, &event.TaskID,
			&authorizationRefs, &artifactRefs, &event.Payload, &event.CorrelationID, &createdAt, &event.SchemaVersion); err != nil {
			return nil, err
		}
		if json.Unmarshal(authorizationRefs, &event.AuthorizationRefs) != nil || json.Unmarshal(artifactRefs, &event.ArtifactRefs) != nil {
			return nil, fmt.Errorf("projection admission event references are invalid")
		}
		parsed, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("projection admission event time is invalid")
		}
		event.CreatedAt = parsed
		payload, present, err := events.AdmittedProjection(event)
		var record events.ProjectionRecord
		canonical, canonicalErr := json.Marshal(payload.Projection)
		if err != nil || !present || canonicalErr != nil || !bytes.Equal(body, canonical) || decodeExactJSONBytes(body, &record) != nil || record.ProjectionKind != kind || record.RecordID != recordID || record.Version != version || !reflect.DeepEqual(record, payload.Projection) || admissionEventID != event.EventID || admissionEventID != payload.Admission.EventRef || admissionFingerprint != payload.Admission.Fingerprint {
			return nil, fmt.Errorf("projection record %s/%s/%d lacks its exact event-coupled admission", kind, recordID, version)
		}
		bodies = append(bodies, body)
	}
	return bodies, rows.Err()
}

func collectRecordBodies(rows *sql.Rows, err error) ([][]byte, error) {
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close() // Iteration and close failures are reported by rows.Err.
	}()
	var out [][]byte
	for rows.Next() {
		var body []byte
		if err := rows.Scan(&body); err != nil {
			return nil, err
		}
		out = append(out, body)
	}
	return out, rows.Err()
}

func (l *SQLite) Append(ctx context.Context, d events.TrustedDraft) (events.Event, error) {
	if err := events.ValidateOrdinaryEventPayload(d.Payload); err != nil {
		return events.Event{}, err
	}
	if events.RequiresProjectionAdmission(d.EventType, d.SourceActorID) {
		return events.Event{}, fmt.Errorf("projection lifecycle events require typed admission")
	}
	if d.EventType == "INBOX_EVENTS_OBSERVED" {
		return events.Event{}, fmt.Errorf("inbox observations require atomic inbox admission")
	}
	switch d.EventType {
	case "WORK_COMPLETION_EVALUATED", "WORK_COMPLETED", "GOAL_PROGRESS_EVALUATED", "GOAL_ACHIEVED":
		return events.Event{}, fmt.Errorf("terminal evidence requires its typed admission")
	}
	if d.EventType == "INTENT_CONFIRMED" || d.EventType == "INTAKE_ABANDONED" {
		return events.Event{}, fmt.Errorf("intent terminal event requires typed admission")
	}
	if d.EventType == "MESSAGE" || d.RecipientScope != "" || d.RecipientID != "" {
		return l.appendAddressed(ctx, d)
	}
	var appended events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		appended, err = appendEvent(ctx, tx, d)
		return err
	})
	return appended, err
}

// AppendIntakeAbandonment serializes the terminal transition with the current
// intake stream so it cannot race an Intent confirmation. Exact retries are
// idempotent; a changed request or a confirmed stream fails closed.
func (l *SQLite) AppendIntakeAbandonment(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	var abandonment events.IntakeAbandonedPayload
	if draft.EventType != "INTAKE_ABANDONED" || draft.OrganizationID == "" ||
		decodeExactJSON(draft.Payload, &abandonment) != nil || abandonment.MessageID == "" ||
		abandonment.SourcePrincipalID == "" || abandonment.SourcePrincipalKind == "" || abandonment.SourceChannel == "" ||
		abandonment.SourcePrincipalKind != string(core.PrincipalHuman) || abandonment.SourceChannel != "HUMAN_DIRECT" ||
		!core.ValidIntentSourceIdentity(core.ID(abandonment.SourcePrincipalID), core.PrincipalKind(abandonment.SourcePrincipalKind), abandonment.SourceChannel) ||
		draft.SourceActorID != abandonment.SourcePrincipalID || draft.SourceExecutionID != "" ||
		draft.RecipientScope != "" || draft.RecipientID != "" || draft.TaskID != "task-"+draft.CorrelationID ||
		draft.CorrelationID == "" || len(draft.AuthorizationRefs) != 0 || len(draft.ArtifactRefs) != 0 {
		return events.Event{}, fmt.Errorf("complete intake abandonment is required")
	}
	var appended events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND correlation_id=? AND event_type IN ('INTAKE_MESSAGE_RECORDED','INTAKE_ABANDONED','INTENT_CONFIRMED') ORDER BY sequence LIMIT ?`, draft.OrganizationID, draft.CorrelationID, events.ReviewedIntentEvidenceLimit+1))
		if err != nil {
			return fmt.Errorf("read intake abandonment boundary: %w", err)
		}
		if len(stream) > events.ReviewedIntentEvidenceLimit {
			return fmt.Errorf("intake evidence exceeds its admission bound")
		}
		var initial events.IntakeMessageRecordedPayload
		foundInitial := false
		var existing *events.Event
		for _, candidate := range stream {
			switch candidate.EventType {
			case "INTENT_CONFIRMED":
				return fmt.Errorf("confirmed intent cannot be abandoned")
			case "INTAKE_ABANDONED":
				var recorded events.IntakeAbandonedPayload
				if existing != nil || decodeExactJSONBytes(candidate.Payload, &recorded) != nil || !reflect.DeepEqual(recorded, abandonment) ||
					candidate.OrganizationID != draft.OrganizationID || candidate.SourceActorID != draft.SourceActorID ||
					candidate.SourceExecutionID != "" || candidate.RecipientScope != "" || candidate.RecipientID != "" ||
					candidate.TaskID != draft.TaskID || len(candidate.AuthorizationRefs) != 0 || len(candidate.ArtifactRefs) != 0 ||
					candidate.CorrelationID != draft.CorrelationID || candidate.SchemaVersion != events.SchemaVersion {
					return fmt.Errorf("intake abandonment conflicts with durable state")
				}
				copy := candidate
				existing = &copy
			case "INTAKE_MESSAGE_RECORDED":
				if foundInitial {
					continue
				}
				if decodeExactJSONBytes(candidate.Payload, &initial) != nil || candidate.SourceActorID != initial.SourcePrincipalID ||
					candidate.OrganizationID != draft.OrganizationID || candidate.TaskID != draft.TaskID || candidate.CorrelationID != draft.CorrelationID {
					return fmt.Errorf("intake abandonment source is invalid")
				}
				foundInitial = true
			}
		}
		if !foundInitial || initial.SourcePrincipalID != abandonment.SourcePrincipalID ||
			initial.SourcePrincipalKind != abandonment.SourcePrincipalKind || initial.SourceChannel != abandonment.SourceChannel {
			return fmt.Errorf("intake abandonment is not owned by its original principal")
		}
		if existing != nil {
			appended = *existing
			return nil
		}
		appended, err = appendEvent(ctx, tx, draft)
		return err
	})
	return appended, err
}

func (l *SQLite) AppendIntentConfirmation(ctx context.Context, draft events.TrustedDraft, goalID, replacesWorkID core.ID) (events.Event, error) {
	if draft.EventType != "INTENT_CONFIRMED" || draft.OrganizationID == "" {
		return events.Event{}, fmt.Errorf("complete intent confirmation is required")
	}
	var confirmation events.IntentConfirmedPayload
	if err := decodeExactJSON(draft.Payload, &confirmation); err != nil || confirmation.GoalID != string(goalID) || confirmation.ReplacesWorkID != string(replacesWorkID) || confirmation.IntentID != "intent-"+draft.CorrelationID || confirmation.Version < 1 || confirmation.Fingerprint == "" || confirmation.ConfirmingActorID == "" || confirmation.ConfirmingActorKind == "" || confirmation.SourceChannel == "" || confirmation.MessageID == "" ||
		draft.SourceActorID != confirmation.ConfirmingActorID || draft.SourceExecutionID != "" || draft.RecipientScope != "" || draft.RecipientID != "" || draft.TaskID != "task-"+draft.CorrelationID || draft.CorrelationID == "" || len(draft.AuthorizationRefs) != 0 || len(draft.ArtifactRefs) != 0 {
		return events.Event{}, fmt.Errorf("intent confirmation does not match its reviewed Goal selection")
	}
	var event events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE correlation_id=? AND event_type IN ('INTAKE_MESSAGE_RECORDED','INTENT_DRAFTED','INTAKE_ABANDONED','INTENT_CONFIRMED') ORDER BY sequence LIMIT ?`, draft.CorrelationID, events.ReviewedIntentEvidenceLimit+1))
		if err != nil {
			return fmt.Errorf("read reviewed intent evidence: %w", err)
		}
		if len(stream) > events.ReviewedIntentEvidenceLimit {
			return fmt.Errorf("intent review evidence exceeds its admission bound")
		}
		existing := make([]events.Event, 0, 1)
		for _, candidate := range stream {
			if candidate.EventType == "INTENT_CONFIRMED" {
				existing = append(existing, candidate)
			}
		}
		if len(existing) > 1 {
			return fmt.Errorf("intent has multiple durable confirmations")
		}
		if len(existing) == 1 {
			var recorded events.IntentConfirmedPayload
			candidate := existing[0]
			if decodeExactJSONBytes(candidate.Payload, &recorded) != nil || !reflect.DeepEqual(recorded, confirmation) || candidate.OrganizationID != draft.OrganizationID || candidate.EventType != draft.EventType || candidate.SourceActorID != draft.SourceActorID || candidate.SourceExecutionID != draft.SourceExecutionID || candidate.RecipientScope != draft.RecipientScope || candidate.RecipientID != draft.RecipientID || candidate.TaskID != draft.TaskID || !slices.Equal(candidate.AuthorizationRefs, draft.AuthorizationRefs) || !slices.Equal(candidate.ArtifactRefs, draft.ArtifactRefs) || candidate.CorrelationID != draft.CorrelationID || candidate.SchemaVersion != events.SchemaVersion {
				return fmt.Errorf("intent confirmation conflicts with durable state")
			}
			if goalID == "" {
				if err := events.ValidateReviewedIntentAdmission(stream, candidate); err != nil {
					return err
				}
			} else {
				goal, err := activeGoalAtIntentConfirmation(ctx, tx, draft.OrganizationID, goalID, candidate.Sequence)
				if err != nil {
					return err
				}
				if err := validateActiveMissionAt(ctx, tx, draft.OrganizationID, goal.MissionID, candidate.Sequence); err != nil {
					return fmt.Errorf("intent confirmation lacks its historical active Mission: %w", err)
				}
				if err := events.ValidateReviewedGoalIntentAdmission(stream, candidate, goal); err != nil {
					return err
				}
			}
			if err := validateReplacementWorkAtIntentConfirmation(ctx, tx, draft.OrganizationID, goalID, replacesWorkID, candidate); err != nil {
				return err
			}
			event = candidate
			return nil
		}
		var goal core.Goal
		if goalID != "" {
			body, found, readErr := latestRecordBody(ctx, tx, "goal", string(goalID))
			if readErr != nil {
				return fmt.Errorf("read confirmed intent goal: %w", readErr)
			}
			if !found {
				return fmt.Errorf("confirmed intent goal is unavailable")
			}
			var record events.ProjectionRecord
			if json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &goal) != nil || goal.ID != goalID || string(goal.OrganizationID) != draft.OrganizationID || goal.Status != core.GoalActive {
				return fmt.Errorf("confirmed intent requires an active goal in its organization")
			}
			missionBody, missionFound, readErr := latestRecordBody(ctx, tx, "mission", string(goal.MissionID))
			if readErr != nil {
				return fmt.Errorf("read confirmed intent Mission: %w", readErr)
			}
			var missionRecord events.ProjectionRecord
			var mission core.Mission
			if !missionFound || decodeExactJSONBytes(missionBody, &missionRecord) != nil || decodeExactJSONBytes(missionRecord.Value, &mission) != nil ||
				mission.ID != goal.MissionID || string(mission.OrganizationID) != draft.OrganizationID || mission.Status != core.MissionActive || !core.ValidMission(mission) {
				return fmt.Errorf("confirmed intent requires an active Mission in its organization")
			}
		}
		confirmationBody, err := json.Marshal(draft.Payload)
		if err != nil {
			return fmt.Errorf("encode intent confirmation: %w", err)
		}
		candidate := events.Event{
			OrganizationID: draft.OrganizationID, EventType: draft.EventType, SourceActorID: draft.SourceActorID,
			SourceExecutionID: draft.SourceExecutionID, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID,
			TaskID: draft.TaskID, AuthorizationRefs: draft.AuthorizationRefs, ArtifactRefs: draft.ArtifactRefs,
			Payload: confirmationBody, CorrelationID: draft.CorrelationID, SchemaVersion: events.SchemaVersion,
		}
		if goalID == "" {
			if err := events.ValidateReviewedIntentAdmission(stream, candidate); err != nil {
				return err
			}
		} else if err := events.ValidateReviewedGoalIntentAdmission(stream, candidate, goal); err != nil {
			return err
		}
		if err := validateReplacementWorkAtIntentConfirmation(ctx, tx, draft.OrganizationID, goalID, replacesWorkID, candidate); err != nil {
			return err
		}
		event, err = appendEvent(ctx, tx, draft)
		return err
	})
	return event, err
}

func validateReplacementWorkAtIntentConfirmation(ctx context.Context, tx *sql.Tx, organizationID string, goalID, replacesWorkID core.ID, confirmation events.Event) error {
	if replacesWorkID == "" {
		return nil
	}
	if organizationID == "" || !core.ValidWorkReferenceID(string(replacesWorkID)) {
		return fmt.Errorf("intent replacement Work boundary is invalid")
	}
	var body []byte
	var predecessorSequence int64
	err := tx.QueryRowContext(ctx, `SELECT r.body,e.sequence FROM records r JOIN events e ON e.event_id=r.admission_event_id WHERE r.kind='work' AND r.record_id=? ORDER BY r.version DESC LIMIT 1`, string(replacesWorkID)).Scan(&body, &predecessorSequence)
	if err != nil {
		return fmt.Errorf("confirmed replacement Work is unavailable")
	}
	var record events.ProjectionRecord
	var predecessor core.Work
	if decodeExactJSONBytes(body, &record) != nil || decodeExactJSONBytes(record.Value, &predecessor) != nil || predecessor.ID != replacesWorkID || predecessor.Status != core.WorkFailed || predecessor.GoalID != goalID || record.CorrelationID == "" || confirmation.Sequence > 0 && predecessorSequence >= confirmation.Sequence {
		return fmt.Errorf("confirmed replacement requires a prior failed Work with the same Goal binding")
	}
	intentBody, found, err := latestRecordBody(ctx, tx, "intent", string(predecessor.IntentID))
	if err != nil || !found {
		return fmt.Errorf("confirmed replacement Work lacks its durable Intent")
	}
	var intentRecord events.ProjectionRecord
	var intent core.Intent
	if decodeExactJSONBytes(intentBody, &intentRecord) != nil || decodeExactJSONBytes(intentRecord.Value, &intent) != nil || intent.ID != predecessor.IntentID || intent.OrganizationID != core.ID(organizationID) || intentRecord.CorrelationID != record.CorrelationID {
		return fmt.Errorf("confirmed replacement Work crosses its organization or Intent boundary")
	}
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE organization_id=? AND event_type='INTENT_CONFIRMED' AND correlation_id<>? AND json_extract(payload,'$.replaces_work_id')=?`, organizationID, confirmation.CorrelationID, string(replacesWorkID)).Scan(&existing); err != nil {
		return fmt.Errorf("read replacement Work lineage: %w", err)
	}
	if existing != 0 {
		return fmt.Errorf("failed Work already has a reviewed replacement")
	}
	return nil
}

func activeGoalAtIntentConfirmation(ctx context.Context, tx *sql.Tx, organizationID string, goalID core.ID, confirmationSequence int64) (core.Goal, error) {
	if organizationID == "" || goalID == "" || confirmationSequence < 1 {
		return core.Goal{}, fmt.Errorf("intent confirmation Goal boundary is invalid")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND sequence<? AND json_extract(payload,'$.projection.projection_kind')='goal' AND json_extract(payload,'$.projection.record_id')=? ORDER BY sequence DESC LIMIT 1`, organizationID, confirmationSequence, string(goalID)))
	if err != nil || len(stream) != 1 {
		return core.Goal{}, fmt.Errorf("intent confirmation lacks its historical active Goal")
	}
	payload, present, err := events.AdmittedProjection(stream[0])
	if err != nil || !present {
		return core.Goal{}, fmt.Errorf("intent confirmation has invalid historical Goal evidence")
	}
	var goal core.Goal
	if decodeExactJSONBytes(payload.Projection.Value, &goal) != nil || payload.Projection.ProjectionKind != "goal" || payload.Projection.RecordID != string(goalID) || goal.ID != goalID || string(goal.OrganizationID) != organizationID || goal.Status != core.GoalActive || !core.ValidGoal(goal) {
		return core.Goal{}, fmt.Errorf("intent confirmation Goal was not active at admission")
	}
	return goal, nil
}

func (l *SQLite) appendAddressed(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	if draft.RecipientScope == "" || draft.RecipientID == "" {
		return events.Event{}, fmt.Errorf("addressed event recipient is required")
	}
	return l.appendWithProjection(ctx, draft, func(tx *sql.Tx, event events.Event) error {
		return projectInbox(ctx, tx, event)
	})
}

func projectInbox(ctx context.Context, tx *sql.Tx, event events.Event) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO inbox(recipient_scope,recipient_id,event_id,organization_id,task_id,available_at) VALUES(?,?,?,?,?,?)`, event.RecipientScope, event.RecipientID, event.EventID, event.OrganizationID, event.TaskID, event.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("project addressed event to inbox: %w", err)
	}
	return nil
}

func (l *SQLite) ObserveInbox(ctx context.Context, draft events.TrustedDraft, recipientScope, recipientID string, eventIDs []string) (events.Event, error) {
	var payload struct {
		EventIDs []string `json:"event_ids"`
	}
	if draft.EventType != "INBOX_EVENTS_OBSERVED" || draft.OrganizationID == "" || draft.SourceActorID == "" || draft.SourceExecutionID == "" || draft.TaskID == "" || draft.CorrelationID == "" || draft.RecipientScope != recipientScope || draft.RecipientID != recipientID || len(eventIDs) == 0 || decodeExactJSON(draft.Payload, &payload) != nil || !slices.Equal(payload.EventIDs, eventIDs) {
		return events.Event{}, fmt.Errorf("complete observation identity and matching recipient events are required")
	}
	distinct := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		if eventID == "" {
			return events.Event{}, fmt.Errorf("observation event ids must be non-empty")
		}
		distinct[eventID] = struct{}{}
	}
	if len(distinct) != len(eventIDs) {
		return events.Event{}, fmt.Errorf("observation event ids must be distinct")
	}
	var observation events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		startEvent, err := resolveInboxObservationExecution(ctx, tx, draft, recipientScope, recipientID)
		if err != nil {
			return err
		}
		persisted := draft
		persisted.Payload = events.InboxEventsObservedPayload{EventIDs: append([]string(nil), eventIDs...), ExecutionStartEventRef: startEvent.EventID}
		observation, err = appendEvent(ctx, tx, persisted)
		if err != nil {
			return err
		}
		now := observation.CreatedAt.Format(time.RFC3339Nano)
		var previousSequence int64
		for _, eventID := range eventIDs {
			var eventSequence int64
			if err := tx.QueryRowContext(ctx, `SELECT addressed.sequence FROM inbox AS i JOIN events AS addressed ON addressed.event_id=i.event_id AND addressed.organization_id=i.organization_id AND addressed.recipient_scope=i.recipient_scope AND addressed.recipient_id=i.recipient_id WHERE i.organization_id=? AND i.recipient_scope=? AND i.recipient_id=? AND i.event_id=? AND i.observed_at=''`, draft.OrganizationID, recipientScope, recipientID, eventID).Scan(&eventSequence); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("inbox event %s is not available to recipient", eventID)
				}
				return fmt.Errorf("read inbox event %s: %w", eventID, err)
			}
			if eventSequence >= startEvent.Sequence || previousSequence > 0 && eventSequence <= previousSequence {
				return fmt.Errorf("inbox event %s is outside the execution's ordered input boundary", eventID)
			}
			previousSequence = eventSequence
			result, err := tx.ExecContext(ctx, `UPDATE inbox SET observed_at=?,observation_event_id=? WHERE organization_id=? AND recipient_scope=? AND recipient_id=? AND event_id=? AND observed_at=''`, now, observation.EventID, draft.OrganizationID, recipientScope, recipientID, eventID)
			if err != nil {
				return fmt.Errorf("observe inbox event %s: %w", eventID, err)
			}
			changed, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if changed != 1 {
				return fmt.Errorf("inbox event %s is not available to recipient", eventID)
			}
		}
		return nil
	})
	return observation, err
}

func resolveInboxObservationExecution(ctx context.Context, tx *sql.Tx, draft events.TrustedDraft, recipientScope, recipientID string) (events.Event, error) {
	body, found, err := latestRecordBody(ctx, tx, "task", draft.TaskID)
	if err != nil {
		return events.Event{}, fmt.Errorf("read inbox observation task: %w", err)
	}
	var record events.ProjectionRecord
	var task core.Task
	if !found || json.Unmarshal(body, &record) != nil || json.Unmarshal(record.Value, &task) != nil || record.ProjectionKind != "task" || record.RecordID != draft.TaskID || record.Version < 1 || record.CorrelationID != draft.CorrelationID || task.ID != core.ID(draft.TaskID) || task.ExecutionKind != core.ExecutionAgent || task.Status != core.TaskRunning || task.AssigneeID == "" || draft.SourceActorID != string(task.AssigneeID) || draft.SourceExecutionID != fmt.Sprintf("execution-%s-v%d", task.ID, record.Version) {
		return events.Event{}, fmt.Errorf("inbox observation is not bound to a running Agent execution")
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE organization_id=? ORDER BY sequence`, draft.OrganizationID))
	if err != nil {
		return events.Event{}, fmt.Errorf("read inbox observation execution boundary: %w", err)
	}
	var startEvent events.Event
	for _, event := range stream {
		if event.EventType != "EXECUTION_STARTED" || event.OrganizationID != draft.OrganizationID || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != draft.TaskID || event.CorrelationID != draft.CorrelationID {
			continue
		}
		payload, present, admissionErr := events.AdmittedProjection(event)
		if admissionErr != nil {
			return events.Event{}, admissionErr
		}
		if !present || !reflect.DeepEqual(payload.Projection, record) {
			continue
		}
		if startEvent.EventID != "" {
			return events.Event{}, fmt.Errorf("running Agent execution has multiple start transitions")
		}
		startEvent = event
	}
	if startEvent.EventID == "" {
		return events.Event{}, fmt.Errorf("running Agent execution lacks its exact start transition")
	}
	if err := events.ValidateAgentDispatchStart(startEvent, task, record.Version, stream); err != nil {
		return events.Event{}, fmt.Errorf("running Agent execution lacks valid dispatch admission: %w", err)
	}
	var teamRevisions map[core.ID][]events.TeamRevisionBinding
	if recipientScope == events.RecipientTeam {
		teamBodies, err := admittedProjectionRecordBodies(ctx, tx, `WHERE r.kind='team' ORDER BY r.record_id,r.version`)
		if err != nil {
			return events.Event{}, fmt.Errorf("read inbox observation Team history: %w", err)
		}
		teamRevisions, err = events.ResolveTeamRevisionBindings(draft.OrganizationID, teamBodies, stream)
		if err != nil {
			return events.Event{}, fmt.Errorf("resolve inbox observation Team history: %w", err)
		}
	}
	if !events.ExecutionRecipientAllowed(teamRevisions, task, startEvent.Sequence, recipientScope, recipientID) {
		return events.Event{}, fmt.Errorf("inbox observation recipient is outside its running Agent execution")
	}
	return startEvent, nil
}

// appendWithProjection commits the authoritative event before its derived
// availability/state rows inside the same transaction. Any projection failure
// rolls the event back as well.
func (l *SQLite) appendWithProjection(ctx context.Context, draft events.TrustedDraft, project func(*sql.Tx, events.Event) error) (events.Event, error) {
	var event events.Event
	err := l.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		event, err = appendEvent(ctx, tx, draft)
		if err != nil {
			return err
		}
		return project(tx, event)
	})
	return event, err
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func appendEvent(ctx context.Context, db sqlExecutor, d events.TrustedDraft) (events.Event, error) {
	id, err := newEventID()
	if err != nil {
		return events.Event{}, err
	}
	return appendEventWithID(ctx, db, d, id)
}

func newEventID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("generate event id: %w", err)
	}
	return "evt-" + hex.EncodeToString(random[:]), nil
}

func appendEventWithID(ctx context.Context, db sqlExecutor, d events.TrustedDraft, id string) (events.Event, error) {
	if id == "" {
		return events.Event{}, fmt.Errorf("event id is required")
	}
	if err := events.ValidateOrdinaryEventPayload(d.Payload); err != nil {
		return events.Event{}, err
	}
	data, err := json.Marshal(d.Payload)
	if err != nil {
		return events.Event{}, fmt.Errorf("encode event: %w", err)
	}
	return insertEvent(ctx, db, d, id, data, time.Now().UTC(), true)
}

func appendProjectionEvent(ctx context.Context, db sqlExecutor, draft events.TrustedDraft, record events.ProjectionRecord, detail json.RawMessage) (events.Event, events.ProjectionEventPayload, error) {
	id, err := newEventID()
	if err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, err
	}
	event, err := insertEvent(ctx, db, draft, id, []byte(`{}`), time.Now().UTC(), false)
	if err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, err
	}
	payload, err := events.SealProjectionEvent(event, record, detail)
	if err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, fmt.Errorf("encode projection event: %w", err)
	}
	event.Payload = data
	admitted, present, err := events.AdmittedProjection(event)
	if err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, fmt.Errorf("validate sealed projection event: %w", err)
	}
	if !present || !reflect.DeepEqual(admitted, payload) {
		return events.Event{}, events.ProjectionEventPayload{}, fmt.Errorf("sealed projection event changed during serialization")
	}
	if err := events.ValidateProjectionEventBoundary(event, admitted); err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, fmt.Errorf("validate sealed projection boundary: %w", err)
	}
	result, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=? AND sequence=?`, data, event.EventID, event.Sequence)
	if err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, fmt.Errorf("seal projection event: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return events.Event{}, events.ProjectionEventPayload{}, fmt.Errorf("seal projection event boundary")
	}
	if err := sealEventIntegrity(ctx, db, event.Sequence); err != nil {
		return events.Event{}, events.ProjectionEventPayload{}, err
	}
	return event, payload, nil
}

func insertEvent(ctx context.Context, db sqlExecutor, d events.TrustedDraft, id string, data []byte, now time.Time, sealIntegrity bool) (events.Event, error) {
	auth, _ := json.Marshal(d.AuthorizationRefs)
	artifacts, _ := json.Marshal(d.ArtifactRefs)
	r, err := db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, d.OrganizationID, d.EventType, d.SourceActorID, d.SourceExecutionID, d.RecipientScope, d.RecipientID, d.TaskID, auth, artifacts, data, d.CorrelationID, now.Format(time.RFC3339Nano), events.SchemaVersion)
	if err != nil {
		return events.Event{}, fmt.Errorf("append event: %w", err)
	}
	seq, err := r.LastInsertId()
	if err != nil {
		return events.Event{}, err
	}
	event := events.Event{EventID: id, Sequence: seq, OrganizationID: d.OrganizationID, EventType: d.EventType, SourceActorID: d.SourceActorID, SourceExecutionID: d.SourceExecutionID, RecipientScope: d.RecipientScope, RecipientID: d.RecipientID, TaskID: d.TaskID, AuthorizationRefs: d.AuthorizationRefs, ArtifactRefs: d.ArtifactRefs, CreatedAt: now, SchemaVersion: events.SchemaVersion, Payload: data, CorrelationID: d.CorrelationID}
	if err := syncPendingCompletionReview(ctx, db, event); err != nil {
		return events.Event{}, err
	}
	if sealIntegrity {
		if err := sealEventIntegrity(ctx, db, event.Sequence); err != nil {
			return events.Event{}, err
		}
	}
	return event, nil
}

func syncPendingCompletionReview(ctx context.Context, db sqlExecutor, event events.Event) error {
	switch event.EventType {
	case "COMPLETION_REVIEW_REQUESTED":
		if event.OrganizationID == "" || event.TaskID == "" || event.CorrelationID == "" || event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() {
			return fmt.Errorf("pending completion-review projection identity is invalid")
		}
		result, err := db.ExecContext(ctx, `INSERT INTO pending_completion_reviews(organization_id,task_id,correlation_id,request_event_id,request_sequence,updated_at)
VALUES(?,?,?,?,?,?)
ON CONFLICT(organization_id,task_id,correlation_id) DO UPDATE SET request_event_id=excluded.request_event_id,request_sequence=excluded.request_sequence,updated_at=excluded.updated_at
WHERE pending_completion_reviews.request_sequence<excluded.request_sequence`, event.OrganizationID, event.TaskID, event.CorrelationID, event.EventID, event.Sequence, event.CreatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("update pending completion-review projection: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("pending completion-review projection conflicts with durable event sequence")
		}
	case "COMPLETION_REVIEW_DECIDED":
		if event.OrganizationID == "" || event.TaskID == "" || event.CorrelationID == "" || event.Sequence < 1 {
			return fmt.Errorf("terminal completion-review projection identity is invalid")
		}
		result, err := db.ExecContext(ctx, `DELETE FROM pending_completion_reviews WHERE organization_id=? AND task_id=? AND correlation_id=? AND request_sequence<?`, event.OrganizationID, event.TaskID, event.CorrelationID, event.Sequence)
		if err != nil {
			return fmt.Errorf("remove terminal pending completion review: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("terminal completion review lacks its pending durable request")
		}
	case "TASK_VERIFIED_COMPLETE", "COMPLETION_REJECTED", "TASK_DEPENDENCY_FAILED", "TASK_REMEDIATION_FAILED", "TASK_WORK_FAILED":
		if event.OrganizationID == "" || event.TaskID == "" || event.CorrelationID == "" || event.Sequence < 1 {
			return fmt.Errorf("terminal task projection identity is invalid")
		}
		if _, err := db.ExecContext(ctx, `DELETE FROM pending_completion_reviews WHERE organization_id=? AND task_id=? AND correlation_id=? AND request_sequence<?`, event.OrganizationID, event.TaskID, event.CorrelationID, event.Sequence); err != nil {
			return fmt.Errorf("remove terminal task's pending completion review: %w", err)
		}
	}
	return nil
}
func (l *SQLite) Events(ctx context.Context, correlationID string) ([]events.Event, error) {
	return collectEvents(l.db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE (?='' OR correlation_id=?) ORDER BY sequence`, correlationID, correlationID))
}

// VerifiedReplayEvents returns a bounded tenant/correlation slice and the
// verified complete-ledger head from one read transaction. The transaction is
// never promoted to a writer and the selected events are not republished.
func (l *SQLite) VerifiedReplayEvents(ctx context.Context, organizationID, correlationID string, limit int) (events.VerifiedEventSnapshot, error) {
	if organizationID == "" || correlationID == "" || limit < 1 || limit > 256 {
		return events.VerifiedEventSnapshot{}, fmt.Errorf("verified replay requires organization, correlation, and a limit from 1 to 256")
	}
	tx, err := l.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return events.VerifiedEventSnapshot{}, fmt.Errorf("begin verified replay snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	head, err := ValidateEventIntegrity(ctx, tx)
	if err != nil {
		return events.VerifiedEventSnapshot{}, fmt.Errorf("verify replay ledger snapshot: %w", err)
	}
	stream, err := collectEvents(tx.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND correlation_id=? ORDER BY sequence LIMIT ?`, organizationID, correlationID, limit+1))
	if err != nil {
		return events.VerifiedEventSnapshot{}, fmt.Errorf("read verified replay stream: %w", err)
	}
	if len(stream) > limit {
		return events.VerifiedEventSnapshot{}, fmt.Errorf("verified replay stream exceeds its %d event limit", limit)
	}
	if err := tx.Commit(); err != nil {
		return events.VerifiedEventSnapshot{}, fmt.Errorf("finish verified replay snapshot: %w", err)
	}
	return events.VerifiedEventSnapshot{
		OrganizationID: organizationID,
		CorrelationID:  correlationID,
		Algorithm:      head.Algorithm,
		LedgerEvents:   head.EventCount,
		LedgerSequence: head.Sequence,
		LedgerEventID:  head.EventID,
		LedgerSHA256:   head.SHA256,
		Events:         stream,
	}, nil
}

func (l *SQLite) StrategyCreationEvents(ctx context.Context, organizationID, correlationID string) ([]events.Event, error) {
	if organizationID == "" || correlationID == "" {
		return nil, fmt.Errorf("strategy creation organization and correlation are required")
	}
	return collectEvents(l.db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events
WHERE organization_id=? AND correlation_id=? AND event_type IN ('ORGANIZATION_CREATED','MISSION_CREATED','GOAL_CREATED')
ORDER BY sequence LIMIT 4`, organizationID, correlationID))
}

func (l *SQLite) RecentEvents(ctx context.Context, organizationID, eventType string, limit int) ([]events.Event, error) {
	if organizationID == "" || eventType == "" || limit < 1 || limit > 100 {
		return nil, fmt.Errorf("organization, event type, and bounded limit are required")
	}
	return collectEvents(l.db.QueryContext(ctx, `SELECT event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version
FROM events WHERE organization_id=? AND event_type=? ORDER BY sequence DESC LIMIT ?`, organizationID, eventType, limit))
}

func (l *SQLite) PendingCompletionReviewEvents(ctx context.Context, organizationID, afterEventID string, limit int) ([]events.Event, error) {
	if organizationID == "" || limit < 1 || limit > 101 {
		return nil, fmt.Errorf("organization and bounded completion-review limit are required")
	}
	cursorSequence := int64(^uint64(0) >> 1)
	if afterEventID != "" {
		if err := l.db.QueryRowContext(ctx, `SELECT sequence FROM events WHERE organization_id=? AND event_type='COMPLETION_REVIEW_REQUESTED' AND event_id=?`, organizationID, afterEventID).Scan(&cursorSequence); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("completion-review cursor is outside the requested organization")
			}
			return nil, fmt.Errorf("resolve completion-review cursor: %w", err)
		}
	}
	return collectEvents(l.db.QueryContext(ctx, `SELECT request.event_id,request.sequence,request.organization_id,request.event_type,request.source_actor_id,request.source_execution_id,request.recipient_scope,request.recipient_id,request.task_id,request.authorization_refs,request.artifact_refs,request.payload,request.correlation_id,request.created_at,request.schema_version
FROM pending_completion_reviews AS pending
JOIN events AS request ON request.event_id=pending.request_event_id
  AND request.sequence=pending.request_sequence
  AND request.organization_id=pending.organization_id
  AND request.task_id=pending.task_id
  AND request.correlation_id=pending.correlation_id
  AND request.event_type='COMPLETION_REVIEW_REQUESTED'
WHERE pending.organization_id=? AND pending.request_sequence<?
ORDER BY pending.request_sequence DESC LIMIT ?`, organizationID, cursorSequence, limit))
}

func (l *SQLite) Inbox(ctx context.Context, recipientScope, recipientID string) ([]events.Event, error) {
	return collectEvents(l.db.QueryContext(ctx, `SELECT e.event_id,e.sequence,e.organization_id,e.event_type,e.source_actor_id,e.source_execution_id,e.recipient_scope,e.recipient_id,e.task_id,e.authorization_refs,e.artifact_refs,e.payload,e.correlation_id,e.created_at,e.schema_version
FROM inbox i JOIN events e ON e.event_id=i.event_id
WHERE i.recipient_scope=? AND i.recipient_id=? AND i.observed_at=''
ORDER BY e.sequence`, recipientScope, recipientID))
}

func (l *SQLite) InboxObservations(ctx context.Context) (map[string]events.InboxObservationBinding, error) {
	return inboxObservationBindings(ctx, l.db)
}

func inboxObservationBindings(ctx context.Context, queryer rowsQueryer) (map[string]events.InboxObservationBinding, error) {
	rows, err := queryer.QueryContext(ctx, `SELECT i.observation_event_id,observation.payload,i.event_id
FROM inbox AS i
JOIN events AS addressed ON addressed.event_id=i.event_id
  AND addressed.organization_id=i.organization_id
  AND addressed.recipient_scope=i.recipient_scope
  AND addressed.recipient_id=i.recipient_id
JOIN events AS observation ON observation.event_id=i.observation_event_id
  AND observation.event_type='INBOX_EVENTS_OBSERVED'
  AND observation.organization_id=i.organization_id
  AND observation.recipient_scope=i.recipient_scope
  AND observation.recipient_id=i.recipient_id
WHERE i.observation_event_id<>''
ORDER BY i.observation_event_id,addressed.sequence`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	bindings := make(map[string]events.InboxObservationBinding)
	payloads := make(map[string]events.InboxEventsObservedPayload)
	for rows.Next() {
		var observationEventID, eventID string
		var body []byte
		if err := rows.Scan(&observationEventID, &body, &eventID); err != nil {
			return nil, err
		}
		var payload events.InboxEventsObservedPayload
		if err := decodeExactJSONBytes(body, &payload); err != nil || payload.ExecutionStartEventRef == "" || len(payload.EventIDs) == 0 {
			return nil, fmt.Errorf("inbox observation payload is invalid")
		}
		if prior, exists := payloads[observationEventID]; exists && !reflect.DeepEqual(prior, payload) {
			return nil, fmt.Errorf("inbox observation payload is inconsistent")
		}
		payloads[observationEventID] = payload
		binding := bindings[observationEventID]
		if binding.ExecutionStartEventRef != "" && binding.ExecutionStartEventRef != payload.ExecutionStartEventRef {
			return nil, fmt.Errorf("inbox observation execution binding is inconsistent")
		}
		binding.ExecutionStartEventRef = payload.ExecutionStartEventRef
		binding.EventIDs = append(binding.EventIDs, eventID)
		bindings[observationEventID] = binding
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for observationEventID, binding := range bindings {
		payload := payloads[observationEventID]
		if payload.ExecutionStartEventRef != binding.ExecutionStartEventRef || !slices.Equal(payload.EventIDs, binding.EventIDs) {
			return nil, fmt.Errorf("inbox observation does not match its projected recipient events")
		}
	}
	return bindings, nil
}

func collectEvents(rows *sql.Rows, err error) ([]events.Event, error) {
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close() // Iteration and close failures are reported by rows.Err.
	}()
	var out []events.Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

type rowScanner interface {
	Scan(...any) error
}

func scanEvent(row rowScanner) (events.Event, error) {
	var event events.Event
	var createdAt string
	var authorizationRefs, artifactRefs []byte
	if err := row.Scan(&event.EventID, &event.Sequence, &event.OrganizationID, &event.EventType, &event.SourceActorID, &event.SourceExecutionID, &event.RecipientScope, &event.RecipientID, &event.TaskID, &authorizationRefs, &artifactRefs, &event.Payload, &event.CorrelationID, &createdAt, &event.SchemaVersion); err != nil {
		return events.Event{}, err
	}
	if err := json.Unmarshal(authorizationRefs, &event.AuthorizationRefs); err != nil {
		return events.Event{}, err
	}
	if err := json.Unmarshal(artifactRefs, &event.ArtifactRefs); err != nil {
		return events.Event{}, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return events.Event{}, err
	}
	event.CreatedAt = parsed
	return event, nil
}
