// Package projections materializes durable organizational and work state from
// versioned Event Contracts. It contains no scheduling or execution policy.
package projections

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	KindOrganization          = "organization"
	KindMission               = "mission"
	KindGoal                  = "goal"
	KindTeam                  = "team"
	KindAgentBlueprint        = "agent_blueprint"
	KindExecutionProfile      = "execution_profile"
	KindAgent                 = "agent"
	KindIntent                = "intent"
	KindWork                  = "work"
	KindTask                  = "task"
	KindLabExperiment         = "lab_experiment"
	KindLabPromotionCandidate = "lab_promotion_candidate"
)

var projectionKinds = [...]string{
	KindOrganization,
	KindMission,
	KindGoal,
	KindTeam,
	KindAgentBlueprint,
	KindExecutionProfile,
	KindAgent,
	KindIntent,
	KindWork,
	KindTask,
	KindLabExperiment,
	KindLabPromotionCandidate,
}

type Versioned[T any] = core.DurableState[T]

type Snapshot struct {
	Organizations       map[core.ID]Versioned[core.Organization]
	Missions            map[core.ID]Versioned[core.Mission]
	Goals               map[core.ID]Versioned[core.Goal]
	Teams               map[core.ID]Versioned[core.Team]
	AgentBlueprints     map[core.ID]Versioned[core.AgentBlueprint]
	ExecutionProfiles   map[core.ID]Versioned[core.ExecutionProfile]
	Agents              map[core.ID]Versioned[core.Agent]
	Intents             map[core.ID]Versioned[core.Intent]
	Works               map[core.ID]Versioned[core.Work]
	Tasks               map[core.ID]Versioned[core.Task]
	Experiments         map[core.ID]Versioned[core.Experiment]
	PromotionCandidates map[core.ID]Versioned[core.PromotionCandidate]
}

type Repository struct{ gateway *events.Gateway }

func New(gateway *events.Gateway) *Repository { return &Repository{gateway: gateway} }

func (r *Repository) SaveOrganization(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Organization, detail any) error {
	return r.save(ctx, string(value.ID), eventType, actorID, "", correlationID, KindOrganization, value.ID, version, value, detail)
}

func (r *Repository) SaveMission(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Mission, detail any) error {
	if actorID != "runtime" || !validMissionEventType(eventType) {
		return fmt.Errorf("mission revisions require a runtime-owned lifecycle event")
	}
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindMission, value.ID, version, value, detail)
}

func (r *Repository) SaveGoal(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Goal, detail any) error {
	if actorID != "runtime" || !validGoalEventType(eventType) {
		return fmt.Errorf("goal revisions require a runtime-owned lifecycle event")
	}
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindGoal, value.ID, version, value, detail)
}

func (r *Repository) EvaluateGoalProgress(ctx context.Context, organizationID core.ID, goalID core.ID) (events.GoalProgressAdmission, error) {
	if r == nil || r.gateway == nil || organizationID == "" || goalID == "" {
		return events.GoalProgressAdmission{}, fmt.Errorf("durable Goal progress gateway is required")
	}
	return r.gateway.EvaluateGoalProgress(ctx, string(organizationID), goalID)
}

func validMissionEventType(eventType string) bool {
	return eventType == "MISSION_CREATED" || eventType == "MISSION_REVISED" || eventType == "MISSION_RETIRED"
}

func validGoalEventType(eventType string) bool {
	return eventType == "GOAL_CREATED" || eventType == "GOAL_REFINED" || eventType == "GOAL_PAUSED" || eventType == "GOAL_RESUMED" || eventType == "GOAL_RETIRED"
}

func (r *Repository) SaveTeam(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Team, detail any) error {
	expectedEventType := "TEAM_REVISED"
	if version == 1 {
		expectedEventType = "TEAM_CREATED"
	}
	if actorID != "runtime" || eventType != expectedEventType {
		return fmt.Errorf("team revisions require the runtime-owned lifecycle event")
	}
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindTeam, value.ID, version, value, detail)
}

func (r *Repository) SaveAgentBlueprint(ctx context.Context, eventType, actorID, correlationID string, version int, value core.AgentBlueprint, detail any) error {
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindAgentBlueprint, value.ID, version, value, detail)
}

func (r *Repository) SaveExecutionProfile(ctx context.Context, eventType, actorID, correlationID string, version int, value core.ExecutionProfile, detail any) error {
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindExecutionProfile, value.ID, version, value, detail)
}

func (r *Repository) SaveAgent(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Agent, detail any) error {
	if actorID != "runtime" || correlationID == "" {
		return fmt.Errorf("complete runtime-owned Agent transition is required")
	}
	if err := events.ValidateAgentProjectionTarget(eventType, version, value); err != nil {
		return err
	}
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindAgent, value.ID, version, value, detail)
}

func (r *Repository) SaveIntent(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Intent, detail any) error {
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindIntent, value.ID, version, value, detail)
}

func (r *Repository) SaveWork(ctx context.Context, organizationID core.ID, eventType, actorID, correlationID string, version int, value core.Work, detail any) error {
	if value.Status == core.WorkCompleted || eventType == "WORK_COMPLETED" {
		if value.Status != core.WorkCompleted || eventType != "WORK_COMPLETED" {
			return fmt.Errorf("work completion requires its exact runtime-owned lifecycle event")
		}
		var evidence events.WorkCompletionTransitionPayload
		encoded, err := json.Marshal(detail)
		if err == nil {
			decoder := json.NewDecoder(bytes.NewReader(encoded))
			decoder.DisallowUnknownFields()
			err = decoder.Decode(&evidence)
		}
		if err != nil {
			return fmt.Errorf("completed work requires exact durable evidence")
		}
		return r.SaveCompletedWork(ctx, organizationID, actorID, correlationID, version, value, evidence)
	}
	validFailure := value.Status == core.WorkFailed && (eventType == "WORK_FAILED" || eventType == "WORK_PLANNING_FAILED")
	if actorID != "runtime" || value.Status == core.WorkActive && eventType != "WORK_CREATED" || value.Status != core.WorkActive && !validFailure {
		return fmt.Errorf("work revisions require the exact runtime-owned lifecycle event")
	}
	return r.save(ctx, string(organizationID), eventType, actorID, "", correlationID, KindWork, value.ID, version, value, detail)
}

func (r *Repository) SaveCompletedWork(ctx context.Context, organizationID core.ID, actorID, correlationID string, version int, value core.Work, detail events.WorkCompletionTransitionPayload) error {
	if r == nil || r.gateway == nil || organizationID == "" || actorID != "runtime" || correlationID == "" || value.ID == "" || value.Status != core.WorkCompleted {
		return fmt.Errorf("complete evidence-backed work transition is required")
	}
	_, err := r.gateway.PublishWorkCompletion(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: "WORK_COMPLETED", SourceActorID: actorID,
			CorrelationID: correlationID, Payload: detail,
		},
		ProjectionKind: KindWork, RecordID: string(value.ID), Version: version, Value: value,
	})
	return err
}

func (r *Repository) SaveTask(ctx context.Context, organizationID core.ID, eventType, actorID, correlationID string, version int, value core.Task, detail any) error {
	if actorID != "runtime" || organizationID == "" || correlationID == "" || value.ID == "" {
		return fmt.Errorf("complete runtime-owned Task transition is required")
	}
	if err := events.ValidateTaskProjectionTarget(eventType, version, value); err != nil {
		return err
	}
	return r.save(ctx, string(organizationID), eventType, actorID, string(value.ID), correlationID, KindTask, value.ID, version, value, detail)
}

func (r *Repository) SaveExperiment(ctx context.Context, eventType, correlationID string, version int, value core.Experiment) error {
	if r == nil || r.gateway == nil || correlationID == "" || value.ID == "" {
		return fmt.Errorf("complete Lab experiment transition is required")
	}
	if err := events.ValidateExperimentProjectionTarget(eventType, version, value); err != nil {
		return err
	}
	return r.save(ctx, string(value.OrganizationID), eventType, "runtime", "", correlationID, KindLabExperiment, value.ID, version, value, nil)
}

func (r *Repository) SavePromotionCandidate(ctx context.Context, correlationID string, value core.PromotionCandidate) error {
	if r == nil || r.gateway == nil || correlationID == "" || value.ID == "" {
		return fmt.Errorf("complete Lab promotion nomination is required")
	}
	if err := events.ValidatePromotionCandidateProjectionTarget("LAB_PROMOTION_CANDIDATE_CREATED", 1, value); err != nil {
		return err
	}
	return r.save(ctx, string(value.OrganizationID), "LAB_PROMOTION_CANDIDATE_CREATED", "runtime", "", correlationID, KindLabPromotionCandidate, value.ID, 1, value, nil)
}

// SaveExperimentalSubmission atomically admits a new Intent, its bounded Work,
// and the Work's experimental containment. A restart can therefore never
// observe an experimental request as ordinary uncontained Work.
func (r *Repository) SaveExperimentalSubmission(ctx context.Context, correlationID string, intent core.Intent, work core.Work, experiment core.Experiment) error {
	if r == nil || r.gateway == nil || correlationID == "" || intent.ID == "" || work.ID == "" || experiment.ID == "" ||
		intent.OrganizationID == "" || work.IntentID != intent.ID || experiment.OrganizationID != intent.OrganizationID || experiment.WorkID != work.ID ||
		work.Status != core.WorkActive {
		return fmt.Errorf("complete experimental Intent, Work, and containment are required")
	}
	if err := events.ValidateExperimentProjectionTarget("LAB_EXPERIMENT_STARTED", 1, experiment); err != nil {
		return err
	}
	drafts := []events.ProjectionDraft{
		{
			Event: events.TrustedDraft{
				OrganizationID: string(intent.OrganizationID), EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID,
			},
			ProjectionKind: KindIntent, RecordID: string(intent.ID), Version: 1, Value: intent,
		},
		{
			Event: events.TrustedDraft{
				OrganizationID: string(intent.OrganizationID), EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID,
			},
			ProjectionKind: KindWork, RecordID: string(work.ID), Version: 1, Value: work,
		},
		{
			Event: events.TrustedDraft{
				OrganizationID: string(intent.OrganizationID), EventType: "LAB_EXPERIMENT_STARTED", SourceActorID: "runtime", CorrelationID: correlationID,
			},
			ProjectionKind: KindLabExperiment, RecordID: string(experiment.ID), Version: 1, Value: experiment,
		},
	}
	_, err := r.gateway.PublishProjections(ctx, drafts)
	return err
}

func (r *Repository) StartAgentExecution(ctx context.Context, organizationID core.ID, correlationID string, version int, value core.Task, mode string, strategicEventRefs []string, strategicContextRefs []core.VersionedRef, routes []events.InboxRoute) (events.Event, []events.InboxSelection, error) {
	if r == nil || r.gateway == nil || organizationID == "" || correlationID == "" || value.ID == "" || value.ExecutionKind != core.ExecutionAgent || value.Status != core.TaskRunning || version < 2 {
		return events.Event{}, nil, fmt.Errorf("complete Agent execution-start projection is required")
	}
	return r.gateway.PublishExecutionStart(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: "EXECUTION_STARTED", SourceActorID: "runtime",
			TaskID: string(value.ID), CorrelationID: correlationID, Payload: events.ExecutionStartDetail{Mode: mode, StrategicEventRefs: strategicEventRefs, StrategicContextRefs: strategicContextRefs},
		},
		ProjectionKind: KindTask, RecordID: string(value.ID), Version: version, Value: value,
	}, routes)
}

// StartTaskExecution admits deterministic and user-operated execution starts
// through the same transactional strategic-context boundary as Agent work.
// It does not grant capabilities, approval authority, or effect permission.
func (r *Repository) StartTaskExecution(ctx context.Context, organizationID core.ID, correlationID string, version int, value core.Task, mode, inputEventRef string, strategicEventRefs []string, strategicContextRefs []core.VersionedRef) (events.Event, error) {
	if r == nil || r.gateway == nil || organizationID == "" || correlationID == "" || value.ID == "" || value.Status != core.TaskRunning || version < 2 ||
		value.ExecutionKind != core.ExecutionDeterministic && value.ExecutionKind != core.ExecutionHuman {
		return events.Event{}, fmt.Errorf("complete deterministic or user execution-start projection is required")
	}
	started, selections, err := r.gateway.PublishExecutionStart(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: "EXECUTION_STARTED", SourceActorID: "runtime",
			TaskID: string(value.ID), CorrelationID: correlationID, Payload: events.ExecutionStartDetail{Mode: mode, InputEventRef: inputEventRef, StrategicEventRefs: strategicEventRefs, StrategicContextRefs: strategicContextRefs},
		},
		ProjectionKind: KindTask, RecordID: string(value.ID), Version: version, Value: value,
	}, nil)
	if err == nil && len(selections) != 0 {
		return events.Event{}, fmt.Errorf("non-Agent execution start selected an inbox")
	}
	return started, err
}

// SaveNewTasks atomically creates a complete Task DAG. Every Task starts at
// version one; later transitions continue through SaveTask.
func (r *Repository) SaveNewTasks(ctx context.Context, organizationID core.ID, actorID, correlationID string, values []core.Task) error {
	if r == nil || r.gateway == nil || organizationID == "" || actorID == "" || correlationID == "" || len(values) == 0 {
		return fmt.Errorf("complete Task-DAG projection identity is required")
	}
	drafts := make([]events.ProjectionDraft, 0, len(values))
	for _, value := range values {
		if value.ID == "" {
			return fmt.Errorf("Task-DAG projection contains an empty task identity")
		}
		drafts = append(drafts, events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: string(organizationID), EventType: "TASK_CREATED", SourceActorID: actorID,
				TaskID: string(value.ID), CorrelationID: correlationID,
			},
			ProjectionKind: KindTask, RecordID: string(value.ID), Version: 1, Value: value,
		})
	}
	_, err := r.gateway.PublishProjections(ctx, drafts)
	return err
}

// SaveBlockedTask atomically persists the blocked child projection and makes
// the same Event Contract available to its parent Task for remediation.
func (r *Repository) SaveBlockedTask(ctx context.Context, organizationID core.ID, actorID, correlationID string, version int, value core.Task, detail events.TaskBlockedPayload, parentTaskID core.ID) error {
	if value.Status != core.TaskBlocked || value.ParentID == "" || value.ParentID != parentTaskID {
		return fmt.Errorf("blocked child task and exact parent are required")
	}
	return r.saveAddressed(ctx, string(organizationID), "TASK_BLOCKED", actorID, string(value.ID), correlationID, KindTask, value.ID, version, value, detail, events.RecipientTask, string(parentTaskID))
}

func (r *Repository) save(ctx context.Context, organizationID, eventType, actorID, taskID, correlationID, kind string, id core.ID, version int, value, detail any) error {
	return r.saveAddressed(ctx, organizationID, eventType, actorID, taskID, correlationID, kind, id, version, value, detail, "", "")
}

func (r *Repository) saveAddressed(ctx context.Context, organizationID, eventType, actorID, taskID, correlationID, kind string, id core.ID, version int, value, detail any, recipientScope, recipientID string) error {
	if r == nil || r.gateway == nil {
		return fmt.Errorf("durable projection gateway is required")
	}
	_, err := r.gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: organizationID,
			EventType:      eventType,
			SourceActorID:  actorID,
			RecipientScope: recipientScope,
			RecipientID:    recipientID,
			TaskID:         taskID,
			CorrelationID:  correlationID,
			Payload:        detail,
		},
		ProjectionKind: kind,
		RecordID:       string(id),
		Version:        version,
		Value:          value,
	})
	return err
}

func (r *Repository) Load(ctx context.Context) (Snapshot, error) {
	if r == nil || r.gateway == nil {
		return Snapshot{}, fmt.Errorf("durable projection gateway is required")
	}
	return r.loadFromRecords(ctx)
}

// ValidateCompletionAdmissions performs the full authoritative-chain audit
// required before recovery mutates durable state. Routine scheduler loads use
// the transactionally admitted records projection and do not replay unrelated
// historical event streams.
func (r *Repository) ValidateCompletionAdmissions(ctx context.Context, snapshot Snapshot) error {
	if r == nil || r.gateway == nil {
		return fmt.Errorf("durable projection gateway is required")
	}
	stream, err := r.gateway.Events(ctx, "")
	if err != nil {
		return err
	}
	inboxObservations, err := r.gateway.InboxObservations(ctx)
	if err != nil {
		return err
	}
	if err := validateProjectionEventAdmissions(stream, inboxObservations); err != nil {
		return err
	}
	records, err := r.readProjectionRecords(ctx)
	if err != nil {
		return err
	}
	if err := validateProjectionRecordCoverage(stream, records); err != nil {
		return err
	}
	if err := validateProjectionEventOrganizationBindings(snapshot, stream); err != nil {
		return err
	}
	if err := validateWorkCompletionAdmissions(snapshot, stream, records[KindTeam], inboxObservations); err != nil {
		return err
	}
	return r.validateGoalAchievementAdmissions(ctx, snapshot)
}

// Rebuild ignores the records table and deterministically replays projection
// records embedded in the authoritative event stream.
func (r *Repository) Rebuild(ctx context.Context) (Snapshot, error) {
	if r == nil || r.gateway == nil {
		return Snapshot{}, fmt.Errorf("durable projection gateway is required")
	}
	stream, err := r.gateway.Events(ctx, "")
	if err != nil {
		return Snapshot{}, err
	}
	inboxObservations, err := r.gateway.InboxObservations(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if err := validateProjectionEventAdmissions(stream, inboxObservations); err != nil {
		return Snapshot{}, err
	}
	records := make(map[string][][]byte)
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return Snapshot{}, fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if !present {
			continue
		}
		if payload.Projection.CorrelationID != event.CorrelationID {
			return Snapshot{}, fmt.Errorf("event %s projection %s has a mismatched correlation boundary", event.EventID, payload.Projection.RecordID)
		}
		body, err := json.Marshal(payload.Projection)
		if err != nil {
			return Snapshot{}, err
		}
		kind := payload.Projection.ProjectionKind
		records[kind] = append(records[kind], body)
	}
	snapshot, err := decodeSnapshot(records)
	if err != nil {
		return Snapshot{}, err
	}
	if err := validateProjectionEventOrganizationBindings(snapshot, stream); err != nil {
		return Snapshot{}, err
	}
	if err := validateWorkCompletionAdmissions(snapshot, stream, records[KindTeam], inboxObservations); err != nil {
		return Snapshot{}, err
	}
	if err := validateGoalAchievementAdmissionsFromEvents(snapshot, stream); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func validateProjectionEventAdmissions(stream []events.Event, inboxObservations map[string]events.InboxObservationBinding) error {
	eventIDs := make(map[string]struct{}, len(stream))
	sequences := make(map[int64]struct{}, len(stream))
	ordered := append([]events.Event(nil), stream...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].Sequence < ordered[right].Sequence })
	reviewEvidence := events.IndexReviewedIntentEvidence(ordered)
	tasks := make(map[core.ID]Versioned[core.Task])
	agents := make(map[core.ID]Versioned[core.Agent])
	missions := make(map[core.ID]Versioned[core.Mission])
	goals := make(map[core.ID]Versioned[core.Goal])
	works := make(map[core.ID]Versioned[core.Work])
	experiments := make(map[core.ID]Versioned[core.Experiment])
	graph := core.DurableGraph{
		Organizations: map[core.ID]core.DurableState[core.Organization]{}, Missions: map[core.ID]core.DurableState[core.Mission]{},
		Goals: map[core.ID]core.DurableState[core.Goal]{}, Teams: map[core.ID]core.DurableState[core.Team]{},
		AgentBlueprints: map[core.ID]core.DurableState[core.AgentBlueprint]{}, ExecutionProfiles: map[core.ID]core.DurableState[core.ExecutionProfile]{},
		Agents: map[core.ID]core.DurableState[core.Agent]{}, Intents: map[core.ID]core.DurableState[core.Intent]{},
		Works: map[core.ID]core.DurableState[core.Work]{}, Tasks: map[core.ID]core.DurableState[core.Task]{},
		Experiments: map[core.ID]core.DurableState[core.Experiment]{}, PromotionCandidates: map[core.ID]core.DurableState[core.PromotionCandidate]{},
	}
	confirmations := make(map[string][]events.Event)
	teamRecords := make(map[string][][]byte)
	blueprintRevisions := make(map[core.ID]map[string]core.AgentBlueprint)
	profileRevisions := make(map[core.ID]map[string]core.ExecutionProfile)
	for _, event := range ordered {
		if event.EventID == "" || event.Sequence < 1 || event.CreatedAt.IsZero() {
			return fmt.Errorf("event stream contains an incomplete envelope")
		}
		if event.SchemaVersion != events.SchemaVersion {
			return fmt.Errorf("event %s uses unsupported schema version %d", event.EventID, event.SchemaVersion)
		}
		if _, duplicate := eventIDs[event.EventID]; duplicate {
			return fmt.Errorf("event stream contains duplicate event id %s", event.EventID)
		}
		if _, duplicate := sequences[event.Sequence]; duplicate {
			return fmt.Errorf("event stream contains duplicate sequence %d at %s", event.Sequence, event.EventType)
		}
		eventIDs[event.EventID] = struct{}{}
		sequences[event.Sequence] = struct{}{}
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if !present {
			if event.EventType == "INTENT_CONFIRMED" {
				var confirmation events.IntentConfirmedPayload
				if decodeExactProjectionJSON(event.Payload, &confirmation) != nil {
					return fmt.Errorf("event %s contains an invalid intent confirmation", event.EventID)
				}
				confirmations[event.CorrelationID] = append(confirmations[event.CorrelationID], event)
				if confirmation.GoalID == "" {
					if err := events.ValidateIndexedReviewedIntentAdmission(reviewEvidence.At(event), event); err != nil {
						return fmt.Errorf("event %s: %w", event.EventID, err)
					}
					continue
				}
				goal, found := graph.Goals[core.ID(confirmation.GoalID)]
				if !found || goal.Value.ID != core.ID(confirmation.GoalID) || goal.Value.Status != core.GoalActive {
					return fmt.Errorf("event %s Goal-bound intent confirmation lacks its active Goal", event.EventID)
				}
				if err := events.ValidateIndexedReviewedGoalIntentAdmission(reviewEvidence.At(event), event, goal.Value); err != nil {
					return fmt.Errorf("event %s: %w", event.EventID, err)
				}
			}
			if events.RequiresProjectionAdmission(event.EventType, event.SourceActorID) {
				return fmt.Errorf("event %s uses a projection lifecycle event without typed admission", event.EventID)
			}
			continue
		}
		if err := events.ValidateProjectionEventBoundary(event, payload); err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		record := payload.Projection
		switch record.ProjectionKind {
		case KindOrganization:
			var value core.Organization
			if decodeExactProjectionJSON(record.Value, &value) != nil || value.ID != core.ID(record.RecordID) || string(value.ID) != event.OrganizationID {
				err = fmt.Errorf("contains an invalid Organization projection")
			} else {
				err = core.AdmitDurableRevision(graph.Organizations, value.ID, record.Version, record.CorrelationID, value, false, nil)
			}
		case KindMission:
			var value core.Mission
			if decodeExactProjectionJSON(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Mission projection")
			} else {
				err = validateOrganizedProjectionLifecycle(value, event, record, graph, missions, "Mission", func(value core.Mission) core.ID { return value.ID }, func(value core.Mission) core.ID { return value.OrganizationID }, events.ValidateMissionProjectionTransition)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Missions, value.ID, record.Version, record.CorrelationID, value, false, core.ValidMissionRevision)
			}
		case KindGoal:
			var value core.Goal
			if decodeExactProjectionJSON(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Goal projection")
			} else {
				err = validateOrganizedProjectionLifecycle(value, event, record, graph, goals, "Goal", func(value core.Goal) core.ID { return value.ID }, func(value core.Goal) core.ID { return value.OrganizationID }, events.ValidateGoalProjectionTransition)
			}
			if err == nil {
				mission, found := graph.Missions[value.MissionID]
				if !found || mission.Value.ID != value.MissionID || mission.Value.OrganizationID != value.OrganizationID {
					err = fmt.Errorf("goal requires its durable same-organization Mission")
				}
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Goals, value.ID, record.Version, record.CorrelationID, value, false, core.ValidGoalRevision)
			}
		case KindAgentBlueprint:
			err = admitVersionedOrganizedProjection(record, event, graph, graph.AgentBlueprints, blueprintRevisions, "Agent blueprint", func(value core.AgentBlueprint) core.ID { return value.ID }, func(value core.AgentBlueprint) core.ID { return value.OrganizationID }, func(value core.AgentBlueprint) string { return value.Version }, core.ValidAgentBlueprint, core.ValidAgentBlueprintRevision)
		case KindExecutionProfile:
			err = admitVersionedOrganizedProjection(record, event, graph, graph.ExecutionProfiles, profileRevisions, "execution profile", func(value core.ExecutionProfile) core.ID { return value.ID }, func(value core.ExecutionProfile) core.ID { return value.OrganizationID }, func(value core.ExecutionProfile) string { return value.Version }, core.ValidExecutionProfile, core.ValidExecutionProfileRevision)
		case KindAgent:
			var value core.Agent
			if decodeExactProjectionJSON(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Agent projection")
			} else {
				err = validateProjectionEventLifecycle(event, record, "Agent", agents, func(value core.Agent) core.ID { return value.ID }, false, events.ValidateAgentProjectionTransition)
			}
			if err == nil {
				err = validateProjectionOrganizationAtAdmission(value.OrganizationID, event, graph)
			}
			if err == nil {
				blueprint, blueprintFound := graph.AgentBlueprints[value.BlueprintID]
				profile, profileFound := graph.ExecutionProfiles[value.ExecutionProfileID]
				if !blueprintFound || !profileFound || !core.ValidAgentConfigurationBinding(value, blueprint.Value, profile.Value) {
					err = fmt.Errorf("agent references invalid pinned configuration at admission")
				}
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Agents, value.ID, record.Version, record.CorrelationID, value, false, core.ValidAgentRevision)
			}
		case KindTeam:
			var value core.Team
			if decodeExactProjectionJSON(record.Value, &value) != nil || value.ID != core.ID(record.RecordID) {
				err = fmt.Errorf("contains an invalid Team projection")
			} else {
				err = validateProjectionTeamAtAdmission(value, event, graph)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Teams, value.ID, record.Version, record.CorrelationID, value, false, core.ValidTeamRevision)
			}
			if err == nil {
				body, marshalErr := json.Marshal(record)
				if marshalErr != nil {
					err = marshalErr
				} else {
					teamRecords[event.OrganizationID] = append(teamRecords[event.OrganizationID], body)
				}
			}
		case KindIntent:
			var value core.Intent
			if decodeExactProjectionJSON(record.Value, &value) != nil || value.ID != core.ID(record.RecordID) {
				err = fmt.Errorf("contains an invalid Intent projection")
			} else {
				err = validateProjectionOrganizationAtAdmission(value.OrganizationID, event, graph)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Intents, value.ID, record.Version, record.CorrelationID, value, true, nil)
			}
		case KindWork:
			err = admitLifecycleProjection(event, record, "Work", works, graph.Works, func(value core.Work) core.ID { return value.ID }, events.ValidateWorkProjectionTransition, func(value core.Work) error {
				return validateProjectionWorkAtAdmission(value, event, record, graph, confirmations, reviewEvidence)
			}, core.ValidWorkRevision)
		case KindTask:
			var value core.Task
			if decodeExactProjectionJSON(record.Value, &value) != nil {
				err = fmt.Errorf("contains an invalid Task projection")
			} else {
				err = validateProjectionEventLifecycle(event, record, "Task", tasks, func(value core.Task) core.ID { return value.ID }, true, events.ValidateTaskProjectionTransition)
			}
			if err == nil {
				err = validateProjectionTaskAtAdmission(value, event, record, graph, ordered)
			}
			if err == nil && event.EventType == "TASK_VERIFIED_COMPLETE" {
				err = validateTaskCompletionAtAdmission(value, event, record, graph, ordered, teamRecords[event.OrganizationID], inboxObservations, blueprintRevisions, profileRevisions)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.Tasks, value.ID, record.Version, record.CorrelationID, value, true, core.ValidTaskRevision)
			}
		case KindLabExperiment:
			err = admitLifecycleProjection(event, record, "Lab experiment", experiments, graph.Experiments, func(value core.Experiment) core.ID { return value.ID }, events.ValidateExperimentProjectionTransition, func(value core.Experiment) error {
				return validateLabExperimentAtAdmission(value, event, record, graph, ordered)
			}, core.ValidExperimentRevision)
		case KindLabPromotionCandidate:
			var value core.PromotionCandidate
			if decodeExactProjectionJSON(record.Value, &value) != nil || events.ValidatePromotionCandidateProjectionTarget(event.EventType, record.Version, value) != nil {
				err = fmt.Errorf("contains an invalid Lab promotion-candidate projection")
			} else {
				err = validateLabPromotionCandidateAtAdmission(value, event, record, graph, ordered)
			}
			if err == nil {
				err = core.AdmitDurableRevision(graph.PromotionCandidates, value.ID, record.Version, record.CorrelationID, value, true, nil)
			}
		default:
			err = fmt.Errorf("contains unsupported projection kind %s", record.ProjectionKind)
		}
		if err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
	}
	return nil
}

func validateProjectionOrganizationAtAdmission(organizationID core.ID, event events.Event, graph core.DurableGraph) error {
	organization, found := graph.Organizations[organizationID]
	if organizationID == "" || !found || organization.Value.ID != organizationID || event.OrganizationID != string(organizationID) {
		return fmt.Errorf("requires its durable parent Organization at admission")
	}
	return nil
}

func validateProjectionTeamAtAdmission(team core.Team, event events.Event, graph core.DurableGraph) error {
	if err := validateProjectionOrganizationAtAdmission(team.OrganizationID, event, graph); err != nil {
		return err
	}
	return core.ValidateTeamRoster(team, graph)
}

func validateProjectionWorkAtAdmission(work core.Work, event events.Event, record events.ProjectionRecord, graph core.DurableGraph, confirmations map[string][]events.Event, reviewEvidence events.ReviewedIntentEvidenceIndex) error {
	intent, found := graph.Intents[work.IntentID]
	if !found || intent.CorrelationID != record.CorrelationID || intent.Value.ID != work.IntentID || intent.Value.GoalID != work.GoalID || intent.Value.NormalizedObjective != work.Objective || string(intent.Value.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("work requires its exact prior Intent on the same organization and correlation boundary")
	}
	if intentRequiresConfirmation(intent.Value) {
		matching := confirmations[record.CorrelationID]
		if len(matching) != 1 || matching[0].Sequence >= event.Sequence {
			return fmt.Errorf("external Work requires one prior reviewed intent confirmation")
		}
		if err := events.ValidateIntentConfirmation(reviewEvidence.At(matching[0]), matching[0], intent.Value); err != nil {
			return err
		}
	}
	return nil
}

func intentRequiresConfirmation(intent core.Intent) bool {
	return intent.GoalID != "" || intent.SourceChannel == "HUMAN_DIRECT" || intent.SourceChannel == "A2A" || intent.SourcePrincipalKind == core.PrincipalHuman || intent.SourcePrincipalKind == core.PrincipalExternalAgent
}

func validateProjectionTaskAtAdmission(task core.Task, event events.Event, record events.ProjectionRecord, graph core.DurableGraph, stream []events.Event) error {
	work, found := graph.Works[task.WorkID]
	if !found || work.CorrelationID != record.CorrelationID || work.Value.ID != task.WorkID || work.Value.Status != core.WorkActive {
		return fmt.Errorf("task requires its exact active Work on the same correlation boundary")
	}
	intent, found := graph.Intents[work.Value.IntentID]
	if !found || intent.CorrelationID != record.CorrelationID || intent.Value.ID != work.Value.IntentID || string(intent.Value.OrganizationID) != event.OrganizationID {
		return fmt.Errorf("task requires its exact Intent organization and correlation boundary")
	}
	if err := core.ValidateTaskAssignment(task, intent.Value.OrganizationID, graph); err != nil {
		return err
	}
	if event.EventType == "EXECUTION_STARTED" {
		return events.ValidateTaskExecutionStart(event, task, record.Version, work.Value, intent.Value, stream)
	}
	return nil
}

func validateOrganizedProjectionLifecycle[T any](value T, event events.Event, record events.ProjectionRecord, graph core.DurableGraph, history map[core.ID]Versioned[T], kind string, identity func(T) core.ID, organization func(T) core.ID, validate func(string, int, *T, T) error) error {
	if err := validateProjectionEventLifecycle(event, record, kind, history, identity, false, validate); err != nil {
		return err
	}
	return validateProjectionOrganizationAtAdmission(organization(value), event, graph)
}

func admitVersionedOrganizedProjection[T any](record events.ProjectionRecord, event events.Event, graph core.DurableGraph, target map[core.ID]Versioned[T], revisions map[core.ID]map[string]T, kind string, identity func(T) core.ID, organization func(T) core.ID, semanticVersion func(T) string, valid func(T) bool, validRevision func(T, T) bool) error {
	var value T
	if decodeExactProjectionJSON(record.Value, &value) != nil || identity(value) != core.ID(record.RecordID) || !valid(value) {
		return fmt.Errorf("contains an invalid %s projection", kind)
	}
	if err := validateProjectionOrganizationAtAdmission(organization(value), event, graph); err != nil {
		return err
	}
	if err := core.AdmitDurableRevision(target, identity(value), record.Version, record.CorrelationID, value, false, validRevision); err != nil {
		return err
	}
	if revisions[identity(value)] == nil {
		revisions[identity(value)] = make(map[string]T)
	}
	revisions[identity(value)][semanticVersion(value)] = value
	return nil
}

func validateLabExperimentAtAdmission(experiment core.Experiment, event events.Event, record events.ProjectionRecord, graph core.DurableGraph, stream []events.Event) error {
	if experiment.ID != core.ID(record.RecordID) || experiment.OrganizationID != core.ID(event.OrganizationID) || record.CorrelationID != event.CorrelationID {
		return fmt.Errorf("lab experiment crosses its durable identity boundary")
	}
	work, found := graph.Works[experiment.WorkID]
	if !found || work.CorrelationID != event.CorrelationID {
		return fmt.Errorf("lab experiment requires its exact bounded Work")
	}
	intent, found := graph.Intents[work.Value.IntentID]
	if !found || intent.Value.OrganizationID != experiment.OrganizationID || experiment.Objective != work.Value.Objective {
		return fmt.Errorf("lab experiment crosses its Work organization or objective")
	}
	switch experiment.Status {
	case core.ExperimentRunning:
		if work.Value.Status != core.WorkActive {
			return fmt.Errorf("running Lab experiment requires active Work")
		}
	case core.ExperimentCompleted:
		if !core.ValidTerminalExperimentWorkStatus(experiment, work.Value) || len(experiment.ResultEventRefs) != 1 {
			return fmt.Errorf("completed Lab experiment requires one exact completed Work transition")
		}
		completion, found := priorEventByID(stream, event.Sequence, experiment.ResultEventRefs[0])
		if !found || completion.OrganizationID != event.OrganizationID || completion.CorrelationID != event.CorrelationID || completion.EventType != "WORK_COMPLETED" {
			return fmt.Errorf("completed Lab experiment result is not its exact Work completion")
		}
		payload, admitted, projectionErr := events.AdmittedProjection(completion)
		var detail events.WorkCompletionTransitionPayload
		if projectionErr != nil || !admitted || payload.Projection.ProjectionKind != KindWork || payload.Projection.RecordID != string(experiment.WorkID) || decodeExactProjectionJSON(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" {
			return fmt.Errorf("completed Lab experiment result lacks evidence-backed Work admission")
		}
		evidence, found := priorEventByID(stream, completion.Sequence, detail.EvidenceEventRef)
		if !found || evidence.EventType != "WORK_COMPLETION_EVALUATED" || evidence.OrganizationID != event.OrganizationID || evidence.CorrelationID != event.CorrelationID || !slices.Equal(experiment.ArtifactRefs, evidence.ArtifactRefs) {
			return fmt.Errorf("completed Lab experiment artifacts do not match Work evidence")
		}
	case core.ExperimentFailed:
		if !core.ValidTerminalExperimentWorkStatus(experiment, work.Value) {
			return fmt.Errorf("failed Lab experiment conflicts with its Work outcome")
		}
	}
	return nil
}

func validateLabPromotionCandidateAtAdmission(candidate core.PromotionCandidate, event events.Event, record events.ProjectionRecord, graph core.DurableGraph, stream []events.Event) error {
	if candidate.ID != core.ID(record.RecordID) || candidate.OrganizationID != core.ID(event.OrganizationID) || record.CorrelationID != event.CorrelationID {
		return fmt.Errorf("lab promotion candidate crosses its durable identity boundary")
	}
	experiment, found := graph.Experiments[candidate.ExperimentID]
	if !found || experiment.Value.Status != core.ExperimentCompleted || experiment.Value.OrganizationID != candidate.OrganizationID ||
		experiment.Version != candidate.ExperimentVersion || experiment.CorrelationID != event.CorrelationID ||
		!slices.Equal(experiment.Value.ResultEventRefs, candidate.ExperimentResultEventRefs) || candidate.CreatedAt.Before(*experiment.Value.FinishedAt) {
		return fmt.Errorf("lab promotion candidate lacks its exact completed experiment")
	}
	work := graph.Works[experiment.Value.WorkID]
	intent := graph.Intents[work.Value.IntentID]
	if candidate.NominatedBy != intent.Value.SourcePrincipalID {
		return fmt.Errorf("lab promotion candidate does not preserve its commissioning actor")
	}
	for _, ref := range candidate.ReproductionEvidenceRefs {
		reproduction, found := priorEventByID(stream, event.Sequence, ref)
		if !found || reproduction.OrganizationID != event.OrganizationID || reproduction.CorrelationID == event.CorrelationID || reproduction.EventType != "WORK_COMPLETED" {
			return fmt.Errorf("lab promotion candidate lacks independent same-organization reproduction evidence")
		}
		payload, admitted, err := events.AdmittedProjection(reproduction)
		if err != nil || !admitted || payload.Projection.ProjectionKind != KindWork || payload.Projection.RecordID == string(experiment.Value.WorkID) {
			return fmt.Errorf("lab promotion reproduction is not a distinct admitted Work completion")
		}
	}
	return nil
}

func priorEventByID(stream []events.Event, beforeSequence int64, eventID string) (events.Event, bool) {
	for _, event := range stream {
		if event.EventID == eventID && event.Sequence < beforeSequence {
			return event, true
		}
	}
	return events.Event{}, false
}

func validateTaskCompletionAtAdmission(task core.Task, event events.Event, record events.ProjectionRecord, graph core.DurableGraph, stream []events.Event, teamRecords [][]byte, inboxObservations map[string]events.InboxObservationBinding, blueprintRevisions map[core.ID]map[string]core.AgentBlueprint, profileRevisions map[core.ID]map[string]core.ExecutionProfile) error {
	work, found := graph.Works[task.WorkID]
	if !found {
		return fmt.Errorf("completed Task lacks its durable Work")
	}
	intent, found := graph.Intents[work.Value.IntentID]
	if !found {
		return fmt.Errorf("completed Task lacks its durable Intent")
	}
	tasks := make([]events.WorkCompletionTaskBinding, 0)
	blueprints := make(map[core.ID]core.AgentBlueprint)
	profiles := make(map[core.ID]core.ExecutionProfile)
	addTask := func(candidate core.Task, version int, correlationID string) error {
		if candidate.WorkID != task.WorkID {
			return nil
		}
		tasks = append(tasks, events.WorkCompletionTaskBinding{Task: candidate, Version: version, CorrelationID: correlationID})
		if candidate.ExecutionKind != core.ExecutionAgent || candidate.AgentConfig == nil {
			return nil
		}
		config := candidate.AgentConfig
		blueprint, blueprintFound := blueprintRevisions[config.BlueprintID][config.BlueprintVersion]
		profile, profileFound := profileRevisions[config.ProfileID][config.ProfileVersion]
		if !blueprintFound || !profileFound {
			return fmt.Errorf("completed Agent Task lacks its pinned configuration revision")
		}
		blueprints[config.BlueprintID] = blueprint
		profiles[config.ProfileID] = profile
		return nil
	}
	for taskID, state := range graph.Tasks {
		if taskID == task.ID {
			continue
		}
		if err := addTask(state.Value, state.Version, state.CorrelationID); err != nil {
			return err
		}
	}
	if err := addTask(task, record.Version, record.CorrelationID); err != nil {
		return err
	}
	teamRevisions, err := events.ResolveTeamRevisionBindings(event.OrganizationID, teamRecords, stream)
	if err != nil {
		return fmt.Errorf("resolve completed Task Team history: %w", err)
	}
	binding := events.WorkCompletionBinding{
		OrganizationID: event.OrganizationID, CorrelationID: record.CorrelationID,
		Work: work.Value, WorkVersion: work.Version, Intent: intent.Value, Tasks: tasks,
		TeamRevisions: teamRevisions, InboxObservations: inboxObservations, AgentBlueprints: blueprints, ExecutionProfiles: profiles,
	}
	_, err = events.ValidateTaskCompletionEvidenceChain(binding, events.WorkCompletionTaskBinding{Task: task, Version: record.Version, CorrelationID: record.CorrelationID}, event, stream)
	return err
}

func validateProjectionEventLifecycle[T any](event events.Event, record events.ProjectionRecord, kind string, history map[core.ID]Versioned[T], identity func(T) core.ID, correlationStable bool, validate func(string, int, *T, T) error) error {
	var value T
	if decodeExactProjectionJSON(record.Value, &value) != nil || identity(value) != core.ID(record.RecordID) {
		return fmt.Errorf("contains an invalid %s projection", kind)
	}
	id := identity(value)
	previous, found := history[id]
	var prior *T
	if found {
		prior = &previous.Value
		if record.Version != previous.Version+1 || correlationStable && record.CorrelationID != previous.CorrelationID {
			return fmt.Errorf("contains noncontiguous %s history", kind)
		}
	}
	if err := validate(event.EventType, record.Version, prior, value); err != nil {
		return err
	}
	history[id] = Versioned[T]{Version: record.Version, CorrelationID: record.CorrelationID, Value: value}
	return nil
}

func admitLifecycleProjection[T any](event events.Event, record events.ProjectionRecord, kind string, history, target map[core.ID]Versioned[T], identity func(T) core.ID, validateTransition func(string, int, *T, T) error, validateAdmission func(T) error, validRevision func(T, T) bool) error {
	var value T
	if decodeExactProjectionJSON(record.Value, &value) != nil {
		return fmt.Errorf("contains an invalid %s projection", kind)
	}
	if err := validateProjectionEventLifecycle(event, record, kind, history, identity, true, validateTransition); err != nil {
		return err
	}
	if err := validateAdmission(value); err != nil {
		return err
	}
	return core.AdmitDurableRevision(target, identity(value), record.Version, record.CorrelationID, value, true, validRevision)
}

type projectionRecordIdentity struct {
	kind    string
	id      string
	version int
}

func validateProjectionRecordCoverage(stream []events.Event, records map[string][][]byte) error {
	eventRecords := make(map[projectionRecordIdentity]events.ProjectionRecord)
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if !present {
			continue
		}
		key := projectionRecordKey(payload.Projection)
		if _, duplicate := eventRecords[key]; duplicate {
			return fmt.Errorf("projection event stream contains duplicate record %s/%s/%d", key.kind, key.id, key.version)
		}
		eventRecords[key] = payload.Projection
	}

	for kind, bodies := range records {
		for _, body := range bodies {
			var record events.ProjectionRecord
			if err := decodeExactProjectionJSON(body, &record); err != nil {
				return fmt.Errorf("projection record for %s is invalid: %w", kind, err)
			}
			if record.ProjectionKind != kind {
				return fmt.Errorf("projection record %s/%s/%d crosses its kind boundary", record.ProjectionKind, record.RecordID, record.Version)
			}
			key := projectionRecordKey(record)
			eventRecord, admitted := eventRecords[key]
			if !admitted || !reflect.DeepEqual(record, eventRecord) {
				return fmt.Errorf("projection record %s/%s/%d lacks one exact event-coupled admission", key.kind, key.id, key.version)
			}
			delete(eventRecords, key)
		}
	}
	if len(eventRecords) != 0 {
		for key := range eventRecords {
			return fmt.Errorf("projection event %s/%s/%d lacks one exact materialized record", key.kind, key.id, key.version)
		}
	}
	return nil
}

func projectionRecordKey(record events.ProjectionRecord) projectionRecordIdentity {
	return projectionRecordIdentity{kind: record.ProjectionKind, id: record.RecordID, version: record.Version}
}

func validateProjectionEventOrganizationBindings(snapshot Snapshot, stream []events.Event) error {
	for _, event := range stream {
		payload, present, err := events.AdmittedProjection(event)
		if err != nil {
			return fmt.Errorf("event %s: %w", event.EventID, err)
		}
		if !present {
			continue
		}
		var organizationID core.ID
		switch payload.Projection.ProjectionKind {
		case KindOrganization:
			organizationID = core.ID(payload.Projection.RecordID)
		case KindMission:
			organizationID, err = projectionOrganizationID[core.Mission](event, payload.Projection, "Mission", func(value core.Mission) core.ID { return value.OrganizationID })
		case KindGoal:
			organizationID, err = projectionOrganizationID[core.Goal](event, payload.Projection, "Goal", func(value core.Goal) core.ID { return value.OrganizationID })
		case KindTeam:
			organizationID, err = projectionOrganizationID[core.Team](event, payload.Projection, "Team", func(value core.Team) core.ID { return value.OrganizationID })
		case KindAgentBlueprint:
			organizationID, err = projectionOrganizationID[core.AgentBlueprint](event, payload.Projection, "Agent blueprint", func(value core.AgentBlueprint) core.ID { return value.OrganizationID })
		case KindExecutionProfile:
			organizationID, err = projectionOrganizationID[core.ExecutionProfile](event, payload.Projection, "execution profile", func(value core.ExecutionProfile) core.ID { return value.OrganizationID })
		case KindAgent:
			var value core.Agent
			if decodeExactProjectionJSON(payload.Projection.Value, &value) != nil {
				return fmt.Errorf("event %s contains an invalid Agent projection", event.EventID)
			}
			blueprint, blueprintFound := snapshot.AgentBlueprints[value.BlueprintID]
			profile, profileFound := snapshot.ExecutionProfiles[value.ExecutionProfileID]
			if !blueprintFound || !profileFound || !core.ValidAgentConfigurationBinding(value, blueprint.Value, profile.Value) {
				return fmt.Errorf("event %s Agent projection references an invalid pinned configuration", event.EventID)
			}
			organizationID = value.OrganizationID
		case KindIntent:
			organizationID, err = projectionOrganizationID[core.Intent](event, payload.Projection, "Intent", func(value core.Intent) core.ID { return value.OrganizationID })
		case KindWork:
			var value core.Work
			if decodeExactProjectionJSON(payload.Projection.Value, &value) != nil {
				return fmt.Errorf("event %s contains an invalid Work projection", event.EventID)
			}
			intent, found := snapshot.Intents[value.IntentID]
			if !found {
				return fmt.Errorf("event %s Work projection lacks its Intent organization", event.EventID)
			}
			organizationID = intent.Value.OrganizationID
		case KindLabExperiment:
			organizationID, err = projectionOrganizationID[core.Experiment](event, payload.Projection, "Lab experiment", func(value core.Experiment) core.ID { return value.OrganizationID })
		case KindLabPromotionCandidate:
			organizationID, err = projectionOrganizationID[core.PromotionCandidate](event, payload.Projection, "Lab promotion candidate", func(value core.PromotionCandidate) core.ID { return value.OrganizationID })
		case KindTask:
			var value core.Task
			if decodeExactProjectionJSON(payload.Projection.Value, &value) != nil {
				return fmt.Errorf("event %s contains an invalid Task projection", event.EventID)
			}
			work, found := snapshot.Works[value.WorkID]
			if !found {
				return fmt.Errorf("event %s Task projection lacks its Work", event.EventID)
			}
			intent, found := snapshot.Intents[work.Value.IntentID]
			if !found {
				return fmt.Errorf("event %s Task projection lacks its Intent organization", event.EventID)
			}
			organizationID = intent.Value.OrganizationID
		default:
			return fmt.Errorf("event %s contains unsupported projection kind %s", event.EventID, payload.Projection.ProjectionKind)
		}
		if err != nil {
			return err
		}
		if organizationID == "" || event.OrganizationID != string(organizationID) {
			return fmt.Errorf("event %s projection crosses its organization boundary", event.EventID)
		}
	}
	return nil
}

func projectionOrganizationID[T any](event events.Event, record events.ProjectionRecord, label string, organization func(T) core.ID) (core.ID, error) {
	var value T
	if decodeExactProjectionJSON(record.Value, &value) != nil {
		return "", fmt.Errorf("event %s contains an invalid %s projection", event.EventID, label)
	}
	return organization(value), nil
}

func (r *Repository) validateGoalAchievementAdmissions(ctx context.Context, snapshot Snapshot) error {
	for goalID, state := range snapshot.Goals {
		if state.Value.Status != core.GoalAchieved {
			continue
		}
		if err := r.gateway.ValidateGoalAchievement(ctx, string(state.Value.OrganizationID), goalID); err != nil {
			return fmt.Errorf("achieved Goal %s lacks exact durable evidence: %w", goalID, err)
		}
	}
	return nil
}

// validateGoalAchievementAdmissionsFromEvents keeps Rebuild independent of
// the records table. The completed-Work admission audit runs first, so this
// pass may safely index only Work evidence that is bound to an exact current
// terminal projection in the same authoritative stream.
func validateGoalAchievementAdmissionsFromEvents(snapshot Snapshot, stream []events.Event) error {
	type workEvidenceBinding struct {
		Evidence           events.GoalWorkEvidence
		CompletionSequence int64
	}
	workEvidence := make(map[string]workEvidenceBinding)
	experimentalWorks := make(map[core.ID]struct{}, len(snapshot.Experiments))
	for _, experiment := range snapshot.Experiments {
		experimentalWorks[experiment.Value.WorkID] = struct{}{}
	}
	for workID, state := range snapshot.Works {
		if state.Value.Status != core.WorkCompleted || state.Value.GoalID == "" {
			continue
		}
		if _, experimental := experimentalWorks[workID]; experimental {
			continue
		}
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("completed work %s references missing intent", workID)
		}
		expectedRecord, err := exactProjectionRecord(KindWork, workID, state)
		if err != nil {
			return err
		}
		matched, detail, err := exactRuntimeProjectionTransition(stream, "WORK_COMPLETED", string(intent.Value.OrganizationID), state.CorrelationID, expectedRecord)
		if err != nil {
			return fmt.Errorf("completed work %s: %w", workID, err)
		}
		evidenceEvent, found := eventByID(stream, detail.EvidenceEventRef)
		var evidence events.WorkCompletionEvidencePayload
		if !found || evidenceEvent.Sequence >= matched.Sequence || decodeExactProjectionJSON(evidenceEvent.Payload, &evidence) != nil || !evidence.Valid() || evidence.Fingerprint != detail.Fingerprint || evidence.GoalID != state.Value.GoalID {
			return fmt.Errorf("completed work %s lacks exact Goal evidence", workID)
		}
		if _, duplicate := workEvidence[evidenceEvent.EventID]; duplicate {
			return fmt.Errorf("completed Work evidence is reused across Goal bindings")
		}
		workEvidence[evidenceEvent.EventID] = workEvidenceBinding{
			Evidence:           events.GoalWorkEvidence{EventRef: evidenceEvent.EventID, EventAt: evidenceEvent.CreatedAt, Evidence: evidence},
			CompletionSequence: matched.Sequence,
		}
	}

	for goalID, state := range snapshot.Goals {
		if state.Value.Status != core.GoalAchieved {
			continue
		}
		if state.Version < 2 || state.Value.Mode != core.GoalTarget {
			return fmt.Errorf("achieved Goal %s has an invalid terminal projection", goalID)
		}
		expectedRecord, err := exactProjectionRecord(KindGoal, goalID, state)
		if err != nil {
			return err
		}
		transition, transitionDetail, err := exactRuntimeProjectionTransition(stream, "GOAL_ACHIEVED", string(state.Value.OrganizationID), state.CorrelationID, expectedRecord)
		if err != nil {
			return fmt.Errorf("achieved Goal %s: %w", goalID, err)
		}
		prior, priorSequence, err := priorGoalRevisionFromEvents(stream, state, transition.Sequence)
		if err != nil {
			return fmt.Errorf("achieved Goal %s: %w", goalID, err)
		}
		evaluationEvent, found := eventByID(stream, transitionDetail.EvidenceEventRef)
		var evaluation events.GoalProgressEvaluatedPayload
		if !found || priorSequence >= evaluationEvent.Sequence || evaluationEvent.Sequence >= transition.Sequence || evaluationEvent.EventType != "GOAL_PROGRESS_EVALUATED" || evaluationEvent.OrganizationID != string(state.Value.OrganizationID) || !runtimeOwnedProjectionEvent(evaluationEvent, state.CorrelationID) || decodeExactProjectionJSON(evaluationEvent.Payload, &evaluation) != nil || evaluation.Result != events.GoalProgressTargetAchieved || evaluation.Fingerprint != transitionDetail.Fingerprint {
			return fmt.Errorf("achieved Goal %s lacks exact progress evidence", goalID)
		}
		if err := validateActiveMissionAtEvent(stream, string(state.Value.OrganizationID), prior.Value.MissionID, evaluationEvent.Sequence); err != nil {
			return fmt.Errorf("achieved Goal %s lacks its active mission: %w", goalID, err)
		}
		selected := make([]events.GoalWorkEvidence, 0, len(evaluation.WorkEvidenceRefs))
		for _, ref := range evaluation.WorkEvidenceRefs {
			binding, ok := workEvidence[ref]
			if !ok || binding.Evidence.Evidence.GoalID != goalID {
				return fmt.Errorf("achieved Goal %s references missing or cross-Goal Work evidence", goalID)
			}
			if binding.CompletionSequence >= evaluationEvent.Sequence {
				return fmt.Errorf("achieved Goal %s references Work completed after its evaluation", goalID)
			}
			selected = append(selected, binding.Evidence)
		}
		if err := events.ValidateGoalProgressEvaluation(prior.Value, prior.Version, selected, evaluation); err != nil {
			return fmt.Errorf("achieved Goal %s lacks authoritative completed-Work evidence: %w", goalID, err)
		}
	}
	return nil
}

type evidenceTransitionDetail struct {
	EvidenceEventRef string `json:"evidence_event_ref"`
	Fingerprint      string `json:"fingerprint"`
}

func exactProjectionRecord[T any](kind string, id core.ID, state Versioned[T]) (events.ProjectionRecord, error) {
	value, err := json.Marshal(state.Value)
	if err != nil {
		return events.ProjectionRecord{}, err
	}
	return events.ProjectionRecord{ProjectionKind: kind, RecordID: string(id), Version: state.Version, CorrelationID: state.CorrelationID, Value: value}, nil
}

func exactRuntimeProjectionTransition(stream []events.Event, eventType, organizationID, correlationID string, expected events.ProjectionRecord) (events.Event, evidenceTransitionDetail, error) {
	var matched events.Event
	var matchedDetail evidenceTransitionDetail
	for _, event := range stream {
		if event.EventType != eventType || event.OrganizationID != organizationID {
			continue
		}
		var payload events.ProjectionEventPayload
		var detail evidenceTransitionDetail
		if decodeExactProjectionJSON(event.Payload, &payload) != nil || !reflect.DeepEqual(payload.Projection, expected) || decodeExactProjectionJSON(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" || !runtimeOwnedProjectionEvent(event, correlationID) {
			continue
		}
		if matched.EventID != "" {
			return events.Event{}, evidenceTransitionDetail{}, fmt.Errorf("multiple authoritative %s transitions", eventType)
		}
		matched, matchedDetail = event, detail
	}
	if matched.EventID == "" {
		return events.Event{}, evidenceTransitionDetail{}, fmt.Errorf("authoritative %s transition is missing", eventType)
	}
	return matched, matchedDetail, nil
}

func priorGoalRevisionFromEvents(stream []events.Event, achieved Versioned[core.Goal], beforeSequence int64) (Versioned[core.Goal], int64, error) {
	var prior Versioned[core.Goal]
	var priorSequence int64
	for _, event := range stream {
		if event.Sequence >= beforeSequence || event.OrganizationID != string(achieved.Value.OrganizationID) || !runtimeOwnedProjectionEvent(event, achieved.CorrelationID) {
			continue
		}
		var payload events.ProjectionEventPayload
		var goal core.Goal
		if decodeExactProjectionJSON(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != KindGoal || payload.Projection.RecordID != string(achieved.Value.ID) || payload.Projection.Version != achieved.Version-1 || payload.Projection.CorrelationID != achieved.CorrelationID || decodeExactProjectionJSON(payload.Projection.Value, &goal) != nil || !validActiveGoalProjectionEventType(event.EventType, payload.Projection.Version) {
			continue
		}
		if prior.Version != 0 {
			return Versioned[core.Goal]{}, 0, fmt.Errorf("pre-achievement revision has multiple authoritative transitions")
		}
		prior = Versioned[core.Goal]{Version: payload.Projection.Version, CorrelationID: payload.Projection.CorrelationID, Value: goal}
		priorSequence = event.Sequence
	}
	if prior.Version == 0 || prior.Value.Status != core.GoalActive || !core.ValidGoalRevision(prior.Value, achieved.Value) {
		return Versioned[core.Goal]{}, 0, fmt.Errorf("terminal projection does not follow one exact active revision")
	}
	return prior, priorSequence, nil
}

func validActiveGoalProjectionEventType(eventType string, version int) bool {
	if version == 1 {
		return eventType == "GOAL_CREATED"
	}
	return eventType == "GOAL_REFINED" || eventType == "GOAL_RESUMED"
}

func validateActiveMissionAtEvent(stream []events.Event, organizationID string, missionID core.ID, evaluationSequence int64) error {
	var matched events.Event
	var record events.ProjectionRecord
	for _, event := range stream {
		if event.Sequence < 1 || event.OrganizationID != organizationID {
			continue
		}
		var payload events.ProjectionEventPayload
		if decodeExactProjectionJSON(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != KindMission || payload.Projection.RecordID != string(missionID) {
			continue
		}
		if event.Sequence == evaluationSequence {
			return fmt.Errorf("mission transition collides with the Goal evaluation boundary")
		}
		if event.Sequence > evaluationSequence {
			continue
		}
		if matched.EventID != "" && event.Sequence == matched.Sequence {
			return fmt.Errorf("mission evaluation boundary is ambiguous")
		}
		if matched.EventID == "" || event.Sequence > matched.Sequence {
			matched = event
			record = payload.Projection
		}
	}
	var mission core.Mission
	if matched.EventID == "" || record.Version < 1 || record.CorrelationID == "" || decodeExactProjectionJSON(record.Value, &mission) != nil ||
		mission.ID != missionID || string(mission.OrganizationID) != organizationID || mission.Status != core.MissionActive || !core.ValidMission(mission) ||
		matched.EventType != activeMissionEventType(record.Version) || !runtimeOwnedProjectionEvent(matched, record.CorrelationID) {
		return fmt.Errorf("mission was not active at the Goal evaluation")
	}
	return nil
}

func activeMissionEventType(version int) string {
	if version == 1 {
		return "MISSION_CREATED"
	}
	return "MISSION_REVISED"
}

func runtimeOwnedProjectionEvent(event events.Event, correlationID string) bool {
	return event.SourceActorID == "runtime" && event.SourceExecutionID == "" && event.RecipientScope == "" && event.RecipientID == "" && event.TaskID == "" && len(event.AuthorizationRefs) == 0 && len(event.ArtifactRefs) == 0 && event.CorrelationID == correlationID && event.SchemaVersion == events.SchemaVersion
}

func eventByID(stream []events.Event, eventID string) (events.Event, bool) {
	for _, event := range stream {
		if event.EventID == eventID {
			return event, true
		}
	}
	return events.Event{}, false
}

func decodeExactProjectionJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func validateWorkCompletionAdmissions(snapshot Snapshot, stream []events.Event, teamRecords [][]byte, inboxObservations map[string]events.InboxObservationBinding) error {
	for workID, state := range snapshot.Works {
		if state.Value.Status != core.WorkCompleted {
			continue
		}
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("completed work %s references missing intent", workID)
		}
		var transition events.Event
		var transitionDetail events.WorkCompletionTransitionPayload
		for _, event := range stream {
			if event.EventType != "WORK_COMPLETED" {
				continue
			}
			var payload events.ProjectionEventPayload
			var projected core.Work
			var detail events.WorkCompletionTransitionPayload
			if event.OrganizationID != string(intent.Value.OrganizationID) || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.TaskID != "" || event.CorrelationID != state.CorrelationID ||
				json.Unmarshal(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != KindWork || payload.Projection.RecordID != string(workID) || payload.Projection.Version != state.Version || payload.Projection.CorrelationID != state.CorrelationID ||
				json.Unmarshal(payload.Projection.Value, &projected) != nil || !reflect.DeepEqual(projected, state.Value) || json.Unmarshal(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" {
				continue
			}
			if transition.EventID != "" {
				return fmt.Errorf("completed work %s has multiple authoritative transitions", workID)
			}
			transition, transitionDetail = event, detail
		}
		if transition.EventID == "" {
			return fmt.Errorf("completed work %s lacks an authoritative transition", workID)
		}
		var evidenceEvent events.Event
		for _, event := range stream {
			if event.EventID == transitionDetail.EvidenceEventRef {
				evidenceEvent = event
				break
			}
		}
		if evidenceEvent.EventID == "" || evidenceEvent.Sequence >= transition.Sequence {
			return fmt.Errorf("completed work %s lacks exact durable evidence", workID)
		}
		tasks := make([]events.WorkCompletionTaskBinding, 0)
		for _, task := range snapshot.Tasks {
			if task.Value.WorkID == workID {
				tasks = append(tasks, events.WorkCompletionTaskBinding{Task: task.Value, Version: task.Version, CorrelationID: task.CorrelationID})
			}
		}
		profiles := make(map[core.ID]core.ExecutionProfile, len(snapshot.ExecutionProfiles))
		for profileID, profile := range snapshot.ExecutionProfiles {
			profiles[profileID] = profile.Value
		}
		blueprints := make(map[core.ID]core.AgentBlueprint, len(snapshot.AgentBlueprints))
		for blueprintID, blueprint := range snapshot.AgentBlueprints {
			blueprints[blueprintID] = blueprint.Value
		}
		teamRevisions, err := events.ResolveTeamRevisionBindings(string(intent.Value.OrganizationID), teamRecords, stream)
		if err != nil {
			return fmt.Errorf("completed work %s has invalid Team history: %w", workID, err)
		}
		binding := events.WorkCompletionBinding{
			OrganizationID: string(intent.Value.OrganizationID), CorrelationID: state.CorrelationID,
			Work: state.Value, WorkVersion: state.Version, Intent: intent.Value, Tasks: tasks,
			TeamRevisions: teamRevisions, InboxObservations: inboxObservations, AgentBlueprints: blueprints, ExecutionProfiles: profiles,
		}
		evidence, err := events.ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, stream)
		if err != nil || evidence.Fingerprint != transitionDetail.Fingerprint {
			return fmt.Errorf("completed work %s lacks exact durable evidence", workID)
		}
	}
	return nil
}

func (r *Repository) loadFromRecords(ctx context.Context) (Snapshot, error) {
	records, err := r.readProjectionRecords(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return decodeSnapshot(records)
}

func (r *Repository) readProjectionRecords(ctx context.Context) (map[string][][]byte, error) {
	records := make(map[string][][]byte, len(projectionKinds))
	for _, kind := range projectionKinds {
		rows, err := r.gateway.ProjectionRecords(ctx, kind, "")
		if err != nil {
			return nil, err
		}
		records[kind] = rows
	}
	return records, nil
}

func decodeSnapshot(records map[string][][]byte) (Snapshot, error) {
	snapshot := Snapshot{
		Organizations:       make(map[core.ID]Versioned[core.Organization]),
		Missions:            make(map[core.ID]Versioned[core.Mission]),
		Goals:               make(map[core.ID]Versioned[core.Goal]),
		Teams:               make(map[core.ID]Versioned[core.Team]),
		AgentBlueprints:     make(map[core.ID]Versioned[core.AgentBlueprint]),
		ExecutionProfiles:   make(map[core.ID]Versioned[core.ExecutionProfile]),
		Agents:              make(map[core.ID]Versioned[core.Agent]),
		Intents:             make(map[core.ID]Versioned[core.Intent]),
		Works:               make(map[core.ID]Versioned[core.Work]),
		Tasks:               make(map[core.ID]Versioned[core.Task]),
		Experiments:         make(map[core.ID]Versioned[core.Experiment]),
		PromotionCandidates: make(map[core.ID]Versioned[core.PromotionCandidate]),
	}
	if err := decodeKind(records[KindOrganization], snapshot.Organizations, false, nil); err != nil {
		return Snapshot{}, fmt.Errorf("decode organizations: %w", err)
	}
	if err := decodeKind(records[KindMission], snapshot.Missions, false, sameMissionRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode missions: %w", err)
	}
	if err := decodeKind(records[KindGoal], snapshot.Goals, false, sameGoalRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode goals: %w", err)
	}
	if err := decodeKind(records[KindTeam], snapshot.Teams, false, sameTeamRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode teams: %w", err)
	}
	if err := decodeKind(records[KindAgentBlueprint], snapshot.AgentBlueprints, false, sameAgentBlueprintRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode Agent blueprints: %w", err)
	}
	if err := decodeKind(records[KindExecutionProfile], snapshot.ExecutionProfiles, false, sameExecutionProfileRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode execution profiles: %w", err)
	}
	if err := decodeKind(records[KindAgent], snapshot.Agents, false, sameAgentRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode agents: %w", err)
	}
	if err := decodeKind(records[KindIntent], snapshot.Intents, true, nil); err != nil {
		return Snapshot{}, fmt.Errorf("decode intents: %w", err)
	}
	if err := decodeKind(records[KindWork], snapshot.Works, true, sameWorkRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode works: %w", err)
	}
	if err := decodeKind(records[KindTask], snapshot.Tasks, true, sameTaskRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode tasks: %w", err)
	}
	if err := decodeKind(records[KindLabExperiment], snapshot.Experiments, true, sameExperimentRecord); err != nil {
		return Snapshot{}, fmt.Errorf("decode Lab experiments: %w", err)
	}
	if err := decodeKind(records[KindLabPromotionCandidate], snapshot.PromotionCandidates, true, nil); err != nil {
		return Snapshot{}, fmt.Errorf("decode Lab promotion candidates: %w", err)
	}
	if err := ValidateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func decodeKind[T any](bodies [][]byte, target map[core.ID]Versioned[T], correlationStable bool, sameRecordConfiguration func(T, T) bool) error {
	for _, body := range bodies {
		var record events.ProjectionRecord
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		id := core.ID(record.RecordID)
		var value T
		if err := json.Unmarshal(record.Value, &value); err != nil {
			return err
		}
		if err := core.AdmitDurableRevision(target, id, record.Version, record.CorrelationID, value, correlationStable, sameRecordConfiguration); err != nil {
			return err
		}
	}
	return nil
}

func sameAgentBlueprintRecord(left, right core.AgentBlueprint) bool {
	return core.ValidAgentBlueprintRevision(left, right)
}

func sameTeamRecord(left, right core.Team) bool {
	return core.ValidTeamRevision(left, right)
}

func sameMissionRecord(left, right core.Mission) bool {
	return core.ValidMissionRevision(left, right)
}

func sameGoalRecord(left, right core.Goal) bool {
	return core.ValidGoalRevision(left, right)
}

func sameWorkRecord(left, right core.Work) bool {
	return core.ValidWorkRevision(left, right)
}

func sameTaskRecord(left, right core.Task) bool {
	return core.ValidTaskRevision(left, right)
}

func sameExperimentRecord(left, right core.Experiment) bool {
	return core.ValidExperimentRevision(left, right)
}

func sameExecutionProfileRecord(left, right core.ExecutionProfile) bool {
	return core.ValidExecutionProfileRevision(left, right)
}

func sameAgentRecord(left, right core.Agent) bool {
	return core.ValidAgentRevision(left, right)
}

// ValidateSnapshot applies the complete fail-closed projection graph contract.
// Recovery uses the same core validator so startup certification cannot drift
// from routine materialization as the organizational model evolves.
func ValidateSnapshot(snapshot Snapshot) error {
	return core.ValidateDurableGraph(core.DurableGraph{
		Organizations:       snapshot.Organizations,
		Missions:            snapshot.Missions,
		Goals:               snapshot.Goals,
		Teams:               snapshot.Teams,
		AgentBlueprints:     snapshot.AgentBlueprints,
		ExecutionProfiles:   snapshot.ExecutionProfiles,
		Agents:              snapshot.Agents,
		Intents:             snapshot.Intents,
		Works:               snapshot.Works,
		Tasks:               snapshot.Tasks,
		Experiments:         snapshot.Experiments,
		PromotionCandidates: snapshot.PromotionCandidates,
	})
}
