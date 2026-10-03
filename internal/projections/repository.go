// Package projections materializes durable organizational and work state from
// versioned Event Contracts. It contains no scheduling or execution policy.
package projections

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
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
	KindKnowledge             = "knowledge"
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
	KindKnowledge,
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
	Knowledge           map[core.ID]Versioned[core.KnowledgeRecord]
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

// SaveStrategyBootstrap atomically creates an optional Organization and its
// first Mission/Goal pair. The authenticated requester is retained only as
// provenance inside runtime-owned projection events.
func (r *Repository) SaveStrategyBootstrap(ctx context.Context, organization *core.Organization, mission core.Mission, goal core.Goal, correlationID string, detail events.StrategyBootstrapDetail) error {
	if r == nil || r.gateway == nil || correlationID == "" || !detail.Valid() || detail.RequestID != correlationID ||
		!core.ValidMission(mission) || mission.Status != core.MissionActive || !core.ValidGoal(goal) || goal.Status != core.GoalActive ||
		mission.OrganizationID != goal.OrganizationID || mission.ID != goal.MissionID {
		return fmt.Errorf("complete strategy bootstrap admission is required")
	}
	drafts := make([]events.ProjectionDraft, 0, 3)
	if organization != nil {
		if organization.ID == "" || organization.ID != mission.OrganizationID || organization.Name == "" || organization.PolicyVersion == "" {
			return fmt.Errorf("strategy bootstrap organization is invalid")
		}
		drafts = append(drafts, events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: string(organization.ID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime",
				CorrelationID: correlationID, Payload: detail,
			},
			ProjectionKind: KindOrganization, RecordID: string(organization.ID), Version: 1, Value: *organization,
		})
	}
	drafts = append(drafts,
		events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: string(mission.OrganizationID), EventType: "MISSION_CREATED", SourceActorID: "runtime",
				CorrelationID: correlationID, Payload: detail,
			},
			ProjectionKind: KindMission, RecordID: string(mission.ID), Version: 1, Value: mission,
		},
		events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: string(goal.OrganizationID), EventType: "GOAL_CREATED", SourceActorID: "runtime",
				CorrelationID: correlationID, Payload: detail,
			},
			ProjectionKind: KindGoal, RecordID: string(goal.ID), Version: 1, Value: goal,
		},
	)
	_, err := r.gateway.PublishProjections(ctx, drafts)
	return err
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
			err = boundaryjson.Unmarshal(encoded, &evidence)
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
		work.Status != core.WorkActive || intent.ReplacesWorkID != "" || work.ReplacesWorkID != "" {
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

// SaveReplacementSubmission atomically admits a freshly reviewed Intent and
// Work whose immutable lineage points to one failed predecessor. It creates no
// Plan, Task, approval, capability, effect permission, artifact, or completion
// inheritance.
func (r *Repository) SaveReplacementSubmission(ctx context.Context, correlationID string, intent core.Intent, work core.Work) error {
	if r == nil || r.gateway == nil || correlationID == "" || intent.ID == "" || work.ID == "" || intent.OrganizationID == "" ||
		work.IntentID != intent.ID || work.ReplacesWorkID == "" || intent.ReplacesWorkID != work.ReplacesWorkID || work.Status != core.WorkActive {
		return fmt.Errorf("complete reviewed replacement Intent and Work are required")
	}
	drafts := []events.ProjectionDraft{
		{
			Event:          events.TrustedDraft{OrganizationID: string(intent.OrganizationID), EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID},
			ProjectionKind: KindIntent, RecordID: string(intent.ID), Version: 1, Value: intent,
		},
		{
			Event:          events.TrustedDraft{OrganizationID: string(intent.OrganizationID), EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID},
			ProjectionKind: KindWork, RecordID: string(work.ID), Version: 1, Value: work,
		},
	}
	_, err := r.gateway.PublishProjections(ctx, drafts)
	return err
}

func (r *Repository) StartAgentExecution(ctx context.Context, organizationID core.ID, correlationID string, version int, value core.Task, mode string, inputEventRefs, strategicEventRefs []string, strategicContextRefs []core.VersionedRef, routes []events.InboxRoute, validate events.ExecutionStartValidator) (events.Event, []events.InboxSelection, error) {
	if r == nil || r.gateway == nil || organizationID == "" || correlationID == "" || value.ID == "" || value.ExecutionKind != core.ExecutionAgent || value.Status != core.TaskRunning || version < 2 {
		return events.Event{}, nil, fmt.Errorf("complete Agent execution-start projection is required")
	}
	return r.gateway.PublishExecutionStart(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: "EXECUTION_STARTED", SourceActorID: "runtime",
			TaskID: string(value.ID), CorrelationID: correlationID, Payload: events.ExecutionStartDetail{Mode: mode, InputEventRefs: inputEventRefs, StrategicEventRefs: strategicEventRefs, StrategicContextRefs: strategicContextRefs},
		},
		ProjectionKind: KindTask, RecordID: string(value.ID), Version: version, Value: value,
	}, routes, validate)
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
	}, nil, nil)
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
	leaseAdmissions, freezeAdmissions, err := projectionKnowledgeAuthorityAdmissions(ctx, r.gateway, stream)
	if err != nil {
		return err
	}
	if err := validateProjectionEventAdmissions(stream, inboxObservations, leaseAdmissions, freezeAdmissions); err != nil {
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
	leaseAdmissions, freezeAdmissions, err := projectionKnowledgeAuthorityAdmissions(ctx, r.gateway, stream)
	if err != nil {
		return Snapshot{}, err
	}
	if err := validateProjectionEventAdmissions(stream, inboxObservations, leaseAdmissions, freezeAdmissions); err != nil {
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
	if err := validateGoalAchievementAdmissionsFromEvents(snapshot, stream, records[KindTeam], inboxObservations); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func projectionKnowledgeAuthorityAdmissions(ctx context.Context, gateway *events.Gateway, stream []events.Event) ([]events.CapabilityLeaseAdmission, []events.OrganizationFreezeAdmission, error) {
	for _, event := range stream {
		if events.RequiresModelStopAdmission(event.EventType) {
			return gateway.KnowledgeAuthorityAdmissions(ctx)
		}
		if event.EventType == "TOOL_OUTCOME_RECORDED" {
			var outcome core.ToolOutcome
			if json.Unmarshal(event.Payload, &outcome) == nil && outcome.ErrorClass == "security_hold" {
				return gateway.KnowledgeAuthorityAdmissions(ctx)
			}
		}
		if events.RequiresAuthorityRecordAdmission(event.EventType) {
			return gateway.KnowledgeAuthorityAdmissions(ctx)
		}
	}
	return nil, nil, nil
}

func validateProjectionEventAdmissions(stream []events.Event, inboxObservations map[string]events.InboxObservationBinding, leaseAdmissions []events.CapabilityLeaseAdmission, freezeAdmissions []events.OrganizationFreezeAdmission) error {
	_, err := events.ValidateProjectionHistory(stream, inboxObservations, leaseAdmissions, freezeAdmissions)
	return err
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
		case KindKnowledge:
			organizationID, err = projectionOrganizationID[core.KnowledgeRecord](event, payload.Projection, "knowledge", func(value core.KnowledgeRecord) core.ID { return value.OrganizationID })
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

func validateGoalAchievementAdmissionsFromEvents(snapshot Snapshot, stream []events.Event, teamRecords [][]byte, inboxObservations map[string]events.InboxObservationBinding) error {
	return events.ValidateGoalCompletions(snapshotGraph(snapshot), stream, teamRecords, inboxObservations)
}

func decodeExactProjectionJSON(data []byte, target any) error {
	return boundaryjson.Unmarshal(data, target)
}

func validateWorkCompletionAdmissions(snapshot Snapshot, stream []events.Event, teamRecords [][]byte, inboxObservations map[string]events.InboxObservationBinding) error {
	return events.ValidateWorkCompletions(snapshotGraph(snapshot), stream, teamRecords, inboxObservations)
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
		Knowledge:           make(map[core.ID]Versioned[core.KnowledgeRecord]),
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
	if err := decodeKnowledgeKind(records[KindKnowledge], snapshot.Knowledge); err != nil {
		return Snapshot{}, fmt.Errorf("decode knowledge: %w", err)
	}
	if err := ValidateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func decodeKnowledgeKind(bodies [][]byte, target map[core.ID]Versioned[core.KnowledgeRecord]) error {
	for _, body := range bodies {
		var record events.ProjectionRecord
		var value core.KnowledgeRecord
		if decodeExactProjectionJSON(body, &record) != nil || decodeExactProjectionJSON(record.Value, &value) != nil ||
			record.ProjectionKind != KindKnowledge || record.RecordID != string(value.KnowledgeID) ||
			record.Version != value.Version || !core.ValidKnowledgeRecord(value) {
			return fmt.Errorf("knowledge record is invalid")
		}
		if err := core.AdmitDurableRevision(target, value.KnowledgeID, record.Version, record.CorrelationID, value, true, core.ValidKnowledgeRevision); err != nil {
			return err
		}
	}
	return nil
}

func decodeKind[T any](bodies [][]byte, target map[core.ID]Versioned[T], correlationStable bool, sameRecordConfiguration func(T, T) bool) error {
	for _, body := range bodies {
		var record events.ProjectionRecord
		if err := boundaryjson.Unmarshal(body, &record); err != nil {
			return err
		}
		id := core.ID(record.RecordID)
		var value T
		if err := boundaryjson.Unmarshal(record.Value, &value); err != nil {
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
	return core.ValidateDurableGraph(snapshotGraph(snapshot))
}

func snapshotGraph(snapshot Snapshot) core.DurableGraph {
	return core.DurableGraph{
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
		Knowledge:           snapshot.Knowledge,
	}
}
