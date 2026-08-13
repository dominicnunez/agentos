// Package projections materializes durable organizational and work state from
// versioned Event Contracts. It contains no scheduling or execution policy.
package projections

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	KindOrganization     = "organization"
	KindMission          = "mission"
	KindGoal             = "goal"
	KindTeam             = "team"
	KindAgentBlueprint   = "agent_blueprint"
	KindExecutionProfile = "execution_profile"
	KindAgent            = "agent"
	KindIntent           = "intent"
	KindWork             = "work"
	KindTask             = "task"
)

type Versioned[T any] struct {
	Version       int
	CorrelationID string
	// Generic instantiations read Value throughout app and projection code, but
	// Gallow cannot currently connect those reads to this generic declaration.
	// gallow-ignore-next-line unused-field
	Value T
}

type Snapshot struct {
	Organizations     map[core.ID]Versioned[core.Organization]
	Missions          map[core.ID]Versioned[core.Mission]
	Goals             map[core.ID]Versioned[core.Goal]
	Teams             map[core.ID]Versioned[core.Team]
	AgentBlueprints   map[core.ID]Versioned[core.AgentBlueprint]
	ExecutionProfiles map[core.ID]Versioned[core.ExecutionProfile]
	Agents            map[core.ID]Versioned[core.Agent]
	Intents           map[core.ID]Versioned[core.Intent]
	Works             map[core.ID]Versioned[core.Work]
	Tasks             map[core.ID]Versioned[core.Task]
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
	return r.save(ctx, string(organizationID), eventType, actorID, string(value.ID), correlationID, KindTask, value.ID, version, value, detail)
}

func (r *Repository) StartAgentExecution(ctx context.Context, organizationID core.ID, correlationID string, version int, value core.Task, mode string, routes []events.InboxRoute) (events.Event, []events.InboxSelection, error) {
	if r == nil || r.gateway == nil || organizationID == "" || correlationID == "" || value.ID == "" || value.ExecutionKind != core.ExecutionAgent || value.Status != core.TaskRunning || version < 2 {
		return events.Event{}, nil, fmt.Errorf("complete Agent execution-start projection is required")
	}
	return r.gateway.PublishExecutionStart(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID), EventType: "EXECUTION_STARTED", SourceActorID: "runtime",
			TaskID: string(value.ID), CorrelationID: correlationID, Payload: events.ExecutionStartDetail{Mode: mode},
		},
		ProjectionKind: KindTask, RecordID: string(value.ID), Version: version, Value: value,
	}, routes)
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
	teamRecords, err := r.gateway.ProjectionRecords(ctx, KindTeam, "")
	if err != nil {
		return err
	}
	inboxObservations, err := r.gateway.InboxObservations(ctx)
	if err != nil {
		return err
	}
	return validateWorkCompletionAdmissions(snapshot, stream, teamRecords, inboxObservations)
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
	records := make(map[string][][]byte)
	for _, event := range stream {
		var payload events.ProjectionEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Projection.ProjectionKind == "" {
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
	inboxObservations, err := r.gateway.InboxObservations(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if err := validateWorkCompletionAdmissions(snapshot, stream, records[KindTeam], inboxObservations); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
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
	records := make(map[string][][]byte)
	for _, kind := range []string{KindOrganization, KindMission, KindGoal, KindTeam, KindAgentBlueprint, KindExecutionProfile, KindAgent, KindIntent, KindWork, KindTask} {
		rows, err := r.gateway.ProjectionRecords(ctx, kind, "")
		if err != nil {
			return Snapshot{}, err
		}
		records[kind] = rows
	}
	return decodeSnapshot(records)
}

func decodeSnapshot(records map[string][][]byte) (Snapshot, error) {
	snapshot := Snapshot{
		Organizations:     make(map[core.ID]Versioned[core.Organization]),
		Missions:          make(map[core.ID]Versioned[core.Mission]),
		Goals:             make(map[core.ID]Versioned[core.Goal]),
		Teams:             make(map[core.ID]Versioned[core.Team]),
		AgentBlueprints:   make(map[core.ID]Versioned[core.AgentBlueprint]),
		ExecutionProfiles: make(map[core.ID]Versioned[core.ExecutionProfile]),
		Agents:            make(map[core.ID]Versioned[core.Agent]),
		Intents:           make(map[core.ID]Versioned[core.Intent]),
		Works:             make(map[core.ID]Versioned[core.Work]),
		Tasks:             make(map[core.ID]Versioned[core.Task]),
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
	if err := decodeKind(records[KindTeam], snapshot.Teams, false, nil); err != nil {
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
	if err := decodeKind(records[KindTask], snapshot.Tasks, true, nil); err != nil {
		return Snapshot{}, fmt.Errorf("decode tasks: %w", err)
	}
	if err := validateSnapshot(snapshot); err != nil {
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
		previous, exists := target[id]
		wantVersion := 1
		if exists {
			wantVersion = previous.Version + 1
		}
		if record.Version != wantVersion {
			return fmt.Errorf("record %s version %d follows %d", id, record.Version, previous.Version)
		}
		if correlationStable {
			if record.CorrelationID == "" {
				return fmt.Errorf("record %s version %d has no correlation boundary", id, record.Version)
			}
			if exists && record.CorrelationID != previous.CorrelationID {
				return fmt.Errorf("record %s changes correlation boundary at version %d", id, record.Version)
			}
		}
		var value T
		if err := json.Unmarshal(record.Value, &value); err != nil {
			return err
		}
		if exists && sameRecordConfiguration != nil && !sameRecordConfiguration(previous.Value, value) {
			return fmt.Errorf("record %s changes immutable configuration at version %d", id, record.Version)
		}
		target[id] = Versioned[T]{Version: record.Version, CorrelationID: record.CorrelationID, Value: value}
	}
	return nil
}

func sameAgentBlueprintRecord(left, right core.AgentBlueprint) bool {
	right.Status = left.Status
	return reflect.DeepEqual(left, right)
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

func validMissionValue(mission core.Mission) bool {
	return core.ValidMission(mission)
}

func validGoalValue(goal core.Goal) bool {
	return core.ValidGoal(goal)
}

func sameExecutionProfileRecord(left, right core.ExecutionProfile) bool {
	right.Status = left.Status
	return reflect.DeepEqual(left, right)
}

func sameAgentRecord(left, right core.Agent) bool {
	return left.ID == right.ID && left.OrganizationID == right.OrganizationID
}

func validateSnapshot(snapshot Snapshot) error {
	for id, state := range snapshot.Organizations {
		if err := validateIdentity("organization", id, state.Value.ID); err != nil {
			return err
		}
	}
	organized := make([]organizedIdentity, 0, len(snapshot.Missions)+len(snapshot.Goals)+len(snapshot.AgentBlueprints)+len(snapshot.ExecutionProfiles)+len(snapshot.Agents)+len(snapshot.Teams)+len(snapshot.Intents))
	for id, state := range snapshot.Missions {
		organized = append(organized, organizedIdentity{"mission", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.Goals {
		organized = append(organized, organizedIdentity{"goal", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.AgentBlueprints {
		organized = append(organized, organizedIdentity{"Agent blueprint", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.ExecutionProfiles {
		organized = append(organized, organizedIdentity{"execution profile", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.Agents {
		organized = append(organized, organizedIdentity{"agent", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.Teams {
		organized = append(organized, organizedIdentity{"team", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.Intents {
		organized = append(organized, organizedIdentity{"intent", id, state.Value.ID, state.Value.OrganizationID})
	}
	for _, record := range organized {
		if err := validateOrganizedIdentity(record.kind, record.recordID, record.valueID, record.organizationID, snapshot.Organizations); err != nil {
			return err
		}
	}
	if err := validateRoster(snapshot); err != nil {
		return err
	}
	for id, state := range snapshot.Missions {
		mission := state.Value
		if !validMissionValue(mission) {
			return fmt.Errorf("mission %s is incomplete or has unsupported status", id)
		}
	}
	for id, state := range snapshot.Goals {
		goal := state.Value
		mission, ok := snapshot.Missions[goal.MissionID]
		if !ok || mission.Value.OrganizationID != goal.OrganizationID {
			return fmt.Errorf("goal %s references invalid mission %s", id, goal.MissionID)
		}
		if !validGoalValue(goal) {
			return fmt.Errorf("goal %s is incomplete or has unsupported mode or status", id)
		}
	}
	for id, state := range snapshot.Teams {
		for _, memberID := range state.Value.MemberAgentIDs {
			member, ok := snapshot.Agents[memberID]
			if !ok || member.Value.OrganizationID != state.Value.OrganizationID {
				return fmt.Errorf("team %s references invalid member agent %s", id, memberID)
			}
		}
	}
	for id, state := range snapshot.Works {
		if err := validateIdentity("work", id, state.Value.ID); err != nil {
			return err
		}
		if state.Value.Status != core.WorkActive && state.Value.Status != core.WorkCompleted && state.Value.Status != core.WorkFailed {
			return fmt.Errorf("work %s has unsupported status %s", id, state.Value.Status)
		}
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("work %s references missing intent %s", id, state.Value.IntentID)
		}
		if state.Value.GoalID != intent.Value.GoalID {
			return fmt.Errorf("work %s does not match its accepted intent goal", id)
		}
		if state.Value.Objective != intent.Value.NormalizedObjective {
			return fmt.Errorf("work %s does not match its accepted intent objective", id)
		}
		if state.CorrelationID == "" || intent.CorrelationID != state.CorrelationID {
			return fmt.Errorf("work %s crosses its intent correlation boundary", id)
		}
		if state.Value.GoalID != "" {
			goal, ok := snapshot.Goals[state.Value.GoalID]
			if !ok || goal.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("work %s references invalid goal %s", id, state.Value.GoalID)
			}
		}
	}
	for id, state := range snapshot.Tasks {
		task := state.Value
		if err := validateIdentity("task", id, task.ID); err != nil {
			return err
		}
		work, ok := snapshot.Works[task.WorkID]
		if !ok {
			return fmt.Errorf("task %s references missing work %s", id, task.WorkID)
		}
		if state.CorrelationID == "" || work.CorrelationID != state.CorrelationID {
			return fmt.Errorf("task %s crosses its work correlation boundary", id)
		}
		intent := snapshot.Intents[work.Value.IntentID]
		switch task.AssigneeType {
		case "":
			if task.AssigneeID != "" || task.AgentConfig != nil {
				return fmt.Errorf("task %s has assignment details without an assignee type", id)
			}
		case "AGENT":
			agent, ok := snapshot.Agents[task.AssigneeID]
			if !ok || agent.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("task %s references invalid assignee agent %s", id, task.AssigneeID)
			}
			if err := validateTaskAgentConfig(id, task.AgentConfig, intent.Value.OrganizationID, snapshot); err != nil {
				return err
			}
		case "TEAM":
			if task.AgentConfig != nil {
				return fmt.Errorf("task %s has Agent configuration for a Team assignment", id)
			}
			team, ok := snapshot.Teams[task.AssigneeID]
			if !ok || team.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("task %s references invalid assignee team %s", id, task.AssigneeID)
			}
		default:
			return fmt.Errorf("task %s has unsupported assignee type %s", id, task.AssigneeType)
		}
		if task.ParentID != "" {
			parent, ok := snapshot.Tasks[task.ParentID]
			if !ok || parent.Value.WorkID != task.WorkID || parent.CorrelationID != state.CorrelationID || task.ParentID == id {
				return fmt.Errorf("task %s references invalid parent %s", id, task.ParentID)
			}
		}
		for _, dependencyID := range task.DependsOn {
			dependency, ok := snapshot.Tasks[dependencyID]
			if !ok || dependency.Value.WorkID != task.WorkID || dependency.CorrelationID != state.CorrelationID || dependencyID == id {
				return fmt.Errorf("task %s references invalid dependency %s", id, dependencyID)
			}
		}
	}
	return nil
}

func validateTaskAgentConfig(taskID core.ID, config *core.AgentConfig, organizationID core.ID, snapshot Snapshot) error {
	if config == nil || config.BlueprintID == "" || config.BlueprintVersion == "" || config.ProfileID == "" || config.ProfileVersion == "" || config.RuntimeAdapter == "" {
		return fmt.Errorf("task %s has incomplete pinned Agent configuration", taskID)
	}
	blueprint, ok := snapshot.AgentBlueprints[config.BlueprintID]
	if !ok || blueprint.Value.OrganizationID != organizationID || blueprint.Value.Version != config.BlueprintVersion {
		return fmt.Errorf("task %s references invalid pinned blueprint %s", taskID, config.BlueprintID)
	}
	profile, ok := snapshot.ExecutionProfiles[config.ProfileID]
	if !ok || profile.Value.OrganizationID != organizationID || profile.Value.Version != config.ProfileVersion {
		return fmt.Errorf("task %s references invalid pinned execution profile %s", taskID, config.ProfileID)
	}
	return nil
}

func validateRoster(snapshot Snapshot) error {
	for id, state := range snapshot.AgentBlueprints {
		blueprint := state.Value
		if blueprint.Version == "" || blueprint.Role == "" || blueprint.OperatingInstructions == "" || !validRosterStatus(blueprint.Status) {
			return fmt.Errorf("agent blueprint %s is incomplete", id)
		}
		if err := validateDistinctStrings("agent blueprint required capability classes", id, blueprint.RequiredCapabilityClasses); err != nil {
			return err
		}
	}
	for id, state := range snapshot.ExecutionProfiles {
		profile := state.Value
		if profile.Version == "" || profile.ModelProvider == "" || profile.Model == "" || profile.PromptVersion == "" || !validRosterStatus(profile.Status) {
			return fmt.Errorf("execution profile %s is incomplete", id)
		}
		if err := validateDistinctStrings("execution profile tool refs", id, profile.ToolRefs); err != nil {
			return err
		}
	}
	for id, state := range snapshot.Agents {
		agent := state.Value
		if agent.BlueprintID == "" || agent.BlueprintVersion == "" || agent.ExecutionProfileID == "" || agent.ExecutionProfileVersion == "" || agent.RuntimeAdapter == "" || !validRosterStatus(agent.Status) {
			return fmt.Errorf("agent %s is incomplete", id)
		}
		blueprint, ok := snapshot.AgentBlueprints[agent.BlueprintID]
		if !ok || blueprint.Value.OrganizationID != agent.OrganizationID || blueprint.Value.Version != agent.BlueprintVersion {
			return fmt.Errorf("agent %s references invalid blueprint %s", id, agent.BlueprintID)
		}
		profile, ok := snapshot.ExecutionProfiles[agent.ExecutionProfileID]
		if !ok || profile.Value.OrganizationID != agent.OrganizationID || profile.Value.Version != agent.ExecutionProfileVersion {
			return fmt.Errorf("agent %s references invalid execution profile %s", id, agent.ExecutionProfileID)
		}
	}
	return nil
}

func validRosterStatus(status string) bool {
	return status == "ACTIVE" || status == "INACTIVE"
}

func validateDistinctStrings(kind string, id core.ID, values []string) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return fmt.Errorf("%s %s contains an empty value", kind, id)
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s %s contains duplicate value %s", kind, id, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

type organizedIdentity struct {
	kind           string
	recordID       core.ID
	valueID        core.ID
	organizationID core.ID
}

func validateIdentity(kind string, recordID, valueID core.ID) error {
	if recordID == "" || valueID != recordID {
		return fmt.Errorf("%s record %s has mismatched identity %s", kind, recordID, valueID)
	}
	return nil
}

func validateOrganizedIdentity(kind string, recordID, valueID, organizationID core.ID, organizations map[core.ID]Versioned[core.Organization]) error {
	if err := validateIdentity(kind, recordID, valueID); err != nil {
		return err
	}
	if _, ok := organizations[organizationID]; !ok {
		return fmt.Errorf("%s %s references missing organization %s", kind, recordID, organizationID)
	}
	return nil
}
