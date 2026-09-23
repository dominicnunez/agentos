package events

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/core"
)

// ValidateProjectionCompletions validates the final durable graph and all Work
// and Goal completion evidence. Call ValidateProjectionHistory first with the
// complete supporting history; this function does not establish record coverage
// or ledger integrity. Inbox observations must be backed by the same snapshot.
func ValidateProjectionCompletions(graph core.DurableGraph, stream []Event, inboxObservations map[string]InboxObservationBinding) error {
	if err := core.ValidateDurableGraph(graph); err != nil {
		return err
	}
	var teamRecords [][]byte
	for _, event := range stream {
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			return err
		}
		if !present || payload.Projection.ProjectionKind != "team" {
			continue
		}
		body, err := json.Marshal(payload.Projection)
		if err != nil {
			return err
		}
		teamRecords = append(teamRecords, body)
	}
	if err := ValidateWorkCompletions(graph, stream, teamRecords, inboxObservations); err != nil {
		return err
	}
	return ValidateGoalCompletions(graph, stream, teamRecords, inboxObservations)
}

// ValidateGoalCompletions keeps Rebuild independent of
// the records table. The completed-Work admission audit runs first, so this
// pass may safely index only Work evidence that is bound to an exact current
// terminal projection in the same authoritative stream.
func ValidateGoalCompletions(snapshot core.DurableGraph, stream []Event, teamRecords [][]byte, inboxObservations map[string]InboxObservationBinding) error {
	type workEvidenceBinding struct {
		Evidence           GoalWorkEvidence
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
		expectedRecord, err := exactProjectionRecord("work", workID, state)
		if err != nil {
			return err
		}
		matched, detail, err := exactRuntimeProjectionTransition(stream, "WORK_COMPLETED", string(intent.Value.OrganizationID), state.CorrelationID, expectedRecord)
		if err != nil {
			return fmt.Errorf("completed work %s: %w", workID, err)
		}
		evidenceEvent, found := eventByID(stream, detail.EvidenceEventRef)
		var evidence WorkCompletionEvidencePayload
		if !found || evidenceEvent.Sequence >= matched.Sequence || boundaryjson.Unmarshal(evidenceEvent.Payload, &evidence) != nil || !evidence.Valid() || evidence.Fingerprint != detail.Fingerprint || evidence.GoalID != state.Value.GoalID {
			return fmt.Errorf("completed work %s lacks exact Goal evidence", workID)
		}
		if _, duplicate := workEvidence[evidenceEvent.EventID]; duplicate {
			return fmt.Errorf("completed Work evidence is reused across Goal bindings")
		}
		workEvidence[evidenceEvent.EventID] = workEvidenceBinding{
			Evidence:           GoalWorkEvidence{EventRef: evidenceEvent.EventID, EventAt: evidenceEvent.CreatedAt, Evidence: evidence},
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
		expectedRecord, err := exactProjectionRecord("goal", goalID, state)
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
		var evaluation GoalProgressEvaluatedPayload
		if !found || priorSequence >= evaluationEvent.Sequence || evaluationEvent.Sequence >= transition.Sequence || evaluationEvent.EventType != "GOAL_PROGRESS_EVALUATED" || evaluationEvent.OrganizationID != string(state.Value.OrganizationID) || !runtimeOwnedProjectionEvent(evaluationEvent, state.CorrelationID) || boundaryjson.Unmarshal(evaluationEvent.Payload, &evaluation) != nil || evaluation.Result != GoalProgressTargetAchieved || evaluation.Fingerprint != transitionDetail.Fingerprint {
			return fmt.Errorf("achieved Goal %s lacks exact progress evidence", goalID)
		}
		if err := validateActiveMissionAtEvent(stream, string(state.Value.OrganizationID), prior.Value.MissionID, evaluationEvent.Sequence); err != nil {
			return fmt.Errorf("achieved Goal %s lacks its active mission: %w", goalID, err)
		}
		selected := make([]GoalWorkEvidence, 0, len(evaluation.WorkEvidenceRefs))
		workUseSequences := make(map[core.ID]int64, len(evaluation.WorkEvidenceRefs))
		for _, ref := range evaluation.WorkEvidenceRefs {
			binding, ok := workEvidence[ref]
			if !ok || binding.Evidence.Evidence.GoalID != goalID {
				return fmt.Errorf("achieved Goal %s references missing or cross-Goal Work evidence", goalID)
			}
			if binding.CompletionSequence >= evaluationEvent.Sequence {
				return fmt.Errorf("achieved Goal %s references Work completed after its evaluation", goalID)
			}
			selected = append(selected, binding.Evidence)
			workUseSequences[binding.Evidence.Evidence.WorkID] = evaluationEvent.Sequence
		}
		if err := validateWorkCompletionAdmissionsAtUse(snapshot, stream, teamRecords, inboxObservations, workUseSequences); err != nil {
			return fmt.Errorf("achieved Goal %s used invalid Work evidence at evaluation: %w", goalID, err)
		}
		if err := ValidateGoalProgressEvaluation(prior.Value, prior.Version, selected, evaluation); err != nil {
			return fmt.Errorf("achieved Goal %s lacks authoritative completed-Work evidence: %w", goalID, err)
		}
	}
	return nil
}

type evidenceTransitionDetail struct {
	EvidenceEventRef string `json:"evidence_event_ref"`
	Fingerprint      string `json:"fingerprint"`
}

func exactProjectionRecord[T any](kind string, id core.ID, state core.DurableState[T]) (ProjectionRecord, error) {
	value, err := json.Marshal(state.Value)
	if err != nil {
		return ProjectionRecord{}, err
	}
	return ProjectionRecord{ProjectionKind: kind, RecordID: string(id), Version: state.Version, CorrelationID: state.CorrelationID, Value: value}, nil
}

func exactRuntimeProjectionTransition(stream []Event, eventType, organizationID, correlationID string, expected ProjectionRecord) (Event, evidenceTransitionDetail, error) {
	var matched Event
	var matchedDetail evidenceTransitionDetail
	for _, event := range stream {
		if event.EventType != eventType || event.OrganizationID != organizationID {
			continue
		}
		var payload ProjectionEventPayload
		var detail evidenceTransitionDetail
		if boundaryjson.Unmarshal(event.Payload, &payload) != nil || !reflect.DeepEqual(payload.Projection, expected) || boundaryjson.Unmarshal(payload.Detail, &detail) != nil || detail.EvidenceEventRef == "" || detail.Fingerprint == "" || !runtimeOwnedProjectionEvent(event, correlationID) {
			continue
		}
		if matched.EventID != "" {
			return Event{}, evidenceTransitionDetail{}, fmt.Errorf("multiple authoritative %s transitions", eventType)
		}
		matched, matchedDetail = event, detail
	}
	if matched.EventID == "" {
		return Event{}, evidenceTransitionDetail{}, fmt.Errorf("authoritative %s transition is missing", eventType)
	}
	return matched, matchedDetail, nil
}

func priorGoalRevisionFromEvents(stream []Event, achieved core.DurableState[core.Goal], beforeSequence int64) (core.DurableState[core.Goal], int64, error) {
	var prior core.DurableState[core.Goal]
	var priorSequence int64
	for _, event := range stream {
		if event.Sequence >= beforeSequence || event.OrganizationID != string(achieved.Value.OrganizationID) || !runtimeOwnedProjectionEvent(event, achieved.CorrelationID) {
			continue
		}
		var payload ProjectionEventPayload
		var goal core.Goal
		if boundaryjson.Unmarshal(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != "goal" || payload.Projection.RecordID != string(achieved.Value.ID) || payload.Projection.Version != achieved.Version-1 || payload.Projection.CorrelationID != achieved.CorrelationID || boundaryjson.Unmarshal(payload.Projection.Value, &goal) != nil || !validActiveGoalProjectionEventType(event.EventType, payload.Projection.Version) {
			continue
		}
		if prior.Version != 0 {
			return core.DurableState[core.Goal]{}, 0, fmt.Errorf("pre-achievement revision has multiple authoritative transitions")
		}
		prior = core.DurableState[core.Goal]{Version: payload.Projection.Version, CorrelationID: payload.Projection.CorrelationID, Value: goal}
		priorSequence = event.Sequence
	}
	if prior.Version == 0 || prior.Value.Status != core.GoalActive || !core.ValidGoalRevision(prior.Value, achieved.Value) {
		return core.DurableState[core.Goal]{}, 0, fmt.Errorf("terminal projection does not follow one exact active revision")
	}
	return prior, priorSequence, nil
}

func validActiveGoalProjectionEventType(eventType string, version int) bool {
	if version == 1 {
		return eventType == "GOAL_CREATED"
	}
	return eventType == "GOAL_REFINED" || eventType == "GOAL_RESUMED"
}

func validateActiveMissionAtEvent(stream []Event, organizationID string, missionID core.ID, evaluationSequence int64) error {
	var matched Event
	var record ProjectionRecord
	for _, event := range stream {
		if event.Sequence < 1 || event.OrganizationID != organizationID {
			continue
		}
		var payload ProjectionEventPayload
		if boundaryjson.Unmarshal(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != "mission" || payload.Projection.RecordID != string(missionID) {
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
	if matched.EventID == "" || record.Version < 1 || record.CorrelationID == "" || boundaryjson.Unmarshal(record.Value, &mission) != nil ||
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

func runtimeOwnedProjectionEvent(event Event, correlationID string) bool {
	return event.SourceActorID == "runtime" && event.SourceExecutionID == "" && event.RecipientScope == "" && event.RecipientID == "" && event.TaskID == "" && len(event.AuthorizationRefs) == 0 && len(event.ArtifactRefs) == 0 && event.CorrelationID == correlationID && event.SchemaVersion == SchemaVersion
}

func eventByID(stream []Event, eventID string) (Event, bool) {
	for _, event := range stream {
		if event.EventID == eventID {
			return event, true
		}
	}
	return Event{}, false
}

// ValidateWorkCompletions verifies completed Work against exact event evidence
// and the caller's admitted Team revisions and inbox observations.
func ValidateWorkCompletions(snapshot core.DurableGraph, stream []Event, teamRecords [][]byte, inboxObservations map[string]InboxObservationBinding) error {
	return validateWorkCompletionAdmissionsAtUse(snapshot, stream, teamRecords, inboxObservations, nil)
}

func validateWorkCompletionAdmissionsAtUse(snapshot core.DurableGraph, stream []Event, teamRecords [][]byte, inboxObservations map[string]InboxObservationBinding, useSequences map[core.ID]int64) error {
	var startHistory *executionHistory
	for workID, state := range snapshot.Works {
		if useSequences != nil {
			if _, selected := useSequences[workID]; !selected {
				continue
			}
		}
		if state.Value.Status != core.WorkCompleted {
			continue
		}
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("completed work %s references missing intent", workID)
		}
		var transition Event
		var transitionDetail WorkCompletionTransitionPayload
		for _, event := range stream {
			if event.EventType != "WORK_COMPLETED" {
				continue
			}
			var payload ProjectionEventPayload
			var projected core.Work
			var detail WorkCompletionTransitionPayload
			if event.OrganizationID != string(intent.Value.OrganizationID) || event.SourceActorID != "runtime" || event.SourceExecutionID != "" || event.TaskID != "" || event.CorrelationID != state.CorrelationID ||
				json.Unmarshal(event.Payload, &payload) != nil || payload.Projection.ProjectionKind != "work" || payload.Projection.RecordID != string(workID) || payload.Projection.Version != state.Version || payload.Projection.CorrelationID != state.CorrelationID ||
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
		var evidenceEvent Event
		for _, event := range stream {
			if event.EventID == transitionDetail.EvidenceEventRef {
				evidenceEvent = event
				break
			}
		}
		if evidenceEvent.EventID == "" || evidenceEvent.Sequence >= transition.Sequence {
			return fmt.Errorf("completed work %s lacks exact durable evidence", workID)
		}
		tasks := make([]WorkCompletionTaskBinding, 0)
		for _, task := range snapshot.Tasks {
			if task.Value.WorkID == workID {
				tasks = append(tasks, WorkCompletionTaskBinding{Task: task.Value, Version: task.Version, CorrelationID: task.CorrelationID})
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
		teamRevisions, err := ResolveTeamRevisionBindings(string(intent.Value.OrganizationID), teamRecords, stream)
		if err != nil {
			return fmt.Errorf("completed work %s has invalid Team history: %w", workID, err)
		}
		binding := WorkCompletionBinding{
			executionHistory: startHistory,
			OrganizationID:   string(intent.Value.OrganizationID), CorrelationID: state.CorrelationID,
			Work: state.Value, WorkVersion: state.Version, Intent: intent.Value, Tasks: tasks,
			TeamRevisions: teamRevisions, InboxObservations: inboxObservations, AgentBlueprints: blueprints, ExecutionProfiles: profiles,
		}
		if startHistory == nil {
			startHistory = newExecutionHistory(stream)
			binding.executionHistory = startHistory
		}
		binding.CompletionSequence = transition.Sequence
		if useSequences != nil {
			binding.CompletionSequence = useSequences[workID]
			if binding.CompletionSequence <= transition.Sequence {
				return fmt.Errorf("work evidence use must follow completion")
			}
		}
		evidence, err := ValidateWorkCompletionEvidenceChain(binding, evidenceEvent, stream)
		if err != nil || evidence.Fingerprint != transitionDetail.Fingerprint {
			return fmt.Errorf("completed work %s lacks exact durable evidence", workID)
		}
	}
	return nil
}
