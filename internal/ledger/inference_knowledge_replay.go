package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

type inferenceExecutionHistory struct {
	eventsByID       map[string]events.Event
	correlations     map[string][]events.Event
	taskHistory      map[string][]events.Event
	workTaskHistory  map[core.ID][]events.Event
	addressed        map[[2]string][]events.Event
	knowledge        *events.ExecutionKnowledgeReplay
	manifests        map[string][]events.Event
	tasks            map[string]currentProjectionAdmission[core.Task]
	taskEvents       map[string]events.Event
	teamBodies       [][]byte
	teamEvents       []events.Event
	teamBindings     map[core.ID][]events.TeamRevisionBinding
	teamBindingCount int
	finished         map[[3]string]bool
	projections      map[[2]string][]inferenceProjectionRevision
	workTasks        map[core.ID]map[string]currentProjectionAdmission[core.Task]
}

type inferenceProjectionRevision struct {
	sequence int64
	record   events.ProjectionRecord
	event    events.Event
}

func newInferenceExecutionHistory() *inferenceExecutionHistory {
	return &inferenceExecutionHistory{
		eventsByID:      make(map[string]events.Event),
		correlations:    make(map[string][]events.Event),
		taskHistory:     make(map[string][]events.Event),
		workTaskHistory: make(map[core.ID][]events.Event),
		addressed:       make(map[[2]string][]events.Event),
		manifests:       make(map[string][]events.Event),
		tasks:           make(map[string]currentProjectionAdmission[core.Task]),
		taskEvents:      make(map[string]events.Event),
		finished:        make(map[[3]string]bool),
		projections:     make(map[[2]string][]inferenceProjectionRevision),
		workTasks:       make(map[core.ID]map[string]currentProjectionAdmission[core.Task]),
	}
}

// Observe each event once in ledger order. A reservation sees only the history
// already observed, including the exact Task revision at its admission boundary.
func (h *inferenceExecutionHistory) observe(event events.Event) error {
	if h.knowledge == nil {
		h.knowledge = events.NewExecutionKnowledgeReplay(event.OrganizationID)
	}
	h.eventsByID[event.EventID] = event
	h.correlations[event.CorrelationID] = append(h.correlations[event.CorrelationID], event)
	if event.TaskID != "" {
		h.taskHistory[event.TaskID] = append(h.taskHistory[event.TaskID], event)
	}
	if event.RecipientScope != "" {
		key := [2]string{event.RecipientScope, event.RecipientID}
		h.addressed[key] = append(h.addressed[key], event)
	}
	if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
		h.manifests[event.SourceExecutionID] = append(h.manifests[event.SourceExecutionID], event)
	}
	if event.EventType == "EXECUTION_FINISHED" {
		h.finished[[3]string{event.SourceExecutionID, event.TaskID, event.CorrelationID}] = true
	}
	projection, present, err := events.AdmittedProjection(event)
	if err != nil || !present {
		return err
	}
	record := projection.Projection
	switch record.ProjectionKind {
	case "work", "intent", "agent", "agent_blueprint", "execution_profile", "mission", "goal":
		key := [2]string{record.ProjectionKind, record.RecordID}
		h.projections[key] = append(h.projections[key], inferenceProjectionRevision{sequence: event.Sequence, record: record, event: event})
	}
	switch projection.Projection.ProjectionKind {
	case "knowledge":
		if err := h.knowledge.Observe(event); err != nil {
			return err
		}
	case "team":
		body, err := json.Marshal(projection.Projection)
		if err != nil {
			return err
		}
		h.teamBodies = append(h.teamBodies, body)
		h.teamEvents = append(h.teamEvents, event)
	case "task":
		var task core.Task
		if decodeExactJSONBytes(projection.Projection.Value, &task) != nil {
			return fmt.Errorf("inference Task history is invalid")
		}
		h.tasks[projection.Projection.RecordID] = currentProjectionAdmission[core.Task]{value: task, record: projection.Projection}
		h.taskEvents[projection.Projection.RecordID] = event
		if h.workTasks[task.WorkID] == nil {
			h.workTasks[task.WorkID] = make(map[string]currentProjectionAdmission[core.Task])
		}
		h.workTasks[task.WorkID][record.RecordID] = h.tasks[record.RecordID]
		h.workTaskHistory[task.WorkID] = append(h.workTaskHistory[task.WorkID], event)
		if event.EventType == "EXECUTION_STARTED" && task.ExecutionKind == core.ExecutionAgent {
			teams, err := h.resolveTeams(event.OrganizationID)
			if err != nil {
				return err
			}
			if err := h.knowledge.CaptureStart(event, task, teams); err != nil {
				return err
			}
		}
	}
	return nil
}

// New v5 reservations bind their exact manifest. Historical accounting without
// a v5 execution retains its previous contract; a v5 manifest cannot lose its
// reference or be replayed under a different inference purpose.
func (h *inferenceExecutionHistory) validateReservation(ctx context.Context, tx *sql.Tx, reservation events.Event, payload events.InferenceReservedPayload, inbox *map[string]events.InboxObservationBinding) error {
	var manifest core.ExecutionContextManifest
	var manifestEvent events.Event
	task := h.tasks[reservation.TaskID].value
	taskRecord := h.tasks[reservation.TaskID].record
	taskEvent := h.taskEvents[reservation.TaskID]
	matching := h.manifests[reservation.SourceExecutionID]
	if len(matching) > 1 {
		return fmt.Errorf("inference execution manifest history is invalid")
	}
	if len(matching) == 1 {
		manifestEvent = matching[0]
		if decodeExactJSONBytes(manifestEvent.Payload, &manifest) != nil {
			return fmt.Errorf("inference execution manifest history is invalid")
		}
	}
	if manifestEvent.EventID == "" {
		if payload.ExecutionManifestRef != "" || taskEvent.EventID != "" && payload.Purpose == string(inference.PurposeTaskExecution) {
			return fmt.Errorf("inference reservation lacks its execution manifest")
		}
		return nil
	}
	if manifest.ContextBuilderVersion != "v5" {
		if payload.ExecutionManifestRef != "" {
			return fmt.Errorf("current inference reference targets a historical manifest")
		}
		switch manifest.ContextBuilderVersion {
		case "v1", "v2", "v3", "v4":
			return nil
		}
		return fmt.Errorf("inference manifest version is unsupported")
	}
	if payload.ExecutionManifestRef != manifestEvent.EventID || payload.Purpose != string(inference.PurposeTaskExecution) ||
		payload.RequestID != reservation.SourceExecutionID || manifestEvent.SourceActorID != "runtime" ||
		manifestEvent.TaskID != reservation.TaskID || manifestEvent.CorrelationID != reservation.CorrelationID ||
		manifest.ExecutionID != core.ID(reservation.SourceExecutionID) || manifest.TaskID != task.ID || manifest.AgentID != task.AssigneeID ||
		manifest.ExecutionInputSHA256 != payload.PromptSHA256 || manifest.Provider != payload.Provider || manifest.Model != payload.Model ||
		manifest.ExecutionProfileVersion != payload.ExecutionProfileVersion || taskEvent.EventType != "EXECUTION_STARTED" ||
		task.Status != core.TaskRunning || task.ModelInferencePolicy == core.InferenceForbidden ||
		manifestEvent.Sequence <= taskEvent.Sequence || reservation.SourceExecutionID != fmt.Sprintf("execution-%s-v%d", task.ID, taskRecord.Version) {
		return fmt.Errorf("inference reservation does not bind its exact admitted execution")
	}
	roster, err := h.dispatchRoster(task, taskEvent.Sequence)
	if err != nil {
		return err
	}
	if err := events.ValidateAgentDispatchStart(taskEvent, task, taskRecord.Version, roster); err != nil {
		return err
	}
	if h.finished[[3]string{reservation.SourceExecutionID, reservation.TaskID, reservation.CorrelationID}] {
		return fmt.Errorf("inference reservation follows execution finish")
	}
	teams, err := h.resolveTeams(reservation.OrganizationID)
	if err != nil {
		return err
	}
	if *inbox == nil {
		*inbox, err = inboxObservationBindings(ctx, tx)
		if err != nil {
			return err
		}
	}
	binding, err := h.executionBinding(reservation, task, taskEvent.Sequence, teams, *inbox)
	if err != nil {
		return fmt.Errorf("inference execution configuration is invalid: %w", err)
	}
	stream := h.executionContextStream(reservation.CorrelationID, binding.Work, task, taskEvent.Sequence, teams)
	if _, err := h.knowledge.ValidateManifest(binding, task, reservation.SourceExecutionID, taskEvent, reservation.Sequence, stream); err != nil {
		return fmt.Errorf("inference reservation used invalid execution context: %w", err)
	}
	return nil
}

// Select by durable scope, never by the manifest's claimed references. Keeping
// every applicable candidate lets reconstruction detect omissions and additions.
func (h *inferenceExecutionHistory) executionContextStream(correlationID string, work core.Work, task core.Task, startSequence int64, teams map[core.ID][]events.TeamRevisionBinding) []events.Event {
	selected := make(map[string]events.Event)
	add := func(stream []events.Event) {
		for _, event := range stream {
			selected[event.EventID] = event
		}
	}
	add(h.correlations[correlationID])
	add(h.workTaskHistory[task.WorkID])
	add(h.taskHistory[string(task.ID)])
	for _, dependency := range task.DependsOn {
		add(h.taskHistory[string(dependency)])
	}
	missions := make(map[core.ID]struct{})
	for _, revision := range h.projections[[2]string{"goal", string(work.GoalID)}] {
		selected[revision.event.EventID] = revision.event
		var goal core.Goal
		if json.Unmarshal(revision.record.Value, &goal) == nil {
			missions[goal.MissionID] = struct{}{}
		}
	}
	for missionID := range missions {
		for _, revision := range h.projections[[2]string{"mission", string(missionID)}] {
			selected[revision.event.EventID] = revision.event
		}
	}
	for _, route := range events.AgentExecutionRoutes(teams, task, startSequence) {
		stream := h.addressed[[2]string{route.Scope, route.ID}]
		add(stream)
		for _, event := range stream {
			if event.EventType != "INBOX_EVENTS_OBSERVED" || event.Sequence >= startSequence {
				continue
			}
			var observation events.InboxEventsObservedPayload
			// The shared validator reports malformed observations. Here we only
			// expand references required to verify the consuming execution.
			if json.Unmarshal(event.Payload, &observation) == nil {
				if start, found := h.eventsByID[observation.ExecutionStartEventRef]; found {
					selected[start.EventID] = start
				}
				for _, id := range observation.EventIDs {
					if addressed, found := h.eventsByID[id]; found {
						selected[id] = addressed
					}
				}
			}
		}
	}
	stream := make([]events.Event, 0, len(selected))
	for _, event := range selected {
		stream = append(stream, event)
	}
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	return stream
}

// Dispatch validation needs the latest admitted revision of each pinned roster
// object before start. Older references fail because they are not in this view;
// later revisions cannot retroactively invalidate a previously admitted start.
func (h *inferenceExecutionHistory) dispatchRoster(task core.Task, before int64) ([]events.Event, error) {
	if task.AgentConfig == nil {
		return nil, fmt.Errorf("inference Task configuration is missing")
	}
	keys := [][2]string{{"agent", string(task.AssigneeID)}, {"agent_blueprint", string(task.AgentConfig.BlueprintID)}, {"execution_profile", string(task.AgentConfig.ProfileID)}}
	roster := make([]events.Event, 0, len(keys))
	for _, key := range keys {
		history := h.projections[key]
		index := sort.Search(len(history), func(i int) bool { return history[i].sequence >= before })
		if index == 0 {
			return nil, fmt.Errorf("inference %s roster revision is unavailable", key[0])
		}
		roster = append(roster, history[index-1].event)
	}
	return roster, nil
}

func (h *inferenceExecutionHistory) resolveTeams(organizationID string) (map[core.ID][]events.TeamRevisionBinding, error) {
	if h.teamBindings == nil || h.teamBindingCount != len(h.teamBodies) {
		bindings, err := events.ResolveTeamRevisionBindings(organizationID, h.teamBodies, h.teamEvents)
		if err != nil {
			return nil, err
		}
		h.teamBindings, h.teamBindingCount = bindings, len(h.teamBodies)
	}
	return h.teamBindings, nil
}

func (h *inferenceExecutionHistory) projectionBefore(kind string, id core.ID, sequence int64) (events.ProjectionRecord, error) {
	history := h.projections[[2]string{kind, string(id)}]
	index := sort.Search(len(history), func(i int) bool { return history[i].sequence >= sequence })
	if index == 0 {
		return events.ProjectionRecord{}, fmt.Errorf("inference %s projection is missing before execution", kind)
	}
	return history[index-1].record, nil
}

func (h *inferenceExecutionHistory) executionBinding(reservation events.Event, task core.Task, startSequence int64, teams map[core.ID][]events.TeamRevisionBinding, inbox map[string]events.InboxObservationBinding) (events.WorkCompletionBinding, error) {
	binding := events.WorkCompletionBinding{OrganizationID: reservation.OrganizationID, CorrelationID: reservation.CorrelationID, TeamRevisions: teams, InboxObservations: inbox, AgentBlueprints: make(map[core.ID]core.AgentBlueprint), ExecutionProfiles: make(map[core.ID]core.ExecutionProfile)}
	workRecord, err := h.projectionBefore("work", task.WorkID, startSequence)
	if err != nil {
		return binding, err
	}
	if decodeExactJSONBytes(workRecord.Value, &binding.Work) != nil || binding.Work.ID != task.WorkID || binding.Work.Status != core.WorkActive || workRecord.CorrelationID != reservation.CorrelationID {
		return binding, fmt.Errorf("inference durable Work is invalid")
	}
	binding.WorkVersion = workRecord.Version
	intentRecord, err := h.projectionBefore("intent", binding.Work.IntentID, startSequence)
	if err != nil {
		return binding, err
	}
	if decodeExactJSONBytes(intentRecord.Value, &binding.Intent) != nil || binding.Intent.ID != binding.Work.IntentID || binding.Intent.OrganizationID != core.ID(reservation.OrganizationID) || intentRecord.CorrelationID != reservation.CorrelationID {
		return binding, fmt.Errorf("inference durable Intent is invalid")
	}
	for _, candidate := range h.workTasks[task.WorkID] {
		binding.Tasks = append(binding.Tasks, events.WorkCompletionTaskBinding{Task: candidate.value, Version: candidate.record.Version, CorrelationID: candidate.record.CorrelationID})
	}
	if task.AgentConfig == nil {
		return binding, fmt.Errorf("inference Task configuration is missing")
	}
	config := task.AgentConfig
	blueprint, err := inferenceSemanticRevision[core.AgentBlueprint](h, "agent_blueprint", config.BlueprintID, config.BlueprintVersion, startSequence, func(value core.AgentBlueprint) string { return value.Version })
	if err != nil {
		return binding, err
	}
	profile, err := inferenceSemanticRevision[core.ExecutionProfile](h, "execution_profile", config.ProfileID, config.ProfileVersion, startSequence, func(value core.ExecutionProfile) string { return value.Version })
	if err != nil {
		return binding, err
	}
	binding.AgentBlueprints[config.BlueprintID], binding.ExecutionProfiles[config.ProfileID] = blueprint, profile
	return binding, nil
}

func inferenceSemanticRevision[T any](h *inferenceExecutionHistory, kind string, id core.ID, version string, before int64, semanticVersion func(T) string) (T, error) {
	var selected T
	found := false
	for _, revision := range h.projections[[2]string{kind, string(id)}] {
		if revision.sequence >= before {
			break
		}
		var candidate T
		if decodeExactJSONBytes(revision.record.Value, &candidate) != nil {
			return selected, fmt.Errorf("inference %s revision is invalid", kind)
		}
		if semanticVersion(candidate) != version {
			continue
		}
		selected, found = candidate, true
	}
	if !found {
		return selected, fmt.Errorf("inference %s semantic revision is unavailable", kind)
	}
	return selected, nil
}
