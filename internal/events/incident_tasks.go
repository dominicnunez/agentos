package events

import (
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
)

// IncidentTaskAdmissions derives Task identities and their first admission
// sequence from sealed Work/Task history. Ordinary Task mentions grant no link.
// Exact backing records and snapshot integrity remain the reader's responsibility.
func IncidentTaskAdmissions(stream []Event) (map[string]int64, error) {
	works := map[core.ID]core.Work{}
	workScopes := map[core.ID][2]string{}
	priorTasks := map[string]core.Task{}
	versions := map[[2]string]int{}
	tasks := map[string]int64{}
	var lastSequence int64
	for _, event := range stream {
		if event.Sequence <= lastSequence {
			return nil, fmt.Errorf("incident history is unordered")
		}
		lastSequence = event.Sequence
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			return nil, err
		}
		if !present {
			if RequiresProjectionAdmission(event.EventType, event.SourceActorID) {
				return nil, fmt.Errorf("incident lifecycle lacks projection admission")
			}
			continue
		}
		if err := ValidateProjectionEventBoundary(event, payload); err != nil {
			return nil, err
		}
		projection := payload.Projection
		if projection.ProjectionKind != "work" && projection.ProjectionKind != "task" {
			continue
		}
		key := [2]string{projection.ProjectionKind, projection.RecordID}
		if projection.Version != versions[key]+1 {
			return nil, fmt.Errorf("incident projection history is incomplete")
		}
		versions[key] = projection.Version
		if projection.ProjectionKind == "work" {
			var work core.Work
			if decodeExactEventJSON(projection.Value, &work) != nil || string(work.ID) != projection.RecordID {
				return nil, fmt.Errorf("incident Work identity is invalid")
			}
			var prior *core.Work
			if value, ok := works[work.ID]; ok {
				prior = &value
				if workScopes[work.ID] != [2]string{event.OrganizationID, event.CorrelationID} {
					return nil, fmt.Errorf("incident Work changes scope")
				}
			}
			if err := ValidateWorkProjectionTransition(event.EventType, projection.Version, prior, work); err != nil {
				return nil, err
			}
			works[work.ID] = work
			workScopes[work.ID] = [2]string{event.OrganizationID, event.CorrelationID}
			continue
		}
		var task core.Task
		if decodeExactEventJSON(projection.Value, &task) != nil || string(task.ID) != projection.RecordID || works[task.WorkID].ID == "" || workScopes[task.WorkID] != [2]string{event.OrganizationID, event.CorrelationID} {
			return nil, fmt.Errorf("incident Task lacks its exact prior Work")
		}
		var prior *core.Task
		if value, ok := priorTasks[projection.RecordID]; ok {
			prior = &value
		}
		if err := ValidateTaskProjectionTransition(event.EventType, projection.Version, prior, task); err != nil {
			return nil, err
		}
		priorTasks[projection.RecordID] = task
		if projection.Version == 1 {
			tasks[projection.RecordID] = event.Sequence
		}
	}
	return tasks, nil
}
