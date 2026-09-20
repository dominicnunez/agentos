package events

import (
	"fmt"
	"sort"
)

// IncidentAdmissions derives every classified boundary and rejects admissions
// after a hold, execution stop, or durable Task suspension.
func IncidentAdmissions(work, related []Event, freezes []OrganizationFreezeAdmission) ([]IncidentAdmission, error) {
	stream := append(append([]Event(nil), work...), related...)
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	suspended := map[string]bool{}
	stopped := map[string]bool{}
	var admissions []IncidentAdmission
	for _, event := range stream {
		if event.EventType == "TASK_EXECUTION_SUSPENDED" {
			suspended[event.TaskID] = true
		}
		if event.EventType == "EXECUTION_STOP_REQUESTED" || event.EventType == "MODEL_STOP_REQUESTED" {
			stopped[event.SourceExecutionID] = true
		}
		admission, present, err := AdmissionForIncident(event)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		frozen := false
		for _, freeze := range freezes {
			if freeze.Sequence < event.Sequence {
				frozen = freeze.Frozen
			}
		}
		if frozen || stopped[admission.ExecutionID] || suspended[event.TaskID] {
			return nil, fmt.Errorf("incident admission follows containment boundary")
		}
		admissions = append(admissions, admission)
	}
	return admissions, nil
}
