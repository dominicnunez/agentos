package events

import (
	"encoding/json"
	"fmt"
	"sort"
)

const (
	MaximumIncidentEvidence      = 4096
	MaximumIncidentEvidenceBytes = 32 << 20
)

// ValidateIncidentHistory validates semantic admission using private dependencies.
// Exact database selection, backing projection records and ledger integrity are
// the reader's responsibility. Dependencies never become displayed Work events.
func ValidateIncidentHistory(snapshot IncidentSnapshot) (map[string]int64, error) {
	if err := boundIncidentEvidence(snapshot); err != nil {
		return nil, err
	}
	stream := make([]Event, 0, len(snapshot.Work.Events)+len(snapshot.RelatedEvents)+len(snapshot.DependencyEvents))
	stream = append(stream, snapshot.Work.Events...)
	stream = append(stream, snapshot.RelatedEvents...)
	stream = append(stream, snapshot.DependencyEvents...)
	if snapshot.Work.LedgerEvents > 0 && int64(len(stream)) > snapshot.Work.LedgerEvents {
		return nil, fmt.Errorf("incident dependencies exceed its ledger event count")
	}
	for _, event := range stream {
		if event.OrganizationID != snapshot.Work.OrganizationID {
			return nil, fmt.Errorf("incident dependency crosses its organization")
		}
		if snapshot.Work.LedgerSequence > 0 && event.Sequence > snapshot.Work.LedgerSequence {
			return nil, fmt.Errorf("incident dependency follows its snapshot head")
		}
	}
	for _, event := range snapshot.Work.Events {
		if event.CorrelationID != snapshot.Work.CorrelationID {
			return nil, fmt.Errorf("incident Work history crosses its correlation")
		}
	}
	sort.Slice(stream, func(i, j int) bool { return stream[i].Sequence < stream[j].Sequence })
	authority := append(append([]AuthorityRecord(nil), snapshot.FreezeRecords...), snapshot.AuthorityRecords...)
	leases, freezes, err := ResolveAuthorityAdmissions(stream, authority)
	if err != nil {
		return nil, err
	}
	graph, err := ValidateProjectionHistory(stream, snapshot.InboxObservations, leases, freezes)
	if err != nil {
		return nil, err
	}
	if err := ValidateProjectionCompletions(graph, stream, snapshot.InboxObservations); err != nil {
		return nil, err
	}
	if err := validateIncidentInbox(snapshot, stream); err != nil {
		return nil, err
	}
	tasks := make(map[string]int64)
	for _, event := range snapshot.Work.Events {
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			return nil, err
		}
		if present && payload.Projection.ProjectionKind == "task" && payload.Projection.Version == 1 {
			tasks[payload.Projection.RecordID] = event.Sequence
		}
	}
	return tasks, nil
}

func validateIncidentInbox(snapshot IncidentSnapshot, stream []Event) error {
	var observations []Event
	for _, event := range stream {
		if event.EventType == "INBOX_EVENTS_OBSERVED" {
			observations = append(observations, event)
		}
	}
	if len(observations) != len(snapshot.InboxObservations) {
		return fmt.Errorf("incident inbox backing does not match its observations")
	}
	if len(observations) == 0 {
		return nil
	}
	indexed := make(map[string]Event, len(stream))
	var teams [][]byte
	for _, event := range stream {
		indexed[event.EventID] = event
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			return err
		}
		if present && payload.Projection.ProjectionKind == "team" {
			body, err := json.Marshal(payload.Projection)
			if err != nil {
				return err
			}
			teams = append(teams, body)
		}
	}
	revisions, err := ResolveTeamRevisionBindings(snapshot.Work.OrganizationID, teams, stream)
	if err != nil {
		return err
	}
	binding := WorkCompletionBinding{OrganizationID: snapshot.Work.OrganizationID, TeamRevisions: revisions, InboxObservations: snapshot.InboxObservations}
	observed := make(map[string]struct{})
	for _, event := range observations {
		if err := validateInboxObservation(binding, event, indexed, observed); err != nil {
			return err
		}
	}
	return nil
}

func boundIncidentEvidence(snapshot IncidentSnapshot) error {
	count := len(snapshot.DependencyEvents) + len(snapshot.AuthorityRecords) + len(snapshot.FreezeRecords) + len(snapshot.InboxObservations)
	if count > MaximumIncidentEvidence || len(snapshot.Work.Events)+len(snapshot.RelatedEvents) > 256 {
		return fmt.Errorf("incident supporting evidence exceeds its item bound")
	}
	remaining := MaximumIncidentEvidenceBytes
	for _, stream := range [][]Event{snapshot.Work.Events, snapshot.RelatedEvents, snapshot.DependencyEvents} {
		for _, event := range stream {
			remaining -= len(event.Payload) + len(event.EventID) + len(event.OrganizationID) + len(event.CorrelationID) + len(event.EventType) + len(event.SourceActorID) + len(event.SourceExecutionID) + len(event.RecipientID) + len(event.RecipientScope) + len(event.TaskID)
			for _, refs := range [][]string{event.AuthorizationRefs, event.ArtifactRefs} {
				if len(refs) > MaximumIncidentEvidence {
					return fmt.Errorf("incident evidence has too many references")
				}
				for _, ref := range refs {
					remaining -= len(ref)
				}
			}
			if remaining < 0 {
				return fmt.Errorf("incident evidence exceeds its byte bound")
			}
		}
	}
	for _, records := range [][]AuthorityRecord{snapshot.FreezeRecords, snapshot.AuthorityRecords} {
		for _, record := range records {
			remaining -= len(record.Body) + len(record.Kind) + len(record.RecordID) + len(record.AdmissionEventID)
			if remaining < 0 {
				return fmt.Errorf("incident authority evidence exceeds its byte bound")
			}
		}
	}
	for key, binding := range snapshot.InboxObservations {
		if len(binding.EventIDs) > MaximumIncidentEvidence {
			return fmt.Errorf("incident inbox evidence has too many references")
		}
		remaining -= len(key) + len(binding.ExecutionStartEventRef)
		for _, eventID := range binding.EventIDs {
			remaining -= len(eventID)
		}
		if remaining < 0 {
			return fmt.Errorf("incident inbox evidence exceeds its byte bound")
		}
	}
	return nil
}
