package events

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

// FreezeCursor is an immutable proof that one organization's freeze prefix was
// validated through an exact durable record and Event Contract pair. Its proof
// fields are deliberately opaque outside this package.
type FreezeCursor struct {
	organization string
	version      int
	eventRef     string
	sequence     int64
	priorBody    string
	seenEvents   map[string]struct{}
}

// ResolveFreezeHistory validates a complete single-organization freeze
// history and returns a cursor that can validate later complete suffixes.
func ResolveFreezeHistory(stream []Event, records []AuthorityRecord) (FreezeCursor, []OrganizationFreezeAdmission, error) {
	return (FreezeCursor{}).Append(stream, records)
}

// Append validates a complete suffix without revisiting the cursor's proven
// prefix. It never mutates the receiver, including when validation fails.
func (cursor FreezeCursor) Append(stream []Event, records []AuthorityRecord) (FreezeCursor, []OrganizationFreezeAdmission, error) {
	suffixEvents := make(map[string]struct{})
	for _, event := range stream {
		if event.EventType != "FREEZE_SET" {
			continue
		}
		if event.EventID == "" {
			return FreezeCursor{}, nil, fmt.Errorf("freeze admission event has no identity")
		}
		if _, duplicate := cursor.seenEvents[event.EventID]; duplicate {
			return FreezeCursor{}, nil, fmt.Errorf("freeze admission event %s duplicates the validated prefix", event.EventID)
		}
		if _, duplicate := suffixEvents[event.EventID]; duplicate {
			return FreezeCursor{}, nil, fmt.Errorf("freeze admission event %s is duplicated", event.EventID)
		}
		suffixEvents[event.EventID] = struct{}{}
	}
	if len(suffixEvents) == 0 && len(records) == 0 {
		return cursor, nil, nil
	}

	ordered := append([]AuthorityRecord(nil), records...)
	for _, record := range ordered {
		if record.Kind != authorityKindFreeze || record.AdmissionEventID == "" {
			return FreezeCursor{}, nil, fmt.Errorf("freeze suffix requires exact bound freeze records")
		}
		if cursor.organization != "" && record.RecordID != cursor.organization {
			return FreezeCursor{}, nil, fmt.Errorf("freeze suffix crosses organizations")
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Version < ordered[j].Version })
	seen := cloneFreezeEventSet(cursor.seenEvents)
	index := indexAuthorityEvents(stream)
	next := cursor
	admissions := make([]OrganizationFreezeAdmission, 0, len(ordered))
	for _, record := range ordered {
		event, err := matchAuthorityEvent(index, record, next.organization, next.sequence, seen)
		if err != nil {
			return FreezeCursor{}, nil, err
		}
		advanced, admission, err := appendFreezePair(next, record, event, seen)
		if err != nil {
			return FreezeCursor{}, nil, err
		}
		seen[event.EventID] = struct{}{}
		advanced.seenEvents = seen
		next = advanced
		admissions = append(admissions, admission)
	}
	for _, event := range stream {
		if event.EventType != "FREEZE_SET" {
			continue
		}
		if _, used := seen[event.EventID]; !used {
			return FreezeCursor{}, nil, fmt.Errorf("freeze admission event %s has no exact durable record", event.EventID)
		}
	}
	return next, admissions, nil
}

func appendFreezePair(cursor FreezeCursor, record AuthorityRecord, event Event, usedEvents map[string]struct{}) (FreezeCursor, OrganizationFreezeAdmission, error) {
	var prior AuthorityRecord
	if cursor.version != 0 {
		prior = AuthorityRecord{
			Kind:             authorityKindFreeze,
			RecordID:         cursor.organization,
			Version:          cursor.version,
			Body:             []byte(cursor.priorBody),
			AdmissionEventID: cursor.eventRef,
		}
	}
	if err := ValidateFreezeRecord(record, prior); err != nil {
		return FreezeCursor{}, OrganizationFreezeAdmission{}, err
	}
	if record.AdmissionEventID == "" || record.AdmissionEventID != event.EventID || event.EventID == "" ||
		event.EventType != "FREEZE_SET" || event.OrganizationID != record.RecordID ||
		event.Sequence <= cursor.sequence || event.SourceActorID == "" || event.CreatedAt.IsZero() ||
		event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" ||
		len(event.ArtifactRefs) != 0 || event.SchemaVersion != SchemaVersion || !bytes.Equal(event.Payload, record.Body) {
		return FreezeCursor{}, OrganizationFreezeAdmission{}, fmt.Errorf("freeze record %s/%d lacks exact Event Contract admission", record.RecordID, record.Version)
	}
	if cursor.organization != "" && event.OrganizationID != cursor.organization {
		return FreezeCursor{}, OrganizationFreezeAdmission{}, fmt.Errorf("freeze history crosses organizations")
	}
	if _, duplicate := usedEvents[event.EventID]; duplicate {
		return FreezeCursor{}, OrganizationFreezeAdmission{}, fmt.Errorf("freeze admission event %s is duplicated", event.EventID)
	}
	if err := ValidateStoredAuthorityDraft(TrustedDraft{
		OrganizationID: event.OrganizationID, EventType: event.EventType, SourceActorID: event.SourceActorID,
		SourceExecutionID: event.SourceExecutionID, TaskID: event.TaskID, AuthorizationRefs: event.AuthorizationRefs,
		ArtifactRefs: event.ArtifactRefs, RecipientID: event.RecipientID, RecipientScope: event.RecipientScope,
	}, record.Kind, record.RecordID, record.Version, record.Body); err != nil {
		return FreezeCursor{}, OrganizationFreezeAdmission{}, fmt.Errorf("freeze record %s/%d crosses its exact Event Contract: %w", record.RecordID, record.Version, err)
	}
	var state organizationFreezePayload
	if decodeExactEventJSON(record.Body, &state) != nil {
		return FreezeCursor{}, OrganizationFreezeAdmission{}, fmt.Errorf("freeze record %s/%d is invalid", record.RecordID, record.Version)
	}
	next := FreezeCursor{
		organization: record.RecordID,
		version:      record.Version,
		eventRef:     event.EventID,
		sequence:     event.Sequence,
		priorBody:    string(record.Body),
	}
	admission := OrganizationFreezeAdmission{
		OrganizationID: state.OrganizationID,
		EventRef:       event.EventID,
		Frozen:         state.Frozen,
		Sequence:       event.Sequence,
		Version:        record.Version,
		Control:        state.Control,
	}
	return next, admission, nil
}

func cloneFreezeEventSet(source map[string]struct{}) map[string]struct{} {
	cloned := make(map[string]struct{}, len(source))
	for eventID := range source {
		cloned[eventID] = struct{}{}
	}
	return cloned
}

// ValidateFreezeRecord verifies an exact predecessor rather than trusting a
// command's claimed event identity. Legacy metadata may form a prefix only.
func ValidateFreezeRecord(record, prior AuthorityRecord) error {
	if record.Kind != authorityKindFreeze {
		return fmt.Errorf("freeze record kind is invalid")
	}
	if err := validateAuthorityRecordTransition(record.Kind, record.RecordID, record.Version, record.Body, prior.Body, false); err != nil {
		return err
	}
	var state, before core.FreezeState
	if decodeExactEventJSON(record.Body, &state) != nil {
		return fmt.Errorf("freeze record is invalid")
	}
	if record.Version > 1 {
		if prior.Kind != record.Kind || prior.RecordID != record.RecordID || prior.Version != record.Version-1 ||
			prior.AdmissionEventID == "" || decodeExactEventJSON(prior.Body, &before) != nil {
			return fmt.Errorf("freeze predecessor is invalid")
		}
	} else if prior.Version != 0 || prior.AdmissionEventID != "" {
		return fmt.Errorf("initial freeze has a predecessor")
	}
	if state.Control == nil {
		if before.Control != nil {
			return fmt.Errorf("freeze control evidence cannot be removed")
		}
		return nil
	}
	control := state.Control
	if control.ActorID == "" || control.ActorKind != core.PrincipalHuman ||
		control.PriorVersion != record.Version-1 || control.PriorEventRef != prior.AdmissionEventID {
		return fmt.Errorf("freeze control does not bind its exact predecessor")
	}
	if !state.Frozen && (record.Version == 1 || !before.Frozen) {
		return fmt.Errorf("release requires a current hold")
	}
	return nil
}
