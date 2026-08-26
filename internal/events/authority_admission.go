package events

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

// KnowledgeAuthorityRecord is a non-projection authority record read from the
// same durable snapshot as its Event Contracts.
type KnowledgeAuthorityRecord struct {
	Kind     string
	RecordID string
	Version  int
	Body     []byte
}

type knowledgeAuthorityEventKey struct {
	eventType string
	payload   string
}

// ResolveKnowledgeAuthorityAdmissions admits only capability and freeze
// history with a one-to-one, ordered, exact durable record binding. Bare event
// labels never become replay authority.
func ResolveKnowledgeAuthorityAdmissions(stream []Event, records []KnowledgeAuthorityRecord) ([]CapabilityLeaseAdmission, []OrganizationFreezeAdmission, error) {
	ordered := append([]KnowledgeAuthorityRecord(nil), records...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Kind != ordered[j].Kind {
			return ordered[i].Kind < ordered[j].Kind
		}
		if ordered[i].RecordID != ordered[j].RecordID {
			return ordered[i].RecordID < ordered[j].RecordID
		}
		return ordered[i].Version < ordered[j].Version
	})

	leaseVersions := make(map[core.ID]int)
	leaseSequences := make(map[core.ID]int64)
	freezeVersions := make(map[core.ID]int)
	freezeSequences := make(map[core.ID]int64)
	usedEvents := make(map[string]struct{})
	eventIndex := make(map[knowledgeAuthorityEventKey][]Event)
	for _, event := range stream {
		if RequiresRecordAdmission(event.EventType) {
			key := knowledgeAuthorityEventKey{eventType: event.EventType, payload: string(event.Payload)}
			eventIndex[key] = append(eventIndex[key], event)
		}
	}
	leases := make([]CapabilityLeaseAdmission, 0)
	freezes := make([]OrganizationFreezeAdmission, 0)
	for _, record := range ordered {
		switch record.Kind {
		case "capability_lease":
			admission, eventID, err := resolveCapabilityLeaseAdmission(eventIndex, record, leaseVersions, leaseSequences, usedEvents)
			if err != nil {
				return nil, nil, err
			}
			leaseVersions[admission.Lease.ID] = record.Version
			leaseSequences[admission.Lease.ID] = admission.Sequence
			usedEvents[eventID] = struct{}{}
			leases = append(leases, admission)
		case "organization_freeze":
			admission, eventID, err := resolveOrganizationFreezeAdmission(eventIndex, record, freezeVersions, freezeSequences, usedEvents)
			if err != nil {
				return nil, nil, err
			}
			freezeVersions[admission.OrganizationID] = record.Version
			freezeSequences[admission.OrganizationID] = admission.Sequence
			usedEvents[eventID] = struct{}{}
			freezes = append(freezes, admission)
		default:
			return nil, nil, fmt.Errorf("unsupported knowledge authority record kind %q", record.Kind)
		}
	}
	for _, event := range stream {
		if !RequiresRecordAdmission(event.EventType) {
			continue
		}
		if _, found := usedEvents[event.EventID]; !found {
			return nil, nil, fmt.Errorf("authority admission event %s has no exact durable record", event.EventID)
		}
	}
	return leases, freezes, nil
}

func resolveCapabilityLeaseAdmission(eventIndex map[knowledgeAuthorityEventKey][]Event, record KnowledgeAuthorityRecord, versions map[core.ID]int, sequences map[core.ID]int64, used map[string]struct{}) (CapabilityLeaseAdmission, string, error) {
	var lease core.CapabilityLease
	if decodeExactEventJSON(record.Body, &lease) != nil || lease.ID == "" || string(lease.ID) != record.RecordID || record.Version != versions[lease.ID]+1 {
		return CapabilityLeaseAdmission{}, "", fmt.Errorf("capability lease %s/%d has invalid or noncontiguous state", record.RecordID, record.Version)
	}
	expectedType := "CAPABILITY_GRANTED"
	if record.Version == 1 && lease.RevokedAt != nil {
		return CapabilityLeaseAdmission{}, "", fmt.Errorf("capability lease %s starts revoked", record.RecordID)
	}
	if record.Version > 1 {
		expectedType = "CAPABILITY_REVOKED"
		if lease.RevokedAt == nil {
			return CapabilityLeaseAdmission{}, "", fmt.Errorf("capability lease %s/%d lacks revocation time", record.RecordID, record.Version)
		}
	}
	var matched Event
	for _, event := range eventIndex[knowledgeAuthorityEventKey{eventType: expectedType, payload: string(record.Body)}] {
		_, alreadyUsed := used[event.EventID]
		if alreadyUsed || event.EventType != expectedType || event.OrganizationID == "" || event.TaskID != string(lease.OriginTaskID) ||
			event.Sequence <= sequences[lease.ID] || event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" ||
			len(event.ArtifactRefs) != 0 || event.SchemaVersion != SchemaVersion || !bytes.Equal(event.Payload, record.Body) {
			continue
		}
		if matched.EventID != "" {
			return CapabilityLeaseAdmission{}, "", fmt.Errorf("capability lease %s/%d has ambiguous Event Contract admission", record.RecordID, record.Version)
		}
		matched = event
	}
	if matched.EventID == "" {
		return CapabilityLeaseAdmission{}, "", fmt.Errorf("capability lease %s/%d lacks exact Event Contract admission", record.RecordID, record.Version)
	}
	return CapabilityLeaseAdmission{Lease: lease, OrganizationID: core.ID(matched.OrganizationID), Sequence: matched.Sequence}, matched.EventID, nil
}

func resolveOrganizationFreezeAdmission(eventIndex map[knowledgeAuthorityEventKey][]Event, record KnowledgeAuthorityRecord, versions map[core.ID]int, sequences map[core.ID]int64, used map[string]struct{}) (OrganizationFreezeAdmission, string, error) {
	var state organizationFreezePayload
	if decodeExactEventJSON(record.Body, &state) != nil || state.OrganizationID == "" || string(state.OrganizationID) != record.RecordID ||
		record.Version != versions[state.OrganizationID]+1 || state.UpdatedAt.IsZero() {
		return OrganizationFreezeAdmission{}, "", fmt.Errorf("organization freeze %s/%d has invalid or noncontiguous state", record.RecordID, record.Version)
	}
	var matched Event
	for _, event := range eventIndex[knowledgeAuthorityEventKey{eventType: "FREEZE_SET", payload: string(record.Body)}] {
		_, alreadyUsed := used[event.EventID]
		if alreadyUsed || event.EventType != "FREEZE_SET" || event.OrganizationID != record.RecordID || event.Sequence <= sequences[state.OrganizationID] ||
			event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || len(event.ArtifactRefs) != 0 ||
			event.SchemaVersion != SchemaVersion || !bytes.Equal(event.Payload, record.Body) {
			continue
		}
		if matched.EventID != "" {
			return OrganizationFreezeAdmission{}, "", fmt.Errorf("organization freeze %s/%d has ambiguous Event Contract admission", record.RecordID, record.Version)
		}
		matched = event
	}
	if matched.EventID == "" {
		return OrganizationFreezeAdmission{}, "", fmt.Errorf("organization freeze %s/%d lacks exact Event Contract admission", record.RecordID, record.Version)
	}
	return OrganizationFreezeAdmission{OrganizationID: state.OrganizationID, Frozen: state.Frozen, Sequence: matched.Sequence}, matched.EventID, nil
}
