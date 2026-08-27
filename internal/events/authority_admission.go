package events

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

const (
	authorityKindLease  = "capability_lease"
	authorityKindFreeze = "organization_freeze"
)

// AuthorityRecord is a non-projection authority record read from the same
// durable snapshot as its Event Contracts.
type AuthorityRecord struct {
	Kind     string
	RecordID string
	Version  int
	Body     []byte
}

// CapabilityLeaseAdmission binds one exact lease revision to the tenant and
// position of its durable Event Contract.
type CapabilityLeaseAdmission struct {
	Lease          core.CapabilityLease
	OrganizationID core.ID
	Sequence       int64
}

// OrganizationFreezeAdmission binds one exact freeze revision to its durable
// Event Contract.
type OrganizationFreezeAdmission struct {
	OrganizationID core.ID
	Frozen         bool
	Sequence       int64
}

type organizationFreezePayload struct {
	OrganizationID core.ID   `json:"organization_id"`
	Frozen         bool      `json:"frozen"`
	Reason         string    `json:"reason,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// RequiresAuthorityRecordAdmission identifies authority lifecycle labels that
// must be written atomically with a closed durable record. Bare labels never
// become authority.
func RequiresAuthorityRecordAdmission(eventType string) bool {
	switch eventType {
	case "CAPABILITY_GRANTED", "CAPABILITY_REVOKED", "FREEZE_SET":
		return true
	default:
		return false
	}
}

// ValidateAuthorityRecordDraft rejects malformed authority before either the
// Event Contract or its durable state can be committed.
func ValidateAuthorityRecordDraft(draft TrustedDraft, kind, recordID string, version int, body []byte) error {
	if RequiresAuthorityRecordAdmission(draft.EventType) && kind != authorityKindLease && kind != authorityKindFreeze {
		return fmt.Errorf("authority event type does not match its record kind")
	}
	if draft.OrganizationID == "" || draft.SourceActorID == "" || draft.SourceExecutionID != "" ||
		draft.RecipientScope != "" || draft.RecipientID != "" || len(draft.ArtifactRefs) != 0 {
		return fmt.Errorf("authority record crosses its trusted envelope")
	}
	switch kind {
	case authorityKindLease:
		var lease core.CapabilityLease
		if decodeExactEventJSON(body, &lease) != nil || lease.ID == "" || string(lease.ID) != recordID ||
			lease.ActorID == "" || lease.Action == "" || lease.Resource == "" || lease.Scope == "" ||
			lease.OriginTaskID == "" || draft.TaskID != string(lease.OriginTaskID) {
			return fmt.Errorf("capability lease record admission is invalid")
		}
		expectedType := "CAPABILITY_GRANTED"
		if version == 1 {
			if lease.RevokedAt != nil {
				return fmt.Errorf("capability lease cannot start revoked")
			}
		} else {
			expectedType = "CAPABILITY_REVOKED"
			if lease.RevokedAt == nil || lease.RevokedAt.IsZero() {
				return fmt.Errorf("capability revocation requires its timestamp")
			}
		}
		if draft.EventType != expectedType {
			return fmt.Errorf("capability lease event does not match its version")
		}
	case authorityKindFreeze:
		var state organizationFreezePayload
		if decodeExactEventJSON(body, &state) != nil || state.OrganizationID == "" ||
			string(state.OrganizationID) != recordID || state.UpdatedAt.IsZero() ||
			draft.EventType != "FREEZE_SET" || draft.OrganizationID != recordID {
			return fmt.Errorf("organization freeze record admission is invalid")
		}
	default:
		if RequiresAuthorityRecordAdmission(draft.EventType) {
			return fmt.Errorf("authority event requires its exact authority record kind")
		}
	}
	return nil
}

// ValidateAuthorityRecordTransition enforces the complete, closed revision
// history for authority-bearing generic records.
func ValidateAuthorityRecordTransition(kind, recordID string, version int, body, priorBody []byte) error {
	if recordID == "" || version < 1 || version != 1 && len(priorBody) == 0 || version == 1 && len(priorBody) != 0 {
		return fmt.Errorf("authority record version is noncontiguous")
	}
	switch kind {
	case authorityKindLease:
		var current core.CapabilityLease
		if decodeExactEventJSON(body, &current) != nil || current.ID == "" || string(current.ID) != recordID {
			return fmt.Errorf("capability lease state is invalid")
		}
		if version == 1 {
			if current.RevokedAt != nil {
				return fmt.Errorf("capability lease cannot start revoked")
			}
			return nil
		}
		if version != 2 || current.RevokedAt == nil || current.RevokedAt.IsZero() {
			return fmt.Errorf("capability lease permits exactly one terminal revocation")
		}
		var prior core.CapabilityLease
		if decodeExactEventJSON(priorBody, &prior) != nil || prior.RevokedAt != nil ||
			prior.ID != current.ID || prior.ActorID != current.ActorID || prior.Action != current.Action ||
			prior.Resource != current.Resource || prior.Scope != current.Scope ||
			prior.OriginTaskID != current.OriginTaskID || !sameOptionalTime(prior.ExpiresAt, current.ExpiresAt) {
			return fmt.Errorf("capability revocation changes its granted authority")
		}
	case authorityKindFreeze:
		var current organizationFreezePayload
		if decodeExactEventJSON(body, &current) != nil || current.OrganizationID == "" ||
			string(current.OrganizationID) != recordID || current.UpdatedAt.IsZero() {
			return fmt.Errorf("organization freeze state is invalid")
		}
		if version == 1 {
			return nil
		}
		var prior organizationFreezePayload
		if decodeExactEventJSON(priorBody, &prior) != nil || prior.OrganizationID != current.OrganizationID ||
			!current.UpdatedAt.After(prior.UpdatedAt) {
			return fmt.Errorf("organization freeze revision is not a later state for the same tenant")
		}
	default:
		return fmt.Errorf("record kind is not authority-bearing")
	}
	return nil
}

// ResolveAuthorityAdmissions proves a one-to-one, ordered, exact binding
// between authority records and Event Contracts in one durable snapshot.
func ResolveAuthorityAdmissions(stream []Event, records []AuthorityRecord) ([]CapabilityLeaseAdmission, []OrganizationFreezeAdmission, error) {
	ordered := append([]AuthorityRecord(nil), records...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Kind != ordered[j].Kind {
			return ordered[i].Kind < ordered[j].Kind
		}
		if ordered[i].RecordID != ordered[j].RecordID {
			return ordered[i].RecordID < ordered[j].RecordID
		}
		return ordered[i].Version < ordered[j].Version
	})

	usedEvents := make(map[string]struct{})
	eventIndex := indexAuthorityEvents(stream)
	priorBodies := make(map[string][]byte)
	priorSequences := make(map[string]int64)
	organizations := make(map[string]string)
	versions := make(map[string]int)
	leasing := make([]CapabilityLeaseAdmission, 0)
	freezes := make([]OrganizationFreezeAdmission, 0)
	for _, record := range ordered {
		key := record.Kind + "\x00" + record.RecordID
		if record.Version != versions[key]+1 {
			return nil, nil, fmt.Errorf("authority record %s/%s/%d is noncontiguous", record.Kind, record.RecordID, record.Version)
		}
		if err := ValidateAuthorityRecordTransition(record.Kind, record.RecordID, record.Version, record.Body, priorBodies[key]); err != nil {
			return nil, nil, fmt.Errorf("authority record %s/%s/%d: %w", record.Kind, record.RecordID, record.Version, err)
		}
		event, err := matchAuthorityEvent(eventIndex, record, organizations[key], priorSequences[key], usedEvents)
		if err != nil {
			return nil, nil, err
		}
		versions[key] = record.Version
		priorBodies[key] = append([]byte(nil), record.Body...)
		priorSequences[key] = event.Sequence
		organizations[key] = event.OrganizationID
		usedEvents[event.EventID] = struct{}{}
		switch record.Kind {
		case authorityKindLease:
			var lease core.CapabilityLease
			_ = decodeExactEventJSON(record.Body, &lease)
			leasing = append(leasing, CapabilityLeaseAdmission{Lease: lease, OrganizationID: core.ID(event.OrganizationID), Sequence: event.Sequence})
		case authorityKindFreeze:
			var state organizationFreezePayload
			_ = decodeExactEventJSON(record.Body, &state)
			freezes = append(freezes, OrganizationFreezeAdmission{OrganizationID: state.OrganizationID, Frozen: state.Frozen, Sequence: event.Sequence})
		}
	}
	for _, event := range stream {
		if !RequiresAuthorityRecordAdmission(event.EventType) {
			continue
		}
		if _, used := usedEvents[event.EventID]; !used {
			return nil, nil, fmt.Errorf("authority admission event %s has no exact durable record", event.EventID)
		}
	}
	return leasing, freezes, nil
}

type authorityEventIndex map[string][]Event

func indexAuthorityEvents(stream []Event) authorityEventIndex {
	indexed := make(authorityEventIndex)
	for _, event := range stream {
		if !RequiresAuthorityRecordAdmission(event.EventType) {
			continue
		}
		taskID := event.TaskID
		if event.EventType == "FREEZE_SET" {
			taskID = ""
		}
		key := event.EventType + "\x00" + taskID + "\x00" + string(event.Payload)
		indexed[key] = append(indexed[key], event)
	}
	return indexed
}

func matchAuthorityEvent(index authorityEventIndex, record AuthorityRecord, organizationID string, after int64, used map[string]struct{}) (Event, error) {
	expectedType := "FREEZE_SET"
	var lease core.CapabilityLease
	if record.Kind == authorityKindLease {
		if decodeExactEventJSON(record.Body, &lease) != nil {
			return Event{}, fmt.Errorf("capability lease %s/%d is invalid", record.RecordID, record.Version)
		}
		expectedType = "CAPABILITY_GRANTED"
		if record.Version > 1 {
			expectedType = "CAPABILITY_REVOKED"
		}
	}
	taskID := ""
	if record.Kind == authorityKindLease {
		taskID = string(lease.OriginTaskID)
	}
	candidates := index[expectedType+"\x00"+taskID+"\x00"+string(record.Body)]
	var matched Event
	for _, event := range candidates {
		if _, alreadyUsed := used[event.EventID]; alreadyUsed || event.EventType != expectedType ||
			event.EventID == "" || event.Sequence <= after || event.OrganizationID == "" || organizationID != "" && event.OrganizationID != organizationID || event.SourceActorID == "" || event.CreatedAt.IsZero() ||
			event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" ||
			len(event.ArtifactRefs) != 0 || event.SchemaVersion != SchemaVersion || !bytes.Equal(event.Payload, record.Body) {
			continue
		}
		if record.Kind == authorityKindFreeze && event.OrganizationID != record.RecordID {
			continue
		}
		if matched.EventID != "" {
			return Event{}, fmt.Errorf("authority record %s/%s/%d has ambiguous Event Contract admission", record.Kind, record.RecordID, record.Version)
		}
		matched = event
	}
	if matched.EventID == "" {
		return Event{}, fmt.Errorf("authority record %s/%s/%d lacks exact Event Contract admission", record.Kind, record.RecordID, record.Version)
	}
	return matched, nil
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}
