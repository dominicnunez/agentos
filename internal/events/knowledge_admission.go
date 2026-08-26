package events

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/dominicnunez/agentos/internal/core"
)

type knowledgeAdmissionVersion struct {
	version       int
	correlationID string
	value         core.KnowledgeRecord
}

// KnowledgeAdmissionValidator is the shared stateful validator used by live
// materialization and offline recovery. Knowledge remains context, never
// authority; every lifecycle revision must be reconstructable from events.
type KnowledgeAdmissionValidator struct {
	events             map[string]Event
	history            map[core.ID]knowledgeAdmissionVersion
	revisions          map[core.ID]map[int]core.KnowledgeRecord
	admissionSequences map[core.ID]int64
}

func NewKnowledgeAdmissionValidator(stream []Event) *KnowledgeAdmissionValidator {
	index := make(map[string]Event, len(stream))
	for _, event := range stream {
		index[event.EventID] = event
	}
	return &KnowledgeAdmissionValidator{
		events:             index,
		history:            make(map[core.ID]knowledgeAdmissionVersion),
		revisions:          make(map[core.ID]map[int]core.KnowledgeRecord),
		admissionSequences: make(map[core.ID]int64),
	}
}

func (v *KnowledgeAdmissionValidator) Validate(value core.KnowledgeRecord, event Event, record ProjectionRecord, graph core.DurableGraph) error {
	if v == nil || !core.ValidKnowledgeRecord(value) || value.KnowledgeID != core.ID(record.RecordID) || value.Version != record.Version {
		return fmt.Errorf("contains an invalid knowledge projection")
	}
	organization, found := graph.Organizations[value.OrganizationID]
	if value.OrganizationID == "" || !found || organization.Value.ID != value.OrganizationID || event.OrganizationID != string(value.OrganizationID) {
		return fmt.Errorf("knowledge requires its durable parent Organization at admission")
	}
	if record.CorrelationID != "knowledge-"+record.RecordID {
		return fmt.Errorf("knowledge correlation is not deterministic")
	}
	switch value.Scope {
	case core.KnowledgeScopeOrganization:
		if value.ScopeID != value.OrganizationID {
			return fmt.Errorf("knowledge crosses its Organization scope")
		}
	case core.KnowledgeScopeAgent:
		agent, found := graph.Agents[value.ScopeID]
		if !found || agent.Value.OrganizationID != value.OrganizationID {
			return fmt.Errorf("knowledge references an invalid Agent scope")
		}
	case core.KnowledgeScopeTeam:
		team, found := graph.Teams[value.ScopeID]
		if !found || team.Value.OrganizationID != value.OrganizationID {
			return fmt.Errorf("knowledge references an invalid Team scope")
		}
	default:
		return fmt.Errorf("knowledge has an unsupported scope")
	}
	evidenceArtifacts := make(map[string]struct{})
	for _, refs := range [][]string{value.ProvenanceEventRefs, value.OccurrenceEventRefs, value.ValidationRefs} {
		for _, ref := range refs {
			evidence, found := v.events[ref]
			if !found || evidence.Sequence >= event.Sequence || evidence.OrganizationID != event.OrganizationID {
				return fmt.Errorf("knowledge references unavailable, future, or cross-organization evidence")
			}
			for _, artifactRef := range evidence.ArtifactRefs {
				evidenceArtifacts[artifactRef] = struct{}{}
			}
		}
	}
	for _, artifactRef := range value.EvidenceArtifactRefs {
		if _, evidenced := evidenceArtifacts[artifactRef]; !evidenced {
			return fmt.Errorf("knowledge artifact is absent from its referenced evidence events")
		}
	}
	if !v.hasAuthenticatedCreator(value) {
		return fmt.Errorf("knowledge creator kind is not bound to authenticated provenance")
	}
	for _, ref := range value.DerivedKnowledgeRefs {
		version, err := strconv.Atoi(ref.Version)
		derived, found := v.revisions[core.ID(ref.ID)][version]
		current, currentFound := v.history[core.ID(ref.ID)]
		if err != nil || version < 1 || strconv.Itoa(version) != ref.Version || ref.ID == record.RecordID || !found ||
			derived.OrganizationID != value.OrganizationID || derived.Status != core.KnowledgeActive || !currentFound ||
			current.version != version || current.value.Status != core.KnowledgeActive {
			return fmt.Errorf("knowledge derived reference lacks an exact prior active revision")
		}
	}
	if value.CreatedAt.After(event.CreatedAt) || value.LastVerifiedAt != nil && value.LastVerifiedAt.After(event.CreatedAt) {
		return fmt.Errorf("knowledge timestamps postdate their admitting event")
	}
	previous, found := v.history[value.KnowledgeID]
	if found {
		if record.Version != previous.version+1 || record.CorrelationID != previous.correlationID {
			return fmt.Errorf("knowledge history is noncontiguous")
		}
		if err := core.ValidateKnowledgeTransition(event.EventType, previous.value, value); err != nil {
			return err
		}
	} else if record.Version != 1 || value.Status != core.KnowledgeCandidate {
		return fmt.Errorf("knowledge history does not begin with a candidate")
	}
	if event.EventType == "KNOWLEDGE_ACTIVATED" && governedKnowledgeValidator(value.ValidatedByKind) {
		proposalSequence := v.admissionSequences[value.KnowledgeID]
		if proposalSequence < 1 || !v.hasAuthorizedJudgment(value, proposalSequence) {
			return fmt.Errorf("knowledge activation lacks an authenticated validator admission")
		}
	}
	if event.EventType == "KNOWLEDGE_ACTIVATED" && value.Basis == core.KnowledgeBasisRepeatedPattern {
		proposalSequence := v.admissionSequences[value.KnowledgeID]
		if proposalSequence < 1 || !v.hasSubsequentValidation(value.OccurrenceEventRefs, value.ValidationRefs, proposalSequence) {
			return fmt.Errorf("repeated-pattern activation lacks validation evidence admitted after the proposal")
		}
	}
	v.history[value.KnowledgeID] = knowledgeAdmissionVersion{version: record.Version, correlationID: record.CorrelationID, value: value}
	v.admissionSequences[value.KnowledgeID] = event.Sequence
	if v.revisions[value.KnowledgeID] == nil {
		v.revisions[value.KnowledgeID] = make(map[int]core.KnowledgeRecord)
	}
	v.revisions[value.KnowledgeID][record.Version] = value
	return nil
}

func (v *KnowledgeAdmissionValidator) hasAuthenticatedCreator(value core.KnowledgeRecord) bool {
	if value.CreatedByKind == core.PrincipalRuntime {
		return value.CreatedBy == "runtime"
	}
	for _, ref := range value.ProvenanceEventRefs {
		evidence, found := v.events[ref]
		if found && ValidKnowledgeCreatorEvidence(evidence, value) {
			return true
		}
	}
	return false
}

// ValidKnowledgeCreatorEvidence binds a claimed non-runtime creator kind to an
// authenticated boundary event. Internal Agents and A2A actors are distinct.
func ValidKnowledgeCreatorEvidence(event Event, value core.KnowledgeRecord) bool {
	if event.OrganizationID != string(value.OrganizationID) || event.SourceActorID != string(value.CreatedBy) {
		return false
	}
	if value.CreatedByKind == core.PrincipalAgent {
		return event.EventType == "KNOWLEDGE_PROPOSED" && event.SourceExecutionID != ""
	}
	expectedChannel := "HUMAN_DIRECT"
	if value.CreatedByKind == core.PrincipalExternalAgent {
		expectedChannel = "A2A"
	} else if value.CreatedByKind != core.PrincipalHuman {
		return false
	}
	switch event.EventType {
	case "INTAKE_MESSAGE_RECORDED":
		var payload IntakeMessageRecordedPayload
		return decodeExactPayload(event.Payload, &payload) == nil && payload.SourcePrincipalID == string(value.CreatedBy) &&
			payload.SourcePrincipalKind == string(value.CreatedByKind) && payload.SourceChannel == expectedChannel
	case "HUMAN_INPUT_RECEIVED", "A2A_INPUT_RECEIVED":
		var payload OperatorInputReceivedPayload
		return decodeExactPayload(event.Payload, &payload) == nil && payload.SourcePrincipalID == string(value.CreatedBy) &&
			payload.SourcePrincipalKind == string(value.CreatedByKind) && payload.SourceChannel == expectedChannel
	default:
		return false
	}
}

func governedKnowledgeValidator(kind core.PrincipalKind) bool {
	return kind == core.PrincipalHuman || kind == core.PrincipalAgent || kind == core.PrincipalExternalAgent
}

func (v *KnowledgeAdmissionValidator) hasAuthorizedJudgment(value core.KnowledgeRecord, proposalSequence int64) bool {
	for _, ref := range value.ValidationRefs {
		judgment, found := v.events[ref]
		if !found || judgment.Sequence <= proposalSequence || judgment.EventType != "CAPABILITY_CHECKED" ||
			judgment.OrganizationID != string(value.OrganizationID) || judgment.SourceActorID != string(value.ValidatedBy) ||
			judgment.RecipientScope != "" || judgment.RecipientID != "" || judgment.TaskID == "" ||
			len(judgment.AuthorizationRefs) == 0 || len(judgment.ArtifactRefs) != 0 || judgment.SchemaVersion != SchemaVersion {
			continue
		}
		var trace core.AuthorizationTrace
		if decodeExactPayload(judgment.Payload, &trace) != nil || !trace.Allowed || trace.LeaseID == "" ||
			trace.ActorID != value.ValidatedBy || trace.ActorKind != value.ValidatedByKind || trace.TaskID != core.ID(judgment.TaskID) ||
			trace.Action != "knowledge.validate" || trace.Resource != string(value.KnowledgeID) || trace.Scope != string(value.OrganizationID) ||
			!slices.Contains(judgment.AuthorizationRefs, string(trace.LeaseID)) {
			continue
		}
		return true
	}
	return false
}

func (v *KnowledgeAdmissionValidator) hasSubsequentValidation(occurrences, validation []string, proposalSequence int64) bool {
	seen := make(map[string]struct{}, len(occurrences))
	for _, ref := range occurrences {
		seen[ref] = struct{}{}
	}
	for _, ref := range validation {
		evidence, found := v.events[ref]
		if _, repeated := seen[ref]; !repeated && found && evidence.Sequence > proposalSequence {
			return true
		}
	}
	return false
}
