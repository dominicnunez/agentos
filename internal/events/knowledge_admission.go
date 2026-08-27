package events

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	stream             []Event
	events             map[string]Event
	history            map[core.ID]knowledgeAdmissionVersion
	revisions          map[core.ID]map[int]core.KnowledgeRecord
	revisionAdmittedAt map[core.ID]map[int]time.Time
	admissionSequences map[core.ID]int64
	leaseAdmissions    map[core.ID][]CapabilityLeaseAdmission
	freezeAdmissions   map[core.ID][]OrganizationFreezeAdmission
}

func NewKnowledgeAdmissionValidator(stream []Event) *KnowledgeAdmissionValidator {
	index := make(map[string]Event, len(stream))
	for _, event := range stream {
		index[event.EventID] = event
	}
	return &KnowledgeAdmissionValidator{
		stream:             append([]Event(nil), stream...),
		events:             index,
		history:            make(map[core.ID]knowledgeAdmissionVersion),
		revisions:          make(map[core.ID]map[int]core.KnowledgeRecord),
		revisionAdmittedAt: make(map[core.ID]map[int]time.Time),
		admissionSequences: make(map[core.ID]int64),
		leaseAdmissions:    make(map[core.ID][]CapabilityLeaseAdmission),
		freezeAdmissions:   make(map[core.ID][]OrganizationFreezeAdmission),
	}
}

func (v *KnowledgeAdmissionValidator) UseOrganizationFreezeAdmissions(admissions []OrganizationFreezeAdmission) {
	if v == nil {
		return
	}
	v.freezeAdmissions = make(map[core.ID][]OrganizationFreezeAdmission)
	for _, admission := range admissions {
		v.freezeAdmissions[admission.OrganizationID] = append(v.freezeAdmissions[admission.OrganizationID], admission)
	}
}

// UseCapabilityLeaseAdmissions replaces event-derived lease history with the
// exact record-backed admissions verified by offline recovery.
func (v *KnowledgeAdmissionValidator) UseCapabilityLeaseAdmissions(admissions []CapabilityLeaseAdmission) {
	if v == nil {
		return
	}
	v.leaseAdmissions = make(map[core.ID][]CapabilityLeaseAdmission)
	for _, admission := range admissions {
		v.leaseAdmissions[admission.Lease.ID] = append(v.leaseAdmissions[admission.Lease.ID], admission)
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
	var latestSourceAt time.Time
	var latestValidationAt time.Time
	for index, refs := range [][]string{value.ProvenanceEventRefs, value.OccurrenceEventRefs, value.ValidationRefs} {
		for _, ref := range refs {
			evidence, found := v.events[ref]
			if !found || evidence.Sequence >= event.Sequence || evidence.OrganizationID != event.OrganizationID {
				return fmt.Errorf("knowledge references unavailable, future, or cross-organization evidence")
			}
			for _, artifactRef := range evidence.ArtifactRefs {
				evidenceArtifacts[artifactRef] = struct{}{}
			}
			if index == 2 && evidence.CreatedAt.After(latestValidationAt) {
				latestValidationAt = evidence.CreatedAt
			}
			if index < 2 && evidence.CreatedAt.After(latestSourceAt) {
				latestSourceAt = evidence.CreatedAt
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
	requiresCurrentLineage := event.EventType == "KNOWLEDGE_PROPOSED" || event.EventType == "KNOWLEDGE_ACTIVATED"
	for _, ref := range value.DerivedKnowledgeRefs {
		version, err := strconv.Atoi(ref.Version)
		derived, found := v.revisions[core.ID(ref.ID)][version]
		derivedAdmittedAt := v.revisionAdmittedAt[core.ID(ref.ID)][version]
		current, currentFound := v.history[core.ID(ref.ID)]
		if err != nil || version < 1 || strconv.Itoa(version) != ref.Version || ref.ID == record.RecordID || !found ||
			derivedAdmittedAt.IsZero() || derived.OrganizationID != value.OrganizationID || derived.Status != core.KnowledgeActive ||
			requiresCurrentLineage && (!currentFound || current.version != version || current.value.Status != core.KnowledgeActive) {
			return fmt.Errorf("knowledge derived reference lacks an exact prior active revision")
		}
		if !core.KnowledgeDerivedScopeAllowed(derived, value) {
			return fmt.Errorf("knowledge derived scope exceeds its source scope")
		}
		if derivedAdmittedAt.After(latestSourceAt) {
			latestSourceAt = derivedAdmittedAt
		}
	}
	if value.CreatedAt.After(event.CreatedAt) || value.LastVerifiedAt != nil && value.LastVerifiedAt.After(event.CreatedAt) {
		return fmt.Errorf("knowledge timestamps postdate their admitting event")
	}
	if value.CreatedAt.Before(latestSourceAt) {
		return fmt.Errorf("knowledge creation predates its provenance, occurrence, or derived source evidence")
	}
	if value.LastVerifiedAt != nil && value.LastVerifiedAt.Before(latestValidationAt) {
		return fmt.Errorf("knowledge verification predates its validation evidence")
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
	if event.EventType == "KNOWLEDGE_ACTIVATED" {
		proposalSequence := v.admissionSequences[value.KnowledgeID]
		if proposalSequence < 1 || !v.hasPostProposalValidation(value, proposalSequence) {
			return fmt.Errorf("knowledge activation lacks validation evidence admitted after its candidate proposal")
		}
		if governedKnowledgeValidator(value.ValidatedByKind) && !v.hasAuthorizedJudgment(value, proposalSequence, event) {
			return fmt.Errorf("knowledge activation lacks an authenticated validator admission")
		}
	}
	v.history[value.KnowledgeID] = knowledgeAdmissionVersion{version: record.Version, correlationID: record.CorrelationID, value: value}
	v.admissionSequences[value.KnowledgeID] = event.Sequence
	if v.revisions[value.KnowledgeID] == nil {
		v.revisions[value.KnowledgeID] = make(map[int]core.KnowledgeRecord)
		v.revisionAdmittedAt[value.KnowledgeID] = make(map[int]time.Time)
	}
	v.revisions[value.KnowledgeID][record.Version] = value
	v.revisionAdmittedAt[value.KnowledgeID][record.Version] = event.CreatedAt
	return nil
}

func (v *KnowledgeAdmissionValidator) hasAuthenticatedCreator(value core.KnowledgeRecord) bool {
	if value.CreatedByKind == core.PrincipalRuntime {
		return value.CreatedBy == "runtime"
	}
	for _, ref := range value.ProvenanceEventRefs {
		evidence, found := v.events[ref]
		if found && (value.CreatedByKind == core.PrincipalAgent && ValidAgentKnowledgeCreatorEvidence(evidence, value, v.stream) ||
			value.CreatedByKind != core.PrincipalAgent && ValidKnowledgeCreatorEvidence(evidence, value)) {
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
	expectedChannel := "HUMAN_DIRECT"
	if value.CreatedByKind == core.PrincipalExternalAgent {
		expectedChannel = "A2A"
	} else if value.CreatedByKind != core.PrincipalHuman {
		return false
	}
	switch event.EventType {
	case "INTAKE_MESSAGE_RECORDED":
		var payload IntakeMessageRecordedPayload
		return validKnowledgeIntakeCreatorEnvelope(event, &payload) && payload.SourcePrincipalID == string(value.CreatedBy) &&
			payload.SourcePrincipalKind == string(value.CreatedByKind) && payload.SourceChannel == expectedChannel
	case "HUMAN_INPUT_RECEIVED", "A2A_INPUT_RECEIVED":
		payload, err := DecodeDurableOperatorInput(event)
		return err == nil && payload.SourcePrincipalID == string(value.CreatedBy) &&
			payload.SourcePrincipalKind == string(value.CreatedByKind) && payload.SourceChannel == expectedChannel
	default:
		return false
	}
}

func validKnowledgeIntakeCreatorEnvelope(event Event, payload *IntakeMessageRecordedPayload) bool {
	return decodeExactEventJSON(event.Payload, payload) == nil && event.EventID != "" && event.Sequence > 0 && !event.CreatedAt.IsZero() &&
		event.SchemaVersion == SchemaVersion && event.OrganizationID != "" && event.SourceActorID != "" &&
		event.SourceExecutionID == "" && event.RecipientScope == "" && event.RecipientID == "" && event.TaskID != "" &&
		event.CorrelationID != "" && len(event.AuthorizationRefs) == 0 && len(event.ArtifactRefs) == 0 &&
		payload.MessageID != "" && strings.TrimSpace(payload.Text) != "" && utf8.ValidString(payload.Text) &&
		payload.SourcePrincipalID == event.SourceActorID &&
		validReviewedOperatorIdentity(payload.SourcePrincipalID, payload.SourcePrincipalKind, payload.SourceChannel)
}

// ValidAgentKnowledgeCreatorEvidence proves that an internal Agent proposal
// came from the exact durable Agent execution admitted for its Task.
func ValidAgentKnowledgeCreatorEvidence(proposal Event, value core.KnowledgeRecord, stream []Event) bool {
	if value.CreatedByKind != core.PrincipalAgent || proposal.EventType != "KNOWLEDGE_PROPOSED" ||
		proposal.OrganizationID != string(value.OrganizationID) || proposal.SourceActorID != string(value.CreatedBy) ||
		!validAgentKnowledgeProposalPayload(proposal, value) || !ValidAgentExecutionEvidence(proposal, value.CreatedBy, stream) {
		return false
	}
	return true
}

func validAgentKnowledgeProposalPayload(proposal Event, value core.KnowledgeRecord) bool {
	var payload KnowledgeProposedPayload
	return decodeKnowledgeProposedPayload(proposal.Payload, &payload) == nil && payload.ValidFor(proposal.ArtifactRefs) &&
		payload.KnowledgeID != nil && *payload.KnowledgeID == value.KnowledgeID && payload.KnowledgeType == value.Type &&
		payload.Title != nil && *payload.Title == value.Title && payload.Content == value.Content &&
		payload.BasisType != nil && *payload.BasisType == value.Basis && payload.Applicability != nil && *payload.Applicability == value.Applicability &&
		slices.Equal(payload.OccurrenceEventRefs, value.OccurrenceEventRefs) && slices.Equal(payload.EvidenceArtifactRefs, value.EvidenceArtifactRefs)
}

// ValidAgentExecutionEvidence proves that an Agent-authored event came from
// the exact durable Agent execution admitted for its Task while that
// execution was live. The event payload remains untrusted.
func ValidAgentExecutionEvidence(emitted Event, actorID core.ID, stream []Event) bool {
	if actorID == "" || emitted.OrganizationID == "" || emitted.SourceActorID != string(actorID) ||
		emitted.SourceExecutionID == "" || emitted.TaskID == "" || emitted.RecipientScope != "" || emitted.RecipientID != "" ||
		len(emitted.AuthorizationRefs) != 0 || emitted.SchemaVersion != SchemaVersion || emitted.EventID == "" || emitted.Sequence < 1 || emitted.CreatedAt.IsZero() {
		return false
	}
	for _, start := range stream {
		if start.EventType != "EXECUTION_STARTED" || start.Sequence >= emitted.Sequence || start.OrganizationID != emitted.OrganizationID || emitted.CreatedAt.Before(start.CreatedAt) ||
			start.TaskID != emitted.TaskID || start.CorrelationID != emitted.CorrelationID {
			continue
		}
		payload, present, err := AdmittedProjection(start)
		if err != nil || !present || payload.Projection.ProjectionKind != "task" {
			continue
		}
		var task core.Task
		if decodeExactEventJSON(payload.Projection.Value, &task) != nil || task.ID != core.ID(emitted.TaskID) ||
			task.AssigneeID != actorID || task.AssigneeType != "AGENT" || task.ExecutionKind != core.ExecutionAgent {
			continue
		}
		detail, err := executionStartDetail(start)
		if err != nil || detail.DispatchBinding.DispatchID != core.ID(emitted.SourceExecutionID) || detail.DispatchBinding.AgentID != actorID {
			continue
		}
		if ValidateAgentDispatchStart(start, task, payload.Projection.Version, stream) == nil &&
			agentExecutionOpenAtProposal(start, payload.Projection.Version, emitted, stream) {
			return true
		}
	}
	return false
}

func agentExecutionOpenAtProposal(start Event, startVersion int, proposal Event, stream []Event) bool {
	for _, event := range stream {
		if event.Sequence <= start.Sequence || event.Sequence >= proposal.Sequence || event.OrganizationID != start.OrganizationID ||
			event.TaskID != start.TaskID || event.CorrelationID != start.CorrelationID {
			continue
		}
		if event.EventType == "EXECUTION_FINISHED" && event.SourceExecutionID == proposal.SourceExecutionID {
			return false
		}
		payload, present, err := AdmittedProjection(event)
		if err == nil && present && payload.Projection.ProjectionKind == "task" && payload.Projection.RecordID == start.TaskID &&
			payload.Projection.Version > startVersion {
			return false
		}
	}
	return true
}

func governedKnowledgeValidator(kind core.PrincipalKind) bool {
	return kind == core.PrincipalHuman || kind == core.PrincipalAgent || kind == core.PrincipalExternalAgent
}

func (v *KnowledgeAdmissionValidator) hasAuthorizedJudgment(value core.KnowledgeRecord, proposalSequence int64, activation Event) bool {
	foundAuthorization := false
	for _, ref := range value.ValidationRefs {
		judgment, found := v.events[ref]
		if !found || judgment.EventType != "CAPABILITY_CHECKED" {
			continue
		}
		if judgment.Sequence <= proposalSequence ||
			judgment.OrganizationID != string(value.OrganizationID) || judgment.SourceActorID != string(value.ValidatedBy) ||
			judgment.RecipientScope != "" || judgment.RecipientID != "" || judgment.TaskID == "" ||
			len(judgment.AuthorizationRefs) == 0 || len(judgment.ArtifactRefs) != 0 || judgment.SchemaVersion != SchemaVersion {
			return false
		}
		var trace core.AuthorizationTrace
		if decodeExactPayload(judgment.Payload, &trace) != nil || !trace.Allowed || trace.LeaseID == "" ||
			trace.ActorID != value.ValidatedBy || trace.ActorKind != value.ValidatedByKind || trace.TaskID != core.ID(judgment.TaskID) ||
			trace.Action != "knowledge.validate" || trace.Resource != string(value.KnowledgeID) || trace.Scope != string(value.OrganizationID) ||
			len(judgment.AuthorizationRefs) != 1 || judgment.AuthorizationRefs[0] != string(trace.LeaseID) {
			return false
		}
		if !v.leaseAllowsAt(trace, value, judgment, activation) || !v.hasJudgmentStatementAfter(value, proposalSequence, judgment) {
			return false
		}
		foundAuthorization = true
	}
	return foundAuthorization
}

func (v *KnowledgeAdmissionValidator) hasJudgmentStatementAfter(value core.KnowledgeRecord, proposalSequence int64, authorization Event) bool {
	for _, ref := range value.ValidationRefs {
		statement, found := v.events[ref]
		if found && statement.EventType != "CAPABILITY_CHECKED" && statement.Sequence > authorization.Sequence &&
			statement.TaskID == authorization.TaskID && !v.organizationFrozenAt(value.OrganizationID, statement.Sequence) &&
			ValidKnowledgeJudgmentStatement(statement, value, authorization.EventID, v.stream) {
			return true
		}
	}
	return false
}

func (v *KnowledgeAdmissionValidator) leaseAllowsAt(trace core.AuthorizationTrace, value core.KnowledgeRecord, judgment, activation Event) bool {
	if v.organizationFrozenAt(value.OrganizationID, judgment.Sequence) || v.organizationFrozenAt(value.OrganizationID, activation.Sequence) {
		return false
	}
	var atJudgment, atActivation CapabilityLeaseAdmission
	for _, admission := range v.leaseAdmissions[trace.LeaseID] {
		if admission.Sequence < judgment.Sequence && admission.Sequence > atJudgment.Sequence {
			atJudgment = admission
		}
		if admission.Sequence < activation.Sequence && admission.Sequence > atActivation.Sequence {
			atActivation = admission
		}
	}
	lease := atActivation.Lease
	return atJudgment.Sequence > 0 && atJudgment.Sequence == atActivation.Sequence &&
		atActivation.OrganizationID == value.OrganizationID && lease.ID == trace.LeaseID &&
		lease.ActorID == trace.ActorID && lease.ActorKind == trace.ActorKind && lease.OriginTaskID == trace.TaskID &&
		lease.Action == trace.Action && lease.Resource == trace.Resource && lease.Scope == trace.Scope && lease.RevokedAt == nil &&
		(lease.ExpiresAt == nil || (judgment.CreatedAt.Before(*lease.ExpiresAt) && activation.CreatedAt.Before(*lease.ExpiresAt)))
}

func (v *KnowledgeAdmissionValidator) organizationFrozenAt(organizationID core.ID, beforeSequence int64) bool {
	var current OrganizationFreezeAdmission
	for _, admission := range v.freezeAdmissions[organizationID] {
		if admission.Sequence < beforeSequence && admission.Sequence > current.Sequence {
			current = admission
		}
	}
	return current.Sequence > 0 && current.Frozen
}

func (v *KnowledgeAdmissionValidator) hasPostProposalValidation(value core.KnowledgeRecord, proposalSequence int64) bool {
	occurrences := make(map[string]struct{}, len(value.OccurrenceEventRefs))
	executions := make(map[string]struct{}, len(value.ValidationRefs))
	hasStatement := false
	for _, ref := range value.OccurrenceEventRefs {
		occurrences[ref] = struct{}{}
	}
	for _, ref := range value.ValidationRefs {
		evidence, found := v.events[ref]
		if !found || !ValidKnowledgeValidationEvidence(evidence, value, proposalSequence, v.stream) {
			return false
		}
		if value.Basis == core.KnowledgeBasisRepeatedPattern {
			if _, repeated := occurrences[ref]; repeated {
				return false
			}
		}
		if value.ValidationMethod == core.KnowledgeValidationRepeatedObservation {
			if _, duplicate := executions[evidence.SourceExecutionID]; duplicate {
				return false
			}
			executions[evidence.SourceExecutionID] = struct{}{}
		}
		if knowledgeJudgmentMethod(value.ValidationMethod) && evidence.EventType != "CAPABILITY_CHECKED" {
			hasStatement = true
		}
	}
	return len(value.ValidationRefs) > 0 && (!knowledgeJudgmentMethod(value.ValidationMethod) || hasStatement)
}

// ValidKnowledgeValidationEvidence binds each supported validation method to a
// concrete Event Contract and the exact candidate identity. Unsupported
// methods fail closed until they have an equally specific contract.
func ValidKnowledgeValidationEvidence(evidence Event, value core.KnowledgeRecord, proposalSequence int64, stream []Event) bool {
	if evidence.EventID == "" || evidence.Sequence <= proposalSequence || evidence.OrganizationID != string(value.OrganizationID) || evidence.SchemaVersion != SchemaVersion {
		return false
	}
	switch value.ValidationMethod {
	case core.KnowledgeValidationDeterministic:
		return validDeterministicKnowledgeValidation(evidence, value, stream)
	case core.KnowledgeValidationRepeatedObservation:
		var observation EvidencePublishedPayload
		return evidence.EventType == "EVIDENCE_PUBLISHED" && evidence.SourceActorID != "" && evidence.SourceExecutionID != "" && evidence.TaskID != "" &&
			evidence.RecipientScope == "" && evidence.RecipientID == "" && len(evidence.AuthorizationRefs) == 0 &&
			decodeExactEventJSON(evidence.Payload, &observation) == nil && observation.ValidFor(evidence.ArtifactRefs) &&
			knowledgeEvidenceArtifactsBound(evidence.ArtifactRefs, value.EvidenceArtifactRefs) &&
			ValidAgentExecutionEvidence(evidence, core.ID(evidence.SourceActorID), stream)
	case core.KnowledgeValidationHuman:
		return evidence.EventType == "CAPABILITY_CHECKED" || ValidKnowledgeJudgmentStatement(evidence, value, evidenceJudgmentAuthorizationRef(evidence), stream)
	case core.KnowledgeValidationIndependentAgent:
		if evidence.EventType == "CAPABILITY_CHECKED" {
			return true
		}
		return ValidKnowledgeJudgmentStatement(evidence, value, evidenceJudgmentAuthorizationRef(evidence), stream)
	case core.KnowledgeValidationExperimental, core.KnowledgeValidationMixed, core.KnowledgeValidationUnvalidated:
		return false
	default:
		return false
	}
}

func knowledgeJudgmentMethod(method core.KnowledgeValidationMethod) bool {
	return method == core.KnowledgeValidationHuman || method == core.KnowledgeValidationIndependentAgent
}

// ValidKnowledgeJudgmentStatement binds a typed statement to the exact
// candidate, authorizing capability check, principal, and Agent execution.
func ValidKnowledgeJudgmentStatement(evidence Event, value core.KnowledgeRecord, authorizationEventID string, stream []Event) bool {
	if evidence.EventID == "" || evidence.Sequence < 1 || evidence.CreatedAt.IsZero() || evidence.SchemaVersion != SchemaVersion ||
		evidence.OrganizationID != string(value.OrganizationID) || evidence.SourceActorID != string(value.ValidatedBy) ||
		evidence.TaskID == "" || evidence.CorrelationID == "" || evidence.RecipientScope != "" || evidence.RecipientID != "" ||
		len(evidence.AuthorizationRefs) != 0 || authorizationEventID == "" {
		return false
	}
	var judgment KnowledgeJudgmentPayload
	if decodeExactEventJSON(evidence.Payload, &judgment) != nil ||
		!judgment.ValidFor(evidence, value.KnowledgeID, knowledgeCandidateVersion(value), authorizationEventID, evidence.ArtifactRefs) ||
		!knowledgeEvidenceArtifactsBound(evidence.ArtifactRefs, value.EvidenceArtifactRefs) {
		return false
	}
	switch value.ValidatedByKind {
	case core.PrincipalHuman:
		return evidence.EventType == "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED" && evidence.SourceExecutionID == ""
	case core.PrincipalExternalAgent:
		return evidence.EventType == "A2A_KNOWLEDGE_JUDGMENT_RECEIVED" && evidence.SourceExecutionID == ""
	case core.PrincipalAgent:
		return evidence.EventType == "KNOWLEDGE_JUDGMENT_PUBLISHED" && evidence.SourceExecutionID != "" &&
			ValidAgentExecutionEvidence(evidence, value.ValidatedBy, stream)
	case core.PrincipalRuntime:
		return false
	default:
		return false
	}
}

func evidenceJudgmentAuthorizationRef(evidence Event) string {
	var judgment KnowledgeJudgmentPayload
	if decodeExactEventJSON(evidence.Payload, &judgment) != nil {
		return ""
	}
	return judgment.CapabilityCheckEventID
}

func knowledgeCandidateVersion(value core.KnowledgeRecord) int {
	if value.SupersedesVersion != nil {
		return *value.SupersedesVersion
	}
	return value.Version - 1
}

func validDeterministicKnowledgeValidation(validation Event, value core.KnowledgeRecord, stream []Event) bool {
	if validation.EventType != "KNOWLEDGE_VALIDATION_RECORDED" || validation.SourceActorID != "runtime" || validation.SourceExecutionID == "" || validation.TaskID == "" ||
		validation.CorrelationID == "" || validation.RecipientScope != "" || validation.RecipientID != "" || len(validation.AuthorizationRefs) != 0 {
		return false
	}
	var binding KnowledgeDeterministicValidationPayload
	if decodeExactEventJSON(validation.Payload, &binding) != nil ||
		!binding.ValidFor(value.KnowledgeID, knowledgeCandidateVersion(value), validation.ArtifactRefs) ||
		!knowledgeEvidenceArtifactsBound(validation.ArtifactRefs, value.EvidenceArtifactRefs) {
		return false
	}
	outcomeEvent, found := eventWithID(stream, binding.OutcomeEventRef)
	var outcome core.ToolOutcome
	if !found || outcomeEvent.Sequence <= 0 || outcomeEvent.Sequence >= validation.Sequence || outcomeEvent.EventType != "TOOL_OUTCOME_RECORDED" ||
		outcomeEvent.OrganizationID != validation.OrganizationID || outcomeEvent.SourceActorID != "runtime" ||
		outcomeEvent.SourceExecutionID != validation.SourceExecutionID || outcomeEvent.TaskID != validation.TaskID || outcomeEvent.CorrelationID != validation.CorrelationID ||
		outcomeEvent.RecipientScope != "" || outcomeEvent.RecipientID != "" || len(outcomeEvent.AuthorizationRefs) != 0 ||
		decodeExactEventJSON(outcomeEvent.Payload, &outcome) != nil || !outcome.Valid() || outcome.Status != core.OutcomeSucceeded ||
		!slices.Equal(outcomeEvent.ArtifactRefs, outcome.ArtifactRefs) || !slices.Equal(validation.ArtifactRefs, outcome.ArtifactRefs) ||
		outcome.StartedAt.Before(value.CreatedAt) || outcome.FinishedAt.After(outcomeEvent.CreatedAt) {
		return false
	}
	var start Event
	var task core.Task
	var taskVersion int
	for _, candidate := range stream {
		if candidate.EventType != "EXECUTION_STARTED" || candidate.Sequence >= outcomeEvent.Sequence || candidate.OrganizationID != validation.OrganizationID ||
			candidate.TaskID != validation.TaskID || candidate.CorrelationID != validation.CorrelationID {
			continue
		}
		projection, present, err := AdmittedProjection(candidate)
		if err != nil || !present || projection.Projection.ProjectionKind != "task" || projection.Projection.RecordID != validation.TaskID {
			continue
		}
		var projected core.Task
		if decodeExactEventJSON(projection.Projection.Value, &projected) != nil || projected.ID != core.ID(validation.TaskID) ||
			projected.Status != core.TaskRunning || projected.ExecutionKind != core.ExecutionDeterministic ||
			fmt.Sprintf("execution-%s-v%d", projected.ID, projection.Projection.Version) != validation.SourceExecutionID {
			continue
		}
		if _, err := nonAgentExecutionStartDetail(candidate, core.ExecutionDeterministic); err != nil || start.EventID != "" {
			return false
		}
		start, task, taskVersion = candidate, projected, projection.Projection.Version
	}
	if start.EventID == "" || !agentExecutionOpenAtProposal(start, taskVersion, validation, stream) {
		return false
	}
	verified, available := core.VerifyPersistedPostcondition(task, outcome, "")
	return available && verified.PostconditionStatus == core.PostconditionVerified
}

func knowledgeEvidenceArtifactsBound(evidenceRefs, knowledgeRefs []string) bool {
	allowed := make(map[string]struct{}, len(knowledgeRefs))
	for _, ref := range knowledgeRefs {
		allowed[ref] = struct{}{}
	}
	for _, ref := range evidenceRefs {
		if _, found := allowed[ref]; !found {
			return false
		}
	}
	return true
}
