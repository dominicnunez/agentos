package core

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaximumKnowledgeContentBytes       = 64 << 10
	MaximumKnowledgeTitleBytes         = 4 << 10
	MaximumKnowledgeApplicabilityBytes = 16 << 10
	MaximumKnowledgeLimitationsBytes   = 16 << 10
	MaximumKnowledgeReferences         = 256
	MaximumKnowledgeTags               = 64
	MaximumKnowledgeReferenceBytes     = 4096
)

func ValidKnowledgeRecord(record KnowledgeRecord) bool {
	if !ValidGoalReferenceID(string(record.KnowledgeID)) || !ValidGoalReferenceID(string(record.OrganizationID)) ||
		!ValidGoalReferenceID(string(record.ScopeID)) || record.Version < 1 ||
		!validKnowledgeType(record.Type) || !validKnowledgeScope(record) || !validKnowledgeStatus(record.Status) ||
		!boundedRequiredKnowledgeText(record.Title, MaximumKnowledgeTitleBytes) ||
		!boundedRequiredKnowledgeText(record.Content, MaximumKnowledgeContentBytes) ||
		!boundedOptionalKnowledgeText(record.Applicability, MaximumKnowledgeApplicabilityBytes) ||
		!boundedOptionalKnowledgeText(record.Limitations, MaximumKnowledgeLimitationsBytes) ||
		!validKnowledgeBasis(record.Basis) || !validKnowledgePrincipal(record.CreatedBy, record.CreatedByKind) || record.CreatedAt.IsZero() ||
		!validKnowledgeStrings(record.Tags, MaximumKnowledgeTags, 256, true) ||
		!validKnowledgeStrings(record.ProvenanceEventRefs, MaximumKnowledgeReferences, MaximumKnowledgeReferenceBytes, false) ||
		!validKnowledgeStrings(record.OccurrenceEventRefs, MaximumKnowledgeReferences, MaximumKnowledgeReferenceBytes, true) ||
		!validKnowledgeStrings(record.EvidenceArtifactRefs, MaximumKnowledgeReferences, MaximumKnowledgeReferenceBytes, true) ||
		!validKnowledgeStrings(record.ValidationRefs, MaximumKnowledgeReferences, MaximumKnowledgeReferenceBytes, true) ||
		!validKnowledgeVersionedRefs(record.DerivedKnowledgeRefs) {
		return false
	}
	if record.Basis == KnowledgeBasisRepeatedPattern && len(record.OccurrenceEventRefs) < 3 {
		return false
	}
	if record.SupersedesVersion != nil && (*record.SupersedesVersion < 1 || *record.SupersedesVersion >= record.Version) {
		return false
	}
	switch record.Status {
	case KnowledgeCandidate:
		return record.ValidationMethod == KnowledgeValidationUnvalidated && len(record.ValidationRefs) == 0 &&
			record.ValidatedBy == "" && record.ValidatedByKind == "" && record.LastVerifiedAt == nil
	case KnowledgeActive:
		return validKnowledgeValidation(record.ValidationMethod) && record.ValidationMethod != KnowledgeValidationUnvalidated &&
			len(record.ValidationRefs) > 0 && validKnowledgePrincipal(record.ValidatedBy, record.ValidatedByKind) &&
			record.LastVerifiedAt != nil && !record.LastVerifiedAt.IsZero() && !record.LastVerifiedAt.Before(record.CreatedAt)
	case KnowledgeSuperseded, KnowledgeStale, KnowledgeQuarantined:
		return record.Version > 1 && validTerminalKnowledgeValidation(record)
	default:
		return false
	}
}

func ValidateKnowledgeProjectionTarget(eventType string, version int, record KnowledgeRecord) error {
	if !ValidKnowledgeRecord(record) || record.Version != version {
		return fmt.Errorf("knowledge projection value is invalid")
	}
	switch eventType {
	case "KNOWLEDGE_PROPOSED":
		if version != 1 || record.Status != KnowledgeCandidate {
			return fmt.Errorf("knowledge proposal must create candidate version 1")
		}
	case "KNOWLEDGE_ACTIVATED":
		if version < 2 || record.Status != KnowledgeActive {
			return fmt.Errorf("knowledge activation must create an active revision")
		}
	case "KNOWLEDGE_SUPERSEDED":
		if version < 2 || record.Status != KnowledgeSuperseded {
			return fmt.Errorf("knowledge supersession must create a superseded revision")
		}
	case "KNOWLEDGE_MARKED_STALE":
		if version < 2 || record.Status != KnowledgeStale {
			return fmt.Errorf("knowledge staleness must create a stale revision")
		}
	case "KNOWLEDGE_QUARANTINED":
		if version < 2 || record.Status != KnowledgeQuarantined {
			return fmt.Errorf("knowledge quarantine must create a quarantined revision")
		}
	default:
		return fmt.Errorf("knowledge projection event is unsupported")
	}
	return nil
}

func ValidateKnowledgeTransition(eventType string, prior, next KnowledgeRecord) error {
	if prior.KnowledgeID != next.KnowledgeID || prior.OrganizationID != next.OrganizationID || next.Version != prior.Version+1 ||
		next.SupersedesVersion == nil || *next.SupersedesVersion != prior.Version ||
		prior.Type != next.Type || prior.Scope != next.Scope || prior.ScopeID != next.ScopeID || prior.Title != next.Title || prior.Content != next.Content ||
		prior.Basis != next.Basis || !slices.Equal(prior.Tags, next.Tags) || !slices.Equal(prior.ProvenanceEventRefs, next.ProvenanceEventRefs) ||
		!slices.Equal(prior.OccurrenceEventRefs, next.OccurrenceEventRefs) || !reflect.DeepEqual(prior.DerivedKnowledgeRefs, next.DerivedKnowledgeRefs) ||
		!slices.Equal(prior.EvidenceArtifactRefs, next.EvidenceArtifactRefs) || prior.Applicability != next.Applicability ||
		prior.CreatedBy != next.CreatedBy || prior.CreatedByKind != next.CreatedByKind || !prior.CreatedAt.Equal(next.CreatedAt) {
		return fmt.Errorf("knowledge revision changes immutable candidate identity or provenance")
	}
	switch eventType {
	case "KNOWLEDGE_ACTIVATED":
		if prior.Status != KnowledgeCandidate || next.Status != KnowledgeActive {
			return fmt.Errorf("only a candidate may be activated")
		}
		if prior.CreatedByKind == PrincipalExternalAgent && next.ValidatedByKind == PrincipalExternalAgent && prior.CreatedBy == next.ValidatedBy {
			return fmt.Errorf("an Agent cannot activate its own proposed knowledge")
		}
	case "KNOWLEDGE_SUPERSEDED":
		if prior.Status != KnowledgeActive || next.Status != KnowledgeSuperseded || !sameKnowledgeValidation(prior, next) {
			return fmt.Errorf("only active knowledge may be superseded")
		}
	case "KNOWLEDGE_MARKED_STALE":
		if prior.Status != KnowledgeActive || next.Status != KnowledgeStale || !sameKnowledgeValidation(prior, next) {
			return fmt.Errorf("only active knowledge may be marked stale")
		}
	case "KNOWLEDGE_QUARANTINED":
		if (prior.Status != KnowledgeCandidate && prior.Status != KnowledgeActive) || next.Status != KnowledgeQuarantined ||
			(prior.Status == KnowledgeActive && !sameKnowledgeValidation(prior, next)) {
			return fmt.Errorf("only candidate or active knowledge may be quarantined")
		}
	default:
		return fmt.Errorf("knowledge transition is unsupported")
	}
	return nil
}

func validTerminalKnowledgeValidation(record KnowledgeRecord) bool {
	if record.ValidationMethod == KnowledgeValidationUnvalidated {
		return len(record.ValidationRefs) == 0 && record.ValidatedBy == "" && record.ValidatedByKind == "" && record.LastVerifiedAt == nil
	}
	return validKnowledgeValidation(record.ValidationMethod) && len(record.ValidationRefs) > 0 &&
		validKnowledgePrincipal(record.ValidatedBy, record.ValidatedByKind) && record.LastVerifiedAt != nil &&
		!record.LastVerifiedAt.IsZero() && !record.LastVerifiedAt.Before(record.CreatedAt)
}

func sameKnowledgeValidation(prior, next KnowledgeRecord) bool {
	return prior.ValidationMethod == next.ValidationMethod && slices.Equal(prior.ValidationRefs, next.ValidationRefs) &&
		prior.ValidatedBy == next.ValidatedBy && prior.ValidatedByKind == next.ValidatedByKind &&
		equalOptionalTime(prior.LastVerifiedAt, next.LastVerifiedAt)
}

func equalOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right)
}

func validKnowledgeType(value KnowledgeType) bool {
	return value == KnowledgeExperience || value == KnowledgeLesson || value == KnowledgeClaim || value == KnowledgeProcedure
}

func validKnowledgeScope(record KnowledgeRecord) bool {
	if record.ScopeID == "" {
		return false
	}
	switch record.Scope {
	case KnowledgeScopeAgent, KnowledgeScopeTeam:
		return true
	case KnowledgeScopeOrganization:
		return record.ScopeID == record.OrganizationID
	default:
		return false
	}
}

func validKnowledgeStatus(value KnowledgeStatus) bool {
	return value == KnowledgeCandidate || value == KnowledgeActive || value == KnowledgeSuperseded || value == KnowledgeStale || value == KnowledgeQuarantined
}

func validKnowledgeBasis(value KnowledgeBasis) bool {
	return value == KnowledgeBasisSingleExperience || value == KnowledgeBasisRepeatedPattern || value == KnowledgeBasisExperiment ||
		value == KnowledgeBasisHumanInput || value == KnowledgeBasisExternalEvidence || value == KnowledgeBasisDerived || value == KnowledgeBasisMixed
}

func validKnowledgeValidation(value KnowledgeValidationMethod) bool {
	return value == KnowledgeValidationUnvalidated || value == KnowledgeValidationDeterministic || value == KnowledgeValidationExperimental ||
		value == KnowledgeValidationRepeatedObservation || value == KnowledgeValidationIndependentAgent || value == KnowledgeValidationHuman || value == KnowledgeValidationMixed
}

func validKnowledgePrincipal(id ID, kind PrincipalKind) bool {
	if !ValidGoalReferenceID(string(id)) {
		return false
	}
	return kind == PrincipalHuman || kind == PrincipalExternalAgent || kind == PrincipalRuntime
}

func boundedRequiredKnowledgeText(value string, maximum int) bool {
	return value != "" && strings.TrimSpace(value) != "" && len(value) <= maximum && utf8.ValidString(value)
}

func boundedOptionalKnowledgeText(value string, maximum int) bool {
	return value == "" || len(value) <= maximum && utf8.ValidString(value)
}

func validKnowledgeStrings(values []string, maximumCount, maximumBytes int, emptyAllowed bool) bool {
	if len(values) > maximumCount || !emptyAllowed && len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" || len(value) > maximumBytes || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validKnowledgeVersionedRefs(values []VersionedRef) bool {
	if len(values) > MaximumKnowledgeReferences {
		return false
	}
	if !slices.IsSortedFunc(values, func(left, right VersionedRef) int {
		return strings.Compare(left.ID+"\x00"+left.Version, right.ID+"\x00"+right.Version)
	}) {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		key := value.ID + "\x00" + value.Version
		if !ValidGoalReferenceID(value.ID) || value.Version == "" || len(value.Version) > 64 ||
			value.MaterializationState != MaterializedFull || !utf8.ValidString(value.ID) || !utf8.ValidString(value.Version) {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}
