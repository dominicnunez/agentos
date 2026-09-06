package core

// KnowledgeContextUse is an independently admitted judgment about the exact
// candidate's use. An absent classification preserves historical records but
// does not authorize their use in new model context.
type KnowledgeContextUse string

const (
	KnowledgeFactualReference KnowledgeContextUse = "FACTUAL_REFERENCE"
	KnowledgeBehavioralPolicy KnowledgeContextUse = "BEHAVIORAL_POLICY"
)

func ValidKnowledgeContextUse(use KnowledgeContextUse) bool {
	return use == "" || use == KnowledgeFactualReference || use == KnowledgeBehavioralPolicy
}

func validKnowledgeContextClassification(record KnowledgeRecord) bool {
	if record.ContextUse == "" {
		return true
	}
	return ValidKnowledgeContextUse(record.ContextUse) && record.Status != KnowledgeCandidate &&
		record.ValidationMethod == KnowledgeValidationHuman && record.ValidatedByKind == PrincipalHuman &&
		(record.CreatedByKind != PrincipalHuman || record.CreatedBy != record.ValidatedBy)
}

// KnowledgeEligibleForModelContext is only the record-level portion of the
// boundary. Callers must also verify admitted judgment evidence, scope and the
// complete current derivative lineage. Policy evidence is not implemented.
func KnowledgeEligibleForModelContext(record KnowledgeRecord) bool {
	return ValidKnowledgeRecord(record) && record.Status == KnowledgeActive && record.ContextUse == KnowledgeFactualReference
}
