package core

import (
	"slices"
	"testing"
	"time"
)

func TestValidKnowledgeRecordSeparatesCandidateFromValidatedKnowledge(t *testing.T) {
	now := time.Now().UTC()
	candidate := KnowledgeRecord{
		KnowledgeID: "knowledge-1", OrganizationID: "org-1", Version: 1,
		Type: KnowledgeLesson, Scope: KnowledgeScopeOrganization, ScopeID: "org-1",
		Status: KnowledgeCandidate, Title: "Check the API version", Content: "Verify the supported API version before deployment.",
		Basis: KnowledgeBasisSingleExperience, ProvenanceEventRefs: []string{"event-1"},
		CreatedBy: "agent-1", CreatedByKind: PrincipalExternalAgent, CreatedAt: now,
		ValidationMethod: KnowledgeValidationUnvalidated,
	}
	if !ValidKnowledgeRecord(candidate) {
		t.Fatalf("valid candidate rejected: %+v", candidate)
	}
	for name, mutate := range map[string]func(*KnowledgeRecord){
		"no tenant":           func(record *KnowledgeRecord) { record.OrganizationID = "" },
		"cross scope":         func(record *KnowledgeRecord) { record.ScopeID = "org-2" },
		"unknown type":        func(record *KnowledgeRecord) { record.Type = "MEMORY" },
		"no provenance":       func(record *KnowledgeRecord) { record.ProvenanceEventRefs = nil },
		"candidate validated": func(record *KnowledgeRecord) { record.ValidatedBy = "agent-2" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := candidate
			mutate(&changed)
			if ValidKnowledgeRecord(changed) {
				t.Fatalf("invalid candidate accepted: %+v", changed)
			}
		})
	}

	verified := now.Add(time.Minute)
	active := candidate
	active.Version = 2
	active.Status = KnowledgeActive
	active.ValidationMethod = KnowledgeValidationDeterministic
	active.ValidationRefs = []string{"event-2"}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = PrincipalRuntime
	active.LastVerifiedAt = &verified
	supersedes := 1
	active.SupersedesVersion = &supersedes
	if !ValidKnowledgeRecord(active) {
		t.Fatalf("valid active knowledge rejected: %+v", active)
	}
	active.ValidationMethod = KnowledgeValidationHuman
	if ValidKnowledgeRecord(active) {
		t.Fatal("runtime identity was mislabeled as user judgment")
	}
	active.ValidationMethod = KnowledgeValidationRepeatedObservation
	if ValidKnowledgeRecord(active) {
		t.Fatal("one event was accepted as repeated-observation validation")
	}
}

func TestValidKnowledgeRecordRequiresCautiousPatternAndDerivedProvenance(t *testing.T) {
	now := time.Now().UTC()
	record := KnowledgeRecord{
		KnowledgeID: "knowledge-pattern", OrganizationID: "org-1", Version: 1,
		Type: KnowledgeLesson, Scope: KnowledgeScopeOrganization, ScopeID: "org-1",
		Status: KnowledgeCandidate, Title: "Repeated failure", Content: "A repeated failure deserves investigation.",
		Basis: KnowledgeBasisRepeatedPattern, ProvenanceEventRefs: []string{"event-1", "event-2", "event-3"},
		OccurrenceEventRefs: []string{"event-1", "event-2", "event-3"}, CreatedBy: "agent-1",
		CreatedByKind: PrincipalExternalAgent, CreatedAt: now, ValidationMethod: KnowledgeValidationUnvalidated,
		DerivedKnowledgeRefs: []VersionedRef{
			{ID: "knowledge-a", Version: "1", MaterializationState: MaterializedFull},
			{ID: "knowledge-b", Version: "2", MaterializationState: MaterializedFull},
		},
	}
	if !ValidKnowledgeRecord(record) {
		t.Fatalf("valid pattern candidate rejected: %+v", record)
	}
	record.OccurrenceEventRefs = record.OccurrenceEventRefs[:2]
	if ValidKnowledgeRecord(record) {
		t.Fatal("two occurrences became a repeated-pattern candidate")
	}
	record.OccurrenceEventRefs = []string{"event-1", "event-2", "event-3"}
	slices.Reverse(record.DerivedKnowledgeRefs)
	if ValidKnowledgeRecord(record) {
		t.Fatal("noncanonical derived knowledge lineage was accepted")
	}
}

func TestKnowledgeActivationRejectsAgentSelfValidation(t *testing.T) {
	now := time.Now().UTC()
	prior := KnowledgeRecord{
		KnowledgeID: "knowledge-1", OrganizationID: "org-1", Version: 1,
		Type: KnowledgeLesson, Scope: KnowledgeScopeOrganization, ScopeID: "org-1",
		Status: KnowledgeCandidate, Title: "Observed pattern", Content: "An Agent observed a pattern.",
		Basis: KnowledgeBasisSingleExperience, ProvenanceEventRefs: []string{"event-1"},
		CreatedBy: "agent-1", CreatedByKind: PrincipalExternalAgent, CreatedAt: now,
		ValidationMethod: KnowledgeValidationUnvalidated,
	}
	next := prior
	next.Version = 2
	next.Status = KnowledgeActive
	next.ValidationMethod = KnowledgeValidationIndependentAgent
	next.ValidationRefs = []string{"event-2"}
	next.ValidatedBy = "agent-1"
	next.ValidatedByKind = PrincipalExternalAgent
	verifiedAt := now.Add(time.Minute)
	next.LastVerifiedAt = &verifiedAt
	supersedes := 1
	next.SupersedesVersion = &supersedes
	if err := ValidateKnowledgeTransition("KNOWLEDGE_ACTIVATED", prior, next); err == nil {
		t.Fatal("Agent activated its own proposed knowledge")
	}
	next.ValidatedBy = "agent-2"
	if err := ValidateKnowledgeTransition("KNOWLEDGE_ACTIVATED", prior, next); err != nil {
		t.Fatalf("independent Agent validation rejected: %v", err)
	}
}

func TestKnowledgeCorrectionPreservesIdentityButAllowsNewEvidenceAndContent(t *testing.T) {
	prior := KnowledgeRecord{
		KnowledgeID: "knowledge-1", OrganizationID: "org-1", Version: 2,
		Type: KnowledgeProcedure, Scope: KnowledgeScopeOrganization, ScopeID: "org-1",
		Status: KnowledgeActive, Title: "Old procedure", Content: "Use the old process.",
		Basis: KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{"event-1"},
		CreatedBy: "user-1", CreatedByKind: PrincipalHuman, CreatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC),
		ValidationMethod: KnowledgeValidationHuman, ValidationRefs: []string{"event-2"}, ValidatedBy: "user-2", ValidatedByKind: PrincipalHuman,
	}
	verifiedAt := prior.CreatedAt.Add(time.Minute)
	prior.LastVerifiedAt = &verifiedAt
	next := prior
	next.Version = 3
	next.Status = KnowledgeCandidate
	next.Title = "Corrected procedure"
	next.Content = "Use the verified new process."
	next.ProvenanceEventRefs = []string{"event-3"}
	next.CreatedBy = "agent-2"
	next.CreatedByKind = PrincipalExternalAgent
	next.CreatedAt = verifiedAt.Add(time.Minute)
	next.ValidationMethod = KnowledgeValidationUnvalidated
	next.ValidationRefs = nil
	next.ValidatedBy = ""
	next.ValidatedByKind = ""
	next.LastVerifiedAt = nil
	next.SupersedesVersion = integerPointerForKnowledgeTest(2)
	if !ValidKnowledgeRecord(next) {
		t.Fatalf("valid corrected candidate rejected: %+v", next)
	}
	if err := ValidateKnowledgeTransition("KNOWLEDGE_PROPOSED", prior, next); err != nil {
		t.Fatalf("corrected candidate transition rejected: %v", err)
	}
	next.ScopeID = "org-2"
	if err := ValidateKnowledgeTransition("KNOWLEDGE_PROPOSED", prior, next); err == nil {
		t.Fatal("corrected candidate changed its immutable scope")
	}
}

func integerPointerForKnowledgeTest(value int) *int { return &value }
