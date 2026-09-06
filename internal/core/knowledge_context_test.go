package core

import (
	"testing"
	"time"
)

func TestKnowledgeContextRequiresIndependentExplicitClassification(t *testing.T) {
	now := time.Now().UTC()
	prior := 1
	record := KnowledgeRecord{
		KnowledgeID: "knowledge-1", OrganizationID: "org-1", Version: 2,
		Type: KnowledgeClaim, Scope: KnowledgeScopeOrganization, ScopeID: "org-1",
		Status: KnowledgeActive, Title: "Inventory", Content: "The inventory contains three items.",
		Basis: KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{"source-1"},
		CreatedBy: "author", CreatedByKind: PrincipalHuman, CreatedAt: now,
		ValidationMethod: KnowledgeValidationHuman, ValidationRefs: []string{"judgment-1"},
		ValidatedBy: "reviewer", ValidatedByKind: PrincipalHuman, LastVerifiedAt: &now, SupersedesVersion: &prior,
	}
	if !ValidKnowledgeRecord(record) || KnowledgeEligibleForModelContext(record) {
		t.Fatal("legacy validation must remain readable but cannot establish factual context use")
	}
	context := AgentExecutionInputContext{
		Blueprint: AgentBlueprint{ID: "blueprint-1", Version: "v1", OperatingInstructions: "Report inventory facts."},
		Task:      Task{ID: "task-1", Description: "Count inventory."},
		Knowledge: []KnowledgeRecord{record},
	}
	legacy, err := MaterializeStructuredAgentExecutionInput(context)
	if err != nil || legacy.Messages[0].Source.Reference != "execution-contract-v4" {
		t.Fatalf("historical v4 input rejected: %v", err)
	}
	if _, err := BindCurrentAgentExecutionInput("org-1", "execution-1", context); err == nil {
		t.Fatal("current binding accepted unclassified Knowledge")
	}
	record.ContextUse = KnowledgeFactualReference
	if !KnowledgeEligibleForModelContext(record) {
		t.Fatal("independently classified factual record was rejected")
	}
	context.Knowledge[0] = record
	current, err := BindCurrentAgentExecutionInput("org-1", "execution-1", context)
	if err != nil || current.Request().Messages[0].Source.Reference != "execution-contract-v5" {
		t.Fatalf("current factual input rejected: %v", err)
	}
	for _, kind := range []KnowledgeType{KnowledgeExperience, KnowledgeLesson, KnowledgeClaim, KnowledgeProcedure} {
		record.Type = kind
		record.ContextUse = KnowledgeBehavioralPolicy
		if !ValidKnowledgeRecord(record) || KnowledgeEligibleForModelContext(record) {
			t.Fatalf("policy record type %s escaped the boundary", kind)
		}
		context.Knowledge[0] = record
		if _, err := MaterializeCurrentAgentExecutionInput(context); err == nil {
			t.Fatalf("policy record type %s materialized directly", kind)
		}
	}
	record.ContextUse = KnowledgeFactualReference
	record.ValidatedBy = record.CreatedBy
	if ValidKnowledgeRecord(record) {
		t.Fatal("author approved its own factual classification")
	}
	record.ValidatedBy = "reviewer"
	record.ValidatedByKind = PrincipalAgent
	record.ValidationMethod = KnowledgeValidationIndependentAgent
	if ValidKnowledgeRecord(record) {
		t.Fatal("Agent judgment substituted for independent classification")
	}
}
