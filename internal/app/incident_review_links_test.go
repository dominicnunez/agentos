package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestIncidentIncomingCompletionReviewEvidence(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	service := NewWithModel(gateway, describedModel{})
	selected, err := service.Submit(t.Context(), Submit{RequestID: "selected-review", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := service.Submit(t.Context(), Submit{RequestID: "foreign-review", OrganizationID: "org-2", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	selectedReview, found, err := service.CompletionReview(t.Context(), "org-1", string(selected.Task.ID))
	if err != nil || !found {
		t.Fatalf("selected writer review: found=%t err=%v", found, err)
	}
	foreignReview, found, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID))
	if err != nil || !found {
		t.Fatalf("foreign writer review: found=%t err=%v", found, err)
	}
	selectedCorrelation := selected.Events[0].CorrelationID
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err != nil {
		t.Fatalf("independent selected review incident: %v", err)
	}
	refs := append([]string(nil), foreignReview.Request.EvidenceRefs...)
	refs[0] = selectedReview.Request.EvidenceRefs[0]
	contract := foreignReview.Request.Contract
	contract.TaskVersion++
	request, err := completion.NewReviewRequest("org-2", foreign.Task.ID, contract.TaskVersion, foreignReview.Request.Objective, contract, refs, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := service.Events(t.Context(), foreign.Events[0].CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	var original events.Event
	for _, event := range stream {
		if event.EventType == "COMPLETION_REVIEW_REQUESTED" {
			original = event
		}
	}
	if original.EventID == "" {
		t.Fatal("foreign writer review request missing")
	}
	if _, err := gateway.PublishTrusted(context.Background(), events.TrustedDraft{
		OrganizationID: original.OrganizationID, EventType: original.EventType,
		SourceActorID: original.SourceActorID, SourceExecutionID: original.SourceExecutionID,
		TaskID: original.TaskID, CorrelationID: original.CorrelationID, Payload: request,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID)); err == nil {
		t.Fatal("review owner accepted a request referencing another organization's outcome")
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err == nil {
		t.Fatal("incident omitted foreign review request targeting selected outcome event")
	}
}

func TestIncidentIncomingCompletionDecisionEvidence(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	service := NewWithModel(gateway, describedModel{})
	selected, err := service.Submit(t.Context(), Submit{RequestID: "selected-decision", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := service.Submit(t.Context(), Submit{RequestID: "foreign-decision", OrganizationID: "org-2", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	selectedReview, found, err := service.CompletionReview(t.Context(), "org-1", string(selected.Task.ID))
	if err != nil || !found {
		t.Fatalf("selected writer review: found=%t err=%v", found, err)
	}
	foreignReview, found, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID))
	if err != nil || !found {
		t.Fatalf("foreign writer review: found=%t err=%v", found, err)
	}
	selectedCorrelation := selected.Events[0].CorrelationID
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err != nil {
		t.Fatalf("independent selected review incident: %v", err)
	}
	refs := append([]string(nil), foreignReview.Request.EvidenceRefs...)
	refs[0] = selectedReview.Request.EvidenceRefs[0]
	decision := completion.HumanReview{
		ReviewID: foreignReview.Request.ID, OrganizationID: foreignReview.Request.OrganizationID,
		TaskID: foreignReview.Request.TaskID, TaskVersion: foreignReview.Request.TaskVersion,
		Fingerprint: foreignReview.Request.Fingerprint, Decision: completion.ReviewApprove,
		ReviewerID: "reviewer-1", Method: core.AssuranceHumanJudgment,
		EvidenceRefs: refs, DecidedAt: time.Now().UTC(),
	}
	if _, err := gateway.PublishTrusted(t.Context(), events.TrustedDraft{
		OrganizationID: "org-2", EventType: "COMPLETION_REVIEW_DECIDED", SourceActorID: "reviewer-1",
		TaskID: string(foreign.Task.ID), CorrelationID: foreign.Events[0].CorrelationID, Payload: decision,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID)); err == nil {
		t.Fatal("review owner accepted a decision referencing another organization's outcome")
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err == nil {
		t.Fatal("incident omitted foreign review decision targeting selected outcome event")
	}
}

func TestIncidentIncomingCompletionCandidateResult(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	service := NewWithModel(gateway, describedModel{})
	selected, err := service.Submit(t.Context(), Submit{RequestID: "selected-candidate", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := service.Submit(t.Context(), Submit{RequestID: "foreign-candidate", OrganizationID: "org-2", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	selectedReview, found, err := service.CompletionReview(t.Context(), "org-1", string(selected.Task.ID))
	if err != nil || !found {
		t.Fatalf("selected writer review: found=%t err=%v", found, err)
	}
	foreignReview, found, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID))
	if err != nil || !found {
		t.Fatalf("foreign writer review: found=%t err=%v", found, err)
	}
	selectedCorrelation := selected.Events[0].CorrelationID
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err != nil {
		t.Fatalf("independent selected review incident: %v", err)
	}
	stream, err := service.Events(t.Context(), foreign.Events[0].CorrelationID)
	if err != nil {
		t.Fatal(err)
	}
	var original events.Event
	for _, event := range stream {
		if event.EventID == foreignReview.Request.EvidenceRefs[2] {
			original = event
		}
	}
	if original.EventID == "" {
		t.Fatal("foreign writer candidate missing")
	}
	var payload events.CandidateCompletePayload
	if err := json.Unmarshal(original.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	payload.ResultEventID = selectedReview.Request.EvidenceRefs[1]
	badCandidate, err := gateway.PublishTrusted(t.Context(), events.TrustedDraft{
		OrganizationID: original.OrganizationID, EventType: original.EventType,
		SourceActorID: original.SourceActorID, SourceExecutionID: original.SourceExecutionID,
		TaskID: original.TaskID, CorrelationID: original.CorrelationID,
		ArtifactRefs: original.ArtifactRefs, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	refs := append([]string(nil), foreignReview.Request.EvidenceRefs...)
	refs[2] = badCandidate.EventID
	contract := foreignReview.Request.Contract
	contract.TaskVersion++
	request, err := completion.NewReviewRequest("org-2", foreign.Task.ID, contract.TaskVersion, foreignReview.Request.Objective, contract, refs, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.PublishTrusted(t.Context(), events.TrustedDraft{
		OrganizationID: "org-2", EventType: "COMPLETION_REVIEW_REQUESTED", SourceActorID: "runtime",
		SourceExecutionID: original.SourceExecutionID, TaskID: original.TaskID,
		CorrelationID: original.CorrelationID, Payload: request,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID)); err == nil {
		t.Fatal("review owner accepted candidate bound to another organization's result")
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err == nil {
		t.Fatal("incident omitted foreign candidate targeting selected result event")
	}
}

func TestIncidentIndependentCompletionReviewsSameOrganization(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := NewWithModel(events.NewGateway(store), describedModel{})
	selected, err := service.Submit(t.Context(), Submit{RequestID: "selected-independent-review", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := service.Submit(t.Context(), Submit{RequestID: "foreign-independent-review", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := service.CompletionReview(t.Context(), "org-1", string(foreign.Task.ID)); err != nil || !found {
		t.Fatalf("independent same-organization review: found=%t err=%v", found, err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected.Events[0].CorrelationID, 256); err != nil {
		t.Fatalf("independent same-organization review affected selected incident: %v", err)
	}
}
