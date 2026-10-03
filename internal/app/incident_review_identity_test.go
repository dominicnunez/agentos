package app

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestIncidentIncomingReviewTaskIdentity(t *testing.T) {
	for _, kind := range []string{"request", "decision"} {
		t.Run(kind, func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			gateway := events.NewGateway(store)
			service := NewWithModel(gateway, describedModel{})
			selected, err := service.Submit(t.Context(), Submit{RequestID: "selected-review-task", OrganizationID: "org-1", Statement: "summarize", Kind: core.ExecutionAgent})
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := service.Submit(t.Context(), Submit{RequestID: "foreign-review-task", OrganizationID: "org-2", Statement: "summarize", Kind: core.ExecutionAgent})
			if err != nil {
				t.Fatal(err)
			}
			foreignReview, found, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID))
			if err != nil || !found {
				t.Fatalf("foreign writer review: found=%t err=%v", found, err)
			}
			selectedCorrelation := selected.Events[0].CorrelationID
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err != nil {
				t.Fatalf("independent selected incident: %v", err)
			}
			foreignCorrelation := foreign.Events[0].CorrelationID
			switch kind {
			case "request":
				contract := foreignReview.Request.Contract
				contract.TaskID = selected.Task.ID
				contract.TaskVersion++
				request, err := completion.NewReviewRequest("org-2", selected.Task.ID, contract.TaskVersion, foreignReview.Request.Objective, contract, foreignReview.Request.EvidenceRefs, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				stream, err := service.Events(t.Context(), foreignCorrelation)
				if err != nil {
					t.Fatal(err)
				}
				var sourceExecution string
				for _, event := range stream {
					if event.EventType == "COMPLETION_REVIEW_REQUESTED" {
						sourceExecution = event.SourceExecutionID
					}
				}
				if sourceExecution == "" {
					t.Fatal("writer review request execution missing")
				}
				if _, err := gateway.PublishTrusted(t.Context(), events.TrustedDraft{
					OrganizationID: "org-2", EventType: "COMPLETION_REVIEW_REQUESTED", SourceActorID: "runtime",
					SourceExecutionID: sourceExecution, TaskID: string(foreign.Task.ID), CorrelationID: foreignCorrelation, Payload: request,
				}); err != nil {
					t.Fatal(err)
				}
			case "decision":
				decision := completion.HumanReview{
					ReviewID: foreignReview.Request.ID, OrganizationID: "org-2", TaskID: selected.Task.ID,
					TaskVersion: foreignReview.Request.TaskVersion, Fingerprint: foreignReview.Request.Fingerprint,
					Decision: completion.ReviewApprove, ReviewerID: "reviewer-1", Method: core.AssuranceHumanJudgment,
					EvidenceRefs: foreignReview.Request.EvidenceRefs, DecidedAt: time.Now().UTC(),
				}
				if _, err := gateway.PublishTrusted(t.Context(), events.TrustedDraft{
					OrganizationID: "org-2", EventType: "COMPLETION_REVIEW_DECIDED", SourceActorID: "reviewer-1",
					TaskID: string(foreign.Task.ID), CorrelationID: foreignCorrelation, Payload: decision,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := service.CompletionReview(t.Context(), "org-2", string(foreign.Task.ID)); err == nil {
				t.Fatal("review owner accepted mismatched payload Task identity")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err == nil {
				t.Fatal("incident omitted foreign review claiming selected global Task")
			}
		})
	}
}
