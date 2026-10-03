package ledger_test

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type incidentReviewModel struct{}

func (m incidentReviewModel) CompleteRequest(ctx context.Context, request modelinput.Request) (execution.ModelResponse, error) {
	body, err := request.Canonical()
	if err != nil {
		return execution.ModelResponse{}, err
	}
	return m.Complete(ctx, string(body))
}

func (incidentReviewModel) Name() string { return "codex-subscription/test-model" }
func (incidentReviewModel) Descriptor() execution.ModelDescriptor {
	return execution.ModelDescriptor{Provider: "codex-subscription", Model: "test-model", ExecutionProfileVersion: "v1-codex-subscription"}
}
func (incidentReviewModel) Complete(_ context.Context, prompt string) (execution.ModelResponse, error) {
	return execution.ModelResponse{Text: "configured-model: " + prompt, Usage: events.InferenceUsageRecordedPayload{Source: "provider_cli", Provider: "codex-subscription", Model: "test-model", InputTokens: 1, OutputTokens: 1, TotalTokens: 2}}, nil
}

func TestIncidentReviewCandidates(t *testing.T) {
	ledger.ParallelIncidentTestForTest(t)
	for _, kind := range []string{"COMPLETION_REVIEW_REQUESTED", "COMPLETION_REVIEW_DECIDED"} {
		for _, mode := range []string{"foreign", "other-correlation", "after-manifest", "deterministic", "unconsumed"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "review-candidates.db")
				store, err := ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				service := app.NewWithModel(events.NewGateway(store), incidentReviewModel{})
				finish := func(organization, id string, executionKind core.ExecutionKind, approve bool) app.Result {
					statement := "summarize"
					if executionKind == core.ExecutionDeterministic {
						statement = "echo selected"
					}
					result, err := service.Submit(t.Context(), app.Submit{RequestID: id, OrganizationID: organization, Statement: statement, Kind: executionKind})
					if err != nil {
						t.Fatal(err)
					}
					if executionKind == core.ExecutionAgent && approve {
						view, found, err := service.CompletionReview(t.Context(), organization, string(result.Task.ID))
						if err != nil || !found {
							t.Fatalf("actual writer review: found=%t err=%v", found, err)
						}
						if _, err := service.ReviewCompletion(t.Context(), app.CompletionReviewInput{OrganizationID: organization, TaskID: string(view.Request.TaskID), ReviewID: string(view.Request.ID), Fingerprint: view.Request.Fingerprint, Decision: core.CompletionReviewApprove, ReviewerID: "reviewer-1", ReviewerKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", Feedback: "Reviewed candidate"}); err != nil {
							t.Fatal(err)
						}
					}
					return result
				}
				foreign := finish("org-2", "foreign-review-candidate", core.ExecutionAgent, true)
				selectedKind := core.ExecutionAgent
				if mode == "deterministic" {
					selectedKind = core.ExecutionDeterministic
				}
				selected := finish("org-1", "selected-review-candidate", selectedKind, mode != "unconsumed")
				if _, err := service.Recover(t.Context()); err != nil {
					t.Fatalf("healthy writer recovery: %v", err)
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				var source, boundary events.Event
				selectedCorrelation := selected.Events[0].CorrelationID
				for _, event := range stream {
					if event.CorrelationID == foreign.Events[0].CorrelationID && event.EventType == kind {
						source = event
					}
					if event.CorrelationID == selectedCorrelation && event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
						boundary = event
					}
				}
				if source.EventID == "" {
					t.Fatal("foreign actual writer review event missing")
				}
				if source.TaskID == string(selected.Task.ID) {
					t.Fatal("review candidate must retain its independent foreign Task")
				}
				for _, event := range selected.Events {
					if strings.Contains(string(source.Payload), event.EventID) {
						t.Fatal("foreign review payload already references selected evidence")
					}
				}
				if boundary.EventID == "" && mode != "deterministic" {
					t.Fatal("selected Agent manifest missing")
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err != nil {
					t.Fatalf("healthy incident: %v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
					t.Fatalf("healthy full recovery: %v", err)
				}
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				insertion := boundary.Sequence
				if mode == "after-manifest" {
					insertion++
				}
				if mode == "deterministic" {
					insertion = stream[len(stream)-1].Sequence + 1
				}
				correlation := selectedCorrelation
				if mode == "other-correlation" {
					correlation = "unrelated-review-correlation"
				}
				ledger.InsertReviewCandidateForTest(t, store, source, insertion, correlation)
				mutated, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				graph, ownerErr := events.ValidateProjectionHistory(mutated, nil, nil, nil)
				if ownerErr == nil {
					ownerErr = events.ValidateProjectionCompletions(graph, mutated, nil)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				_, recoveryErr := ledgerrecovery.Verify(t.Context(), path)
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, incidentErr := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256)
				if mode != "foreign" {
					if ownerErr != nil || recoveryErr != nil || incidentErr != nil {
						t.Fatalf("unused review candidate changed selected operation: owner=%v full=%v incident=%v", ownerErr, recoveryErr, incidentErr)
					}
					return
				}
				want := "execution completion revision request is invalid"
				if kind == "COMPLETION_REVIEW_DECIDED" {
					want = "execution completion revision is invalid"
				}
				if ownerErr == nil || !strings.Contains(ownerErr.Error(), want) {
					t.Fatalf("completion owner did not reject relevant foreign review: %v", ownerErr)
				}
				if recoveryErr == nil || !strings.Contains(recoveryErr.Error(), want) {
					t.Fatalf("full owner did not reject relevant foreign review: %v", recoveryErr)
				}
				t.Logf("full recovery rejected relevant review: %v", recoveryErr)
				if incidentErr == nil {
					t.Fatal("incident omitted foreign review sharing consumed Agent correlation")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid incident returned partial evidence")
				}
			})
		}
	}
}
