package ledger_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/completion"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/planning"
)

type incidentCompletionPlanner struct{ count int }

func (incidentCompletionPlanner) Descriptor() (planning.Descriptor, bool) {
	return planning.Descriptor{}, false
}
func (p incidentCompletionPlanner) Build(_ context.Context, input planning.Input, kind core.ExecutionKind) (planning.Result, error) {
	tasks := make([]core.PlanTask, p.count)
	tasks[0] = core.PlanTask{Key: "root", Description: input.Intent.Objective, ExecutionKind: kind, ModelInferencePolicy: core.InferenceAllowed}
	for i := 1; i < p.count; i++ {
		tasks[i] = core.PlanTask{Key: fmt.Sprintf("part-%d", i), Description: input.Intent.Objective, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden}
		tasks[0].DependsOn = append(tasks[0].DependsOn, tasks[i].Key)
	}
	return planning.Result{Tasks: tasks}, nil
}

// Vary completed Tasks in one admitted Work, then vary unrelated retained history.
// Measure complete public reads; fixture construction is excluded.
func TestIncidentCompletionGrowth(t *testing.T) {
	for _, count := range []int{1, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			service := app.NewWithModelAndPlanner(events.NewGateway(store), incidentReviewModel{}, incidentCompletionPlanner{count})
			result, err := service.Submit(t.Context(), app.Submit{RequestID: "completion-growth", OrganizationID: "org-1", Statement: "echo bounded work", Kind: core.ExecutionAgent})
			if err != nil {
				t.Fatal(err)
			}
			for range count {
				if _, err := service.Recover(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			review, found, err := service.CompletionReview(t.Context(), "org-1", string(result.Task.ID))
			if err != nil || !found {
				t.Fatalf("complete actual Agent root: found=%t err=%v", found, err)
			}
			if _, err := service.ReviewCompletion(t.Context(), app.CompletionReviewInput{OrganizationID: "org-1", TaskID: string(review.Request.TaskID), ReviewID: string(review.Request.ID), Fingerprint: review.Request.Fingerprint, Decision: completion.ReviewApprove, ReviewerID: "reviewer", ReviewerKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", Feedback: "Checked bounded integration"}); err != nil {
				t.Fatal(err)
			}

			correlation := result.Events[0].CorrelationID
			for _, unrelated := range []int{0, 256} {
				if unrelated > 0 {
					for i := range unrelated {
						if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: fmt.Sprintf("unrelated-completion-%d", i), Payload: map[string]string{"note": "unrelated history"}}); err != nil {
							t.Fatal(err)
						}
					}
				}
				start := time.Now()
				for range 5 {
					snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
					if err != nil {
						t.Fatal(err)
					}
					completed := 0
					for _, event := range snapshot.Work.Events {
						if event.EventType == "TASK_VERIFIED_COMPLETE" {
							completed++
						}
					}
					if completed != count {
						t.Fatalf("completed Tasks=%d want %d", completed, count)
					}
				}
				t.Logf("completed=%d unrelated=%d five public reads=%s", count, unrelated, time.Since(start))
			}
		})
	}
}
