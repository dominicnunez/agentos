package ledger_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestIncidentRejectsMovedPromotion(t *testing.T) {
	for _, missing := range []string{"", "event", "record"} {
		t.Run("missing-"+missing, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "promotion.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			gateway := events.NewGateway(store)
			runtime := app.New(gateway)
			spec := struct {
				SandboxRef           string
				CapabilityProfileRef string
				Budget               core.ExperimentBudget
			}{SandboxRef: "lab-deterministic-no-effects-v1", CapabilityProfileRef: "lab-no-effects-v1", Budget: core.ExperimentBudget{MaxWallTimeSeconds: 60, AllowedInferencePools: []string{"deterministic"}}}
			spec.Budget.MaxExecutions = 2
			spec.Budget.MaxUsageUnits = 1000
			spec.Budget.MaxChildren = 1
			experiment, err := runtime.SubmitExperiment(t.Context(), app.Submit{RequestID: "experiment", OrganizationID: "org-1", Statement: "echo candidate", Kind: core.ExecutionDeterministic}, spec)
			if err != nil {
				t.Fatal(err)
			}
			reproduction, err := runtime.Submit(t.Context(), app.Submit{RequestID: "reproduction", OrganizationID: "org-1", Statement: "echo independent", Kind: core.ExecutionDeterministic})
			if err != nil {
				t.Fatal(err)
			}
			var reproductionRef string
			for _, event := range reproduction.Events {
				if event.EventType == "WORK_COMPLETED" {
					reproductionRef = event.EventID
				}
			}
			if reproductionRef == "" {
				t.Fatal("reproduction did not complete")
			}
			var commissioningActor core.ID
			for _, event := range experiment.Events {
				projection, present, err := events.AdmittedProjection(event)
				if err != nil {
					t.Fatal(err)
				}
				if present && projection.Projection.ProjectionKind == "intent" {
					var intent core.Intent
					if err := json.Unmarshal(projection.Projection.Value, &intent); err != nil {
						t.Fatal(err)
					}
					commissioningActor = intent.SourcePrincipalID
				}
			}
			candidate := core.PromotionCandidate{ID: "promotion-1", OrganizationID: "org-1", ExperimentID: experiment.Experiment.ID, ExperimentVersion: 2, TargetKind: core.PromotionTargetKnowledge, TargetRef: "candidate-1", Summary: "independently reproduced", ExperimentResultEventRefs: experiment.Experiment.ResultEventRefs, ReproductionEvidenceRefs: []string{reproductionRef}, NominatedBy: commissioningActor, Status: core.PromotionCandidateStatus, CreatedAt: time.Now().UTC()}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "LAB_PROMOTION_CANDIDATE_CREATED", SourceActorID: "runtime", CorrelationID: experiment.Events[0].CorrelationID}, ProjectionKind: "lab_promotion_candidate", RecordID: string(candidate.ID), Version: 1, Value: candidate}); err != nil {
				t.Fatal(err)
			}
			correlation := experiment.Events[0].CorrelationID
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
				t.Fatalf("valid nomination rejected: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
				t.Fatalf("valid recovery: %v", err)
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			ledger.MoveIncidentProjectionForTest(t, store, "lab_promotion_candidate", missing)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ledgerrecovery.Verify(t.Context(), path); err == nil {
				t.Fatal("recovery accepted moved promotion")
			}
			store, err = ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err == nil {
				t.Fatal("incident accepted moved promotion")
			}
		})
	}
}
