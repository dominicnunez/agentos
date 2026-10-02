package ledger_test

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestIncidentIncomingLabEvidence(t *testing.T) {
	ledger.ParallelIncidentTestForTest(t)
	for _, field := range []string{"result_event_refs", "experiment_result_event_refs", "reproduction_evidence_refs"} {
		for _, side := range []string{"both", "event", "record", "unrelated"} {
			t.Run(field+"/"+side, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "lab-evidence.db")
				store, err := ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				runtime := app.New(events.NewGateway(store))
				selected, err := runtime.Submit(t.Context(), app.Submit{RequestID: "selected", OrganizationID: "org-1", Statement: "echo selected", Kind: core.ExecutionDeterministic})
				if err != nil {
					t.Fatal(err)
				}
				var selectedRef string
				for _, event := range selected.Events {
					if event.EventType == "WORK_COMPLETED" {
						selectedRef = event.EventID
					}
				}
				if selectedRef == "" {
					t.Fatal("selected Work did not complete")
				}
				spec := struct {
					SandboxRef           string
					CapabilityProfileRef string
					Budget               core.ExperimentBudget
				}{SandboxRef: "lab-deterministic-no-effects-v1", CapabilityProfileRef: "lab-no-effects-v1", Budget: core.ExperimentBudget{MaxExecutions: 2, MaxUsageUnits: 1000, MaxWallTimeSeconds: 60, MaxChildren: 1, AllowedInferencePools: []string{"deterministic"}}}
				experiment, err := runtime.SubmitExperiment(t.Context(), app.Submit{RequestID: "experiment", OrganizationID: "org-2", Statement: "echo candidate", Kind: core.ExecutionDeterministic}, spec)
				if err != nil {
					t.Fatal(err)
				}
				reproduction, err := runtime.Submit(t.Context(), app.Submit{RequestID: "reproduction", OrganizationID: "org-2", Statement: "echo independent", Kind: core.ExecutionDeterministic})
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
					t.Fatal("independent Work did not complete")
				}
				var commissioningActor core.ID
				var experimentAdmission string
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
					if event.EventType == "LAB_EXPERIMENT_COMPLETED" {
						experimentAdmission = event.EventID
					}
				}
				if experimentAdmission == "" {
					t.Fatal("experiment did not complete")
				}
				candidate := core.PromotionCandidate{ID: "promotion-1", OrganizationID: "org-2", ExperimentID: experiment.Experiment.ID, ExperimentVersion: 2, TargetKind: core.PromotionTargetKnowledge, TargetRef: "candidate-1", Summary: "independently reproduced", ExperimentResultEventRefs: experiment.Experiment.ResultEventRefs, ReproductionEvidenceRefs: []string{reproductionRef}, NominatedBy: commissioningActor, Status: core.PromotionCandidateStatus, CreatedAt: time.Now().UTC()}
				promotion, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "LAB_PROMOTION_CANDIDATE_CREATED", SourceActorID: "runtime", CorrelationID: experiment.Events[0].CorrelationID}, ProjectionKind: "lab_promotion_candidate", RecordID: string(candidate.ID), Version: 1, Value: candidate})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected.Events[0].CorrelationID, 256); err != nil {
					t.Fatalf("valid unrelated Lab history: %v", err)
				}
				admission := promotion.EventID
				if field == "result_event_refs" {
					admission = experimentAdmission
				}
				if side != "unrelated" {
					ledger.ChangeLabEvidenceForTest(t, store, admission, field, selectedRef, side)
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				if side == "unrelated" || side == "record" {
					if fullErr != nil {
						t.Fatalf("valid full projection history: %v", fullErr)
					}
				} else {
					want := "completed Lab experiment result is not its exact Work completion"
					switch field {
					case "experiment_result_event_refs":
						want = "lab promotion candidate lacks its exact completed experiment"
					case "reproduction_evidence_refs":
						want = "lab promotion candidate lacks independent same-organization reproduction evidence"
					}
					if fullErr == nil || !strings.Contains(fullErr.Error(), want) {
						t.Fatalf("full projection history did not reject %s: %v", field, fullErr)
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected.Events[0].CorrelationID, 256)
				if side == "unrelated" {
					if err != nil {
						t.Fatalf("unrelated Lab evidence rejected: %v", err)
					}
					return
				}
				if err == nil || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatalf("incident omitted incoming Lab evidence: error %v", err)
				}
			})
		}
	}
}
