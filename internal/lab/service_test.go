package lab_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/lab"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/projections"
)

type failLabTerminalLedger struct {
	*ledger.SQLite
	fail bool
}

func (l *failLabTerminalLedger) AppendProjection(ctx context.Context, draft events.ProjectionDraft) (events.Event, error) {
	if l.fail && (draft.Event.EventType == "LAB_EXPERIMENT_COMPLETED" || draft.Event.EventType == "LAB_EXPERIMENT_FAILED") {
		l.fail = false
		return events.Event{}, errors.New("injected Lab terminal persistence failure")
	}
	return l.SQLite.AppendProjection(ctx, draft)
}

func TestExperimentalWorkCompletesWithoutGainingTrust(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	service := app.New(gateway)

	result, err := service.SubmitExperiment(ctx, app.Submit{
		RequestID: "experiment-request", OrganizationID: "org-1", Statement: "echo bounded experimental result", Kind: core.ExecutionDeterministic,
	}, experimentSpec())
	if err != nil {
		t.Fatal(err)
	}
	if result.Work.Status != core.WorkCompleted || result.Experiment == nil || result.Experiment.Status != core.ExperimentCompleted || result.Experiment.TrustLabel != core.ExperimentTrustUnverified {
		t.Fatalf("experimental work did not close at unverified trust: work=%+v experiment=%+v", result.Work, result.Experiment)
	}
	if len(result.Experiment.ResultEventRefs) != 1 {
		t.Fatalf("experiment result refs=%v", result.Experiment.ResultEventRefs)
	}
	completion := eventByID(t, result.Events, result.Experiment.ResultEventRefs[0])
	if completion.EventType != "WORK_COMPLETED" {
		t.Fatalf("experiment result ref selected %s", completion.EventType)
	}
	intentCreated := eventOfType(t, result.Events, "INTENT_CREATED")
	workCreated := eventOfType(t, result.Events, "WORK_CREATED")
	experimentStarted := eventOfType(t, result.Events, "LAB_EXPERIMENT_STARTED")
	if workCreated.Sequence != intentCreated.Sequence+1 || experimentStarted.Sequence != workCreated.Sequence+1 {
		t.Fatalf("experimental submission was not admitted as one ordered projection set: intent=%d work=%d experiment=%d", intentCreated.Sequence, workCreated.Sequence, experimentStarted.Sequence)
	}

	if _, retryErr := service.Submit(ctx, app.Submit{
		RequestID: "experiment-request", OrganizationID: "org-1", Statement: "echo bounded experimental result", Kind: core.ExecutionDeterministic,
	}); retryErr == nil || !strings.Contains(retryErr.Error(), "experimental Work") {
		t.Fatalf("ordinary retry silently downgraded experimental Work: %v", retryErr)
	}
	if _, retryErr := service.SubmitExperiment(ctx, app.Submit{
		RequestID: "experiment-request", OrganizationID: "org-1", Statement: "echo bounded experimental result", Kind: core.ExecutionDeterministic,
	}, experimentSpec()); retryErr != nil {
		t.Fatalf("identical experimental retry was not idempotent: %v", retryErr)
	}
	if _, err := app.New(gateway).Recover(ctx); err != nil {
		t.Fatalf("restart rejected durable Lab state: %v", err)
	}
}

func TestExperimentalSubmissionRejectsUnenforcedInferenceAndEffectProfiles(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := app.New(events.NewGateway(store))
	base := app.Submit{RequestID: "unsafe-experiment", OrganizationID: "org-1", Statement: "echo unsafe", Kind: core.ExecutionAgent}
	if _, err := service.SubmitExperiment(context.Background(), base, experimentSpec()); err == nil {
		t.Fatal("adaptive inference entered the Lab before its resource-budget bridge exists")
	}
	base.Kind = core.ExecutionDeterministic
	spec := experimentSpec()
	spec.CapabilityProfileRef = "production-effects"
	if _, err := service.SubmitExperiment(context.Background(), base, spec); err == nil {
		t.Fatal("effect-shaped Lab capability profile was accepted")
	}
	spec = experimentSpec()
	spec.Budget.MaxMeteredCostMicrounits = 1
	if _, err := service.SubmitExperiment(context.Background(), base, spec); err == nil {
		t.Fatal("metered inference entered the Lab before cost enforcement exists")
	}
}

func TestOrdinaryWorkCannotBeRetrofittedWithLabContainment(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := app.New(events.NewGateway(store))
	ordinary := app.Submit{RequestID: "ordinary-request", OrganizationID: "org-1", Statement: "echo ordinary result", Kind: core.ExecutionDeterministic}
	if _, err := service.Submit(ctx, ordinary); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SubmitExperiment(ctx, ordinary, experimentSpec()); err == nil || !strings.Contains(err.Error(), "lacks its immutable experimental containment") {
		t.Fatalf("ordinary Work was retrofitted as experimental: %v", err)
	}
}

func TestRecoveryClosesExperimentLeftRunningAfterWorkFailure(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(&failLabTerminalLedger{SQLite: store, fail: true})
	runtime := app.New(gateway)
	_, err = runtime.SubmitExperiment(ctx, app.Submit{
		RequestID: "failed-experiment", OrganizationID: "org-1", Statement: "unsupported deterministic operation", Kind: core.ExecutionDeterministic,
	}, experimentSpec())
	if err == nil {
		t.Fatal("unsupported deterministic Work unexpectedly completed")
	}
	snapshot, err := projections.New(gateway).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var experiment projections.Versioned[core.Experiment]
	for _, admitted := range snapshot.Experiments {
		experiment = admitted
	}
	if experiment.Value.Status != core.ExperimentRunning || snapshot.Works[experiment.Value.WorkID].Value.Status != core.WorkFailed {
		t.Fatalf("test did not reach the intended crash window: experiment=%+v", experiment)
	}
	if _, err := app.New(gateway).Recover(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = projections.New(gateway).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	experiment = snapshot.Experiments[experiment.Value.ID]
	if experiment.Value.Status != core.ExperimentFailed || experiment.Value.FailureCode != core.ExperimentFailureWorkFailed {
		t.Fatalf("recovery did not close failed experiment: %+v", experiment)
	}
}

func TestPromotionNominationRequiresIndependentSameOrganizationWork(t *testing.T) {
	ctx := context.Background()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	runtime := app.New(gateway)
	experimentResult, err := runtime.SubmitExperiment(ctx, app.Submit{
		RequestID: "experiment", OrganizationID: "org-1", Statement: "echo candidate method result", Kind: core.ExecutionDeterministic,
	}, experimentSpec())
	if err != nil {
		t.Fatal(err)
	}
	reproduction, err := runtime.Submit(ctx, app.Submit{
		RequestID: "reproduction", OrganizationID: "org-1", Statement: "echo independent reproduction", Kind: core.ExecutionDeterministic,
	})
	if err != nil {
		t.Fatal(err)
	}
	reproductionRef := eventOfType(t, reproduction.Events, "WORK_COMPLETED").EventID
	secondReproduction, err := runtime.Submit(ctx, app.Submit{
		RequestID: "reproduction-2", OrganizationID: "org-1", Statement: "echo second independent reproduction", Kind: core.ExecutionDeterministic,
	})
	if err != nil {
		t.Fatal(err)
	}
	reproductionRefs := []string{reproductionRef, eventOfType(t, secondReproduction.Events, "WORK_COMPLETED").EventID}
	slices.Sort(reproductionRefs)
	slices.Reverse(reproductionRefs)
	labService := lab.New(gateway)
	candidate, err := labService.Nominate(ctx, lab.Nomination{
		OrganizationID: "org-1", ExperimentID: experimentResult.Experiment.ID,
		TargetKind: core.PromotionTargetKnowledge, TargetRef: "knowledge-candidate-1", Summary: "candidate method reproduced",
		ReproductionEvidenceRefs: reproductionRefs, NominatedBy: "agent-reviewer-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Status != core.PromotionCandidateStatus {
		t.Fatalf("candidate status=%s", candidate.Status)
	}
	if !slices.IsSorted(candidate.ReproductionEvidenceRefs) {
		t.Fatalf("candidate evidence is not canonical: %v", candidate.ReproductionEvidenceRefs)
	}
	retry, err := labService.Nominate(ctx, lab.Nomination{
		OrganizationID: "org-1", ExperimentID: experimentResult.Experiment.ID,
		TargetKind: core.PromotionTargetKnowledge, TargetRef: "knowledge-candidate-1", Summary: "candidate method reproduced",
		ReproductionEvidenceRefs: append([]string(nil), candidate.ReproductionEvidenceRefs...), NominatedBy: "agent-reviewer-1",
	})
	if err != nil || retry.ID != candidate.ID {
		t.Fatalf("permuted evidence did not replay idempotently: retry=%+v err=%v", retry, err)
	}
	snapshot, err := projections.New(gateway).Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := snapshot.PromotionCandidates[candidate.ID]; !found {
		t.Fatal("promotion nomination was not durably materialized")
	}

	_, err = labService.Nominate(ctx, lab.Nomination{
		OrganizationID: "org-1", ExperimentID: experimentResult.Experiment.ID,
		TargetKind: core.PromotionTargetSkill, TargetRef: "skill-candidate-1", Summary: "reused experiment result",
		ReproductionEvidenceRefs: append([]string(nil), experimentResult.Experiment.ResultEventRefs...), NominatedBy: "agent-reviewer-1",
	})
	if err == nil {
		t.Fatal("experiment-selected result was reused as independent reproduction")
	}

	foreign, err := runtime.Submit(ctx, app.Submit{
		RequestID: "foreign", OrganizationID: "org-2", Statement: "echo unrelated foreign evidence", Kind: core.ExecutionDeterministic,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = labService.Nominate(ctx, lab.Nomination{
		OrganizationID: "org-1", ExperimentID: experimentResult.Experiment.ID,
		TargetKind: core.PromotionTargetConfiguration, TargetRef: "config-candidate-1", Summary: "foreign evidence",
		ReproductionEvidenceRefs: []string{eventOfType(t, foreign.Events, "WORK_COMPLETED").EventID}, NominatedBy: "agent-reviewer-1",
	})
	if err == nil || !strings.Contains(err.Error(), "same-organization") {
		t.Fatalf("foreign-organization evidence was accepted: %v", err)
	}
}

func experimentSpec() lab.Spec {
	return lab.Spec{
		SandboxRef: "sandbox-disposable-1", CapabilityProfileRef: lab.NoEffectsCapabilityProfile,
		Budget: core.ExperimentBudget{MaxExecutions: 2, MaxUsageUnits: 1000, MaxWallTimeSeconds: 60, MaxChildren: 1, AllowedInferencePools: []string{lab.DeterministicInferencePool}},
	}
}

func eventOfType(t *testing.T, stream []events.Event, eventType string) events.Event {
	t.Helper()
	for _, event := range stream {
		if event.EventType == eventType {
			return event
		}
	}
	t.Fatalf("event %s not found", eventType)
	return events.Event{}
}

func eventByID(t *testing.T, stream []events.Event, eventID string) events.Event {
	t.Helper()
	for _, event := range stream {
		if event.EventID == eventID {
			return event
		}
	}
	t.Fatalf("event %s not found", eventID)
	return events.Event{}
}
