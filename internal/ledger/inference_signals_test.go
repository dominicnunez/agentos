package ledger

import (
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"testing"
	"time"
)

func TestSignalExpiryRecheckedAtAdmissionAndHistoricalReplay(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	p := testInferencePolicy(now)
	model := execution.FakeModel{}
	descriptor := model.Descriptor()
	p.Version, p.ConnectionID = inference.ConnectionPolicyVersion, "account"
	p.Provider, p.Model, p.ExecutionProfileVersion = descriptor.Provider, descriptor.Model, descriptor.ExecutionProfileVersion
	p.Mode, p.Pricing = inference.Local, nil
	p.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxConcurrentRequests: 2}
	p.Routing = &inference.RoutePolicy{OrganizationID: p.OrganizationID, Locality: inference.LocalOnly, DataClasses: []string{"internal"}}
	p.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, Local: true, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Hour), Signals: &inference.RoutingSignals{Health: "READY", ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Minute), EvidenceRef: "observation", ExpectedLatencyMilliseconds: 100, Evaluation: &inference.RoutingEvaluation{Metric: "classification-v1", ScoreBasisPoints: 9000, EvaluatorID: "independent-evaluator", EvidenceRef: "evaluation"}}}
	if err := store.ActivateInferencePolicy(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	metadata, err := p.Catalog.Metadata(p)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := inference.NewConnectionRegistry(store, []inference.Connection{{ID: "account", Adapter: model, Metadata: &metadata}})
	if err != nil {
		t.Fatal(err)
	}
	request := modelinput.RouteRequirements{OrganizationID: p.OrganizationID, Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 100, OutputTokens: 20, Locality: modelinput.LocalOnly, DataClass: "internal", RequireHealthy: true, MaxLatencyMilliseconds: 100, Evaluation: &modelinput.EvaluationRequirement{Metric: "classification-v1", MinimumScoreBasisPoints: 9000}}
	selected, err := registry.Select(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	call := testInferenceRequest("healthy-attempt")
	call.ConnectionID, call.Descriptor = "account", descriptor
	call.Scope.Routing = &request
	call.Scope.RoutingDecision = &selected.Decision
	reserved, err := store.ReserveInference(t.Context(), call)
	if err != nil {
		t.Fatal("healthy evidence rejected at admission", err)
	}
	if _, err := store.ReconcileInference(t.Context(), reserved, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	// A fresh selection remains advisory if its evidence expires before dispatch.
	selected, err = registry.Select(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	call = testInferenceRequest("expired-attempt")
	call.ConnectionID, call.Descriptor = "account", descriptor
	call.Scope.Routing = &request
	call.Scope.RoutingDecision = &selected.Decision
	store.now = func() time.Time { return now.Add(time.Minute) }
	if _, err := store.ReserveInference(t.Context(), call); err == nil {
		t.Fatal("expired health evidence admitted")
	}
	if _, err := registry.Select(t.Context(), request); err == nil {
		t.Fatal("expired health evidence selected")
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal("current expiry invalidated valid historical evidence", err)
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM inference_reservations").Scan(&count); err != nil || count != 1 {
		t.Fatal("failed admission changed reservations", count, err)
	}
}
