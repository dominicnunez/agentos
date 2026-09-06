package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestInferenceSelectionUsesSharedLedgerBudget(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "selection.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	model := execution.FakeModel{}
	descriptor := model.Descriptor()
	var policies []inference.Policy
	var connections []inference.Connection
	for _, id := range []string{"large", "small"} {
		p := testInferencePolicy(now)
		p.Version, p.ConnectionID = inference.ConnectionPolicyVersion, id
		p.Provider, p.Model, p.ExecutionProfileVersion = descriptor.Provider, descriptor.Model, descriptor.ExecutionProfileVersion
		p.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 120, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
		if id == "small" {
			p.MaxInputTokensPerRequest, p.MaxOutputTokensPerRequest = 50, 10
		}
		p.Routing = &inference.RoutePolicy{OrganizationID: p.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
		p.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: p.AuthorizationExpiresAt}
		metadata, err := p.Catalog.Metadata(p)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, inference.Connection{ID: id, Adapter: model, Metadata: &metadata})
		policies = append(policies, p)
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	registry, err := inference.NewConnectionRegistry(store, connections)
	if err != nil {
		t.Fatal(err)
	}
	request := inference.RouteRequirements{OrganizationID: "organization-1", Capabilities: []inference.Capability{inference.Text}, InputTokens: 50, OutputTokens: 10, Locality: inference.CloudAllowed, DataClass: "internal", PreferredConnections: []string{"large"}}
	selected, err := registry.Select(t.Context(), request)
	if err != nil || selected.ConnectionID != "large" {
		t.Fatalf("initial selection=%+v err=%v", selected, err)
	}
	initialSequence := selected.Decision.SnapshotSequence
	if selected.ValidateFor(request) != nil || initialSequence <= 0 || selected.Decision.SharedBudgetRejections != 0 || !selected.Decision.SelectedAt.Equal(now) {
		t.Fatalf("initial decision lost its verified snapshot: %+v", selected.Decision)
	}
	call := testInferenceRequest("small-use")
	call.ConnectionID, call.Descriptor = "small", descriptor
	if _, err := store.ReserveInference(t.Context(), call); err == nil {
		t.Fatal("catalog call omitted its constraints")
	}
	constraints := request.Clone()
	call.Scope.Routing = &constraints
	constraints.DataClass = "secret"
	if _, err := store.ReserveInference(t.Context(), call); err == nil {
		t.Fatal("unapproved classification admitted")
	}
	constraints.DataClass = "internal"
	reservation, err := store.ReserveInference(t.Context(), call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
		t.Fatal(err)
	}
	selected, err = registry.Select(t.Context(), request)
	if err != nil || selected.ConnectionID != "small" {
		t.Fatalf("shared budget selection=%+v err=%v", selected, err)
	}
	var currentSequence int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT MAX(sequence) FROM events`).Scan(&currentSequence); err != nil {
		t.Fatal(err)
	}
	if selected.ValidateFor(request) != nil || selected.Decision.SharedBudgetRejections != 1 || selected.Decision.SnapshotSequence != currentSequence || currentSequence <= initialSequence {
		t.Fatalf("decision lost shared-budget filtering or ledger cutoff: %+v", selected.Decision)
	}
	request.ConnectionID = "large"
	if _, err := registry.Select(t.Context(), request); err == nil {
		t.Fatal("explicit route bypassed shared budget")
	}
	request.ConnectionID = ""
	request.Locality = inference.LocalOnly
	if _, err := registry.Select(t.Context(), request); err == nil {
		t.Fatal("local-only request selected cloud")
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("selection mutated accounting: count=%d err=%v", count, err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,'$.routing.data_class','secret') AS BLOB) WHERE event_type='INFERENCE_RESERVED'`); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
		t.Fatal("replay accepted prohibited classification")
	}
}
