package ledger

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestRoutingPolicyChangesRequireCompleteAtomicSet(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	store, err := Open(filepath.Join(t.TempDir(), "routing-policy.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.now = func() time.Time { return now }
	var policies []inference.Policy
	for _, id := range []string{"first", "second"} {
		p := testInferencePolicy(now)
		p.Version, p.ConnectionID = inference.ConnectionPolicyVersion, id
		p.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
		p.Routing = &inference.RoutePolicy{OrganizationID: p.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
		policies = append(policies, p)
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	for i := range policies {
		policies[i].AuthorizedAt = policies[i].AuthorizedAt.Add(time.Minute)
		policies[i].Routing = &inference.RoutePolicy{OrganizationID: policies[i].OrganizationID, Locality: inference.LocalOnly, DataClasses: []string{"internal"}}
	}
	if err := store.ActivateInferencePolicy(t.Context(), policies[0]); err == nil {
		t.Fatal("singleton update left conflicting routing rules")
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies[:1]); err == nil {
		t.Fatal("partial policy set left conflicting routing rules")
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("rejected changes corrupted history: %v", err)
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
		t.Fatalf("complete update failed: %v", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("atomic update replay failed: %v", err)
	}
	call := testInferenceRequest("prohibited-cloud")
	call.ConnectionID = "first"
	if _, err := store.ReserveInference(t.Context(), call); err == nil {
		t.Fatal("new local-only policy did not prohibit cloud reservation")
	}
}

func TestRoutingHistoryRejectsInterruptedPolicySet(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	activations := make(map[string]inference.Policy)
	var stream []events.Event
	for _, id := range []string{"first-old", "second-old", "first-new", "second-new"} {
		connection := "first"
		if id == "second-old" || id == "second-new" {
			connection = "second"
		}
		p := testInferencePolicy(now)
		p.Version, p.ConnectionID = inference.ConnectionPolicyVersion, connection
		p.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
		p.Routing = &inference.RoutePolicy{OrganizationID: p.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
		if id == "first-new" || id == "second-new" {
			p.Routing.Locality = inference.LocalOnly
			p.AuthorizedAt = p.AuthorizedAt.Add(time.Minute)
		}
		activations[id] = p
		stream = append(stream, events.Event{EventID: id, OrganizationID: p.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED"})
	}
	if err := validateOrganizationBudgetHistory(stream, activations); err != nil {
		t.Fatal(err)
	}
	if err := validateOrganizationBudgetHistory(stream[:3], activations); err == nil {
		t.Fatal("unfinished routing revision accepted")
	}
	interrupted := append([]events.Event(nil), stream[:3]...)
	interrupted = append(interrupted, events.Event{EventType: "MESSAGE", OrganizationID: "organization-1"}, stream[3])
	if err := validateOrganizationBudgetHistory(interrupted, activations); err == nil {
		t.Fatal("intervening event accepted during routing disagreement")
	}
}
