package ledger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

func decisionHistoryFixture(t *testing.T) (inference.Policy, modelinput.RouteRequirements, modelinput.RouteDecision) {
	t.Helper()
	now := time.Date(2026, 9, 6, 12, 30, 0, 0, time.UTC)
	policy := testInferencePolicy(now)
	policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "account"
	policy.MaxTokensPerWindow, policy.ContinuityReserveTokens = 1000, 0
	policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 120, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 4}
	policy.Routing = &inference.RoutePolicy{OrganizationID: policy.OrganizationID, Locality: inference.CloudAllowed, DataClasses: []string{"internal"}}
	policy.Catalog = &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, ContextTokens: 120, OutputTokens: 20, DataClasses: []string{"internal"}, ValidUntil: policy.AuthorizationExpiresAt}
	metadata, err := policy.Catalog.Metadata(policy)
	if err != nil {
		t.Fatal(err)
	}
	requirements := modelinput.RouteRequirements{OrganizationID: policy.OrganizationID, Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 100, OutputTokens: 20, Locality: modelinput.CloudAllowed, DataClass: "internal"}
	selected, err := (inference.Broker{Routes: []inference.RouteMetadata{metadata}, Manager: inference.Manager{Pools: []inference.Pool{{ID: "account", Policy: policy, Available: true}}}}).Select(now, requirements, *policy.Routing)
	if err != nil {
		t.Fatal(err)
	}
	selected.Decision.SnapshotSequence = 1
	return policy, requirements, selected.Decision
}

func decisionHistoryEvent(t *testing.T, sequence int64, organization, kind string, body any) events.Event {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return events.Event{Sequence: sequence, OrganizationID: organization, EventType: kind, Payload: payload}
}

func TestRoutingDecisionUsesPolicyAtSelectionNotCurrentPolicy(t *testing.T) {
	policy, requirements, decision := decisionHistoryFixture(t)
	changed := policy
	changed.MaxTokensPerWindow++
	activations := map[string]inference.Policy{"old": policy, "new": changed}
	stream := []events.Event{
		{EventID: "old", Sequence: 1, OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: decision.SelectedAt.Add(-time.Minute)},
		{EventID: "new", Sequence: 2, OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: decision.SelectedAt.Add(time.Minute)},
	}
	check := func(d modelinput.RouteDecision) error {
		origin := decisionHistoryEvent(t, 3, policy.OrganizationID, "PLANNING_CONTEXT_MANIFESTED", events.PlanningContextPayload{Routing: &requirements, RoutingDecision: &d})
		return validateRoutingDecisionHistory(append(stream, origin), activations, nil)
	}
	if err := check(decision); err != nil {
		t.Fatal("later policy change invalidated legitimate selection", err)
	}
	decision.SnapshotSequence = 2
	if err := check(decision); err == nil {
		t.Fatal("old policy claimed active after replacement")
	}
	decision.PolicyFingerprint, _ = changed.Fingerprint()
	if err := check(decision); err == nil {
		t.Fatal("selection timestamp predates activation")
	}
	decision.SelectedAt = stream[1].CreatedAt
	if err := check(decision); err != nil {
		t.Fatal(err)
	}
	decision.SelectedAt = changed.AuthorizationExpiresAt
	if err := check(decision); err == nil {
		t.Fatal("expired historical authorization accepted")
	}
}

func TestRoutingDecisionCannotUseLaterRefundOrFrozenSnapshot(t *testing.T) {
	policy, requirements, decision := decisionHistoryFixture(t)
	other := policy
	other.ConnectionID = "other"
	start, end := inferenceWindow(decision.SelectedAt, time.Hour)
	stream := []events.Event{
		{EventID: "selected", Sequence: 1, OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: policy.AuthorizedAt},
		{EventID: "other", Sequence: 2, OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: policy.AuthorizedAt},
		decisionHistoryEvent(t, 3, policy.OrganizationID, "INFERENCE_RESERVED", events.InferenceReservedPayload{ReservationID: "prior", ConnectionID: "other", Provider: policy.Provider, Model: policy.Model, ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: decision.ReservedCostNanoUSD, AdmittedAt: decision.SelectedAt.Format(time.RFC3339Nano), WindowStartedAt: start, WindowExpiresAt: end}),
		decisionHistoryEvent(t, 4, policy.OrganizationID, "INFERENCE_RECONCILED", events.InferenceReconciledPayload{ReservationID: "prior"}),
	}
	activations := map[string]inference.Policy{"selected": policy, "other": other}
	check := func(cutoff int64, freezes map[core.ID][]events.OrganizationFreezeAdmission) error {
		decision.SnapshotSequence = cutoff
		origin := decisionHistoryEvent(t, 5, policy.OrganizationID, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", events.IntentNormalizationContextPayload{Routing: &requirements, RoutingDecision: &decision})
		return validateRoutingDecisionHistory(append(stream, origin), activations, freezes)
	}
	if err := check(3, nil); err == nil {
		t.Fatal("later refund justified selection over shared budget")
	}
	if err := check(4, nil); err != nil {
		t.Fatal("refunded capacity remained unavailable", err)
	}
	freezes := map[core.ID][]events.OrganizationFreezeAdmission{core.ID(policy.OrganizationID): {{OrganizationID: core.ID(policy.OrganizationID), Frozen: true, Sequence: 2}}}
	if err := check(4, freezes); err == nil {
		t.Fatal("frozen snapshot accepted")
	}
	// Same-account outstanding calls must also enforce the account concurrency cap.
	policy.OrganizationBudget.MaxTokensPerWindow = 1000
	var err error
	decision.PolicyFingerprint, err = policy.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	activations["selected"] = policy
	prior := events.InferenceReservedPayload{ReservationID: "prior", ConnectionID: policy.ConnectionID, Provider: policy.Provider, Model: policy.Model, ReservedInputTokens: 100, ReservedOutputTokens: 20, ReservedCostNanoUSD: decision.ReservedCostNanoUSD, AdmittedAt: decision.SelectedAt.Format(time.RFC3339Nano), WindowStartedAt: start, WindowExpiresAt: end}
	stream[2] = decisionHistoryEvent(t, 3, policy.OrganizationID, "INFERENCE_RESERVED", prior)
	if err := check(3, nil); err == nil {
		t.Fatal("account concurrency bypassed at historical cutoff")
	}
}

func TestRoutingDecisionRejectsMissingCutoffAndChangedEstimate(t *testing.T) {
	policy, requirements, decision := decisionHistoryFixture(t)
	stream := []events.Event{{EventID: "policy", Sequence: 1, OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: policy.AuthorizedAt}}
	for _, mutate := range []func(*modelinput.RouteDecision){
		func(d *modelinput.RouteDecision) { d.SnapshotSequence = 2 },
		func(d *modelinput.RouteDecision) { d.PolicyFingerprint = modelinput.TextDigest("unavailable") },
		func(d *modelinput.RouteDecision) { d.ReservedInputTokens++ },
		func(d *modelinput.RouteDecision) { d.ReservedCostNanoUSD++ },
	} {
		changed := decision
		mutate(&changed)
		origin := decisionHistoryEvent(t, 3, policy.OrganizationID, "PLANNING_CONTEXT_MANIFESTED", events.PlanningContextPayload{Routing: &requirements, RoutingDecision: &changed})
		if err := validateRoutingDecisionHistory(append(stream, origin), map[string]inference.Policy{"policy": policy}, nil); err == nil {
			t.Fatal("invalid historical decision accepted")
		}
	}
}
