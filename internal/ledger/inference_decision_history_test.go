package ledger

import (
	"encoding/json"
	"path/filepath"
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
	policy.MaxConcurrentRequests = 2
	decision.PolicyFingerprint, err = policy.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	activations["selected"] = policy
	if err := check(3, nil); err != nil {
		t.Fatal("available second account slot rejected", err)
	}
}

func TestRoutingDecisionRejectsPartialPolicySetCutoff(t *testing.T) {
	policy, requirements, decision := decisionHistoryFixture(t)
	other := policy
	other.ConnectionID = "other"
	changed := policy
	rules := *policy.Routing
	rules.DataClasses = []string{"internal", "public"}
	changed.Routing = &rules
	changedOther := changed
	changedOther.ConnectionID = other.ConnectionID
	activations := map[string]inference.Policy{"old": policy, "other": other, "new": changed, "new-other": changedOther}
	stream := []events.Event{}
	for i, id := range []string{"old", "other", "new", "new-other"} {
		stream = append(stream, events.Event{EventID: id, Sequence: int64(i + 1), OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: decision.SelectedAt})
	}
	decision.PolicyFingerprint, _ = changed.Fingerprint()
	for _, cutoff := range []int64{3, 4} {
		decision.SnapshotSequence = cutoff
		origin := decisionHistoryEvent(t, 5, policy.OrganizationID, "PLANNING_CONTEXT_MANIFESTED", events.PlanningContextPayload{Routing: &requirements, RoutingDecision: &decision})
		err := validateRoutingDecisionHistory(append(stream, origin), activations, nil)
		if cutoff == 3 && err == nil {
			t.Fatal("partial policy-set cutoff accepted")
		}
		if cutoff == 4 && err != nil {
			t.Fatal("complete policy-set cutoff rejected", err)
		}
	}
}

func TestRoutingDecisionStandalonePendingRequestRejectsForgedPolicy(t *testing.T) {
	policy, requirements, decision := decisionHistoryFixture(t)
	store, err := Open(filepath.Join(t.TempDir(), "pending.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// Use the real event clock so the selected timestamp follows activation.
	now := time.Now().UTC()
	policy.AuthorizedAt = now.Add(-time.Minute)
	policy.AuthorizationExpiresAt = now.Add(time.Hour)
	policy.Pricing.ExpiresAt = policy.AuthorizationExpiresAt
	policy.Catalog.ValidUntil = policy.AuthorizationExpiresAt
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	decision.SelectedAt = time.Now().UTC()
	decision.PolicyFingerprint, err = policy.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(t.Context(), `SELECT MAX(sequence) FROM events`).Scan(&decision.SnapshotSequence); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("standalone")
	request.ConnectionID = policy.ConnectionID
	request.Scope.Routing, request.Scope.RoutingDecision = &requirements, &decision
	validFingerprint := decision.PolicyFingerprint
	decision.PolicyFingerprint = modelinput.TextDigest("forged policy")
	if _, err := store.ReserveInference(t.Context(), request); err == nil {
		t.Fatal("standalone request bypassed historical policy verification")
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM inference_reservations`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected decision changed accounting: count=%d err=%v", count, err)
	}
	decision.PolicyFingerprint = validFingerprint
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal("valid standalone decision rejected", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal("valid standalone reservation failed replay", err)
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

func TestRoutingDecisionCannotBackdateAccountingSnapshot(t *testing.T) {
	policy, requirements, decision := decisionHistoryFixture(t)
	start, end := inferenceWindow(decision.SelectedAt, time.Hour)
	reserved := events.InferenceReservedPayload{ReservationID: "prior", ConnectionID: policy.ConnectionID, Provider: policy.Provider, Model: policy.Model, ReservedInputTokens: 100, ReservedOutputTokens: 20, AdmittedAt: decision.SelectedAt.Format(time.RFC3339Nano), WindowStartedAt: start, WindowExpiresAt: end}
	stream := []events.Event{
		{EventID: "policy", Sequence: 1, OrganizationID: policy.OrganizationID, EventType: "INFERENCE_POLICY_ACTIVATED", CreatedAt: policy.AuthorizedAt},
		decisionHistoryEvent(t, 2, policy.OrganizationID, "INFERENCE_RESERVED", reserved),
		decisionHistoryEvent(t, 3, policy.OrganizationID, "INFERENCE_RECONCILED", events.InferenceReconciledPayload{ReservationID: "prior"}),
	}
	decision.SnapshotSequence = 3
	check := func() error {
		origin := decisionHistoryEvent(t, 4, policy.OrganizationID, "PLANNING_CONTEXT_MANIFESTED", events.PlanningContextPayload{Routing: &requirements, RoutingDecision: &decision})
		return validateRoutingDecisionHistory(append(stream, origin), map[string]inference.Policy{"policy": policy}, nil)
	}
	if err := check(); err != nil {
		t.Fatal("baseline refunded snapshot rejected", err)
	}
	stream[2].CreatedAt = decision.SelectedAt.Add(time.Second)
	if err := check(); err == nil {
		t.Fatal("selection predating refund timestamp accepted")
	}
	decision.SelectedAt = stream[2].CreatedAt
	if err := check(); err != nil {
		t.Fatal("selection at refund timestamp rejected", err)
	}
	reserved.AdmittedAt = decision.SelectedAt.Add(time.Second).Format(time.RFC3339Nano)
	stream[1] = decisionHistoryEvent(t, 2, policy.OrganizationID, "INFERENCE_RESERVED", reserved)
	if err := check(); err == nil {
		t.Fatal("selection predating admission timestamp accepted")
	}
}
