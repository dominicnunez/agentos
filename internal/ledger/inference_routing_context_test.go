package ledger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

func TestAuxiliaryRoutingContextRejectsDifferentValidConstraints(t *testing.T) {
	for _, purpose := range []inference.Purpose{inference.PurposePlanning, inference.PurposeIntentNormalization} {
		routing := inference.RouteRequirements{OrganizationID: "org", Capabilities: []inference.Capability{inference.Text}, InputTokens: 100, OutputTokens: 20, Locality: inference.CloudAllowed, DataClass: "internal"}
		fingerprint, err := routing.Fingerprint()
		if err != nil {
			t.Fatal(err)
		}
		decision := modelinput.RouteDecision{Version: 1, RequirementsFingerprint: fingerprint, PolicyFingerprint: modelinput.TextDigest("policy"), SelectedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC), SnapshotSequence: 1, ConnectionID: "account", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile", Reason: modelinput.RouteOrdered, ReservedInputTokens: 100, ReservedOutputTokens: 20}
		if err := decision.ValidateFor(routing); err != nil {
			t.Fatal(err)
		}
		payload := events.InferenceReservedPayload{Purpose: string(purpose), RequestID: "execution", IntentID: "intent", ConnectionID: "account", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile", ExecutionManifestRef: "context", Routing: &routing, RoutingDecision: modelinput.CloneRouteDecision(&decision)}
		reservation := events.Event{OrganizationID: "org", SourceExecutionID: "execution", TaskID: "task", CorrelationID: "work", Sequence: 3}
		manifest := reservation
		manifest.EventID, manifest.SourceActorID, manifest.Sequence = "context", "runtime", 2
		var body any = events.PlanningContextPayload{ConnectionID: "account", IntentID: "intent", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile", Routing: &routing, RoutingDecision: &decision}
		manifest.EventType = "PLANNING_CONTEXT_MANIFESTED"
		if purpose == inference.PurposeIntentNormalization {
			manifest.EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
			body = events.IntentNormalizationContextPayload{ConnectionID: "account", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile", Routing: &routing, RoutingDecision: &decision}
		}
		manifest.Payload, _ = json.Marshal(body)
		if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err != nil {
			t.Fatal(err)
		}
		payload.RoutingDecision.SnapshotSequence++
		if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err == nil {
			t.Fatal("reservation substituted decision snapshot")
		}
		payload.RoutingDecision = nil
		if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err == nil {
			t.Fatal("reservation removed decision")
		}
		payload.RoutingDecision = modelinput.CloneRouteDecision(&decision)
		manifest.Sequence = 1
		if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err == nil {
			t.Fatal("decision snapshot did not precede context")
		}
		manifest.Sequence = 2
		routing.DataClass = "public"
		if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err == nil {
			t.Fatal("different valid data classification lost its origin binding")
		}
		payload.Routing = nil
		if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err == nil {
			t.Fatal("removed routing constraints accepted")
		}
	}
}

func TestTaskRoutingConstraintsCannotBeAddedAfterManifest(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateInferenceKnowledge(t.Context(), tx, request); err != nil {
		t.Fatal(err)
	}
	request.Scope.Routing = &inference.RouteRequirements{OrganizationID: request.Scope.OrganizationID, Capabilities: []inference.Capability{inference.Text}, InputTokens: 100, OutputTokens: 20, Locality: inference.CloudAllowed, DataClass: "internal"}
	if err := validateInferenceKnowledge(t.Context(), tx, request); err == nil {
		t.Fatal("request substituted constraints after execution manifest")
	}
}
