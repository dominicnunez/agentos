package ledger

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestAuxiliaryInferenceLedgerAdmissionAndReplay(t *testing.T) {
	for _, purpose := range []inference.Purpose{inference.PurposePlanning, inference.PurposeIntentNormalization} {
		t.Run(string(purpose), func(t *testing.T) {
			now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			store, err := Open(filepath.Join(t.TempDir(), "auxiliary.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			store.now = func() time.Time { return now }
			var policies []inference.Policy
			for _, account := range []string{"first", "second"} {
				policy := testInferencePolicy(now)
				policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, account
				policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
				policies = append(policies, policy)
			}
			if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("auxiliary-execution")
			request.Scope.Purpose, request.ConnectionID = purpose, "first"
			eventType := "PLANNING_CONTEXT_MANIFESTED"
			var body any = events.PlanningContextPayload{ConnectionID: "first", IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
			if purpose == inference.PurposeIntentNormalization {
				eventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
				body = events.IntentNormalizationContextPayload{ConnectionID: "first", Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
			}
			manifest, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: request.Scope.OrganizationID, EventType: eventType, SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: body})
			if err != nil {
				t.Fatal(err)
			}
			wrong := request
			wrong.ConnectionID = "second"
			if _, err := store.ReserveInference(t.Context(), wrong); err == nil || !strings.Contains(err.Error(), "admitted context") {
				t.Fatalf("substituted account admission: %v", err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatalf("correct admission after rollback: %v", err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("valid replay: %v", err)
			}
			var original []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_type='INFERENCE_RESERVED'`).Scan(&original); err != nil {
				t.Fatal(err)
			}
			var payload events.InferenceReservedPayload
			if err := json.Unmarshal(original, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.ExecutionManifestRef != manifest.EventID {
				t.Fatal("reservation did not persist exact auxiliary context reference")
			}
			for _, ref := range []string{"", "substituted-context"} {
				payload.ExecutionManifestRef = ref
				changed, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, changed); err != nil {
					t.Fatal(err)
				}
				if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "auxiliary inference context reference") {
					t.Fatalf("tampered reference replay: %v", err)
				}
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, original); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("restored replay: %v", err)
			}
		})
	}
}

func TestAuxiliaryInferenceContextBinding(t *testing.T) {
	for _, purpose := range []inference.Purpose{inference.PurposePlanning, inference.PurposeIntentNormalization} {
		t.Run(string(purpose), func(t *testing.T) {
			payload := events.InferenceReservedPayload{Purpose: string(purpose), RequestID: "execution", IntentID: "intent", ExecutionManifestRef: "context", ConnectionID: "account", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile"}
			reservation := events.Event{OrganizationID: "org", SourceExecutionID: "execution", TaskID: "task", CorrelationID: "correlation", Sequence: 5}
			manifest := reservation
			manifest.EventID, manifest.SourceActorID, manifest.Sequence = "context", "runtime", 4
			var body any = events.PlanningContextPayload{ConnectionID: "account", IntentID: "intent", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile"}
			manifest.EventType = "PLANNING_CONTEXT_MANIFESTED"
			if purpose == inference.PurposeIntentNormalization {
				manifest.EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
				body = events.IntentNormalizationContextPayload{ConnectionID: "account", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile"}
			}
			manifest.Payload, _ = json.Marshal(body)
			if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err != nil {
				t.Fatal(err)
			}
			for _, mutation := range []struct {
				name  string
				apply func(*events.Event, *events.InferenceReservedPayload)
			}{
				{"account", func(_ *events.Event, p *events.InferenceReservedPayload) { p.ConnectionID = "other" }},
				{"missing reference", func(_ *events.Event, p *events.InferenceReservedPayload) { p.ExecutionManifestRef = "" }},
				{"substituted reference", func(_ *events.Event, p *events.InferenceReservedPayload) { p.ExecutionManifestRef = "other" }},
				{"provider", func(_ *events.Event, p *events.InferenceReservedPayload) { p.Provider = "other" }},
				{"tenant", func(e *events.Event, _ *events.InferenceReservedPayload) { e.OrganizationID = "other" }},
				{"correlation", func(e *events.Event, _ *events.InferenceReservedPayload) { e.CorrelationID = "other" }},
				{"task", func(e *events.Event, _ *events.InferenceReservedPayload) { e.TaskID = "other" }},
				{"execution", func(e *events.Event, _ *events.InferenceReservedPayload) { e.SourceExecutionID = "other" }},
				{"actor", func(e *events.Event, _ *events.InferenceReservedPayload) { e.SourceActorID = "other" }},
				{"future", func(e *events.Event, _ *events.InferenceReservedPayload) { e.Sequence = 6 }},
				{"type", func(e *events.Event, _ *events.InferenceReservedPayload) {
					e.EventType = "EXECUTION_CONTEXT_MANIFESTED"
				}},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					e, p := manifest, payload
					mutation.apply(&e, &p)
					if validateAuxiliaryInferenceContext(reservation, p, []events.Event{e}) == nil {
						t.Fatal("accepted invalid context binding")
					}
				})
			}
			if validateAuxiliaryInferenceContext(reservation, payload, nil) == nil {
				t.Fatal("accepted deleted context")
			}
			if validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest, manifest}) == nil {
				t.Fatal("accepted duplicate context")
			}
			payload.ExecutionManifestRef = ""
			if err := validateAuxiliaryInferenceContext(reservation, payload, nil); err != nil {
				t.Fatalf("library accounting history: %v", err)
			}
			payload.ConnectionID = ""
			switch value := body.(type) {
			case events.PlanningContextPayload:
				value.ConnectionID = ""
				manifest.Payload, _ = json.Marshal(value)
			case events.IntentNormalizationContextPayload:
				value.ConnectionID = ""
				manifest.Payload, _ = json.Marshal(value)
			}
			if err := validateAuxiliaryInferenceContext(reservation, payload, []events.Event{manifest}); err != nil {
				t.Fatalf("historical singleton context without reference: %v", err)
			}
		})
	}
}
