package app

import (
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/projections"
	"strings"
	"testing"
)

func TestNormalizationRejectsUnpairedRoutingBeforePublication(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	gateway := events.NewGateway(store)
	seedTestGoal(t, t.Context(), projections.New(gateway), "org", "mission", "goal", core.GoalActive)
	service := New(gateway)
	before, err := service.RecordIntakeMessage(t.Context(), IntakeMessage{RequestID: "request", OrganizationID: "org", MessageID: "message", Text: "perform work", SourcePrincipalID: "user", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT"})
	if err != nil {
		t.Fatal(err)
	}
	base := IntentNormalizationContext{ExecutionID: "normalization", SourceMessageID: "message", PromptVersion: "v1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake"}
	for _, missing := range []string{"decision", "requirements"} {
		in := base
		if missing == "decision" {
			in.Routing = &modelinput.RouteRequirements{OrganizationID: "org", Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 100, OutputTokens: 20, Locality: modelinput.LocalOnly, DataClass: "internal"}
		} else {
			in.RoutingDecision = &modelinput.RouteDecision{}
		}
		if _, err := service.RecordIntentNormalizationContext(t.Context(), "org", "request", in); err == nil || !strings.Contains(err.Error(), "present together") {
			t.Fatalf("missing %s was not rejected before publication: %v", missing, err)
		}
		after, err := service.ExternalEvents(t.Context(), "org", "request")
		if err != nil || len(after) != len(before) {
			t.Fatalf("invalid context changed history: %d -> %d, %v", len(before), len(after), err)
		}
		if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
			t.Fatal("rejected input corrupted history", err)
		}
	}
	if _, err := service.RecordIntentNormalizationContext(t.Context(), "org", "request", base); err != nil {
		t.Fatal("legacy context rejected", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal("valid context corrupted history", err)
	}
}

func TestAuxiliaryRetryIdentityIncludesConnection(t *testing.T) {
	first := events.InferenceUsageRecordedPayload{ConnectionID: "first", Source: "provider", Provider: "same", Model: "same"}
	second := first
	second.ConnectionID = "second"
	if sameInferenceUsage(first, second) {
		t.Fatal("retry substituted a different usage account")
	}
	if !sameInferenceUsage(first, first) {
		t.Fatal("identical usage rejected")
	}
	context := events.IntentNormalizationContextPayload{ConnectionID: "first", SourceMessageID: "message", Provider: "same", Model: "same"}
	changed := context
	changed.ConnectionID = "second"
	if sameNormalizationContext(context, changed) {
		t.Fatal("retry substituted a different normalization account")
	}
	context.Routing = &modelinput.RouteRequirements{OrganizationID: "org", Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 10, OutputTokens: 5, Locality: modelinput.LocalOnly, DataClass: "internal"}
	changed = context
	changed.Routing = modelinput.CloneRouteRequirements(context.Routing)
	changed.Routing.DataClass = "public"
	if sameNormalizationContext(context, changed) {
		t.Fatal("retry changed normalization classification")
	}
	changed.Routing = nil
	if sameNormalizationContext(context, changed) {
		t.Fatal("retry removed normalization requirements")
	}
}
