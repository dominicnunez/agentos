package app

import (
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"testing"
)

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
