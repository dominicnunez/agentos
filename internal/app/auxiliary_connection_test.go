package app

import (
	"github.com/dominicnunez/agentos/internal/events"
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
}
