package app

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	"strings"
	"testing"
)

func TestCanceledRejectionPersistsExactMessageWithoutPrivateCause(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var first events.Event
	for _, id := range []string{"first", "newer"} {
		e, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org", CorrelationID: "conversation", SourceActorID: "human", EventType: "INTAKE_MESSAGE_RECORDED", Payload: events.IntakeMessageRecordedPayload{MessageID: id}})
		if err != nil {
			t.Fatal(err)
		}
		if id == "first" {
			first = e
		}
	}
	service := New(events.NewGateway(store))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cause := errors.Join(context.Canceled, errors.New("synthetic-private-canary"))
	result := service.RecordInferenceRouteRejection(ctx, "org", "conversation", "INTENT_NORMALIZATION", "first", cause)
	if !errors.Is(result, context.Canceled) {
		t.Fatal("lost cancellation identity", result)
	}
	stream, err := store.Events(t.Context(), "conversation")
	if err != nil {
		t.Fatal(err)
	}
	if len(stream) != 3 {
		t.Fatalf("diagnostic missing: events=%d result=%v", len(stream), result)
	}
	var payload events.InferenceRouteRejectedPayload
	if err := json.Unmarshal(stream[2].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Category != "CANCELED" || payload.OriginEventRef != first.EventID || strings.Contains(string(stream[2].Payload), "synthetic-private-canary") {
		t.Fatal("wrong or unsafe rejection", payload)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.RecordInferenceRouteRejection(t.Context(), "org", "conversation", "INTENT_NORMALIZATION", "missing", cause); err == nil || !strings.Contains(err.Error(), "origin is unavailable") {
		t.Fatal("missing origin was not reported", err)
	}
}
