package intake

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type stopNormalizationLedger struct {
	*ledger.SQLite
	eventType string
	after     bool
	stop      func()
}

func (l *stopNormalizationLedger) Append(ctx context.Context, draft events.TrustedDraft) (events.Event, error) {
	trigger := func(after bool) {
		if l.stop != nil && draft.EventType == l.eventType && after == l.after {
			stop := l.stop
			l.stop = nil
			stop()
		}
	}
	trigger(false)
	event, err := l.SQLite.Append(ctx, draft)
	if err == nil {
		trigger(true)
	}
	return event, err
}

type countedNormalizer struct {
	Normalizer
	calls int
}

func (n *countedNormalizer) Normalize(ctx context.Context, turns []ConversationTurn) (Normalization, error) {
	n.calls++
	return n.Normalizer.Normalize(ctx, turns)
}

func TestNormalizationStopAdmission(t *testing.T) {
	for _, test := range []struct {
		name, boundary                           string
		after                                    bool
		calls, manifest, stops, drafts, failures int
	}{
		{name: "before manifest", boundary: "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"},
		{name: "after manifest", boundary: "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", after: true, manifest: 1, stops: 1},
		{name: "after usage", boundary: "INFERENCE_USAGE_RECORDED", after: true, calls: 1, manifest: 1, stops: 1},
		{name: "before draft", boundary: "INTENT_DRAFTED", calls: 1, manifest: 1, stops: 1},
		{name: "committed draft", boundary: "INTENT_DRAFTED", after: true, calls: 1, manifest: 1, drafts: 1},
		{name: "before failure", boundary: "INTENT_NORMALIZATION_FAILED", calls: 1, manifest: 1, stops: 1},
		{name: "committed failure", boundary: "INTENT_NORMALIZATION_FAILED", after: true, calls: 1, manifest: 1, failures: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			writer := &stopNormalizationLedger{SQLite: store, eventType: test.boundary, after: test.after}
			gateway := events.NewGateway(writer)
			runtime := app.New(gateway)
			writer.stop = runtime.StopExecutions
			response := `{"state":"READY_FOR_REVIEW","reply":"Review this intent.","intent":{"mode":"STANDARD","objective":"Prepare a Linux release","context":[],"deliverables":[{"value":"Linux binary","origin":"EXPLICIT","source_message_id":"message-1"}],"completion_criteria":[{"value":"Binary passes verification","origin":"EXPLICIT","source_message_id":"message-1"}],"constraints":[],"resolved_decisions":[],"consequence_candidates":[],"missing_user_inputs":[]}}`
			if test.boundary == "INTENT_NORMALIZATION_FAILED" {
				response = `{}`
			}
			base, err := NewModelNormalizer(normalizationModel{response: response})
			if err != nil {
				t.Fatal(err)
			}
			normalizer := &countedNormalizer{Normalizer: base}
			service := NewWithNormalizer(runtime, normalizer)
			principal := testPrincipal("human-1", core.PrincipalHuman, ChannelHumanDirect)
			message := Message{ConversationID: "stop-normalization", MessageID: "message-1", Text: "Prepare a Linux release"}
			_, handleErr := service.Handle(t.Context(), principal, message)
			if writer.stop != nil {
				t.Fatalf("stop boundary was not reached: %v", handleErr)
			}
			if test.drafts == 0 && handleErr == nil {
				t.Fatal("interrupted or rejected draft succeeded")
			}
			if test.drafts == 1 && handleErr != nil {
				t.Fatalf("committed draft was discarded: %v", handleErr)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := runtime.WaitForStops(ctx); err != nil {
				t.Fatal(err)
			}
			stream := externalStream(t, store, message.ConversationID)
			if normalizer.calls != test.calls || countEvents(stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED") != test.manifest || countEvents(stream, "MODEL_STOP_REQUESTED") != test.stops || countEvents(stream, "MODEL_STOP_CONFIRMED") != test.stops || countEvents(stream, "INTENT_DRAFTED") != test.drafts || countEvents(stream, "INTENT_NORMALIZATION_FAILED") != test.failures || countEvents(stream, "INFERENCE_USAGE_RECORDED") != test.calls {
				t.Fatalf("normalization stop boundary mismatch: calls=%d events=%+v", normalizer.calls, stream)
			}
			for _, event := range stream {
				if event.EventType != "MODEL_STOP_CONFIRMED" {
					continue
				}
				var detail events.ModelStopResult
				if err := json.Unmarshal(event.Payload, &detail); err != nil {
					t.Fatal(err)
				}
				want := "RETURNED"
				if test.calls == 0 {
					want = "NOT_STARTED"
				}
				if detail.LocalState != want {
					t.Fatalf("local stop proof=%+v", detail)
				}
			}
			if _, _, err := app.New(events.NewGateway(store)).OrganizationState(t.Context(), core.ID(principal.OrganizationID)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
