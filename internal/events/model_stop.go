package events

import (
	"context"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

// ModelStopRequest closes one manifested auxiliary invocation. It does not
// assert that the local handler returned or that remote computation stopped.
type ModelStopRequest struct {
	ContextEventRef string                  `json:"context_event_ref"`
	ReasonClass     string                  `json:"reason_class"`
	Hold            *core.SecurityHoldCause `json:"hold,omitempty"`
}

// ModelStopResult distinguishes missing acknowledgement, a fenced unstarted
// invocation, and local return.
// Confirmation grants neither remote-stop knowledge nor permission to replay.
type ModelStopResult struct {
	StopRequestRef string                     `json:"stop_request_ref"`
	LocalState     string                     `json:"local_state,omitempty"`
	ReturnedAt     *time.Time                 `json:"returned_at,omitempty"`
	UsageEventRef  string                     `json:"usage_event_ref,omitempty"`
	ProviderStop   *core.ProviderStopEvidence `json:"provider_stop,omitempty"`
}

// ModelStopReturn is runtime-owned evidence that a start fence prevented the
// invocation (NOT_STARTED), or that the invoked model handler returned (RETURNED).
type ModelStopReturn struct {
	LocalState   string
	ReturnedAt   time.Time
	Usage        *InferenceUsageRecordedPayload
	ProviderStop *core.ProviderStopEvidence
}

type modelStopStore interface {
	RequestModelStop(context.Context, string, string) (Event, bool, error)
	RecordModelStop(context.Context, string, *ModelStopReturn) (Event, error)
}

// RequestModelStop derives scope from the exact manifest. completed reports an
// ordinary durable result/failure which committed before this stop request.
func (g *Gateway) RequestModelStop(ctx context.Context, contextEventRef, reason string) (Event, bool, error) {
	store, ok := g.ledger.(modelStopStore)
	if !ok {
		return Event{}, false, fmt.Errorf("durable model stop is unavailable")
	}
	return store.RequestModelStop(ctx, contextEventRef, reason)
}

// RecordModelStop records uncertainty for nil, a fenced unstarted invocation,
// or local return with atomic accounting. It never publishes discarded output.
func (g *Gateway) RecordModelStop(ctx context.Context, requestRef string, returned *ModelStopReturn) (Event, error) {
	store, ok := g.ledger.(modelStopStore)
	if !ok {
		return Event{}, fmt.Errorf("durable model stop is unavailable")
	}
	return store.RecordModelStop(ctx, requestRef, returned)
}

func RequiresModelStopAdmission(eventType string) bool {
	return eventType == "MODEL_STOP_REQUESTED" || eventType == "MODEL_STOP_UNCERTAIN" || eventType == "MODEL_STOP_CONFIRMED"
}

func ValidModelStopReason(reason string) bool {
	switch reason {
	case "security_hold", "containment_unavailable", "runtime_shutdown", "caller_cancelled", "deadline_exceeded":
		return true
	}
	return false
}
