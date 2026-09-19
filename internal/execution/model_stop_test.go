package execution

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestModelStopOutcomeSurvivesSafeModelError(t *testing.T) {
	want := ModelStopOutcome{
		LocalTurnStopped:          true,
		LocalProcessStopAttempted: true,
		LocalProcessStopped:       true,
		RemoteStatus:              RemoteStopUncertain,
	}
	private := errors.New("private provider stop detail")
	safe := SafeModelError(ModelCallFailed, withModelStopOutcome(errors.Join(context.Canceled, private), want))
	got, ok := StopOutcome(safe)
	if !ok || got != want {
		t.Fatalf("stop outcome=%+v found=%v", got, ok)
	}
	if !errors.Is(safe, context.Canceled) || strings.Contains(safe.Error(), private.Error()) {
		t.Fatalf("safe error lost cancellation or exposed diagnostics: %v", safe)
	}

	resanitized := SafeModelError(ModelCallFailed, safe)
	got, ok = StopOutcome(resanitized)
	if !ok || got != want {
		t.Fatalf("resanitized stop outcome=%+v found=%v", got, ok)
	}

	guardWrapped := SafeModelError(InferenceRecordFailed, errors.Join(resanitized, errors.New("private accounting failure")))
	guardWrapped = WithReconciledUsage(guardWrapped, events.InferenceUsageRecordedPayload{
		Source: "provider_cli", Provider: "codex-subscription", Model: "gpt-test", InputTokens: 3, OutputTokens: 2, TotalTokens: 5,
	})
	got, ok = StopOutcome(guardWrapped)
	if !ok || got != want || !errors.Is(guardWrapped, context.Canceled) {
		t.Fatalf("guard-wrapped stop outcome=%+v found=%v err=%v", got, ok, guardWrapped)
	}

	joinedAfterFaultWithoutEvidence := errors.Join(SafeModelError(ModelCallFailed, errors.New("unrelated failure")), guardWrapped)
	got, ok = StopOutcome(joinedAfterFaultWithoutEvidence)
	if !ok || got != want {
		t.Fatalf("joined stop outcome=%+v found=%v", got, ok)
	}
}
