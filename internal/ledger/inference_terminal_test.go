package ledger

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestTerminalInferenceAccountingSurvivesRestart(t *testing.T) {
	for _, state := range []inference.Reconciliation{
		inference.ReconciliationTerminalFailed, inference.ReconciliationTerminalIncomplete,
		inference.ReconciliationTerminalFailedNoUsage, inference.ReconciliationTerminalIncompleteNoUsage,
	} {
		t.Run(string(state), func(t *testing.T) {
			now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
			path := filepath.Join(t.TempDir(), "ledger.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			store.now = func() time.Time { return now }
			if err = store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("terminal"))
			if err != nil {
				t.Fatal(err)
			}
			usage := testInferenceUsage()
			var observed *events.InferenceUsageRecordedPayload
			wantCost := int64(400000)
			wantInput, wantOutput := int64(100), int64(20)
			if state == inference.ReconciliationTerminalFailed || state == inference.ReconciliationTerminalIncomplete {
				observed = &usage
				wantCost = 70000
				wantInput = 10
				wantOutput = 5
			}
			cost, err := store.ReconcileInference(t.Context(), reservation, observed, state)
			if err != nil || cost != wantCost {
				t.Fatalf("cost=%d err=%v", cost, err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.Close() })
			restarted.now = func() time.Time { return now.Add(time.Minute) }
			if err = restarted.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("terminal accounting failed historical validation: %v", err)
			}
			stream, err := restarted.Events(t.Context(), "work-1")
			if err != nil {
				t.Fatal(err)
			}
			if len(stream) != 2 {
				t.Fatalf("events=%d", len(stream))
			}
			var payload events.InferenceReconciledPayload
			if err = json.Unmarshal(stream[1].Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.State != string(state) || payload.ChargedInputTokens != wantInput || payload.ChargedOutputTokens != wantOutput || payload.ChargedCostNanoUSD != wantCost {
				t.Fatalf("replayed evidence=%+v", payload)
			}
			if _, err = restarted.ReserveInference(t.Context(), testInferenceRequest("terminal")); err == nil {
				t.Fatal("terminal request retried")
			}
		})
	}
}

func TestInvalidTerminalUsageCannotReleaseReservation(t *testing.T) {
	for _, name := range []string{"missing", "provider", "model", "connection", "negative", "over input", "over output"} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
			store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			store.now = func() time.Time { return now }
			if err = store.ActivateInferencePolicy(t.Context(), testInferencePolicy(now)); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), testInferenceRequest("invalid-terminal"))
			if err != nil {
				t.Fatal(err)
			}
			usage := testInferenceUsage()
			observed := &usage
			switch name {
			case "missing":
				observed = nil
			case "provider":
				usage.Provider = "other"
			case "model":
				usage.Model = "other"
			case "connection":
				usage.ConnectionID = "other"
			case "negative":
				usage.InputTokens = -1
				usage.TotalTokens = 4
			case "over input":
				usage.InputTokens = 101
				usage.TotalTokens = 106
			case "over output":
				usage.OutputTokens = 21
				usage.TotalTokens = 31
			}
			if _, err = store.ReconcileInference(t.Context(), reservation, observed, inference.ReconciliationTerminalIncomplete); err == nil {
				t.Fatal("invalid usage accepted")
			}
			var state string
			var input, output, cost int64
			if err = store.db.QueryRowContext(t.Context(), `SELECT state,charged_input_tokens,charged_output_tokens,charged_cost_nano_usd FROM inference_reservations WHERE reservation_id=?`, reservation.ID).Scan(&state, &input, &output, &cost); err != nil {
				t.Fatal(err)
			}
			if state != inferenceStateReserved || input != 100 || output != 20 || cost != 400000 {
				t.Fatalf("invalid evidence released quota: %s %d/%d/%d", state, input, output, cost)
			}
			if err = store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
