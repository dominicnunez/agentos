package execution

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestOpenAITerminalEvidencePreservesUsageWithoutOutput(t *testing.T) {
	for _, status := range []string{"incomplete", "failed"} {
		for _, withUsage := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/usage=%v", status, withUsage), func(t *testing.T) {
				body := openAITestResponseObject("must not escape")
				body["status"] = status
				body["output"] = []any{}
				if withUsage {
					body["usage"] = map[string]any{"input_tokens": 84, "output_tokens": 16, "total_tokens": 100}
				} else {
					body["usage"] = nil
				}
				encoded, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := decodeOpenAIResponse(encoded)
				if err != nil {
					t.Fatal(err)
				}
				adapter := &OpenAIAPI{model: openAITestModel}
				response, err := adapter.validateResponse(decoded)
				if err == nil || response.Text != "" {
					t.Fatal("terminal response became success")
				}
				outcome, ok := TerminalResponseOutcome(fmt.Errorf("wrapped: %w", err))
				if !ok || outcome.Status != status || (outcome.Usage != nil) != withUsage {
					t.Fatalf("outcome=%+v", outcome)
				}
				if withUsage {
					if outcome.Usage.InputTokens != 84 || outcome.Usage.OutputTokens != 16 {
						t.Fatal("lost usage")
					}
					outcome.Usage.InputTokens = 0
					again, _ := TerminalResponseOutcome(err)
					if again.Usage.InputTokens != 84 {
						t.Fatal("evidence aliases mutable caller copy")
					}
				}
			})
		}
	}
}

func TestOpenAITerminalEvidenceSurvivesHTTPBoundary(t *testing.T) {
	for _, status := range []string{"failed", "incomplete"} {
		t.Run(status, func(t *testing.T) {
			body := openAITestResponseObject("partial text must remain unavailable")
			body["status"] = status
			adapter := openAITestAdapter(t, body)
			response, err := adapter.Complete(t.Context(), "offline test prompt")
			outcome, ok := TerminalResponseOutcome(err)
			if !ok || outcome.Status != status || outcome.Usage == nil || outcome.Usage.TotalTokens != 15 || response.Text != "" {
				t.Fatalf("HTTP terminal evidence lost: outcome=%+v error=%v", outcome, err)
			}
			if WasRequestNotSent(err) {
				t.Fatal("definite response became not sent")
			}
		})
	}
}

func TestOpenAITerminalEvidenceRejectsUntrustedAccounting(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"model":   func(v map[string]any) { v["model"] = "other-model" },
		"profile": func(v map[string]any) { v["tool_choice"] = "auto" },
		"negative": func(v map[string]any) {
			v["usage"] = map[string]any{"input_tokens": -1, "output_tokens": 3, "total_tokens": 2}
		},
		"inconsistent": func(v map[string]any) {
			v["usage"] = map[string]any{"input_tokens": 12, "output_tokens": 3, "total_tokens": 99}
		},
		"unknown status": func(v map[string]any) { v["status"] = "in_progress" },
	} {
		t.Run(name, func(t *testing.T) {
			body := openAITestResponseObject("untrusted")
			body["status"] = "incomplete"
			mutate(body)
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeOpenAIResponse(encoded)
			if err != nil {
				t.Fatal(err)
			}
			_, err = (&OpenAIAPI{model: openAITestModel}).validateResponse(decoded)
			if err == nil {
				t.Fatal("accepted invalid response")
			}
			if _, ok := TerminalResponseOutcome(err); ok {
				t.Fatal("invalid evidence accepted for accounting")
			}
		})
	}
}
