package effectstatus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/effects"
)

func TestReconciliationRejectsAmbiguousSuccessfulResponse(t *testing.T) {
	obligation := attemptedEffect("effect-http")
	encoded, err := json.Marshal(reconciliationResponse{
		EffectObligationID: string(obligation.ID), IdempotencyKey: obligation.IdempotencyKey,
		EffectFingerprint: obligation.EffectFingerprint, State: effects.ReconciliationConfirmed,
		EvidenceRefs: []string{"receipt"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"duplicate state": strings.Replace(string(encoded), `"state":`, `"state":"synthetic-secret","state":`, 1),
		"case alias":      strings.Replace(string(encoded), `"state":`, `"STATE":`, 1),
		"unknown":         strings.TrimSuffix(string(encoded), "}") + `,"synthetic-secret":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			reconciler := httpStatusReconciler{client: server.Client(), statusURL: server.URL}
			observation, err := reconciler.Check(context.Background(), obligation)
			if err == nil || observation.State != "" || strings.Contains(err.Error(), "synthetic-secret") {
				t.Fatal("ambiguous response trusted or unsafe diagnostic returned")
			}
		})
	}
}
