package ledger

import (
	"bytes"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentDuplicatePolicyConnection(t *testing.T) {
	for _, channel := range []string{"both", "record", "event", "escaped/both"} {
		t.Run(channel, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			policy := testInferencePolicy(time.Now().UTC())
			policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "selected-connection"
			policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			other := policy
			other.ConnectionID = "independent-connection"
			if err := store.ActivateInferencePolicy(t.Context(), other); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("selected-policy-call")
			request.ConnectionID = policy.ConnectionID
			manifest := events.PlanningContextPayload{ConnectionID: policy.ConnectionID, IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: policy.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: manifest}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("full valid baseline: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, request.Scope.CorrelationID, 256); err != nil {
				t.Fatalf("valid incident baseline: %v", err)
			}
			fingerprint, err := other.Fingerprint()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var body []byte
				var activation string
				if err := tx.QueryRowContext(t.Context(), `SELECT body,activation_event_id FROM inference_policies WHERE policy_fingerprint=?`, fingerprint).Scan(&body, &activation); err != nil {
					return err
				}
				if channel != "event" {
					body = duplicateJSONMember(t, body, "connection_id", []byte(`"selected-connection"`))
					if channel == "escaped/both" {
						body = bytes.ReplaceAll(body, []byte(`"connection_id"`), []byte(`"connection\u005fid"`))
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE inference_policies SET body=? WHERE policy_fingerprint=?`, body, fingerprint); err != nil {
						return err
					}
				}
				if channel != "record" {
					if err := tx.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_id=?`, activation).Scan(&body); err != nil {
						return err
					}
					body = duplicateJSONMember(t, body, "connection_id", []byte(`"selected-connection"`))
					if channel == "escaped/both" {
						body = bytes.ReplaceAll(body, []byte(`"connection_id"`), []byte(`"connection\u005fid"`))
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, activation); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("full policy owner accepted duplicate selected connection claim")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, request.Scope.CorrelationID, 256)
			if err == nil {
				t.Fatal("incident omitted later duplicate selected connection claim")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("duplicate policy returned partial evidence")
			}
		})
	}
}
