package ledger

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// A selected connection's complete replacement history is private support. Its
// policies, activations and accounting share the advertised aggregate bound.
func TestIncidentInferenceAggregateBudget(t *testing.T) {
	parallelIncidentTest(t)
	for _, revisions := range []int{1, 40, 800, events.MaximumIncidentEvidence/2 + 1} {
		t.Run(fmt.Sprint(revisions), func(t *testing.T) {
			store, policy, correlation := appendIncidentBudgetHistory(t, revisions)
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("valid writer history rejected by full admission validator: %v", err)
			}
			start := time.Now()
			for range 3 {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, correlation, 256)
				if 2*revisions+1 > events.MaximumIncidentEvidence {
					if err == nil {
						t.Fatal("accepted policy and activation support above aggregate item bound")
					}
					if !strings.Contains(err.Error(), "limit") {
						t.Fatalf("unexpected above-bound rejection: %v", err)
					}
					if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatal("above-bound failure returned partial snapshot")
					}
					return
				}
				if err != nil {
					t.Fatalf("valid %d policy replacements (%d policy/accounting items) within aggregate bound: %v", revisions, 2*revisions+1, err)
				}
				if len(snapshot.Admissions) != 1 {
					t.Fatalf("admissions=%d want one selected reservation", len(snapshot.Admissions))
				}
				if len(snapshot.Work.Events) != 3 {
					t.Fatalf("private policy history changed public Work: events=%d want3", len(snapshot.Work.Events))
				}
			}
			t.Logf("revisions=%d three complete public reads=%s", revisions, time.Since(start))
		})
	}
}

func appendIncidentBudgetHistory(t *testing.T, revisions int) (*SQLite, inference.Policy, string) {
	t.Helper()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	store.now = func() time.Time { return now }
	policy := testInferencePolicy(now)
	policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "aggregate-budget-connection"
	policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("aggregate-budget-call")
	request.ConnectionID = policy.ConnectionID
	manifest := events.PlanningContextPayload{ConnectionID: policy.ConnectionID, IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: policy.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: manifest}); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < revisions; i++ {
		policy.AuthorizedAt = policy.AuthorizedAt.Add(time.Second)
		if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
	}
	return store, policy, request.Scope.CorrelationID
}

// Later valid replacements must not hide an invalid earlier revision or an
// orphaned side of the policy/activation pair after the duplicate pass is gone.
func TestIncidentInferenceAggregateHistoryFailure(t *testing.T) {
	parallelIncidentTest(t)
	for _, damage := range []string{"invalid-earlier", "missing-policy", "missing-activation-reference"} {
		t.Run(damage, func(t *testing.T) {
			store, policy, correlation := appendIncidentBudgetHistory(t, 800)
			if _, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, correlation, 256); err != nil {
				t.Fatalf("healthy large baseline: %v", err)
			}
			var fingerprint string
			if err := store.db.QueryRowContext(t.Context(), `SELECT p.policy_fingerprint FROM inference_policies p JOIN events e ON e.event_id=p.activation_event_id WHERE p.connection_id=? ORDER BY e.sequence LIMIT 1`, policy.ConnectionID).Scan(&fingerprint); err != nil {
				t.Fatal(err)
			}
			var query string
			switch damage {
			case "invalid-earlier":
				query = `UPDATE inference_policies SET body='{}' WHERE policy_fingerprint=?`
			case "missing-policy":
				query = `DELETE FROM inference_policies WHERE policy_fingerprint=?`
			case "missing-activation-reference":
				query = `UPDATE inference_policies SET activation_event_id='' WHERE policy_fingerprint=?`
			}
			if _, err := store.db.ExecContext(t.Context(), query, fingerprint); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("full owner accepted damaged earlier policy support")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, correlation, 256)
			if err == nil {
				t.Fatal("incident accepted damaged earlier policy support behind valid replacements")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("damaged support returned partial snapshot")
			}
		})
	}
}

func TestIncidentInferenceAggregateBytes(t *testing.T) {
	parallelIncidentTest(t)
	for _, padding := range []int{6 << 20, 12 << 20} {
		t.Run(fmt.Sprint(padding), func(t *testing.T) {
			store, policy, correlation := appendIncidentBudgetHistory(t, 3)
			// JSON whitespace changes stored resource consumption without changing the
			// admitted policy, fingerprint, authorization or replacement semantics.
			if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET body=CAST(body || ? AS BLOB) WHERE connection_id=?`, strings.Repeat(" ", padding), policy.ConnectionID); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("valid padded support failed full validator: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, correlation, 256)
			if 3*padding > events.MaximumIncidentEvidenceBytes {
				if err == nil {
					t.Fatal("accepted policy support exceeding aggregate byte limit")
				}
				if !strings.Contains(err.Error(), "support limit") {
					t.Fatalf("unexpected byte-bound rejection: %v", err)
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("byte-bound failure returned partial snapshot")
				}
				return
			}
			if err != nil || len(snapshot.Admissions) != 1 {
				t.Fatalf("healthy18MiB policy support should fit32MiB: admissions=%d err=%v", len(snapshot.Admissions), err)
			}
		})
	}
}
