package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentRejectsRetiredPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policies.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	store.now = func() time.Time { return now }
	first := testInferencePolicy(now)
	first.Version, first.ConnectionID = inference.ConnectionPolicyVersion, "connection"
	first.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
	if err := store.ActivateInferencePolicy(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.AuthorizedAt = first.AuthorizedAt.Add(time.Minute)
	if err := store.ActivateInferencePolicy(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("call")
	request.ConnectionID = first.ConnectionID
	manifest := events.PlanningContextPayload{ConnectionID: first.ConnectionID, IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: first.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: manifest}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), first.OrganizationID, request.Scope.CorrelationID, 256); err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := first.Fingerprint()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var body []byte
		if err := tx.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_type='INFERENCE_RESERVED'`).Scan(&body); err != nil {
			return err
		}
		var payload events.InferenceReservedPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			return err
		}
		payload.PolicyFingerprint = fingerprint
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, body); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE inference_reservations SET policy_fingerprint=?`, fingerprint); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "not active at reservation") {
		t.Fatalf("full accounting validation: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), first.OrganizationID, request.Scope.CorrelationID, 256); err == nil {
		t.Fatal("incident accepted retired connection policy")
	}
}

func TestIncidentConnectionPolicyHistory(t *testing.T) {
	for _, mutation := range []string{"historical", "other-connection", "other-organization", "missing-policy", "missing-activation", "missing-activation-ref", "moved-policy", "moved-policy-and-event", "missing-reconciliation", "late-reconciliation", "retired-active"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "history.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC()
			store.now = func() time.Time { return now }
			first := testInferencePolicy(now)
			first.Version, first.ConnectionID = inference.ConnectionPolicyVersion, "connection"
			first.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
			if err := store.ActivateInferencePolicy(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("historical-call")
			request.ConnectionID = first.ConnectionID
			manifest := events.PlanningContextPayload{ConnectionID: first.ConnectionID, IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: first.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: manifest}); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
				t.Fatal(err)
			}
			second := first
			second.AuthorizedAt = first.AuthorizedAt.Add(time.Minute)
			if mutation == "other-connection" {
				second.ConnectionID = "other"
			}
			if mutation == "other-organization" {
				second.OrganizationID = "other"
			}
			if err := store.ActivateInferencePolicy(t.Context(), second); err != nil {
				t.Fatal(err)
			}
			fingerprint, _ := second.Fingerprint()
			if _, err := store.VerifiedIncidentEvents(t.Context(), first.OrganizationID, request.Scope.CorrelationID, 256); err != nil {
				t.Fatalf("valid historical baseline: %v", err)
			}
			if mutation == "historical" {
				checkIncidentPolicyBudget(t, store, first.OrganizationID, reservation)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var err error
				switch mutation {
				case "missing-policy":
					_, err = tx.ExecContext(t.Context(), `DELETE FROM inference_policies WHERE policy_fingerprint=?`, fingerprint)
				case "missing-activation":
					_, err = tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id=(SELECT activation_event_id FROM inference_policies WHERE policy_fingerprint=?)`, fingerprint)
				case "missing-activation-ref":
					_, err = tx.ExecContext(t.Context(), `UPDATE inference_policies SET activation_event_id='' WHERE policy_fingerprint=?`, fingerprint)
				case "moved-policy":
					_, err = tx.ExecContext(t.Context(), `UPDATE inference_policies SET connection_id='elsewhere' WHERE policy_fingerprint=?`, fingerprint)
				case "moved-policy-and-event":
					if _, err = tx.ExecContext(t.Context(), `UPDATE events SET organization_id='other' WHERE event_id=(SELECT activation_event_id FROM inference_policies WHERE policy_fingerprint=?)`, fingerprint); err != nil {
						return err
					}
					_, err = tx.ExecContext(t.Context(), `UPDATE inference_policies SET organization_id='other' WHERE policy_fingerprint=?`, fingerprint)
				case "missing-reconciliation":
					if _, err = tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=? WHERE event_type='INFERENCE_RECONCILED'`, []byte(`{}`)); err != nil {
						return err
					}
					_, err = tx.ExecContext(t.Context(), `UPDATE inference_reservations SET state='RESERVED'`)
				case "retired-active":
					if _, err = tx.ExecContext(t.Context(), `UPDATE inference_policies SET active=0`); err != nil {
						return err
					}
					_, err = tx.ExecContext(t.Context(), `UPDATE inference_policies SET active=1 WHERE policy_fingerprint<>?`, fingerprint)
				case "late-reconciliation":
					var activation, reconciliation int64
					if err = tx.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_id=(SELECT activation_event_id FROM inference_policies WHERE policy_fingerprint=?)`, fingerprint).Scan(&activation); err != nil {
						return err
					}
					if err = tx.QueryRowContext(t.Context(), `SELECT sequence FROM events WHERE event_type='INFERENCE_RECONCILED'`).Scan(&reconciliation); err != nil {
						return err
					}
					if _, err = tx.ExecContext(t.Context(), `UPDATE events SET sequence=1000000 WHERE sequence=?`, activation); err != nil {
						return err
					}
					if _, err = tx.ExecContext(t.Context(), `UPDATE events SET sequence=? WHERE sequence=?`, activation, reconciliation); err != nil {
						return err
					}
					_, err = tx.ExecContext(t.Context(), `UPDATE events SET sequence=? WHERE sequence=1000000`, reconciliation)
				}
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = store.VerifiedIncidentEvents(t.Context(), first.OrganizationID, request.Scope.CorrelationID, 256)
			valid := mutation == "historical" || mutation == "other-connection" || mutation == "other-organization"
			if (err == nil) != valid {
				t.Fatalf("valid=%t error=%v", valid, err)
			}
			if (mutation == "late-reconciliation" || mutation == "missing-reconciliation") && !strings.Contains(err.Error(), "outstanding") {
				t.Fatalf("unexpected rejection: %v", err)
			}
		})
	}
}

func checkIncidentPolicyBudget(t *testing.T, store *SQLite, organization string, reservation inference.Reservation) {
	t.Helper()
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	budget := incidentBudget{events: 100, bytes: 1 << 20}
	support, err := loadIncidentInferenceSupport(t.Context(), tx, organization, []string{reservation.ID}, []string{reservation.PolicyFingerprint}, &budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(support.policies) != 2 || 100-budget.events != 5 {
		t.Fatalf("complete accounting/policy/event budget: policies=%d used=%d", len(support.policies), 100-budget.events)
	}
	used := int64(1<<20) - budget.bytes
	for _, test := range []struct {
		name   string
		budget incidentBudget
		valid  bool
	}{
		{"exact", incidentBudget{events: 5, bytes: used}, true},
		{"one-event-short", incidentBudget{events: 4, bytes: used}, false},
		{"one-byte-short", incidentBudget{events: 5, bytes: used - 1}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := loadIncidentInferenceSupport(t.Context(), tx, organization, []string{reservation.ID}, []string{reservation.PolicyFingerprint}, &test.budget)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%t err=%v", test.valid, err)
			}
		})
	}
}

func TestIncidentPolicyHistoryQueries(t *testing.T) {
	var baseline int64
	for _, revisions := range []int{1, 40} {
		t.Run(fmt.Sprint(revisions), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policies.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC()
			store.now = func() time.Time { return now }
			policy := testInferencePolicy(now)
			policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, "connection"
			policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxCostNanoUSDPerWindow: 1000000, MaxConcurrentRequests: 2}
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("call")
			request.ConnectionID = policy.ConnectionID
			manifest := events.PlanningContextPayload{ConnectionID: policy.ConnectionID, IntentID: request.Scope.IntentID, Provider: request.Descriptor.Provider, Model: request.Descriptor.Model, ExecutionProfileVersion: request.Descriptor.ExecutionProfileVersion}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: policy.OrganizationID, EventType: "PLANNING_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: manifest}); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationUncertain); err != nil {
				t.Fatal(err)
			}
			for i := 1; i < revisions; i++ {
				policy.AuthorizedAt = policy.AuthorizedAt.Add(time.Second)
				if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.db.Close(); err != nil {
				t.Fatal(err)
			}
			var count atomic.Int64
			store.db = sql.OpenDB(&incidentCountConnector{path: path, count: &count, inner: store.db.Driver()})
			store.db.SetMaxOpenConns(1)
			for read := range 2 {
				count.Store(0)
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), policy.OrganizationID, request.Scope.CorrelationID, 256)
				if err != nil || len(snapshot.Admissions) != 1 {
					t.Fatalf("admissions=%d err=%v", len(snapshot.Admissions), err)
				}
				queries := count.Load()
				t.Logf("revisions=%d read=%d statements=%d", revisions, read, queries)
				if revisions == 1 && read == 0 {
					baseline = queries
				}
				if queries > baseline {
					t.Fatalf("policy revisions increase query count: baseline=%d actual=%d", baseline, queries)
				}
			}
		})
	}
}
