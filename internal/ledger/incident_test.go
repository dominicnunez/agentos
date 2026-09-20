package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func incidentEffectFixture(t *testing.T, store *SQLite) core.EffectObligation {
	t.Helper()
	task := stopTestExecution(t, store)
	lease := core.CapabilityLease{ID: "incident-lease", ActorID: task.AssigneeID, ActorKind: core.PrincipalAgent, OriginTaskID: task.ID, Action: "send", Resource: "destination", Scope: "org-1"}
	if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_GRANTED", "owner", string(task.ID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	return appendApprovedEffectAttempt(t, store, task, lease, "incident-effect", "incident-approval")
}

func TestIncidentTaskEffectHistory(t *testing.T) {
	for _, mutation := range []string{"none", "earlier-record", "missing-record", "orphan-record", "cross-tenant", "orphan-event", "orphan-task", "orphan-envelope"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "effects.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			attempt := incidentEffectFixture(t, store)
			confirmed := attempt
			confirmed.Status = core.EffectConfirmed
			confirmed.ConfirmationEvidenceRefs = []string{"receipt"}
			if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(attempt.TaskID), attempt.AuthorizationRefs, confirmed.ConfirmationEvidenceRefs, "effect", string(attempt.ID), 2, confirmed); err != nil {
				t.Fatal(err)
			}
			if mutation == "orphan-event" {
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					_, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", TaskID: string(attempt.TaskID), EventType: "EFFECT_OBLIGATION_TRANSITIONED", Payload: confirmed})
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			switch mutation {
			case "orphan-envelope":
				orphan := attempt
				orphan.ID = "orphan-effect"
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					_, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "other", TaskID: "other-task", EventType: "EFFECT_OBLIGATION_TRANSITIONED", Payload: orphan})
					return err
				}); err != nil {
					t.Fatal(err)
				}
			case "orphan-task":
				_, err = store.db.ExecContext(t.Context(), `INSERT INTO records SELECT kind,record_id,version+1,body,'','',created_at FROM records WHERE kind='task' AND version=(SELECT MAX(version) FROM records WHERE kind='task')`)
				if err != nil {
					t.Fatal(err)
				}
			case "earlier-record":
				bad := attempt
				bad.Status = core.EffectConfirmed
				body, _ := json.Marshal(bad)
				if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='effect' AND version=1`, body); err != nil {
					t.Fatal(err)
				}
			case "missing-record":
				if _, err := store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='effect' AND version=1`); err != nil {
					t.Fatal(err)
				}
			case "orphan-record":
				if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records SELECT kind,record_id,3,body,admission_event_id,admission_fingerprint,created_at FROM records WHERE kind='effect' AND version=2`); err != nil {
					t.Fatal(err)
				}
			case "cross-tenant":
				bad := attempt
				bad.OrganizationID = "other"
				body, _ := json.Marshal(bad)
				if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='effect' AND version=1`, body); err != nil {
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
			t.Cleanup(func() { _ = store.Close() })
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
			if mutation != "none" {
				if err == nil {
					t.Fatal("accepted broken earlier effect history")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.RelatedEvents) != 2 {
				t.Fatalf("effect history=%d", len(snapshot.RelatedEvents))
			}
			var attempts int
			for _, admission := range snapshot.Admissions {
				if admission.Kind == "EFFECT_ATTEMPT" {
					attempts++
					if admission.TaskID != string(attempt.TaskID) || admission.ExecutionID != "" {
						t.Fatalf("fabricated execution linkage: %+v", admission)
					}
				}
			}
			if attempts != 1 {
				t.Fatalf("attempt boundaries=%d", attempts)
			}
		})
	}
}

func TestIncidentRejectsTasklessEffect(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	modelStopManifest(t, store, false, "planning-call")
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		_, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "model-stop", EventType: "EFFECT_OBLIGATION_TRANSITIONED", Payload: core.EffectObligation{ID: "taskless-effect", OrganizationID: "org-1", Status: core.EffectAttempted}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err == nil {
		t.Fatal("taskless planning stream granted effect evidence")
	}
}

func TestIncidentRejectsFreezeOrphansAndOversize(t *testing.T) {
	for _, mutation := range []string{"orphan", "earlier-bad", "bytes"} {
		t.Run(mutation, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			modelStopManifest(t, store, false, "first")
			appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
			appendInferenceFreeze(t, store, "org-1", 2, false)
			switch mutation {
			case "orphan":
				_, err = store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='organization_freeze' AND version=1`)
			case "earlier-bad":
				_, err = store.db.ExecContext(t.Context(), `UPDATE records SET body='{}' WHERE kind='organization_freeze' AND version=1`)
			case "bytes":
				_, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "model-stop", EventType: "AUDIT_NOTE", Payload: map[string]string{"data": strings.Repeat("x", 2<<20)}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err == nil {
				t.Fatal("accepted incomplete or oversized evidence")
			}
		})
	}
}

func TestIncidentReadSnapshotExcludesConcurrentHold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.db.ExecContext(t.Context(), `PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	modelStopManifest(t, store, false, "first")
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	head, err := ValidateEventIntegrity(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	appendHistoricalInferenceFreeze(t, writer, "org-1", 1, true)
	snapshot, err := readIncident(t.Context(), tx, "org-1", "model-stop", 256)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Work.LedgerSequence != head.Sequence || len(snapshot.RelatedEvents) != 0 {
		t.Fatal("read mixed a newer hold into the earlier verified snapshot")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	later, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
	if err != nil || len(later.RelatedEvents) != 1 {
		t.Fatalf("next read lost committed hold: related=%d err=%v", len(later.RelatedEvents), err)
	}
}

func TestIncidentInferenceRequiresExactAccounting(t *testing.T) {
	for _, mutation := range []string{"none", "reservation", "policy", "orphan", "later-duplicate", "released-stale", "oversized-row", "oversized-row-unicode", "oversized-row-nul"} {
		t.Run(mutation, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			manifest := modelStopManifest(t, store, false, "inference-call")
			now := time.Now().UTC()
			store.now = func() time.Time { return now }
			policy := testInferencePolicy(now)
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("inference-call")
			request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
			request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "released-stale":
				stream, readErr := store.Events(t.Context(), "model-stop")
				if readErr != nil {
					t.Fatal(readErr)
				}
				reserved := stream[len(stream)-1]
				appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
				appendInferenceFreeze(t, store, "org-1", 2, false)
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=CAST('{}' AS BLOB) WHERE event_id=?`, reserved.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					if err := rebuildEventIntegrity(t.Context(), tx); err != nil {
						return err
					}
					draft := modelStopDraft(manifest)
					draft.EventType, draft.Payload = "INFERENCE_RESERVED", reserved.Payload
					_, err := appendEvent(t.Context(), tx, draft)
					return err
				})
			case "reservation":
				_, err = store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET prompt_sha256=?`, strings.Repeat("b", 64))
			case "oversized-row", "oversized-row-unicode", "oversized-row-nul":
				value := strings.Repeat("x", 2<<20)
				switch mutation {
				case "oversized-row-unicode":
					value = strings.Repeat("界", 700000)
				case "oversized-row-nul":
					value = "\x00" + value
				}
				_, err = store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET request_id=?`, value)
			case "policy":
				_, err = store.db.ExecContext(t.Context(), `UPDATE inference_policies SET body='{}'`)
			case "orphan":
				_, err = store.db.ExecContext(t.Context(), `DELETE FROM inference_reservations`)
			case "later-duplicate":
				stream, readErr := store.Events(t.Context(), "model-stop")
				if readErr != nil {
					t.Fatal(readErr)
				}
				last := stream[len(stream)-1]
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					draft := modelStopDraft(manifest)
					draft.EventType, draft.Payload = "INFERENCE_RESERVED", last.Payload
					_, err := appendEvent(t.Context(), tx, draft)
					return err
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
			if strings.HasPrefix(mutation, "oversized-row") && (err == nil || !strings.Contains(err.Error(), "accounting exceeds byte limit")) {
				t.Fatalf("oversized accounting was not rejected before reading its fields: %v", err)
			}
			if mutation != "none" {
				t.Logf("rejected %s: %v", mutation, err)
				if err == nil {
					t.Fatal("unvalidated inference label became an admission")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Admissions) != 1 || snapshot.Admissions[0].Kind != "INFERENCE_RESERVATION" {
				t.Fatalf("inference admissions=%+v", snapshot.Admissions)
			}
		})
	}
}

func TestIncidentRejectsOrphanInferenceRows(t *testing.T) {
	for _, scope := range []string{"selected", "other-organization", "other-correlation"} {
		t.Run(scope, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "inference.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			modelStopManifest(t, store, false, "first")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			policy.MaxConcurrentRequests = 2
			policy.MaxTokensPerWindow = 1000
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest("first")
			request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
			request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
			first, err := store.ReserveInference(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			// Retain a real accounting row but remove its admission contract from
			// an otherwise integrity-valid history. A later valid reservation must
			// not hide the earlier orphan.
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=CAST('{}' AS BLOB) WHERE event_type='INFERENCE_RESERVED'`); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			switch scope {
			case "other-organization":
				_, err = store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET organization_id='other-org' WHERE reservation_id=?`, first.ID)
			case "other-correlation":
				_, err = store.db.ExecContext(t.Context(), `UPDATE inference_reservations SET correlation_id='other-run' WHERE reservation_id=?`, first.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			request.Scope.RequestID, request.Scope.ExecutionID = "later", "later"
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
			if scope == "selected" {
				if err == nil {
					t.Fatal("later valid admission hid selected orphan accounting")
				}
				return
			}
			if err != nil || len(snapshot.Admissions) != 1 {
				t.Fatalf("unrelated orphan affected selected incident: admissions=%d err=%v", len(snapshot.Admissions), err)
			}
		})
	}
}

func TestIncidentInferenceSupportBudget(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprint(distinct), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			policy := testInferencePolicy(time.Now().UTC())
			policy.MaxConcurrentRequests, policy.MaxTokensPerWindow = 2, 1000
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			first, err := store.ReserveInference(t.Context(), testInferenceRequest("first"))
			if err != nil {
				t.Fatal(err)
			}
			if distinct {
				if _, err := store.ReconcileInference(t.Context(), first, nil, inference.ReconciliationUncertain); err != nil {
					t.Fatal(err)
				}
				policy.AuthorizedBy = "other-owner"
				policy.AuthorizedAt = policy.AuthorizedAt.Add(time.Second)
				if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.ReserveInference(t.Context(), testInferenceRequest("later")); err != nil {
				t.Fatal(err)
			}
			// JSON whitespace preserves the exact decoded policy and its
			// fingerprint while exercising the stored support-byte boundary.
			if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET body=CAST(body || ? AS BLOB)`, strings.Repeat(" ", 1100000)); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256)
			if distinct {
				if err == nil {
					t.Fatal("distinct policy support exceeded aggregate byte bound")
				}
				return
			}
			if err != nil || len(snapshot.Admissions) != 2 {
				t.Fatalf("shared policy support charged repeatedly: admissions=%d err=%v", len(snapshot.Admissions), err)
			}
		})
	}
}

func TestIncidentInferenceGrowth(t *testing.T) {
	for _, count := range []int{1, 100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			policy := testInferencePolicy(time.Now().UTC())
			policy.MaxConcurrentRequests, policy.MaxTokensPerWindow = count, 100000
			policy.Pricing.MaxCostNanoUSDPerWindow = 100000000
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			for i := range count {
				if _, err := store.ReserveInference(t.Context(), testInferenceRequest(fmt.Sprint(i))); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			for range 5 {
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256)
				if err != nil || len(snapshot.Admissions) != count {
					t.Fatalf("inference snapshot admissions=%d err=%v", len(snapshot.Admissions), err)
				}
			}
			t.Logf("%d reservations; five complete incident snapshots=%s", count, time.Since(start))
		})
	}
}

func TestIncidentGrowth(t *testing.T) {
	for _, count := range []int{1, 200} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			for i := range count {
				if _, err := store.Append(context.Background(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "run", EventType: "AUDIT_NOTE", Payload: map[string]int{"i": i}}); err != nil {
					t.Fatal(err)
				}
			}
			start := time.Now()
			for range 5 {
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "run", 256); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("%d events; five complete incident snapshots=%s", count, time.Since(start))
		})
	}
}

func TestIncidentIncludesUncorrelatedHold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "incident.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest := modelStopManifest(t, store, false, "planning-1")
	appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
	request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "security_hold")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordModelStop(t.Context(), request.EventID, nil); err != nil {
		t.Fatal(err)
	}
	appendInferenceFreeze(t, store, "org-1", 2, false)
	appendHistoricalInferenceFreeze(t, store, "other-org", 1, true)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	snapshot, err := events.NewGateway(store).VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Work.Events) != 3 || len(snapshot.RelatedEvents) != 2 || len(snapshot.FreezeRecords) != 2 {
		t.Fatalf("incomplete incident snapshot: work=%d related=%d freezes=%d", len(snapshot.Work.Events), len(snapshot.RelatedEvents), len(snapshot.FreezeRecords))
	}
	for _, event := range snapshot.RelatedEvents {
		if event.OrganizationID != "org-1" || event.EventType != "FREEZE_SET" {
			t.Fatalf("unrelated evidence: %+v", event)
		}
	}
	body, err := json.Marshal(snapshot)
	if err != nil || string(body) != "{}" {
		t.Fatalf("private evidence serialized: %s err=%v", body, err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 4); err == nil {
		t.Fatal("combined evidence limit ignored")
	}
}
