package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentIncomingExecutionContextEventRefs(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "context-links.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planScopeParents(t, store, "selected-org", "selected", "selected-work")
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "incoming", true)
	request := appendBenchmarkTaskInference(t, store, agent, config, "incoming")
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("writer-created inference history failed full replay: %v", err)
	}
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
	if err != nil {
		t.Fatalf("independent selected Work failed incident validation: %v", err)
	}
	var selectedWorkID, manifestID string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE organization_id='selected-org' AND correlation_id='selected' AND event_type='WORK_CREATED'`).Scan(&selectedWorkID); err != nil {
		t.Fatal(err)
	}
	var body []byte
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id,payload FROM events WHERE organization_id='org-1' AND correlation_id='incoming' AND event_type='EXECUTION_CONTEXT_MANIFESTED'`).Scan(&manifestID, &body); err != nil {
		t.Fatal(err)
	}
	for _, event := range baseline.DependencyEvents {
		if event.EventID == manifestID {
			t.Fatal("unrelated execution manifest was already selected")
		}
	}
	var manifest core.ExecutionContextManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.EventRefs = []string{selectedWorkID}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		changed, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, changed, manifestID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "execution context references do not match") {
		t.Fatalf("full inference replay did not reject mismatched event refs: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
	if err == nil {
		t.Fatal("incident omitted a foreign execution manifest referencing selected Work")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed incident returned partial evidence")
	}
}

func TestIncidentIncomingAuxiliaryManifestRef(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "auxiliary-context.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	selected := modelStopManifest(t, store, true, "selected-call")
	foreignDraft := modelStopDraft(selected)
	foreignDraft.OrganizationID = "other-org"
	foreignDraft.CorrelationID = "other-run"
	foreignDraft.TaskID = "task-other-run"
	foreignDraft.SourceExecutionID = "other-call"
	foreignDraft.EventType = selected.EventType
	var foreignContext events.IntentNormalizationContextPayload
	if err := json.Unmarshal(selected.Payload, &foreignContext); err != nil {
		t.Fatal(err)
	}
	// This auxiliary input list is shape-checked, not resolved as global
	// event evidence. Its foreign value must not link the incident by itself.
	foreignContext.InputEventRefs = []string{selected.EventID}
	foreignDraft.Payload = foreignContext
	foreign, err := store.Append(t.Context(), foreignDraft)
	if err != nil {
		t.Fatal(err)
	}
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "other-org", "provider", "model", "v1"
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest("other-call")
	request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "other-org", "other-run", "task-other-run", "intent-model-stop"
	request.Scope.Purpose = inference.PurposeIntentNormalization
	request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("valid auxiliary reservation and shape-only input reference failed full replay: %v", err)
	}
	baseline, err := store.VerifiedIncidentEvents(t.Context(), selected.OrganizationID, selected.CorrelationID, 256)
	if err != nil {
		t.Fatalf("valid selected incident: %v", err)
	}
	for _, event := range baseline.DependencyEvents {
		if event.EventID == foreign.EventID {
			t.Fatal("shape-only auxiliary input reference selected foreign context")
		}
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,'$.execution_manifest_ref',?) AS BLOB) WHERE event_type='INFERENCE_RESERVED' AND organization_id='other-org'`, selected.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "auxiliary inference context reference") {
		t.Fatalf("full inference replay did not reject foreign auxiliary manifest reference: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), selected.OrganizationID, selected.CorrelationID, 256)
	if err == nil {
		t.Fatal("incident omitted a foreign reservation targeting selected auxiliary manifest")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed incident returned partial evidence")
	}
}

func TestIncidentUnconsumedManifestSharedInbox(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "shared-inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planScopeParents(t, store, "org-1", "selected", "selected-work")
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "shared-inbox", false)
	message, err := store.Append(t.Context(), events.TrustedDraft{
		OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected",
		RecipientScope: events.RecipientAgent, RecipientID: string(agent.ID), Payload: map[string]string{"note": "shared inbox evidence"},
	})
	if err != nil {
		t.Fatal(err)
	}
	appendBenchmarkTaskInference(t, store, agent, config, "incoming")
	var body []byte
	var manifestID string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id,payload FROM events WHERE event_type='EXECUTION_CONTEXT_MANIFESTED' AND correlation_id='incoming'`).Scan(&manifestID, &body); err != nil {
		t.Fatal(err)
	}
	var manifest core.ExecutionContextManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(manifest.EventRefs, message.EventID) {
		t.Fatalf("writer did not select shared inbox event: %v", manifest.EventRefs)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("valid unconsumed manifest failed inference recovery: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
	if err != nil {
		t.Fatalf("valid shared inbox history crossed its selected incident: %v", err)
	}
	found := false
	for _, event := range snapshot.DependencyEvents {
		if event.EventID == manifestID {
			found = true
		}
	}
	if !found {
		t.Fatal("valid unconsumed manifest did not exercise the incoming reference selector")
	}
}
