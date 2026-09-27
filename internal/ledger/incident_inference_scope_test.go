package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentInferenceForeignScope(t *testing.T) {
	for _, channel := range []string{"all", "row-task", "event-task", "payload-manifest", "duplicate-payload", "missing-purpose", "auxiliary-purpose", "orphan-row", "orphan-event", "terminal-event"} {
		t.Run(channel, func(t *testing.T) { testIncidentInferenceScope(t, channel) })
	}
}

func testIncidentInferenceScope(t *testing.T, channel string) {
	path := filepath.Join(t.TempDir(), "inference.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
	request := appendBenchmarkTaskInference(t, store, agent, config, "selected")
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	reservation, err := store.ReserveInference(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if channel == "terminal-event" {
		if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
			t.Fatal(err)
		}
	}
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
	if err != nil {
		t.Fatalf("valid incident baseline: %v", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("valid full recovery baseline: %v", err)
	}
	if len(baseline.Admissions) != 2 {
		t.Fatalf("baseline admissions: %d", len(baseline.Admissions))
	}
	var manifest string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`).Scan(&manifest); err != nil {
		t.Fatal(err)
	}
	rowTask, rowExecution, rowRequest := "foreign-task", "foreign-execution", "foreign-request"
	eventTask, eventExecution, payloadRequest, payloadManifest := "foreign-task", "foreign-execution", "foreign-request", "foreign-manifest"
	if channel == "all" || channel == "row-task" || channel == "missing-purpose" || channel == "auxiliary-purpose" || channel == "orphan-row" {
		rowTask = request.Scope.TaskID
	}
	if channel == "all" {
		rowExecution = request.Scope.ExecutionID
	}
	if channel == "all" {
		rowRequest = request.Scope.RequestID
	}
	if channel == "all" || channel == "event-task" || channel == "missing-purpose" || channel == "auxiliary-purpose" || channel == "orphan-event" {
		eventTask = request.Scope.TaskID
	}
	if channel == "all" {
		eventExecution = request.Scope.ExecutionID
	}
	if channel == "all" {
		payloadRequest = request.Scope.RequestID
	}
	if channel == "all" || channel == "payload-manifest" {
		payloadManifest = manifest
	}
	purpose := string(inference.PurposeTaskExecution)
	if channel == "missing-purpose" {
		purpose = ""
	}
	if channel == "auxiliary-purpose" {
		purpose = string(inference.PurposePlanning)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id='foreign-org',correlation_id='foreign-work',task_id=?,source_execution_id=?,payload=CAST(json_set(payload,'$.request_id',?,'$.execution_manifest_ref',?,'$.purpose',?) AS BLOB) WHERE event_type='INFERENCE_RESERVED'`, eventTask, eventExecution, payloadRequest, payloadManifest, purpose); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE inference_reservations SET organization_id='foreign-org',correlation_id='foreign-work',task_id=?,execution_id=?,request_id=?,purpose=?`, rowTask, rowExecution, rowRequest, purpose); err != nil {
			return err
		}
		switch channel {
		case "missing-purpose":
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_remove(payload,'$.purpose') AS BLOB) WHERE event_type='INFERENCE_RESERVED'`); err != nil {
				return err
			}
		case "orphan-row":
			if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_type='INFERENCE_RESERVED'`); err != nil {
				return err
			}
		case "orphan-event":
			if _, err := tx.ExecContext(t.Context(), `DELETE FROM inference_reservations`); err != nil {
				return err
			}
		case "duplicate-payload":
			body := fmt.Sprintf(`{"execution_manifest_ref":"foreign-manifest","execution_manifest_ref":%q}`, manifest)
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, []byte(body)); err != nil {
				return err
			}
		case "terminal-event":
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id='foreign-org',correlation_id='foreign-work',task_id=?,source_execution_id='foreign-execution' WHERE event_type='INFERENCE_RECONCILED'`, request.Scope.TaskID); err != nil {
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
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	recoveryErr := store.ValidateInferenceAdmissions(t.Context())
	t.Logf("full recovery after mutation: %v", recoveryErr)
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
	if err == nil || !strings.Contains(err.Error(), "materialized Task organization") {
		t.Fatalf("omitted foreign reservation referencing selected materialized Task: admissions=%d recovery=%v", len(snapshot.Admissions), recoveryErr)
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("returned partial incident evidence")
	}
}

func TestIncidentForeignAuxiliaryIDs(t *testing.T) {
	for _, kind := range []string{"virtual", "materialized-execution"} {
		t.Run(kind, func(t *testing.T) { testIncidentAuxiliaryIDs(t, kind) })
	}
}

func testIncidentAuxiliaryIDs(t *testing.T, kind string) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	correlation, taskID, executionID := "model-stop", "task-model-stop", "shared-call"
	if kind == "virtual" {
		modelStopManifest(t, store, false, executionID)
	} else {
		agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
		request := appendBenchmarkTaskInference(t, store, agent, config, "selected")
		correlation, taskID, executionID = "selected", "unrelated-virtual-task", request.Scope.ExecutionID
	}
	before, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err != nil {
		t.Fatal(err)
	}
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "foreign-org", "provider", "model", "v1"
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	request := testInferenceRequest(executionID)
	request.Scope.OrganizationID, request.Scope.TaskID, request.Scope.CorrelationID = "foreign-org", taskID, "foreign-work"
	request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("valid auxiliary writer history rejected by full recovery: %v", err)
	}
	after, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err != nil {
		t.Fatalf("tenant-scoped auxiliary IDs poisoned selected incident: %v", err)
	}
	if !reflect.DeepEqual(before.Work.Events, after.Work.Events) || !reflect.DeepEqual(before.Admissions, after.Admissions) {
		t.Fatal("foreign auxiliary reservation changed selected evidence")
	}
}
