package ledger

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

func setupAdmittedTaskInference(t *testing.T) (*SQLite, inference.InferenceRequest) {
	t.Helper()
	ctx := t.Context()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendTaskProjectionParents(t, ctx, store, "org-1", "inference-test", "work-1")
	agent, config := appendTaskAssignmentAgent(t, ctx, store, "org-1", "inference-test", false)
	task := appendPendingAgentExecutionTask(t, ctx, store, "inference-test", "task-inference", agent, config)
	if _, err := startPendingAgentExecution(ctx, store, "inference-test", task); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	policy := inference.Policy{Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: "test", Model: "test", ExecutionProfileVersion: config.ProfileVersion, Mode: inference.Local, MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 240, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour)}
	if err := store.ActivateInferencePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	request := inference.InferenceRequest{Scope: inference.Scope{OrganizationID: "org-1", Purpose: inference.PurposeTaskExecution, RequestID: "execution-task-inference-v2", ExecutionID: "execution-task-inference-v2", TaskID: string(task.ID), CorrelationID: "inference-test"}, Descriptor: execution.ModelDescriptor{Provider: "test", Model: "test", ExecutionProfileVersion: config.ProfileVersion}, PromptSHA256: core.FingerprintExecutionInput("test")}
	return store, request
}

func TestInferenceRecoveryRejectsSubstitutedManifestReference(t *testing.T) {
	ctx := t.Context()
	store, request := setupAdmittedTaskInference(t)
	reservation, err := store.ReserveInference(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(ctx, reservation, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(ctx); err != nil {
		t.Fatalf("valid history rejected: %v", err)
	}
	var original []byte
	if err := store.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_type='INFERENCE_RESERVED'`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	var payload events.InferenceReservedPayload
	if err := json.Unmarshal(original, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ExecutionManifestRef == "" {
		t.Fatal("reservation omitted its execution manifest")
	}
	for _, replacement := range []string{"", "unadmitted-manifest"} {
		payload.ExecutionManifestRef = replacement
		changed, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, changed); err != nil {
			t.Fatal(err)
		}
		if err := store.ValidateInferenceAdmissions(ctx); err == nil {
			t.Fatalf("recovery validator accepted reference %q", replacement)
		}
		if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, original); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTaskInferenceRejectsSubstitutedRequest(t *testing.T) {
	for name, alter := range map[string]func(*inference.InferenceRequest){
		"request": func(r *inference.InferenceRequest) { r.Scope.RequestID = "different-request" },
		"execution": func(r *inference.InferenceRequest) {
			r.Scope.ExecutionID = "different-execution"
			r.Scope.RequestID = r.Scope.ExecutionID
		},
		"task":        func(r *inference.InferenceRequest) { r.Scope.TaskID = "different-task" },
		"correlation": func(r *inference.InferenceRequest) { r.Scope.CorrelationID = "different-correlation" },
		"input": func(r *inference.InferenceRequest) {
			r.PromptSHA256 = core.FingerprintExecutionInput("different-input")
		},
		"provider": func(r *inference.InferenceRequest) { r.Descriptor.Provider = "different-provider" },
		"model":    func(r *inference.InferenceRequest) { r.Descriptor.Model = "different-model" },
		"profile":  func(r *inference.InferenceRequest) { r.Descriptor.ExecutionProfileVersion = "different-profile" },
	} {
		t.Run(name, func(t *testing.T) {
			store, request := setupAdmittedTaskInference(t)
			changed := request
			alter(&changed)
			if _, err := store.ReserveInference(t.Context(), changed); err == nil {
				t.Fatal("substituted request admitted")
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatalf("denied request consumed the valid execution's admission: %v", err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTaskInferenceFreezeReleasePreservesExecutionAuthority(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	appendInferenceFreeze(t, store, "org-1", 1, true)
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("frozen execution admission: %v", err)
	}
	appendInferenceFreeze(t, store, "org-1", 2, false)
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTaskInferenceRejectsFinishedExecution(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	// Append a runtime finish while retaining the running Task projection to
	// exercise the execution lifetime boundary independently of Task status.
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		_, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: request.Scope.OrganizationID, EventType: "EXECUTION_FINISHED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: map[string]string{"state": "finished"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "finished") {
		t.Fatalf("finished execution admission: %v", err)
	}
}
