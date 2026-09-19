package ledger_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestModelRetryRejectsEarlierMalformedProofBeforeAdmission(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprint(normalization), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "proof.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			draft := events.TrustedDraft{OrganizationID: "org", TaskID: "task-run", CorrelationID: "run", SourceExecutionID: "first", SourceActorID: "runtime", EventType: "PLANNING_CONTEXT_MANIFESTED", Payload: events.PlanningContextPayload{PlanID: "plan-run", IntentID: "intent-run", IntentFingerprint: "fingerprint", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}}
			purpose := inference.PurposePlanning
			if normalization {
				draft.EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
				draft.Payload = events.IntentNormalizationContextPayload{SourceMessageID: "message", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}
				purpose = inference.PurposeIntentNormalization
			}
			if _, err := store.Append(t.Context(), draft); err != nil {
				t.Fatal(err)
			}
			request := inference.InferenceRequest{Scope: inference.Scope{OrganizationID: "org", CorrelationID: "run", TaskID: "task-run", IntentID: "intent-run", RequestID: "first", ExecutionID: "first", Purpose: purpose}, Descriptor: execution.ModelDescriptor{Provider: "provider", Model: "model", ExecutionProfileVersion: "v1"}, PromptSHA256: strings.Repeat("a", 64)}
			if err := store.RecordInferenceNotSent(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `INSERT INTO events SELECT 3,event_id||'-later',organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version FROM events WHERE sequence=2`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `INSERT INTO event_integrity SELECT 3,event_id||'-later',algorithm,previous_hash,event_hash||'-later' FROM event_integrity WHERE sequence=2`); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(map[string]string{"request_id": "first", "prompt_sha256": strings.Repeat("z", 64)})
			if _, err := db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE sequence=2`, body); err != nil {
				t.Fatal(err)
			}
			resealStopRecoveryIntegrity(t, t.Context(), db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			draft.SourceExecutionID = "next"
			if _, err := reopened.Append(t.Context(), draft); err == nil {
				t.Fatal("later valid proof erased earlier malformed proof at live retry admission")
			}
			if err := reopened.CheckExecutionContainment(t.Context(), "org", "task-run", "run", "first"); err == nil {
				t.Fatal("filtered containment read trusted poisoned proof")
			}
		})
	}
}

func TestModelStopFileReplayRejectsRetroactiveRetry(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprint(normalization), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "retry-order.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			draft := events.TrustedDraft{OrganizationID: "org", TaskID: "task-run", CorrelationID: "run", SourceActorID: "runtime", EventType: "PLANNING_CONTEXT_MANIFESTED", Payload: events.PlanningContextPayload{PlanID: "plan-run", IntentID: "intent-run", IntentFingerprint: "fingerprint", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}}
			if normalization {
				draft.EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
				draft.Payload = events.IntentNormalizationContextPayload{SourceMessageID: "message", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}
			}
			for _, execution := range []string{"first", "second"} {
				draft.SourceExecutionID = execution
				manifest, err := store.Append(t.Context(), draft)
				if err != nil {
					t.Fatal(err)
				}
				if execution == "first" {
					next := draft
					next.SourceExecutionID = "too-early"
					if _, err := store.Append(t.Context(), next); err == nil {
						t.Fatal("live admission allowed unresolved retry")
					}
				}
				request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "caller_cancelled")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "NOT_STARTED"}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := ledgerrecovery.VerifyLive(t.Context(), path); err != nil {
				t.Fatalf("valid ordered retry: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			// Move the second manifest before the first attempt's stop evidence.
			if _, err := db.ExecContext(t.Context(), `UPDATE events SET sequence=-sequence WHERE sequence BETWEEN 2 AND 4; UPDATE events SET sequence=CASE sequence WHEN -4 THEN 2 WHEN -2 THEN 3 WHEN -3 THEN 4 END WHERE sequence<0`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(t.Context(), `UPDATE event_integrity SET sequence=-sequence WHERE sequence BETWEEN 2 AND 4; UPDATE event_integrity SET sequence=CASE sequence WHEN -4 THEN 2 WHEN -2 THEN 3 WHEN -3 THEN 4 END WHERE sequence<0`); err != nil {
				t.Fatal(err)
			}
			resealStopRecoveryIntegrity(t, t.Context(), db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			draft.SourceExecutionID = "third"
			if _, err := reopened.Append(t.Context(), draft); err == nil {
				t.Fatal("filtered live retry read ignored invalid prior chronology")
			}
			if _, err := projections.New(events.NewGateway(reopened)).Rebuild(t.Context()); err == nil || !strings.Contains(err.Error(), "unresolved context") {
				t.Fatalf("projection replay error=%v", err)
			}
			if _, err := ledgerrecovery.VerifyLive(t.Context(), path); err == nil || !strings.Contains(err.Error(), "unresolved context") {
				t.Fatalf("independent recovery error=%v", err)
			}
		})
	}
}

func TestModelStopFileReplayRejectsEarlyBadLaterGood(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model-stop.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(store)
	draft := events.TrustedDraft{OrganizationID: "org-1", TaskID: "task-model-stop", CorrelationID: "model-stop", SourceActorID: "runtime", EventType: "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", Payload: events.IntentNormalizationContextPayload{SourceMessageID: "message-1", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}}
	var firstContext, firstRequest events.Event
	for _, execution := range []string{"model-call-1", "model-call-2"} {
		draft.SourceExecutionID = execution
		manifest, err := gateway.PublishTrusted(t.Context(), draft)
		if err != nil {
			t.Fatal(err)
		}
		request, completed, err := gateway.RequestModelStop(t.Context(), manifest.EventID, "runtime_shutdown")
		if err != nil || completed {
			t.Fatalf("request=%v completed=%t", err, completed)
		}
		if execution == "model-call-1" {
			firstContext, firstRequest = manifest, request
		}
		if _, err := gateway.RecordModelStop(t.Context(), request.EventID, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := gateway.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := projections.New(gateway).Rebuild(t.Context()); err != nil {
		t.Fatalf("valid live projection: %v", err)
	}
	if _, err := ledgerrecovery.VerifyLive(t.Context(), path); err != nil {
		t.Fatalf("valid recovery: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var payload events.ModelStopRequest
	if json.Unmarshal(firstRequest.Payload, &payload) != nil {
		t.Fatal("bad test payload")
	}
	payload.ContextEventRef = "missing-earlier-context"
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, firstRequest.EventID); err != nil {
		t.Fatal(err)
	}
	resealStopRecoveryIntegrity(t, t.Context(), db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	gateway = events.NewGateway(reopened)
	if _, _, err := gateway.RequestModelStop(t.Context(), firstContext.EventID, "runtime_shutdown"); err == nil {
		t.Fatal("live stop read ignored earlier invalid context")
	}
	draft.SourceExecutionID = "model-call-3"
	if _, err := gateway.PublishTrusted(t.Context(), draft); err == nil {
		t.Fatal("later valid stop masked invalid earlier retry evidence")
	}
	if _, err := projections.New(gateway).Rebuild(t.Context()); err == nil || !strings.Contains(err.Error(), "model stop") {
		t.Fatalf("live projection error=%v", err)
	}
	if _, err := ledgerrecovery.VerifyLive(t.Context(), path); err == nil || !strings.Contains(err.Error(), "model stop") {
		t.Fatalf("full replay error=%v", err)
	}
}
