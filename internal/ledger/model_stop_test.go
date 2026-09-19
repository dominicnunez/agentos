package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func modelStopManifest(t *testing.T, store *SQLite, normalization bool, execution string) events.Event {
	t.Helper()
	draft := events.TrustedDraft{OrganizationID: "org-1", TaskID: "task-model-stop", CorrelationID: "model-stop", SourceExecutionID: execution, SourceActorID: "runtime", EventType: "PLANNING_CONTEXT_MANIFESTED", Payload: events.PlanningContextPayload{PlanID: "plan-model-stop", IntentID: "intent-model-stop", IntentFingerprint: "fingerprint", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}}
	if normalization {
		draft.EventType = "INTENT_NORMALIZATION_CONTEXT_MANIFESTED"
		draft.Payload = events.IntentNormalizationContextPayload{SourceMessageID: "message-1", PromptVersion: "v1", Provider: "provider", Model: "model", ExecutionProfileVersion: "v1", InputEventRefs: []string{"input"}}
	}
	event, err := store.Append(t.Context(), draft)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestStopNamespaceRejectsRecordWriter(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, eventType := range []string{"EXECUTION_STOP_REQUESTED", "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED", "MODEL_STOP_REQUESTED", "MODEL_STOP_UNCERTAIN", "MODEL_STOP_CONFIRMED", "INFERENCE_NOT_SENT"} {
		if err := store.AppendRecord(t.Context(), "org-1", eventType, "runtime", "task-1", nil, nil, "authorization_trace", eventType, 1, struct{}{}); err == nil {
			t.Fatalf("generic record writer admitted %s", eventType)
		}
	}
	if count := stopAdmissionEventCount(t, store); count != 0 {
		t.Fatalf("reserved writer published %d events", count)
	}
}

func TestModelStopRejectsResultWithMissingExecution(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(fmt.Sprint(normalization), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			manifest := modelStopManifest(t, store, normalization, "model-call")
			appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
			request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "security_hold")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			appendInferenceFreeze(t, store, "org-1", 2, false)
			result := modelStopDraft(manifest)
			result.SourceExecutionID = ""
			result.EventType = "PLAN_CREATED"
			plan := core.Plan{ID: "plan-model-stop", IntentID: "intent-model-stop", IntentFingerprint: "fingerprint", Version: 1, CreatedAt: time.Now().UTC()}
			plan.Fingerprint, _ = core.FingerprintPlan(plan)
			result.Payload = plan
			if normalization {
				draft := core.IntentDraft{ID: "intent-model-stop", OrganizationID: "org-1", Version: 1, Objective: "bounded work", CreatedAt: time.Now().UTC()}
				draft.Fingerprint, _ = core.FingerprintIntentDraft(draft)
				result.EventType = "INTENT_DRAFTED"
				result.Payload = events.IntentDraftedPayload{SourceMessageID: "message-1", Draft: draft}
			}
			if _, err := store.Append(t.Context(), result); err == nil {
				t.Fatal("missing execution bypassed committed stop after release")
			}
			if normalization {
				payload := result.Payload.(events.IntentDraftedPayload)
				payload.SourceMessageID = "new-literal-input"
				result.Payload = payload
				if _, err := store.Append(t.Context(), result); err != nil {
					t.Fatalf("unrelated literal normalization was blocked: %v", err)
				}
			} else {
				// Import the omitted identity through the internal append seam to
				// exercise the bounded strategic reader independently of admission.
				if err := store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
					if _, err := appendEvent(ctx, tx, result); err != nil {
						return err
					}
					_, err := boundedStrategicExecutionEvents(ctx, tx, "org-1", "model-stop", core.Work{GoalID: "goal"}, events.ExecutionStartDetail{StrategicEventRefs: []string{"mission", "goal"}, StrategicContextRefs: make([]core.VersionedRef, 2)})
					if err == nil || !strings.Contains(err.Error(), "manifested execution") {
						t.Fatalf("bounded strategic reader bypassed missing identity: %v", err)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestModelRetryOrdinaryClosureAgreement(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		for _, success := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalization=%t/success=%t", normalization, success), func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				manifest := modelStopManifest(t, store, normalization, "first")
				closure := modelStopDraft(manifest)
				closure.EventType, closure.Payload = "PLANNING_FAILED", map[string]string{"code": "FAILED", "reason": "ordinary failure", "evidence_event_ref": manifest.EventID}
				if normalization {
					closure.EventType, closure.Payload = "INTENT_NORMALIZATION_FAILED", struct{}{}
				}
				if success {
					closure.EventType = "PLAN_CREATED"
					plan := core.Plan{ID: "plan-model-stop", IntentID: "intent-model-stop", IntentFingerprint: "fingerprint", Version: 1, CreatedAt: time.Now().UTC()}
					plan.Fingerprint, _ = core.FingerprintPlan(plan)
					closure.Payload = plan
					if normalization {
						draft := core.IntentDraft{ID: "intent-model-stop", OrganizationID: "org-1", Version: 1, CreatedAt: time.Now().UTC()}
						draft.Fingerprint, _ = core.FingerprintIntentDraft(draft)
						closure.EventType, closure.Payload = "INTENT_DRAFTED", events.IntentDraftedPayload{SourceMessageID: "message-1", Draft: draft}
					}
				}
				if _, err := store.Append(t.Context(), closure); err != nil {
					t.Fatal(err)
				}
				prior, err := store.Events(t.Context(), "model-stop")
				if err != nil {
					t.Fatal(err)
				}
				next := modelStopDraft(manifest)
				next.EventType, next.Payload, next.SourceExecutionID = manifest.EventType, manifest.Payload, "second"
				want := normalization && !success
				if _, err := store.Append(t.Context(), next); (err == nil) != want {
					t.Fatalf("live retry allowed=%t want=%t err=%v", err == nil, want, err)
				}
				candidate := manifest
				candidate.EventID, candidate.SourceExecutionID, candidate.Sequence = "candidate", "second", prior[len(prior)-1].Sequence+1
				if err := events.ValidateModelStops(append(prior, candidate), nil); (err == nil) != want {
					t.Fatalf("replay retry allowed=%t want=%t err=%v", err == nil, want, err)
				}
			})
		}
	}
}

func TestModelStopRetryEvidence(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		for _, reason := range []string{"runtime_shutdown", "caller_cancelled", "deadline_exceeded", "containment_unavailable", "security_hold"} {
			for _, local := range []string{"RETURNED", "NOT_STARTED"} {
				t.Run(fmt.Sprintf("normalization=%t/%s/%s", normalization, reason, local), func(t *testing.T) {
					store, err := Open(":memory:")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					manifest := modelStopManifest(t, store, normalization, "model-call-1")
					if reason == "security_hold" {
						appendHistoricalInferenceFreeze(t, store, "org-1", 1, true)
					}
					request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, reason)
					if err != nil {
						t.Fatal(err)
					}
					if reason == "security_hold" {
						appendInferenceFreeze(t, store, "org-1", 2, false)
					}
					next := modelStopDraft(manifest)
					next.EventType, next.Payload, next.SourceExecutionID = manifest.EventType, manifest.Payload, "model-call-2"
					if _, err := store.Append(t.Context(), next); err == nil {
						t.Fatal("pending stop permitted another attempt")
					}
					returned := &events.ModelStopReturn{LocalState: local}
					if local == "RETURNED" {
						returned.ReturnedAt = time.Now().UTC()
					}
					if _, err := store.RecordModelStop(t.Context(), request.EventID, returned); err != nil {
						t.Fatal(err)
					}
					_, err = store.Append(t.Context(), next)
					want := local == "NOT_STARTED" || normalization && reason != "security_hold" && reason != "containment_unavailable"
					if (err == nil) != want {
						t.Fatalf("retry allowed=%t want=%t err=%v", err == nil, want, err)
					}
					stream, err := store.Events(t.Context(), "model-stop")
					if err != nil {
						t.Fatal(err)
					}
					_, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					if err := events.ValidateModelStops(stream, freezes); err != nil {
						t.Fatalf("live accepted but replay rejected: %v", err)
					}
				})
			}
		}
	}
}

func TestModelStopAtomicAccountingAndCompletedFailure(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	manifest := modelStopManifest(t, store, true, "model-call-1")
	request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "caller_cancelled")
	if err != nil {
		t.Fatal(err)
	}
	usage := events.InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, TotalTokens: 1}
	returned := &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: time.Now().UTC(), Usage: &usage}
	if _, err := store.db.ExecContext(t.Context(), `CREATE TRIGGER reject_model_stop BEFORE INSERT ON events WHEN NEW.event_type='MODEL_STOP_CONFIRMED' BEGIN SELECT RAISE(ABORT,'injected confirmation failure'); END`); err != nil {
		t.Fatal(err)
	}
	before := stopAdmissionEventCount(t, store)
	if _, err := store.RecordModelStop(t.Context(), request.EventID, returned); err == nil {
		t.Fatal("ignored confirmation failure")
	}
	if after := stopAdmissionEventCount(t, store); after != before {
		t.Fatal("partial usage committed")
	}
	if _, err := store.db.ExecContext(t.Context(), `DROP TRIGGER reject_model_stop`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordModelStop(t.Context(), request.EventID, returned); err != nil {
		t.Fatal(err)
	}
	changed := *returned
	changed.Usage = nil
	if _, err := store.RecordModelStop(t.Context(), request.EventID, &changed); err == nil {
		t.Fatal("retry removed accounting")
	}
	next := modelStopDraft(manifest)
	next.EventType, next.Payload, next.SourceExecutionID = manifest.EventType, manifest.Payload, "model-call-2"
	second, err := store.Append(t.Context(), next)
	if err != nil {
		t.Fatal(err)
	}
	failure := modelStopDraft(second)
	failure.EventType, failure.Payload = "INTENT_NORMALIZATION_FAILED", struct{}{}
	if _, err := store.Append(t.Context(), failure); err != nil {
		t.Fatal(err)
	}
	if request, completed, err := store.RequestModelStop(t.Context(), second.EventID, "runtime_shutdown"); err != nil || !completed || request.EventID != "" {
		t.Fatalf("ordinary closure lost: completed=%t request=%s err=%v", completed, request.EventID, err)
	}
}

func TestModelStopHistoryCost(t *testing.T) {
	for _, size := range []int{1, 1000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			for i := 0; i < size; i++ {
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "model-stop", EventType: "HISTORICAL_DIAGNOSTIC", Payload: struct{}{}}); err != nil {
					t.Fatal(err)
				}
			}
			manifest := modelStopManifest(t, store, false, "model-call-1")
			if _, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "runtime_shutdown"); err != nil {
				t.Fatal(err)
			}
			rows, err := store.db.QueryContext(t.Context(), `EXPLAIN QUERY PLAN SELECT event_id FROM events WHERE correlation_id=? AND organization_id=? AND source_execution_id=? AND event_type='MODEL_STOP_REQUESTED' LIMIT 1`, manifest.CorrelationID, manifest.OrganizationID, manifest.SourceExecutionID)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var plan string
			for rows.Next() {
				var a, b, c int
				var detail string
				if err := rows.Scan(&a, &b, &c, &detail); err != nil {
					t.Fatal(err)
				}
				plan += detail
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(plan, "events_execution_idx") {
				t.Fatalf("unbounded lookup plan=%s", plan)
			}
			draft := modelStopDraft(manifest)
			draft.EventType, draft.Payload = "CUSTOM_MODEL_OUTPUT", struct{}{}
			started := time.Now()
			for range 50 {
				if _, err := store.Append(t.Context(), draft); !errors.Is(err, core.ErrExecutionStopped) {
					t.Fatal(err)
				}
			}
			t.Logf("history=%d; 50 complete denied publications=%s", size, time.Since(started))
		})
	}
}

func TestModelStopRejectsNewInferenceAndFalseNotStarted(t *testing.T) {
	for _, reserveFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(reserveFirst), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			manifest := modelStopManifest(t, store, false, "model-call-1")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			request := testInferenceRequest(manifest.SourceExecutionID)
			request.Scope.OrganizationID, request.Scope.CorrelationID, request.Scope.TaskID, request.Scope.IntentID = "org-1", "model-stop", "task-model-stop", "intent-model-stop"
			request.Descriptor.Provider, request.Descriptor.Model, request.Descriptor.ExecutionProfileVersion = "provider", "model", "v1"
			if reserveFirst {
				if _, err := store.ReserveInference(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			}
			stop, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "runtime_shutdown")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); !errors.Is(err, core.ErrExecutionStopped) {
				t.Fatalf("stopped reinference=%v", err)
			}
			if _, err := store.RecordModelStop(t.Context(), stop.EventID, &events.ModelStopReturn{LocalState: "NOT_STARTED"}); (err != nil) != reserveFirst {
				t.Fatalf("not started reserveFirst=%t err=%v", reserveFirst, err)
			}
			if !reserveFirst {
				if err := store.RecordInferenceNotSent(t.Context(), request); err == nil {
					t.Fatal("not-started confirmation admitted contradictory later guard invocation")
				}
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("valid stop failed inference replay: %v", err)
			}
		})
	}
}

func TestModelStopIndexMigrationPreservesEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model-stop-v11.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest := modelStopManifest(t, store, false, "model-call-1")
	if _, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "runtime_shutdown"); err != nil {
		t.Fatal(err)
	}
	before, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DROP INDEX events_execution_idx; PRAGMA user_version=11`); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := storageSchemaFingerprint(t.Context(), store.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE agentos_storage SET storage_version=11,schema_fingerprint=?`, fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	after, err := reopened.Events(t.Context(), "")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("migration changed events: %v", err)
	}
	if _, err := ValidateStorageContract(t.Context(), reopened.db); err != nil {
		t.Fatal(err)
	}
}

func TestModelStopRetryAttemptGrowth(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		for _, attempts := range []int{1, 128} {
			t.Run(fmt.Sprintf("normalization=%t/attempts=%d", normalization, attempts), func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				for attempt := range attempts {
					manifest := modelStopManifest(t, store, normalization, fmt.Sprintf("attempt-%d", attempt))
					request, _, err := store.RequestModelStop(t.Context(), manifest.EventID, "caller_cancelled")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := store.RecordModelStop(t.Context(), request.EventID, &events.ModelStopReturn{LocalState: "NOT_STARTED"}); err != nil {
						t.Fatal(err)
					}
				}
				started := time.Now()
				modelStopManifest(t, store, normalization, "next-independent-attempt")
				t.Logf("prior attempts=%d; complete fresh admission=%s", attempts, time.Since(started))
				if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
					t.Fatalf("independent replay of admitted attempts: %v", err)
				}
			})
		}
	}
}

func TestModelStopPersistsBeforeReturn(t *testing.T) {
	for _, normalization := range []bool{false, true} {
		t.Run(map[bool]string{false: "planning", true: "normalization"}[normalization], func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			manifest := modelStopManifest(t, store, normalization, "model-call-1")
			request, completed, err := store.RequestModelStop(t.Context(), manifest.EventID, "runtime_shutdown")
			if err != nil || completed || request.EventType != "MODEL_STOP_REQUESTED" {
				t.Fatalf("request=%+v completed=%t err=%v", request, completed, err)
			}
			uncertain, err := store.RecordModelStop(t.Context(), request.EventID, nil)
			if err != nil || uncertain.EventType != "MODEL_STOP_UNCERTAIN" {
				t.Fatalf("uncertain=%+v err=%v", uncertain, err)
			}
			usage := events.InferenceUsageRecordedPayload{Source: "provider", Provider: "provider", Model: "model", InputTokens: 1, OutputTokens: 2, TotalTokens: 3}
			returned := &events.ModelStopReturn{LocalState: "RETURNED", ReturnedAt: time.Now().UTC(), Usage: &usage}
			confirmed, err := store.RecordModelStop(t.Context(), request.EventID, returned)
			if err != nil || confirmed.EventType != "MODEL_STOP_CONFIRMED" {
				t.Fatalf("confirmed=%+v err=%v", confirmed, err)
			}
			if retry, err := store.RecordModelStop(t.Context(), request.EventID, returned); err != nil || retry.EventID != confirmed.EventID {
				t.Fatalf("retry=%+v err=%v", retry, err)
			}
			var payload events.ModelStopResult
			if json.Unmarshal(confirmed.Payload, &payload) != nil || payload.UsageEventRef == "" || payload.ReturnedAt == nil {
				t.Fatal("missing local return evidence")
			}
			stream, err := store.Events(t.Context(), "model-stop")
			if err != nil || len(stream) != 5 {
				t.Fatalf("events=%d err=%v", len(stream), err)
			}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: manifest.OrganizationID, TaskID: manifest.TaskID, CorrelationID: manifest.CorrelationID, SourceExecutionID: manifest.SourceExecutionID, SourceActorID: "runtime", EventType: "CUSTOM_MODEL_OUTPUT", Payload: struct{}{}}); !errors.Is(err, core.ErrExecutionStopped) {
				t.Fatalf("post-stop output=%v", err)
			}
		})
	}
}
