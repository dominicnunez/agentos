package ledger_test

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
	"github.com/dominicnunez/agentos/internal/projections"
)

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
