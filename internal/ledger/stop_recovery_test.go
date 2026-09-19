package ledger_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

type stopRecoveryModel struct{ execution.FakeModel }

func (stopRecoveryModel) CompleteRequest(context.Context, modelinput.Request) (execution.ModelResponse, error) {
	return execution.ModelResponse{}, execution.SafeModelError(execution.ModelCallFailed, core.ErrContainmentUnavailable)
}

func countStopRecoveryEvents(stream []events.Event, eventType string) int {
	count := 0
	for _, event := range stream {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}

func TestExecutionStopPersistsAcrossFileRecovery(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "execution-stop.db")
	store, gateway, _, _ := seedFileBackedExecutionStop(t, ctx, path)

	recovered, err := app.NewWithModel(gateway, stopRecoveryModel{}).Recover(ctx)
	if err != nil {
		_ = store.Close()
		t.Fatalf("recover admitted execution stop before reopening: %v", err)
	}
	if recovered.TasksExecuted != 0 || recovered.BlockedPreserved != 1 {
		_ = store.Close()
		t.Fatalf("recovery resumed stopped work before reopening: %+v", recovered)
	}
	if _, err := ledgerrecovery.VerifyLive(ctx, path); err != nil {
		_ = store.Close()
		t.Fatalf("verify live execution-stop ledger: %v", err)
	}
	before, err := gateway.Events(ctx, "")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopenedGateway := events.NewGateway(reopened)
	recovered, err = app.NewWithModel(reopenedGateway, stopRecoveryModel{}).Recover(ctx)
	if err != nil {
		t.Fatalf("recover admitted execution stop: %v", err)
	}
	if recovered.TasksExecuted != 0 || recovered.BlockedPreserved != 1 {
		t.Fatalf("recovery resumed stopped work: %+v", recovered)
	}
	after, err := reopenedGateway.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []string{"INFERENCE_RESERVED", "INFERENCE_USAGE_RECORDED", "TOOL_OUTCOME_RECORDED", "TASK_RESULT_RECORDED", "RESULT_PUBLISHED"} {
		if got, want := countStopRecoveryEvents(after, eventType), countStopRecoveryEvents(before, eventType); got != want {
			t.Fatalf("recovery appended %s: before=%d after=%d", eventType, want, got)
		}
	}
}

func TestRecoveryRejectsStopRequestBoundToLaterTenantExecution(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "tampered-execution-stop.db")
	store, gateway, _, later := seedFileBackedExecutionStop(t, ctx, path)
	if _, err := app.NewWithModel(gateway, stopRecoveryModel{}).Recover(ctx); err != nil {
		_ = store.Close()
		t.Fatalf("valid fixture did not replay before tampering: %v", err)
	}
	if _, err := ledgerrecovery.VerifyLive(ctx, path); err != nil {
		_ = store.Close()
		t.Fatalf("valid fixture did not verify before tampering: %v", err)
	}
	stream, err := gateway.Events(ctx, "")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	request, laterStart := stopRecoveryEvents(t, stream, later.Task.ID)
	if request.Sequence >= laterStart.Sequence || request.OrganizationID == laterStart.OrganizationID {
		_ = store.Close()
		t.Fatalf("fixture lacks later unrelated execution: request=%+v later=%+v", request, laterStart)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	tamperStopRequestStart(t, ctx, path, request.EventID, laterStart.EventID)

	tampered, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tamperedGateway := events.NewGateway(tampered)
	if _, err := app.NewWithModel(tamperedGateway, stopRecoveryModel{}).Recover(ctx); err == nil || !strings.Contains(err.Error(), "exact prior start") {
		_ = tampered.Close()
		t.Fatalf("app recovery accepted a stop request bound to a later tenant execution: %v", err)
	}
	if err := tampered.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ledgerrecovery.Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "exact prior start") {
		t.Fatalf("offline recovery accepted a stop request bound to a later tenant execution: %v", err)
	}
}

func seedFileBackedExecutionStop(t *testing.T, ctx context.Context, path string) (*ledger.SQLite, *events.Gateway, app.Result, app.Result) {
	t.Helper()
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gateway := events.NewGateway(store)
	service := app.NewWithModel(gateway, stopRecoveryModel{})
	stopped, err := service.Submit(ctx, app.Submit{
		RequestID: "file-stop", OrganizationID: "org-stopped", Statement: "prepare a note", Kind: core.ExecutionAgent,
	})
	if !errors.Is(err, core.ErrContainmentUnavailable) || stopped.Task.Status != core.TaskBlocked {
		_ = store.Close()
		t.Fatalf("submit did not durably stop: status=%s err=%v", stopped.Task.Status, err)
	}
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := service.WaitForStops(stopCtx); err != nil {
		_ = store.Close()
		t.Fatalf("drain execution-stop evidence: %v", err)
	}
	later, err := service.Submit(ctx, app.Submit{
		RequestID: "later-valid", OrganizationID: "org-later", Statement: "echo later", Kind: core.ExecutionDeterministic,
	})
	if err != nil || later.Task.Status != core.TaskCompleted {
		_ = store.Close()
		t.Fatalf("append later unrelated work: status=%s err=%v", later.Task.Status, err)
	}
	stream, err := gateway.Events(ctx, "")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	for _, eventType := range []string{"EXECUTION_STOP_REQUESTED", "TASK_EXECUTION_SUSPENDED", "EXECUTION_STOP_CONFIRMED"} {
		if countStopRecoveryEvents(stream, eventType) != 1 {
			_ = store.Close()
			t.Fatalf("durable fixture has %d %s events", countStopRecoveryEvents(stream, eventType), eventType)
		}
	}
	return store, gateway, stopped, later
}

func stopRecoveryEvents(t *testing.T, stream []events.Event, laterTaskID core.ID) (events.Event, events.Event) {
	t.Helper()
	var request, laterStart events.Event
	for _, event := range stream {
		if event.EventType == "EXECUTION_STOP_REQUESTED" {
			request = event
		}
		if event.EventType == "EXECUTION_STARTED" && event.TaskID == string(laterTaskID) {
			laterStart = event
		}
	}
	if request.EventID == "" || laterStart.EventID == "" {
		t.Fatalf("fixture lacks stop request or later execution start: request=%+v later=%+v", request, laterStart)
	}
	return request, laterStart
}

func tamperStopRequestStart(t *testing.T, ctx context.Context, path, requestID, executionStartRef string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var payload []byte
	if err := db.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_id=? AND event_type='EXECUTION_STOP_REQUESTED'`, requestID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var request events.ExecutionStopRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		t.Fatal(err)
	}
	request.ExecutionStartRef = executionStartRef
	payload, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_id=?`, payload, requestID)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		t.Fatalf("tamper stop request rows=%d err=%v", changed, err)
	}
	resealStopRecoveryIntegrity(t, ctx, db)
}

type stopRecoveryIntegrityEvent struct {
	sequence                                             int64
	eventID, organizationID, eventType, sourceActorID    string
	sourceExecution, recipientScope, recipientID, taskID string
	authorizationRaw, artifactsRaw, payloadRaw           []byte
	correlationID, createdAt                             string
	schemaVersion                                        int64
}

func resealStopRecoveryIntegrity(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT sequence,event_id,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,CAST(authorization_refs AS BLOB),CAST(artifact_refs AS BLOB),CAST(payload AS BLOB),correlation_id,created_at,schema_version FROM events ORDER BY sequence`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var stream []stopRecoveryIntegrityEvent
	for rows.Next() {
		var event stopRecoveryIntegrityEvent
		if err := rows.Scan(&event.sequence, &event.eventID, &event.organizationID, &event.eventType, &event.sourceActorID,
			&event.sourceExecution, &event.recipientScope, &event.recipientID, &event.taskID, &event.authorizationRaw,
			&event.artifactsRaw, &event.payloadRaw, &event.correlationID, &event.createdAt, &event.schemaVersion); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		stream = append(stream, event)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	previous := ""
	for _, event := range stream {
		digest := sha256.New()
		writeStopRecoveryIntegrityField(digest, []byte("agentos.event-integrity.v1"))
		writeStopRecoveryIntegrityField(digest, []byte(ledger.EventIntegrityAlgorithm))
		writeStopRecoveryIntegrityInt(digest, event.sequence)
		writeStopRecoveryIntegrityField(digest, []byte(previous))
		writeStopRecoveryIntegrityField(digest, []byte(event.eventID))
		writeStopRecoveryIntegrityField(digest, []byte(event.organizationID))
		writeStopRecoveryIntegrityField(digest, []byte(event.eventType))
		writeStopRecoveryIntegrityField(digest, []byte(event.sourceActorID))
		writeStopRecoveryIntegrityField(digest, []byte(event.sourceExecution))
		writeStopRecoveryIntegrityField(digest, []byte(event.recipientScope))
		writeStopRecoveryIntegrityField(digest, []byte(event.recipientID))
		writeStopRecoveryIntegrityField(digest, []byte(event.taskID))
		writeStopRecoveryIntegrityField(digest, event.authorizationRaw)
		writeStopRecoveryIntegrityField(digest, event.artifactsRaw)
		writeStopRecoveryIntegrityField(digest, event.payloadRaw)
		writeStopRecoveryIntegrityField(digest, []byte(event.correlationID))
		writeStopRecoveryIntegrityField(digest, []byte(event.createdAt))
		writeStopRecoveryIntegrityInt(digest, event.schemaVersion)
		current := hex.EncodeToString(digest.Sum(nil))
		result, err := db.ExecContext(ctx, `UPDATE event_integrity SET previous_hash=?,event_hash=? WHERE sequence=? AND event_id=?`, previous, current, event.sequence, event.eventID)
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := result.RowsAffected(); err != nil || changed != 1 {
			t.Fatalf("reseal event %d rows=%d err=%v", event.sequence, changed, err)
		}
		previous = current
	}
}

func writeStopRecoveryIntegrityField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func writeStopRecoveryIntegrityInt(digest hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = digest.Write(encoded[:])
}
