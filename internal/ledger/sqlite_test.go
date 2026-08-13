package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestExternalWorkIndexMigratesLegacyCorrelation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "org-1", PolicyVersion: "v1", CreatedAt: now}
	agent := core.Agent{ID: "agent-local-org-1", OrganizationID: organization.ID, BlueprintVersion: "v1-local-worker", ExecutionProfileVersion: "v1-fake", RuntimeAdapter: "local", Status: "ACTIVE"}
	intent := core.Intent{ID: "intent-legacy-request", OrganizationID: organization.ID, OriginalInstruction: "echo legacy", NormalizedObjective: "echo legacy", SourcePrincipalID: "agent-1", SourcePrincipalKind: core.PrincipalExternalAgent, SourceChannel: "A2A", SourceMessageID: "message-1", CreatedAt: now}
	work := core.Work{ID: "work-legacy-request", IntentID: intent.ID, Objective: intent.OriginalInstruction, Status: "COMPLETED", CreatedAt: now}
	task := core.Task{ID: "task-legacy-request", WorkID: work.ID, Description: intent.OriginalInstruction, ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "AGENT", AssigneeID: agent.ID, TaskContractVersion: "1", Status: core.TaskPending}
	for _, projection := range []struct {
		eventType, kind, recordID, taskID string
		value                             any
	}{
		{"ORGANIZATION_CREATED", "organization", string(organization.ID), "", organization},
		{"AGENT_CREATED", "agent", string(agent.ID), "", agent},
		{"INTENT_CREATED", "intent", string(intent.ID), "", intent},
		{"WORK_CREATED", "work", string(work.ID), "", work},
		{"TASK_CREATED", "task", string(task.ID), string(task.ID), task},
	} {
		if err := insertLegacyProjection(context.Background(), legacy, projection.eventType, projection.kind, projection.recordID, projection.taskID, projection.value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := legacy.db.ExecContext(context.Background(), `DELETE FROM external_work`); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	migrated, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrated.Close() })
	correlationID, found, err := migrated.ResolveExternalWork(context.Background(), "org-1", "legacy-request")
	if err != nil || !found || correlationID != "legacy-request" {
		t.Fatalf("correlation=%q found=%t err=%v", correlationID, found, err)
	}
	requestID, found, err := migrated.ResolveExternalRequest(context.Background(), "org-1", correlationID)
	if err != nil || !found || requestID != "legacy-request" {
		t.Fatalf("request=%q found=%t err=%v", requestID, found, err)
	}
	requestID, taskCorrelationID, found, err := migrated.ResolveExternalTask(context.Background(), "org-1", "task-legacy-request")
	if err != nil || !found || requestID != "legacy-request" || taskCorrelationID != correlationID {
		t.Fatalf("task request=%q correlation=%q found=%t err=%v", requestID, taskCorrelationID, found, err)
	}
	stream, err := migrated.Events(context.Background(), correlationID)
	if err != nil || len(stream) != 5 {
		t.Fatalf("legacy stream=%+v err=%v", stream, err)
	}
	if err := migrated.Close(); err != nil {
		t.Fatal(err)
	}
	migrated, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := migrated.Events(context.Background(), correlationID)
	if err != nil || len(after) != len(stream) {
		t.Fatalf("repeated migration changed legacy work: before=%d after=%d err=%v", len(stream), len(after), err)
	}
}

func TestReserveExternalWorkAvoidsLegacyCorrelationNamespace(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "attacker-org", EventType: "LEGACY_EVENT", CorrelationID: "w-collision", Payload: map[string]string{"source": "legacy"}}); err != nil {
		t.Fatal(err)
	}
	candidates := []string{"w-collision", "w-reserved"}
	next := 0
	store.newWorkCorrelation = func() (string, error) {
		if next >= len(candidates) {
			return "", errors.New("unexpected allocation attempt")
		}
		candidate := candidates[next]
		next++
		return candidate, nil
	}

	correlationID, err := store.ReserveExternalWork(ctx, "victim-org", "request-1")
	if err != nil || correlationID != "w-reserved" {
		t.Fatalf("reserved correlation=%q err=%v", correlationID, err)
	}
	replayed, err := store.ReserveExternalWork(ctx, "victim-org", "request-1")
	if err != nil || replayed != correlationID || next != len(candidates) {
		t.Fatalf("replayed reservation=%q attempts=%d err=%v", replayed, next, err)
	}
	legacy, err := store.Events(ctx, "w-collision")
	if err != nil || len(legacy) != 1 || legacy[0].OrganizationID != "attacker-org" {
		t.Fatalf("legacy stream changed=%+v err=%v", legacy, err)
	}
}

func insertLegacyProjection(ctx context.Context, l *SQLite, eventType, kind, recordID, taskID string, value any) error {
	encodedValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	record := events.ProjectionRecord{ProjectionKind: kind, RecordID: recordID, Version: 1, CorrelationID: "legacy-request", Value: encodedValue}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: eventType, SourceActorID: "runtime", TaskID: taskID, CorrelationID: "legacy-request"}
	event, payload, err := appendProjectionEvent(ctx, l.db, draft, record, nil)
	if err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, `INSERT INTO records(kind,record_id,version,body,admission_event_id,admission_fingerprint,created_at) VALUES(?,?,?,?,?,?,?)`, kind, recordID, 1, body, event.EventID, payload.Admission.Fingerprint, event.CreatedAt.Format(time.RFC3339Nano))
	return err
}

func TestAppendAndRead(t *testing.T) {
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	e, err := l.Append(context.Background(), events.TrustedDraft{OrganizationID: "o", EventType: "AUDIT_NOTE", TaskID: "1", Payload: map[string]string{"ok": "yes"}, CorrelationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	if e.Sequence != 1 {
		t.Fatalf("sequence=%d", e.Sequence)
	}
	got, err := l.Events(context.Background(), "c")
	if err != nil || len(got) != 1 {
		t.Fatalf("events=%d err=%v", len(got), err)
	}
}

func TestEventsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(ctx, events.TrustedDraft{OrganizationID: "o", EventType: "TASK_BLOCKED", TaskID: "task-1", Payload: map[string]string{"reason": "input required"}, CorrelationID: "request-1"}); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	eventsAfterRestart, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsAfterRestart) != 1 || eventsAfterRestart[0].EventType != "TASK_BLOCKED" || eventsAfterRestart[0].TaskID != "task-1" {
		t.Fatalf("persisted events after reopen=%+v", eventsAfterRestart)
	}
}

func TestProjectionVersionConflictRollsBackItsEvent(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	draft := events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_BLOCKED", SourceActorID: "runtime", RecipientScope: events.RecipientTask, RecipientID: "task-parent", TaskID: "task-1", CorrelationID: "request-1"},
		ProjectionKind: "task",
		RecordID:       "task-1",
		Version:        1,
		Value:          map[string]string{"status": "PENDING"},
	}
	if _, err := l.AppendProjection(ctx, draft); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendProjection(ctx, draft); err == nil {
		t.Fatal("duplicate projection version was accepted")
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(stream) != 1 {
		t.Fatalf("projection failure left an orphan event: %+v", stream)
	}
	available, err := l.Inbox(ctx, events.RecipientTask, "task-parent")
	if err != nil || len(available) != 1 || available[0].EventID != stream[0].EventID {
		t.Fatalf("projection failure changed addressed availability: inbox=%+v err=%v", available, err)
	}
}

func TestProjectionRecordLoadsRequireExactEventAdmission(t *testing.T) {
	for name, corrupt := range map[string]func(context.Context, *SQLite) error{
		"record fingerprint": func(ctx context.Context, store *SQLite) error {
			_, err := store.db.ExecContext(ctx, `UPDATE records SET admission_fingerprint=? WHERE kind='task' AND record_id='task-1'`, strings.Repeat("0", 64))
			return err
		},
		"record event reference": func(ctx context.Context, store *SQLite) error {
			_, err := store.db.ExecContext(ctx, `UPDATE records SET admission_event_id='missing-event' WHERE kind='task' AND record_id='task-1'`)
			return err
		},
		"event envelope": func(ctx context.Context, store *SQLite) error {
			_, err := store.db.ExecContext(ctx, `UPDATE events SET organization_id='org-2' WHERE correlation_id='work-1'`)
			return err
		},
		"event sequence": func(ctx context.Context, store *SQLite) error {
			_, err := store.db.ExecContext(ctx, `UPDATE events SET sequence=sequence+100 WHERE correlation_id='work-1'`)
			return err
		},
		"event time": func(ctx context.Context, store *SQLite) error {
			_, err := store.db.ExecContext(ctx, `UPDATE events SET created_at='2030-01-01T00:00:00Z' WHERE correlation_id='work-1'`)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			event, err := store.AppendProjection(ctx, events.ProjectionDraft{
				Event: events.TrustedDraft{
					OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime",
					TaskID: "task-1", CorrelationID: "work-1",
				},
				ProjectionKind: "task", RecordID: "task-1", Version: 1,
				Value: core.Task{ID: "task-1", WorkID: "work-1", Status: core.TaskPending},
			})
			if err != nil {
				t.Fatal(err)
			}
			payload, present, err := events.AdmittedProjection(event)
			if err != nil || !present || payload.Admission.EventRef != event.EventID {
				t.Fatalf("projection was not sealed: present=%t payload=%+v err=%v", present, payload, err)
			}
			if records, err := store.Records(ctx, "task", "task-1"); err != nil || len(records) != 1 {
				t.Fatalf("valid admitted record unavailable: records=%d err=%v", len(records), err)
			}
			if err := corrupt(ctx, store); err != nil {
				t.Fatal(err)
			}
			if records, err := store.Records(ctx, "task", "task-1"); err == nil || len(records) != 0 {
				t.Fatalf("corrupt admission was returned: records=%d err=%v", len(records), err)
			}
			if records, err := store.LatestRecords(ctx, "task"); err == nil || len(records) != 0 {
				t.Fatalf("corrupt latest admission was returned: records=%d err=%v", len(records), err)
			}
		})
	}
}

func TestGenericSQLiteAppendCannotMintProjectionAuthority(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for name, draft := range map[string]events.TrustedDraft{
		"reserved payload": {
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1",
			Payload: map[string]any{"projection": map[string]string{"record_id": "team-1"}},
		},
		"projection event": {
			OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "work-1",
			Payload: map[string]string{"note": "ordinary payload"},
		},
		"array payload": {
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1",
			Payload: []string{"ordinary"},
		},
		"scalar payload": {
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1",
			Payload: "ordinary",
		},
		"null payload": {
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := store.Append(ctx, draft); err == nil {
				t.Fatalf("generic append accepted %s", name)
			}
		})
	}
	stream, err := store.Events(ctx, "")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected generic projections changed ledger: events=%d err=%v", len(stream), err)
	}
}

func TestProjectionWriterRejectsInvalidBoundaryBeforePersistence(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "setup-1",
		},
		ProjectionKind: "organization", RecordID: "org-1", Version: 1,
		Value: core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()},
	}); err == nil {
		t.Fatal("projection writer accepted an unsupported lifecycle boundary")
	}
	stream, err := store.Events(ctx, "")
	if err != nil || len(stream) != 0 {
		t.Fatalf("invalid projection boundary changed ledger: events=%d err=%v", len(stream), err)
	}
	records, err := store.Records(ctx, "organization", "org-1")
	if err != nil || len(records) != 0 {
		t.Fatalf("invalid projection boundary materialized state: records=%d err=%v", len(records), err)
	}
}

func TestProjectionWriterRejectsUnknownFieldsBeforePersistence(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	value := struct {
		core.Organization
		Unexpected string `json:"unexpected"`
	}{
		Organization: core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()},
		Unexpected:   "unreviewed",
	}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1",
		},
		ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: value,
	}); err == nil {
		t.Fatal("projection writer accepted an unknown value field")
	}
	stream, err := store.Events(ctx, "")
	if err != nil || len(stream) != 0 {
		t.Fatalf("unknown projection field changed ledger: events=%d err=%v", len(stream), err)
	}
	records, err := store.Records(ctx, "organization", "org-1")
	if err != nil || len(records) != 0 {
		t.Fatalf("unknown projection field materialized state: records=%d err=%v", len(records), err)
	}
}

func TestProjectionWriterRejectsMalformedSealedJSONBeforePersistence(t *testing.T) {
	for name, draft := range map[string]events.ProjectionDraft{
		"projection value": {
			Event: events.TrustedDraft{
				OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1",
			},
			ProjectionKind: "organization", RecordID: "org-1", Version: 1,
			Value: json.RawMessage(`{"id":"org-1","id":"org-2"}`),
		},
		"projection detail": {
			Event: events.TrustedDraft{
				OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-1",
				Payload: json.RawMessage(`{"reason":"one","reason":"two"}`),
			},
			ProjectionKind: "organization", RecordID: "org-1", Version: 1,
			Value: core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: time.Now().UTC()},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.AppendProjection(ctx, draft); err == nil {
				t.Fatalf("projection writer accepted malformed %s", name)
			}
			stream, err := store.Events(ctx, "")
			if err != nil || len(stream) != 0 {
				t.Fatalf("malformed %s changed ledger: events=%d err=%v", name, len(stream), err)
			}
			records, err := store.Records(ctx, "organization", "org-1")
			if err != nil || len(records) != 0 {
				t.Fatalf("malformed %s materialized state: records=%d err=%v", name, len(records), err)
			}
		})
	}
}

func TestOpenRejectsNonemptyPreAdmissionProjectionSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-admission.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE records(
kind TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL, body BLOB NOT NULL,
created_at TEXT NOT NULL, PRIMARY KEY(kind,record_id,version));
INSERT INTO records(kind,record_id,version,body,created_at) VALUES('task','task-1',1,'{}','now');`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(path); err == nil {
		_ = store.Close()
		t.Fatal("nonempty pre-admission projection schema was migrated permissively")
	}
}

func TestProjectionBatchRollsBackCompleteTaskGraph(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	drafts := []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: "task-1", CorrelationID: "request-1"}, ProjectionKind: "task", RecordID: "task-1", Version: 1, Value: core.Task{ID: "task-1"}},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", TaskID: "task-1", CorrelationID: "request-1"}, ProjectionKind: "task", RecordID: "task-1", Version: 1, Value: core.Task{ID: "task-1"}},
	}
	if _, err := l.AppendProjections(ctx, drafts); err == nil {
		t.Fatal("conflicting Task-DAG batch was accepted")
	}
	stream, err := l.Events(ctx, "request-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("failed Task-DAG batch left events=%+v err=%v", stream, err)
	}
	records, err := l.Records(ctx, "task", "task-1")
	if err != nil || len(records) != 0 {
		t.Fatalf("failed Task-DAG batch left records=%d err=%v", len(records), err)
	}
}

func TestChildTaskIsNotExternallyAddressable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	correlationID, err := l.ReserveExternalWork(ctx, "org-1", "request-1")
	if err != nil {
		t.Fatal(err)
	}
	rootID := "task-" + correlationID
	childID := rootID + "-child"
	drafts := []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: childID, CorrelationID: correlationID}, ProjectionKind: "task", RecordID: childID, Version: 1, Value: core.Task{ID: core.ID(childID), ParentID: core.ID(rootID)}},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: rootID, CorrelationID: correlationID}, ProjectionKind: "task", RecordID: rootID, Version: 1, Value: core.Task{ID: core.ID(rootID)}},
	}
	if _, err := l.AppendProjections(ctx, drafts); err != nil {
		t.Fatal(err)
	}
	requestID, resolved, found, err := l.ResolveExternalTask(ctx, "org-1", rootID)
	if err != nil || !found || requestID != "request-1" || resolved != correlationID {
		t.Fatalf("root resolution request=%q correlation=%q found=%v err=%v", requestID, resolved, found, err)
	}
	if _, _, found, err := l.ResolveExternalTask(ctx, "org-1", childID); err != nil || found {
		t.Fatalf("internal child became externally addressable: found=%v err=%v", found, err)
	}
	// Simulate a row created by the former startup migration and prove the next
	// startup repairs the durable boundary rather than preserving stale access.
	if _, err := l.db.ExecContext(ctx, `INSERT INTO external_tasks(organization_id,task_id,correlation_id) VALUES(?,?,?)`, "org-1", childID, correlationID); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if _, _, found, err := l.ResolveExternalTask(ctx, "org-1", childID); err != nil || found {
		t.Fatalf("startup migration preserved an externally addressable child: found=%v err=%v", found, err)
	}
	if requestID, resolved, found, err := l.ResolveExternalTask(ctx, "org-1", rootID); err != nil || !found || requestID != "request-1" || resolved != correlationID {
		t.Fatalf("startup migration lost root addressability: request=%q correlation=%q found=%v err=%v", requestID, resolved, found, err)
	}
}

func TestMessageInboxSurvivesReopenAndObservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	message, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "MESSAGE",
		SourceActorID:  "agent-1",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "agent-2",
		TaskID:         "task-1",
		Payload:        map[string]any{"body": "restart-safe handoff"},
	})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	task := core.Task{ID: "task-2", WorkID: "work-1", Description: "consume inbox", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-2", TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task,
	}); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	startEvent, selections, err := l.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: "task-2"}, {Scope: events.RecipientAgent, ID: "agent-2"}})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if len(selections) != 2 || len(selections[0].Events) != 0 || len(selections[1].Events) != 1 || selections[1].Events[0].EventID != message.EventID {
		_ = l.Close()
		t.Fatalf("atomic execution inbox selections=%+v", selections)
	}
	lateMessage, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "MESSAGE", SourceActorID: "agent-1",
		RecipientScope: events.RecipientAgent, RecipientID: "agent-2", TaskID: "task-1",
		Payload: map[string]any{"body": "arrived after execution start"},
	})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if _, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INBOX_EVENTS_OBSERVED", SourceActorID: "agent-2",
		SourceExecutionID: "execution-task-2-v2", RecipientScope: events.RecipientAgent, RecipientID: "agent-2",
		TaskID: "task-2", CorrelationID: "work-1", Payload: map[string]any{"event_ids": []string{message.EventID}},
	}); err == nil {
		_ = l.Close()
		t.Fatal("generic ledger append minted an inbox observation")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	available, err := l.Inbox(ctx, events.RecipientAgent, "agent-2")
	if err != nil || len(available) != 2 || available[0].EventID != message.EventID || available[1].EventID != lateMessage.EventID {
		_ = l.Close()
		t.Fatalf("reopened inbox=%+v err=%v", available, err)
	}
	observation, err := l.ObserveInbox(ctx, events.TrustedDraft{
		OrganizationID:    "org-1",
		EventType:         "INBOX_EVENTS_OBSERVED",
		SourceActorID:     "agent-2",
		SourceExecutionID: "execution-unrelated-v2",
		RecipientScope:    events.RecipientAgent,
		RecipientID:       "agent-2",
		TaskID:            "task-2",
		CorrelationID:     "work-1",
		Payload:           map[string]any{"event_ids": []string{message.EventID}},
	}, events.RecipientAgent, "agent-2", []string{message.EventID})
	if err == nil {
		_ = l.Close()
		t.Fatal("unrelated execution identity observed the Agent inbox")
	}
	if _, err := l.ObserveInbox(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INBOX_EVENTS_OBSERVED", SourceActorID: "agent-2", SourceExecutionID: "execution-task-2-v2",
		RecipientScope: events.RecipientAgent, RecipientID: "agent-2", TaskID: "task-2", CorrelationID: "work-1",
		Payload: map[string]any{"event_ids": []string{lateMessage.EventID}},
	}, events.RecipientAgent, "agent-2", []string{lateMessage.EventID}); err == nil {
		_ = l.Close()
		t.Fatal("execution observed inbox input that arrived after it started")
	}
	observation, err = l.ObserveInbox(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INBOX_EVENTS_OBSERVED", SourceActorID: "agent-2", SourceExecutionID: "execution-task-2-v2",
		RecipientScope: events.RecipientAgent, RecipientID: "agent-2", TaskID: "task-2", CorrelationID: "work-1",
		Payload: map[string]any{"event_ids": []string{message.EventID}},
	}, events.RecipientAgent, "agent-2", []string{message.EventID})
	if err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	if observation.EventType != "INBOX_EVENTS_OBSERVED" {
		_ = l.Close()
		t.Fatalf("observation=%+v", observation)
	}
	var observationPayload events.InboxEventsObservedPayload
	if err := json.Unmarshal(observation.Payload, &observationPayload); err != nil || observationPayload.ExecutionStartEventRef != startEvent.EventID || !slices.Equal(observationPayload.EventIDs, []string{message.EventID}) {
		_ = l.Close()
		t.Fatalf("observation was not bound to its exact execution start: payload=%+v err=%v", observationPayload, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	available, err = l.Inbox(ctx, events.RecipientAgent, "agent-2")
	if err != nil || len(available) != 1 || available[0].EventID != lateMessage.EventID {
		t.Fatalf("observed inbox after reopen=%+v err=%v", available, err)
	}
	stream, err := l.Events(ctx, "")
	if err != nil || len(stream) != 5 || stream[4].EventID != observation.EventID {
		t.Fatalf("durable message/observation stream=%+v err=%v", stream, err)
	}
}

func TestMessageRollbackOnInboxFailure(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if _, err := l.db.ExecContext(ctx, `CREATE TRIGGER fail_inbox_insert BEFORE INSERT ON inbox BEGIN SELECT RAISE(FAIL, 'injected inbox failure'); END;`); err != nil {
		t.Fatal(err)
	}
	_, err = l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "MESSAGE",
		SourceActorID:  "agent-1",
		RecipientScope: events.RecipientAgent,
		RecipientID:    "agent-2",
		Payload:        map[string]any{"body": "must not become available"},
	})
	if err == nil {
		t.Fatal("injected inbox projection failure was ignored")
	}
	stream, readErr := l.Events(ctx, "")
	if readErr != nil || len(stream) != 0 {
		t.Fatalf("failed message left ledger evidence: events=%+v err=%v", stream, readErr)
	}
	available, readErr := l.Inbox(ctx, events.RecipientAgent, "agent-2")
	if readErr != nil || len(available) != 0 {
		t.Fatalf("failed message became available: inbox=%+v err=%v", available, readErr)
	}
}

func TestOpenMigratesEventRoutingColumns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE events (
sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, organization_id TEXT NOT NULL,
event_type TEXT NOT NULL, source_actor_id TEXT NOT NULL DEFAULT '', source_execution_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', authorization_refs BLOB NOT NULL, artifact_refs BLOB NOT NULL, payload BLOB NOT NULL,
correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, schema_version INTEGER NOT NULL);`)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	message, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1",
		EventType:      "MESSAGE",
		SourceActorID:  "agent-1",
		RecipientScope: events.RecipientTask,
		RecipientID:    "task-1",
		Payload:        map[string]any{"body": "after migration"},
	})
	if err != nil {
		t.Fatal(err)
	}
	available, err := l.Inbox(ctx, events.RecipientTask, "task-1")
	if err != nil || len(available) != 1 || available[0].EventID != message.EventID {
		t.Fatalf("migrated inbox=%+v err=%v", available, err)
	}
}

func TestApprovalConsumptionAndAttemptTransitionAreAtomic(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "actor-1", OriginTaskID: "task-1", Action: "send", Resource: "customer-1", Scope: "org-1"}
	if err := l.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "human-1", "task-1", nil, nil, "capability_lease", "lease-1", 1, lease); err != nil {
		t.Fatal(err)
	}
	pending := map[string]any{"effect_obligation_id": "effect-1", "status": "PENDING"}
	if err := l.AppendRecord(ctx, "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", "task-1", nil, nil, "effect", "effect-1", 1, pending); err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "send", Resource: "customer-1", Scope: "org-1", ConsequenceBoundary: core.BoundaryPublicExternal, AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1"}
	fingerprint, err := core.FingerprintEffect(obligation)
	if err != nil {
		t.Fatal(err)
	}
	obligation.EffectFingerprint = fingerprint
	approval := core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "send", Resource: "customer-1", Boundary: core.BoundaryPublicExternal, Status: core.ApprovalApproved, EffectFingerprint: fingerprint, SingleUse: true}
	if err := l.AppendRecord(ctx, "org-1", "APPROVAL_DECIDED", "human-1", "task-1", nil, nil, "approval", "approval-1", 1, approval); err != nil {
		t.Fatal(err)
	}
	if _, err := l.db.ExecContext(ctx, `CREATE TRIGGER fail_effect_attempt BEFORE INSERT ON records WHEN NEW.kind='effect' AND NEW.version=2 BEGIN SELECT RAISE(FAIL, 'injected attempt failure'); END;`); err != nil {
		t.Fatal(err)
	}
	attempted := map[string]any{"effect_obligation_id": "effect-1", "status": "ATTEMPTED"}
	_, err = l.AuthorizeAndAppendEffectAttempt(ctx, obligation, 2, attempted)
	if err == nil {
		t.Fatal("injected attempt failure was ignored")
	}
	stream, err := l.Events(ctx, "")
	if err != nil || len(stream) != 3 || stream[1].EventType != "EFFECT_OBLIGATION_TRANSITIONED" || stream[2].EventType != "APPROVAL_DECIDED" {
		t.Fatalf("failed atomic transition left approval consumption: events=%+v err=%v", stream, err)
	}
	rows, err := l.Records(ctx, "effect", "effect-1")
	if err != nil || len(rows) != 1 {
		t.Fatalf("failed atomic transition changed effect: rows=%d err=%v", len(rows), err)
	}
	if _, err := l.db.ExecContext(ctx, `DROP TRIGGER fail_effect_attempt`); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AuthorizeAndAppendEffectAttempt(ctx, obligation, 2, attempted); err != nil {
		t.Fatalf("rolled-back approval could not be retried: %v", err)
	}
	if _, err := l.AuthorizeAndAppendEffectAttempt(ctx, obligation, 3, attempted); err == nil {
		t.Fatal("consumed single-use approval was reused")
	}
}

func TestAppendRecordRejectsEveryOrganizationalProjectionNamespace(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	for _, kind := range []string{"organization", "mission", "goal", "team", "agent_blueprint", "execution_profile", "agent", "intent", "work", "task", "future_projection"} {
		t.Run(kind, func(t *testing.T) {
			if err := l.AppendRecord(ctx, "org-1", "FORGED_PROJECTION", "runtime", "", nil, nil, kind, "forged-1", 1, map[string]string{"id": "forged-1"}); err == nil {
				t.Fatalf("generic record writer accepted reserved %s projection", kind)
			}
			rows, err := l.Records(ctx, kind, "forged-1")
			if err != nil || len(rows) != 0 {
				t.Fatalf("rejected %s projection changed records: rows=%d err=%v", kind, len(rows), err)
			}
		})
	}
	stream, err := l.Events(ctx, "")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected projection writes changed the ledger: events=%d err=%v", len(stream), err)
	}
}

func TestCompletedWorkRequiresExactDurableEvidence(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	now := time.Now().UTC().Truncate(time.Microsecond)
	criteria := []core.IntentValue{{Value: "verified", Origin: "USER"}}
	appendTestMission(t, ctx, l, "org-1", "mission-1", now)
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "verify work", Mode: core.GoalTarget, SuccessCriteria: criteria, Status: core.GoalActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"},
		ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal,
	}); err != nil {
		t.Fatal(err)
	}
	reviewed := appendReviewedGoalIntent(t, ctx, l, "org-1", "run-1", "intent-run-1", "goal-1", "verify work", core.ExecutionDeterministic, now)
	intentFingerprint := reviewed.Fingerprint
	confirmation := events.IntentConfirmedPayload{
		IntentID: "intent-run-1", GoalID: "goal-1", Version: 1, Fingerprint: intentFingerprint,
		ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "message-1",
	}
	if _, err := l.AppendIntentConfirmation(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: confirmation, CorrelationID: "run-1",
	}, "goal-1"); err != nil {
		t.Fatal(err)
	}
	intent := core.Intent{
		ID: "intent-run-1", OrganizationID: "org-1", GoalID: "goal-1", NormalizedObjective: "verify work", CompletionCriteria: criteria, AcceptedFingerprint: intentFingerprint,
		SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "message-1", CreatedAt: now,
	}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "intent", RecordID: "intent-run-1", Version: 1, Value: intent,
	}); err != nil {
		t.Fatal(err)
	}
	active := core.Work{ID: "work-1", IntentID: "intent-run-1", GoalID: "goal-1", Objective: "verify work", Status: core.WorkActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: "work-1", Version: 1, Value: active,
	}); err != nil {
		t.Fatal(err)
	}
	completed := active
	completed.Status = core.WorkCompleted
	draft := events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_COMPLETED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: "work-1", Version: 2, Value: completed,
	}
	if _, err := l.AppendProjection(ctx, draft); err == nil {
		t.Fatal("generic projection path admitted completed Work")
	}
	draft.Event.Payload = events.WorkCompletionTransitionPayload{EvidenceEventRef: "evt-missing", Fingerprint: string(make([]byte, 64))}
	if _, err := l.AppendWorkCompletion(ctx, draft); err == nil {
		t.Fatal("missing evidence authorized Work completion")
	}
	plan := core.Plan{ID: "plan-1", IntentID: "intent-1", IntentFingerprint: intentFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root"}}, CreatedAt: now}
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", SourceExecutionID: "planning-1", TaskID: "task-run-1", Payload: plan, CorrelationID: "run-1"}); err != nil {
		t.Fatal(err)
	}
	task := core.Task{ID: "task-run-1", WorkID: "work-1", Description: "verify", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "run-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task,
	}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskCompleted
	failedOutcome := core.ToolOutcome{
		ToolInvocationID: "tool-invocation-1", ToolID: "test.verifier", ToolVersion: "v1",
		Status: core.OutcomeFailed, PostconditionStatus: core.PostconditionFailed,
		Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now,
	}
	outcomeEvent, err := l.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", TaskID: string(task.ID), Payload: failedOutcome, CorrelationID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	forged := events.CompletionDecisionPayload{
		Contract: core.CompletionContract{TaskID: task.ID, TaskVersion: 2, Criteria: []core.CompletionCriterion{{ID: "verified", Assurance: core.AssuranceDeterministic, Required: true}}},
		Result:   events.CompletionDecisionResultPayload{Complete: true}, OutcomeEventRef: outcomeEvent.EventID,
	}
	verificationEvent, err := l.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "COMPLETION_VERIFIED", SourceActorID: "runtime", TaskID: string(task.ID), Payload: forged, CorrelationID: "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	taskEvent, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_VERIFIED_COMPLETE", SourceActorID: "runtime", TaskID: string(task.ID), Payload: forged, CorrelationID: "run-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence := events.WorkCompletionEvidencePayload{
		WorkID: "work-1", WorkVersion: 2, GoalID: "goal-1", IntentID: "intent-1",
		IntentFingerprint: intentFingerprint,
		PlanID:            "plan-1", PlanVersion: 1,
		Criteria: criteria,
		Tasks: []events.WorkCompletionTaskEvidencePayload{{
			TaskID: "task-run-1", TaskVersion: 2, VerificationEventRef: "evt-verification", CompletionEventRef: "evt-completion", ArtifactRefs: []string{},
		}},
		ArtifactRefs: []string{}, CreatedAt: now,
	}
	evidence.Fingerprint, err = evidence.ExpectedFingerprint()
	if err != nil || !evidence.Valid() {
		t.Fatalf("test evidence is invalid: evidence=%+v err=%v", evidence, err)
	}
	if _, err := l.AppendWorkCompletionEvidence(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "WORK_COMPLETION_EVALUATED", SourceActorID: "runtime",
		ArtifactRefs: evidence.ArtifactRefs, Payload: evidence, CorrelationID: "run-1",
	}); err == nil {
		t.Fatal("typed admission accepted nonexistent Task evidence references")
	}
	forgedEvidence := evidence
	forgedEvidence.Tasks = append([]events.WorkCompletionTaskEvidencePayload(nil), evidence.Tasks...)
	forgedEvidence.Tasks[0].VerificationEventRef = verificationEvent.EventID
	forgedEvidence.Tasks[0].CompletionEventRef = taskEvent.EventID
	forgedEvidence.Fingerprint, err = forgedEvidence.ExpectedFingerprint()
	if err != nil || !forgedEvidence.Valid() {
		t.Fatalf("forged test evidence is invalid: evidence=%+v err=%v", forgedEvidence, err)
	}
	if _, err := l.AppendWorkCompletionEvidence(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "WORK_COMPLETION_EVALUATED", SourceActorID: "runtime",
		ArtifactRefs: forgedEvidence.ArtifactRefs, Payload: forgedEvidence, CorrelationID: "run-1",
	}); err == nil {
		t.Fatal("typed admission accepted a forged completion decision")
	}
	records, err := l.Records(ctx, "work", "work-1")
	if err != nil || len(records) != 1 {
		t.Fatalf("rejected Work completion changed durable projection: records=%d err=%v", len(records), err)
	}
}

func TestGenericAppendRejectsTypedTerminalEvidence(t *testing.T) {
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for _, eventType := range []string{"WORK_COMPLETION_EVALUATED", "WORK_COMPLETED", "GOAL_PROGRESS_EVALUATED", "GOAL_ACHIEVED"} {
		if _, err := l.Append(context.Background(), events.TrustedDraft{
			OrganizationID: "org-1", EventType: eventType, SourceActorID: "runtime", CorrelationID: "run-1",
		}); err == nil {
			t.Fatalf("generic ledger append accepted %s", eventType)
		}
	}
	stream, err := l.Events(context.Background(), "run-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("rejected terminal evidence changed the ledger: events=%d err=%v", len(stream), err)
	}
}

func TestGoalProgressWitnessSelectionCrossesFormerEvidenceWindow(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	wanted := core.IntentValue{Value: "verified recurring outcome", Origin: "RUNTIME_TEST"}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "keep producing verified outcomes", Mode: core.GoalContinuous, SuccessCriteria: []core.IntentValue{wanted}, Status: core.GoalActive, CreatedAt: time.Now().UTC()}
	var selectedID, finalTransitionID string
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		for index := 0; index < 4097; index++ {
			criteria := []core.IntentValue{{Value: "unrelated outcome", Origin: "RUNTIME_TEST"}}
			if index == 4096 {
				criteria = []core.IntentValue{wanted}
			}
			correlationID := fmt.Sprintf("work-%d", index)
			evidenceEvent, err := appendEvent(ctx, tx, events.TrustedDraft{
				OrganizationID: "org-1", EventType: "WORK_COMPLETION_EVALUATED", SourceActorID: "runtime",
				CorrelationID: correlationID, Payload: map[string]any{"criteria": criteria},
			})
			if err != nil {
				return err
			}
			workID := core.ID(correlationID)
			workValue, err := json.Marshal(core.Work{ID: workID, GoalID: goal.ID, Status: core.WorkCompleted})
			if err != nil {
				return err
			}
			detail, err := json.Marshal(events.WorkCompletionTransitionPayload{EvidenceEventRef: evidenceEvent.EventID, Fingerprint: fmt.Sprintf("%064d", index)})
			if err != nil {
				return err
			}
			transition, _, err := appendProjectionEvent(ctx, tx, events.TrustedDraft{
				OrganizationID: "org-1", EventType: "WORK_COMPLETED", SourceActorID: "runtime", CorrelationID: correlationID,
			}, events.ProjectionRecord{ProjectionKind: "work", RecordID: string(workID), Version: 2, CorrelationID: correlationID, Value: workValue}, detail)
			if err != nil {
				return err
			}
			if index == 4096 {
				finalTransitionID = transition.EventID
			}
		}
		selected, err := goalProgressWitnessTransitions(ctx, tx, "org-1", goal)
		if err != nil {
			return err
		}
		if len(selected) != 1 {
			return fmt.Errorf("selected %d witnesses, want 1", len(selected))
		}
		selectedID = selected[0].EventID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if selectedID != finalTransitionID {
		t.Fatalf("criterion evidence after the former 4096-Work window was not selected: got=%s want=%s", selectedID, finalTransitionID)
	}
}

func TestGoalRevisionMustPrecedeProgressEvaluation(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	goal := core.Goal{
		ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "produce evidence",
		Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified", Origin: "RUNTIME_TEST"}}, Status: core.GoalActive, CreatedAt: time.Now().UTC(),
	}
	value, err := json.Marshal(goal)
	if err != nil {
		t.Fatal(err)
	}
	record := events.ProjectionRecord{ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, CorrelationID: "goal-1", Value: value}
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		revision, _, err := appendProjectionEvent(ctx, tx, events.TrustedDraft{
			OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1",
		}, record, nil)
		if err != nil {
			return err
		}
		if err := validateGoalRevisionBeforeEvaluation(ctx, tx, "org-1", record, revision.Sequence+1); err != nil {
			return fmt.Errorf("valid causal Goal revision was rejected: %w", err)
		}
		if err := validateGoalRevisionBeforeEvaluation(ctx, tx, "org-1", record, revision.Sequence); err == nil {
			return errors.New("Goal evaluation was accepted before its revision")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoalMissionMustBeActiveAtEvaluation(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	now := time.Now().UTC()
	appendTestMission(t, ctx, l, "org-1", "mission-1", now)
	retired := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "durable direction", Status: core.MissionRetired, CreatedAt: now}
	retirement, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_RETIRED", SourceActorID: "runtime", CorrelationID: "mission-1"},
		ProjectionKind: "mission", RecordID: "mission-1", Version: 2, Value: retired,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = l.withTx(ctx, func(tx *sql.Tx) error {
		if err := validateActiveMissionAt(ctx, tx, "org-1", "mission-1", retirement.Sequence); err != nil {
			return fmt.Errorf("legitimate post-evaluation retirement was rejected: %w", err)
		}
		if err := validateActiveMissionAt(ctx, tx, "org-1", "mission-1", retirement.Sequence+1); err == nil {
			return errors.New("evaluation after Mission retirement was accepted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntentConfirmationGoalArgumentMatchesPayload(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	draft := events.TrustedDraft{
		OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", CorrelationID: "run-1",
		Payload: events.IntentConfirmedPayload{IntentID: "intent-1", GoalID: "goal-2", Version: 1, Fingerprint: "fingerprint", MessageID: "message-1"},
	}
	if _, err := l.AppendIntentConfirmation(ctx, draft, "goal-1"); err == nil {
		t.Fatal("ledger checked one Goal while persisting a different payload Goal")
	}
	stream, err := l.Events(ctx, "run-1")
	if err != nil || len(stream) != 0 {
		t.Fatalf("mismatched Goal confirmation reached ledger: events=%+v err=%v", stream, err)
	}
}

func TestGoalBoundIntentConfirmationConcurrentReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	appendTestMission(t, ctx, l, "org-1", "mission-1", now)
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded direction", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "done", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"},
		ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal,
	}); err != nil {
		t.Fatal(err)
	}
	confirmation := events.IntentConfirmedPayload{
		IntentID: "intent-run-1", GoalID: "goal-1", Version: 1, Fingerprint: appendReviewedGoalIntent(t, ctx, l, "org-1", "run-1", "intent-run-1", "goal-1", "bounded work", core.ExecutionDeterministic, now).Fingerprint,
		ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1",
	}
	draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: confirmation, CorrelationID: "run-1"}
	start := make(chan struct{})
	eventsOut := make(chan events.Event, 8)
	errorsOut := make(chan error, 8)
	var callers sync.WaitGroup
	for range 8 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-start
			event, err := l.AppendIntentConfirmation(ctx, draft, "goal-1")
			eventsOut <- event
			errorsOut <- err
		}()
	}
	close(start)
	callers.Wait()
	close(eventsOut)
	close(errorsOut)
	var eventID string
	for err := range errorsOut {
		if err != nil {
			t.Fatalf("identical concurrent confirmation failed: %v", err)
		}
	}
	for event := range eventsOut {
		if eventID == "" {
			eventID = event.EventID
		}
		if event.EventID == "" || event.EventID != eventID {
			t.Fatalf("concurrent confirmation did not reuse one event: first=%s event=%+v", eventID, event)
		}
	}
	stream, err := l.Events(ctx, "run-1")
	if err != nil || len(stream) != 3 || stream[2].EventID != eventID {
		t.Fatalf("concurrent confirmation appended duplicates: events=%+v err=%v", stream, err)
	}
	conflict := confirmation
	conflict.MessageID = "confirmation-2"
	draft.Payload = conflict
	if _, err := l.AppendIntentConfirmation(ctx, draft, "goal-1"); err == nil {
		t.Fatal("conflicting confirmation reused or replaced durable state")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	draft.Payload = confirmation
	replayed, err := reopened.AppendIntentConfirmation(ctx, draft, "goal-1")
	if err != nil || replayed.EventID != eventID {
		t.Fatalf("restart did not preserve idempotent confirmation: event=%+v err=%v", replayed, err)
	}
}

func TestGoalBoundIntentConfirmationRequiresExactReviewedEvidence(t *testing.T) {
	for _, test := range []struct {
		name       string
		sourceText string
		mutate     func(*events.IntentConfirmedPayload, *events.TrustedDraft)
		seedReview bool
	}{
		{name: "missing durable review"},
		{name: "Goal absent from attributed message", seedReview: true, sourceText: "Use goal-10 for this work"},
		{name: "changed reviewed fingerprint", seedReview: true, sourceText: "Use goal-1 for this work", mutate: func(payload *events.IntentConfirmedPayload, _ *events.TrustedDraft) {
			payload.Fingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}},
		{name: "runtime actor", seedReview: true, sourceText: "Use goal-1 for this work", mutate: func(payload *events.IntentConfirmedPayload, draft *events.TrustedDraft) {
			payload.ConfirmingActorID = "runtime"
			payload.ConfirmingActorKind = string(core.PrincipalRuntime)
			payload.SourceChannel = "INTERNAL"
			draft.SourceActorID = "runtime"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			l, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			now := time.Now().UTC()
			appendTestMission(t, ctx, l, "org-1", "mission-1", now)
			goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded direction", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "done", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
			if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
				Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"},
				ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal,
			}); err != nil {
				t.Fatal(err)
			}
			fingerprint := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			if test.seedReview {
				fingerprint = appendReviewedGoalIntentWithSource(t, ctx, l, "org-1", "run-1", "intent-run-1", "goal-1", "bounded work", test.sourceText, core.ExecutionDeterministic, now).Fingerprint
			}
			payload := events.IntentConfirmedPayload{
				IntentID: "intent-run-1", GoalID: "goal-1", Version: 1, Fingerprint: fingerprint,
				ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-1",
			}
			draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: payload, CorrelationID: "run-1"}
			if test.mutate != nil {
				test.mutate(&payload, &draft)
				draft.Payload = payload
			}
			before, err := l.Events(ctx, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.AppendIntentConfirmation(ctx, draft, "goal-1"); err == nil {
				t.Fatal("unreviewed or forged Goal-bound confirmation was admitted")
			}
			after, err := l.Events(ctx, "run-1")
			if err != nil || len(after) != len(before) {
				t.Fatalf("rejected confirmation changed the ledger: before=%d after=%d err=%v", len(before), len(after), err)
			}
		})
	}
}

func TestMissionAndGoalRevisionsRequireExactRuntimeLifecycleEvents(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	now := time.Now().UTC()
	appendTestMission(t, ctx, l, "org-1", "mission-1", now)
	mission := core.Mission{ID: "mission-1", OrganizationID: "org-1", Statement: "durable direction", Status: core.MissionActive, CreatedAt: now}
	revisedMission := mission
	revisedMission.Statement = "refined direction"
	for name, draft := range map[string]events.ProjectionDraft{
		"Agent authored": {Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_REVISED", SourceActorID: "agent-1", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: "mission-1", Version: 2, Value: revisedMission},
		"mislabeled":     {Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_RETIRED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: "mission-1", Version: 2, Value: revisedMission},
	} {
		t.Run(name+" Mission", func(t *testing.T) {
			if _, err := l.AppendProjection(ctx, draft); err == nil {
				t.Fatal("invalid Mission revision was admitted")
			}
		})
	}
	missionRecords, err := l.Records(ctx, "mission", "mission-1")
	if err != nil || len(missionRecords) != 1 {
		t.Fatalf("rejected Mission revision changed durable state: records=%d err=%v", len(missionRecords), err)
	}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_REVISED", SourceActorID: "runtime", CorrelationID: "mission-1"}, ProjectionKind: "mission", RecordID: "mission-1", Version: 2, Value: revisedMission}); err != nil {
		t.Fatalf("valid Mission refinement failed: %v", err)
	}
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded direction", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "done", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	for name, draft := range map[string]events.ProjectionDraft{
		"Agent authored": {Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "agent-1", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal},
		"mislabeled":     {Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_ACHIEVED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal},
	} {
		t.Run(name+" Goal", func(t *testing.T) {
			if _, err := l.AppendProjection(ctx, draft); err == nil {
				t.Fatal("invalid Goal creation was admitted")
			}
		})
	}
	goalRecords, err := l.Records(ctx, "goal", "goal-1")
	if err != nil || len(goalRecords) != 0 {
		t.Fatalf("rejected Goal creation changed durable state: records=%d err=%v", len(goalRecords), err)
	}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal}); err != nil {
		t.Fatalf("valid Goal creation failed: %v", err)
	}
	refinedGoal := goal
	refinedGoal.Objective = "more specific direction"
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_ACHIEVED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: "goal-1", Version: 2, Value: refinedGoal}); err == nil {
		t.Fatal("mislabeled Goal refinement was admitted")
	}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_REFINED", SourceActorID: "runtime", CorrelationID: "goal-1"}, ProjectionKind: "goal", RecordID: "goal-1", Version: 2, Value: refinedGoal}); err != nil {
		t.Fatalf("valid Goal refinement failed: %v", err)
	}
}

func TestWorkProjectionMatchesAcceptedIntentGoal(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	now := time.Now().UTC()
	appendTestMission(t, ctx, l, "org-1", "mission-1", now)
	goal := core.Goal{ID: "goal-a", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded direction", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "done", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-a"},
		ProjectionKind: "goal", RecordID: "goal-a", Version: 1, Value: goal,
	}); err != nil {
		t.Fatal(err)
	}
	reviewed := appendReviewedGoalIntent(t, ctx, l, "org-1", "run-1", "intent-run-1", "goal-a", "bounded work", core.ExecutionDeterministic, now)
	fingerprint := reviewed.Fingerprint
	confirmation := events.IntentConfirmedPayload{IntentID: "intent-run-1", GoalID: "goal-a", Version: 1, Fingerprint: fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "message-1"}
	if _, err := l.AppendIntentConfirmation(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: confirmation, CorrelationID: "run-1"}, "goal-a"); err != nil {
		t.Fatal(err)
	}
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", GoalID: "goal-a", NormalizedObjective: "bounded work", AcceptedFingerprint: fingerprint, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "message-1", CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent,
	}); err != nil {
		t.Fatal(err)
	}
	failedWithoutCreation := core.Work{ID: "work-version-gap", IntentID: intent.ID, GoalID: intent.GoalID, Objective: "bounded work", Status: core.WorkFailed, CreatedAt: now}
	streamBefore, err := l.Events(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: string(failedWithoutCreation.ID), Version: 2, Value: failedWithoutCreation,
	}); err == nil {
		t.Fatal("Work version gap was appended without an active creation")
	}
	streamAfter, err := l.Events(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(streamAfter) != len(streamBefore) {
		t.Fatal("rejected Work version gap left an authoritative event")
	}
	objectiveMismatch := core.Work{ID: "work-objective-mismatch", IntentID: intent.ID, GoalID: intent.GoalID, Objective: "different objective", Status: core.WorkActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: string(objectiveMismatch.ID), Version: 1, Value: objectiveMismatch,
	}); err == nil {
		t.Fatal("Work objective differed from its accepted Intent")
	}
	streamAfter, err = l.Events(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(streamAfter) != len(streamBefore) {
		t.Fatal("rejected Work objective mismatch left an authoritative event")
	}
	work := core.Work{ID: "work-1", IntentID: intent.ID, GoalID: "goal-b", Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work,
	}); err == nil {
		t.Fatal("Work was bound to a Goal different from its accepted Intent")
	}
	records, err := l.Records(ctx, "work", "work-1")
	if err != nil || len(records) != 0 {
		t.Fatalf("mismatched Work binding reached records: records=%d err=%v", len(records), err)
	}
	active := core.Work{ID: "work-lifecycle", IntentID: intent.ID, GoalID: intent.GoalID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	creation := events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: string(active.ID), Version: 1, Value: active,
	}
	for name, mutate := range map[string]func(*events.ProjectionDraft){
		"Agent authored": func(draft *events.ProjectionDraft) { draft.Event.SourceActorID = "agent-1" },
		"mislabeled":     func(draft *events.ProjectionDraft) { draft.Event.EventType = "WORK_FAILED" },
		"addressed": func(draft *events.ProjectionDraft) {
			draft.Event.RecipientScope, draft.Event.RecipientID = events.RecipientAgent, "agent-1"
		},
	} {
		t.Run(name+" creation", func(t *testing.T) {
			draft := creation
			mutate(&draft)
			if _, err := l.AppendProjection(ctx, draft); err == nil {
				t.Fatal("invalid Work creation was admitted")
			}
		})
	}
	if _, err := l.AppendProjection(ctx, creation); err != nil {
		t.Fatalf("valid Work creation failed: %v", err)
	}
	failed := active
	failed.Status = core.WorkFailed
	failure := events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: string(failed.ID), Version: 2, Value: failed,
	}
	for name, mutate := range map[string]func(*events.ProjectionDraft){
		"Agent authored": func(draft *events.ProjectionDraft) { draft.Event.SourceActorID = "agent-1" },
		"mislabeled":     func(draft *events.ProjectionDraft) { draft.Event.EventType = "WORK_CREATED" },
		"execution bound": func(draft *events.ProjectionDraft) {
			draft.Event.SourceExecutionID = "execution-1"
		},
	} {
		t.Run(name+" failure", func(t *testing.T) {
			draft := failure
			mutate(&draft)
			if _, err := l.AppendProjection(ctx, draft); err == nil {
				t.Fatal("invalid Work failure was admitted")
			}
		})
	}
	records, err = l.Records(ctx, "work", string(active.ID))
	if err != nil || len(records) != 1 {
		t.Fatalf("rejected Work revisions changed durable state: records=%d err=%v", len(records), err)
	}
	if _, err := l.AppendProjection(ctx, failure); err != nil {
		t.Fatalf("valid Work failure failed: %v", err)
	}
	records, err = l.Records(ctx, "work", string(active.ID))
	if err != nil || len(records) != 2 {
		t.Fatalf("valid Work failure was not durable: records=%d err=%v", len(records), err)
	}
}

func TestGoalBoundWorkRequiresAtomicIntentConfirmation(t *testing.T) {
	ctx := context.Background()
	l, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	now := time.Now().UTC()
	appendTestMission(t, ctx, l, "org-1", "mission-1", now)
	goal := core.Goal{ID: "goal-1", OrganizationID: "org-1", MissionID: "mission-1", Objective: "bounded direction", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "done", Origin: "USER"}}, Status: core.GoalActive, CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "goal-1"},
		ProjectionKind: "goal", RecordID: "goal-1", Version: 1, Value: goal,
	}); err != nil {
		t.Fatal(err)
	}
	reviewed := appendReviewedGoalIntent(t, ctx, l, "org-1", "run-1", "intent-run-1", "goal-1", "bounded work", core.ExecutionDeterministic, now)
	fingerprint := reviewed.Fingerprint
	confirmation := events.IntentConfirmedPayload{IntentID: "intent-run-1", GoalID: "goal-1", Version: 1, Fingerprint: fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "message-1"}
	confirmationDraft := events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-run-1", Payload: confirmation, CorrelationID: "run-1"}
	if _, err := l.Append(ctx, confirmationDraft); err == nil {
		t.Fatal("generic ledger append bypassed atomic Goal admission")
	}
	intent := core.Intent{ID: "intent-run-1", OrganizationID: "org-1", GoalID: "goal-1", NormalizedObjective: "bounded work", AcceptedFingerprint: fingerprint, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "message-1", CreatedAt: now}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "intent", RecordID: "intent-run-1", Version: 1, Value: intent,
	}); err != nil {
		t.Fatal(err)
	}
	work := core.Work{ID: "work-1", IntentID: "intent-run-1", GoalID: "goal-1", Objective: "bounded work", Status: core.WorkActive, CreatedAt: now}
	workDraft := events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "run-1"},
		ProjectionKind: "work", RecordID: "work-1", Version: 1, Value: work,
	}
	if _, err := l.AppendProjection(ctx, workDraft); err == nil {
		t.Fatal("Goal-bound Work was admitted without atomic Intent confirmation")
	}
	if _, err := l.AppendIntentConfirmation(ctx, confirmationDraft, "goal-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.AppendProjection(ctx, workDraft); err != nil {
		t.Fatalf("atomically confirmed Goal-bound Work was rejected: %v", err)
	}
}

func appendReviewedGoalIntent(t *testing.T, ctx context.Context, l *SQLite, organizationID, correlationID, intentID, goalID, objective string, kind core.ExecutionKind, createdAt time.Time) core.IntentDraft {
	t.Helper()
	return appendReviewedGoalIntentWithSource(t, ctx, l, organizationID, correlationID, intentID, goalID, objective, objective+" under "+goalID, kind, createdAt)
}

func appendReviewedGoalIntentWithSource(t *testing.T, ctx context.Context, l *SQLite, organizationID, correlationID, intentID, goalID, objective, sourceText string, kind core.ExecutionKind, createdAt time.Time) core.IntentDraft {
	t.Helper()
	sourceMessageID := "source-" + correlationID
	taskID := "task-" + correlationID
	message := events.IntakeMessageRecordedPayload{
		MessageID: sourceMessageID, Text: sourceText,
		SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", RequestedExecutionKind: kind,
	}
	if _, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", TaskID: taskID, Payload: message, CorrelationID: correlationID,
	}); err != nil {
		t.Fatal(err)
	}
	draft := core.IntentDraft{
		ID: core.ID(intentID), OrganizationID: core.ID(organizationID), Version: 1, Status: core.IntentStatusReadyForReview, RequestedExecutionKind: kind,
		Goal: &core.IntentValue{Value: goalID, Origin: "EXPLICIT", SourceMessageID: sourceMessageID}, Objective: objective,
		Context: []core.IntentValue{}, Deliverables: []core.IntentValue{{Value: "Produce the requested result.", Origin: "DEFAULT"}},
		CompletionCriteria: []core.IntentValue{{Value: "The result is independently verified.", Origin: "DEFAULT"}}, Constraints: []core.IntentValue{},
		ResolvedDecisions: []core.IntentDecision{}, ConsequenceCandidates: []string{}, MissingUserInputs: []core.IntentValue{}, CreatedAt: createdAt,
	}
	fingerprint, err := core.FingerprintIntentDraft(draft)
	if err != nil {
		t.Fatal(err)
	}
	draft.Fingerprint = fingerprint
	if _, err := l.Append(ctx, events.TrustedDraft{
		OrganizationID: organizationID, EventType: "INTENT_DRAFTED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlationID,
		Payload: events.IntentDraftedPayload{SourceMessageID: sourceMessageID, Draft: draft, Reply: "Review the proposed intent before work begins."},
	}); err != nil {
		t.Fatal(err)
	}
	return draft
}

func appendTestMission(t *testing.T, ctx context.Context, l *SQLite, organizationID, missionID core.ID, createdAt time.Time) {
	t.Helper()
	organization := core.Organization{ID: organizationID, Name: "Organization", PolicyVersion: "v1", CreatedAt: createdAt}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organizationID), EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: string(organizationID)},
		ProjectionKind: "organization", RecordID: string(organizationID), Version: 1, Value: organization,
	}); err != nil {
		t.Fatal(err)
	}
	mission := core.Mission{ID: missionID, OrganizationID: organizationID, Statement: "durable direction", Status: core.MissionActive, CreatedAt: createdAt}
	if _, err := l.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(organizationID), EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: string(missionID)},
		ProjectionKind: "mission", RecordID: string(missionID), Version: 1, Value: mission,
	}); err != nil {
		t.Fatal(err)
	}
}
