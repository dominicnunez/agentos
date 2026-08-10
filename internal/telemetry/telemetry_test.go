package telemetry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestRunTelemetryIsComplete(t *testing.T) {
	started := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	task := core.Task{ID: "task-1", ExecutionKind: core.ExecutionAgent, Status: core.TaskPending}
	projection := events.ProjectionEventPayload{Projection: events.ProjectionRecord{ProjectionKind: "task", RecordID: "task-1", Version: 1, Value: jsonBody(t, task)}}
	task.Status = core.TaskCompleted
	completedProjection := events.ProjectionEventPayload{Projection: events.ProjectionRecord{ProjectionKind: "task", RecordID: "task-1", Version: 2, Value: jsonBody(t, task)}}
	cost := 0.125
	stream := []events.Event{
		testEvent(t, "e1", "TASK_CREATED", "task-1", "", started, projection),
		testEvent(t, "e2", "EXECUTION_STARTED", "task-1", "", started.Add(time.Second), projection),
		testEvent(t, "e3", "EXECUTION_CONTEXT_MANIFESTED", "task-1", "execution-1", started.Add(2*time.Second), core.ExecutionContextManifest{ExecutionID: "execution-1", ExecutionProfileVersion: "profile-v1", Provider: "provider", Model: "model"}),
		testEvent(t, "e4", "TOOL_OUTCOME_RECORDED", "task-1", "execution-1", started.Add(3*time.Second), core.ToolOutcome{Status: core.OutcomeSucceeded, RecoveryAttempted: true}),
		testEvent(t, "e5", "INFERENCE_USAGE_RECORDED", "task-1", "execution-1", started.Add(4*time.Second), events.InferenceUsageRecordedPayload{Source: "provider_response", Provider: "provider", Model: "model", InputTokens: 10, OutputTokens: 5, TotalTokens: 15, CostUSD: &cost}),
		testEvent(t, "e6", "MESSAGE", "task-1", "", started.Add(5*time.Second), map[string]string{"body": "update"}),
		testEvent(t, "e7", "TASK_BLOCKED", "task-1", "", started.Add(6*time.Second), map[string]string{"reason": "input"}),
		testEvent(t, "e8", "TASK_RECOVERED", "task-1", "", started.Add(7*time.Second), map[string]string{"reason": "restart"}),
		testEvent(t, "e9", "A2A_INPUT_RECEIVED", "task-1", "", started.Add(8*time.Second), map[string]string{"text": "answer"}),
		testEvent(t, "e9r", "COMPLETION_REVIEW_DECIDED", "task-1", "", started.Add(8500*time.Millisecond), map[string]string{"decision": "APPROVE"}),
		testEvent(t, "e10", "CAPABILITY_DENIED", "task-1", "", started.Add(9*time.Second), map[string]string{"reason": "missing"}),
		testEvent(t, "e11", "RESULT_PUBLISHED", "task-1", "execution-1", started.Add(10*time.Second), events.ResultPublishedPayload{Summary: "done", ArtifactRefs: []string{"artifact-1"}}),
		testEvent(t, "e12", "COMPLETION_VERIFIED", "task-1", "", started.Add(11*time.Second), map[string]bool{"complete": true}),
		testEvent(t, "e13", "TASK_VERIFIED_COMPLETE", "task-1", "", started.Add(12*time.Second), completedProjection),
	}
	stream[11].ArtifactRefs = []string{"artifact-1"}

	run, err := Project("request-1", stream)
	if err != nil {
		t.Fatal(err)
	}
	if run.Outcome != "VERIFIED_COMPLETE" || run.WallTimeMilliseconds != 12_000 {
		t.Fatalf("terminal summary=%+v", run)
	}
	if len(run.ExecutionMechanisms) != 1 || run.ExecutionMechanisms[0] != (MechanismCount{Kind: core.ExecutionAgent, Count: 1}) {
		t.Fatalf("mechanisms=%+v", run.ExecutionMechanisms)
	}
	if len(run.ModelUses) != 1 || run.ModelUses[0].TotalTokens != 15 || run.ModelUses[0].CostUSD == nil || *run.ModelUses[0].CostUSD != cost || !run.CostComplete || run.TotalCostUSD != cost {
		t.Fatalf("model usage=%+v run=%+v", run.ModelUses, run)
	}
	if run.ToolCalls != 1 || run.Messages != 1 || run.Blocks != 1 || run.Retries != 2 || run.HumanInterventions != 2 || run.SafetyDenials != 1 {
		t.Fatalf("operational counters=%+v", run)
	}
	if len(run.CompletionEvidenceEventRefs) != 4 || len(run.CompletionEvidenceArtifactRefs) != 1 || run.CompletionEvidenceArtifactRefs[0] != "artifact-1" {
		t.Fatalf("completion evidence=%+v", run)
	}
}

func TestProjectRejectsCrossOrganizationAndIncompleteStreams(t *testing.T) {
	now := time.Now().UTC()
	first := testEvent(t, "e1", "MESSAGE", "task-1", "", now, map[string]string{"body": "one"})
	second := testEvent(t, "e2", "TASK_VERIFIED_COMPLETE", "task-1", "", now.Add(time.Second), map[string]string{"status": "COMPLETED"})
	second.OrganizationID = "other-org"
	if _, err := Project("request-1", []events.Event{first, second}); err == nil {
		t.Fatal("cross-organization stream was summarized")
	}
	if _, err := Project("request-1", []events.Event{first}); err == nil {
		t.Fatal("incomplete stream was summarized")
	}
}

func TestRecordedRejectsDuplicateOrMalformedContracts(t *testing.T) {
	run := Run{SchemaVersion: SchemaVersion, CorrelationID: "request-1", OrganizationID: "org-1", Outcome: "VERIFIED_COMPLETE"}
	event := testEvent(t, "e1", "RUN_TELEMETRY_RECORDED", "", "", time.Now().UTC(), run)
	recorded, found, err := Recorded([]events.Event{event})
	if err != nil || !found || recorded.CorrelationID != run.CorrelationID {
		t.Fatalf("recorded=%+v found=%v err=%v", recorded, found, err)
	}
	if _, _, err := Recorded([]events.Event{event, event}); err == nil {
		t.Fatal("duplicate run telemetry was accepted")
	}
	event.Payload = json.RawMessage(`{"schema_version":1}`)
	if _, _, err := Recorded([]events.Event{event}); err == nil {
		t.Fatal("malformed run telemetry was accepted")
	}
}

func testEvent(t *testing.T, id, eventType, taskID, executionID string, at time.Time, payload any) events.Event {
	t.Helper()
	return events.Event{EventID: id, OrganizationID: "org-1", EventType: eventType, TaskID: taskID, SourceExecutionID: executionID, CreatedAt: at, Payload: jsonBody(t, payload), CorrelationID: "request-1"}
}

func jsonBody(t *testing.T, value any) json.RawMessage {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
