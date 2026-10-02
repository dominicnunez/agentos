package ledger

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDetailOwners(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{
		"submission_event_ref",
		"judgment_ref",
		"input_event_ref",
		"dispatch_binding.agent_event_ref",
		"dispatch_binding.blueprint_event_ref",
		"dispatch_binding.execution_profile_event_ref",
	} {
		t.Run(field, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			planScopeParents(t, store, "selected-org", "selected", "selected-work")
			kind := "TASK_VERIFIED_COMPLETE"
			if field == "input_event_ref" {
				appendDetailHumanStart(t, store)
				kind = "EXECUTION_STARTED"
			} else if strings.HasPrefix(field, "dispatch_binding.") {
				incidentTestExecution(t, store)
				kind = "EXECUTION_STARTED"
			} else {
				completedHumanPlanScope(t, store, "org-1")
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
			if err != nil {
				t.Fatalf("valid owner projection history: %v", err)
			}
			if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
				t.Fatalf("valid owner completion history: %v", err)
			}
			var selected, incoming events.Event
			for _, event := range stream {
				if event.OrganizationID == "selected-org" && event.EventType == "WORK_CREATED" {
					selected = event
				}
				if event.OrganizationID == "org-1" && event.EventType == kind {
					incoming = event
				}
			}
			if selected.EventID == "" || incoming.EventID == "" {
				t.Fatal("owner did not admit required fixture events")
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", incoming.CorrelationID, 256); err != nil {
				t.Fatalf("valid owner incident: %v", err)
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", selected.CorrelationID, 256)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range baseline.DependencyEvents {
				if event.EventID == incoming.EventID {
					t.Fatal("unrelated admission selected before reference changed")
				}
			}
			// Structured Human completion forbids judgment_ref; selecting forbidden
			// metadata is necessary for its owner to reject it too.
			var target any = selected.EventID
			if member, nested := strings.CutPrefix(field, "dispatch_binding."); nested {
				payload, _, err := events.AdmittedProjection(incoming)
				if err != nil {
					t.Fatal(err)
				}
				var detail struct {
					Binding map[string]any `json:"dispatch_binding"`
				}
				if err := json.Unmarshal(payload.Detail, &detail); err != nil || detail.Binding == nil {
					t.Fatalf("missing owner dispatch binding: %v", err)
				}
				if ref, ok := detail.Binding[member].(string); !ok || ref == "" {
					t.Fatal("owner did not supply the tested dispatch reference")
				}
				detail.Binding[member] = selected.EventID
				field, target = "dispatch_binding", detail.Binding
			}
			ChangeIncidentDetailForTest(t, store, incoming.EventID, field, target)
			stream, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
				t.Fatal("full owner validation accepted mismatched transition detail")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", selected.CorrelationID, 256)
			if err == nil {
				t.Fatal("incident omitted incoming owner transition detail")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed incident returned partial evidence")
			}
		})
	}
}

func appendDetailHumanStart(t *testing.T, store *SQLite) {
	t.Helper()
	const correlation = "human-input"
	intent, work := planScopeParents(t, store, "org-1", correlation, "human-input-work")
	task := core.Task{ID: "task-human-input", WorkID: work.ID, Description: "provide input", ExecutionKind: core.ExecutionHuman, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
		t.Fatal(err)
	}
	plan := core.Plan{ID: "plan-human-input", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: time.Now().UTC()}
	var err error
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation, Payload: plan}); err != nil {
		t.Fatal(err)
	}
	input, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "HUMAN_INPUT_RECEIVED", SourceActorID: "user-1", TaskID: string(task.ID), CorrelationID: correlation, Payload: events.OperatorInputReceivedPayload{MessageID: "input-1", Text: "provided input", SourcePrincipalID: "user-1", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT"}})
	if err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	if _, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation, Payload: events.ExecutionStartDetail{Mode: "OPERATOR_HUMAN_INPUT", InputEventRef: input.EventID}}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, nil, nil); err != nil {
		t.Fatal(err)
	}
}
