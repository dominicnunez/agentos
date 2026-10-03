package events

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestCompletionHistoryPreservesResultCandidates(t *testing.T) {
	contract := core.CompletionContract{TaskID: "task", TaskVersion: 1, RequiredFields: []core.CompletionFieldRequirement{{Name: "response", MinBytes: 1, MaxBytes: 64}}}
	task := core.Task{ID: "task", WorkID: "work", Status: core.TaskCompleted, ExecutionKind: core.ExecutionHuman, CompletionContract: &contract}
	outcome := core.HumanTaskCompletionOutcome("submission", nil, time.Unix(2, 0).UTC())
	summary, err := core.ToolOutcomeSummary(outcome)
	if err != nil {
		t.Fatal(err)
	}
	decision := CompletionDecisionPayload{Contract: contract, Result: core.EvaluateHumanTaskCompletion(contract, core.HumanTaskSubmission{MessageID: "message", Fields: map[string]string{"response": "done"}}), OutcomeEventRef: "outcome", SubmissionEventRef: "submission"}
	event := func(id, kind string, sequence int64, payload any) Event {
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return Event{EventID: id, EventType: kind, Sequence: sequence, OrganizationID: "org", CorrelationID: "correlation", TaskID: string(task.ID), SourceActorID: "runtime", SourceExecutionID: "human-completion-submission", SchemaVersion: SchemaVersion, CreatedAt: time.Unix(sequence, 0).UTC(), Payload: body}
	}
	stream := []Event{
		event("outcome", "TOOL_OUTCOME_RECORDED", 10, outcome),
		event("result", "RESULT_PUBLISHED", 20, ResultPublishedPayload{Summary: summary}),
		event("candidate", "CANDIDATE_COMPLETE", 30, CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: "result"}),
		event("verified", "COMPLETION_VERIFIED", 40, decision),
		event("completed", "TASK_VERIFIED_COMPLETE", 50, nil),
	}
	stream[4].SourceExecutionID = ""
	value, _ := json.Marshal(task)
	detail, _ := json.Marshal(decision)
	sealed, err := SealProjectionEvent(stream[4], ProjectionRecord{ProjectionKind: "task", RecordID: string(task.ID), Version: 2, CorrelationID: "correlation", Value: value}, detail)
	if err != nil {
		t.Fatal(err)
	}
	stream[4].Payload, _ = json.Marshal(sealed)
	submission := event("submission", "HUMAN_TASK_COMPLETION_SUBMITTED", 1, HumanTaskCompletionSubmittedPayload{MessageID: "message", SourcePrincipalID: "user", SourceChannel: "HUMAN_DIRECT", Fields: map[string]string{"response": "done"}})
	submission.SourceActorID, submission.SourceExecutionID = "user", ""
	stream = append(stream, submission)
	binding := WorkCompletionBinding{OrganizationID: "org", CorrelationID: "correlation", Work: core.Work{ID: "work", IntentID: "intent", Status: core.WorkActive, Objective: "objective"}, Intent: core.Intent{ID: "intent", OrganizationID: "org", NormalizedObjective: "objective"}}
	for name, mutate := range map[string]func([]Event) []Event{
		"valid": func(s []Event) []Event { return s },
		"foreign result candidate": func(s []Event) []Event {
			e := s[1]
			e.EventID = "foreign"
			e.OrganizationID = "foreign"
			return append(s, e)
		},
		"duplicate result": func(s []Event) []Event { e := s[1]; e.EventID = "duplicate"; return append(s, e) },
		"foreign verification": func(s []Event) []Event {
			e := s[3]
			e.EventID = "foreign"
			e.OrganizationID = "foreign"
			return append(s, e)
		},
		"first ID wins": func(s []Event) []Event { e := s[0]; e.OrganizationID = "foreign"; return append([]Event{e}, s...) },
	} {
		t.Run(name, func(t *testing.T) {
			selected := mutate(append([]Event(nil), stream...))
			validator := NewCompletionEvidenceValidator(selected)
			_, admissionErr := validator.ValidateTask(binding, WorkCompletionTaskBinding{Task: task, Version: 2, CorrelationID: "correlation"}, stream[4])
			if (admissionErr == nil) != (name == "valid") {
				t.Fatalf("completion admission: %v", admissionErr)
			}
			history := validator.history
			got, _, err := resolveVerifiedTaskResult("org", "correlation", task, 2, selected, 60, history)
			if name == "valid" {
				if err != nil || got.EventID != "result" {
					t.Fatalf("valid result: %s, %v", got.EventID, err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid candidate was removed by the evidence index")
			}
		})
	}
}

// The manifest fixtures exercise historical context versions and exact Knowledge
// use boundaries. Compare both readers at that boundary without claiming these
// partial histories prove the surrounding Task completion or dispatch chain.
func completionModelParity(t *testing.T, binding WorkCompletionBinding, task core.Task, executionID string, start, outcome Event, stream []Event) (executionModel, error) {
	t.Helper()
	scan, scanErr := completionExecutionModel(binding, task, executionID, start, outcome, stream)
	validator := NewCompletionEvidenceValidator(stream)
	indexedBinding := binding
	indexedBinding.completionHistory, indexedBinding.executionHistory = validator.history, validator.startHistory()
	indexed, indexedErr := completionExecutionModel(indexedBinding, task, executionID, start, outcome, stream)
	if (scanErr == nil) != (indexedErr == nil) || scanErr == nil && !reflect.DeepEqual(scan, indexed) {
		t.Fatalf("manifest index changed replay: scan=%+v (%v), indexed=%+v (%v)", scan, scanErr, indexed, indexedErr)
	}
	if scanErr == nil {
		for _, mutation := range []string{"foreign manifest", "late manifest"} {
			changed := append([]Event(nil), stream...)
			var candidate Event
			for _, event := range stream {
				if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" && event.TaskID == string(task.ID) && event.SourceExecutionID == executionID && event.CorrelationID == binding.CorrelationID {
					candidate = event
					break
				}
			}
			if candidate.EventID == "" {
				t.Fatal("valid manifest fixture lacks its candidate")
			}
			candidate.EventID = mutation
			if mutation == "foreign manifest" {
				candidate.OrganizationID = "foreign"
			} else {
				candidate.Sequence = outcome.Sequence + 1
			}
			changed = append(changed, candidate)
			_, fullErr := completionExecutionModel(binding, task, executionID, start, outcome, changed)
			changedValidator := NewCompletionEvidenceValidator(changed)
			indexedBinding.completionHistory, indexedBinding.executionHistory = changedValidator.history, changedValidator.startHistory()
			_, candidateErr := completionExecutionModel(indexedBinding, task, executionID, start, outcome, changed)
			if fullErr == nil || candidateErr == nil {
				t.Fatalf("%s candidate was hidden: scan=%v indexed=%v", mutation, fullErr, candidateErr)
			}
		}
	}
	return scan, scanErr
}

func TestCompletionHistoryPreservesInboxBoundary(t *testing.T) {
	task := core.Task{ID: "task", ExecutionKind: core.ExecutionAgent, AssigneeType: "AGENT", AssigneeID: "agent"}
	detail, _ := json.Marshal(ExecutionStartDetail{InboxCutoffSequence: 5, DispatchBinding: &AgentDispatchBinding{}})
	body, _ := json.Marshal(ProjectionEventPayload{Detail: detail})
	start := Event{EventID: "start", Sequence: 10, OrganizationID: "org", EventType: "EXECUTION_STARTED", TaskID: "task", CorrelationID: "work", Payload: body}
	stream := []Event{
		{EventID: "cross-correlation", Sequence: 3, OrganizationID: "org", CorrelationID: "other-work", RecipientScope: RecipientAgent, RecipientID: "agent"},
		{EventID: "task-route", Sequence: 4, OrganizationID: "org", RecipientScope: RecipientTask, RecipientID: "task"},
		{EventID: "after-cutoff", Sequence: 6, OrganizationID: "org", RecipientScope: RecipientAgent, RecipientID: "agent"},
		{EventID: "foreign", Sequence: 2, OrganizationID: "foreign", RecipientScope: RecipientAgent, RecipientID: "agent"},
		start,
	}
	binding := WorkCompletionBinding{OrganizationID: "org", CorrelationID: "work", completionHistory: newCompletionHistory(stream)}
	refs, _, err := executionInbox(binding, task, start, stream)
	if err != nil || !slices.Equal(refs, []string{"cross-correlation", "task-route"}) {
		t.Fatalf("inbox boundary: %v, %v", refs, err)
	}
	for _, invalid := range []Event{{EventID: "", OrganizationID: "foreign"}, {EventID: "foreign", OrganizationID: "unrelated"}} {
		changed := append(append([]Event(nil), stream...), invalid)
		binding.completionHistory = newCompletionHistory(changed)
		if _, _, err := executionInbox(binding, task, start, changed); err == nil {
			t.Fatal("global inbox identity violation was hidden")
		}
	}
}

func TestCompletionHistoryRetainsMalformedProjectionCandidates(t *testing.T) {
	malformed := Event{EventID: "malformed", EventType: "TASK_CREATED", OrganizationID: "org", CorrelationID: "work", Sequence: 1, Payload: json.RawMessage(`{`)}
	later := Event{EventID: "later", EventType: "MESSAGE", OrganizationID: "org", CorrelationID: "work", Sequence: 2, Payload: json.RawMessage(`{}`)}
	stream := []Event{malformed, later}
	history := newCompletionHistory(stream)
	if _, err := replayExecutionKnowledge("org", 3, history.knowledgeEvents(stream, "org")); err == nil {
		t.Fatal("earlier malformed projection disappeared from Knowledge validation")
	}
	if _, err := ResolveExecutionCoordination("org", "work", "work", "task", 3, history.coordinationEvents(stream, "org", "work")); err == nil {
		t.Fatal("earlier malformed projection disappeared from coordination validation")
	}
	if _, err := replayExecutionKnowledge("foreign", 3, history.knowledgeEvents(stream, "foreign")); err != nil {
		t.Fatalf("other organization poisoned Knowledge: %v", err)
	}
}
