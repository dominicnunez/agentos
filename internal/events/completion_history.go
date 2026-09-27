package events

import (
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

// CompletionEvidenceValidator reuses evidence indexes for one immutable stream.
// Callers must retain the complete supporting history and must not mutate the
// stream or its payloads during validation. It does not certify ledger integrity
// or projection coverage and never substitutes final state for admission state.
// A validator belongs to one validation operation and must not be used
// concurrently: its execution-history cache and lazy indexes are operation-local.
type CompletionEvidenceValidator struct {
	history *completionHistory
	starts  *executionHistory
}

func NewCompletionEvidenceValidator(stream []Event) *CompletionEvidenceValidator {
	return &CompletionEvidenceValidator{history: newCompletionHistory(stream)}
}

func (v *CompletionEvidenceValidator) ValidateTask(binding WorkCompletionBinding, task WorkCompletionTaskBinding, completion Event) (CompletionDecisionPayload, error) {
	binding.completionHistory = v.history
	if binding.executionHistory == nil {
		binding.executionHistory = v.startHistory()
	}
	return validateTaskCompletionEvidenceChain(binding, task, completion, v.history.stream)
}

func (v *CompletionEvidenceValidator) startHistory() *executionHistory {
	if v.starts == nil {
		v.starts = newExecutionHistory(v.history.stream)
	}
	return v.starts
}

// Index keys reproduce candidate selection, not the subsequent trust checks.
// In particular task candidates retain foreign organizations, duplicate IDs and
// original input order; a reference lookup retains the scan API's first match.
type completionHistory struct {
	stream             []Event
	ids                map[string]int
	identityErr        error
	inboxIDs           map[string]Event
	tasks              map[[3]string][]Event
	correlations       map[[2]string][]Event
	recipients         map[[3]string][]Event
	knowledge          map[string][]Event
	coordination       map[[2]string][]Event
	projectionsIndexed bool
}

func newCompletionHistory(stream []Event) *completionHistory {
	h := &completionHistory{stream: stream, ids: make(map[string]int, len(stream)), tasks: map[[3]string][]Event{}, correlations: map[[2]string][]Event{}, recipients: map[[3]string][]Event{}}
	for position, event := range stream {
		if event.EventID == "" && h.identityErr == nil {
			h.identityErr = fmt.Errorf("execution inbox event identity is invalid")
		}
		if _, found := h.ids[event.EventID]; found {
			if h.identityErr == nil {
				h.identityErr = fmt.Errorf("execution inbox event identity is duplicated")
			}
		} else {
			h.ids[event.EventID] = position
		}
		switch event.EventType {
		case "TASK_VERIFIED_COMPLETE", "COMPLETION_VERIFIED", "RESULT_PUBLISHED", "CANDIDATE_COMPLETE", "EXECUTION_STARTED", "EXECUTION_CONTEXT_MANIFESTED", "COMPLETION_REVIEW_REQUESTED":
			taskKey := [3]string{event.EventType, event.TaskID, event.CorrelationID}
			h.tasks[taskKey] = append(h.tasks[taskKey], event)
		}
		if event.EventType == "COMPLETION_REVIEW_REQUESTED" || event.EventType == "COMPLETION_REVIEW_DECIDED" {
			correlationKey := [2]string{event.EventType, event.CorrelationID}
			h.correlations[correlationKey] = append(h.correlations[correlationKey], event)
		}
		if event.RecipientScope != "" {
			recipient := [3]string{event.OrganizationID, event.RecipientScope, event.RecipientID}
			h.recipients[recipient] = append(h.recipients[recipient], event)
		}
	}
	return h
}

// Agent context validation needs projection candidates. Deterministic and Human
// completions do not; do not decode their entire histories to build unused indexes.
func (h *completionHistory) indexProjections() {
	if h.projectionsIndexed {
		return
	}
	h.knowledge = make(map[string][]Event)
	h.coordination = make(map[[2]string][]Event)
	for _, event := range h.stream {
		payload, present, err := AdmittedProjection(event)
		if err != nil || present && payload.Projection.ProjectionKind == "knowledge" {
			h.knowledge[event.OrganizationID] = append(h.knowledge[event.OrganizationID], event)
		}
		if err != nil || present && payload.Projection.ProjectionKind == "task" {
			key := [2]string{event.OrganizationID, event.CorrelationID}
			h.coordination[key] = append(h.coordination[key], event)
		}
	}
	h.projectionsIndexed = true
}

func (h *completionHistory) taskEvents(stream []Event, kind, task, correlation string) []Event {
	if h == nil {
		return stream
	}
	return h.tasks[[3]string{kind, task, correlation}]
}

func (h *completionHistory) correlationEvents(stream []Event, kind, correlation string) []Event {
	if h == nil {
		return stream
	}
	return h.correlations[[2]string{kind, correlation}]
}

func (h *completionHistory) event(stream []Event, id string) (Event, bool) {
	if h == nil {
		return eventWithID(stream, id)
	}
	position, found := h.ids[id]
	if !found {
		return Event{}, false
	}
	return h.stream[position], true
}

func (h *completionHistory) knowledgeEvents(stream []Event, organization string) []Event {
	if h == nil {
		return stream
	}
	h.indexProjections()
	return h.knowledge[organization]
}

func (h *completionHistory) coordinationEvents(stream []Event, organization, correlation string) []Event {
	if h == nil {
		return stream
	}
	h.indexProjections()
	return h.coordination[[2]string{organization, correlation}]
}

func (h *completionHistory) inboxEvents(stream []Event, organization string, routes map[string]struct{}) []Event {
	if h == nil {
		return stream
	}
	var selected []Event
	for route := range routes {
		// Recipient keys are produced only by recipientKey from known routes.
		for _, scope := range []string{RecipientTask, RecipientAgent, RecipientTeam} {
			prefix := scope + "\x00"
			if len(route) >= len(prefix) && route[:len(prefix)] == prefix {
				selected = append(selected, h.recipients[[3]string{organization, scope, route[len(prefix):]}]...)
				break
			}
		}
	}
	sort.SliceStable(selected, func(i, j int) bool { return h.ids[selected[i].EventID] < h.ids[selected[j].EventID] })
	return selected
}

func (h *completionHistory) inboxIndex() map[string]Event {
	if h.inboxIDs == nil {
		h.inboxIDs = make(map[string]Event, len(h.ids))
		for id, position := range h.ids {
			h.inboxIDs[id] = h.stream[position]
		}
	}
	return h.inboxIDs
}

func (h *completionHistory) resolveResult(binding WorkCompletionBinding, task core.Task, version int, stream []Event, before int64) (Event, ResultPublishedPayload, error) {
	return resolveVerifiedTaskResult(binding.OrganizationID, binding.CorrelationID, task, version, stream, before, h)
}

func (h *completionHistory) referenceEvents(stream []Event, refs []string) []Event {
	if h == nil {
		return stream
	}
	selected := make([]Event, 0, len(refs))
	for _, ref := range refs {
		if position, found := h.ids[ref]; found {
			selected = append(selected, h.stream[position])
		}
	}
	return selected
}

func completionPlanStrategy(binding WorkCompletionBinding, stream []Event) (core.Plan, *core.StrategicContext, error) {
	if binding.executionHistory == nil {
		return ResolvePlanStrategicContext(binding.OrganizationID, binding.CorrelationID, binding.Work, binding.Intent, stream)
	}
	plan, event, err := binding.executionHistory.resolvePlan(binding.OrganizationID, binding.CorrelationID, binding.Work, binding.Intent)
	if err != nil {
		return core.Plan{}, nil, err
	}
	context, err := resolveStrategicContextByRefs(binding.OrganizationID, binding.Work, binding.completionHistory.referenceEvents(stream, plan.StrategicEventRefs), plan.StrategicEventRefs, plan.StrategicContextRefs, event.Sequence)
	if err != nil {
		return core.Plan{}, nil, err
	}
	return plan, context, nil
}
