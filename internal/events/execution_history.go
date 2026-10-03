package events

import (
	"sort"

	"github.com/dominicnunez/agentos/internal/core"
)

// executionHistory belongs to one immutable validation snapshot. It retains
// negative evidence (duplicates, malformed projections and superseding records)
// as well as the positive references used by the shared admission validators.
// It must not be shared between concurrent validations or after stream mutation.
type executionHistory struct {
	stream       []Event
	ids          map[string][]int
	correlations map[string][]Event
	projections  map[[2]string][]int
	strategies   map[[3]string][]int
	badStrategy  map[[3]string][]int
	malformed    []int
	plans        map[[2]string]resolvedStartPlan
}

type resolvedStartPlan struct {
	plan  core.Plan
	event Event
	err   error
}

func newExecutionHistory(stream []Event) *executionHistory {
	h := &executionHistory{stream: stream, ids: map[string][]int{}, correlations: map[string][]Event{}, projections: map[[2]string][]int{}, strategies: map[[3]string][]int{}, badStrategy: map[[3]string][]int{}, plans: map[[2]string]resolvedStartPlan{}}
	for i, event := range stream {
		h.ids[event.EventID] = append(h.ids[event.EventID], i)
		h.correlations[event.CorrelationID] = append(h.correlations[event.CorrelationID], event)
		payload, present, err := AdmittedProjection(event)
		if err != nil {
			h.malformed = append(h.malformed, i)
			continue
		}
		if !present {
			continue
		}
		record := payload.Projection
		key := [2]string{record.ProjectionKind, record.RecordID}
		h.projections[key] = append(h.projections[key], i)
		if record.ProjectionKind != "mission" && record.ProjectionKind != "goal" {
			continue
		}
		strategyKey := [3]string{event.OrganizationID, record.ProjectionKind, record.RecordID}
		h.strategies[strategyKey] = append(h.strategies[strategyKey], i)
		if record.ProjectionKind == "mission" {
			_, _, err = exactMissionProjection(event.OrganizationID, event)
		} else {
			_, _, err = exactGoalProjection(event.OrganizationID, event)
		}
		if err != nil {
			h.badStrategy[strategyKey] = append(h.badStrategy[strategyKey], i)
		}
	}
	order := func(indices []int) {
		sort.SliceStable(indices, func(i, j int) bool { return stream[indices[i]].Sequence < stream[indices[j]].Sequence })
	}
	order(h.malformed)
	for _, indices := range h.projections {
		order(indices)
	}
	for _, indices := range h.strategies {
		order(indices)
	}
	for _, indices := range h.badStrategy {
		order(indices)
	}
	return h
}

func (h *executionHistory) event(id string) (Event, bool) {
	indices := h.ids[id]
	if len(indices) == 0 {
		return Event{}, false
	}
	return h.stream[indices[0]], true
}

func (h *executionHistory) resolvePlan(organization, correlation string, work core.Work, intent core.Intent) (core.Plan, Event, error) {
	return bindResolvedPlan(organization, correlation, work, intent, func() (core.Plan, Event, error) {
		key := [2]string{organization, correlation}
		if result, found := h.plans[key]; found {
			return result.plan, result.event, result.err
		}
		// Keep the entire correlation, including accounting and stop evidence
		// that can disprove a claimed planning retry. Cross-correlation duplicate
		// event IDs must also remain visible to the retry proof reader.
		selected := map[int]bool{}
		for _, event := range h.correlations[correlation] {
			for _, i := range h.ids[event.EventID] {
				selected[i] = true
			}
		}
		plan, event, err := resolvePlanEvent(organization, correlation, h.selected(selected))
		h.plans[key] = resolvedStartPlan{plan: plan, event: event, err: err}
		return plan, event, err
	})
}

func (h *executionHistory) selected(indices map[int]bool) []Event {
	ordered := make([]int, 0, len(indices))
	for i := range indices {
		ordered = append(ordered, i)
	}
	sort.Ints(ordered)
	stream := make([]Event, 0, len(ordered))
	for _, i := range ordered {
		stream = append(stream, h.stream[i])
	}
	return stream
}

func (h *executionHistory) dispatchStream(start Event, binding *AgentDispatchBinding) []Event {
	selected := map[int]bool{}
	for _, ref := range []struct {
		event, kind string
		id          core.ID
	}{
		{binding.AgentEventRef, "agent", binding.AgentID},
		{binding.BlueprintEventRef, "agent_blueprint", binding.BlueprintID},
		{binding.ExecutionProfileEventRef, "execution_profile", binding.ExecutionProfileID},
	} {
		indices := h.ids[ref.event]
		for _, i := range indices {
			selected[i] = true
		}
		if len(indices) != 1 {
			continue
		}
		sequence := h.stream[indices[0]].Sequence
		// Any tenant's later revision with the same kind/id supersedes the
		// reference under the original dispatch contract. Do not filter by org.
		h.firstBetween(selected, h.projections[[2]string{ref.kind, string(ref.id)}], sequence, start.Sequence)
		h.firstBetween(selected, h.malformed, sequence, start.Sequence)
	}
	return h.selected(selected)
}

func (h *executionHistory) firstBetween(selected map[int]bool, indices []int, after, before int64) {
	i := sort.Search(len(indices), func(i int) bool { return h.stream[indices[i]].Sequence > after })
	if i < len(indices) && h.stream[indices[i]].Sequence < before {
		selected[indices[i]] = true
	}
}

func (h *executionHistory) strategyStream(organization string, work core.Work, before int64, refs []string) []Event {
	selected := map[int]bool{}
	for _, ref := range refs {
		if indices := h.ids[ref]; len(indices) != 0 {
			selected[indices[0]] = true
		}
	}
	if work.GoalID == "" {
		return h.selected(selected)
	}
	// Latest-strategy resolution rejects malformed projections of any kind
	// before its boundary, even if their identity cannot be decoded.
	if len(h.malformed) != 0 && (before <= 0 || h.stream[h.malformed[0]].Sequence < before) {
		selected[h.malformed[0]] = true
	}
	goal, found := h.latestStrategy(selected, [3]string{organization, "goal", string(work.GoalID)}, before)
	if found {
		_, value, err := exactGoalProjection(organization, goal)
		if err == nil {
			h.latestStrategy(selected, [3]string{organization, "mission", string(value.MissionID)}, before)
		}
	}
	return h.selected(selected)
}

func (h *executionHistory) latestStrategy(selected map[int]bool, key [3]string, before int64) (Event, bool) {
	indices := h.strategies[key]
	end := len(indices)
	if before > 0 {
		end = sort.Search(end, func(i int) bool { return h.stream[indices[i]].Sequence >= before })
	}
	if bad := h.badStrategy[key]; len(bad) != 0 && (before <= 0 || h.stream[bad[0]].Sequence < before) {
		selected[bad[0]] = true
	}
	if end == 0 {
		return Event{}, false
	}
	sequence := h.stream[indices[end-1]].Sequence
	if sequence <= 0 {
		return Event{}, false
	}
	// The scan API selects the first event when sequence numbers tie.
	i := sort.Search(end, func(i int) bool { return h.stream[indices[i]].Sequence >= sequence })
	selected[indices[i]] = true
	return h.stream[indices[i]], true
}
