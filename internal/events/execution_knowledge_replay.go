package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
)

// ExecutionKnowledgeReplay validates Knowledge transitions once in ledger order.
// Its token index contains only currently eligible factual candidates; complete
// current records remain available for exact transitive lineage validation.
type ExecutionKnowledgeReplay struct {
	organizationID string
	sequence       int64
	history        map[core.ID]executionKnowledgeRevision
	candidates     map[[3]string]map[core.ID]struct{}
	starts         map[string]executionKnowledgeStart
}

type executionKnowledgeStart struct {
	sequence int64
	task     []byte
	selected []KnowledgeSelection
}

func NewExecutionKnowledgeReplay(organizationID string) *ExecutionKnowledgeReplay {
	return &ExecutionKnowledgeReplay{organizationID: organizationID, history: make(map[core.ID]executionKnowledgeRevision), candidates: make(map[[3]string]map[core.ID]struct{}), starts: make(map[string]executionKnowledgeStart)}
}

// CaptureStart freezes the independently selected factual context before later
// Knowledge changes. Consumers must still validate the Task's dispatch contract.
func (r *ExecutionKnowledgeReplay) CaptureStart(start Event, task core.Task, teams map[core.ID][]TeamRevisionBinding) error {
	if start.EventID == "" || start.OrganizationID != r.organizationID || start.EventType != "EXECUTION_STARTED" || start.TaskID != string(task.ID) {
		return fmt.Errorf("knowledge start identity is invalid")
	}
	if _, found := r.starts[start.EventID]; found {
		return fmt.Errorf("knowledge start was captured twice")
	}
	selected, err := r.Select(task, start.Sequence, teams)
	if err != nil {
		return err
	}
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}
	r.starts[start.EventID] = executionKnowledgeStart{sequence: start.Sequence, task: body, selected: selected}
	return nil
}

func (r *ExecutionKnowledgeReplay) selectionAtStart(organizationID string, start Event, task core.Task) ([]KnowledgeSelection, error) {
	captured, found := r.starts[start.EventID]
	body, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}
	if !found || organizationID != r.organizationID || start.OrganizationID != r.organizationID || captured.sequence != start.Sequence || !bytes.Equal(captured.task, body) {
		return nil, fmt.Errorf("knowledge selection lacks its exact captured start")
	}
	return captured.selected, nil
}

// ValidateManifest uses frozen start selection and the current validated lineage
// while preserving the shared manifest/input reconstruction contract.
func (r *ExecutionKnowledgeReplay) ValidateManifest(binding WorkCompletionBinding, task core.Task, executionID string, start Event, before int64, stream []Event) (core.ExecutionContextManifest, error) {
	if r.organizationID == "" || binding.OrganizationID != r.organizationID {
		return core.ExecutionContextManifest{}, fmt.Errorf("knowledge replay organization does not match execution")
	}
	binding.knowledgeReplay = r
	return ValidateAgentExecutionManifest(binding, task, executionID, start, before, stream)
}

func (r *ExecutionKnowledgeReplay) Observe(event Event) error {
	if r.organizationID == "" || event.OrganizationID != r.organizationID || event.Sequence <= r.sequence {
		return fmt.Errorf("knowledge replay requires ordered events from one organization")
	}
	payload, present, err := AdmittedProjection(event)
	if err != nil {
		return err
	}
	if !present || payload.Projection.ProjectionKind != "knowledge" {
		r.sequence = event.Sequence
		return nil
	}
	var record core.KnowledgeRecord
	if decodeExactEventJSON(payload.Projection.Value, &record) != nil || record.KnowledgeID != core.ID(payload.Projection.RecordID) || record.Version != payload.Projection.Version || string(record.OrganizationID) != r.organizationID || core.ValidateKnowledgeProjectionTarget(event.EventType, payload.Projection.Version, record) != nil {
		return fmt.Errorf("execution knowledge revision is invalid")
	}
	previous, found := r.history[record.KnowledgeID]
	if !found && record.Version != 1 || found && core.ValidateKnowledgeTransition(event.EventType, previous.record, record) != nil {
		return fmt.Errorf("execution knowledge history is noncontiguous")
	}
	if found {
		r.indexRecord(previous.record, false)
	}
	r.history[record.KnowledgeID] = executionKnowledgeRevision{record: record, sequence: event.Sequence}
	r.indexRecord(record, true)
	r.sequence = event.Sequence
	return nil
}

func (r *ExecutionKnowledgeReplay) indexRecord(record core.KnowledgeRecord, add bool) {
	if !core.KnowledgeEligibleForModelContext(record) {
		return
	}
	tokens := executionKnowledgeTokens(record.Title + " " + record.Content + " " + record.Applicability + " " + strings.Join(record.Tags, " "))
	for token := range tokens {
		key := [3]string{string(record.Scope), string(record.ScopeID), token}
		if add {
			if r.candidates[key] == nil {
				r.candidates[key] = make(map[core.ID]struct{})
			}
			r.candidates[key][record.KnowledgeID] = struct{}{}
		} else {
			delete(r.candidates[key], record.KnowledgeID)
			if len(r.candidates[key]) == 0 {
				delete(r.candidates, key)
			}
		}
	}
}

// Select must run before observing the start event. Selection uses the same
// relevance, ordering, byte limits, classification and lineage checks as replay.
func (r *ExecutionKnowledgeReplay) Select(task core.Task, before int64, teams map[core.ID][]TeamRevisionBinding) ([]KnowledgeSelection, error) {
	if r.organizationID == "" || before <= r.sequence || task.ExecutionKind != core.ExecutionAgent || task.AssigneeID == "" {
		return nil, fmt.Errorf("knowledge selection boundary is invalid")
	}
	scopes := [][2]string{{string(core.KnowledgeScopeOrganization), r.organizationID}, {string(core.KnowledgeScopeAgent), string(task.AssigneeID)}}
	for _, route := range AgentExecutionRoutes(teams, task, before) {
		if route.Scope == RecipientTeam {
			scopes = append(scopes, [2]string{string(core.KnowledgeScopeTeam), route.ID})
		}
	}
	history := make(map[core.ID]executionKnowledgeRevision)
	var include func(core.ID)
	include = func(id core.ID) {
		if _, found := history[id]; found {
			return
		}
		revision, found := r.history[id]
		if !found {
			return
		}
		history[id] = revision
		for _, ref := range revision.record.DerivedKnowledgeRefs {
			include(core.ID(ref.ID))
		}
	}
	for token := range executionKnowledgeTokens(task.Description + " " + task.ExecutionBrief) {
		for _, scope := range scopes {
			for id := range r.candidates[[3]string{scope[0], scope[1], token}] {
				include(id)
			}
		}
	}
	return selectExecutionKnowledge(r.organizationID, task, before, teams, history, true)
}

func (r *ExecutionKnowledgeReplay) ValidateUse(task core.Task, manifest core.ExecutionContextManifest, before int64, teams map[core.ID][]TeamRevisionBinding) error {
	if r.organizationID == "" || before <= r.sequence || task.ExecutionKind != core.ExecutionAgent || task.AssigneeID == "" || manifest.ContextBuilderVersion != "v5" {
		return fmt.Errorf("knowledge use boundary is invalid")
	}
	return validateExecutionKnowledgeHistoryAtUse(r.organizationID, task, manifest, before, teams, r.history)
}
