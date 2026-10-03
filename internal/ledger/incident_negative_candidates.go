package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type incidentNegativeWindow struct {
	After        int64  `json:"after"`
	Before       int64  `json:"before"`
	Organization string `json:"organization"`
}

// Selected dispatch and strategy owners retain malformed temporal sources even
// when those sources name no selected identity. A consumed Agent manifest also
// requires the same-organization Knowledge history before its start (and, for
// nonempty v5 references, before use). This transient proof does not expand the
// snapshot or replay unrelated projection, inference or authority domains.
func validateIncidentNegativeEvents(ctx context.Context, tx *sql.Tx, stream []events.Event, budget *incidentBudget) error {
	windows, knowledgeBefore, organization, err := incidentNegativeWindows(stream)
	if err != nil {
		return err
	}
	if len(windows) == 0 {
		return nil
	}
	known := make(map[string]bool, len(stream)+len(budget.checkedEvents))
	for _, event := range stream {
		known[event.EventID] = true
	}
	for id := range budget.checkedEvents {
		known[id] = true
	}
	ids := make([]string, 0, len(known))
	for id := range known {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	windowJSON, err := json.Marshal(windows)
	if err != nil {
		return err
	}
	knownJSON, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	after, before := windows[0].After, windows[0].Before
	for _, window := range windows {
		after = min(after, window.After)
		before = max(before, window.Before)
	}
	where := `sequence>? AND sequence<? AND event_id NOT IN (SELECT value FROM json_each(?))
		AND EXISTS(SELECT 1 FROM json_each(?) window WHERE sequence>json_extract(window.value,'$.after') AND sequence<json_extract(window.value,'$.before')
		AND (json_extract(window.value,'$.organization')='' OR organization_id=json_extract(window.value,'$.organization')))
		AND (` + incidentNegativeJSON + `)`
	// incidentEvents preflights the full event metadata and payload aggregate,
	// then runs exact AdmittedProjection before crediting checkedEvents. Only
	// newly read evidence is charged; known policies/activations and graph rows
	// retain their existing debit. Healthy candidates remain transient support.
	candidates, err := incidentEvents(ctx, tx, budget, where, after, before, string(knownJSON), string(windowJSON))
	if err != nil {
		return fmt.Errorf("incident temporal negative evidence: %w", err)
	}
	if knowledgeBefore == 0 {
		return nil
	}
	knowledge := make([]events.Event, 0)
	for _, source := range [][]events.Event{stream, candidates} {
		for _, event := range source {
			if event.OrganizationID != organization || event.Sequence >= knowledgeBefore {
				continue
			}
			projection, present, err := events.AdmittedProjection(event)
			if err != nil {
				return err
			}
			if present && projection.Projection.ProjectionKind == "knowledge" {
				knowledge = append(knowledge, event)
			}
		}
	}
	sort.Slice(knowledge, func(i, j int) bool { return knowledge[i].Sequence < knowledge[j].Sequence })
	replay := events.NewExecutionKnowledgeReplay(organization)
	for _, event := range knowledge {
		if err := replay.Observe(event); err != nil {
			return fmt.Errorf("incident consumed Knowledge history: %w", err)
		}
	}
	return nil
}

func incidentNegativeWindows(stream []events.Event) ([]incidentNegativeWindow, int64, string, error) {
	byID := make(map[string]events.Event, len(stream))
	starts := map[[3]string]events.Event{}
	workGoals := map[string][]int64{}
	manifests := map[[3]string]core.ExecutionContextManifest{}
	for _, event := range stream {
		byID[event.EventID] = event
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
			var manifest core.ExecutionContextManifest
			if err := json.Unmarshal(event.Payload, &manifest); err != nil {
				return nil, 0, "", err
			}
			manifests[[3]string{event.CorrelationID, event.TaskID, event.SourceExecutionID}] = manifest
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, 0, "", err
		}
		if !present {
			continue
		}
		if projection.Projection.ProjectionKind == "work" {
			var work core.Work
			if err := json.Unmarshal(projection.Projection.Value, &work); err != nil {
				return nil, 0, "", err
			}
			if work.GoalID != "" {
				workGoals[string(work.ID)] = append(workGoals[string(work.ID)], event.Sequence)
			}
		}
		if event.EventType == "EXECUTION_STARTED" {
			execution, err := events.ContainmentExecutionID(event)
			if err != nil {
				return nil, 0, "", err
			}
			starts[[3]string{event.CorrelationID, event.TaskID, execution}] = event
		}
	}
	windows := []incidentNegativeWindow{}
	for _, start := range starts {
		projection, _, err := events.AdmittedProjection(start)
		if err != nil {
			return nil, 0, "", err
		}
		var task core.Task
		var detail events.ExecutionStartDetail
		if err := json.Unmarshal(projection.Projection.Value, &task); err != nil {
			return nil, 0, "", err
		}
		if err := json.Unmarshal(projection.Detail, &detail); err != nil {
			return nil, 0, "", err
		}
		for _, sequence := range workGoals[string(task.WorkID)] {
			if sequence < start.Sequence {
				windows = append(windows, incidentNegativeWindow{Before: start.Sequence})
				break
			}
		}
		if task.ExecutionKind != core.ExecutionAgent {
			continue
		}
		if detail.DispatchBinding == nil {
			return nil, 0, "", fmt.Errorf("incident Agent start lacks dispatch binding")
		}
		lower := start.Sequence
		for _, id := range []string{detail.DispatchBinding.AgentEventRef, detail.DispatchBinding.BlueprintEventRef, detail.DispatchBinding.ExecutionProfileEventRef} {
			ref, found := byID[id]
			if !found {
				return nil, 0, "", fmt.Errorf("incident dispatch lacks pinned roster evidence")
			}
			lower = min(lower, ref.Sequence)
		}
		windows = append(windows, incidentNegativeWindow{After: lower, Before: start.Sequence})
	}
	var knowledgeBefore int64
	organization := ""
	consume := func(key [3]string, use int64) {
		manifest, found := manifests[key]
		start, started := starts[key]
		if !found || !started {
			return
		}
		switch manifest.ContextBuilderVersion {
		case "v2", "v3", "v4", "v5":
		default:
			return
		}
		projection, _, err := events.AdmittedProjection(start)
		if err != nil {
			return
		}
		var task core.Task
		if json.Unmarshal(projection.Projection.Value, &task) != nil || task.ExecutionKind != core.ExecutionAgent {
			return
		}
		boundary := start.Sequence
		if manifest.ContextBuilderVersion == "v5" && len(manifest.KnowledgeRefs) != 0 {
			boundary = max(boundary, use)
		}
		knowledgeBefore = max(knowledgeBefore, boundary)
		organization = start.OrganizationID
	}
	for _, event := range stream {
		if event.EventType == "INFERENCE_RESERVED" {
			consume([3]string{event.CorrelationID, event.TaskID, event.SourceExecutionID}, event.Sequence)
		}
		if event.EventType != "TASK_VERIFIED_COMPLETE" {
			continue
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, 0, "", err
		}
		if !present {
			continue
		}
		var task core.Task
		var decision events.CompletionDecisionPayload
		if err := json.Unmarshal(projection.Projection.Value, &task); err != nil {
			return nil, 0, "", err
		}
		if task.ExecutionKind != core.ExecutionAgent {
			continue
		}
		if err := json.Unmarshal(projection.Detail, &decision); err != nil {
			return nil, 0, "", err
		}
		outcome, found := byID[decision.OutcomeEventRef]
		if found {
			consume([3]string{event.CorrelationID, string(task.ID), outcome.SourceExecutionID}, event.Sequence)
		}
	}
	// Completion owners reuse the exact Task decision at progressively later
	// boundaries: aggregate Work evidence, terminal Work admission, and the
	// achieved Goal evaluation that consumes that completed Work. Standalone
	// evaluations carry no such authority; follow only selected terminal claims.
	consumeWork := func(evidenceID string, use int64) error {
		evidenceEvent, found := byID[evidenceID]
		if !found || evidenceEvent.EventType != "WORK_COMPLETION_EVALUATED" {
			return nil // The exact aggregate owner reports missing/wrong evidence.
		}
		var evidence events.WorkCompletionEvidencePayload
		if err := json.Unmarshal(evidenceEvent.Payload, &evidence); err != nil {
			return err
		}
		for _, claim := range evidence.Tasks {
			verification, found := byID[claim.VerificationEventRef]
			if !found || verification.EventType != "COMPLETION_VERIFIED" {
				continue
			}
			var decision events.CompletionDecisionPayload
			if err := json.Unmarshal(verification.Payload, &decision); err != nil {
				return err
			}
			outcome, found := byID[decision.OutcomeEventRef]
			if found {
				consume([3]string{evidenceEvent.CorrelationID, string(claim.TaskID), outcome.SourceExecutionID}, max(evidenceEvent.Sequence, use))
			}
		}
		return nil
	}
	completedWorkEvidence := map[string]bool{}
	for _, event := range stream {
		if event.EventType != "WORK_COMPLETED" {
			continue
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, 0, "", err
		}
		if !present || projection.Projection.ProjectionKind != "work" {
			continue
		}
		var detail events.WorkCompletionTransitionPayload
		if err := json.Unmarshal(projection.Detail, &detail); err != nil {
			return nil, 0, "", err
		}
		completedWorkEvidence[detail.EvidenceEventRef] = true
		if err := consumeWork(detail.EvidenceEventRef, event.Sequence); err != nil {
			return nil, 0, "", err
		}
	}
	for _, event := range stream {
		if event.EventType != "GOAL_ACHIEVED" {
			continue
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			return nil, 0, "", err
		}
		if !present || projection.Projection.ProjectionKind != "goal" {
			continue
		}
		var detail events.GoalAchievementTransitionPayload
		if err := json.Unmarshal(projection.Detail, &detail); err != nil {
			return nil, 0, "", err
		}
		evaluationEvent, found := byID[detail.EvidenceEventRef]
		if !found || evaluationEvent.EventType != "GOAL_PROGRESS_EVALUATED" {
			continue
		}
		var evaluation events.GoalProgressEvaluatedPayload
		if err := json.Unmarshal(evaluationEvent.Payload, &evaluation); err != nil {
			return nil, 0, "", err
		}
		for _, ref := range evaluation.WorkEvidenceRefs {
			if completedWorkEvidence[ref] {
				if err := consumeWork(ref, evaluationEvent.Sequence); err != nil {
					return nil, 0, "", err
				}
			}
		}
	}
	if knowledgeBefore != 0 {
		windows = append(windows, incidentNegativeWindow{Before: knowledgeBefore, Organization: organization})
	}
	return normalizeNegativeWindows(windows), knowledgeBefore, organization, nil
}

// Merge only strictly overlapping intervals: both bounds are exclusive, so
// touching intervals must retain the excluded sequence at their common edge.
// Global coverage can remove a fully subsumed organization-specific interval.
func normalizeNegativeWindows(windows []incidentNegativeWindow) []incidentNegativeWindow {
	sort.Slice(windows, func(i, j int) bool {
		if windows[i].Organization != windows[j].Organization {
			return windows[i].Organization < windows[j].Organization
		}
		if windows[i].After != windows[j].After {
			return windows[i].After < windows[j].After
		}
		return windows[i].Before < windows[j].Before
	})
	merged := make([]incidentNegativeWindow, 0, len(windows))
	for _, window := range windows {
		if window.After >= window.Before {
			continue
		}
		if len(merged) != 0 {
			previous := &merged[len(merged)-1]
			if previous.Organization == window.Organization && window.After < previous.Before {
				previous.Before = max(previous.Before, window.Before)
				continue
			}
		}
		merged = append(merged, window)
	}
	result := make([]incidentNegativeWindow, 0, len(merged))
	for _, window := range merged {
		subsumed := false
		if window.Organization != "" {
			for _, global := range merged {
				if global.Organization == "" && global.After <= window.After && global.Before >= window.Before {
					subsumed = true
					break
				}
			}
		}
		if !subsumed {
			result = append(result, window)
		}
	}
	return result
}

// An ordinary printable-ASCII object without Unicode escapes has the same
// decoded keys in SQLite and Go. Prove its size, duplicate-key and depth limits
// before excluding it. All reserved contracts and ambiguous/Unicode sources go
// to the shared exact owner; SQL is candidate discovery, not seal validation.
var incidentNegativeJSON = fmt.Sprintf(`CASE
	WHEN length(CAST(payload AS BLOB))>%d OR instr(CAST(payload AS BLOB),X'00')>0 OR NOT json_valid(payload) THEN 1
	WHEN json_type(payload)<>'object' THEN 1
	WHEN CAST(payload AS TEXT) GLOB '*[^ -~]*' OR instr(CAST(payload AS TEXT),'\u')>0 THEN 1
	ELSE EXISTS(WITH RECURSIVE
		nodes AS MATERIALIZED (SELECT id,parent,key,type FROM json_tree(payload)),
		depths(id,depth) AS (SELECT id,0 FROM nodes WHERE parent IS NULL UNION ALL SELECT n.id,d.depth+1 FROM nodes n JOIN depths d ON n.parent=d.id WHERE d.depth<=%d)
		SELECT 1 FROM nodes WHERE parent=0 AND lower(key) IN ('projection','admission')
		UNION ALL SELECT 1 FROM nodes n JOIN nodes p ON p.id=n.parent WHERE p.type='object' GROUP BY n.parent,n.key HAVING COUNT(*)>1
		UNION ALL SELECT 1 FROM depths WHERE depth>%d)
	END`, boundaryjson.MaximumBytes, boundaryjson.MaximumDepth, boundaryjson.MaximumDepth)
