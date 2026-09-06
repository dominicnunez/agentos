package events

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestResolveExecutionKnowledgeSelectsExactRelevantActiveScope(t *testing.T) {
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "prepare verified rollback procedure", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", Status: core.TaskRunning,
	}
	organization := activeExecutionKnowledge(t, 1, "knowledge-org", core.KnowledgeScopeOrganization, "org-1", "Rollback procedure", "The rollback rehearsal restored three records.")
	agent := activeExecutionKnowledge(t, 4, "knowledge-agent", core.KnowledgeScopeAgent, "agent-1", "Verified recovery", "Rollback recovery evidence is stored in archive 17.")
	wrongAgent := activeExecutionKnowledge(t, 7, "knowledge-other-agent", core.KnowledgeScopeAgent, "agent-2", "Rollback secret", "The private rollback archive contains two records.")
	irrelevant := activeExecutionKnowledge(t, 10, "knowledge-irrelevant", core.KnowledgeScopeOrganization, "org-1", "Marketing notes", "Audience demand rose by three percent.")
	stream := append(append(append(organization, agent...), wrongAgent...), irrelevant...)

	selected, err := ResolveExecutionKnowledge("org-1", task, 20, nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].Record.KnowledgeID != "knowledge-agent" || selected[1].Record.KnowledgeID != "knowledge-org" {
		t.Fatalf("unexpected deterministic knowledge selection: %+v", selected)
	}
	for _, selection := range selected {
		if selection.Record.Status != core.KnowledgeActive {
			t.Fatalf("non-active knowledge selected: %+v", selection.Record)
		}
	}
}

func TestIncrementalExecutionKnowledgeMatchesReplay(t *testing.T) {
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "verify rollback", ExecutionKind: core.ExecutionAgent, AssigneeID: "agent-1", Status: core.TaskRunning}
	stream := activeExecutionKnowledge(t, 1, "knowledge-org", core.KnowledgeScopeOrganization, "org-1", "Rollback evidence", "Rollback restored three records.")
	stream = append(stream, activeExecutionKnowledge(t, 4, "knowledge-other", core.KnowledgeScopeAgent, "other-agent", "Rollback evidence", "Rollback restored seven records.")...)
	stream = append(stream, activeExecutionKnowledge(t, 7, "knowledge-irrelevant", core.KnowledgeScopeOrganization, "org-1", "Revenue", "Sales increased.")...)
	replay := NewExecutionKnowledgeReplay("org-1")
	for _, event := range stream {
		if err := replay.Observe(event); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := replay.Select(task, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := ResolveExecutionKnowledge("org-1", task, 10, nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected, expected) || len(selected) != 1 {
		t.Fatalf("indexed selection differs: got %+v want %+v", selected, expected)
	}
	manifest := core.ExecutionContextManifest{ContextBuilderVersion: "v5", KnowledgeRefs: []core.VersionedRef{{ID: "knowledge-org", Version: "2", MaterializationState: core.MaterializedFull}}}
	if err := replay.ValidateUse(task, manifest, 10, nil); err != nil {
		t.Fatal(err)
	}
	stale := decodeKnowledgeProjection(t, stream[1])
	stale.Version, stale.Status, stale.SupersedesVersion = 3, core.KnowledgeStale, integerRef(2)
	invalidation := executionKnowledgeProjection(t, 11, "KNOWLEDGE_STALE", stale)
	if err := replay.Observe(invalidation); err != nil {
		t.Fatal(err)
	}
	stream = append(stream, invalidation)
	selected, err = replay.Select(task, 12, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected, err = ResolveExecutionKnowledge("org-1", task, 12, nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected, expected) || len(selected) != 0 {
		t.Fatal("stale candidate survived indexed selection")
	}
	if replay.ValidateUse(task, manifest, 12, nil) == nil || ValidateExecutionKnowledgeAtUse("org-1", task, manifest, 12, nil, stream) == nil {
		t.Fatal("stale manifested revision survived use validation")
	}
	if _, err := replay.Select(task, 10, nil); err == nil {
		t.Fatal("incremental state was reused for an earlier boundary")
	}
	if err := replay.Observe(invalidation); err == nil {
		t.Fatal("duplicate sequence accepted")
	}
}

func TestIncrementalKnowledgeKeepsStartSelectionSeparateFromUse(t *testing.T) {
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "verify rollback", ExecutionKind: core.ExecutionAgent, AssigneeID: "agent-1", Status: core.TaskRunning}
	replay := NewExecutionKnowledgeReplay("org-1")
	initial := activeExecutionKnowledge(t, 1, "knowledge-original", core.KnowledgeScopeOrganization, "org-1", "Rollback evidence", "Rollback restored three records.")
	for _, event := range initial {
		if err := replay.Observe(event); err != nil {
			t.Fatal(err)
		}
	}
	start := Event{EventID: "start-1", Sequence: 3, OrganizationID: "org-1", EventType: "EXECUTION_STARTED", TaskID: string(task.ID)}
	if err := replay.CaptureStart(start, task, nil); err != nil {
		t.Fatal(err)
	}
	for _, event := range activeExecutionKnowledge(t, 4, "knowledge-later", core.KnowledgeScopeOrganization, "org-1", "Rollback evidence", "Rollback restored nine records.") {
		if err := replay.Observe(event); err != nil {
			t.Fatal(err)
		}
	}
	frozen, err := replay.selectionAtStart("org-1", start, task)
	if err != nil || len(frozen) != 1 || frozen[0].Record.KnowledgeID != "knowledge-original" {
		t.Fatalf("start selection changed: %+v, %v", frozen, err)
	}
	current, err := replay.Select(task, 6, nil)
	if err != nil || len(current) != 2 || current[0].Record.KnowledgeID != "knowledge-later" {
		t.Fatalf("new start selection did not include later evidence: %+v, %v", current, err)
	}
	manifest := core.ExecutionContextManifest{ContextBuilderVersion: "v5", KnowledgeRefs: []core.VersionedRef{{ID: "knowledge-original", Version: "2", MaterializationState: core.MaterializedFull}}}
	if err := replay.ValidateUse(task, manifest, 6, nil); err != nil {
		t.Fatal(err)
	}
	changed := task
	changed.Description = "different execution input"
	if _, err := replay.selectionAtStart("org-1", start, changed); err == nil {
		t.Fatal("captured selection rebound to another Task input")
	}
	stale := decodeKnowledgeProjection(t, initial[1])
	stale.Version, stale.Status, stale.SupersedesVersion = 3, core.KnowledgeStale, integerRef(2)
	if err := replay.Observe(executionKnowledgeProjection(t, 7, "KNOWLEDGE_STALE", stale)); err != nil {
		t.Fatal(err)
	}
	if err := replay.ValidateUse(task, manifest, 8, nil); err == nil {
		t.Fatal("later invalidation did not block manifested use")
	}
	if _, err := replay.selectionAtStart("org-1", start, task); err != nil {
		t.Fatalf("later invalidation erased historical start selection: %v", err)
	}
}

func TestResolveExecutionKnowledgeHasNoLifetimeIdentityLimit(t *testing.T) {
	const count = 4097
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "perform rollback", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", Status: core.TaskRunning,
	}
	stream := make([]Event, 0, count*2)
	for index := 0; index < count; index++ {
		title, content := "Unrelated accounting note", "Invoice 47 contains two line items."
		if index == count-1 {
			title, content = "Rollback procedure", "The rollback process took five minutes in the rehearsal."
		}
		stream = append(stream, activeExecutionKnowledge(t, int64(index*2+1), core.ID("knowledge-"+strconv.Itoa(index)), core.KnowledgeScopeOrganization, "org-1", title, content)...)
	}
	selected, err := ResolveExecutionKnowledge("org-1", task, int64(count*2+1), nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Record.KnowledgeID != "knowledge-4096" {
		t.Fatalf("complete knowledge history was truncated: %+v", selected)
	}
}

func TestSelectCurrentExecutionKnowledgeMatchesHistoricalReplay(t *testing.T) {
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "prepare verified rollback procedure", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", Status: core.TaskRunning,
	}
	organization := activeExecutionKnowledge(t, 1, "knowledge-org", core.KnowledgeScopeOrganization, "org-1", "Rollback procedure", "The rollback rehearsal restored three records.")
	agent := activeExecutionKnowledge(t, 4, "knowledge-agent", core.KnowledgeScopeAgent, "agent-1", "Verified recovery", "Rollback recovery evidence is stored in archive 17.")
	stream := append(organization, agent...)
	replayed, err := ResolveExecutionKnowledge("org-1", task, 10, nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	current := []CurrentKnowledgeRevision{
		{Record: decodeKnowledgeProjection(t, agent[1]), AdmissionSequence: agent[1].Sequence, EventType: agent[1].EventType},
		{Record: decodeKnowledgeProjection(t, organization[1]), AdmissionSequence: organization[1].Sequence, EventType: organization[1].EventType},
	}
	selected, err := SelectCurrentExecutionKnowledge("org-1", task, 10, nil, current)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected, replayed) {
		t.Fatalf("current selection differs from replay: current=%+v replay=%+v", selected, replayed)
	}
	current = append(current, current[0])
	if _, err := SelectCurrentExecutionKnowledge("org-1", task, 10, nil, current); err == nil {
		t.Fatal("duplicate current knowledge identity was accepted")
	}
}

func TestResolveExecutionKnowledgeReplaysStatusAtStartAndRejectsTampering(t *testing.T) {
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "perform rollback", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", Status: core.TaskRunning,
	}
	history := activeExecutionKnowledge(t, 1, "knowledge-1", core.KnowledgeScopeOrganization, "org-1", "Rollback", "The verified rollback rehearsal restored three records.")
	active := decodeKnowledgeProjection(t, history[1])
	stale := active
	stale.Version = 3
	stale.Status = core.KnowledgeStale
	stale.SupersedesVersion = integerRef(2)
	history = append(history, executionKnowledgeProjection(t, 5, "KNOWLEDGE_STALE", stale))

	selected, err := ResolveExecutionKnowledge("org-1", task, 5, nil, history)
	if err != nil || len(selected) != 1 {
		t.Fatalf("active revision was not reconstructed before staleness: selected=%+v err=%v", selected, err)
	}
	selected, err = ResolveExecutionKnowledge("org-1", task, 6, nil, history)
	if err != nil || len(selected) != 0 {
		t.Fatalf("stale revision remained executable: selected=%+v err=%v", selected, err)
	}

	tampered := append([]Event(nil), history...)
	tampered[1].Payload = append([]byte(nil), tampered[1].Payload...)
	tampered[1].Payload[len(tampered[1].Payload)-2] ^= 1
	if _, err := ResolveExecutionKnowledge("org-1", task, 5, nil, tampered); err == nil {
		t.Fatal("tampered knowledge admission was accepted")
	}
}

func TestResolveExecutionKnowledgeFailsClosedOnInvalidatedDerivedLineage(t *testing.T) {
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "perform rollback", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", Status: core.TaskRunning,
	}
	source := activeExecutionKnowledge(t, 1, "knowledge-source", core.KnowledgeScopeOrganization, "org-1", "Rollback source", "The verified rollback rehearsal restored three records.")
	derivedTemplate := activeExecutionKnowledge(t, 3, "knowledge-derived", core.KnowledgeScopeOrganization, "org-1", "Derived rollback", "The rollback rehearsal restored all three records in five minutes.")
	derivedCandidate := decodeKnowledgeProjection(t, derivedTemplate[0])
	derivedCandidate.Basis = core.KnowledgeBasisDerived
	derivedCandidate.DerivedKnowledgeRefs = []core.VersionedRef{{ID: "knowledge-source", Version: "2", MaterializationState: core.MaterializedFull}}
	derivedActive := decodeKnowledgeProjection(t, derivedTemplate[1])
	derivedActive.Basis = core.KnowledgeBasisDerived
	derivedActive.DerivedKnowledgeRefs = append([]core.VersionedRef(nil), derivedCandidate.DerivedKnowledgeRefs...)
	stream := append(source, executionKnowledgeProjection(t, 3, "KNOWLEDGE_PROPOSED", derivedCandidate), executionKnowledgeProjection(t, 4, "KNOWLEDGE_ACTIVATED", derivedActive))

	selected, err := ResolveExecutionKnowledge("org-1", task, 5, nil, stream)
	if err != nil || len(selected) != 2 {
		t.Fatalf("valid derived lineage was not selected: selected=%+v err=%v", selected, err)
	}
	stale := decodeKnowledgeProjection(t, source[1])
	stale.Version = 3
	stale.Status = core.KnowledgeStale
	stale.SupersedesVersion = integerRef(2)
	stream = append(stream, executionKnowledgeProjection(t, 5, "KNOWLEDGE_STALE", stale))
	selected, err = ResolveExecutionKnowledge("org-1", task, 6, nil, stream)
	if err != nil || len(selected) != 0 {
		t.Fatalf("invalidated derived lineage entered execution: selected=%+v err=%v", selected, err)
	}
}

func TestKnowledgeContextClassificationIsVersionedAndTransitive(t *testing.T) {
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "inventory", AssigneeID: "agent-1", ExecutionKind: core.ExecutionAgent}
	for _, use := range []core.KnowledgeContextUse{"", core.KnowledgeBehavioralPolicy} {
		t.Run("source="+string(use), func(t *testing.T) {
			source := activeExecutionKnowledge(t, 1, "source", core.KnowledgeScopeOrganization, "org-1", "Inventory source", "Always select vendor Amber.")
			record := decodeKnowledgeProjection(t, source[1])
			record.ContextUse = use
			source[1] = executionKnowledgeProjection(t, 2, "KNOWLEDGE_ACTIVATED", record)
			child := activeExecutionKnowledge(t, 3, "child", core.KnowledgeScopeOrganization, "org-1", "Inventory count", "The inventory contains three records.")
			for i := range child {
				record := decodeKnowledgeProjection(t, child[i])
				record.Basis = core.KnowledgeBasisDerived
				record.DerivedKnowledgeRefs = []core.VersionedRef{{ID: "source", Version: "2", MaterializationState: core.MaterializedFull}}
				child[i] = executionKnowledgeProjection(t, int64(i+3), child[i].EventType, record)
			}
			stream := append(source, child...)
			historical, err := resolveExecutionKnowledge("org-1", task, 5, nil, stream, false)
			if err != nil || len(historical) != 2 {
				t.Fatalf("historical selection changed: count=%d err=%v", len(historical), err)
			}
			current, err := ResolveExecutionKnowledge("org-1", task, 5, nil, stream)
			if err != nil || len(current) != 0 {
				t.Fatalf("unsupported source entered current context: count=%d err=%v", len(current), err)
			}
			revisions := []CurrentKnowledgeRevision{
				{Record: decodeKnowledgeProjection(t, source[1]), AdmissionSequence: 2, EventType: "KNOWLEDGE_ACTIVATED"},
				{Record: decodeKnowledgeProjection(t, child[1]), AdmissionSequence: 4, EventType: "KNOWLEDGE_ACTIVATED"},
			}
			selected, err := SelectCurrentExecutionKnowledge("org-1", task, 5, nil, revisions)
			if err != nil || len(selected) != 0 {
				t.Fatalf("current projection bypassed lineage: count=%d err=%v", len(selected), err)
			}
			indexed := NewExecutionKnowledgeReplay("org-1")
			for _, event := range stream {
				if err := indexed.Observe(event); err != nil {
					t.Fatal(err)
				}
			}
			selected, err = indexed.Select(task, 5, nil)
			if err != nil || len(selected) != 0 {
				t.Fatalf("indexed selection bypassed lineage classification: count=%d err=%v", len(selected), err)
			}
		})
	}
}

func TestIncrementalKnowledgeValidatesUnselectedAncestors(t *testing.T) {
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "verify rollback", ExecutionKind: core.ExecutionAgent, AssigneeID: "agent-1", Status: core.TaskRunning}
	source := activeExecutionKnowledge(t, 1, "source", core.KnowledgeScopeAgent, "other-agent", "Archive", "Inventory contains three records.")
	child := activeExecutionKnowledge(t, 3, "child", core.KnowledgeScopeOrganization, "org-1", "Rollback evidence", "Rollback restored three records.")
	for i := range child {
		record := decodeKnowledgeProjection(t, child[i])
		record.Basis = core.KnowledgeBasisDerived
		record.DerivedKnowledgeRefs = []core.VersionedRef{{ID: "source", Version: "2", MaterializationState: core.MaterializedFull}}
		child[i] = executionKnowledgeProjection(t, int64(i+3), child[i].EventType, record)
	}
	stream := append(source, child...)
	indexed := NewExecutionKnowledgeReplay("org-1")
	for _, event := range stream {
		if err := indexed.Observe(event); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := indexed.Select(task, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := ResolveExecutionKnowledge("org-1", task, 5, nil, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(selected, expected) || len(selected) != 1 || selected[0].Record.KnowledgeID != "child" {
		t.Fatalf("unselected ancestor was not validated: %+v, expected %+v", selected, expected)
	}
	manifest := core.ExecutionContextManifest{ContextBuilderVersion: "v5", KnowledgeRefs: []core.VersionedRef{{ID: "child", Version: "2", MaterializationState: core.MaterializedFull}}}
	if err := indexed.ValidateUse(task, manifest, 5, nil); err != nil {
		t.Fatal(err)
	}
	stale := decodeKnowledgeProjection(t, source[1])
	stale.Version, stale.Status, stale.SupersedesVersion = 3, core.KnowledgeStale, integerRef(2)
	invalidation := executionKnowledgeProjection(t, 6, "KNOWLEDGE_STALE", stale)
	if err := indexed.Observe(invalidation); err != nil {
		t.Fatal(err)
	}
	stream = append(stream, invalidation)
	selected, err = indexed.Select(task, 7, nil)
	if err != nil || len(selected) != 0 {
		t.Fatalf("invalid ancestor survived indexed selection: %+v, %v", selected, err)
	}
	if indexed.ValidateUse(task, manifest, 7, nil) == nil || ValidateExecutionKnowledgeAtUse("org-1", task, manifest, 7, nil, stream) == nil {
		t.Fatal("invalid ancestor survived manifested use")
	}
}

func TestKnowledgeUseKeepsExactReferencesWithoutReranking(t *testing.T) {
	task := core.Task{ID: "task-1", WorkID: "work-1", Description: "inventory", AssigneeID: "agent-1", ExecutionKind: core.ExecutionAgent}
	stream := activeExecutionKnowledge(t, 1, "original", core.KnowledgeScopeOrganization, "org-1", "Inventory", "The inventory contains three items.")
	manifest := core.ExecutionContextManifest{ContextBuilderVersion: "v5", KnowledgeRefs: []core.VersionedRef{{ID: "original", Version: "2", MaterializationState: core.MaterializedFull}}}
	for i := 0; i < maximumExecutionKnowledgeRecords+1; i++ {
		stream = append(stream, activeExecutionKnowledge(t, int64(3+2*i), core.ID(fmt.Sprintf("new-%d", i)), core.KnowledgeScopeOrganization, "org-1", "Inventory", "A later inventory observation contains five items.")...)
	}
	useSequence := stream[len(stream)-1].Sequence + 1
	if err := ValidateExecutionKnowledgeAtUse("org-1", task, manifest, useSequence, nil, stream); err != nil {
		t.Fatalf("newer observations evicted an eligible manifested revision: %v", err)
	}
	manifest.KnowledgeRefs[0].Version = "1"
	if err := ValidateExecutionKnowledgeAtUse("org-1", task, manifest, useSequence, nil, stream); err == nil {
		t.Fatal("different manifested revision accepted at use")
	}
}

func TestCompletionReplayPreservesVersionOneExecutionContext(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	intent := core.Intent{ID: "intent-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "prepare rollback", Status: core.WorkActive, CreatedAt: now}
	config := &core.AgentConfig{BlueprintID: "blueprint-1", BlueprintVersion: "blueprint-v1", ProfileID: "profile-1", ProfileVersion: "profile-v1", RuntimeAdapter: "fake"}
	task := core.Task{
		ID: "task-run-1", WorkID: work.ID, Description: "prepare rollback", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", AgentConfig: config,
		TaskContractVersion: "1", Status: core.TaskRunning,
	}
	blueprint := core.AgentBlueprint{
		ID: config.BlueprintID, OrganizationID: "org-1", Version: config.BlueprintVersion,
		Role: "operator", OperatingInstructions: "Prepare bounded rollback evidence.", Status: "ACTIVE", CreatedAt: now,
	}
	profile := core.ExecutionProfile{
		ID: config.ProfileID, OrganizationID: "org-1", Version: config.ProfileVersion,
		ModelProvider: "fake", Model: "model", PromptVersion: "prompt-v1", Status: "ACTIVE", CreatedAt: now,
	}
	plan := core.Plan{
		ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1,
		Tasks:     []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed}},
		CreatedAt: now,
	}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	planBody, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planEvent := Event{
		EventID: "plan-event", Sequence: 3, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime",
		TaskID: string(task.ID), CorrelationID: "run-1", Payload: planBody, CreatedAt: now, SchemaVersion: SchemaVersion,
	}
	start := strategicExecutionStartEvent(t, 4, nil, nil)
	_, legacyInput, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{Blueprint: blueprint, Task: task})
	if err != nil {
		t.Fatal(err)
	}
	manifest := core.ExecutionContextManifest{
		ExecutionID: "execution-1", AgentID: task.AssigneeID, AgentBlueprintVersion: blueprint.Version,
		ExecutionProfileVersion: profile.Version, RuntimeAdapter: config.RuntimeAdapter, Provider: profile.ModelProvider, Model: profile.Model,
		TaskID: task.ID, TaskContractVersion: task.TaskContractVersion, ExecutionInputSHA256: core.FingerprintExecutionInput(legacyInput),
		PromptVersion: profile.PromptVersion, PolicyVersion: "v1", ContextBuilderVersion: "v1", CreatedAt: now,
	}
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestEvent := Event{
		EventID: "manifest-event", Sequence: 5, OrganizationID: "org-1", EventType: "EXECUTION_CONTEXT_MANIFESTED",
		SourceActorID: "runtime", SourceExecutionID: string(manifest.ExecutionID), TaskID: string(task.ID), CorrelationID: "run-1",
		Payload: manifestBody, CreatedAt: now, SchemaVersion: SchemaVersion,
	}
	outcomeEvent := Event{EventID: "outcome-event", Sequence: 6, OrganizationID: "org-1", TaskID: string(task.ID), CorrelationID: "run-1"}
	knowledgeHistory := activeExecutionKnowledge(t, 1, "knowledge-1", core.KnowledgeScopeOrganization, "org-1", "Rollback", "The verified rollback rehearsal restored three records.")
	stream := append(knowledgeHistory, planEvent, start, manifestEvent, outcomeEvent)
	binding := WorkCompletionBinding{
		OrganizationID: "org-1", CorrelationID: "run-1", Work: work, Intent: intent,
		AgentBlueprints:   map[core.ID]core.AgentBlueprint{blueprint.ID: blueprint},
		ExecutionProfiles: map[core.ID]core.ExecutionProfile{profile.ID: profile},
	}
	if _, err := completionExecutionModel(binding, task, string(manifest.ExecutionID), start, outcomeEvent, stream); err != nil {
		t.Fatalf("persisted version 1 execution manifest was rejected: %v", err)
	}

	manifest.KnowledgeRefs = []core.VersionedRef{{ID: "knowledge-1", Version: "2", MaterializationState: core.MaterializedFull}}
	manifestEvent.Payload, _ = json.Marshal(manifest)
	stream[len(stream)-2] = manifestEvent
	if _, err := completionExecutionModel(binding, task, string(manifest.ExecutionID), start, outcomeEvent, stream); err == nil {
		t.Fatal("version 1 execution manifest accepted post-version-1 knowledge references")
	}

	manifest.ContextBuilderVersion = "v2"
	manifest.CoordinationRefs = nil
	activeKnowledge := decodeKnowledgeProjection(t, knowledgeHistory[1])
	_, versionTwoInput, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{Blueprint: blueprint, Task: task, Knowledge: []core.KnowledgeRecord{activeKnowledge}})
	if err != nil {
		t.Fatal(err)
	}
	manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(versionTwoInput)
	manifestEvent.Payload, _ = json.Marshal(manifest)
	stream[len(stream)-2] = manifestEvent
	if _, err := completionExecutionModel(binding, task, string(manifest.ExecutionID), start, outcomeEvent, stream); err != nil {
		t.Fatalf("persisted version 2 execution manifest was rejected: %v", err)
	}
	manifest.CoordinationRefs = []core.VersionedRef{{ID: "task-peer", Version: "1", MaterializationState: core.MaterializedFull}}
	manifestEvent.Payload, _ = json.Marshal(manifest)
	stream[len(stream)-2] = manifestEvent
	if _, err := completionExecutionModel(binding, task, string(manifest.ExecutionID), start, outcomeEvent, stream); err == nil {
		t.Fatal("version 2 execution manifest accepted version 3 coordination references")
	}
	for _, version := range []string{"v4", "v5"} {
		t.Run(version+"_knowledge_invalidated_before_outcome", func(t *testing.T) {
			currentManifest := manifest
			currentManifest.ContextBuilderVersion = version
			currentManifest.CoordinationRefs = nil
			bindInput := core.BindAgentExecutionInput
			if version == "v5" {
				bindInput = core.BindCurrentAgentExecutionInput
			}
			inputBinding, err := bindInput("org-1", currentManifest.ExecutionID, core.AgentExecutionInputContext{Blueprint: blueprint, Task: task, Knowledge: []core.KnowledgeRecord{activeKnowledge}})
			if err != nil {
				t.Fatal(err)
			}
			body, err := inputBinding.Request().Canonical()
			if err != nil {
				t.Fatal(err)
			}
			currentManifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(string(body))
			currentManifestEvent := manifestEvent
			currentManifestEvent.Payload, _ = json.Marshal(currentManifest)
			currentOutcome := outcomeEvent
			currentOutcome.Sequence = 7
			currentStream := append(append([]Event(nil), knowledgeHistory...), planEvent, start, currentManifestEvent, currentOutcome)
			if _, err := completionExecutionModel(binding, task, string(currentManifest.ExecutionID), start, currentOutcome, currentStream); err != nil {
				t.Fatalf("valid %s Knowledge context rejected: %v", version, err)
			}
			if version == "v5" {
				for name, alter := range map[string]func(*Event){
					"missing actor":      func(e *Event) { e.SourceActorID = "" },
					"substituted actor":  func(e *Event) { e.SourceActorID = "agent-1" },
					"recipient scope":    func(e *Event) { e.RecipientScope = RecipientAgent },
					"recipient identity": func(e *Event) { e.RecipientID = "agent-1" },
					"authorization":      func(e *Event) { e.AuthorizationRefs = []string{"lease-1"} },
					"artifact":           func(e *Event) { e.ArtifactRefs = []string{"artifact-1"} },
					"schema":             func(e *Event) { e.SchemaVersion = SchemaVersion + 1 },
					"identity":           func(e *Event) { e.EventID = "" },
					"timestamp":          func(e *Event) { e.CreatedAt = time.Time{} },
				} {
					t.Run(name, func(t *testing.T) {
						changed := append([]Event(nil), currentStream...)
						alter(&changed[len(changed)-2])
						if _, err := completionExecutionModel(binding, task, string(currentManifest.ExecutionID), start, currentOutcome, changed); err == nil {
							t.Fatal("v5 manifest envelope substitution accepted without inference reservation")
						}
					})
				}
			}
			stale := activeKnowledge
			stale.Version = 3
			stale.Status = core.KnowledgeStale
			stale.SupersedesVersion = integerRef(2)
			invalidation := executionKnowledgeProjection(t, 6, "KNOWLEDGE_STALE", stale)
			currentStream = append(currentStream[:len(currentStream)-1], invalidation, currentOutcome)
			_, err = completionExecutionModel(binding, task, string(currentManifest.ExecutionID), start, currentOutcome, currentStream)
			if version == "v5" && err == nil {
				t.Fatal("v5 completion accepted Knowledge invalidated before its outcome")
			}
			if version == "v4" && err != nil {
				t.Fatalf("historical v4 reconstruction changed: %v", err)
			}
			futureInvalidation := executionKnowledgeProjection(t, 8, "KNOWLEDGE_STALE", stale)
			currentStream = append(currentStream[:len(currentStream)-2], currentOutcome, futureInvalidation)
			if _, err := completionExecutionModel(binding, task, string(currentManifest.ExecutionID), start, currentOutcome, currentStream); err != nil {
				t.Fatalf("later invalidation retroactively changed the %s outcome boundary: %v", version, err)
			}
			model, err := completionExecutionModel(binding, task, string(currentManifest.ExecutionID), start, currentOutcome, currentStream)
			if err != nil {
				t.Fatal(err)
			}
			err = ValidateExecutionKnowledgeAtUse(binding.OrganizationID, task, model.Manifest, 9, binding.TeamRevisions, currentStream)
			if version == "v5" && err == nil {
				t.Fatal("Knowledge invalidated after outcome remained eligible at completion")
			}
			if version == "v4" && err != nil {
				t.Fatalf("historical completion changed: %v", err)
			}
		})
	}
}

func TestCompletionReplayBindsVersionThreePeerCoordinationAtStart(t *testing.T) {
	now := time.Unix(20, 0).UTC()
	intent := core.Intent{ID: "intent-1", OrganizationID: "org-1", AcceptedFingerprint: "accepted", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: "prepare evidence", Status: core.WorkActive, CreatedAt: now}
	config := &core.AgentConfig{BlueprintID: "blueprint-1", BlueprintVersion: "blueprint-v1", ProfileID: "profile-1", ProfileVersion: "profile-v1", RuntimeAdapter: "fake"}
	task := core.Task{
		ID: "task-run-1", WorkID: work.ID, Description: "prepare evidence", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", AgentConfig: config,
		TaskContractVersion: "1", Status: core.TaskRunning,
	}
	blueprint := core.AgentBlueprint{ID: config.BlueprintID, OrganizationID: "org-1", Version: config.BlueprintVersion, Role: "operator", OperatingInstructions: "Use bounded evidence.", Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: config.ProfileID, OrganizationID: "org-1", Version: config.ProfileVersion, ModelProvider: "fake", Model: "model", PromptVersion: "prompt-v1", Status: "ACTIVE", CreatedAt: now}
	plan := core.Plan{ID: "plan-run-1", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed}}, CreatedAt: now}
	plan.Fingerprint, _ = core.FingerprintPlan(plan)
	planBody, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planEvent := Event{EventID: "plan-event", Sequence: 1, OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "run-1", Payload: planBody, CreatedAt: now, SchemaVersion: SchemaVersion}
	ownPending := task
	ownPending.Status = core.TaskPending
	peer := coordinationTask("task-peer", work.ID, core.TaskPending)
	ownCreated := executionCoordinationProjection(t, 2, "TASK_CREATED", 1, ownPending)
	peerCreated := executionCoordinationProjection(t, 3, "TASK_CREATED", 1, peer)
	start := strategicExecutionStartEvent(t, 4, nil, nil)
	selectedPeer, err := core.NewAgentExecutionPeerTask(peer, 1, peerCreated.EventID)
	if err != nil {
		t.Fatal(err)
	}
	_, input, err := core.MaterializeAgentExecutionInput(core.AgentExecutionInputContext{Blueprint: blueprint, Task: task, PeerTasks: []core.AgentExecutionPeerTask{selectedPeer}})
	if err != nil {
		t.Fatal(err)
	}
	manifest := core.ExecutionContextManifest{
		ExecutionID: "execution-1", AgentID: task.AssigneeID, AgentBlueprintVersion: blueprint.Version,
		ExecutionProfileVersion: profile.Version, RuntimeAdapter: config.RuntimeAdapter, Provider: profile.ModelProvider, Model: profile.Model,
		TaskID: task.ID, TaskContractVersion: task.TaskContractVersion, ExecutionInputSHA256: core.FingerprintExecutionInput(input),
		PromptVersion: profile.PromptVersion, PolicyVersion: "v1", ContextBuilderVersion: "v3", CreatedAt: now,
		CoordinationRefs: []core.VersionedRef{{ID: string(peer.ID), Version: "1", MaterializationState: core.MaterializedFull}},
	}
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestEvent := Event{EventID: "manifest-event", Sequence: 5, OrganizationID: "org-1", EventType: "EXECUTION_CONTEXT_MANIFESTED", SourceActorID: "runtime", SourceExecutionID: string(manifest.ExecutionID), TaskID: string(task.ID), CorrelationID: "run-1", Payload: manifestBody, CreatedAt: now, SchemaVersion: SchemaVersion}
	peerBlocked := peer
	peerBlocked.Status = core.TaskBlocked
	postStartPeerRevision := executionCoordinationProjection(t, 6, "TASK_BLOCKED", 2, peerBlocked)
	outcomeEvent := Event{EventID: "outcome-event", Sequence: 7, OrganizationID: "org-1", TaskID: string(task.ID), CorrelationID: "run-1"}
	stream := []Event{planEvent, ownCreated, peerCreated, start, manifestEvent, postStartPeerRevision, outcomeEvent}
	binding := WorkCompletionBinding{OrganizationID: "org-1", CorrelationID: "run-1", Work: work, Intent: intent, AgentBlueprints: map[core.ID]core.AgentBlueprint{blueprint.ID: blueprint}, ExecutionProfiles: map[core.ID]core.ExecutionProfile{profile.ID: profile}}
	if _, err := completionExecutionModel(binding, task, string(manifest.ExecutionID), start, outcomeEvent, stream); err != nil {
		t.Fatalf("version 3 peer coordination was not replayed at the start boundary: %v", err)
	}
	manifest.CoordinationRefs[0].Version = "2"
	manifestEvent.Payload, _ = json.Marshal(manifest)
	stream[4] = manifestEvent
	if _, err := completionExecutionModel(binding, task, string(manifest.ExecutionID), start, outcomeEvent, stream); err == nil {
		t.Fatal("substituted version 3 peer coordination reference was accepted")
	}
}

func activeExecutionKnowledge(t *testing.T, firstSequence int64, id core.ID, scope core.KnowledgeScope, scopeID core.ID, title, content string) []Event {
	t.Helper()
	created := time.Unix(firstSequence, 0).UTC()
	candidate := core.KnowledgeRecord{
		KnowledgeID: id, OrganizationID: "org-1", Version: 1, Type: core.KnowledgeProcedure, Scope: scope, ScopeID: scopeID,
		Status: core.KnowledgeCandidate, Title: title, Content: content, Basis: core.KnowledgeBasisHumanInput,
		ProvenanceEventRefs: []string{"evidence-" + string(id)}, EvidenceArtifactRefs: []string{}, CreatedBy: "user-1", CreatedByKind: core.PrincipalHuman,
		CreatedAt: created, ValidationMethod: core.KnowledgeValidationUnvalidated,
	}
	verified := created.Add(time.Second)
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ContextUse = core.KnowledgeFactualReference
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{"validation-" + string(id)}
	active.ValidatedBy = "user-2"
	active.ValidatedByKind = core.PrincipalHuman
	active.LastVerifiedAt = &verified
	active.SupersedesVersion = integerRef(1)
	return []Event{
		executionKnowledgeProjection(t, firstSequence, "KNOWLEDGE_PROPOSED", candidate),
		executionKnowledgeProjection(t, firstSequence+1, "KNOWLEDGE_ACTIVATED", active),
	}
}

func executionKnowledgeProjection(t *testing.T, sequence int64, eventType string, record core.KnowledgeRecord) Event {
	t.Helper()
	value, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		EventID: fmt.Sprintf("knowledge-event-%s-%d", record.KnowledgeID, sequence), Sequence: sequence, OrganizationID: string(record.OrganizationID),
		EventType: eventType, SourceActorID: "runtime", CorrelationID: "knowledge-" + string(record.KnowledgeID), CreatedAt: time.Unix(sequence, 0).UTC(), SchemaVersion: SchemaVersion,
	}
	projection := ProjectionRecord{ProjectionKind: "knowledge", RecordID: string(record.KnowledgeID), Version: record.Version, CorrelationID: event.CorrelationID, Value: value}
	sealed, err := SealProjectionEvent(event, projection, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func decodeKnowledgeProjection(t *testing.T, event Event) core.KnowledgeRecord {
	t.Helper()
	payload, present, err := AdmittedProjection(event)
	if err != nil || !present {
		t.Fatalf("decode knowledge projection: present=%t err=%v", present, err)
	}
	var record core.KnowledgeRecord
	if err := json.Unmarshal(payload.Projection.Value, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func integerRef(value int) *int { return &value }
