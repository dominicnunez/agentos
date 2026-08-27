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
	organization := activeExecutionKnowledge(t, 1, "knowledge-org", core.KnowledgeScopeOrganization, "org-1", "Rollback procedure", "Verify rollback evidence before applying it.")
	agent := activeExecutionKnowledge(t, 4, "knowledge-agent", core.KnowledgeScopeAgent, "agent-1", "Verified recovery", "Preserve evidence during rollback recovery.")
	wrongAgent := activeExecutionKnowledge(t, 7, "knowledge-other-agent", core.KnowledgeScopeAgent, "agent-2", "Rollback secret", "Do not cross the Agent scope.")
	irrelevant := activeExecutionKnowledge(t, 10, "knowledge-irrelevant", core.KnowledgeScopeOrganization, "org-1", "Marketing notes", "Research audience demand.")
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

func TestResolveExecutionKnowledgeHasNoLifetimeIdentityLimit(t *testing.T) {
	const count = 4097
	task := core.Task{
		ID: "task-1", WorkID: "work-1", Description: "perform rollback", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: "agent-1", Status: core.TaskRunning,
	}
	stream := make([]Event, 0, count*2)
	for index := 0; index < count; index++ {
		title, content := "Unrelated accounting note", "Reconcile a numbered invoice."
		if index == count-1 {
			title, content = "Rollback procedure", "Use the verified rollback process."
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
	organization := activeExecutionKnowledge(t, 1, "knowledge-org", core.KnowledgeScopeOrganization, "org-1", "Rollback procedure", "Verify rollback evidence before applying it.")
	agent := activeExecutionKnowledge(t, 4, "knowledge-agent", core.KnowledgeScopeAgent, "agent-1", "Verified recovery", "Preserve evidence during rollback recovery.")
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
	history := activeExecutionKnowledge(t, 1, "knowledge-1", core.KnowledgeScopeOrganization, "org-1", "Rollback", "Use verified rollback steps.")
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
	source := activeExecutionKnowledge(t, 1, "knowledge-source", core.KnowledgeScopeOrganization, "org-1", "Rollback source", "Use verified rollback steps.")
	derivedTemplate := activeExecutionKnowledge(t, 3, "knowledge-derived", core.KnowledgeScopeOrganization, "org-1", "Derived rollback", "Apply the validated rollback lesson.")
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
	stream := append(activeExecutionKnowledge(t, 1, "knowledge-1", core.KnowledgeScopeOrganization, "org-1", "Rollback", "Use verified rollback steps."), planEvent, start, manifestEvent, outcomeEvent)
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
