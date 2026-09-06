package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

// Exercise the complete recovery validator with growing durable planning
// histories. Setup uses only synthetic local accounting, without a model call.
func BenchmarkInferencePlanningRecovery(b *testing.B) {
	for _, count := range []int{100, 1000} {
		b.Run(fmt.Sprintf("reservations-%d", count), func(b *testing.B) {
			store, err := Open(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			policy := testInferencePolicy(time.Now().UTC())
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(b.Context(), policy); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < count; i++ {
				reservation, err := store.ReserveInference(b.Context(), testInferenceRequest(fmt.Sprintf("request-%d", i)))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := store.ReconcileInference(b.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.ValidateInferenceAdmissions(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkInferenceTaskRecovery(b *testing.B) {
	for _, count := range []int{25, 100, 400} {
		b.Run(fmt.Sprintf("executions-%d", count), func(b *testing.B) {
			store, err := Open(":memory:")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })
			agent, config := appendTaskAssignmentAgent(b, b.Context(), store, "org-1", "benchmark", true)
			now := time.Now().UTC()
			policy := inference.Policy{Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: "provider", Model: "model", ExecutionProfileVersion: config.ProfileVersion, Mode: inference.Local, MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 240, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour)}
			if err := store.ActivateInferencePolicy(b.Context(), policy); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < count; i++ {
				request := appendBenchmarkTaskInference(b, store, agent, config, fmt.Sprintf("benchmark-%d", i))
				reservation, err := store.ReserveInference(b.Context(), request)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := store.ReconcileInference(b.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.ValidateInferenceAdmissions(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func appendBenchmarkTaskInference(t testing.TB, store *SQLite, agent core.Agent, config core.AgentConfig, correlationID string) inference.InferenceRequest {
	t.Helper()
	now := time.Now().UTC()
	intent := core.Intent{ID: core.ID("intent-" + correlationID), OrganizationID: "org-1", OriginalInstruction: "bounded work", NormalizedObjective: "bounded work", AcceptedFingerprint: "internal-" + correlationID, CreatedAt: now}
	work := core.Work{ID: core.ID("work-" + correlationID), IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: core.ID("task-" + correlationID), WorkID: work.ID, Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	plan := core.Plan{ID: core.ID("plan-" + correlationID), IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "agent-work", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: now}
	var err error
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID, Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	var digest string
	if _, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{
		Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		manifest := testAgentStartManifest(task, selection)
		manifest.Provider, manifest.Model, manifest.PromptVersion = "provider", "model", "v1"
		knowledge := make([]core.KnowledgeRecord, 0, len(selection.Knowledge))
		for _, selected := range selection.Knowledge {
			knowledge = append(knowledge, selected.Record)
		}
		var inbox []core.AgentExecutionInboxEvent
		for _, route := range selection.Inbox {
			for _, event := range route.Events {
				inbox = append(inbox, core.AgentExecutionInboxEvent{Sequence: event.Sequence, EventID: event.EventID, EventType: event.EventType, SourceActorID: event.SourceActorID, RecipientScope: event.RecipientScope, RecipientID: event.RecipientID, TaskID: event.TaskID, CreatedAt: event.CreatedAt, Payload: event.Payload})
			}
		}
		sort.Slice(inbox, func(i, j int) bool { return inbox[i].Sequence < inbox[j].Sequence })
		manifest.EventRefs = nil
		for _, event := range inbox {
			manifest.EventRefs = append(manifest.EventRefs, event.EventID)
		}
		input, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, core.AgentExecutionInputContext{Task: task, Knowledge: knowledge, InboxEvents: inbox, Blueprint: core.AgentBlueprint{ID: config.BlueprintID, OrganizationID: "org-1", Version: config.BlueprintVersion, Role: "worker", OperatingInstructions: "bounded work"}})
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		body, err := input.Request().Canonical()
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		digest = core.FingerprintExecutionInput(string(body))
		manifest.ExecutionInputSHA256 = digest
		return manifest, nil
	}); err != nil {
		t.Fatal(err)
	}
	executionID := "execution-" + string(task.ID) + "-v2"
	return inference.InferenceRequest{Scope: inference.Scope{OrganizationID: "org-1", Purpose: inference.PurposeTaskExecution, RequestID: executionID, ExecutionID: executionID, TaskID: string(task.ID), CorrelationID: correlationID}, Descriptor: execution.ModelDescriptor{Provider: "provider", Model: "model", ExecutionProfileVersion: config.ProfileVersion}, PromptSHA256: digest}
}

func appendFactualInferenceKnowledge(t *testing.T, store *SQLite, id, title, content string) core.KnowledgeRecord {
	t.Helper()
	ctx := t.Context()
	artifact := "artifact-" + id
	evidence, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: id, ArtifactRefs: []string{artifact}, Payload: map[string]string{"summary": "synthetic factual evidence"}})
	if err != nil {
		t.Fatal(err)
	}
	candidate := core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: title, Content: content, Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{evidence.EventID}, EvidenceArtifactRefs: []string{artifact}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-" + id, ArtifactRefs: []string{artifact}}, ProjectionKind: "knowledge", RecordID: id, Version: 1, Value: candidate}); err != nil {
		t.Fatal(err)
	}
	lease := core.CapabilityLease{ID: core.ID("lease-" + id), ActorID: "validator", ActorKind: core.PrincipalHuman, OriginTaskID: "validation-task", Action: "knowledge.validate", Resource: id, Scope: "org-1"}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "runtime", "validation-task", nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	trace := core.AuthorizationTrace{Allowed: true, LeaseID: lease.ID, ActorID: lease.ActorID, ActorKind: lease.ActorKind, TaskID: lease.OriginTaskID, Action: lease.Action, Resource: lease.Resource, Scope: lease.Scope}
	capability, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: "validator", TaskID: "validation-task", AuthorizationRefs: []string{string(lease.ID)}, Payload: trace})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: "validator", TaskID: "validation-task", CorrelationID: id, ArtifactRefs: []string{artifact}, Payload: events.KnowledgeJudgmentPayload{KnowledgeID: candidate.KnowledgeID, CandidateVersion: 1, Decision: events.KnowledgeJudgmentValidated, ContextUse: core.KnowledgeFactualReference, Statement: "The synthetic candidate is a factual reference.", CapabilityCheckEventID: capability.EventID, SourcePrincipalID: "validator", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{artifact}}})
	if err != nil {
		t.Fatal(err)
	}
	active := candidate
	previous, verifiedAt := 1, time.Now().UTC()
	active.Version, active.Status, active.ContextUse = 2, core.KnowledgeActive, core.KnowledgeFactualReference
	active.SupersedesVersion, active.LastVerifiedAt = &previous, &verifiedAt
	active.ValidationMethod, active.ValidatedBy, active.ValidatedByKind = core.KnowledgeValidationHuman, "validator", core.PrincipalHuman
	active.ValidationRefs = []string{capability.EventID, statement.EventID}
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_ACTIVATED", SourceActorID: "runtime", CorrelationID: "knowledge-" + id, ArtifactRefs: []string{artifact}}, ProjectionKind: "knowledge", RecordID: id, Version: 2, Value: active}); err != nil {
		t.Fatal(err)
	}
	return active
}

func TestTaskInferenceRecoveryReconstructsFactualContext(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "factual-replay", true)
	selected := appendFactualInferenceKnowledge(t, store, "selected-fact", "Bounded verification", "The bounded rehearsal restored three records.")
	other := appendFactualInferenceKnowledge(t, store, "unselected-fact", "Revenue", "Sales increased by three percent.")
	request := appendBenchmarkTaskInference(t, store, agent, config, "factual-replay")
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("original factual execution rejected: %v", err)
	}
	var body []byte
	if err := store.db.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	var manifest core.ExecutionContextManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.KnowledgeRefs) != 1 || manifest.KnowledgeRefs[0].ID != "selected-fact" {
		t.Fatalf("unexpected initial selection: %+v", manifest.KnowledgeRefs)
	}
	original := append([]byte(nil), body...)
	manifest.KnowledgeRefs = append(manifest.KnowledgeRefs, core.VersionedRef{ID: string(other.KnowledgeID), Version: "2", MaterializationState: core.MaterializedFull})
	body, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`, body); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "knowledge references do not match") {
		t.Fatalf("unselected factual reference was not rejected by reconstruction: %v", err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`, original); err != nil {
		t.Fatal(err)
	}
	previous := 2
	selected.Version, selected.Status, selected.SupersedesVersion = 3, core.KnowledgeStale, &previous
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_STALE", SourceActorID: "runtime", CorrelationID: "knowledge-selected-fact", ArtifactRefs: selected.EvidenceArtifactRefs}, ProjectionKind: "knowledge", RecordID: "selected-fact", Version: 3, Value: selected}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("later invalidation changed a historical reservation: %v", err)
	}
	// A restored reservation moved past the invalidation must lose authority,
	// even though its manifest and accounting values still agree exactly.
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET sequence=(SELECT MAX(sequence)+1 FROM events) WHERE event_type='INFERENCE_RESERVED'`); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "no longer eligible") {
		t.Fatalf("post-invalidation reservation was not rejected: %v", err)
	}
}

func setupAdmittedTaskInference(t *testing.T) (*SQLite, inference.InferenceRequest) {
	t.Helper()
	ctx := t.Context()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	intent := core.Intent{ID: "intent-work-1", OrganizationID: "org-1", OriginalInstruction: "test task", NormalizedObjective: "test task", AcceptedFingerprint: "internal-test", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-inference-test"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "test", PolicyVersion: "v1", CreatedAt: now}},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "inference-test"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "inference-test"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			t.Fatal(err)
		}
	}
	agent, config := appendTaskAssignmentAgent(t, ctx, store, "org-1", "inference-test", false)
	task := appendPendingAgentExecutionTask(t, ctx, store, "inference-test", "task-inference", agent, config)
	plan := core.Plan{ID: "plan-inference-test", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "agent-work", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: now}
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-inference-test", CorrelationID: "inference-test", Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	var digest string
	if _, _, err := store.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "inference-test"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		manifest := testAgentStartManifest(task, selection)
		manifest.Provider, manifest.Model, manifest.PromptVersion = "provider", "model", "v1"
		blueprint := core.AgentBlueprint{ID: config.BlueprintID, OrganizationID: "org-1", Version: config.BlueprintVersion, Role: "worker", OperatingInstructions: "bounded work"}
		input, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, core.AgentExecutionInputContext{Blueprint: blueprint, Task: task})
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		body, err := input.Request().Canonical()
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		digest = core.FingerprintExecutionInput(string(body))
		manifest.ExecutionInputSHA256 = digest
		return manifest, nil
	}); err != nil {
		t.Fatal(err)
	}
	policy := inference.Policy{Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: "provider", Model: "model", ExecutionProfileVersion: config.ProfileVersion, Mode: inference.Local, MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 240, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour)}
	if err := store.ActivateInferencePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	request := inference.InferenceRequest{Scope: inference.Scope{OrganizationID: "org-1", Purpose: inference.PurposeTaskExecution, RequestID: "execution-task-inference-v2", ExecutionID: "execution-task-inference-v2", TaskID: string(task.ID), CorrelationID: "inference-test"}, Descriptor: execution.ModelDescriptor{Provider: "provider", Model: "model", ExecutionProfileVersion: config.ProfileVersion}, PromptSHA256: digest}
	return store, request
}

func TestTaskInferenceRecoveryRetainsCrossWorkInboxObservation(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "inbox-recovery", true)
	policy := testInferencePolicy(time.Now().UTC())
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
	policy.Mode, policy.Pricing = inference.Local, nil
	if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	message, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", RecipientScope: events.RecipientAgent, RecipientID: string(agent.ID), CorrelationID: "external-work", Payload: map[string]string{"note": "The synthetic archive contains three records."}})
	if err != nil {
		t.Fatal(err)
	}
	for _, correlation := range []string{"first-work", "second-work"} {
		request := appendBenchmarkTaskInference(t, store, agent, config, correlation)
		if correlation == "first-work" {
			if _, err := events.NewGateway(store).ObserveInbox(t.Context(), "org-1", string(agent.ID), request.Scope.ExecutionID, request.Scope.TaskID, correlation, events.RecipientAgent, string(agent.ID), []string{message.EventID}); err != nil {
				t.Fatal(err)
			}
		}
		reservation, err := store.ReserveInference(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("cross-Work inbox history rejected: %v", err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var secondStart string
	var observation events.Event
	for _, event := range stream {
		if event.EventType == "EXECUTION_STARTED" && event.CorrelationID == "second-work" {
			secondStart = event.EventID
		}
		if event.EventType == "INBOX_EVENTS_OBSERVED" {
			observation = event
		}
		if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
			var manifest core.ExecutionContextManifest
			if err := json.Unmarshal(event.Payload, &manifest); err != nil {
				t.Fatal(err)
			}
			if event.CorrelationID == "first-work" && (len(manifest.EventRefs) != 1 || manifest.EventRefs[0] != message.EventID) {
				t.Fatalf("first execution did not consume the message: %+v", manifest.EventRefs)
			}
			if event.CorrelationID == "second-work" && len(manifest.EventRefs) != 0 {
				t.Fatalf("second execution consumed the message again: %+v", manifest.EventRefs)
			}
		}
	}
	if observation.EventID == "" || secondStart == "" {
		t.Fatal("missing observation or second execution")
	}
	var payload events.InboxEventsObservedPayload
	if err := json.Unmarshal(observation.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	payload.ExecutionStartEventRef = secondStart
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, observation.EventID); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "observation") {
		t.Fatalf("substituted consuming execution was not rejected: %v", err)
	}
}

func TestTaskInferenceScopedStrategyMatchesFullHistory(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	for _, suffix := range []string{"relevant", "unrelated"} {
		now := time.Now().UTC()
		mission := core.Mission{ID: core.ID("mission-" + suffix), OrganizationID: "org-1", Statement: "durable direction", Status: core.MissionActive, CreatedAt: now}
		goal := core.Goal{ID: core.ID("goal-" + suffix), OrganizationID: "org-1", MissionID: mission.ID, Objective: "measurable outcome", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "evidence", Origin: "TEST"}}, Status: core.GoalActive, CreatedAt: now}
		for _, draft := range []events.ProjectionDraft{
			{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: string(mission.ID)}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission},
			{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: string(goal.ID)}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal},
		} {
			if _, err := store.AppendProjection(t.Context(), draft); err != nil {
				t.Fatal(err)
			}
		}
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	history := newInferenceExecutionHistory()
	for _, event := range stream {
		if err := history.observe(event); err != nil {
			t.Fatal(err)
		}
	}
	task := history.tasks[request.Scope.TaskID].value
	work := core.Work{ID: task.WorkID, GoalID: "goal-relevant"}
	boundary := stream[len(stream)-1].Sequence + 1
	scoped := history.executionContextStream(request.Scope.CorrelationID, work, task, boundary, nil)
	for _, event := range scoped {
		if event.CorrelationID == "goal-unrelated" || event.CorrelationID == "mission-unrelated" {
			t.Fatal("unrelated strategy entered scoped reconstruction")
		}
	}
	full, fullRefs, fullVersions, err := events.ResolveStrategicContext("org-1", work, stream, boundary)
	if err != nil {
		t.Fatal(err)
	}
	selected, refs, versions, err := events.ResolveStrategicContext("org-1", work, scoped, boundary)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(full, selected) || !reflect.DeepEqual(fullRefs, refs) || !reflect.DeepEqual(fullVersions, versions) {
		t.Fatal("scoped strategy differs from complete history")
	}
	exact, err := events.ResolveStrategicContextByRefs("org-1", work, scoped, fullRefs, fullVersions)
	if err != nil || !reflect.DeepEqual(full, exact) {
		t.Fatalf("exact strategic references were not retained: %v", err)
	}
}

func TestInferenceRecoveryRejectsSubstitutedManifestReference(t *testing.T) {
	ctx := t.Context()
	store, request := setupAdmittedTaskInference(t)
	reservation, err := store.ReserveInference(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(ctx, reservation, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(ctx); err != nil {
		t.Fatalf("valid history rejected: %v", err)
	}
	var original []byte
	if err := store.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_type='INFERENCE_RESERVED'`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	var payload events.InferenceReservedPayload
	if err := json.Unmarshal(original, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ExecutionManifestRef == "" {
		t.Fatal("reservation omitted its execution manifest")
	}
	for _, replacement := range []string{"", "unadmitted-manifest"} {
		payload.ExecutionManifestRef = replacement
		changed, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, changed); err != nil {
			t.Fatal(err)
		}
		if err := store.ValidateInferenceAdmissions(ctx); err == nil {
			t.Fatalf("recovery validator accepted reference %q", replacement)
		}
		if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, original); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTaskInferenceRecoveryPreservesHistoricalManifestAccounting(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	var manifestBody, reservationBody []byte
	if err := store.db.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`).Scan(&manifestBody); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(t.Context(), `SELECT payload FROM events WHERE event_type='INFERENCE_RESERVED'`).Scan(&reservationBody); err != nil {
		t.Fatal(err)
	}
	var manifest core.ExecutionContextManifest
	var reservation events.InferenceReservedPayload
	if err := json.Unmarshal(manifestBody, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(reservationBody, &reservation); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"v1", "v2", "v3", "v4"} {
		t.Run(version, func(t *testing.T) {
			manifest.ContextBuilderVersion = version
			body, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`, body); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, reservationBody); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("current manifest reference accepted against a historical builder")
			}
			legacy := reservation
			legacy.ExecutionManifestRef = ""
			body, err = json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='INFERENCE_RESERVED'`, body); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("historical accounting contract changed: %v", err)
			}
		})
	}
}

func TestTaskInferenceRecoveryPreservesStartTimeProfile(t *testing.T) {
	for _, beforeReservation := range []bool{true, false} {
		t.Run(fmt.Sprintf("retired-before-reservation-%t", beforeReservation), func(t *testing.T) {
			store, request := setupAdmittedTaskInference(t)
			retire := func() {
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range stream {
					if event.EventType != "EXECUTION_PROFILE_CREATED" {
						continue
					}
					projection, present, err := events.AdmittedProjection(event)
					if err != nil || !present {
						t.Fatalf("profile admission: %v", err)
					}
					var profile core.ExecutionProfile
					if err := json.Unmarshal(projection.Projection.Value, &profile); err != nil {
						t.Fatal(err)
					}
					profile.Status = "INACTIVE"
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
						Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_UPDATED", SourceActorID: "runtime", CorrelationID: "profile-retirement"},
						ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 2, Value: profile,
					}); err != nil {
						t.Fatal(err)
					}
					return
				}
				t.Fatal("admitted profile not found")
			}
			if beforeReservation {
				retire()
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if !beforeReservation {
				retire()
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("later profile status invalidated admitted execution: %v", err)
			}
		})
	}
}

func TestTaskInferenceDispatchRosterRejectsSupersededReference(t *testing.T) {
	store, _ := setupAdmittedTaskInference(t)
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var start events.Event
	var profile core.ExecutionProfile
	for _, event := range stream {
		if event.EventType == "EXECUTION_STARTED" {
			start = event
		}
		if event.EventType == "EXECUTION_PROFILE_CREATED" {
			projection, present, err := events.AdmittedProjection(event)
			if err != nil || !present {
				t.Fatalf("profile admission: %v", err)
			}
			if err := json.Unmarshal(projection.Projection.Value, &profile); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_UPDATED", SourceActorID: "runtime", CorrelationID: "profile-revision"},
		ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 2, Value: profile,
	}); err != nil {
		t.Fatal(err)
	}
	stream, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	history := newInferenceExecutionHistory()
	for _, event := range stream {
		if err := history.observe(event); err != nil {
			t.Fatal(err)
		}
	}
	task := history.tasks[start.TaskID]
	// Test dispatch proof in isolation: moving the use boundary past a newer
	// admitted roster revision must invalidate the old exact event reference.
	start.Sequence = stream[len(stream)-1].Sequence + 1
	if err := events.ValidateAgentDispatchStart(start, task.value, task.record.Version, stream); err == nil {
		t.Fatal("full history accepted superseded roster reference")
	}
	roster, err := history.dispatchRoster(task.value, start.Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if err := events.ValidateAgentDispatchStart(start, task.value, task.record.Version, roster); err == nil {
		t.Fatal("indexed history accepted superseded roster reference")
	}
}

func TestTaskInferenceContextExcludesUnrelatedActivity(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	history := newInferenceExecutionHistory()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range stream {
		if err := history.observe(event); err != nil {
			t.Fatal(err)
		}
	}
	task := history.tasks[request.Scope.TaskID].value
	start := history.taskEvents[request.Scope.TaskID]
	teams, err := history.resolveTeams("org-1")
	if err != nil {
		t.Fatal(err)
	}
	baseline := history.executionContextStream(request.Scope.CorrelationID, core.Work{ID: task.WorkID}, task, start.Sequence, teams)
	for i := 0; i < 1000; i++ {
		event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "UNRELATED_ACTIVITY", SourceActorID: "runtime", TaskID: fmt.Sprintf("other-task-%d", i), CorrelationID: fmt.Sprintf("other-work-%d", i), RecipientScope: events.RecipientAgent, RecipientID: "other-agent", Payload: map[string]string{"note": "unrelated work"}})
		if err != nil {
			t.Fatal(err)
		}
		if err := history.observe(event); err != nil {
			t.Fatal(err)
		}
	}
	selected := history.executionContextStream(request.Scope.CorrelationID, core.Work{ID: task.WorkID}, task, start.Sequence, teams)
	if len(selected) != len(baseline) {
		t.Fatalf("unrelated activity expanded reconstruction from %d to %d events", len(baseline), len(selected))
	}
	for i := range baseline {
		if selected[i].EventID != baseline[i].EventID {
			t.Fatal("unrelated activity changed reconstruction context")
		}
	}
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("unrelated activity invalidated admitted execution: %v", err)
	}
}

func TestInferenceRecoveryRejectsSelfConsistentInputSubstitution(t *testing.T) {
	ctx := t.Context()
	store, request := setupAdmittedTaskInference(t)
	if _, err := store.ReserveInference(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(ctx); err != nil {
		t.Fatalf("original admission rejected: %v", err)
	}
	// Both sides of the reference and the accounting row agree, but these
	// bytes were never materialized from the execution's admitted context.
	replacement := core.FingerprintExecutionInput("unadmitted replacement input")
	if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=CAST(json_set(payload,'$.execution_input_sha256',?) AS BLOB) WHERE event_type='EXECUTION_CONTEXT_MANIFESTED'`, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload=CAST(json_set(payload,'$.prompt_sha256',?) AS BLOB) WHERE event_type='INFERENCE_RESERVED'`, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE inference_reservations SET prompt_sha256=?`, replacement); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(ctx); err == nil {
		t.Fatal("reservation replay accepted a self-consistent unadmitted input digest")
	}
}

func TestTaskInferenceRejectsSubstitutedRequest(t *testing.T) {
	for name, alter := range map[string]func(*inference.InferenceRequest){
		"request": func(r *inference.InferenceRequest) { r.Scope.RequestID = "different-request" },
		"execution": func(r *inference.InferenceRequest) {
			r.Scope.ExecutionID = "different-execution"
			r.Scope.RequestID = r.Scope.ExecutionID
		},
		"task":        func(r *inference.InferenceRequest) { r.Scope.TaskID = "different-task" },
		"correlation": func(r *inference.InferenceRequest) { r.Scope.CorrelationID = "different-correlation" },
		"input": func(r *inference.InferenceRequest) {
			r.PromptSHA256 = core.FingerprintExecutionInput("different-input")
		},
		"provider": func(r *inference.InferenceRequest) { r.Descriptor.Provider = "different-provider" },
		"model":    func(r *inference.InferenceRequest) { r.Descriptor.Model = "different-model" },
		"profile":  func(r *inference.InferenceRequest) { r.Descriptor.ExecutionProfileVersion = "different-profile" },
	} {
		t.Run(name, func(t *testing.T) {
			store, request := setupAdmittedTaskInference(t)
			changed := request
			alter(&changed)
			if _, err := store.ReserveInference(t.Context(), changed); err == nil {
				t.Fatal("substituted request admitted")
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatalf("denied request consumed the valid execution's admission: %v", err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTaskInferenceFreezeReleasePreservesExecutionAuthority(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	appendInferenceFreeze(t, store, "org-1", 1, true)
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("frozen execution admission: %v", err)
	}
	appendInferenceFreeze(t, store, "org-1", 2, false)
	if _, err := store.ReserveInference(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTaskInferenceRejectsFinishedExecution(t *testing.T) {
	store, request := setupAdmittedTaskInference(t)
	// Append a runtime finish while retaining the running Task projection to
	// exercise the execution lifetime boundary independently of Task status.
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		_, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: request.Scope.OrganizationID, EventType: "EXECUTION_FINISHED", SourceActorID: "runtime", SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: request.Scope.CorrelationID, Payload: map[string]string{"state": "finished"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReserveInference(t.Context(), request); err == nil || !strings.Contains(err.Error(), "finished") {
		t.Fatalf("finished execution admission: %v", err)
	}
}
