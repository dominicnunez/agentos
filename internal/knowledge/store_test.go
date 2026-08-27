package knowledge

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestStoreAdmitsValidatedKnowledgeAndRetrievesOnlyActiveTenantScope(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	orgOneEvent := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	orgTwoEvent := seedKnowledgeOrganization(t, ctx, gateway, "org-2")
	service := New(gateway)

	candidate := knowledgeCandidate("k-1", "org-1", orgOneEvent.EventID)
	proposed, err := service.Propose(ctx, candidate)
	if err != nil || proposed.EventType != "KNOWLEDGE_PROPOSED" {
		t.Fatalf("propose knowledge: event=%+v err=%v", proposed, err)
	}
	if rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10); err != nil || len(rows) != 0 {
		t.Fatalf("candidate leaked into active retrieval: rows=%+v err=%v", rows, err)
	}

	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{orgOneEvent.EventID}
	active.ValidatedBy = "reviewer-1"
	active.ValidatedByKind = core.PrincipalHuman
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("human judgment without authenticated authority was accepted")
	}
	taskID := core.ID("task-knowledge-validation")
	crossTenantLease := core.CapabilityLease{
		ID:           "lease-cross-tenant-validation",
		ActorID:      active.ValidatedBy,
		ActorKind:    active.ValidatedByKind,
		Action:       "knowledge.validate",
		Resource:     string(candidate.KnowledgeID),
		Scope:        string(candidate.OrganizationID),
		OriginTaskID: taskID,
	}
	if err := store.AppendRecord(ctx, "org-2", "CAPABILITY_GRANTED", "runtime", string(taskID), nil, nil, "capability_lease", string(crossTenantLease.ID), 1, crossTenantLease); err != nil {
		t.Fatalf("seed cross-tenant validator lease: %v", err)
	}
	crossTenantJudgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID:    "org-1",
		EventType:         "CAPABILITY_CHECKED",
		SourceActorID:     string(crossTenantLease.ActorID),
		TaskID:            string(taskID),
		AuthorizationRefs: []string{string(crossTenantLease.ID)},
		Payload:           authorizedKnowledgeValidationTrace(crossTenantLease),
	})
	if err != nil {
		t.Fatalf("admit cross-tenant validator judgment: %v", err)
	}
	active.ValidationRefs = []string{crossTenantJudgment.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("validator lease admitted by another organization was accepted")
	}
	lease := core.CapabilityLease{
		ID:           "lease-knowledge-validation",
		ActorID:      active.ValidatedBy,
		ActorKind:    active.ValidatedByKind,
		Action:       "knowledge.validate",
		Resource:     string(candidate.KnowledgeID),
		Scope:        string(candidate.OrganizationID),
		OriginTaskID: taskID,
	}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "runtime", string(taskID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatalf("seed validator lease: %v", err)
	}
	trace := authorizedKnowledgeValidationTrace(lease)
	judgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID:    "org-1",
		EventType:         "CAPABILITY_CHECKED",
		SourceActorID:     string(lease.ActorID),
		TaskID:            string(taskID),
		AuthorizationRefs: []string{string(lease.ID)},
		Payload:           trace,
	})
	if err != nil {
		t.Fatalf("admit validator judgment: %v", err)
	}
	statement := publishKnowledgeHumanStatement(t, ctx, gateway, "org-1", candidate.KnowledgeID, lease.ActorID, taskID, judgment.EventID, "work-knowledge-validation")
	active.ValidationRefs = []string{judgment.EventID, statement.EventID}
	verifiedBeforeEvidence := judgment.CreatedAt.Add(-time.Nanosecond)
	active.LastVerifiedAt = &verifiedBeforeEvidence
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("knowledge verified before its validation evidence was accepted")
	}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	misclassified := active
	misclassified.ValidationMethod = core.KnowledgeValidationIndependentAgent
	misclassified.ValidatedByKind = core.PrincipalExternalAgent
	if _, err := service.Activate(ctx, misclassified); err == nil {
		t.Fatal("human validator authority was relabeled as external Agent judgment")
	}
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatalf("activate knowledge: %v", err)
	}

	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10)
	if err != nil || len(rows) != 1 || rows[0].KnowledgeID != candidate.KnowledgeID {
		t.Fatalf("active knowledge unavailable: rows=%+v err=%v", rows, err)
	}
	if rows, err := service.Search(ctx, "org-2", core.KnowledgeScopeOrganization, "org-2", "rollback", 10); err != nil || len(rows) != 0 {
		t.Fatalf("knowledge crossed tenant: rows=%+v err=%v", rows, err)
	}
	assertActiveKnowledgeSelectedAtExecutionStart(t, ctx, store, gateway, active)

	crossTenant := knowledgeCandidate("k-cross", "org-1", orgTwoEvent.EventID)
	if _, err := service.Propose(ctx, crossTenant); err == nil {
		t.Fatal("cross-tenant provenance was accepted")
	}
	future := knowledgeCandidate("k-future", "org-1", orgOneEvent.EventID)
	future.CreatedAt = time.Now().UTC().Add(time.Hour)
	if _, err := service.Propose(ctx, future); err == nil {
		t.Fatal("knowledge timestamp after its admission was accepted")
	}
	forgedArtifact := knowledgeCandidate("k-artifact", "org-1", orgOneEvent.EventID)
	forgedArtifact.EvidenceArtifactRefs = []string{"artifact-not-on-evidence-event"}
	if _, err := service.Propose(ctx, forgedArtifact); err == nil {
		t.Fatal("unbound knowledge artifact evidence was accepted")
	}
	if err := store.AppendRecord(ctx, "org-1", "KNOWLEDGE_PROPOSED", "runtime", "", nil, nil, "knowledge", "legacy", 1, candidate); err == nil {
		t.Fatal("generic knowledge writer remained available")
	}
	lateStart := knowledgeCandidate("k-late-start", "org-1", orgOneEvent.EventID)
	lateStart.Version = 5
	if _, err := service.Propose(ctx, lateStart); err == nil {
		t.Fatal("knowledge history started above version 1")
	}
	orphanActive := knowledgeCandidate("k-orphan-active", "org-1", orgOneEvent.EventID)
	orphanActive.Version = 2
	orphanActive.Status = core.KnowledgeActive
	orphanActive.ValidationMethod = core.KnowledgeValidationDeterministic
	orphanActive.ValidationRefs = []string{orgOneEvent.EventID}
	orphanActive.ValidatedBy = "runtime"
	orphanActive.ValidatedByKind = core.PrincipalRuntime
	orphanActive.LastVerifiedAt = &verifiedAt
	orphanActive.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, orphanActive); err == nil {
		t.Fatal("active knowledge started without a candidate")
	}
}

func assertActiveKnowledgeSelectedAtExecutionStart(t *testing.T, ctx context.Context, store *ledger.SQLite, gateway *events.Gateway, knowledge core.KnowledgeRecord) {
	t.Helper()
	const correlationID = "execution-knowledge-selection"
	now := time.Now().UTC()
	blueprint := core.AgentBlueprint{
		ID: "blueprint-knowledge-agent", OrganizationID: knowledge.OrganizationID, Version: "v1", Role: "recovery worker",
		OperatingInstructions: "perform bounded recovery work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now,
	}
	profile := core.ExecutionProfile{
		ID: "profile-knowledge-agent", OrganizationID: knowledge.OrganizationID, Version: "v1", ModelProvider: "provider", Model: "model",
		PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now,
	}
	agent := core.Agent{
		ID: "agent-knowledge", OrganizationID: knowledge.OrganizationID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version,
		ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE",
	}
	intent := core.Intent{ID: "intent-knowledge-execution", OrganizationID: knowledge.OrganizationID, OriginalInstruction: "verify rollback procedure", NormalizedObjective: "verify rollback procedure", CreatedAt: now}
	work := core.Work{ID: "work-knowledge-execution", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	config := core.AgentConfig{
		BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: agent.RuntimeAdapter,
	}
	task := core.Task{
		ID: "task-knowledge-execution", WorkID: work.ID, Description: "verify rollback procedure", ExecutionKind: core.ExecutionAgent,
		ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending,
	}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
		{Event: events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
	} {
		if _, err := gateway.PublishProjection(ctx, draft); err != nil {
			t.Fatalf("seed knowledge execution boundary: %v", err)
		}
	}
	task.Status = core.TaskRunning
	validatorCalled := false
	if _, _, err := store.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: string(knowledge.OrganizationID), EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		validatorCalled = true
		if len(selection.Knowledge) != 1 || selection.Knowledge[0].Record.KnowledgeID != knowledge.KnowledgeID || selection.Knowledge[0].Record.Version != knowledge.Version {
			return core.ExecutionContextManifest{}, fmt.Errorf("unexpected transaction-bound knowledge: %+v", selection.Knowledge)
		}
		return core.ExecutionContextManifest{
			ExecutionID: "execution-" + task.ID + "-v2", AgentID: task.AssigneeID,
			AgentBlueprintVersion: task.AgentConfig.BlueprintVersion, ExecutionProfileVersion: task.AgentConfig.ProfileVersion,
			RuntimeAdapter: task.AgentConfig.RuntimeAdapter, Provider: "test", Model: "test", TaskID: task.ID,
			TaskContractVersion: task.TaskContractVersion, PromptVersion: "test", PolicyVersion: "v1",
			KnowledgeRefs:         []core.VersionedRef{{ID: string(knowledge.KnowledgeID), Version: strconv.Itoa(knowledge.Version), MaterializationState: core.MaterializedFull}},
			ContextBuilderVersion: "v3", ExecutionInputSHA256: core.FingerprintExecutionInput("test"), CreatedAt: selection.Started.CreatedAt,
		}, nil
	}); err != nil {
		t.Fatalf("start Agent execution with active knowledge: %v", err)
	}
	if !validatorCalled {
		t.Fatal("execution start did not materialize knowledge inside its transaction")
	}
}

func TestStoreRejectsRevokedValidatorAuthorityAfterJudgmentAdmission(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	candidate := knowledgeCandidate("k-revoked", "org-1", evidence.EventID)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	taskID := core.ID("task-revoked-validator")
	lease := core.CapabilityLease{
		ID:           "lease-revoked-validator",
		ActorID:      "reviewer-revoked",
		ActorKind:    core.PrincipalHuman,
		Action:       "knowledge.validate",
		Resource:     string(candidate.KnowledgeID),
		Scope:        string(candidate.OrganizationID),
		OriginTaskID: taskID,
	}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "runtime", string(taskID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	trace := authorizedKnowledgeValidationTrace(lease)
	judgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID:    "org-1",
		EventType:         "CAPABILITY_CHECKED",
		SourceActorID:     string(lease.ActorID),
		TaskID:            string(taskID),
		AuthorizationRefs: []string{string(lease.ID)},
		Payload:           trace,
	})
	if err != nil {
		t.Fatal(err)
	}
	statement := publishKnowledgeHumanStatement(t, ctx, gateway, "org-1", candidate.KnowledgeID, lease.ActorID, taskID, judgment.EventID, "work-revoked-validation")
	revokedAt := time.Now().UTC()
	lease.RevokedAt = &revokedAt
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_REVOKED", "runtime", string(taskID), nil, nil, "capability_lease", string(lease.ID), 2, lease); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{judgment.EventID, statement.EventID}
	active.ValidatedBy = lease.ActorID
	active.ValidatedByKind = core.PrincipalHuman
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("revoked validator authority was accepted")
	}
}

func TestStoreBindsJudgmentToPriorLeaseAndFreezeState(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	candidate := knowledgeCandidate("k-judgment-order", "org-1", evidence.EventID)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	lease := core.CapabilityLease{
		ID: "lease-judgment-order", ActorID: "reviewer-1", ActorKind: core.PrincipalHuman,
		Action: "knowledge.validate", Resource: string(candidate.KnowledgeID), Scope: "org-1", OriginTaskID: "task-validation",
	}
	prematureJudgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: string(lease.ActorID), TaskID: string(lease.OriginTaskID),
		AuthorizationRefs: []string{string(lease.ID)}, Payload: authorizedKnowledgeValidationTrace(lease),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "runtime", string(lease.OriginTaskID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{prematureJudgment.EventID}
	active.ValidatedBy = lease.ActorID
	active.ValidatedByKind = lease.ActorKind
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("judgment recorded before its lease grant was accepted")
	}
	judgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: string(lease.ActorID), TaskID: string(lease.OriginTaskID),
		AuthorizationRefs: []string{string(lease.ID)}, Payload: authorizedKnowledgeValidationTrace(lease),
	})
	if err != nil {
		t.Fatal(err)
	}
	wrongTaskStatement := publishKnowledgeHumanStatement(t, ctx, gateway, "org-1", candidate.KnowledgeID, lease.ActorID, "task-other", judgment.EventID, "work-other")
	active.ValidationRefs = []string{judgment.EventID, wrongTaskStatement.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("knowledge judgment from a different task used task-scoped authorization")
	}
	frozenAt := time.Now().UTC()
	freeze := struct {
		OrganizationID core.ID   `json:"organization_id"`
		Frozen         bool      `json:"frozen"`
		Reason         string    `json:"reason,omitempty"`
		UpdatedAt      time.Time `json:"updated_at"`
	}{OrganizationID: "org-1", Frozen: true, Reason: "incident", UpdatedAt: frozenAt}
	if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "runtime", string(lease.OriginTaskID), nil, nil, "organization_freeze", "org-1", 1, freeze); err != nil {
		t.Fatal(err)
	}
	frozenStatement := publishKnowledgeHumanStatement(t, ctx, gateway, "org-1", candidate.KnowledgeID, lease.ActorID, lease.OriginTaskID, judgment.EventID, "work-judgment-order")
	active.ValidationRefs = []string{judgment.EventID, frozenStatement.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("knowledge activation while the organization was frozen was accepted")
	}
	freeze.Frozen = false
	freeze.UpdatedAt = time.Now().UTC()
	if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "runtime", string(lease.OriginTaskID), nil, nil, "organization_freeze", "org-1", 2, freeze); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("judgment emitted during a temporary freeze was accepted after unfreeze")
	}
	statement := publishKnowledgeHumanStatement(t, ctx, gateway, "org-1", candidate.KnowledgeID, lease.ActorID, lease.OriginTaskID, judgment.EventID, "work-judgment-order")
	active.ValidationRefs = []string{judgment.EventID, statement.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatalf("activation after an admitted unfreeze was rejected: %v", err)
	}
}

func TestStoreRejectsAgentCreatorWithoutDurableExecutionBinding(t *testing.T) {
	ctx := context.Background()
	_, gateway := newKnowledgeTestStore(t)
	seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	knowledgeID := core.ID("k-agent")
	title := "Rollback procedure"
	basis := core.KnowledgeBasisHumanInput
	applicability := ""
	proposal, err := gateway.PublishAgentDraft(ctx, "org-1", "agent-1", "execution-1", "agent-proposal-1", events.Draft{
		EventType: "KNOWLEDGE_PROPOSED",
		TaskID:    "task-1",
		Payload: events.KnowledgeProposedPayload{
			KnowledgeID: &knowledgeID, KnowledgeType: core.KnowledgeProcedure, Title: &title,
			Content: "Verify the rollback before applying it.", BasisType: &basis, Applicability: &applicability,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := knowledgeCandidate(knowledgeID, "org-1", proposal.EventID)
	candidate.CreatedBy = "agent-1"
	candidate.CreatedByKind = core.PrincipalAgent
	service := New(gateway)
	if _, err := service.Propose(ctx, candidate); err == nil {
		t.Fatal("internal Agent proposal without a durable execution was accepted")
	}
	misclassified := knowledgeCandidate("k-agent-misclassified", "org-1", proposal.EventID)
	misclassified.CreatedBy = "agent-1"
	misclassified.CreatedByKind = core.PrincipalExternalAgent
	if _, err := service.Propose(ctx, misclassified); err == nil {
		t.Fatal("internal Agent proposal was relabeled as an external A2A actor")
	}
}

func authorizedKnowledgeValidationTrace(lease core.CapabilityLease) core.AuthorizationTrace {
	return core.AuthorizationTrace{
		Allowed:   true,
		LeaseID:   lease.ID,
		ActorID:   lease.ActorID,
		ActorKind: lease.ActorKind,
		TaskID:    lease.OriginTaskID,
		Action:    lease.Action,
		Resource:  lease.Resource,
		Scope:     lease.Scope,
		Reason:    "exact capability lease matched",
	}
}

func TestStoreTerminalRevisionRemovesKnowledgeFromRetrieval(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	candidate := knowledgeCandidate("k-1", "org-1", evidence.EventID)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatal(err)
	}
	superseded := active
	superseded.Version = 3
	superseded.Status = core.KnowledgeSuperseded
	superseded.SupersedesVersion = integerPointer(2)
	if _, err := service.Supersede(ctx, superseded); err != nil {
		t.Fatal(err)
	}
	derived := knowledgeCandidate("k-derived", "org-1", evidence.EventID)
	derived.Basis = core.KnowledgeBasisDerived
	derived.DerivedKnowledgeRefs = []core.VersionedRef{{
		ID: string(active.KnowledgeID), Version: "2", MaterializationState: core.MaterializedFull,
	}}
	if _, err := service.Propose(ctx, derived); err == nil {
		t.Fatal("knowledge derived from a no-longer-current active revision was accepted")
	}
	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("terminal knowledge remained active: rows=%+v err=%v", rows, err)
	}
}

func TestStoreCanInvalidateDerivedKnowledgeAfterItsSourceBecomesStale(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	activate := func(candidate core.KnowledgeRecord) core.KnowledgeRecord {
		t.Helper()
		if _, err := service.Propose(ctx, candidate); err != nil {
			t.Fatalf("propose %s: %v", candidate.KnowledgeID, err)
		}
		active := candidate
		active.Version = 2
		active.Status = core.KnowledgeActive
		active.ValidationMethod = core.KnowledgeValidationDeterministic
		active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
		active.ValidatedBy = "runtime"
		active.ValidatedByKind = core.PrincipalRuntime
		verifiedAt := time.Now().UTC()
		active.LastVerifiedAt = &verifiedAt
		active.SupersedesVersion = integerPointer(1)
		if _, err := service.Activate(ctx, active); err != nil {
			t.Fatalf("activate %s: %v", active.KnowledgeID, err)
		}
		return active
	}

	source := activate(knowledgeCandidate("k-source", "org-1", evidence.EventID))
	dependentCandidate := knowledgeCandidate("k-dependent", "org-1", evidence.EventID)
	dependentCandidate.Basis = core.KnowledgeBasisDerived
	dependentCandidate.DerivedKnowledgeRefs = []core.VersionedRef{{
		ID: string(source.KnowledgeID), Version: "2", MaterializationState: core.MaterializedFull,
	}}
	predated := dependentCandidate
	predated.KnowledgeID = "k-dependent-predated"
	predated.CreatedAt = source.CreatedAt
	if _, err := service.Propose(ctx, predated); err == nil {
		t.Fatal("derived knowledge claimed creation before its source activation")
	}
	dependent := activate(dependentCandidate)

	staleSource := source
	staleSource.Version = 3
	staleSource.Status = core.KnowledgeStale
	staleSource.SupersedesVersion = integerPointer(2)
	if _, err := service.MarkStale(ctx, staleSource); err != nil {
		t.Fatalf("mark source stale: %v", err)
	}
	if rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "rollback", 10); err != nil || len(rows) != 0 {
		t.Fatalf("derived knowledge with stale source remained searchable: rows=%+v err=%v", rows, err)
	}
	staleDependent := dependent
	staleDependent.Version = 3
	staleDependent.Status = core.KnowledgeStale
	staleDependent.SupersedesVersion = integerPointer(2)
	if _, err := service.MarkStale(ctx, staleDependent); err != nil {
		t.Fatalf("mark dependent stale after source invalidation: %v", err)
	}
}

func TestStoreRejectsDerivedKnowledgeScopeWidening(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	now := time.Now().UTC()
	blueprint := core.AgentBlueprint{
		ID: "blueprint-source", OrganizationID: "org-1", Version: "v1", Role: "source",
		OperatingInstructions: "Produce scoped knowledge.", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now,
	}
	profile := core.ExecutionProfile{
		ID: "profile-source", OrganizationID: "org-1", Version: "v1", ModelProvider: "fake", Model: "model",
		PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now,
	}
	agent := core.Agent{
		ID: "agent-source", OrganizationID: "org-1", BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version,
		ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "fake", Status: "ACTIVE",
	}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "scope-setup"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "scope-setup"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "scope-setup"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
	} {
		if _, err := gateway.PublishProjection(ctx, draft); err != nil {
			t.Fatalf("seed scoped knowledge Agent: %v", err)
		}
	}
	service := New(gateway)
	source := knowledgeCandidate("k-agent-source", "org-1", evidence.EventID)
	source.Scope = core.KnowledgeScopeAgent
	source.ScopeID = agent.ID
	if _, err := service.Propose(ctx, source); err != nil {
		t.Fatal(err)
	}
	active := source
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, source.KnowledgeID, 1).EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatal(err)
	}
	derived := knowledgeCandidate("k-organization-derived", "org-1", evidence.EventID)
	derived.Basis = core.KnowledgeBasisDerived
	derived.DerivedKnowledgeRefs = []core.VersionedRef{{ID: string(active.KnowledgeID), Version: "2", MaterializationState: core.MaterializedFull}}
	derived.CreatedAt = time.Now().UTC()
	if _, err := service.Propose(ctx, derived); err == nil || !strings.Contains(err.Error(), "scope exceeds") {
		t.Fatalf("Agent-scoped source widened to Organization knowledge: %v", err)
	}
}

func TestStoreRevisesActiveKnowledgeThroughCandidateReview(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	candidate := knowledgeCandidate("k-1", "org-1", evidence.EventID)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatal(err)
	}
	correctionEvidence, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"finding": "procedure changed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	corrected := active
	corrected.Version = 3
	corrected.Status = core.KnowledgeCandidate
	corrected.Title = "Corrected rollback procedure"
	corrected.Content = "Verify the corrected rollback before applying it."
	corrected.ProvenanceEventRefs = []string{correctionEvidence.EventID}
	corrected.CreatedBy = "runtime"
	corrected.CreatedByKind = core.PrincipalRuntime
	corrected.CreatedAt = time.Now().UTC()
	corrected.ValidationMethod = core.KnowledgeValidationUnvalidated
	corrected.ValidationRefs = nil
	corrected.ValidatedBy = ""
	corrected.ValidatedByKind = ""
	corrected.LastVerifiedAt = nil
	corrected.SupersedesVersion = integerPointer(2)
	if _, err := service.Propose(ctx, corrected); err != nil {
		t.Fatalf("propose corrected knowledge: %v", err)
	}
	if rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "corrected", 10); err != nil || len(rows) != 0 {
		t.Fatalf("unvalidated correction entered active retrieval: rows=%+v err=%v", rows, err)
	}
	validation := appendKnowledgeValidation(t, ctx, store, gateway, corrected.KnowledgeID, 3)
	reactivated := corrected
	reactivated.Version = 4
	reactivated.Status = core.KnowledgeActive
	reactivated.ValidationMethod = core.KnowledgeValidationDeterministic
	reactivated.ValidationRefs = []string{validation.EventID}
	reactivated.ValidatedBy = "runtime"
	reactivated.ValidatedByKind = core.PrincipalRuntime
	verifiedAt = time.Now().UTC()
	reactivated.LastVerifiedAt = &verifiedAt
	reactivated.SupersedesVersion = integerPointer(3)
	if _, err := service.Activate(ctx, reactivated); err != nil {
		t.Fatalf("activate corrected knowledge: %v", err)
	}
}

func TestStoreSupportsFailClosedStaleAndQuarantineTransitions(t *testing.T) {
	for _, test := range []struct {
		name       string
		fromActive bool
		status     core.KnowledgeStatus
		eventType  string
		transition func(*Store, context.Context, core.KnowledgeRecord) (events.Event, error)
	}{
		{name: "stale", fromActive: true, status: core.KnowledgeStale, eventType: "KNOWLEDGE_STALE", transition: (*Store).MarkStale},
		{name: "quarantine candidate", status: core.KnowledgeQuarantined, eventType: "KNOWLEDGE_QUARANTINED", transition: (*Store).Quarantine},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, gateway := newKnowledgeTestStore(t)
			evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
			service := New(gateway)
			candidate := knowledgeCandidate("k-1", "org-1", evidence.EventID)
			if _, err := service.Propose(ctx, candidate); err != nil {
				t.Fatal(err)
			}
			prior := candidate
			if test.fromActive {
				prior.Version = 2
				prior.Status = core.KnowledgeActive
				prior.ValidationMethod = core.KnowledgeValidationDeterministic
				prior.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
				prior.ValidatedBy = "runtime"
				prior.ValidatedByKind = core.PrincipalRuntime
				verifiedAt := time.Now().UTC()
				prior.LastVerifiedAt = &verifiedAt
				prior.SupersedesVersion = integerPointer(1)
				if _, err := service.Activate(ctx, prior); err != nil {
					t.Fatal(err)
				}
			}
			next := prior
			next.Version++
			next.Status = test.status
			next.SupersedesVersion = integerPointer(prior.Version)
			if !test.fromActive {
				mutated := next
				mutated.ValidationMethod = core.KnowledgeValidationDeterministic
				mutated.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
				mutated.ValidatedBy = "runtime"
				mutated.ValidatedByKind = core.PrincipalRuntime
				verifiedAt := time.Now().UTC()
				mutated.LastVerifiedAt = &verifiedAt
				if _, err := test.transition(service, ctx, mutated); err == nil {
					t.Fatal("candidate quarantine added validation authority")
				}
			}
			transition, err := test.transition(service, ctx, next)
			if err != nil {
				t.Fatalf("transition knowledge: %v", err)
			}
			if transition.EventType != test.eventType {
				t.Fatalf("transition event type=%q want %q", transition.EventType, test.eventType)
			}
		})
	}
}

func TestPatternCandidateRequiresDistinctConcreteEvents(t *testing.T) {
	for _, refs := range [][]string{{"1", "2"}, {"1", "1", "1"}, {"1", " 1", "1 "}, {"1", "2", ""}} {
		if PatternCandidate(refs) == nil {
			t.Fatalf("invalid occurrence set accepted: %#v", refs)
		}
	}
	if err := PatternCandidate([]string{"1", "2", "3"}); err != nil {
		t.Fatalf("three distinct occurrences rejected: %v", err)
	}
}

func TestRepeatedPatternActivationRequiresEvidenceAfterProposal(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	appendEvidence := func(label string) events.Event {
		t.Helper()
		event, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"label": label},
		})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	occurrenceEvents := []events.Event{appendEvidence("one"), appendEvidence("two"), appendEvidence("three")}
	occurrences := []string{occurrenceEvents[0].EventID, occurrenceEvents[1].EventID, occurrenceEvents[2].EventID}
	olderValidation := appendEvidence("older-validation")
	candidate := knowledgeCandidate("k-pattern", "org-1", occurrences[0])
	candidate.Basis = core.KnowledgeBasisRepeatedPattern
	candidate.ProvenanceEventRefs = append([]string(nil), occurrences...)
	candidate.OccurrenceEventRefs = append([]string(nil), occurrences...)
	service := New(gateway)
	premature := candidate
	premature.KnowledgeID = "k-pattern-premature"
	premature.CreatedAt = occurrenceEvents[len(occurrenceEvents)-1].CreatedAt.Add(-time.Nanosecond)
	if _, err := service.Propose(ctx, premature); err == nil {
		t.Fatal("candidate created before its occurrence evidence was accepted")
	}
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{olderValidation.EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("evidence admitted before the proposal activated repeated-pattern knowledge")
	}
	active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatalf("subsequent validation evidence was rejected: %v", err)
	}
}

func TestDeterministicActivationRequiresEvidenceAfterProposal(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	candidate := knowledgeCandidate("k-deterministic-order", "org-1", evidence.EventID)
	service := New(gateway)
	if _, err := service.Propose(ctx, candidate); err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ValidationMethod = core.KnowledgeValidationDeterministic
	active.ValidationRefs = []string{evidence.EventID}
	active.ValidatedBy = "runtime"
	active.ValidatedByKind = core.PrincipalRuntime
	verifiedAt := time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	active.SupersedesVersion = integerPointer(1)
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("pre-proposal deterministic evidence activated knowledge")
	}
	unrelated, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "knowledge-" + string(candidate.KnowledgeID), Payload: map[string]string{"note": "not validation"},
	})
	if err != nil {
		t.Fatal(err)
	}
	active.ValidationRefs = []string{unrelated.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("unrelated post-proposal event activated deterministic knowledge")
	}

	missingOutcome, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "KNOWLEDGE_VALIDATION_RECORDED", SourceActorID: "runtime",
		SourceExecutionID: "execution-missing", TaskID: "task-missing", CorrelationID: "work-missing",
		Payload: events.KnowledgeDeterministicValidationPayload{
			KnowledgeID: candidate.KnowledgeID, CandidateVersion: 1, OutcomeEventRef: "outcome-missing", ArtifactRefs: []string{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	active.ValidationRefs = []string{missingOutcome.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("deterministic validation referencing a nonexistent outcome was accepted")
	}

	startedAt := time.Now().UTC()
	syntheticOutcome, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime",
		SourceExecutionID: "execution-synthetic", TaskID: "task-synthetic", CorrelationID: "work-synthetic",
		Payload: core.ToolOutcome{
			ToolInvocationID: "synthetic-validation", ToolID: "builtin.echo", ToolVersion: "1", Status: core.OutcomeSucceeded,
			ObservedEffect: "validated", PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable,
			StartedAt: startedAt, FinishedAt: startedAt,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	syntheticBinding, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "KNOWLEDGE_VALIDATION_RECORDED", SourceActorID: "runtime",
		SourceExecutionID: "execution-synthetic", TaskID: "task-synthetic", CorrelationID: "work-synthetic",
		Payload: events.KnowledgeDeterministicValidationPayload{
			KnowledgeID: candidate.KnowledgeID, CandidateVersion: 1, OutcomeEventRef: syntheticOutcome.EventID, ArtifactRefs: []string{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	active.ValidationRefs = []string{syntheticBinding.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("synthetic tool outcome without an admitted deterministic execution activated knowledge")
	}

	wrongCandidate, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "KNOWLEDGE_VALIDATION_RECORDED", SourceActorID: "runtime",
		SourceExecutionID: "execution-synthetic", TaskID: "task-synthetic", CorrelationID: "work-synthetic",
		Payload: events.KnowledgeDeterministicValidationPayload{
			KnowledgeID: "knowledge-other", CandidateVersion: 2, OutcomeEventRef: syntheticOutcome.EventID, ArtifactRefs: []string{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	active.ValidationRefs = []string{wrongCandidate.EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("deterministic validation for a different candidate and version was accepted")
	}

	active.ValidationRefs = []string{appendClosedKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err == nil {
		t.Fatal("deterministic validation recorded after its execution closed was accepted")
	}

	active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
	verifiedAt = time.Now().UTC()
	active.LastVerifiedAt = &verifiedAt
	if _, err := service.Activate(ctx, active); err != nil {
		t.Fatalf("post-proposal deterministic evidence was rejected: %v", err)
	}
}

func TestSearchFiltersBeforeApplyingResultLimit(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	for index := 0; index < 257; index++ {
		candidate := knowledgeCandidate(core.ID(fmt.Sprintf("k-%03d", index)), "org-1", evidence.EventID)
		if index == 256 {
			candidate.Content = "needle-only-last"
		}
		if _, err := service.Propose(ctx, candidate); err != nil {
			t.Fatalf("propose knowledge %d: %v", index, err)
		}
		active := candidate
		active.Version = 2
		active.Status = core.KnowledgeActive
		active.ValidationMethod = core.KnowledgeValidationDeterministic
		active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
		active.ValidatedBy = "runtime"
		active.ValidatedByKind = core.PrincipalRuntime
		verifiedAt := time.Now().UTC()
		active.LastVerifiedAt = &verifiedAt
		active.SupersedesVersion = integerPointer(1)
		if _, err := service.Activate(ctx, active); err != nil {
			t.Fatalf("activate knowledge %d: %v", index, err)
		}
	}
	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "needle-only-last", 1)
	if err != nil || len(rows) != 1 || rows[0].KnowledgeID != "k-256" {
		t.Fatalf("post-filter result window lost the match: rows=%+v err=%v", rows, err)
	}
}

func TestSearchRanksNewestActivationFirst(t *testing.T) {
	ctx := context.Background()
	store, gateway := newKnowledgeTestStore(t)
	evidence := seedKnowledgeOrganization(t, ctx, gateway, "org-1")
	service := New(gateway)
	for _, id := range []core.ID{"older", "newer"} {
		candidate := knowledgeCandidate(id, "org-1", evidence.EventID)
		candidate.Content = "shared retrieval phrase"
		if _, err := service.Propose(ctx, candidate); err != nil {
			t.Fatal(err)
		}
		active := candidate
		active.Version = 2
		active.Status = core.KnowledgeActive
		active.ValidationMethod = core.KnowledgeValidationDeterministic
		active.ValidationRefs = []string{appendKnowledgeValidation(t, ctx, store, gateway, candidate.KnowledgeID, 1).EventID}
		active.ValidatedBy = "runtime"
		active.ValidatedByKind = core.PrincipalRuntime
		verifiedAt := time.Now().UTC()
		active.LastVerifiedAt = &verifiedAt
		active.SupersedesVersion = integerPointer(1)
		if _, err := service.Activate(ctx, active); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := service.Search(ctx, "org-1", core.KnowledgeScopeOrganization, "org-1", "shared retrieval phrase", 1)
	if err != nil || len(rows) != 1 || rows[0].KnowledgeID != "newer" {
		t.Fatalf("bounded search did not prefer newest activation: rows=%+v err=%v", rows, err)
	}
}

func newKnowledgeTestStore(t *testing.T) (*ledger.SQLite, *events.Gateway) {
	t.Helper()
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close ledger: %v", err)
		}
	})
	return store, events.NewGateway(store)
}

func seedKnowledgeOrganization(t *testing.T, ctx context.Context, gateway *events.Gateway, organizationID core.ID) events.Event {
	t.Helper()
	createdAt := time.Date(2026, time.August, 26, 0, 0, 0, 0, time.UTC)
	event, err := gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(organizationID),
			EventType:      "ORGANIZATION_CREATED",
			SourceActorID:  "runtime",
			CorrelationID:  "setup-" + string(organizationID),
		},
		ProjectionKind: "organization",
		RecordID:       string(organizationID),
		Version:        1,
		Value: core.Organization{
			ID:            organizationID,
			Name:          "Test Organization",
			PolicyVersion: "policy-1",
			CreatedAt:     createdAt,
		},
	})
	if err != nil {
		t.Fatalf("seed organization %s: %v", organizationID, err)
	}
	return event
}

var knowledgeValidationSequence atomic.Uint64

func appendKnowledgeValidation(t *testing.T, ctx context.Context, store *ledger.SQLite, gateway *events.Gateway, knowledgeID core.ID, candidateVersion int) events.Event {
	return appendKnowledgeValidationLifecycle(t, ctx, store, gateway, knowledgeID, candidateVersion, false)
}

func appendClosedKnowledgeValidation(t *testing.T, ctx context.Context, store *ledger.SQLite, gateway *events.Gateway, knowledgeID core.ID, candidateVersion int) events.Event {
	return appendKnowledgeValidationLifecycle(t, ctx, store, gateway, knowledgeID, candidateVersion, true)
}

func appendKnowledgeValidationLifecycle(t *testing.T, ctx context.Context, store *ledger.SQLite, gateway *events.Gateway, knowledgeID core.ID, candidateVersion int, closeBeforeValidation bool) events.Event {
	t.Helper()
	suffix := fmt.Sprintf("%s-%d", knowledgeID, knowledgeValidationSequence.Add(1))
	correlationID := "work-validation-" + suffix
	now := time.Now().UTC()
	intent := core.Intent{ID: core.ID("intent-validation-" + suffix), OrganizationID: "org-1", OriginalInstruction: "echo validated", NormalizedObjective: "echo validated", CreatedAt: now}
	work := core.Work{ID: core.ID("work-validation-" + suffix), IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{
		ID: core.ID("task-validation-" + suffix), WorkID: work.ID, Description: "echo validated", ExecutionKind: core.ExecutionDeterministic,
		ModelInferencePolicy: core.InferenceForbidden, RuntimeHandlerRef: "builtin.echo", TaskContractVersion: "1", Status: core.TaskPending,
	}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
	} {
		if _, err := gateway.PublishProjection(ctx, draft); err != nil {
			t.Fatalf("append deterministic validation parent: %v", err)
		}
	}
	running := task
	running.Status = core.TaskRunning
	if _, _, err := store.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID, Payload: events.ExecutionStartDetail{InboxCutoffSequence: 0}},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: running,
	}, nil, nil); err != nil {
		t.Fatalf("start deterministic knowledge validation: %v", err)
	}
	executionID := core.ID(fmt.Sprintf("execution-%s-v2", task.ID))
	outcome := core.ToolOutcome{
		ToolInvocationID: core.ID("validation-" + suffix), ToolID: "builtin.echo", ToolVersion: "1", ObservedEffect: "validated",
		Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable,
		StartedAt: now, FinishedAt: now,
	}
	outcomeEvent, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: string(executionID),
		TaskID: string(task.ID), CorrelationID: correlationID, Payload: outcome,
	})
	if err != nil {
		t.Fatalf("append validation evidence: %v", err)
	}
	if closeBeforeValidation {
		if _, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
			OrganizationID: "org-1", EventType: "EXECUTION_FINISHED", SourceActorID: "runtime", SourceExecutionID: string(executionID),
			TaskID: string(task.ID), CorrelationID: correlationID, Payload: map[string]string{"status": "finished"},
		}); err != nil {
			t.Fatalf("close deterministic knowledge validation execution: %v", err)
		}
	}
	validation, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "KNOWLEDGE_VALIDATION_RECORDED", SourceActorID: "runtime", SourceExecutionID: string(executionID),
		TaskID: string(task.ID), CorrelationID: correlationID,
		Payload: events.KnowledgeDeterministicValidationPayload{KnowledgeID: knowledgeID, CandidateVersion: candidateVersion, OutcomeEventRef: outcomeEvent.EventID, ArtifactRefs: []string{}},
	})
	if err != nil {
		t.Fatalf("bind deterministic validation to knowledge: %v", err)
	}
	return validation
}

func publishKnowledgeHumanStatement(t *testing.T, ctx context.Context, gateway *events.Gateway, organizationID, knowledgeID, actorID, taskID core.ID, capabilityCheckEventID, correlationID string) events.Event {
	t.Helper()
	event, err := gateway.PublishTrusted(ctx, events.TrustedDraft{
		OrganizationID: string(organizationID), EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: string(actorID),
		TaskID: string(taskID), CorrelationID: correlationID,
		Payload: events.KnowledgeJudgmentPayload{
			KnowledgeID: knowledgeID, CandidateVersion: 1, Decision: events.KnowledgeJudgmentValidated,
			Statement: "I independently validate this knowledge candidate.", CapabilityCheckEventID: capabilityCheckEventID,
			SourcePrincipalID: string(actorID), SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{},
		},
	})
	if err != nil {
		t.Fatalf("publish knowledge judgment statement: %v", err)
	}
	return event
}

func knowledgeCandidate(id, organizationID core.ID, evidenceRef string) core.KnowledgeRecord {
	return core.KnowledgeRecord{
		KnowledgeID:         id,
		OrganizationID:      organizationID,
		Version:             1,
		Type:                core.KnowledgeProcedure,
		Scope:               core.KnowledgeScopeOrganization,
		ScopeID:             organizationID,
		Tags:                []string{"recovery"},
		Status:              core.KnowledgeCandidate,
		Title:               "Rollback procedure",
		Content:             "Verify the rollback before applying it.",
		Basis:               core.KnowledgeBasisHumanInput,
		ProvenanceEventRefs: []string{evidenceRef},
		CreatedBy:           "runtime",
		CreatedByKind:       core.PrincipalRuntime,
		CreatedAt:           time.Now().UTC(),
		ValidationMethod:    core.KnowledgeValidationUnvalidated,
	}
}

func integerPointer(value int) *int { return &value }
