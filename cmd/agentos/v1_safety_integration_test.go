package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/audit"
	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/knowledge"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/secrets"
)

type conformanceEffectAdapter struct {
	calls int
}

func (a *conformanceEffectAdapter) Apply(context.Context, core.EffectObligation) ([]string, error) {
	a.calls++
	return []string{"receipt-v1"}, nil
}

type conformanceNotifier struct{}

func (conformanceNotifier) Notify(context.Context, core.HumanApproval) error { return nil }

type v1WireContract struct {
	Manifest          core.ExecutionContextManifest `json:"manifest"`
	Skill             core.Skill                    `json:"skill"`
	KnowledgeStatuses []core.KnowledgeStatus        `json:"knowledge_statuses"`
	EffectStatus      core.EffectStatus             `json:"effect_status"`
}

func TestV1SafetyServicesEnforceAndRecordContracts(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	t.Setenv("AGENTOS_TEST_PROVIDER_TOKEN", "test-secret")
	var secretSource secrets.Source = secrets.Environment{Prefix: "AGENTOS_TEST_"}
	secret, err := secretSource.Resolve(ctx, secrets.Ref("PROVIDER_TOKEN"))
	if err != nil || secret != "test-secret" {
		t.Fatalf("resolve secret by reference: value=%q err=%v", secret, err)
	}

	lease := core.CapabilityLease{
		ID: "lease-1", ActorID: "actor-1", OriginTaskID: "task-1",
		Action: "send", Resource: "customer-1", Scope: "org-1",
	}
	trace := authority.Check(now, "actor-1", "task-1", "send", "customer-1", "org-1", []core.CapabilityLease{lease}, false)
	if !trace.Allowed || trace.LeaseID != lease.ID {
		t.Fatalf("expected exact capability lease to authorize action: %+v", trace)
	}
	if err := l.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "human-1", "task-1", []string{"approval-capability-1"}, nil, "capability_lease", "lease-1", 1, lease); err != nil {
		t.Fatalf("persist capability lease: %v", err)
	}

	manager := inference.Manager{Pools: []inference.Pool{
		{ID: "subscription", Mode: inference.Subscription, Available: false},
		{ID: "metered", Mode: inference.MeteredAPI, Available: false},
		{
			ID: "local", Provider: "fake", Mode: inference.Local,
			AllowedModels: []string{"fake-model/v1"}, Available: true, ConcurrencyLimit: 1,
			Snapshot:          inference.UsageSnapshot{Source: "local", ObservedAt: now, Confidence: 1, Remaining: 10, Unit: "requests"},
			ContinuityReserve: 1,
		},
	}}
	selection, err := manager.Select(inference.Request{RequiredModel: "fake-model/v1", EstimatedUsage: 1})
	if err != nil || selection.PoolID != "local" || selection.Provider != "fake" || selection.Model != "fake-model/v1" || selection.Snapshot.Source != "local" {
		t.Fatalf("select reserve-safe inference pool: selection=%+v err=%v", selection, err)
	}

	record := core.KnowledgeRecord{
		KnowledgeID: "knowledge-1", Version: 1, Type: "LESSON", Scope: "ORGANIZATION",
		Status: core.KnowledgeCandidate, Content: "Require exact authority before effects.",
		ProvenanceEventRefs: []string{"source-event-1"}, CreatedBy: "human-1", CreatedAt: now,
	}
	knowledgeStore := knowledge.New(l)
	if err := knowledgeStore.Propose(ctx, record); err != nil {
		t.Fatalf("propose versioned knowledge: %v", err)
	}
	matches, err := knowledgeStore.Search(ctx, "ORGANIZATION", "authority")
	if err != nil {
		t.Fatalf("search versioned knowledge: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("candidate knowledge must not be returned as active: %+v", matches)
	}

	expiresAt := now.Add(time.Hour)
	approvalService := approvals.New(l, conformanceNotifier{}, approvals.StaticAuthorizer{{OrganizationID: "org-1", HumanID: "human-1", Boundary: core.BoundaryPublicExternal, Risk: "HIGH"}})
	obligation := core.EffectObligation{
		ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "send", Resource: "customer-1", Scope: "org-1",
		ConsequenceBoundary: core.BoundaryPublicExternal,
		Descriptor:          "send greeting", AuthorizationRefs: []string{"lease-1"},
		ApprovalRef: "approval-1", IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"body": "hello"},
	}
	fingerprint, err := effects.Fingerprint(obligation)
	if err != nil {
		t.Fatalf("fingerprint effect: %v", err)
	}
	obligation.EffectFingerprint = fingerprint
	adapter := &conformanceEffectAdapter{}
	coordinator := effects.New(l, adapter, approvalService)
	if _, err := coordinator.Prepare(ctx, obligation); err != nil {
		t.Fatalf("prepare protected effect obligation: %v", err)
	}
	approval, err := approvalService.Request(ctx, core.HumanApproval{
		ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "send", Resource: "customer-1",
		Boundary: core.BoundaryPublicExternal, Risk: "HIGH", Urgency: "MEDIUM",
		EffectFingerprint: fingerprint, ExpiresAt: &expiresAt, SingleUse: true,
	})
	if err != nil {
		t.Fatalf("request exact effect approval: %v", err)
	}
	approval, err = approvalService.Acknowledge(ctx, approval.ID, "human-1")
	if err != nil {
		t.Fatalf("acknowledge exact effect approval: %v", err)
	}
	approval, err = approvalService.BeginDecision(ctx, approval.ID, "human-1")
	if err != nil {
		t.Fatalf("begin exact effect decision: %v", err)
	}
	approval, err = approvalService.Decide(ctx, approvals.Decision{ApprovalID: approval.ID, HumanID: "human-1", EffectFingerprint: fingerprint, Approve: true})
	if err != nil || approval.Status != core.ApprovalApproved {
		t.Fatalf("decide exact effect approval: approval=%+v err=%v", approval, err)
	}
	result, err := coordinator.Execute(ctx, obligation)
	if err != nil || result.Status != core.EffectConfirmed || len(result.ConfirmationEvidenceRefs) != 1 {
		t.Fatalf("execute approved persisted effect: result=%+v err=%v", result, err)
	}
	replay := obligation
	replay.ID = "effect-2"
	replay.IdempotencyKey = "effect-key-2"
	replay.EffectFingerprint, err = effects.Fingerprint(replay)
	if err != nil {
		t.Fatalf("fingerprint replay effect: %v", err)
	}
	if _, err := coordinator.Prepare(ctx, replay); err != nil {
		t.Fatalf("prepare replay obligation: %v", err)
	}
	if _, err := coordinator.Execute(ctx, replay); err == nil {
		t.Fatal("expected reuse of single-use approval to fail closed")
	}
	if adapter.calls != 1 {
		t.Fatalf("external adapter called %d times; want 1", adapter.calls)
	}

	findings, err := audit.New(l).Run(ctx)
	if err != nil {
		t.Fatalf("audit ledger: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("unexpected audit findings: %+v", findings)
	}

	assertV1WireContractRoundTrip(t, now)
}

func assertV1WireContractRoundTrip(t *testing.T, now time.Time) {
	t.Helper()
	want := v1WireContract{
		Manifest: core.ExecutionContextManifest{
			ExecutionID: "execution-1", AgentID: "agent-1", TaskID: "task-1",
			ExecutionProfileVersion: "v1", TaskContractVersion: "v1", ContextBuilderVersion: "v1", CreatedAt: now,
			KnowledgeRefs: []core.VersionedRef{
				{ID: "full", Version: "1", MaterializationState: core.MaterializedFull},
				{ID: "summary", Version: "1", MaterializationState: core.MaterializedSummary},
				{ID: "reference", Version: "1", MaterializationState: core.MaterializedReferenceOnly},
				{ID: "omitted", Version: "1", MaterializationState: core.MaterializedOmitted},
				{ID: "unavailable", Version: "1", MaterializationState: core.MaterializedUnavailable},
			},
		},
		Skill: core.Skill{
			SkillID: "skill-1", Version: 1, Name: "safe-effect", Scope: "ORGANIZATION",
			Status: core.KnowledgeCandidate, InstructionsRef: "artifact://skill-1",
			ProvenanceEventRefs: []string{"source-event-1"}, CreatedBy: "human-1",
		},
		KnowledgeStatuses: []core.KnowledgeStatus{
			core.KnowledgeSuperseded,
			core.KnowledgeStale,
			core.KnowledgeQuarantined,
		},
		EffectStatus: core.EffectCancelled,
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal V1 wire contract: %v", err)
	}
	var got v1WireContract
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal V1 wire contract: %v", err)
	}
	if len(got.Manifest.KnowledgeRefs) != 5 || got.Manifest.KnowledgeRefs[1].MaterializationState != core.MaterializedSummary {
		t.Fatalf("materialization states did not round-trip: %+v", got.Manifest.KnowledgeRefs)
	}
	if got.Skill.SkillID != want.Skill.SkillID || len(got.KnowledgeStatuses) != 3 || got.EffectStatus != core.EffectCancelled {
		t.Fatalf("V1 lifecycle contracts did not round-trip: %+v", got)
	}
}
