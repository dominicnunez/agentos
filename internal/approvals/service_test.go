package approvals_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

type notifier struct {
	calls int
	err   error
}

func (n *notifier) Notify(context.Context, core.HumanApproval) error {
	n.calls++
	return n.err
}

type effectAdapter struct{ calls int }

func (a *effectAdapter) Apply(context.Context, core.EffectObligation) ([]string, error) {
	a.calls++
	return []string{"receipt-1"}, nil
}

type latestInboxStore struct{ bodies [][]byte }

func (s latestInboxStore) AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error {
	return errors.New("append is not supported")
}

func (s latestInboxStore) Records(context.Context, string, string) ([][]byte, error) { return nil, nil }

func (s latestInboxStore) PendingApprovalRecords(context.Context, string, time.Time, int) ([][]byte, error) {
	return s.bodies, nil
}

type legacyApprovalStore struct{ records map[string][][]byte }

func (s *legacyApprovalStore) AppendRecord(_ context.Context, _, _, _, _ string, _, _ []string, kind, id string, _ int, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	key := kind + "/" + id
	s.records[key] = append(s.records[key], body)
	return nil
}

func (s *legacyApprovalStore) Records(_ context.Context, kind, id string) ([][]byte, error) {
	return s.records[kind+"/"+id], nil
}

func TestApprovalInboxLimitExcludesHistoricalRecords(t *testing.T) {
	bodies := make([][]byte, 0, 1001)
	for index := range 1001 {
		body, err := json.Marshal(core.HumanApproval{ID: core.ID(fmt.Sprintf("approval-%d", index)), Status: core.ApprovalDenied})
		if err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, body)
	}
	contexts, err := approvals.New(latestInboxStore{bodies: bodies}, nil, nil).PendingDecisionContexts(t.Context(), "org-1", "approver")
	if err == nil || contexts != nil {
		t.Fatalf("oversized pending approval projection was accepted: contexts=%d err=%v", len(contexts), err)
	}
}

func TestApprovalWaitsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	n := &notifier{}
	authorizer := approvals.StaticAuthorizer{{OrganizationID: "org-1", HumanID: "human-approver", Boundary: core.BoundaryPublicExternal, Risk: "HIGH"}}
	service := approvals.New(l, n, authorizer)
	obligation := core.EffectObligation{
		ID:                  "effect-1",
		OrganizationID:      "org-1",
		TaskID:              "task-1",
		ActorID:             "actor-1",
		Action:              "send",
		Resource:            "customer-1",
		Scope:               "org-1",
		ConsequenceBoundary: core.BoundaryPublicExternal,
		Descriptor:          "send greeting",
		AuthorizationRefs:   []string{"lease-1"},
		ApprovalRef:         "approval-1",
		IdempotencyKey:      "effect-key-1",
		ReplayContext:       map[string]string{"body": "hello"},
	}
	fingerprint := setApprovalTestFingerprint(t, &obligation)
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "actor-1", ActorKind: obligation.ActorKind, OriginTaskID: "task-1", Action: "send", Resource: "customer-1", Scope: "org-1"}
	if err := l.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "human-approver", "task-1", nil, nil, "capability_lease", "lease-1", 1, lease); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	adapter := &effectAdapter{}
	coordinator := effects.New(l, adapter, service)
	if _, err := coordinator.Prepare(ctx, obligation); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	approval, err := service.Request(ctx, core.HumanApproval{
		ID:                 "approval-1",
		OrganizationID:     "org-1",
		TaskID:             "task-1",
		EffectObligationID: "effect-1",
		Action:             "send",
		Resource:           "customer-1",
		Boundary:           core.BoundaryPublicExternal,
		Risk:               "HIGH",
		Urgency:            "MEDIUM",
		EffectFingerprint:  fingerprint,
		SingleUse:          true,
	})
	if err != nil || approval.Status != core.ApprovalNotified || n.calls != 1 {
		_ = l.Close()
		t.Fatalf("request=%+v notify_calls=%d err=%v", approval, n.calls, err)
	}
	effectRows, err := l.Records(ctx, "effect", "effect-1")
	if err != nil || len(effectRows) != 1 {
		_ = l.Close()
		t.Fatalf("waiting effect was not durably prepared: rows=%d err=%v", len(effectRows), err)
	}
	assertEffectWaiting(t, ctx, coordinator, obligation, adapter)
	approval, err = service.Acknowledge(ctx, approval.ID, "human-approver")
	if err != nil || approval.Status != core.ApprovalAcknowledged || approval.DecisionAt != nil {
		_ = l.Close()
		t.Fatalf("acknowledgement became a decision: approval=%+v err=%v", approval, err)
	}
	assertEffectWaiting(t, ctx, coordinator, obligation, adapter)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	service = approvals.New(l, nil, authorizer)
	approval, err = service.Get(ctx, "approval-1")
	if err != nil || approval.Status != core.ApprovalAcknowledged {
		t.Fatalf("approval did not survive restart: approval=%+v err=%v", approval, err)
	}
	coordinator = effects.New(l, adapter, service)
	if _, err := service.BeginDecision(ctx, approval.ID, "external-agent-operator"); !errors.Is(err, approvals.ErrDecisionUnauthorized) {
		t.Fatalf("ordinary external-Agent identity entered decision state: %v", err)
	}
	wrongRiskService := approvals.New(l, nil, approvals.StaticAuthorizer{{OrganizationID: "org-1", HumanID: "human-approver", Boundary: core.BoundaryPublicExternal, Risk: "LOW"}})
	if _, err := wrongRiskService.BeginDecision(ctx, approval.ID, "human-approver"); !errors.Is(err, approvals.ErrDecisionUnauthorized) {
		t.Fatalf("risk-mismatched decision grant was accepted: %v", err)
	}
	approval, err = service.BeginDecision(ctx, approval.ID, "human-approver")
	if err != nil || approval.Status != core.ApprovalPendingDecision {
		t.Fatalf("begin decision=%+v err=%v", approval, err)
	}
	if _, err := service.Decide(ctx, approvals.Decision{ApprovalID: approval.ID, HumanID: "human-approver", EffectFingerprint: "different", Approve: true}); err == nil {
		t.Fatal("mismatched effect decision was accepted")
	}
	assertEffectWaiting(t, ctx, coordinator, obligation, adapter)
	approval, err = service.Decide(ctx, approvals.Decision{ApprovalID: approval.ID, HumanID: "human-approver", EffectFingerprint: fingerprint, Approve: true})
	if err != nil || approval.Status != core.ApprovalApproved || approval.DecidedBy != "human-approver" {
		t.Fatalf("decision=%+v err=%v", approval, err)
	}
	result, err := coordinator.Execute(ctx, obligation)
	if err != nil || result.Status != core.EffectConfirmed || adapter.calls != 1 {
		t.Fatalf("approved effect result=%+v calls=%d err=%v", result, adapter.calls, err)
	}
	terminal, err := service.ReadContext(ctx, approval.ID, "human-approver")
	if err != nil || terminal.Approval.Status != core.ApprovalApproved || terminal.Effect.Status != core.EffectConfirmed || terminal.Effect.EffectFingerprint != fingerprint {
		t.Fatalf("terminal approval context=%+v err=%v", terminal, err)
	}
	recent, err := service.RecentDecisionContexts(ctx, "org-1", "human-approver", 20)
	if err != nil || len(recent) != 1 || recent[0].Approval.ID != approval.ID || recent[0].Approval.Status != core.ApprovalApproved {
		t.Fatalf("recent approval decisions=%+v err=%v", recent, err)
	}
	if _, err := service.DecisionContext(ctx, approval.ID, "human-approver"); err == nil {
		t.Fatal("terminal effect remained eligible for mutation")
	}
	stream, err := l.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, stream, "APPROVAL_REQUESTED", "APPROVAL_NOTIFIED", "APPROVAL_ACKNOWLEDGED", "APPROVAL_DECISION_STARTED", "APPROVAL_DECIDED", "CAPABILITY_CHECKED", "APPROVAL_CONSUMED", "EFFECT_OBLIGATION_TRANSITIONED")
}

func TestNotificationFailureRemainsDurablyPending(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	failing := &notifier{err: fmt.Errorf("delivery unavailable")}
	service := approvals.New(l, failing, nil)
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "deploy", Resource: "agent-os", Scope: "org-1", ConsequenceBoundary: core.BoundaryDeployment, Descriptor: "deploy Agent OS", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1", IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"version": "1"}}
	fingerprint := setApprovalTestFingerprint(t, &obligation)
	if _, err := effects.New(l, &effectAdapter{}, service).Prepare(ctx, obligation); err != nil {
		_ = l.Close()
		t.Fatal(err)
	}
	approval, err := service.Request(ctx, core.HumanApproval{
		ID:                 "approval-1",
		OrganizationID:     "org-1",
		TaskID:             "task-1",
		EffectObligationID: "effect-1",
		Action:             "deploy",
		Resource:           "agent-os",
		Boundary:           core.BoundaryDeployment,
		Risk:               "HIGH",
		Urgency:            "MEDIUM",
		EffectFingerprint:  fingerprint,
	})
	if !errors.Is(err, approvals.ErrNotificationUnavailable) || approval.Status != core.ApprovalPending {
		_ = l.Close()
		t.Fatalf("notification failure=%+v err=%v", approval, err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l, err = ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	recoveredNotifier := &notifier{}
	service = approvals.New(l, recoveredNotifier, nil)
	approval, err = service.Get(ctx, "approval-1")
	if err != nil || approval.Status != core.ApprovalPending {
		t.Fatalf("pending approval after restart=%+v err=%v", approval, err)
	}
	approval, err = service.Notify(ctx, approval.ID)
	if err != nil || approval.Status != core.ApprovalNotified || recoveredNotifier.calls != 1 {
		t.Fatalf("notification retry=%+v calls=%d err=%v", approval, recoveredNotifier.calls, err)
	}
}

func TestApprovalRequestRequiresPreparedMatchingEffect(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	n := &notifier{}
	service := approvals.New(l, n, nil)
	_, err = service.Request(ctx, core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "missing-effect", Action: "deploy", Resource: "agent-os", Boundary: core.BoundaryDeployment, Risk: "HIGH", Urgency: "MEDIUM", EffectFingerprint: "fingerprint-1"})
	if err == nil || n.calls != 0 {
		t.Fatalf("approval without prepared effect was notified: calls=%d err=%v", n.calls, err)
	}
	rows, err := l.Records(ctx, "approval", "approval-1")
	if err != nil || len(rows) != 0 {
		t.Fatalf("approval without prepared effect was persisted: rows=%d err=%v", len(rows), err)
	}
}

func TestApprovalRequestRejectsUnknownRiskAndUrgency(t *testing.T) {
	for _, test := range []struct {
		name, risk, urgency string
	}{
		{name: "unknown risk", risk: "SEVERE", urgency: "HIGH"},
		{name: "unknown urgency", risk: "HIGH", urgency: "IMMEDIATE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := approvals.New(nil, nil, nil)
			_, err := service.Request(t.Context(), core.HumanApproval{
				ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1",
				Action: "deploy", Resource: "agent-os", Boundary: core.BoundaryDeployment,
				Risk: test.risk, Urgency: test.urgency, EffectFingerprint: "fingerprint-1",
			})
			if err == nil {
				t.Fatal("unknown decision level was accepted")
			}
		})
	}
}

func TestApprovalMutationsRevalidateAuthorityAndCurrentEffect(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	obligation := core.EffectObligation{
		ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "agent-1",
		Action: "deploy", Resource: "agent-os", Scope: "org-1", ConsequenceBoundary: core.BoundaryDeployment,
		Descriptor: "deploy exact release", ApprovalRef: "approval-1",
		IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"version": "1.0.0"}, Status: core.EffectPending,
	}
	fingerprint := setApprovalTestFingerprint(t, &obligation)
	if err := l.AppendRecord(ctx, "org-1", "EFFECT_OBLIGATION_PREPARED", "agent-1", "task-1", nil, nil, "effect", "effect-1", 1, obligation); err != nil {
		t.Fatal(err)
	}
	authorizer := approvals.StaticAuthorizer{{OrganizationID: "org-1", HumanID: "approver-1", Boundary: core.BoundaryDeployment, Risk: "HIGH"}}
	service := approvals.New(l, &notifier{}, authorizer)
	approval, err := service.Request(ctx, core.HumanApproval{
		ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1",
		Action: "deploy", Resource: "agent-os", Boundary: core.BoundaryDeployment,
		Risk: "HIGH", Urgency: "MEDIUM", EffectFingerprint: fingerprint, SingleUse: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Acknowledge(ctx, approval.ID, "ordinary-operator"); !errors.Is(err, approvals.ErrDecisionUnauthorized) {
		t.Fatalf("unauthorized acknowledgement error=%v", err)
	}
	if _, err := service.Acknowledge(ctx, approval.ID, "approver-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.BeginDecision(ctx, approval.ID, "approver-1"); err != nil {
		t.Fatal(err)
	}
	obligation.Scope = "expanded-scope"
	if err := l.AppendRecord(ctx, "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "runtime", "task-1", nil, nil, "effect", "effect-1", 2, obligation); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Decide(ctx, approvals.Decision{
		ApprovalID: approval.ID, HumanID: "approver-1", EffectFingerprint: fingerprint, Approve: true,
	}); err == nil {
		t.Fatal("decision was accepted after an authority-bearing effect field changed")
	}
}

func TestLegacyActorlessPendingApprovalCanOnlyBeInspectedAndDenied(t *testing.T) {
	obligation := core.EffectObligation{
		ID: "effect-legacy", OrganizationID: "org-1", TaskID: "task-1", ActorID: "agent-1",
		Action: "deploy", Resource: "agent-os", Scope: "org-1", ConsequenceBoundary: core.BoundaryDeployment,
		Descriptor: "deploy exact release", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-legacy",
		IdempotencyKey: "effect-key-legacy", ReplayContext: map[string]string{"version": "0.9.0"}, Status: core.EffectPending,
	}
	legacyFingerprint := legacyActorlessTestFingerprint(t, obligation)
	obligation.EffectFingerprint = legacyFingerprint
	approval := core.HumanApproval{
		ID: "approval-legacy", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: obligation.ID,
		Action: obligation.Action, Resource: obligation.Resource, Boundary: obligation.ConsequenceBoundary,
		Risk: "HIGH", Urgency: "MEDIUM", EffectFingerprint: legacyFingerprint, Status: core.ApprovalPendingDecision,
	}
	approvedLegacy := approval
	approvedLegacy.Status = core.ApprovalApproved
	if err := approvals.ValidateForEffect(approvedLegacy, obligation, time.Now().UTC()); err == nil {
		t.Fatal("legacy actorless approval was accepted at the effect boundary")
	}
	effectBody, err := json.Marshal(obligation)
	if err != nil {
		t.Fatal(err)
	}
	approvalBody, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	store := &legacyApprovalStore{records: map[string][][]byte{
		"effect/effect-legacy":     {effectBody},
		"approval/approval-legacy": {approvalBody},
	}}
	service := approvals.New(store, nil, approvals.StaticAuthorizer{{
		OrganizationID: "org-1", HumanID: "approver-1", Boundary: core.BoundaryDeployment, Risk: "HIGH",
	}})
	context, err := service.DecisionContext(t.Context(), approval.ID, "approver-1")
	if err != nil || context.Effect.ActorKind != "" || context.Effect.EffectFingerprint != legacyFingerprint {
		t.Fatalf("legacy pending approval was not recoverable for inspection: context=%+v err=%v", context, err)
	}
	if _, err := service.Decide(t.Context(), approvals.Decision{
		ApprovalID: approval.ID, HumanID: "approver-1", EffectFingerprint: legacyFingerprint, Approve: true,
	}); err == nil {
		t.Fatal("legacy actorless approval authorized an effect")
	}
	denied, err := service.Decide(t.Context(), approvals.Decision{
		ApprovalID: approval.ID, HumanID: "approver-1", EffectFingerprint: legacyFingerprint, Approve: false,
	})
	if err != nil || denied.Status != core.ApprovalDenied {
		t.Fatalf("legacy pending approval could not be denied: approval=%+v err=%v", denied, err)
	}
}

func TestDeniedDecisionCancelsPreparedEffect(t *testing.T) {
	ctx := context.Background()
	l, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	authorizer := approvals.StaticAuthorizer{{OrganizationID: "org-1", HumanID: "human-approver", Boundary: core.BoundaryDestructive, Risk: "CRITICAL"}}
	service := approvals.New(l, &notifier{}, authorizer)
	adapter := &effectAdapter{}
	coordinator := effects.New(l, adapter, service)
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", Action: "delete", Resource: "record-1", Scope: "org-1", ConsequenceBoundary: core.BoundaryDestructive, Descriptor: "permanently delete record", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1", IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"permanent": "true"}}
	fingerprint := setApprovalTestFingerprint(t, &obligation)
	if _, err := coordinator.Prepare(ctx, obligation); err != nil {
		t.Fatal(err)
	}
	approval, err := service.Request(ctx, core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "delete", Resource: "record-1", Boundary: core.BoundaryDestructive, Risk: "CRITICAL", Urgency: "MEDIUM", EffectFingerprint: fingerprint, SingleUse: true})
	if err != nil {
		t.Fatal(err)
	}
	approval, err = service.Acknowledge(ctx, approval.ID, "human-approver")
	if err != nil {
		t.Fatal(err)
	}
	approval, err = service.BeginDecision(ctx, approval.ID, "human-approver")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Decide(ctx, approvals.Decision{ApprovalID: approval.ID, HumanID: "human-approver", EffectFingerprint: fingerprint, Approve: false}); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Execute(ctx, obligation)
	if !errors.Is(err, approvals.ErrApprovalDenied) || result.Status != core.EffectCancelled || adapter.calls != 0 {
		t.Fatalf("denied effect result=%+v calls=%d err=%v", result, adapter.calls, err)
	}
	rows, err := l.Records(ctx, "effect", "effect-1")
	if err != nil || len(rows) != 2 {
		t.Fatalf("cancelled effect record versions=%d err=%v", len(rows), err)
	}
}

func assertEffectWaiting(t *testing.T, ctx context.Context, coordinator *effects.Coordinator, obligation core.EffectObligation, adapter *effectAdapter) {
	t.Helper()
	if _, err := coordinator.Execute(ctx, obligation); !errors.Is(err, approvals.ErrApprovalPending) {
		t.Fatalf("protected effect did not wait: %v", err)
	}
	if adapter.calls != 0 {
		t.Fatalf("waiting effect called adapter %d times", adapter.calls)
	}
}

func setApprovalTestFingerprint(t *testing.T, obligation *core.EffectObligation) string {
	t.Helper()
	if obligation.ActorKind == "" {
		obligation.ActorKind = core.PrincipalAgent
	}
	fingerprint, err := effects.Fingerprint(*obligation)
	if err != nil {
		t.Fatal(err)
	}
	obligation.EffectFingerprint = fingerprint
	return fingerprint
}

func legacyActorlessTestFingerprint(t *testing.T, obligation core.EffectObligation) string {
	t.Helper()
	canonical, err := json.Marshal(struct {
		ID                  core.ID           `json:"effect_obligation_id"`
		OrganizationID      core.ID           `json:"organization_id"`
		TaskID              core.ID           `json:"task_id"`
		ActorID             core.ID           `json:"actor_id"`
		Action              string            `json:"action"`
		Resource            string            `json:"resource"`
		Scope               string            `json:"scope"`
		ConsequenceBoundary string            `json:"consequence_boundary"`
		Descriptor          string            `json:"canonical_effect_descriptor"`
		AuthorizationRefs   []string          `json:"authorization_refs"`
		ApprovalRef         string            `json:"approval_ref"`
		IdempotencyKey      string            `json:"idempotency_key"`
		ReplayContext       map[string]string `json:"replay_context"`
	}{
		ID: obligation.ID, OrganizationID: obligation.OrganizationID, TaskID: obligation.TaskID,
		ActorID: obligation.ActorID, Action: obligation.Action, Resource: obligation.Resource,
		Scope: obligation.Scope, ConsequenceBoundary: obligation.ConsequenceBoundary,
		Descriptor: obligation.Descriptor, AuthorizationRefs: obligation.AuthorizationRefs,
		ApprovalRef: obligation.ApprovalRef, IdempotencyKey: obligation.IdempotencyKey,
		ReplayContext: obligation.ReplayContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func assertEventOrder(t *testing.T, stream []events.Event, expected ...string) {
	t.Helper()
	next := 0
	for _, event := range stream {
		if next < len(expected) && event.EventType == expected[next] {
			next++
		}
	}
	if next != len(expected) {
		t.Fatalf("event order missing %q after index %d", expected[next], next)
	}
}
