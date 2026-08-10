package approvals_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

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

func TestProtectedEffectWaitsAcrossRestartForExactAuthorizedDecision(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agentos.db")
	l, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	n := &notifier{}
	authorizer := approvals.StaticAuthorizer{{OrganizationID: "org-1", HumanID: "human-approver", Boundary: core.BoundaryPublicExternal, Risk: "HIGH"}}
	service := approvals.New(l, n, authorizer)
	fingerprint, err := effects.Fingerprint("send", "customer-1", map[string]string{"body": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{
		ID:                  "effect-1",
		OrganizationID:      "org-1",
		TaskID:              "task-1",
		Action:              "send",
		Resource:            "customer-1",
		ConsequenceBoundary: core.BoundaryPublicExternal,
		Descriptor:          "send greeting",
		EffectFingerprint:   fingerprint,
		ApprovalRef:         "approval-1",
		IdempotencyKey:      "effect-key-1",
		ReplayContext:       map[string]string{"body": "hello"},
	}
	adapter := &effectAdapter{}
	coordinator := effects.NewWithApprovals(l, adapter, service)
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
		Urgency:            "NORMAL",
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
	coordinator = effects.NewWithApprovals(l, adapter, service)
	if _, err := service.BeginDecision(ctx, approval.ID, "hermes-operator"); !errors.Is(err, approvals.ErrDecisionUnauthorized) {
		t.Fatalf("ordinary Hermes identity entered decision state: %v", err)
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
	stream, err := l.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	assertEventOrder(t, stream, "APPROVAL_REQUESTED", "APPROVAL_NOTIFIED", "APPROVAL_ACKNOWLEDGED", "APPROVAL_DECISION_STARTED", "APPROVAL_DECIDED", "APPROVAL_CONSUMED", "EFFECT_OBLIGATION_TRANSITIONED")
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
	fingerprint, err := effects.Fingerprint("deploy", "agent-os", map[string]string{"version": "1"})
	if err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", Action: "deploy", Resource: "agent-os", ConsequenceBoundary: core.BoundaryDeployment, Descriptor: "deploy Agent OS", EffectFingerprint: fingerprint, ApprovalRef: "approval-1", IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"version": "1"}}
	if _, err := effects.NewWithApprovals(l, &effectAdapter{}, service).Prepare(ctx, obligation); err != nil {
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
		Urgency:            "NORMAL",
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
	_, err = service.Request(ctx, core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "missing-effect", Action: "deploy", Resource: "agent-os", Boundary: core.BoundaryDeployment, Risk: "HIGH", Urgency: "NORMAL", EffectFingerprint: "fingerprint-1"})
	if err == nil || n.calls != 0 {
		t.Fatalf("approval without prepared effect was notified: calls=%d err=%v", n.calls, err)
	}
	rows, err := l.Records(ctx, "approval", "approval-1")
	if err != nil || len(rows) != 0 {
		t.Fatalf("approval without prepared effect was persisted: rows=%d err=%v", len(rows), err)
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
	coordinator := effects.NewWithApprovals(l, adapter, service)
	fingerprint, _ := effects.Fingerprint("delete", "record-1", map[string]string{"permanent": "true"})
	obligation := core.EffectObligation{ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", Action: "delete", Resource: "record-1", ConsequenceBoundary: core.BoundaryDestructive, Descriptor: "permanently delete record", EffectFingerprint: fingerprint, ApprovalRef: "approval-1", IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"permanent": "true"}}
	if _, err := coordinator.Prepare(ctx, obligation); err != nil {
		t.Fatal(err)
	}
	approval, err := service.Request(ctx, core.HumanApproval{ID: "approval-1", OrganizationID: "org-1", TaskID: "task-1", EffectObligationID: "effect-1", Action: "delete", Resource: "record-1", Boundary: core.BoundaryDestructive, Risk: "CRITICAL", Urgency: "NORMAL", EffectFingerprint: fingerprint, SingleUse: true})
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
