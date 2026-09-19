package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestSuspendedTaskDeniesNewEffectAttemptAndPreservesAttemptedReconciliation(t *testing.T) {
	for _, test := range []struct {
		name    string
		suspend func(*testing.T, *SQLite, core.Task)
	}{
		{
			name: "typed stop suspension",
			suspend: func(t *testing.T, store *SQLite, task core.Task) {
				t.Helper()
				executionID := fmt.Sprintf("execution-%s-v2", task.ID)
				if _, err := store.RequestExecutionStop(t.Context(), "org-1", string(task.ID), "stop-work", executionID, "execution_cancelled"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "legacy admitted suspension",
			suspend: appendLegacyEffectTestSuspension,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			task := stopTestExecution(t, store)

			lease := core.CapabilityLease{
				ID: "lease-stopped-effect", ActorID: task.AssigneeID, ActorKind: core.PrincipalAgent,
				OriginTaskID: task.ID, Action: "send", Resource: "customer-1", Scope: "org-1",
			}
			if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_GRANTED", "human-1", string(task.ID), nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
				t.Fatal(err)
			}

			attempted := appendApprovedEffectAttempt(t, store, task, lease, "effect-before-stop", "approval-before-stop")
			test.suspend(t, store, task)
			if err := validateTaskNotSuspended(t.Context(), store.db, "org-other", string(task.ID)); err != nil {
				t.Fatalf("another organization's effect was denied by this suspension: %v", err)
			}

			blocked := approvedEffect(t, task, lease, "effect-after-stop", "approval-after-stop")
			appendEffectApproval(t, store, blocked, true)
			beforeEvents := stopAdmissionEventCount(t, store)
			blockedAttempt := blocked
			blockedAttempt.Status = core.EffectAttempted
			blockedAttempt.AttemptCount = 1
			if _, err := store.AuthorizeAndAppendEffectAttempt(t.Context(), blocked, 1, blockedAttempt); !errors.Is(err, core.ErrExecutionStopped) {
				t.Fatalf("suspended task effect error = %v", err)
			}
			if afterEvents := stopAdmissionEventCount(t, store); afterEvents != beforeEvents {
				t.Fatalf("denied effect appended %d events", afterEvents-beforeEvents)
			}
			if records, err := store.Records(t.Context(), "effect", string(blocked.ID)); err != nil || len(records) != 0 {
				t.Fatalf("denied effect reached ATTEMPTED: records=%d err=%v", len(records), err)
			}
			var consumed int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM consumed_approvals WHERE approval_id=?`, blocked.ApprovalRef).Scan(&consumed); err != nil || consumed != 0 {
				t.Fatalf("denied effect consumed approval: count=%d err=%v", consumed, err)
			}

			reconciled := attempted
			reconciled.Status = core.EffectConfirmed
			reconciled.ConfirmationEvidenceRefs = []string{"receipt-before-stop"}
			if err := store.AppendRecord(t.Context(), "org-1", "EFFECT_OBLIGATION_TRANSITIONED", "", string(task.ID), reconciled.AuthorizationRefs, reconciled.ConfirmationEvidenceRefs, "effect", string(reconciled.ID), 2, reconciled); err != nil {
				t.Fatalf("reconcile already attempted effect: %v", err)
			}
			if records, err := store.Records(t.Context(), "effect", string(reconciled.ID)); err != nil || len(records) != 2 {
				t.Fatalf("reconciled effect records=%d err=%v", len(records), err)
			}
		})
	}
}

func appendApprovedEffectAttempt(t *testing.T, store *SQLite, task core.Task, lease core.CapabilityLease, effectID, approvalID string) core.EffectObligation {
	t.Helper()
	obligation := approvedEffect(t, task, lease, effectID, approvalID)
	appendEffectApproval(t, store, obligation, true)
	attempted := obligation
	attempted.Status = core.EffectAttempted
	attempted.AttemptCount = 1
	trace, err := store.AuthorizeAndAppendEffectAttempt(t.Context(), obligation, 1, attempted)
	if err != nil || !trace.Allowed {
		t.Fatalf("valid pre-suspension effect was not admitted: trace=%+v err=%v", trace, err)
	}
	return attempted
}

func approvedEffect(t *testing.T, task core.Task, lease core.CapabilityLease, effectID, approvalID string) core.EffectObligation {
	t.Helper()
	obligation := core.EffectObligation{
		ID: core.ID(effectID), OrganizationID: "org-1", TaskID: task.ID, ActorID: lease.ActorID, ActorKind: lease.ActorKind,
		Action: lease.Action, Resource: lease.Resource, Scope: lease.Scope, ConsequenceBoundary: core.BoundaryPublicExternal,
		Descriptor: "send customer message", AuthorizationRefs: []string{string(lease.ID)}, ApprovalRef: approvalID,
		IdempotencyKey: "key-" + effectID, ReplayContext: map[string]string{"body": "hello"},
	}
	fingerprint, err := core.FingerprintEffect(obligation)
	if err != nil {
		t.Fatal(err)
	}
	obligation.EffectFingerprint = fingerprint
	return obligation
}

func appendEffectApproval(t *testing.T, store *SQLite, obligation core.EffectObligation, singleUse bool) {
	t.Helper()
	approval := core.HumanApproval{
		ID: core.ID(obligation.ApprovalRef), OrganizationID: obligation.OrganizationID, TaskID: obligation.TaskID,
		EffectObligationID: obligation.ID, Action: obligation.Action, Resource: obligation.Resource,
		Boundary: obligation.ConsequenceBoundary, Status: core.ApprovalApproved,
		EffectFingerprint: obligation.EffectFingerprint, SingleUse: singleUse,
	}
	if err := store.AppendRecord(t.Context(), string(obligation.OrganizationID), "APPROVAL_DECIDED", "human-1", string(obligation.TaskID), nil, nil, "approval", obligation.ApprovalRef, 1, approval); err != nil {
		t.Fatal(err)
	}
}

func appendLegacyEffectTestSuspension(t *testing.T, store *SQLite, task core.Task) {
	t.Helper()
	stream, err := store.Events(t.Context(), "stop-work")
	if err != nil {
		t.Fatal(err)
	}
	startRef := ""
	for _, event := range stream {
		if event.EventType == "EXECUTION_STARTED" {
			startRef = event.EventID
		}
	}
	if startRef == "" {
		t.Fatal("legacy suspension fixture lacks its execution start")
	}
	task.Status = core.TaskBlocked
	draft := events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: "org-1", EventType: "TASK_EXECUTION_SUSPENDED", SourceActorID: "runtime",
			TaskID: string(task.ID), CorrelationID: "stop-work",
			Payload: map[string]string{
				"execution_start_ref": startRef, "outcome_event_ref": "legacy-outcome",
				"reason": "legacy security reconciliation required",
			},
		},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 3, Value: task,
	}
	prepared, err := prepareProjection(draft, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		_, err := appendPreparedProjection(ctx, tx, prepared)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stream, err = store.Events(t.Context(), "stop-work")
	if err != nil {
		t.Fatal(err)
	}
	if err := events.ValidateExecutionStops(stream, nil); err != nil {
		t.Fatalf("legacy suspension fixture is not valid replay input: %v", err)
	}
}
