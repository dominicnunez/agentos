package ledger

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
)

func TestScopedAuthorityAdmissions(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	lease := core.CapabilityLease{ID: "lease-1", ActorID: "agent-1", ActorKind: core.PrincipalAgent, OriginTaskID: "task-1", Action: "read", Resource: "record-1", Scope: "org-1"}
	if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_GRANTED", "owner-1", "task-1", nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, Reason: "review"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: false, ExpectedVersion: hold.Version, ExpectedEventRef: hold.EventRef}); err != nil {
		t.Fatal(err)
	}

	err = store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		leases, freezes, err := authorityAdmissionsSnapshot(ctx, tx)
		if err != nil {
			return err
		}
		if len(leases) != 1 || leases[0].Lease.ID != lease.ID || leases[0].OrganizationID != "org-1" {
			t.Fatalf("unexpected capability admissions: %+v", leases)
		}
		if len(freezes) != 2 || freezes[0].Version != 1 || !freezes[0].Frozen || freezes[1].Version != 2 || freezes[1].Frozen {
			t.Fatalf("unexpected freeze admissions: %+v", freezes)
		}
		if freezes[0].Control == nil || freezes[1].Control == nil {
			t.Fatal("controlled freeze history lost actor evidence")
		}
		freezes[0].Control.ActorID = "changed"
		_, again, err := authorityAdmissionsSnapshot(ctx, tx)
		if err != nil {
			return err
		}
		if again[0].Control == nil || again[0].Control.ActorID != "owner-1" {
			t.Fatal("returned control aliases immutable scoped history")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestScopedAuthorityAdmissionsRejectsBrokenFreezeBindings(t *testing.T) {
	for _, test := range []struct {
		name   string
		change string
	}{
		{name: "orphan", change: `DELETE FROM records WHERE kind='organization_freeze' AND record_id='org-1'`},
		{name: "wrong-kind", change: `UPDATE events SET event_type='CAPABILITY_GRANTED' WHERE event_type='FREEZE_SET' AND organization_id='org-1'`},
		{name: "cross-organization", change: `UPDATE events SET organization_id='org-2' WHERE event_type='FREEZE_SET' AND organization_id='org-1'`},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.db.ExecContext(t.Context(), test.change); err != nil {
				t.Fatal(err)
			}
			err = store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
				_, _, err := authorityAdmissionsSnapshot(ctx, tx)
				return err
			})
			if err == nil {
				t.Fatal("broken freeze binding was accepted")
			}
		})
	}
}

func TestScopedAuthorityAdmissionsRejectsBrokenCapability(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "agent-1", ActorKind: core.PrincipalAgent, OriginTaskID: "task-1", Action: "read", Resource: "record-1", Scope: "org-1"}
	if err := store.AppendRecord(t.Context(), "org-1", "CAPABILITY_GRANTED", "owner-1", "task-1", nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='capability_lease' AND record_id='lease-1'`); err != nil {
		t.Fatal(err)
	}
	err = store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		_, _, err := authorityAdmissionsSnapshot(ctx, tx)
		return err
	})
	if err == nil {
		t.Fatal("orphan capability admission was accepted")
	}
}
