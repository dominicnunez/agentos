package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestFreezeTxReusesProof(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedFreezeHistory(t, store, 16)
	err = store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		first, err := loadFreezeHistory(ctx, tx, "org-1")
		if err != nil {
			return err
		}
		second, err := loadFreezeHistory(ctx, tx, "org-1")
		if err != nil {
			return err
		}
		if len(first.revisions) != 16 || len(second.revisions) != 16 {
			t.Fatal("transaction lost its complete freeze history")
		}
		// Check reuse of the immutable proof itself, without a machine-specific
		// timing limit that could hide multiple expensive validations.
		if &first.revisions[0] != &second.revisions[0] {
			t.Fatal("unchanged transaction rebuilt its validated freeze history")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFreezeTxRechecksWrites(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedFreezeHistory(t, store, 16)
	rollback := errors.New("rollback test changes")
	var oldContext context.Context
	err = store.withFreezeTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		oldContext = ctx
		before, err := loadFreezeHistory(ctx, tx, "org-1")
		if err != nil {
			return err
		}
		prior, found := before.latest()
		if !found || prior.record.Version != 16 || prior.state.Frozen {
			t.Fatal("unexpected initial freeze history")
		}
		state := core.FreezeState{OrganizationID: "org-1", Frozen: true, UpdatedAt: time.Unix(17, 0).UTC(),
			Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorVersion: 16, PriorEventRef: prior.event.EventID}}
		body, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := appendRecord(ctx, tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", 17, body); err != nil {
			return err
		}
		after, err := loadFreezeHistory(ctx, tx, "org-1")
		if err != nil {
			return err
		}
		latest, found := after.latest()
		if !found || latest.record.Version != 17 || !latest.state.Frozen || len(before.revisions) != 16 {
			t.Fatal("transaction reused stale proof after its own appended hold")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE records SET body='{}' WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`); err != nil {
			return err
		}
		if _, err := loadFreezeHistory(ctx, tx, "org-1"); err == nil {
			t.Fatal("transaction reused proof after rewriting its history")
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("test rollback boundary not reached: %v", err)
	}
	if store.freezes.get("org-1") != nil {
		t.Fatal("private transaction published its uncommitted view")
	}
	// Even an explicitly reused derived context cannot transfer its seventeen-
	// revision proof to a different SQL transaction after rollback.
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		history, err := loadFreezeHistory(oldContext, tx, "org-1")
		if err != nil {
			return err
		}
		latest, found := history.latest()
		if !found || latest.record.Version != 16 || latest.state.Frozen {
			t.Fatal("another transaction accepted rolled-back authority")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
