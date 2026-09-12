package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestFreezeCacheRejectsWarmedHistoricalMutationByOrganization(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate string
	}{
		{
			name: "record body",
			mutate: `UPDATE records SET body='{}'
WHERE kind='organization_freeze' AND record_id='org-1' AND version=1`,
		},
		{
			name: "event body",
			mutate: `UPDATE events SET payload='{}'
WHERE event_id=(SELECT admission_event_id FROM records
WHERE kind='organization_freeze' AND record_id='org-1' AND version=1)`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendInferenceFreeze(t, store, "org-1", 1, true)
			appendInferenceFreeze(t, store, "org-1", 2, false)
			appendInferenceFreeze(t, store, "org-2", 1, true)
			otherHead := appendInferenceFreeze(t, store, "org-2", 2, false)

			affected, finishAffected, err := store.BeginExecutionContext(t.Context(), "org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer finishAffected()
			other, finishOther, err := store.BeginExecutionContext(t.Context(), "org-2")
			if err != nil {
				t.Fatal(err)
			}
			defer finishOther()

			if _, err := store.db.ExecContext(t.Context(), test.mutate); err != nil {
				t.Fatal(err)
			}
			if snapshot, err := store.ReadFreeze(t.Context(), "org-1"); err == nil {
				t.Fatalf("warmed corrupt history returned snapshot %+v", snapshot)
			}
			select {
			case <-affected.Done():
			case <-time.After(2 * containmentObservationTimeout):
				t.Fatal("warmed corrupt history did not stop live work")
			}
			if !errors.Is(context.Cause(affected), core.ErrContainmentUnavailable) {
				t.Fatalf("corrupt history cancellation cause=%v", context.Cause(affected))
			}
			if other.Err() != nil {
				t.Fatalf("corrupt org-1 cache cancelled org-2: %v", context.Cause(other))
			}
			otherCurrent, err := store.ReadFreeze(t.Context(), "org-2")
			if err != nil || !reflect.DeepEqual(otherCurrent, otherHead) {
				t.Fatalf("corrupt org-1 cache changed org-2: snapshot=%+v err=%v", otherCurrent, err)
			}
		})
	}
}

func TestFreezeCacheCrossHandleTailRetainsEarliestHold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "freeze-cache.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	call, finish, err := reader.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	hold, err := writer.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, Reason: "hold"})
	if err != nil {
		t.Fatal(err)
	}
	release, err := writer.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{
		Frozen: false, Reason: "release", ExpectedEventRef: hold.EventRef, ExpectedVersion: hold.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-call.Done():
	case <-time.After(2 * containmentObservationTimeout):
		t.Fatal("cross-handle hold and release did not stop prior work")
	}
	var cause core.SecurityHoldCause
	if !errors.As(context.Cause(call), &cause) || cause.EventRef != hold.EventRef || cause.OrganizationID != "org-1" {
		t.Fatalf("tail cache lost earliest hold: cause=%v hold=%+v", context.Cause(call), hold)
	}
	current, err := reader.ReadFreeze(t.Context(), "org-1")
	if err != nil || !reflect.DeepEqual(current, release) {
		t.Fatalf("reader cache did not reach release head: snapshot=%+v err=%v", current, err)
	}
}

func TestFreezeCacheRolledBackCandidateIsNeverPublished(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	before, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	call, finish, err := store.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()

	rollback := errors.New("test rollback after freeze candidate")
	candidateBuilt := false
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		prior, err := store.freezeView(t.Context(), tx, "org-1", true)
		if err != nil {
			return err
		}
		state := core.FreezeState{
			OrganizationID: "org-1",
			Frozen:         true,
			Reason:         "rolled back hold",
			UpdatedAt:      store.nowUTC(),
			Control:        &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman},
		}
		body, err := json.Marshal(state)
		if err != nil {
			return err
		}
		draft := events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: json.RawMessage(body)}
		if err := events.ValidateAuthorityRecordDraft(draft, "organization_freeze", "org-1", 1, body); err != nil {
			return err
		}
		if err := appendRecord(t.Context(), tx, draft, "organization_freeze", "org-1", 1, body); err != nil {
			return err
		}
		candidate, err := store.appendFreezeView(t.Context(), tx, prior)
		if err != nil {
			return err
		}
		head, found := candidate.history.latest()
		if !found || head.record.Version != 1 || !head.state.Frozen {
			return errors.New("test candidate did not contain appended hold")
		}
		candidateBuilt = true
		return rollback
	})
	if !candidateBuilt || !errors.Is(err, rollback) {
		t.Fatalf("candidate rollback path was not reached: built=%t err=%v", candidateBuilt, err)
	}
	after, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("rolled-back candidate changed published cache: before=%+v after=%+v err=%v", before, after, err)
	}
	if call.Err() != nil {
		t.Fatalf("rolled-back candidate cancelled live work: %v", context.Cause(call))
	}
}

func TestFreezeCacheSnapshotsCloneControlEvidence(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, Reason: "hold"})
	if err != nil || hold.State.Control == nil {
		t.Fatalf("hold snapshot=%+v err=%v", hold, err)
	}
	hold.State.Control.ActorID = "attacker"
	hold.State.Control.ActorKind = core.PrincipalAgent
	hold.State.Control.PriorEventRef = "forged"
	hold.State.Control.PriorVersion = 99

	current, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || current.State.Control == nil || current.State.Control.ActorID != "owner-1" ||
		current.State.Control.ActorKind != core.PrincipalHuman || current.State.Control.PriorEventRef != "" ||
		current.State.Control.PriorVersion != 0 {
		t.Fatalf("returned snapshot mutated cached control evidence: snapshot=%+v err=%v", current, err)
	}
	current.State.Control.ActorID = "second-attacker"
	again, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || again.State.Control == nil || again.State.Control.ActorID != "owner-1" {
		t.Fatalf("read snapshot shared cached control pointer: snapshot=%+v err=%v", again, err)
	}
	if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{
		Frozen: false, Reason: "release", ExpectedEventRef: again.EventRef, ExpectedVersion: again.Version,
	}); err != nil {
		t.Fatalf("mutated snapshots corrupted exact release: %v", err)
	}
}

func TestFreezeCacheOlderTransactionCannotUseNewerPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "older-freeze-snapshot.db")
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if _, err := reader.db.ExecContext(t.Context(), "PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	hold := appendInferenceFreeze(t, reader, "org-1", 1, true)
	release, err := reader.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{
		Frozen: false, Reason: "release", ExpectedEventRef: hold.EventRef, ExpectedVersion: hold.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	if warmed, err := reader.ReadFreeze(t.Context(), "org-1"); err != nil || !reflect.DeepEqual(warmed, release) {
		t.Fatalf("reader did not warm released prefix: snapshot=%+v err=%v", warmed, err)
	}

	old, err := reader.watchDB.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = old.Rollback() }()
	var oldGeneration []byte
	if err := old.QueryRowContext(t.Context(), "SELECT generation FROM freeze_changes WHERE organization_id='org-1'").Scan(&oldGeneration); err != nil {
		t.Fatal(err)
	}
	newHold, err := reader.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{
		Frozen: true, Reason: "new hold", ExpectedEventRef: release.EventRef, ExpectedVersion: release.Version,
	})
	if err != nil {
		t.Fatal(err)
	}
	if newHold.Version != release.Version+1 || newHold.EventRef == "" {
		t.Fatalf("new hold = %#v; want next durable version with event ref", newHold)
	}
	view, err := reader.freezeView(t.Context(), old, "org-1", false)
	if view != nil || !errors.Is(err, core.ErrContainmentUnavailable) {
		t.Fatalf("older transaction trusted newer shared cache: view=%+v err=%v", view, err)
	}
}
