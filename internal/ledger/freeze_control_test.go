package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestFreezeRequiresTypedWrite(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state := authority.FreezeState{OrganizationID: "org-1", Frozen: true, Reason: "owner hold", UpdatedAt: time.Now().UTC()}
	err = store.AppendRecord(t.Context(), "org-1", "FREEZE_SET", "owner-1", "", nil, nil,
		"organization_freeze", "org-1", 1, state)
	if err == nil {
		t.Fatal("generic record writer bypassed the typed freeze control")
	}
	var count int
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records WHERE kind='organization_freeze'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("denied generic freeze wrote %d records", count)
	}
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM events WHERE event_type='FREEZE_SET'").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("denied generic freeze wrote %d events", count)
	}
}

func TestOwnerFreezeCancellation(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	active, finish, err := store.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	other, finishOther, err := store.BeginExecutionContext(t.Context(), "org-2")
	if err != nil {
		t.Fatal(err)
	}
	defer finishOther()
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.SetFreeze(cancelled, "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true}); err == nil {
		t.Fatal("cancelled write succeeded")
	}
	if active.Err() != nil {
		t.Fatal("failed write cancelled active work")
	}
	if _, err := store.SetFreeze(t.Context(), "org-1", "agent-1", core.PrincipalAgent, authority.FreezeChange{Frozen: true}); !errors.Is(err, authority.ErrFreezeUnauthorized) {
		t.Fatalf("agent hold: %v", err)
	}
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(context.Cause(active), core.ErrOrganizationFrozen) {
		t.Fatalf("active context not contained: %v", context.Cause(active))
	}
	var cause core.SecurityHoldCause
	if !errors.As(context.Cause(active), &cause) || cause.EventRef != hold.EventRef || cause.Sequence < 1 {
		t.Fatalf("cancellation lost committed evidence: %v", context.Cause(active))
	}
	if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{ExpectedVersion: hold.Version, ExpectedEventRef: hold.EventRef}); err != nil {
		t.Fatal(err)
	}
	if active.Err() == nil || other.Err() != nil {
		t.Fatal("release revived old work or crossed tenant")
	}
	if _, _, err := store.BeginExecutionContext(active, "org-1"); err == nil {
		t.Fatal("released old context started new work")
	}
	fresh, finishFresh, err := store.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finishFresh()
	if fresh.Err() != nil {
		t.Fatal("independent work could not start after release")
	}
}

func TestOwnerFreezeLegacyAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "freeze.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	legacy := core.FreezeState{OrganizationID: "org-1", Frozen: true, UpdatedAt: time.Unix(20, 0).UTC()}
	body, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	// Historical fixtures bypass the new writer deliberately; the original
	// bytes and their exact Event Contract are preserved, never backfilled.
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		return appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "old-runtime", TaskID: "old-task", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", 1, body)
	})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || hold.State.Control != nil {
		t.Fatalf("legacy status: %+v %v", hold, err)
	}
	store.now = func() time.Time { return time.Unix(10, 0).UTC() }
	released, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{ExpectedVersion: 1, ExpectedEventRef: hold.EventRef})
	if err != nil {
		t.Fatal(err)
	}
	if !released.State.UpdatedAt.After(legacy.UpdatedAt) {
		t.Fatal("clock rollback broke monotonic history")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || current.EventRef != released.EventRef || current.State.Frozen {
		t.Fatalf("restart lost release: %+v %v", current, err)
	}
	rows, err := store.Records(t.Context(), "organization_freeze", "org-1")
	if err != nil || len(rows) != 2 || string(rows[0]) != string(body) {
		t.Fatalf("legacy bytes changed: %v", err)
	}
}

func TestOwnerFreezeConcurrentRelease(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	results := make(chan authority.FreezeSnapshot, 2)
	failures := make(chan error, 2)
	for range 2 {
		group.Go(func() {
			released, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{ExpectedVersion: 1, ExpectedEventRef: hold.EventRef})
			results <- released
			failures <- err
		})
	}
	group.Wait()
	first, second := <-results, <-results
	if err := <-failures; err != nil {
		t.Fatal(err)
	}
	if err := <-failures; err != nil {
		t.Fatal(err)
	}
	if first.EventRef == "" || first.EventRef != second.EventRef || first.Version != 2 || second.Version != 2 {
		t.Fatalf("concurrent retry wrote multiple releases: %+v %+v", first, second)
	}
	rows, err := store.Records(t.Context(), "organization_freeze", "org-1")
	if err != nil || len(rows) != 2 {
		t.Fatalf("duplicate release history: %d %v", len(rows), err)
	}
}

func TestOwnerFreezeFailedCommit(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	active, finish, err := store.BeginExecutionContext(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	_, err = store.db.ExecContext(t.Context(), `CREATE TRIGGER reject_test_freeze BEFORE INSERT ON records WHEN NEW.kind='organization_freeze' BEGIN SELECT RAISE(ABORT,'test persistence failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true})
	if err == nil || result.EventRef != "" || result.Version != 0 {
		t.Fatalf("failed transaction reported durable success: %+v %v", result, err)
	}
	if active.Err() != nil {
		t.Fatal("failed transaction cancelled work")
	}
	var records, eventsCount int
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM records WHERE kind='organization_freeze'").Scan(&records); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM events WHERE event_type='FREEZE_SET'").Scan(&eventsCount); err != nil {
		t.Fatal(err)
	}
	if records != 0 || eventsCount != 0 {
		t.Fatalf("failed transaction leaked authority: %d records, %d events", records, eventsCount)
	}
}

func TestOwnerFreezeAcrossHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "freeze.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	hold, err := first.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := second.ReadFreeze(t.Context(), "org-1")
	if err != nil || observed.EventRef != hold.EventRef {
		t.Fatalf("second handle did not observe hold: %+v %v", observed, err)
	}
	newHold, err := first.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, Reason: "new concern", ExpectedVersion: 1, ExpectedEventRef: hold.EventRef})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{ExpectedVersion: 1, ExpectedEventRef: hold.EventRef}); !errors.Is(err, authority.ErrFreezeConflict) {
		t.Fatalf("second handle released newer hold: %v", err)
	}
	current, err := second.ReadFreeze(t.Context(), "org-1")
	if err != nil || current.EventRef != newHold.EventRef || !current.State.Frozen {
		t.Fatalf("newer hold was lost: %+v %v", current, err)
	}
}

func TestOwnerFreezeMissingPriorEvent(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true})
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt only the historical binding and a newly injected release's claim.
	// Matching two record fields must not substitute for the admitting event.
	_, err = store.db.ExecContext(t.Context(), "UPDATE records SET admission_event_id='missing-event' WHERE kind='organization_freeze' AND record_id='org-1' AND version=1")
	if err != nil {
		t.Fatal(err)
	}
	state := core.FreezeState{OrganizationID: "org-1", UpdatedAt: hold.State.UpdatedAt.Add(time.Second), Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorVersion: 1, PriorEventRef: "missing-event"}}
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		return appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", 2, body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadFreeze(t.Context(), "org-1"); err == nil {
		t.Fatal("release accepted a predecessor without its admitting event")
	}
}

func TestOwnerFreezeBuriedDowngrade(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true})
	if err != nil {
		t.Fatal(err)
	}
	for version := 2; version <= 3; version++ {
		state := core.FreezeState{OrganizationID: "org-1", Frozen: false, UpdatedAt: hold.State.UpdatedAt.Add(time.Duration(version) * time.Second)}
		body, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		// Deliberately inject unsupported history through the internal fixture
		// writer to prove a later legacy-looking pair cannot hide a downgrade.
		err = store.withTx(t.Context(), func(tx *sql.Tx) error {
			return appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "old-runtime", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", version, body)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ReadFreeze(t.Context(), "org-1"); err == nil {
		t.Fatal("buried owner evidence downgrade was accepted")
	}
	var previousEvent string
	if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='organization_freeze' AND record_id='org-1' AND version=3`).Scan(&previousEvent); err != nil {
		t.Fatal(err)
	}
	upgraded := core.FreezeState{OrganizationID: "org-1", Frozen: true, UpdatedAt: hold.State.UpdatedAt.Add(4 * time.Second),
		Control: &core.FreezeEvidence{ActorID: "owner-1", ActorKind: core.PrincipalHuman, PriorVersion: 3, PriorEventRef: previousEvent}}
	body, err := json.Marshal(upgraded)
	if err != nil {
		t.Fatal(err)
	}
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		return appendRecord(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", EventType: "FREEZE_SET", SourceActorID: "owner-1", Payload: json.RawMessage(body)}, "organization_freeze", "org-1", 4, body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadFreeze(t.Context(), "org-1"); err == nil {
		t.Fatal("later controlled state concealed the earlier metadata downgrade")
	}
}

func TestOwnerFreezeExactRelease(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	start, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || start.Version != 0 || start.State.Frozen || start.State.OrganizationID != "org-1" {
		t.Fatalf("initial status: %+v %v", start, err)
	}
	hold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, Reason: "contain work"})
	if err != nil {
		t.Fatal(err)
	}
	if hold.Version != 1 || hold.EventRef == "" || !hold.State.Frozen || hold.State.Control == nil || hold.State.Control.ActorID != "owner-1" {
		t.Fatalf("missing committed evidence: %+v", hold)
	}
	for _, change := range []authority.FreezeChange{
		{ExpectedVersion: 1, ExpectedEventRef: "wrong-event"},
		{ExpectedVersion: 2, ExpectedEventRef: hold.EventRef},
		{},
	} {
		if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, change); !errors.Is(err, authority.ErrFreezeConflict) {
			t.Fatalf("stale release: %v", err)
		}
	}
	release := authority.FreezeChange{Frozen: false, Reason: "owner verified", ExpectedVersion: hold.Version, ExpectedEventRef: hold.EventRef}
	done, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, release)
	if err != nil {
		t.Fatal(err)
	}
	if done.Version != 2 || done.State.Frozen || done.State.Control.PriorEventRef != hold.EventRef {
		t.Fatalf("incorrect release: %+v", done)
	}
	retry, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, release)
	if err != nil || retry.EventRef != done.EventRef || retry.Version != 2 {
		t.Fatalf("retry duplicated release: %+v %v", retry, err)
	}
	newHold, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, authority.FreezeChange{Frozen: true, ExpectedVersion: done.Version, ExpectedEventRef: done.EventRef})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetFreeze(t.Context(), "org-1", "owner-1", core.PrincipalHuman, release); !errors.Is(err, authority.ErrFreezeConflict) {
		t.Fatalf("old release affected new hold: %v", err)
	}
	current, err := store.ReadFreeze(t.Context(), "org-1")
	if err != nil || current.EventRef != newHold.EventRef || !current.State.Frozen {
		t.Fatalf("new hold lost: %+v %v", current, err)
	}
	if _, err := store.SetFreeze(t.Context(), "org-2", "owner-1", core.PrincipalHuman, release); !errors.Is(err, authority.ErrFreezeConflict) {
		t.Fatalf("cross-tenant release: %v", err)
	}
}
