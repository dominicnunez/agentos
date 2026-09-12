package authority

import (
	"context"
	"errors"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

type freezeControlStore struct {
	snapshot  FreezeSnapshot
	err       error
	reads     int
	writes    int
	org       core.ID
	actorID   core.ID
	actorKind core.PrincipalKind
	change    FreezeChange
}

func (s *freezeControlStore) ReadFreeze(_ context.Context, organization core.ID) (FreezeSnapshot, error) {
	s.reads++
	s.org = organization
	return s.snapshot, s.err
}

func (s *freezeControlStore) SetFreeze(_ context.Context, organization, actorID core.ID, actorKind core.PrincipalKind, change FreezeChange) (FreezeSnapshot, error) {
	s.writes++
	s.org, s.actorID, s.actorKind, s.change = organization, actorID, actorKind, change
	return s.snapshot, s.err
}

func TestFreezeControlDeniesEveryNonOwnerIdentityBeforeStore(t *testing.T) {
	for _, test := range []struct {
		name      string
		actorID   core.ID
		actorKind core.PrincipalKind
	}{
		{name: "missing actor", actorKind: core.PrincipalHuman},
		{name: "different human", actorID: "human-2", actorKind: core.PrincipalHuman},
		{name: "owner id as agent", actorID: "owner-1", actorKind: core.PrincipalAgent},
		{name: "owner id with missing kind", actorID: "owner-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &freezeControlStore{}
			control, err := NewFreezeControl(store, "org-1", "owner-1")
			if err != nil {
				t.Fatal(err)
			}

			if _, err := control.Read(t.Context(), test.actorID, test.actorKind); !errors.Is(err, ErrFreezeUnauthorized) {
				t.Fatalf("Read error=%v", err)
			}
			if _, err := control.Change(t.Context(), test.actorID, test.actorKind, FreezeChange{Frozen: true}); !errors.Is(err, ErrFreezeUnauthorized) {
				t.Fatalf("Change error=%v", err)
			}
			if store.reads != 0 || store.writes != 0 {
				t.Fatalf("denied identity reached store: reads=%d writes=%d", store.reads, store.writes)
			}
		})
	}
}

func TestFreezeControlUsesOnlyConfiguredOrganizationAndOwner(t *testing.T) {
	want := FreezeSnapshot{State: FreezeState{OrganizationID: "org-1", Frozen: true}, EventRef: "event-2", Version: 2}
	store := &freezeControlStore{snapshot: want}
	control, err := NewFreezeControl(store, "org-1", "owner-1")
	if err != nil {
		t.Fatal(err)
	}

	got, err := control.Read(t.Context(), "owner-1", core.PrincipalHuman)
	if err != nil || got != want {
		t.Fatalf("Read=(%+v, %v), want (%+v, nil)", got, err, want)
	}
	if store.org != "org-1" || store.reads != 1 {
		t.Fatalf("Read store call org=%q count=%d", store.org, store.reads)
	}

	change := FreezeChange{Frozen: false, Reason: "incident resolved", ExpectedEventRef: "event-2", ExpectedVersion: 2}
	got, err = control.Change(t.Context(), "owner-1", core.PrincipalHuman, change)
	if err != nil || got != want {
		t.Fatalf("Change=(%+v, %v), want (%+v, nil)", got, err, want)
	}
	if store.org != "org-1" || store.actorID != "owner-1" || store.actorKind != core.PrincipalHuman || store.change != change || store.writes != 1 {
		t.Fatalf("Change store call org=%q actor=(%q,%q) change=%+v count=%d", store.org, store.actorID, store.actorKind, store.change, store.writes)
	}
}

func TestNewFreezeControlRejectsIncompleteAuthority(t *testing.T) {
	store := &freezeControlStore{}
	for _, test := range []struct {
		name  string
		store FreezeStore
		org   core.ID
		owner core.ID
	}{
		{name: "missing store", org: "org-1", owner: "owner-1"},
		{name: "missing organization", store: store, owner: "owner-1"},
		{name: "missing owner", store: store, org: "org-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewFreezeControl(test.store, test.org, test.owner); !errors.Is(err, ErrFreezeInvalid) {
				t.Fatalf("NewFreezeControl error=%v", err)
			}
		})
	}
}
