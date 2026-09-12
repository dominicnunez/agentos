package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestFreezeControlSQLiteOwnerLifecycle(t *testing.T) {
	const (
		organization = core.ID("org-1")
		ownerID      = core.ID("owner-1")
		ownerUID     = 1000
	)
	owner := LocalHuman{UID: ownerUID, ID: ownerID, OrganizationID: organization}
	path := filepath.Join(t.TempDir(), "freeze-control.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	handler := newGatewayFreezeControl(t, store, owner)

	oldContext, finishOld, err := store.BeginExecutionContext(t.Context(), string(organization))
	if err != nil {
		t.Fatal(err)
	}
	defer finishOld()

	holdResponse := freezeRequest(t, handler, http.MethodPost, freezeControlPath, ownerUID,
		`{"frozen":true,"reason":"contain active incident","expected_event_ref":"","expected_version":0}`)
	if holdResponse.Code != http.StatusOK {
		t.Fatalf("hold status=%d body=%s", holdResponse.Code, holdResponse.Body.String())
	}
	hold := decodeLedgerFreezeSnapshot(t, holdResponse)
	if hold.Version != 1 || hold.EventRef == "" || hold.State.OrganizationID != organization ||
		!hold.State.Frozen || hold.State.Reason != "contain active incident" || hold.State.UpdatedAt.IsZero() ||
		hold.State.Control == nil || hold.State.Control.ActorID != ownerID ||
		hold.State.Control.ActorKind != core.PrincipalHuman || hold.State.Control.PriorEventRef != "" ||
		hold.State.Control.PriorVersion != 0 {
		t.Fatalf("hold omitted exact committed authority evidence: %+v", hold)
	}
	var holdCause core.SecurityHoldCause
	if !errors.Is(context.Cause(oldContext), core.ErrOrganizationFrozen) ||
		!errors.As(context.Cause(oldContext), &holdCause) || holdCause.OrganizationID != organization ||
		holdCause.EventRef != hold.EventRef || holdCause.Sequence < 1 {
		t.Fatalf("registered execution was not cancelled by exact hold event: %v", context.Cause(oldContext))
	}

	ownerHead := freezeRequest(t, handler, http.MethodGet, freezeControlPath, ownerUID, "")
	if ownerHead.Code != http.StatusOK || !reflect.DeepEqual(decodeLedgerFreezeSnapshot(t, ownerHead), hold) {
		t.Fatalf("owner GET did not return hold head: status=%d body=%s", ownerHead.Code, ownerHead.Body.String())
	}

	releaseBody := fmt.Sprintf(`{"frozen":false,"reason":"owner verified recovery","expected_event_ref":%q,"expected_version":%d}`, hold.EventRef, hold.Version)
	impostor := freezeRequest(t, handler, http.MethodPost, freezeControlPath, ownerUID+1, releaseBody)
	if impostor.Code != http.StatusForbidden {
		t.Fatalf("impostor status=%d body=%s", impostor.Code, impostor.Body.String())
	}
	stale := freezeRequest(t, handler, http.MethodPost, freezeControlPath, ownerUID,
		fmt.Sprintf(`{"frozen":false,"reason":"stale recovery","expected_event_ref":"stale-event","expected_version":%d}`, hold.Version))
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale release status=%d body=%s", stale.Code, stale.Body.String())
	}
	stillHeld := freezeRequest(t, handler, http.MethodGet, freezeControlPath, ownerUID, "")
	if stillHeld.Code != http.StatusOK || !reflect.DeepEqual(decodeLedgerFreezeSnapshot(t, stillHeld), hold) {
		t.Fatalf("denied or stale release changed hold head: status=%d body=%s", stillHeld.Code, stillHeld.Body.String())
	}

	persistedStore, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = persistedStore.Close() })
	handler = newGatewayFreezeControl(t, persistedStore, owner)
	persisted := freezeRequest(t, handler, http.MethodGet, freezeControlPath, ownerUID, "")
	if persisted.Code != http.StatusOK || !reflect.DeepEqual(decodeLedgerFreezeSnapshot(t, persisted), hold) {
		t.Fatalf("independent SQLite reader lost exact hold record/event: status=%d body=%s", persisted.Code, persisted.Body.String())
	}

	releaseResponse := freezeRequest(t, handler, http.MethodPost, freezeControlPath, ownerUID, releaseBody)
	if releaseResponse.Code != http.StatusOK {
		t.Fatalf("exact release status=%d body=%s", releaseResponse.Code, releaseResponse.Body.String())
	}
	released := decodeLedgerFreezeSnapshot(t, releaseResponse)
	if released.Version != hold.Version+1 || released.EventRef == "" || released.EventRef == hold.EventRef ||
		released.State.OrganizationID != organization || released.State.Frozen ||
		released.State.Reason != "owner verified recovery" || released.State.Control == nil ||
		released.State.Control.ActorID != ownerID || released.State.Control.ActorKind != core.PrincipalHuman ||
		released.State.Control.PriorEventRef != hold.EventRef || released.State.Control.PriorVersion != hold.Version {
		t.Fatalf("release omitted exact predecessor evidence: %+v", released)
	}
	var afterReleaseCause core.SecurityHoldCause
	if !errors.Is(context.Cause(oldContext), core.ErrOrganizationFrozen) ||
		!errors.As(context.Cause(oldContext), &afterReleaseCause) || afterReleaseCause != holdCause {
		t.Fatalf("release revived the old execution context: %v", context.Cause(oldContext))
	}

	fresh, finishFresh, err := persistedStore.BeginExecutionContext(t.Context(), string(organization))
	if err != nil {
		t.Fatalf("independent execution was denied after release: %v", err)
	}
	defer finishFresh()
	if fresh.Err() != nil {
		t.Fatalf("independent execution started cancelled after release: %v", context.Cause(fresh))
	}
}

func decodeLedgerFreezeSnapshot(t *testing.T, response *httptest.ResponseRecorder) authority.FreezeSnapshot {
	t.Helper()
	var snapshot authority.FreezeSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
