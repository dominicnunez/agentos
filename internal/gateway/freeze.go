package gateway

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/dominicnunez/agentos/internal/authority"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

const freezeControlPath = "/v1/control/freeze"

type FreezeControl struct {
	service *authority.FreezeControl
	owner   LocalHuman
	access  *localUserAccess
}

type freezeChangeRequest struct {
	Frozen           *bool   `json:"frozen"`
	Reason           *string `json:"reason"`
	ExpectedEventRef *string `json:"expected_event_ref"`
	ExpectedVersion  *int    `json:"expected_version"`
}

func NewFreezeControl(service *authority.FreezeControl, owner LocalHuman) (*FreezeControl, error) {
	if service == nil || owner.UID < 0 || owner.ID == "" || owner.OrganizationID == "" {
		return nil, fmt.Errorf("local freeze service, Linux UID, identity, and organization are required")
	}
	if !service.ConfiguredFor(owner.OrganizationID, owner.ID) {
		return nil, fmt.Errorf("local freeze service is not bound to the configured owner")
	}
	return &FreezeControl{service: service, owner: owner, access: newLocalUserAccess(owner.UID, 4, 60)}, nil
}

func (c *FreezeControl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path != freezeControlPath || r.URL.RawQuery != "" || (r.Method != http.MethodGet && r.Method != http.MethodPost) {
		http.NotFound(w, r)
		return
	}
	release, err := c.access.acquire(r.Context())
	if errors.Is(err, ErrOperatorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "freeze control request limit reached"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "configured local owner required"})
		return
	}
	defer release()

	if r.Method == http.MethodGet {
		snapshot, err := c.service.Read(r.Context(), c.owner.ID, core.PrincipalHuman)
		c.writeResult(w, snapshot, err)
		return
	}
	c.change(w, r)
}

func (c *FreezeControl) change(w http.ResponseWriter, r *http.Request) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "freeze control request is invalid"})
		return
	}
	defer func() { _ = r.Body.Close() }()
	var request freezeChangeRequest
	reader := http.MaxBytesReader(w, r.Body, 4<<10)
	if err := trustconfig.DecodeObject(reader, "freeze control request", &request); err != nil ||
		request.Frozen == nil || request.Reason == nil || request.ExpectedEventRef == nil || request.ExpectedVersion == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "freeze control request is invalid"})
		return
	}
	snapshot, err := c.service.Change(r.Context(), c.owner.ID, core.PrincipalHuman, authority.FreezeChange{
		Frozen:           *request.Frozen,
		Reason:           *request.Reason,
		ExpectedEventRef: *request.ExpectedEventRef,
		ExpectedVersion:  *request.ExpectedVersion,
	})
	c.writeResult(w, snapshot, err)
}

func (c *FreezeControl) writeResult(w http.ResponseWriter, snapshot authority.FreezeSnapshot, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, snapshot)
		return
	}
	switch {
	case errors.Is(err, authority.ErrFreezeInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "freeze control request is invalid"})
	case errors.Is(err, authority.ErrFreezeUnauthorized):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "configured local owner required"})
	case errors.Is(err, authority.ErrFreezeConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "freeze state conflicts with the requested change"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "freeze control is unavailable"})
	}
}
