package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

const approvalPathPrefix = "/v1/control/approvals/"

type ApprovalControl struct {
	service *approvals.Service
	owner   LocalHuman
	limits  *localHumanLimits
}

type approvalMutationRequest struct {
	EffectFingerprint string `json:"effect_fingerprint"`
}

type approvalDecisionRequest struct {
	EffectFingerprint string `json:"effect_fingerprint"`
	Decision          string `json:"decision"`
}

type approvalControlResponse struct {
	ApprovalID                core.ID             `json:"approval_id"`
	OrganizationID            core.ID             `json:"organization_id"`
	TaskID                    core.ID             `json:"task_id"`
	EffectObligationID        core.ID             `json:"effect_obligation_id"`
	Action                    string              `json:"action"`
	Resource                  string              `json:"resource"`
	Scope                     string              `json:"scope"`
	CanonicalEffectDescriptor string              `json:"canonical_effect_descriptor"`
	EffectArguments           map[string]string   `json:"effect_arguments"`
	Boundary                  string              `json:"boundary"`
	Risk                      string              `json:"risk"`
	Urgency                   string              `json:"urgency"`
	EffectFingerprint         string              `json:"effect_fingerprint"`
	Status                    core.ApprovalStatus `json:"status"`
	SingleUse                 bool                `json:"single_use"`
	CreatedAt                 string              `json:"created_at"`
	AcknowledgedAt            string              `json:"acknowledged_at,omitempty"`
	DecisionAt                string              `json:"decision_at,omitempty"`
	ExpiresAt                 string              `json:"expires_at,omitempty"`
}

func NewApprovalControl(service *approvals.Service, owner LocalHuman) (*ApprovalControl, error) {
	if service == nil || owner.UID < 0 || owner.ID == "" || owner.OrganizationID == "" {
		return nil, fmt.Errorf("local approval service, Linux UID, identity, and organization are required")
	}
	return &ApprovalControl{service: service, owner: owner, limits: &localHumanLimits{slots: make(chan struct{}, 4), requestsPerMinute: 60}}, nil
}

func (c *ApprovalControl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	_, release, err := (&Human{owner: c.owner, limits: c.limits}).acquire(r.Context())
	if errors.Is(err, ErrOperatorLimited) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "approval control request limit reached"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "local user owner required"})
		return
	}
	defer release()

	if r.Method == http.MethodGet && r.URL.Path == "/v1/control/approvals" {
		c.list(w, r, c.owner.ID)
		return
	}
	approvalID, operation, ok := approvalRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	humanID := c.owner.ID
	switch {
	case r.Method == http.MethodGet && operation == "":
		c.inspect(w, r, approvalID, humanID)
	case r.Method == http.MethodPost && operation == "acknowledge":
		c.acknowledge(w, r, approvalID, humanID)
	case r.Method == http.MethodPost && operation == "begin":
		c.begin(w, r, approvalID, humanID)
	case r.Method == http.MethodPost && operation == "decision":
		c.decide(w, r, approvalID, humanID)
	default:
		http.NotFound(w, r)
	}
}

func (c *ApprovalControl) list(w http.ResponseWriter, r *http.Request, humanID core.ID) {
	contexts, err := c.service.PendingDecisionContexts(r.Context(), humanID)
	if err != nil {
		writeApprovalError(w, err)
		return
	}
	responses := make([]approvalControlResponse, 0, len(contexts))
	for _, decisionContext := range contexts {
		responses = append(responses, approvalResponse(decisionContext))
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": responses})
}

func (c *ApprovalControl) inspect(w http.ResponseWriter, r *http.Request, approvalID, humanID core.ID) {
	decisionContext, err := c.service.DecisionContext(r.Context(), approvalID, humanID)
	if err != nil {
		writeApprovalError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, approvalResponse(decisionContext))
}

func (c *ApprovalControl) acknowledge(w http.ResponseWriter, r *http.Request, approvalID, humanID core.ID) {
	var request approvalMutationRequest
	decisionContext, ok := c.decodeMutation(w, r, approvalID, humanID, &request, func() string { return request.EffectFingerprint })
	if !ok {
		return
	}
	approval, err := c.service.Acknowledge(r.Context(), approvalID, humanID)
	c.writeMutationResult(w, decisionContext, approval, err)
}

func (c *ApprovalControl) begin(w http.ResponseWriter, r *http.Request, approvalID, humanID core.ID) {
	var request approvalMutationRequest
	decisionContext, ok := c.decodeMutation(w, r, approvalID, humanID, &request, func() string { return request.EffectFingerprint })
	if !ok {
		return
	}
	approval, err := c.service.BeginDecision(r.Context(), approvalID, humanID)
	c.writeMutationResult(w, decisionContext, approval, err)
}

func (c *ApprovalControl) decide(w http.ResponseWriter, r *http.Request, approvalID, humanID core.ID) {
	var request approvalDecisionRequest
	decisionContext, ok := c.decodeMutation(w, r, approvalID, humanID, &request, func() string { return request.EffectFingerprint })
	if !ok {
		return
	}
	if request.Decision != "APPROVE" && request.Decision != "DENY" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "decision must be APPROVE or DENY"})
		return
	}
	approval, err := c.service.Decide(r.Context(), approvals.Decision{
		ApprovalID: approvalID, HumanID: humanID,
		EffectFingerprint: request.EffectFingerprint, Approve: request.Decision == "APPROVE",
	})
	c.writeMutationResult(w, decisionContext, approval, err)
}

func (c *ApprovalControl) decodeMutation(w http.ResponseWriter, r *http.Request, approvalID, humanID core.ID, target any, fingerprint func() string) (approvals.DecisionContext, bool) {
	if !hasJSONContentType(r.Header.Get("Content-Type")) {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "approval control requires application/json"})
		return approvals.DecisionContext{}, false
	}
	defer func() { _ = r.Body.Close() }()
	reader := http.MaxBytesReader(w, r.Body, 4<<10)
	if err := trustconfig.DecodeObject(reader, "approval control request", target); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return approvals.DecisionContext{}, false
	}
	decisionContext, err := c.service.DecisionContext(r.Context(), approvalID, humanID)
	if err != nil {
		writeApprovalError(w, err)
		return approvals.DecisionContext{}, false
	}
	if fingerprint() == "" || fingerprint() != decisionContext.Approval.EffectFingerprint {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "approval no longer matches the exact effect"})
		return approvals.DecisionContext{}, false
	}
	return decisionContext, true
}

func (c *ApprovalControl) writeMutationResult(w http.ResponseWriter, previous approvals.DecisionContext, approval core.HumanApproval, err error) {
	if err != nil {
		writeApprovalError(w, err)
		return
	}
	previous.Approval = approval
	writeJSON(w, http.StatusOK, approvalResponse(previous))
}

func approvalRoute(path string) (core.ID, string, bool) {
	if !strings.HasPrefix(path, approvalPathPrefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, approvalPathPrefix), "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		return "", "", false
	}
	if err := validateOperatorIdentity(parts[0], "organization-placeholder"); err != nil {
		return "", "", false
	}
	operation := ""
	if len(parts) == 2 {
		operation = parts[1]
		if operation == "" {
			return "", "", false
		}
	}
	return core.ID(parts[0]), operation, true
}

func approvalResponse(decisionContext approvals.DecisionContext) approvalControlResponse {
	approval := decisionContext.Approval
	effect := decisionContext.Effect
	response := approvalControlResponse{
		ApprovalID: approval.ID, OrganizationID: approval.OrganizationID, TaskID: approval.TaskID,
		EffectObligationID: approval.EffectObligationID, Action: effect.Action, Resource: effect.Resource,
		Scope: effect.Scope, CanonicalEffectDescriptor: effect.Descriptor, EffectArguments: effect.ReplayContext,
		Boundary: approval.Boundary, Risk: approval.Risk, Urgency: approval.Urgency,
		EffectFingerprint: approval.EffectFingerprint, Status: approval.Status,
		SingleUse: approval.SingleUse, CreatedAt: approval.CreatedAt.UTC().Format(timeFormat),
	}
	if approval.AcknowledgedAt != nil {
		response.AcknowledgedAt = approval.AcknowledgedAt.UTC().Format(timeFormat)
	}
	if approval.DecisionAt != nil {
		response.DecisionAt = approval.DecisionAt.UTC().Format(timeFormat)
	}
	if approval.ExpiresAt != nil {
		response.ExpiresAt = approval.ExpiresAt.UTC().Format(timeFormat)
	}
	return response
}

const timeFormat = "2006-01-02T15:04:05.999999999Z07:00"

func writeApprovalError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, approvals.ErrApprovalNotFound), errors.Is(err, approvals.ErrDecisionUnauthorized):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "approval not found"})
	case errors.Is(err, approvals.ErrApprovalExpired):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "approval expired"})
	default:
		writeJSON(w, http.StatusConflict, map[string]string{"error": "approval state conflicts with the requested operation"})
	}
}
