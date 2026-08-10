package gateway

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

const approvalControlChannel = "APPROVAL_CONTROL"

type ApprovalGrant struct {
	Boundary string `json:"boundary"`
	Risk     string `json:"risk"`
}

type ApprovalActor struct {
	ID                string          `json:"id"`
	OrganizationID    string          `json:"organization_id"`
	Status            OperatorStatus  `json:"status"`
	TokenRef          string          `json:"token_ref"`
	ReviewRef         string          `json:"review_ref"`
	ExpiresAt         *time.Time      `json:"expires_at"`
	MaxConcurrent     int             `json:"max_concurrent"`
	RequestsPerMinute int             `json:"requests_per_minute"`
	Grants            []ApprovalGrant `json:"grants"`
	BearerToken       string          `json:"-"`
}

type ApprovalActorConfig struct {
	Actors []ApprovalActor `json:"actors"`
}

type approvalAuthority struct {
	organizationID core.ID
	status         OperatorStatus
	expiresAt      time.Time
	grants         map[ApprovalGrant]struct{}
}

// ApprovalActorRegistry is both the credential boundary for the dedicated
// control listener and the exact decision authorizer for the approval domain.
// It is deliberately never passed to the work-intake router.
type ApprovalActorRegistry struct {
	credentials *credentialRegistry
	byActor     map[core.ID]approvalAuthority
	now         func() time.Time
}

func DecodeApprovalActorConfig(reader io.Reader) ([]ApprovalActor, error) {
	var config ApprovalActorConfig
	return trustconfig.DecodeEntries(reader, "approval actor registry", "actor", &config, &config.Actors)
}

func NewApprovalActorRegistry(actors []ApprovalActor) (*ApprovalActorRegistry, error) {
	if len(actors) == 0 {
		return nil, fmt.Errorf("at least one approval actor is required")
	}
	entries := make([]credentialEntry, 0, len(actors))
	authorities := make(map[core.ID]approvalAuthority, len(actors))
	for _, actor := range actors {
		if err := validateApprovalActor(actor); err != nil {
			return nil, fmt.Errorf("approval actor %q: %w", actor.ID, err)
		}
		actorID := core.ID(actor.ID)
		if _, exists := authorities[actorID]; exists {
			return nil, fmt.Errorf("approval actor id %q is duplicated", actor.ID)
		}
		grants := make(map[ApprovalGrant]struct{}, len(actor.Grants))
		for _, grant := range actor.Grants {
			if _, exists := grants[grant]; exists {
				return nil, fmt.Errorf("approval actor %q has duplicate grant %s/%s", actor.ID, grant.Boundary, grant.Risk)
			}
			grants[grant] = struct{}{}
		}
		authorities[actorID] = approvalAuthority{
			organizationID: core.ID(actor.OrganizationID), status: actor.Status,
			expiresAt: actor.ExpiresAt.UTC(), grants: grants,
		}
		entries = append(entries, credentialEntry{
			principal: operatorPrincipal(actor.ID, core.PrincipalHuman, actor.OrganizationID, approvalControlChannel, nil, intake.WorkScopeOrganization),
			status:    actor.Status, expiresAt: actor.ExpiresAt.UTC(), bearerToken: actor.BearerToken,
			maxConcurrent: actor.MaxConcurrent, requestsPerMinute: actor.RequestsPerMinute,
		})
	}
	credentials, err := newCredentialRegistry(entries)
	if err != nil {
		return nil, err
	}
	return &ApprovalActorRegistry{credentials: credentials, byActor: authorities, now: time.Now}, nil
}

func (r *ApprovalActorRegistry) Acquire(token string) (*OperatorSession, error) {
	if r == nil {
		return nil, ErrOperatorUnauthorized
	}
	return r.credentials.acquire(token)
}

func (r *ApprovalActorRegistry) CanDecide(_ context.Context, approval core.HumanApproval, humanID core.ID) bool {
	if r == nil || r.now == nil {
		return false
	}
	authority, ok := r.byActor[humanID]
	if !ok || authority.status != OperatorActive || !r.now().UTC().Before(authority.expiresAt) || authority.organizationID != approval.OrganizationID {
		return false
	}
	_, ok = authority.grants[ApprovalGrant{Boundary: approval.Boundary, Risk: approval.Risk}]
	return ok
}

func (r *ApprovalActorRegistry) operatorCredentials() *credentialRegistry {
	if r == nil {
		return nil
	}
	return r.credentials
}

func validateApprovalActor(actor ApprovalActor) error {
	if actor.ID == "" || actor.OrganizationID == "" || actor.TokenRef == "" || actor.ReviewRef == "" {
		return fmt.Errorf("id, organization_id, token_ref, and review_ref are required")
	}
	if err := validateOperatorIdentity(actor.ID, actor.OrganizationID); err != nil {
		return err
	}
	if err := trustconfig.ValidateCredentialLifecycle(string(actor.Status), actor.BearerToken, actor.ExpiresAt); err != nil {
		return err
	}
	if actor.MaxConcurrent < 1 || actor.MaxConcurrent > 64 {
		return fmt.Errorf("max_concurrent must be between 1 and 64")
	}
	if actor.RequestsPerMinute < 1 || actor.RequestsPerMinute > 10_000 {
		return fmt.Errorf("requests_per_minute must be between 1 and 10000")
	}
	if len(actor.Grants) == 0 {
		return fmt.Errorf("at least one exact boundary/risk grant is required")
	}
	for _, grant := range actor.Grants {
		required, err := approvals.RequiresHumanApproval(grant.Boundary)
		if err != nil || !required {
			return fmt.Errorf("grant boundary is not a recognized human consequence boundary")
		}
		if !approvals.ValidDecisionLevel(grant.Risk) {
			return fmt.Errorf("grant risk must be LOW, MEDIUM, HIGH, or CRITICAL")
		}
	}
	return nil
}
