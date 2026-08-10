package gateway

import (
	"fmt"
	"io"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

type ExternalActorRole string

const (
	ExternalRoleSubmitter    ExternalActorRole = "SUBMITTER"
	ExternalRoleCollaborator ExternalActorRole = "COLLABORATOR"
	ExternalRoleObserver     ExternalActorRole = "OBSERVER"
	ExternalRoleResultReader ExternalActorRole = "RESULT_READER"
	ExternalRoleOperator     ExternalActorRole = "OPERATOR"
)

type ExternalActor struct {
	ID                string            `json:"id"`
	OrganizationID    string            `json:"organization_id"`
	Status            OperatorStatus    `json:"status"`
	Role              ExternalActorRole `json:"role"`
	WorkScope         intake.WorkScope  `json:"work_scope"`
	TokenRef          string            `json:"token_ref"`
	ReviewRef         string            `json:"review_ref"`
	ExpiresAt         *time.Time        `json:"expires_at"`
	MaxConcurrent     int               `json:"max_concurrent"`
	RequestsPerMinute int               `json:"requests_per_minute"`
	BearerToken       string            `json:"-"`
}

type ExternalActorConfig struct {
	Actors []ExternalActor `json:"actors"`
}

type ExternalActorRegistry struct {
	credentials *credentialRegistry
}

func (r *ExternalActorRegistry) operatorCredentials() *credentialRegistry {
	if r == nil {
		return nil
	}
	return r.credentials
}

func DecodeExternalActorConfig(reader io.Reader) ([]ExternalActor, error) {
	var config ExternalActorConfig
	return trustconfig.DecodeEntries(reader, "external actor registry", "actor", &config, &config.Actors)
}

func NewExternalActorRegistry(actors []ExternalActor) (*ExternalActorRegistry, error) {
	if len(actors) == 0 {
		return nil, fmt.Errorf("at least one external actor is required")
	}
	entries := make([]credentialEntry, 0, len(actors))
	actorIDs := make(map[string]struct{}, len(actors))
	for _, actor := range actors {
		if err := validateExternalActor(actor); err != nil {
			return nil, fmt.Errorf("external actor %q: %w", actor.ID, err)
		}
		if _, exists := actorIDs[actor.ID]; exists {
			return nil, fmt.Errorf("external actor id %q is duplicated", actor.ID)
		}
		actorIDs[actor.ID] = struct{}{}
		entries = append(entries, credentialEntry{
			principal: operatorPrincipal(
				actor.ID, core.PrincipalExternalAgent, actor.OrganizationID, intake.ChannelA2A,
				actorCapabilities(actor.Role), actor.WorkScope,
			),
			status: actor.Status, expiresAt: actor.ExpiresAt.UTC(), bearerToken: actor.BearerToken,
			maxConcurrent:     actor.MaxConcurrent,
			requestsPerMinute: actor.RequestsPerMinute,
		})
	}
	credentials, err := newCredentialRegistry(entries)
	if err != nil {
		return nil, err
	}
	return &ExternalActorRegistry{credentials: credentials}, nil
}

func (r *ExternalActorRegistry) Acquire(token string) (*OperatorSession, error) {
	if r == nil {
		return nil, ErrOperatorUnauthorized
	}
	return r.credentials.acquire(token)
}

func (r *ExternalActorRegistry) HasCredential(token string) bool {
	return r != nil && r.credentials.hasCredential(token)
}

func validateExternalActor(actor ExternalActor) error {
	if actor.ID == "" || actor.OrganizationID == "" || actor.TokenRef == "" || actor.ReviewRef == "" {
		return fmt.Errorf("id, organization_id, token_ref, and review_ref are required")
	}
	if err := validateOperatorIdentity(actor.ID, actor.OrganizationID); err != nil {
		return err
	}
	if err := trustconfig.ValidateCredentialLifecycle(string(actor.Status), actor.BearerToken, actor.ExpiresAt); err != nil {
		return err
	}
	if actor.WorkScope != intake.WorkScopeOwn && actor.WorkScope != intake.WorkScopeOrganization {
		return fmt.Errorf("work_scope must be OWN or ORGANIZATION")
	}
	if actorCapabilities(actor.Role) == nil {
		return fmt.Errorf("role must be SUBMITTER, COLLABORATOR, OBSERVER, RESULT_READER, or OPERATOR")
	}
	if actor.MaxConcurrent < 1 || actor.MaxConcurrent > 64 {
		return fmt.Errorf("max_concurrent must be between 1 and 64")
	}
	if actor.RequestsPerMinute < 1 || actor.RequestsPerMinute > 10_000 {
		return fmt.Errorf("requests_per_minute must be between 1 and 10000")
	}
	return nil
}

func validateOperatorIdentity(actorID, organizationID string) error {
	if err := intake.ValidateIdentifier("actor", actorID); err != nil {
		return err
	}
	return intake.ValidateIdentifier("organization", organizationID)
}

func actorCapabilities(role ExternalActorRole) []string {
	switch role {
	case ExternalRoleSubmitter:
		return []string{intake.CapabilitySubmitWork}
	case ExternalRoleCollaborator:
		return []string{intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityProvideInput}
	case ExternalRoleObserver:
		return []string{intake.CapabilityReadStatus}
	case ExternalRoleResultReader:
		return []string{intake.CapabilityReadStatus, intake.CapabilityReadResult}
	case ExternalRoleOperator:
		return []string{intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityReadResult, intake.CapabilityProvideInput}
	default:
		return nil
	}
}
