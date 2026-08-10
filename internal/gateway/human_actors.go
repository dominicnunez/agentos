package gateway

import (
	"fmt"
	"io"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

type HumanRole string

const (
	HumanRoleContributor  HumanRole = "CONTRIBUTOR"
	HumanRoleObserver     HumanRole = "OBSERVER"
	HumanRoleResultReader HumanRole = "RESULT_READER"
	HumanRoleOperator     HumanRole = "OPERATOR"
	HumanRoleReviewer     HumanRole = "REVIEWER"
)

type HumanActor struct {
	ID                string           `json:"id"`
	OrganizationID    string           `json:"organization_id"`
	Status            OperatorStatus   `json:"status"`
	Role              HumanRole        `json:"role"`
	WorkScope         intake.WorkScope `json:"work_scope"`
	TokenRef          string           `json:"token_ref"`
	ReviewRef         string           `json:"review_ref"`
	ExpiresAt         *time.Time       `json:"expires_at"`
	MaxConcurrent     int              `json:"max_concurrent"`
	RequestsPerMinute int              `json:"requests_per_minute"`
	BearerToken       string           `json:"-"`
}

type HumanActorConfig struct {
	Actors []HumanActor `json:"actors"`
}

type HumanActorRegistry struct {
	credentials *credentialRegistry
}

func (r *HumanActorRegistry) operatorCredentials() *credentialRegistry {
	if r == nil {
		return nil
	}
	return r.credentials
}

func DecodeHumanActorConfig(reader io.Reader) ([]HumanActor, error) {
	var config HumanActorConfig
	return trustconfig.DecodeEntries(reader, "human actor registry", "actor", &config, &config.Actors)
}

func NewHumanActorRegistry(actors []HumanActor) (*HumanActorRegistry, error) {
	if len(actors) == 0 {
		return nil, fmt.Errorf("at least one human actor is required")
	}
	entries := make([]credentialEntry, 0, len(actors))
	actorIDs := make(map[string]struct{}, len(actors))
	for _, actor := range actors {
		if err := validateHumanActor(actor); err != nil {
			return nil, fmt.Errorf("human actor %q: %w", actor.ID, err)
		}
		if _, exists := actorIDs[actor.ID]; exists {
			return nil, fmt.Errorf("human actor id %q is duplicated", actor.ID)
		}
		actorIDs[actor.ID] = struct{}{}
		entries = append(entries, credentialEntry{
			principal: operatorPrincipal(actor.ID, core.PrincipalHuman, actor.OrganizationID, intake.ChannelHumanDirect, humanCapabilities(actor.Role), actor.WorkScope),
			status:    actor.Status, expiresAt: actor.ExpiresAt.UTC(), bearerToken: actor.BearerToken,
			maxConcurrent: actor.MaxConcurrent, requestsPerMinute: actor.RequestsPerMinute,
		})
	}
	credentials, err := newCredentialRegistry(entries)
	if err != nil {
		return nil, err
	}
	return &HumanActorRegistry{credentials: credentials}, nil
}

func (r *HumanActorRegistry) Acquire(token string) (*OperatorSession, error) {
	if r == nil {
		return nil, ErrOperatorUnauthorized
	}
	return r.credentials.acquire(token)
}

func validateHumanActor(actor HumanActor) error {
	if actor.ID == "" || actor.OrganizationID == "" || actor.TokenRef == "" || actor.ReviewRef == "" {
		return fmt.Errorf("id, organization_id, token_ref, and review_ref are required")
	}
	if err := validateOperatorIdentity(actor.ID, actor.OrganizationID); err != nil {
		return err
	}
	if err := trustconfig.ValidateCredentialLifecycle(string(actor.Status), actor.BearerToken, actor.ExpiresAt); err != nil {
		return err
	}
	if actor.WorkScope != intake.WorkScopeOrganization {
		return fmt.Errorf("work_scope must be ORGANIZATION for a human actor")
	}
	if humanCapabilities(actor.Role) == nil {
		return fmt.Errorf("role must be CONTRIBUTOR, OBSERVER, RESULT_READER, OPERATOR, or REVIEWER")
	}
	if actor.MaxConcurrent < 1 || actor.MaxConcurrent > 64 {
		return fmt.Errorf("max_concurrent must be between 1 and 64")
	}
	if actor.RequestsPerMinute < 1 || actor.RequestsPerMinute > 10_000 {
		return fmt.Errorf("requests_per_minute must be between 1 and 10000")
	}
	return nil
}

func humanCapabilities(role HumanRole) []string {
	switch role {
	case HumanRoleContributor:
		return []string{intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityProvideInput}
	case HumanRoleObserver:
		return []string{intake.CapabilityReadStatus}
	case HumanRoleResultReader:
		return []string{intake.CapabilityReadStatus, intake.CapabilityReadResult}
	case HumanRoleOperator:
		return []string{intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityReadResult, intake.CapabilityProvideInput}
	case HumanRoleReviewer:
		return []string{intake.CapabilityReadStatus, intake.CapabilityReviewCompletion}
	default:
		return nil
	}
}
