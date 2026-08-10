package gateway

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/trustconfig"
)

type ExternalActorStatus string
type ExternalActorRole string

const (
	ExternalActorActive    ExternalActorStatus = "ACTIVE"
	ExternalActorSuspended ExternalActorStatus = "SUSPENDED"
	ExternalActorRevoked   ExternalActorStatus = "REVOKED"

	ExternalRoleSubmitter    ExternalActorRole = "SUBMITTER"
	ExternalRoleCollaborator ExternalActorRole = "COLLABORATOR"
	ExternalRoleObserver     ExternalActorRole = "OBSERVER"
	ExternalRoleResultReader ExternalActorRole = "RESULT_READER"
	ExternalRoleOperator     ExternalActorRole = "OPERATOR"
)

var (
	ErrActorUnauthorized = errors.New("external actor is not authorized")
	ErrActorLimited      = errors.New("external actor request limit reached")
)

type ExternalActor struct {
	ID                string              `json:"id"`
	OrganizationID    string              `json:"organization_id"`
	Status            ExternalActorStatus `json:"status"`
	Role              ExternalActorRole   `json:"role"`
	WorkScope         intake.WorkScope    `json:"work_scope"`
	TokenRef          string              `json:"token_ref"`
	AuthorizationRef  string              `json:"authorization_ref"`
	ExpiresAt         *time.Time          `json:"expires_at"`
	MaxConcurrent     int                 `json:"max_concurrent"`
	RequestsPerMinute int                 `json:"requests_per_minute"`
	BearerToken       string              `json:"-"`
}

type ExternalActorConfig struct {
	Actors []ExternalActor `json:"actors"`
}

type actorRegistration struct {
	principal         intake.Principal
	status            ExternalActorStatus
	expiresAt         time.Time
	requestsPerMinute int
	slots             chan struct{}

	mu          sync.Mutex
	windowStart time.Time
	requests    int
}

type ExternalActorRegistry struct {
	byCredential map[[sha256.Size]byte]*actorRegistration
	now          func() time.Time
}

type ActorSession struct {
	Principal intake.Principal
	actor     *actorRegistration
	release   sync.Once
}

func DecodeExternalActorConfig(reader io.Reader) ([]ExternalActor, error) {
	var config ExternalActorConfig
	return trustconfig.DecodeEntries(reader, "external actor registry", "actor", &config, &config.Actors)
}

func NewExternalActorRegistry(actors []ExternalActor) (*ExternalActorRegistry, error) {
	if len(actors) == 0 {
		return nil, fmt.Errorf("at least one external actor is required")
	}
	registry := &ExternalActorRegistry{byCredential: make(map[[sha256.Size]byte]*actorRegistration, len(actors)), now: time.Now}
	actorIDs := make(map[string]struct{}, len(actors))
	for _, actor := range actors {
		if err := validateExternalActor(actor); err != nil {
			return nil, fmt.Errorf("external actor %q: %w", actor.ID, err)
		}
		if _, exists := actorIDs[actor.ID]; exists {
			return nil, fmt.Errorf("external actor id %q is duplicated", actor.ID)
		}
		actorIDs[actor.ID] = struct{}{}
		credential := sha256.Sum256([]byte(actor.BearerToken))
		if _, exists := registry.byCredential[credential]; exists {
			return nil, fmt.Errorf("external actor credentials must be unique")
		}
		registry.byCredential[credential] = &actorRegistration{
			principal: operatorPrincipal(
				actor.ID, core.PrincipalExternalAgent, actor.OrganizationID, intake.ChannelA2A,
				actorCapabilities(actor.Role), actor.WorkScope,
			),
			status: actor.Status, expiresAt: actor.ExpiresAt.UTC(),
			requestsPerMinute: actor.RequestsPerMinute,
			slots:             make(chan struct{}, actor.MaxConcurrent),
		}
	}
	return registry, nil
}

func (r *ExternalActorRegistry) Acquire(token string) (*ActorSession, error) {
	if r == nil || token == "" {
		return nil, ErrActorUnauthorized
	}
	actor, ok := r.byCredential[sha256.Sum256([]byte(token))]
	now := r.now().UTC()
	if !ok || actor.status != ExternalActorActive || !now.Before(actor.expiresAt) {
		return nil, ErrActorUnauthorized
	}
	select {
	case actor.slots <- struct{}{}:
	default:
		return nil, ErrActorLimited
	}
	actor.mu.Lock()
	if actor.windowStart.IsZero() || now.Sub(actor.windowStart) >= time.Minute {
		actor.windowStart = now
		actor.requests = 0
	}
	if actor.requests >= actor.requestsPerMinute {
		actor.mu.Unlock()
		<-actor.slots
		return nil, ErrActorLimited
	}
	actor.requests++
	actor.mu.Unlock()
	return &ActorSession{Principal: actor.principal, actor: actor}, nil
}

func (r *ExternalActorRegistry) HasCredential(token string) bool {
	if r == nil || token == "" {
		return false
	}
	_, ok := r.byCredential[sha256.Sum256([]byte(token))]
	return ok
}

func (s *ActorSession) Release() {
	if s == nil || s.actor == nil {
		return
	}
	s.release.Do(func() { <-s.actor.slots })
}

func validateExternalActor(actor ExternalActor) error {
	if actor.ID == "" || actor.OrganizationID == "" || actor.AuthorizationRef == "" {
		return fmt.Errorf("id, organization_id, and authorization_ref are required")
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
