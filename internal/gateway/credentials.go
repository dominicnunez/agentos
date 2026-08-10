package gateway

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dominicnunez/agentos/internal/intake"
)

type OperatorStatus string

const (
	OperatorActive    OperatorStatus = "ACTIVE"
	OperatorSuspended OperatorStatus = "SUSPENDED"
	OperatorRevoked   OperatorStatus = "REVOKED"
)

var (
	ErrOperatorUnauthorized = errors.New("operator is not authorized")
	ErrOperatorLimited      = errors.New("operator request limit reached")
)

type credentialEntry struct {
	principal         intake.Principal
	status            OperatorStatus
	expiresAt         time.Time
	bearerToken       string
	maxConcurrent     int
	requestsPerMinute int
}

type credentialRegistration struct {
	principal         intake.Principal
	status            OperatorStatus
	expiresAt         time.Time
	requestsPerMinute int
	slots             chan struct{}

	mu          sync.Mutex
	windowStart time.Time
	requests    int
}

type credentialRegistry struct {
	byCredential map[[sha256.Size]byte]*credentialRegistration
	now          func() time.Time
}

type credentialSource interface {
	operatorCredentials() *credentialRegistry
}

type OperatorSession struct {
	Principal intake.Principal
	actor     *credentialRegistration
	release   sync.Once
}

func newCredentialRegistry(entries []credentialEntry) (*credentialRegistry, error) {
	registry := &credentialRegistry{byCredential: make(map[[sha256.Size]byte]*credentialRegistration, len(entries)), now: time.Now}
	for _, entry := range entries {
		credential := sha256.Sum256([]byte(entry.bearerToken))
		if _, exists := registry.byCredential[credential]; exists {
			return nil, fmt.Errorf("operator credentials must be unique")
		}
		registry.byCredential[credential] = &credentialRegistration{
			principal: entry.principal, status: entry.status, expiresAt: entry.expiresAt,
			requestsPerMinute: entry.requestsPerMinute,
			slots:             make(chan struct{}, entry.maxConcurrent),
		}
	}
	return registry, nil
}

func (r *credentialRegistry) acquire(token string) (*OperatorSession, error) {
	if r == nil || token == "" {
		return nil, ErrOperatorUnauthorized
	}
	actor, ok := r.byCredential[sha256.Sum256([]byte(token))]
	now := r.now().UTC()
	if !ok || actor.status != OperatorActive || !now.Before(actor.expiresAt) {
		return nil, ErrOperatorUnauthorized
	}
	select {
	case actor.slots <- struct{}{}:
	default:
		return nil, ErrOperatorLimited
	}
	actor.mu.Lock()
	if actor.windowStart.IsZero() || now.Sub(actor.windowStart) >= time.Minute {
		actor.windowStart = now
		actor.requests = 0
	}
	if actor.requests >= actor.requestsPerMinute {
		actor.mu.Unlock()
		<-actor.slots
		return nil, ErrOperatorLimited
	}
	actor.requests++
	actor.mu.Unlock()
	return &OperatorSession{Principal: actor.principal, actor: actor}, nil
}

func (r *credentialRegistry) hasCredential(token string) bool {
	if r == nil || token == "" {
		return false
	}
	_, ok := r.byCredential[sha256.Sum256([]byte(token))]
	return ok
}

// OperatorRegistriesOverlap detects credential or principal-ID reuse across
// independently authorized ingress channels without retaining plaintext
// credentials. Each channel must represent a distinct security principal even
// when one person operates several of them.
func OperatorRegistriesOverlap(registries ...credentialSource) bool {
	for left := 0; left < len(registries); left++ {
		leftCredentials := registries[left].operatorCredentials()
		if leftCredentials == nil {
			continue
		}
		for right := left + 1; right < len(registries); right++ {
			rightCredentials := registries[right].operatorCredentials()
			if rightCredentials != nil && credentialRegistriesOverlap(leftCredentials, rightCredentials) {
				return true
			}
		}
	}
	return false
}

func credentialRegistriesOverlap(left, right *credentialRegistry) bool {
	identities := make(map[string]struct{}, len(left.byCredential))
	for credential, registration := range left.byCredential {
		if _, exists := right.byCredential[credential]; exists {
			return true
		}
		identities[registration.principal.ID] = struct{}{}
	}
	for _, registration := range right.byCredential {
		if _, exists := identities[registration.principal.ID]; exists {
			return true
		}
	}
	return false
}

func (s *OperatorSession) Release() {
	if s == nil || s.actor == nil {
		return
	}
	s.release.Do(func() { <-s.actor.slots })
}
