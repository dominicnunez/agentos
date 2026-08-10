package gateway

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/intake"
)

func TestExternalActorRegistryRejectsUnknownExpiredAndRevokedActors(t *testing.T) {
	now := time.Now().UTC()
	active := testExternalActor("active", "org-1", testExternalToken, ExternalRoleCollaborator, intake.WorkScopeOwn)
	expired := testExternalActor("expired", "org-1", testObserverToken, ExternalRoleOperator, intake.WorkScopeOrganization)
	expired.ExpiresAt = timePointer(now.Add(-time.Minute))
	revoked := testExternalActor("revoked", "org-1", testOtherToken, ExternalRoleOperator, intake.WorkScopeOrganization)
	revoked.Status = OperatorRevoked
	suspended := testExternalActor("suspended", "org-1", testOwnReaderToken, ExternalRoleOperator, intake.WorkScopeOrganization)
	suspended.Status = OperatorSuspended
	registry, err := NewExternalActorRegistry([]ExternalActor{active, expired, revoked, suspended})
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{
		"unknown":   "unknown-external-agent-token-00001",
		"expired":   testObserverToken,
		"revoked":   testOtherToken,
		"suspended": testOwnReaderToken,
	} {
		if _, err := registry.Acquire(token); !errors.Is(err, ErrOperatorUnauthorized) {
			t.Fatalf("%s actor err=%v", name, err)
		}
	}
	session, err := registry.Acquire(testExternalToken)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	if session.Principal.ID != "active" || session.Principal.WorkScope != intake.WorkScopeOwn || !session.Principal.Allowed(intake.CapabilityProvideInput) || session.Principal.Allowed(intake.CapabilityReadResult) {
		t.Fatalf("collaborator principal=%+v", session.Principal)
	}
}

func TestExternalActorRegistryEnforcesCredentialAndRequestLimits(t *testing.T) {
	actor := testExternalActor("limited", "org-1", testExternalToken, ExternalRoleSubmitter, intake.WorkScopeOwn)
	actor.MaxConcurrent = 1
	actor.RequestsPerMinute = 2
	registry, err := NewExternalActorRegistry([]ExternalActor{actor})
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.Acquire(testExternalToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire(testExternalToken); !errors.Is(err, ErrOperatorLimited) {
		t.Fatalf("concurrent request err=%v", err)
	}
	first.Release()
	second, err := registry.Acquire(testExternalToken)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if _, err := registry.Acquire(testExternalToken); !errors.Is(err, ErrOperatorLimited) {
		t.Fatalf("per-minute request err=%v", err)
	}
}

func TestExternalActorRegistryRejectsAmbiguousOrWeakAuthority(t *testing.T) {
	valid := testExternalActor("one", "org-1", testExternalToken, ExternalRoleOperator, intake.WorkScopeOwn)
	tests := []struct {
		name   string
		actors []ExternalActor
	}{
		{name: "duplicate id", actors: []ExternalActor{valid, withActorIdentity(testExternalActor("two", "org-1", testObserverToken, ExternalRoleOperator, intake.WorkScopeOwn), "one")}},
		{name: "duplicate credential", actors: []ExternalActor{valid, testExternalActor("two", "org-1", testExternalToken, ExternalRoleOperator, intake.WorkScopeOwn)}},
		{name: "weak credential", actors: []ExternalActor{withActorCredential(valid, "short")}},
		{name: "unknown role", actors: []ExternalActor{withActorRole(valid, "ADMIN")}},
		{name: "invalid actor id", actors: []ExternalActor{withActorIdentity(valid, "actor with spaces")}},
		{name: "invalid organization id", actors: []ExternalActor{withActorOrganization(valid, "org\nforged")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewExternalActorRegistry(test.actors); err == nil {
				t.Fatal("invalid actor registry was accepted")
			}
		})
	}
}

func TestDecodeExternalActorConfigRejectsUnknownAndTrailingContent(t *testing.T) {
	valid := `{"actors":[{"id":"agent","organization_id":"org","status":"ACTIVE","role":"SUBMITTER","work_scope":"OWN","token_ref":"AGENT_TOKEN","review_ref":"review-1","expires_at":"2099-01-01T00:00:00Z","max_concurrent":1,"requests_per_minute":10}]}`
	actors, err := DecodeExternalActorConfig(strings.NewReader(valid))
	if err != nil || len(actors) != 1 || actors[0].TokenRef != "AGENT_TOKEN" {
		t.Fatalf("actors=%+v err=%v", actors, err)
	}
	for _, invalid := range []string{
		strings.Replace(valid, `"actors"`, `"unknown":true,"actors"`, 1),
		valid + `{}`,
	} {
		if _, err := DecodeExternalActorConfig(strings.NewReader(invalid)); err == nil {
			t.Fatal("invalid actor config was accepted")
		}
	}
}

func timePointer(value time.Time) *time.Time { return &value }

func withActorIdentity(actor ExternalActor, id string) ExternalActor {
	actor.ID = id
	return actor
}

func withActorCredential(actor ExternalActor, token string) ExternalActor {
	actor.BearerToken = token
	return actor
}

func withActorRole(actor ExternalActor, role ExternalActorRole) ExternalActor {
	actor.Role = role
	return actor
}

func withActorOrganization(actor ExternalActor, organizationID string) ExternalActor {
	actor.OrganizationID = organizationID
	return actor
}
