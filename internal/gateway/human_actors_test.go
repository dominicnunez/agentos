package gateway

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

func TestHumanActorRegistryEnforcesLifecycleAndRole(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	registry, err := NewHumanActorRegistry([]HumanActor{{
		ID: "human-1", OrganizationID: "org-1", Status: OperatorActive,
		Role: HumanRoleObserver, WorkScope: intake.WorkScopeOrganization,
		TokenRef: "HUMAN_TOKEN", ReviewRef: "review-1", ExpiresAt: &expires,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testHumanToken,
	}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Acquire(testHumanToken)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	if session.Principal.Kind != core.PrincipalHuman || !session.Principal.Allowed(intake.CapabilityReadStatus) || session.Principal.Allowed(intake.CapabilitySubmitWork) {
		t.Fatalf("principal=%+v", session.Principal)
	}

	expired := time.Now().UTC().Add(-time.Second)
	denied, err := NewHumanActorRegistry([]HumanActor{{
		ID: "human-2", OrganizationID: "org-1", Status: OperatorActive,
		Role: HumanRoleOperator, WorkScope: intake.WorkScopeOrganization,
		TokenRef: "EXPIRED_TOKEN", ReviewRef: "review-2", ExpiresAt: &expired,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: "expired-human-operator-token-000001",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := denied.Acquire("expired-human-operator-token-000001"); !errors.Is(err, ErrOperatorUnauthorized) {
		t.Fatalf("expired credential error=%v", err)
	}
}

func TestHumanActorRegistryRejectsUnreviewedOrAmbiguousConfig(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	base := HumanActor{
		ID: "human-1", OrganizationID: "org-1", Status: OperatorActive,
		Role: HumanRoleOperator, WorkScope: intake.WorkScopeOrganization,
		TokenRef: "HUMAN_TOKEN", ReviewRef: "review-1", ExpiresAt: &expires,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testHumanToken,
	}
	for _, test := range []struct {
		name   string
		mutate func(*HumanActor)
	}{
		{name: "missing review", mutate: func(actor *HumanActor) { actor.ReviewRef = "" }},
		{name: "unknown role", mutate: func(actor *HumanActor) { actor.Role = "ADMIN" }},
		{name: "unsupported own scope", mutate: func(actor *HumanActor) { actor.WorkScope = intake.WorkScopeOwn }},
		{name: "weak credential", mutate: func(actor *HumanActor) { actor.BearerToken = "short" }},
		{name: "invalid actor id", mutate: func(actor *HumanActor) { actor.ID = "human with spaces" }},
		{name: "invalid organization id", mutate: func(actor *HumanActor) { actor.OrganizationID = strings.Repeat("o", 257) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := base
			test.mutate(&actor)
			if _, err := NewHumanActorRegistry([]HumanActor{actor}); err == nil {
				t.Fatal("invalid actor was accepted")
			}
		})
	}

	valid := `{"actors":[{"id":"human-1","organization_id":"org-1","status":"ACTIVE","role":"OBSERVER","work_scope":"ORGANIZATION","token_ref":"HUMAN_TOKEN","review_ref":"review-1","expires_at":"2099-01-01T00:00:00Z","max_concurrent":1,"requests_per_minute":10}]}`
	if _, err := DecodeHumanActorConfig(strings.NewReader(valid + `{}`)); err == nil {
		t.Fatal("trailing registry content was accepted")
	}
	if _, err := DecodeHumanActorConfig(strings.NewReader(strings.Replace(valid, `"role":"OBSERVER"`, `"role":"OBSERVER","unknown":true`, 1))); err == nil {
		t.Fatal("unknown registry field was accepted")
	}
}

func TestOperatorRegistriesRejectCrossChannelCredentialReuse(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	humans, err := NewHumanActorRegistry([]HumanActor{{
		ID: "human", OrganizationID: "org", Status: OperatorActive, Role: HumanRoleOperator,
		WorkScope: intake.WorkScopeOrganization, TokenRef: "HUMAN", ReviewRef: "review-human",
		ExpiresAt: &expires, MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testHumanToken,
	}})
	if err != nil {
		t.Fatal(err)
	}
	agents, err := NewExternalActorRegistry([]ExternalActor{{
		ID: "agent", OrganizationID: "org", Status: OperatorActive, Role: ExternalRoleOperator,
		WorkScope: intake.WorkScopeOrganization, TokenRef: "AGENT", ReviewRef: "review-agent",
		ExpiresAt: &expires, MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testHumanToken,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !OperatorRegistriesOverlap(humans, agents) {
		t.Fatal("cross-channel credential reuse was not detected")
	}
}
