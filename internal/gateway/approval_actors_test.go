package gateway

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/intake"
)

const testApprovalToken = "approval-control-token-000000000001"

func TestApprovalActorRegistryEnforcesExactGrantAndLifecycle(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	registry, err := NewApprovalActorRegistry([]ApprovalActor{{
		ID: "approver-1", OrganizationID: "org-1", Status: OperatorActive,
		TokenRef: "APPROVAL_TOKEN", ReviewRef: "review-1", ExpiresAt: &expires,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testApprovalToken,
		Grants: []ApprovalGrant{{Boundary: core.BoundaryDeployment, Risk: "HIGH"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Acquire(testApprovalToken)
	if err != nil {
		t.Fatal(err)
	}
	session.Release()
	approval := core.HumanApproval{OrganizationID: "org-1", Boundary: core.BoundaryDeployment, Risk: "HIGH"}
	if !registry.CanDecide(t.Context(), approval, "approver-1") {
		t.Fatal("exact decision grant was denied")
	}
	for _, changed := range []core.HumanApproval{
		{OrganizationID: "org-2", Boundary: core.BoundaryDeployment, Risk: "HIGH"},
		{OrganizationID: "org-1", Boundary: core.BoundaryDeployment, Risk: "CRITICAL"},
		{OrganizationID: "org-1", Boundary: core.BoundaryFinancial, Risk: "HIGH"},
	} {
		if registry.CanDecide(t.Context(), changed, "approver-1") {
			t.Fatalf("non-exact grant was accepted: %+v", changed)
		}
	}
}

func TestApprovalActorRegistryRejectsInvalidOrAmbiguousConfig(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	base := ApprovalActor{
		ID: "approver-1", OrganizationID: "org-1", Status: OperatorActive,
		TokenRef: "APPROVAL_TOKEN", ReviewRef: "review-1", ExpiresAt: &expires,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testApprovalToken,
		Grants: []ApprovalGrant{{Boundary: core.BoundaryDeployment, Risk: "HIGH"}},
	}
	for _, test := range []struct {
		name   string
		mutate func(*ApprovalActor)
	}{
		{name: "missing review", mutate: func(actor *ApprovalActor) { actor.ReviewRef = "" }},
		{name: "unknown boundary", mutate: func(actor *ApprovalActor) { actor.Grants[0].Boundary = "EVERYTHING" }},
		{name: "unknown risk", mutate: func(actor *ApprovalActor) { actor.Grants[0].Risk = "SEVERE" }},
		{name: "empty grants", mutate: func(actor *ApprovalActor) { actor.Grants = nil }},
		{name: "weak credential", mutate: func(actor *ApprovalActor) { actor.BearerToken = "short" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			actor := base
			actor.Grants = append([]ApprovalGrant(nil), base.Grants...)
			test.mutate(&actor)
			if _, err := NewApprovalActorRegistry([]ApprovalActor{actor}); err == nil {
				t.Fatal("invalid approval actor was accepted")
			}
		})
	}
	duplicate := base
	duplicate.Grants = append(duplicate.Grants, duplicate.Grants[0])
	if _, err := NewApprovalActorRegistry([]ApprovalActor{duplicate}); err == nil {
		t.Fatal("duplicate exact grant was accepted")
	}
	valid := `{"actors":[{"id":"approver-1","organization_id":"org-1","status":"ACTIVE","token_ref":"APPROVAL_TOKEN","review_ref":"review-1","expires_at":"2099-01-01T00:00:00Z","max_concurrent":1,"requests_per_minute":10,"grants":[{"boundary":"AGENT_OS_DEPLOYMENT","risk":"HIGH"}]}]}`
	if _, err := DecodeApprovalActorConfig(strings.NewReader(strings.Replace(valid, `"risk":"HIGH"`, `"risk":"HIGH","inherit":true`, 1))); err == nil {
		t.Fatal("unknown grant field was accepted")
	}
}

func TestApprovalActorRegistryRejectsExpiredAndRevokedCredentials(t *testing.T) {
	expired := time.Now().UTC().Add(-time.Second)
	for _, status := range []OperatorStatus{OperatorActive, OperatorRevoked} {
		registry, err := NewApprovalActorRegistry([]ApprovalActor{{
			ID: "approver-1", OrganizationID: "org-1", Status: status,
			TokenRef: "APPROVAL_TOKEN", ReviewRef: "review-1", ExpiresAt: &expired,
			MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testApprovalToken,
			Grants: []ApprovalGrant{{Boundary: core.BoundaryDeployment, Risk: "HIGH"}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := registry.Acquire(testApprovalToken); !errors.Is(err, ErrOperatorUnauthorized) {
			t.Fatalf("status=%s acquire error=%v", status, err)
		}
		if registry.CanDecide(t.Context(), core.HumanApproval{OrganizationID: "org-1", Boundary: core.BoundaryDeployment, Risk: "HIGH"}, "approver-1") {
			t.Fatalf("status=%s retained decision authority", status)
		}
	}
}

func TestOperatorRegistriesRejectIdentityReuseAcrossApprovalControl(t *testing.T) {
	expires := time.Now().UTC().Add(time.Hour)
	humans, err := NewHumanActorRegistry([]HumanActor{{
		ID: "shared-id", OrganizationID: "org-1", Status: OperatorActive,
		Role: HumanRoleOperator, WorkScope: intake.WorkScopeOrganization,
		TokenRef: "HUMAN", ReviewRef: "review-human", ExpiresAt: &expires,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testHumanToken,
	}})
	if err != nil {
		t.Fatal(err)
	}
	approvers, err := NewApprovalActorRegistry([]ApprovalActor{{
		ID: "shared-id", OrganizationID: "org-1", Status: OperatorActive,
		TokenRef: "APPROVAL", ReviewRef: "review-approval", ExpiresAt: &expires,
		MaxConcurrent: 1, RequestsPerMinute: 10, BearerToken: testApprovalToken,
		Grants: []ApprovalGrant{{Boundary: core.BoundaryDeployment, Risk: "HIGH"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !OperatorRegistriesOverlap(humans, approvers) {
		t.Fatal("cross-channel identity reuse was not detected")
	}
}
