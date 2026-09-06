package main

import (
	"context"
	"errors"
	"github.com/dominicnunez/agentos/internal/inference"
	"reflect"
	"testing"
	"time"
)

type connectionAdmissionFixture struct {
	inferenceAdmissionFixture
	policies    []inference.Policy
	activateErr error
}

func (f *connectionAdmissionFixture) ActivateInferencePolicies(_ context.Context, policies []inference.Policy) error {
	f.steps = append(f.steps, "activate-set")
	f.policies = append([]inference.Policy(nil), policies...)
	return f.activateErr
}
func startupConnectionPolicies() []inference.Policy {
	now := time.Now().UTC()
	budget := &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxConcurrentRequests: 2}
	first := inference.Policy{Version: inference.ConnectionPolicyVersion, ConnectionID: "first", OrganizationBudget: budget, OrganizationID: "organization-1", Provider: "provider", Model: "model", ExecutionProfileVersion: "profile-v1", Mode: inference.Local, MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 240, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour)}
	second := first
	second.ConnectionID = "second"
	return []inference.Policy{first, second}
}
func TestPrepareConnectionPoliciesUsesOneRecoveryAndAtomicActivation(t *testing.T) {
	fixture := &connectionAdmissionFixture{}
	policies := startupConnectionPolicies()
	recovered, err := prepareInferenceAdmissions(t.Context(), fixture, policies...)
	if err != nil || recovered != 1 || !reflect.DeepEqual(fixture.policies, policies) || !reflect.DeepEqual(fixture.steps, []string{"validate", "recover", "activate-set", "validate"}) {
		t.Fatalf("startup steps=%v recovered=%d err=%v", fixture.steps, recovered, err)
	}
}
func TestPrepareConnectionPoliciesRejectsInvalidSetBeforeRecovery(t *testing.T) {
	for _, mutate := range []func([]inference.Policy){
		func(p []inference.Policy) { p[1].ConnectionID = p[0].ConnectionID },
		func(p []inference.Policy) { p[1].OrganizationID = "other" },
		func(p []inference.Policy) { p[1].Version = inference.PolicyVersion },
		func(p []inference.Policy) {
			budget := *p[1].OrganizationBudget
			budget.MaxTokensPerWindow++
			p[1].OrganizationBudget = &budget
		},
	} {
		fixture := &connectionAdmissionFixture{}
		policies := startupConnectionPolicies()
		mutate(policies)
		if _, err := prepareInferenceAdmissions(t.Context(), fixture, policies...); err == nil || len(fixture.steps) != 0 {
			t.Fatalf("invalid set mutated startup: %v, %v", fixture.steps, err)
		}
	}
	legacyStore := &inferenceAdmissionFixture{}
	if _, err := prepareInferenceAdmissions(t.Context(), legacyStore, startupConnectionPolicies()...); err == nil || len(legacyStore.steps) != 0 {
		t.Fatal("non-atomic store accepted connection set")
	}
}
func TestPrepareConnectionPoliciesStopsAfterFailedAtomicActivation(t *testing.T) {
	failed := errors.New("activation failed")
	fixture := &connectionAdmissionFixture{activateErr: failed}
	if _, err := prepareInferenceAdmissions(t.Context(), fixture, startupConnectionPolicies()...); !errors.Is(err, failed) {
		t.Fatalf("activation error lost: %v", err)
	}
	if !reflect.DeepEqual(fixture.steps, []string{"validate", "recover", "activate-set"}) {
		t.Fatalf("continued after failed activation: %v", fixture.steps)
	}
}
