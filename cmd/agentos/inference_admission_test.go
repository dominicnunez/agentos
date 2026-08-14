package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/dominicnunez/agentos/internal/inference"
)

type inferenceAdmissionFixture struct {
	steps       []string
	validateErr error
	validateAt  int
}

func (f *inferenceAdmissionFixture) ValidateInferenceAdmissions(context.Context) error {
	f.steps = append(f.steps, "validate")
	if len(f.steps) == f.validateAt {
		return f.validateErr
	}
	return nil
}

func (f *inferenceAdmissionFixture) RecoverInferenceReservations(context.Context, string) (int, error) {
	f.steps = append(f.steps, "recover")
	return 1, nil
}

func (f *inferenceAdmissionFixture) ActivateInferencePolicy(context.Context, inference.Policy) error {
	f.steps = append(f.steps, "activate")
	return nil
}

func TestPrepareInferenceAdmissionsValidatesBeforeAndAfterMutation(t *testing.T) {
	fixture := &inferenceAdmissionFixture{}
	recovered, err := prepareInferenceAdmissions(t.Context(), fixture, inference.Policy{OrganizationID: "organization-1"})
	want := []string{"validate", "recover", "activate", "validate"}
	if err != nil || recovered != 1 || !reflect.DeepEqual(fixture.steps, want) {
		t.Fatalf("startup sequence=%v recovered=%d err=%v", fixture.steps, recovered, err)
	}
}

func TestPrepareInferenceAdmissionsRejectsTamperingBeforeMutation(t *testing.T) {
	tampered := errors.New("tampered accounting")
	fixture := &inferenceAdmissionFixture{validateErr: tampered, validateAt: 1}
	if _, err := prepareInferenceAdmissions(t.Context(), fixture, inference.Policy{OrganizationID: "organization-1"}); !errors.Is(err, tampered) {
		t.Fatalf("tampered accounting was not rejected: %v", err)
	}
	if want := []string{"validate"}; !reflect.DeepEqual(fixture.steps, want) {
		t.Fatalf("startup mutated a ledger before validation: %v", fixture.steps)
	}
}
