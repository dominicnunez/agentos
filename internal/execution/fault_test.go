package execution

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestSafeModelErrorDropsDiagnosticsAndRetainsOnlyControlFacts(t *testing.T) {
	secret := errors.New("Authorization: Bearer synthetic-private-canary")
	for _, tc := range []struct {
		name                         string
		err                          error
		notSent, cancelled, deadline bool
	}{
		{name: "provider", err: secret},
		{name: "pre-send", err: RequestNotSent(secret), notSent: true},
		{name: "cancelled", err: errors.Join(secret, context.Canceled), cancelled: true},
		{name: "deadline", err: errors.Join(secret, context.DeadlineExceeded), deadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fault := SafeModelError(ModelCallFailed, tc.err)
			if fault == nil || ModelErrorClass(fault) != "provider_failure" ||
				WasRequestNotSent(fault) != tc.notSent || errors.Is(fault, context.Canceled) != tc.cancelled ||
				errors.Is(fault, context.DeadlineExceeded) != tc.deadline || errors.Is(fault, secret) {
				t.Fatal("sanitized fault changed control facts or retained its private cause")
			}
			for current := fault; current != nil; current = errors.Unwrap(current) {
				if strings.Contains(fmt.Sprintf("%v %+v %#v", current, current, current), "synthetic-private-canary") {
					t.Fatal("private diagnostic survived sanitization")
				}
			}
		})
	}
	if SafeModelError(ModelCallFailed, nil) != nil {
		t.Fatal("nil became a failure")
	}
	unknown := SafeModelError(ModelFaultCode("synthetic-private-canary"), secret)
	if ModelErrorClass(unknown) != "provider_failure" || strings.Contains(unknown.Error(), "synthetic-private-canary") {
		t.Fatal("unrecognized category became output content")
	}
}

func TestSafeModelErrorPreservesSpecificFaultButAllowsAccountingFailure(t *testing.T) {
	prior := SafeModelError(InferenceDenied, errors.New("private detail"))
	wrapped := fmt.Errorf("private outer detail: %w", prior)
	if ModelErrorClass(SafeModelError(ModelCallFailed, wrapped)) != string(InferenceDenied) {
		t.Fatal("generic execution boundary discarded the specific safe category")
	}
	if ModelErrorClass(SafeModelError(InferenceRecordFailed, wrapped)) != string(InferenceRecordFailed) {
		t.Fatal("provider category masked a later accounting failure")
	}
}

type secretErrorModel struct{ FakeModel }

func TestSafeModelErrorPreservesUnavailableContainment(t *testing.T) {
	secret := errors.New("private ledger diagnostic")
	err := SafeModelError(InferenceDenied, errors.Join(core.ErrContainmentUnavailable, secret))
	err = SafeModelError(ModelCallFailed, err)
	if !errors.Is(err, core.ErrContainmentUnavailable) || errors.Is(err, secret) || strings.Contains(err.Error(), secret.Error()) {
		t.Fatal("sanitization lost containment control fact or retained diagnostics")
	}
}

func (secretErrorModel) Complete(context.Context, string) (ModelResponse, error) {
	return ModelResponse{}, errors.New("Authorization: Bearer synthetic-private-canary")
}

func TestAgentExecutionDoesNotExposeProviderDiagnostics(t *testing.T) {
	model := secretErrorModel{}
	descriptor := model.Descriptor()
	result, err := NewAgentExecution(model).Execute(t.Context(),
		core.Task{ID: "task", Description: "work", ModelInferencePolicy: core.InferenceAllowed},
		core.ExecutionContextManifest{Provider: descriptor.Provider, Model: descriptor.Model, ExecutionProfileVersion: descriptor.ExecutionProfileVersion})
	if err == nil || result.Outcome.Status != core.OutcomeFailed || result.Outcome.ErrorClass != "provider_failure" || result.InferenceUsage != nil {
		t.Fatal("provider failure did not produce a typed failed outcome")
	}
	if strings.Contains(result.Outcome.ErrorDetail+fmt.Sprint(err), "synthetic-private-canary") {
		t.Fatal("provider diagnostic crossed into work evidence or returned error")
	}
}

func TestSafeModelErrorPreservesHoldWithoutDiagnostics(t *testing.T) {
	hold := core.SecurityHoldCause{OrganizationID: "org-1", EventRef: "freeze-1", Sequence: 4}
	secret := errors.New("private provider diagnostic")
	err := errors.Join(secret, hold, context.Canceled)
	for range 2 {
		err = SafeModelError(ModelCallFailed, err)
		var retained core.SecurityHoldCause
		if !errors.Is(err, core.ErrOrganizationFrozen) || !errors.Is(err, context.Canceled) || !errors.As(err, &retained) || retained != hold {
			t.Fatalf("hold control evidence lost: %v", err)
		}
		if errors.Is(err, secret) || errors.Unwrap(err) != nil || strings.Contains(err.Error(), secret.Error()) {
			t.Fatal("provider diagnostics escaped sanitization")
		}
	}
}
