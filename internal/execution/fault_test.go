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
