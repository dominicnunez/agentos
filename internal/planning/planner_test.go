package planning

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type plannerModel struct {
	text  string
	err   error
	calls int
}

func (*plannerModel) Descriptor() Descriptor {
	return Descriptor{Provider: "test-provider", Model: "test-model", ExecutionProfileVersion: "test-profile"}
}

func (m *plannerModel) CompleteText(context.Context, string) (TextCompletion, error) {
	m.calls++
	if m.err != nil {
		return TextCompletion{}, m.err
	}
	return TextCompletion{Text: m.text, Usage: events.InferenceUsageRecordedPayload{
		Source: "test", Provider: "test-provider", Model: "test-model",
	}}, nil
}

func acceptedDraft() core.IntentDraft {
	return core.IntentDraft{Objective: "prepare and verify a release candidate"}
}

func TestModelPlannerSkipsInferenceForExactDeterministicWork(t *testing.T) {
	model := &plannerModel{err: errors.New("must not be called")}
	planner, err := NewModelPlanner(model)
	if err != nil {
		t.Fatal(err)
	}
	result, err := planner.Build(context.Background(), core.IntentDraft{Objective: "echo hello"}, core.ExecutionDeterministic)
	if err != nil || model.calls != 0 || len(result.Tasks) != 1 || result.Tasks[0].Key != "root" || result.Usage != nil {
		t.Fatalf("result=%+v calls=%d err=%v", result, model.calls, err)
	}
}

func TestModelPlannerBuildsRuntimeOwnedIntegrationRoot(t *testing.T) {
	model := &plannerModel{text: `{"tasks":[{"key":"prepare","description":"prepare the candidate","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]},{"key":"verify","description":"verify the candidate","execution_kind":"AGENT","model_inference_policy":"ALLOWED_IF_JUSTIFIED","depends_on":["prepare"]}]}`}
	planner, err := NewModelPlanner(model)
	if err != nil {
		t.Fatal(err)
	}
	result, err := planner.Build(context.Background(), acceptedDraft(), core.ExecutionAgent)
	if err != nil {
		t.Fatal(err)
	}
	if model.calls != 1 || result.Usage == nil || len(result.Tasks) != 3 {
		t.Fatalf("result=%+v calls=%d", result, model.calls)
	}
	root := result.Tasks[2]
	if root.Key != "root" || root.Description != acceptedDraft().Objective || root.ExecutionKind != core.ExecutionAgent || root.ModelInferencePolicy != core.InferenceAllowed || len(root.DependsOn) != 1 || root.DependsOn[0] != "verify" {
		t.Fatalf("root=%+v", root)
	}
}

func TestModelPlannerAllowsNoValueDecomposition(t *testing.T) {
	model := &plannerModel{text: `{"tasks":[]}`}
	planner, err := NewModelPlanner(model)
	if err != nil {
		t.Fatal(err)
	}
	result, err := planner.Build(context.Background(), acceptedDraft(), core.ExecutionAgent)
	if err != nil || len(result.Tasks) != 1 || result.Tasks[0].Key != "root" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestModelPlannerRejectsUntrustedGraphExpansion(t *testing.T) {
	tests := map[string]string{
		"unknown field":      `{"tasks":[],"authority":"admin"}`,
		"trailing content":   `{"tasks":[]} {}`,
		"reserved root":      `{"tasks":[{"key":"root","description":"work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}]}`,
		"unknown dependency": `{"tasks":[{"key":"work","description":"work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":["missing"]}]}`,
		"cycle":              `{"tasks":[{"key":"one","description":"one","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":["two"]},{"key":"two","description":"two","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":["one"]}]}`,
		"user task":          `{"tasks":[{"key":"ask","description":"ask user","execution_kind":"HUMAN","model_inference_policy":"DISALLOWED","depends_on":[]}]}`,
		"fake deterministic": `{"tasks":[{"key":"delete","description":"delete everything","execution_kind":"DETERMINISTIC","model_inference_policy":"DISALLOWED","depends_on":[]}]}`,
	}
	for name, response := range tests {
		t.Run(name, func(t *testing.T) {
			planner, err := NewModelPlanner(&plannerModel{text: response})
			if err != nil {
				t.Fatal(err)
			}
			result, err := planner.Build(context.Background(), acceptedDraft(), core.ExecutionAgent)
			if err == nil || result.Usage == nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestModelPlannerCapsTotalTaskCount(t *testing.T) {
	items := make([]string, 0, MaximumPlanTasks)
	for index := 0; index < MaximumPlanTasks; index++ {
		items = append(items, `{"key":"task-`+string(rune('a'+index))+`","description":"work","execution_kind":"AGENT","model_inference_policy":"REQUIRED","depends_on":[]}`)
	}
	planner, err := NewModelPlanner(&plannerModel{text: `{"tasks":[` + strings.Join(items, ",") + `]}`})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planner.Build(context.Background(), acceptedDraft(), core.ExecutionAgent); err == nil {
		t.Fatal("oversized Task DAG was accepted")
	}
}
