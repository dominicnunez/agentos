package lab

import (
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestExperimentLimitEvaluationRejectsBudgetAndContainmentDrift(t *testing.T) {
	started := time.Now().UTC()
	experiment := core.Experiment{
		ID: "experiment-1", OrganizationID: "org-1", WorkID: "work-1", Objective: "bounded",
		SandboxRef: "sandbox-1", CapabilityProfileRef: NoEffectsCapabilityProfile,
		Status: core.ExperimentRunning, TrustLabel: core.ExperimentTrustUnverified, StartedAt: started,
		Budget: core.ExperimentBudget{MaxExecutions: 1, MaxUsageUnits: 1, MaxWallTimeSeconds: 10, AllowedInferencePools: []string{DeterministicInferencePool}},
	}
	task := core.Task{ID: "task-1", WorkID: experiment.WorkID, Description: "echo bounded", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskCompleted}
	snapshot := projections.Snapshot{Tasks: map[core.ID]projections.Versioned[core.Task]{task.ID: {Value: task}}}
	if failure := experimentLimitFailure(snapshot, experiment, started.Add(time.Second)); failure != "" {
		t.Fatalf("bounded deterministic experiment failed: %s", failure)
	}

	adaptive := projections.Snapshot{Tasks: map[core.ID]projections.Versioned[core.Task]{task.ID: snapshot.Tasks[task.ID]}}
	state := adaptive.Tasks[task.ID]
	state.Value.ExecutionKind = core.ExecutionAgent
	state.Value.ModelInferencePolicy = core.InferenceAllowed
	adaptive.Tasks[task.ID] = state
	if failure := experimentLimitFailure(adaptive, experiment, started.Add(time.Second)); failure != core.ExperimentFailureContainmentViolated {
		t.Fatalf("adaptive drift failure=%s", failure)
	}
	if failure := experimentLimitFailure(snapshot, experiment, started.Add(11*time.Second)); failure != core.ExperimentFailureBudgetExceeded {
		t.Fatalf("wall-time breach failure=%s", failure)
	}
	second := task
	second.ID = "task-2"
	snapshot.Tasks[second.ID] = projections.Versioned[core.Task]{Value: second}
	if failure := experimentLimitFailure(snapshot, experiment, started.Add(time.Second)); failure != core.ExperimentFailureBudgetExceeded {
		t.Fatalf("execution-count breach failure=%s", failure)
	}
}

func TestValidatePlanEnforcesBudgetBeforeTaskAdmission(t *testing.T) {
	budget := core.ExperimentBudget{MaxExecutions: 1, MaxUsageUnits: 1, MaxWallTimeSeconds: 10, AllowedInferencePools: []string{DeterministicInferencePool}}
	root := core.PlanTask{Key: "root", Description: "echo bounded", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, DependsOn: []string{}}
	if err := ValidatePlan(budget, []core.PlanTask{root}); err != nil {
		t.Fatalf("bounded plan rejected: %v", err)
	}
	child := root
	child.Key = "child"
	if err := ValidatePlan(budget, []core.PlanTask{child, root}); err == nil {
		t.Fatal("execution and child budgets were checked only after Task admission")
	}
	adaptive := root
	adaptive.ExecutionKind = core.ExecutionAgent
	adaptive.ModelInferencePolicy = core.InferenceAllowed
	if err := ValidatePlan(budget, []core.PlanTask{adaptive}); err == nil {
		t.Fatal("adaptive plan crossed deterministic containment")
	}
}
