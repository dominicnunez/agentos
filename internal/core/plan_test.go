package core

import (
	"testing"
	"time"
)

func TestFingerprintPlanBindsValidatedStructure(t *testing.T) {
	plan := Plan{
		ID: "plan-1", IntentID: "intent-1", IntentFingerprint: "accepted", Version: 1,
		Tasks:     []PlanTask{{Key: "root", Description: "work", ExecutionKind: ExecutionAgent, ModelInferencePolicy: InferenceAllowed, DependsOn: []string{}}},
		CreatedAt: time.Date(2026, time.August, 12, 12, 0, 0, 123, time.FixedZone("test", -5*60*60)),
	}
	first, err := FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.Fingerprint = "ignored"
	second, err := FingerprintPlan(plan)
	if err != nil || first != second {
		t.Fatalf("first=%q second=%q err=%v", first, second, err)
	}
	plan.Tasks[0].Description = "different work"
	changed, err := FingerprintPlan(plan)
	if err != nil || changed == first {
		t.Fatalf("changed=%q original=%q err=%v", changed, first, err)
	}
	plan.Tasks[0].Description = "work"
	plan.StrategicEventRefs = []string{"mission-event", "goal-event"}
	plan.StrategicContextRefs = []VersionedRef{
		{ID: "mission/mission-1", Version: "1", MaterializationState: MaterializedFull},
		{ID: "goal/goal-1", Version: "2", MaterializationState: MaterializedFull},
	}
	strategic, err := FingerprintPlan(plan)
	if err != nil || strategic == first {
		t.Fatalf("strategic=%q original=%q err=%v", strategic, first, err)
	}
}
