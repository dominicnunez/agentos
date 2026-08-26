package app

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestBuildAIMSEvidenceIsBoundedVerifiableAndExcludesPrivateContent(t *testing.T) {
	generatedAt := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.FixedZone("test", -5*60*60))
	view := OrganizationSnapshot{
		Organization: OrganizationSummary{ID: "org-1", Name: "Example", PolicyVersion: "policy-v1", Version: 3},
		Missions:     []MissionSummary{{ID: "mission-1", Statement: "private mission text", Status: core.MissionActive}},
		Goals:        []GoalSummary{{ID: "goal-1", Objective: "private goal text", Mode: core.GoalTarget, Status: core.GoalActive}},
		Works: []WorkSummary{
			{ID: "work-1", Objective: "private work text", Mode: core.IntentModeStandard, Status: core.WorkCompleted},
			{ID: "work-2", Objective: "private experiment text", Mode: core.IntentModeExperiment, Status: core.WorkActive},
		},
		Tasks: []TaskSummary{
			{ID: "task-1", Description: "private task text", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, Status: core.TaskCompleted},
			{ID: "task-2", Description: "private agent prompt", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, Status: core.TaskPending},
		},
		Teams: []TeamSummary{{ID: "team-1", Name: "Private team", Status: "ACTIVE"}},
		Agents: []AgentSummary{{
			ID: "agent-1", Role: "researcher", Status: "ACTIVE", BlueprintStatus: "ACTIVE",
			ExecutionProfileStatus: "ACTIVE", Available: true, RuntimeAdapter: "fake",
			ModelProvider: "test-provider", Model: "test-model", Version: 2,
		}},
	}
	export, err := buildAIMSEvidence(view, generatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if export.SchemaVersion != aimsEvidenceSchemaVersion || export.Claim.Certified || export.Claim.Status != "READINESS_WORK_IN_PROGRESS" {
		t.Fatalf("claim=%+v schema=%q", export.Claim, export.SchemaVersion)
	}
	if export.GeneratedAt.Location() != time.UTC || export.Inventory.Operations.Experiments != 1 || export.Inventory.Operations.Tasks != 2 || len(export.Inventory.AISystems) != 1 {
		t.Fatalf("export=%+v", export)
	}
	encoded, err := json.Marshal(export)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"private mission text", "private goal text", "private work text", "private experiment text", "private task text", "private agent prompt", "Private team", "credential_ref", "event_payload", "approval_id", "effect_fingerprint", "capability"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("AIMS evidence leaked %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(string(encoded), `"control":"organization_identity_and_policy"`) || !strings.Contains(string(encoded), `"control":"task_lifecycle_projection"`) ||
		!strings.Contains(string(encoded), `"control":"organizational_team_roster"`) ||
		!strings.Contains(string(encoded), `"state":"PROJECTION_AVAILABLE"`) || !strings.Contains(string(encoded), `"area":"impact_and_risk"`) {
		t.Fatalf("AIMS evidence omitted controls or explicit gaps: %s", encoded)
	}
	if strings.Contains(string(encoded), "TASK_ASSIGNED") || strings.Contains(string(encoded), "LAB_PROMOTION_CANDIDATE_CREATED") ||
		!strings.Contains(string(encoded), "TASK_ASSIGNMENT_REVALIDATED") || !strings.Contains(string(encoded), "WORK_COMPLETED") ||
		!strings.Contains(string(encoded), "WORK_FAILED") || !strings.Contains(string(encoded), "LAB_EXPERIMENT_STARTED") ||
		!strings.Contains(string(encoded), "TEAM_CREATED") || !strings.Contains(string(encoded), "TEAM_REVISED") ||
		!strings.Contains(string(encoded), "ORGANIZATION_CREATED") {
		t.Fatalf("AIMS evidence did not use the closed projection lifecycle contracts: %s", encoded)
	}
}

func TestBuildAIMSEvidenceRejectsMissingGenerationTime(t *testing.T) {
	if _, err := buildAIMSEvidence(OrganizationSnapshot{}, time.Time{}); err == nil {
		t.Fatal("missing generation time was accepted")
	}
}
