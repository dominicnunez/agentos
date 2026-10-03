package recovery

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

// Measure the complete offline verification boundary, including SQL reads,
// integrity checks and nested admission validators. Fixture writes are excluded.
func TestRecoveryStartHistoryGrowth(t *testing.T) {
	var previous float64
	for _, count := range []int{16, 64} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			path := recoveryStartHistory(t, count)
			var result Result
			started := time.Now()
			allocations := testing.AllocsPerRun(1, func() {
				var err error
				result, err = Verify(t.Context(), path)
				if err != nil {
					t.Fatalf("valid start history: %v", err)
				}
			})
			if result.EventCount < int64(5*count) || result.EventChainSHA256 == "" {
				t.Fatalf("verification did not cover the durable history: %+v", result)
			}
			t.Logf("starts=%d events=%d allocations=%.0f average=%s", count, result.EventCount, allocations, time.Since(started)/2)
			// Four times the work should remain comfortably below quadratic
			// allocation growth. Time is reported, not used as a machine-specific gate.
			if previous != 0 && allocations > previous*8 {
				t.Errorf("4x start history used %.2fx allocations; repeated admission scans remain", allocations/previous)
			}
			previous = allocations
		})
	}
}

func BenchmarkRecoveryStartHistory(b *testing.B) {
	for _, count := range []int{64, 256, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			path := recoveryStartHistory(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := Verify(b.Context(), path); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func recoveryStartHistory(t testing.TB, count int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "starts.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Recovery growth", PolicyVersion: "v1", CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "test", Model: "test", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	appendProjection := func(kind, id, label, correlation, taskID string, value any) {
		t.Helper()
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: label, SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlation}, ProjectionKind: kind, RecordID: id, Version: 1, Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	appendProjection("organization", string(organization.ID), "ORGANIZATION_CREATED", "setup", "", organization)
	appendProjection("agent_blueprint", string(blueprint.ID), "AGENT_BLUEPRINT_CREATED", "roster", "", blueprint)
	appendProjection("execution_profile", string(profile.ID), "EXECUTION_PROFILE_CREATED", "roster", "", profile)
	appendProjection("agent", string(agent.ID), "AGENT_CREATED", "roster", "", agent)
	for index := 0; index < count; index++ {
		correlation := fmt.Sprintf("work-%d", index)
		intent := core.Intent{ID: core.ID(fmt.Sprintf("intent-%d", index)), OrganizationID: organization.ID, OriginalInstruction: "bounded work", NormalizedObjective: "bounded work", AcceptedFingerprint: "internal-recovery", CreatedAt: now}
		work := core.Work{ID: core.ID(correlation), IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
		task := core.Task{ID: core.ID(fmt.Sprintf("task-%d", index)), WorkID: work.ID, Description: "bounded work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, RuntimeHandlerRef: "builtin.echo", TaskContractVersion: "1", Status: core.TaskPending}
		if index%2 == 0 {
			task.ExecutionKind = core.ExecutionAgent
			task.ModelInferencePolicy = core.InferenceAllowed
			task.RuntimeHandlerRef = ""
			task.AssigneeType, task.AssigneeID = "AGENT", agent.ID
			task.AgentConfig = &core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: agent.RuntimeAdapter}
		}
		appendProjection("intent", string(intent.ID), "INTENT_CREATED", correlation, "", intent)
		appendProjection("work", string(work.ID), "WORK_CREATED", correlation, "", work)
		appendProjection("task", string(task.ID), "TASK_CREATED", correlation, string(task.ID), task)
		appendRecoveryPlan(t, store, correlation, intent, task)
		task.Status = core.TaskRunning
		draft := events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation, Payload: events.ExecutionStartDetail{}}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}
		var routes []events.InboxRoute
		var manifest func(events.ExecutionStartSelection) (core.ExecutionContextManifest, error)
		if task.ExecutionKind == core.ExecutionAgent {
			routes = []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}
			manifest = func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
				return recoveryTestManifest(task, selection), nil
			}
		}
		if _, _, err := store.AppendExecutionStart(t.Context(), draft, routes, manifest); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: fmt.Sprintf("unrelated-%d", index), Payload: map[string]string{"note": "unrelated history"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
