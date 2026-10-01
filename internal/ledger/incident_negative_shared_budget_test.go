package ledger

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentNegativeSharedActivationBytes(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	store.now = func() time.Time { return now }
	correlation, connection := "shared-temporal-budget", "shared-temporal-connection"
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-shared-temporal-budget", OrganizationID: "org-1", OriginalInstruction: "bounded work", NormalizedObjective: "bounded work", AcceptedFingerprint: "internal-shared-temporal-budget", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "shared-setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	blueprint := core.AgentBlueprint{ID: "shared-blueprint", OrganizationID: "org-1", Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "shared-profile", OrganizationID: "org-1", Version: "v1", ConnectionID: connection, ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "shared-agent", OrganizationID: "org-1", BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "shared-roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "shared-roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "shared-roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	policy := testInferencePolicy(now)
	policy.Version, policy.ConnectionID = inference.ConnectionPolicyVersion, connection
	policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", "v1"
	policy.Mode, policy.Pricing = inference.Local, nil
	policy.OrganizationBudget = &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxConcurrentRequests: 2}
	for range 3 {
		policy.AuthorizedAt = policy.AuthorizedAt.Add(time.Second)
		if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
			t.Fatal(err)
		}
	}
	config := core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: "v1", ProfileID: profile.ID, ProfileVersion: "v1", RuntimeAdapter: "local"}
	task := appendPendingAgentExecutionTask(t, t.Context(), store, correlation, "task-shared-temporal-budget", agent, config)
	plan := core.Plan{ID: "plan-shared-temporal-budget", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: now}
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation, Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	var manifest core.ExecutionContextManifest
	start, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		manifest = testAgentStartManifest(task, selection)
		manifest.ConnectionID, manifest.Provider, manifest.Model, manifest.PromptVersion = connection, "provider", "model", "v1"
		binding, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, core.AgentExecutionInputContext{Blueprint: blueprint, Task: task})
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		body, err := binding.Request().Canonical()
		manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(string(body))
		return manifest, err
	})
	if err != nil {
		t.Fatal(err)
	}
	request := inference.InferenceRequest{ConnectionID: connection, Scope: inference.Scope{OrganizationID: "org-1", Purpose: inference.PurposeTaskExecution, RequestID: string(manifest.ExecutionID), ExecutionID: string(manifest.ExecutionID), TaskID: string(task.ID), CorrelationID: correlation}, Descriptor: execution.ModelDescriptor{Provider: "provider", Model: "model", ExecutionProfileVersion: "v1"}, PromptSHA256: manifest.ExecutionInputSHA256}
	reservation, err := store.ReserveInference(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
		t.Fatal(err)
	}
	// Three policy bodies total18MiB; their exact activation events total12MiB.
	// Valid JSON tabs make every activation a conservative temporal candidate.
	// Each lies strictly after the pinned roster and before the consumed start.
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE inference_policies SET body=CAST(body || ? AS BLOB) WHERE connection_id=?`, strings.Repeat(" ", 6<<20), connection); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST(payload || ? AS BLOB) WHERE event_id IN(SELECT activation_event_id FROM inference_policies WHERE connection_id=?)`, strings.Repeat("\t", 4<<20), connection); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	full, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(full, nil, nil, nil); err != nil {
		t.Fatalf("exact projection owner rejected valid padded activations: %v", err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("full inference owner rejected valid shared support: %v", err)
	}
	windows, _, _, err := incidentNegativeWindows(full)
	if err != nil || len(windows) == 0 {
		t.Fatalf("consumed Agent temporal windows missing: %v", err)
	}
	var candidates, activationBytes, policyBytes int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),SUM(length(CAST(payload AS BLOB))) FROM events WHERE event_id IN(SELECT activation_event_id FROM inference_policies WHERE connection_id=?) AND sequence<? AND (`+incidentNegativeJSON+`)`, connection, start.Sequence).Scan(&candidates, &activationBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(t.Context(), `SELECT SUM(length(CAST(body AS BLOB))) FROM inference_policies WHERE connection_id=?`, connection).Scan(&policyBytes); err != nil {
		t.Fatal(err)
	}
	if candidates != 3 || activationBytes < 12<<20 || policyBytes < 18<<20 || activationBytes+policyBytes >= events.MaximumIncidentEvidenceBytes || 2*activationBytes+policyBytes <= events.MaximumIncidentEvidenceBytes {
		t.Fatalf("fixture does not distinguish charging shared events twice: candidates=%d activationBytes=%d policyBytes=%d", candidates, activationBytes, policyBytes)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err != nil || len(snapshot.Admissions) != 2 {
		t.Fatalf("valid30MiB private support charged twice across final temporal guard: admissions=%d err=%v", len(snapshot.Admissions), err)
	}
	// Keep this proof about byte ownership, not accidentally admitting padded
	// support into the public event graph.
	for _, group := range [][]events.Event{snapshot.Work.Events, snapshot.RelatedEvents, snapshot.DependencyEvents} {
		for _, event := range group {
			if len(event.Payload) >= 4<<20 {
				t.Fatal("private policy activation expanded public snapshot")
			}
		}
	}
}
