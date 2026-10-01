package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentLegacyKnowledgeContext(t *testing.T) {
	for _, version := range []string{"v1", "v2", "v3", "v4", "v5"} {
		for _, relevant := range []bool{false, true} {
			name := version + "/unrelated"
			if relevant {
				name = version + "/omitted"
			}
			t.Run(name, func(t *testing.T) {
				testLegacyKnowledgeContext(t, version, relevant, core.KnowledgeScopeOrganization, "", "direct")
			})
		}
	}
}

func TestIncidentLegacyKnowledgeBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, version, selection string
		scope                    core.KnowledgeScope
		use                      core.KnowledgeContextUse
	}{
		{"agent-scope", "v2", "direct", core.KnowledgeScopeAgent, ""},
		{"team-scope", "v3", "direct", core.KnowledgeScopeTeam, ""},
		{"behavioral-classification", "v4", "direct", core.KnowledgeScopeOrganization, core.KnowledgeBehavioralPolicy},
		{"private-completion-closure", "v4", "private", core.KnowledgeScopeOrganization, ""},
		{"unconsumed-unclassified", "v4", "unconsumed", core.KnowledgeScopeOrganization, ""},
		{"unconsumed-behavioral", "v4", "unconsumed", core.KnowledgeScopeOrganization, core.KnowledgeBehavioralPolicy},
		{"current-behavioral-excluded", "v5", "direct", core.KnowledgeScopeTeam, core.KnowledgeBehavioralPolicy},
		{"current-unclassified-excluded", "v5", "direct", core.KnowledgeScopeAgent, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			testLegacyKnowledgeContext(t, test.version, true, test.scope, test.use, test.selection)
		})
	}
}

func testLegacyKnowledgeContext(t *testing.T, version string, relevant bool, scope core.KnowledgeScope, use core.KnowledgeContextUse, selection string) {
	t.Helper()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	appendLegacyContextParents(t, store)
	blueprint, agent, config := appendLegacyContextAgent(t, store)
	scopeID := core.ID("org-1")
	if scope == core.KnowledgeScopeAgent {
		scopeID = agent.ID
	}
	if scope == core.KnowledgeScopeTeam {
		scopeID = "legacy-team"
		team := core.Team{ID: scopeID, OrganizationID: "org-1", Name: "Legacy team", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-team-roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team}); err != nil {
			t.Fatal(err)
		}
	}
	appendFactualInferenceKnowledge(t, store, "hidden-fact", "Revenue", "Sales increased three percent.")
	// Absent context_use is a supported historical Knowledge revision.
	// Preserve the owning proposal/authorization/judgment transition,
	// changing both the activation and its matching typed judgment.
	rewriteLegacyContext(t, store, nil, scope, scopeID, use)
	task := appendPendingAgentExecutionTask(t, t.Context(), store, "legacy-context", "task-legacy-context", agent, config)
	_, work := latestTestProjection[core.Work](t, t.Context(), store, "work", task.WorkID)
	_, intent := latestTestProjection[core.Intent](t, t.Context(), store, "intent", work.IntentID)
	plan := core.Plan{ID: "plan-legacy-context", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: time.Now().UTC()}
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context", Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	inputContext := core.AgentExecutionInputContext{Blueprint: blueprint, Task: task}
	var manifest core.ExecutionContextManifest
	routes := []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}
	if scope == core.KnowledgeScopeTeam {
		routes = append(routes, events.InboxRoute{Scope: events.RecipientTeam, ID: string(scopeID)})
	}
	start, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{
		Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, routes, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		manifest = testAgentStartManifest(task, selection)
		manifest.AgentBlueprintVersion, manifest.ExecutionProfileVersion, manifest.RuntimeAdapter = config.BlueprintVersion, config.ProfileVersion, config.RuntimeAdapter
		manifest.Provider, manifest.Model, manifest.PromptVersion = "fake", "fake-model/v1", "v1"
		binding, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, inputContext)
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
	// The current writer only creates v5. Reconstruct the historical
	// input using the same explicit compatibility builders exercised by
	// events.TestCompletionReplayPreservesVersionOneExecutionContext.
	manifest.ContextBuilderVersion = version
	var input string
	if version == "v4" || version == "v5" {
		bind := core.BindAgentExecutionInput
		if version == "v5" {
			bind = core.BindCurrentAgentExecutionInput
		}
		binding, err := bind("org-1", manifest.ExecutionID, inputContext)
		if err != nil {
			t.Fatal(err)
		}
		body, err := binding.Request().Canonical()
		if err != nil {
			t.Fatal(err)
		}
		input = string(body)
	} else {
		_, input, err = core.MaterializeAgentExecutionInput(inputContext)
		if err != nil {
			t.Fatal(err)
		}
	}
	manifest.ExecutionInputSHA256 = core.FingerprintExecutionInput(input)
	rewriteLegacyContext(t, store, &manifest, scope, scopeID, use)
	if selection != "unconsumed" {
		appendLegacyContextCompletion(t, store, task, manifest, input)
	}
	correlation := "legacy-context"
	if selection == "private" {
		// The selected candidate names only the private start. Task record
		// closure finds completion, and the following execution selection
		// supplies the manifest and outcome before context discovery.
		correlation = "knowledge-private-context"
		candidate := core.KnowledgeRecord{KnowledgeID: "private-context", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Execution observation", Content: "Retained execution evidence.", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{start.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "knowledge", RecordID: string(candidate.KnowledgeID), Version: 1, Value: candidate}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateTaskCompletionAdmissions(t.Context(), store.db); err != nil {
		t.Fatalf("owner rejected supported historical fixture: %v", err)
	}
	if err := legacyContextReplay(t, store); err != nil {
		t.Fatalf("full projection replay rejected historical fixture: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256); err != nil {
		t.Fatalf("healthy historical incident: %v", err)
	}
	if relevant {
		rewriteIncidentFact(t, store, func(record *core.KnowledgeRecord) { record.Title = "Bounded Agent work" })
	}
	ownerErr := ValidateTaskCompletionAdmissions(t.Context(), store.db)
	replayErr := legacyContextReplay(t, store)
	snapshot, incidentErr := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	wantReject := relevant && selection != "unconsumed" && (version == "v2" || version == "v3" || version == "v4")
	if !wantReject {
		if ownerErr != nil || replayErr != nil || incidentErr != nil {
			t.Fatalf("unconsumed Knowledge changed completion: owner=%v replay=%v incident=%v", ownerErr, replayErr, incidentErr)
		}
		if selection == "unconsumed" {
			for _, event := range snapshot.DependencyEvents {
				projection, present, err := events.AdmittedProjection(event)
				if err != nil {
					t.Fatal(err)
				}
				if present && projection.Projection.ProjectionKind == "knowledge" && projection.Projection.RecordID == "hidden-fact" {
					t.Fatal("unconsumed historical manifest expanded unclassified or behavioral Knowledge")
				}
			}
		}
		return
	}
	if ownerErr == nil || !strings.Contains(ownerErr.Error(), "knowledge references do not match") {
		t.Fatalf("full completion owner did not detect the omitted historical input: %v", ownerErr)
	}
	if replayErr == nil || !strings.Contains(replayErr.Error(), "knowledge references do not match") {
		t.Fatalf("full replay did not detect the omitted historical input: %v", replayErr)
	}
	t.Logf("full completion owner rejection: %v", ownerErr)
	if incidentErr == nil {
		t.Fatal("incident omitted active unclassified Knowledge consumed by its historical completion builder")
	}
	if !strings.Contains(incidentErr.Error(), "knowledge references do not match") {
		t.Fatalf("incident rejected a different boundary from the full completion owner: %v", incidentErr)
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed historical context validation returned partial evidence")
	}
}

func legacyContextReplay(t *testing.T, store *SQLite) error {
	t.Helper()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	graph, err := events.ValidateProjectionHistory(stream, nil, leases, freezes)
	if err != nil {
		return err
	}
	return events.ValidateProjectionCompletions(graph, stream, nil)
}

func appendLegacyContextParents(t *testing.T, store *SQLite) {
	t.Helper()
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-legacy-context", OrganizationID: organization.ID, OriginalInstruction: "bounded work", NormalizedObjective: "bounded work", AcceptedFingerprint: "internal-legacy-context", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-legacy"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-context"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-context"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
}

func appendLegacyContextAgent(t *testing.T, store *SQLite) (core.AgentBlueprint, core.Agent, core.AgentConfig) {
	t.Helper()
	now := time.Now().UTC()
	blueprint := core.AgentBlueprint{ID: "legacy-blueprint", OrganizationID: "org-1", Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "legacy-profile", OrganizationID: "org-1", Version: "v1", ModelProvider: "fake", Model: "fake-model/v1", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "legacy-agent", OrganizationID: "org-1", BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "legacy-roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	return blueprint, agent, core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: agent.RuntimeAdapter}
}

func appendLegacyContextCompletion(t *testing.T, store *SQLite, task core.Task, manifest core.ExecutionContextManifest, input string) {
	t.Helper()
	publish := func(kind, actor string, payload any) events.Event {
		t.Helper()
		event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: kind, SourceActorID: actor, SourceExecutionID: string(manifest.ExecutionID), TaskID: string(task.ID), CorrelationID: "legacy-context", Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	now := time.Now().UTC()
	outcome := core.ToolOutcome{ToolInvocationID: "model-" + task.ID, ToolID: "fake-model/v1", Status: core.OutcomeSucceeded, ObservedEffect: "fake-model: " + input, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
	outcomeEvent := publish("TOOL_OUTCOME_RECORDED", "runtime", outcome)
	summary, err := core.ToolOutcomeSummary(outcome)
	if err != nil {
		t.Fatal(err)
	}
	result := publish("RESULT_PUBLISHED", string(task.AssigneeID), events.ResultPublishedPayload{Summary: summary})
	publish("CANDIDATE_COMPLETE", string(task.AssigneeID), events.CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: result.EventID})
	contract := core.VerifiedOutcomeCompletionContract(task.ID, 2)
	decision := events.CompletionDecisionPayload{Contract: contract, Result: core.EvaluateCompletion(contract, outcome, nil), OutcomeEventRef: outcomeEvent.EventID}
	publish("COMPLETION_VERIFIED", "runtime", decision)
	task.Status = core.TaskCompleted
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_VERIFIED_COMPLETE", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "legacy-context", Payload: decision}, ProjectionKind: "task", RecordID: string(task.ID), Version: 3, Value: task}); err != nil {
		t.Fatal(err)
	}
}

func rewriteLegacyContext(t *testing.T, store *SQLite, manifest *core.ExecutionContextManifest, scope core.KnowledgeScope, scopeID core.ID, use core.KnowledgeContextUse) {
	t.Helper()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		for _, event := range stream {
			var body []byte
			projection, present, err := events.AdmittedProjection(event)
			if err != nil {
				return err
			}
			switch {
			case present && projection.Projection.ProjectionKind == "knowledge":
				var record core.KnowledgeRecord
				if err := json.Unmarshal(projection.Projection.Value, &record); err != nil {
					return err
				}
				record.Scope, record.ScopeID = scope, scopeID
				if record.Status != core.KnowledgeCandidate {
					record.ContextUse = use
				}
				projection.Projection.Value, err = json.Marshal(record)
				if err != nil {
					return err
				}
				sealed, err := events.SealProjectionEvent(event, projection.Projection, projection.Detail)
				if err != nil {
					return err
				}
				body, err = json.Marshal(sealed)
				if err != nil {
					return err
				}
				recordBody, err := json.Marshal(projection.Projection)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, event.EventID); err != nil {
					return err
				}
			case event.EventType == "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED":
				var judgment events.KnowledgeJudgmentPayload
				if err := json.Unmarshal(event.Payload, &judgment); err != nil {
					return err
				}
				judgment.ContextUse = use
				body, err = json.Marshal(judgment)
			case manifest != nil && event.EventType == "EXECUTION_CONTEXT_MANIFESTED":
				body, err = json.Marshal(manifest)
			default:
				continue
			}
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}
