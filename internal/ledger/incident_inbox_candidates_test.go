package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentInboxCandidates(t *testing.T) {
	for _, scope := range []string{events.RecipientTask, events.RecipientAgent, events.RecipientTeam} {
		for _, mode := range []string{"omitted", "missing-row", "moved-row", "moved-event", "other-org", "other-recipient", "after-cutoff", "observed", "removed-before-start", "removed-after-start"} {
			if (mode == "removed-before-start" || mode == "removed-after-start") && scope != events.RecipientTeam {
				continue
			}
			t.Run(scope+"/"+mode, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "inbox.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
				var team core.Team
				if scope == events.RecipientTeam {
					team = core.Team{ID: "inbox-team", OrganizationID: "org-1", Name: "Inbox team", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team}); err != nil {
						t.Fatal(err)
					}
				}
				removeMember := func() {
					team.MemberAgentIDs = []core.ID{}
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_REVISED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 2, Value: team}); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "removed-before-start" {
					removeMember()
				}
				recipient := "task-selected"
				if scope == events.RecipientAgent {
					recipient = string(agent.ID)
				}
				if scope == events.RecipientTeam {
					recipient = "inbox-team"
				}
				noteDraft := events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "other-work", SourceActorID: "runtime", EventType: "AUDIT_NOTE", Payload: map[string]string{"text": "inbox input"}}
				if mode == "observed" {
					noteDraft.RecipientScope, noteDraft.RecipientID = scope, recipient
				}
				note, err := store.Append(t.Context(), noteDraft)
				if err != nil {
					t.Fatal(err)
				}
				var extra []events.InboxRoute
				if scope == events.RecipientTeam && mode != "removed-before-start" {
					extra = []events.InboxRoute{{Scope: events.RecipientTeam, ID: "inbox-team"}}
				}
				request := appendInboxTaskInference(t, store, agent, config, "selected", extra)
				policy := testInferencePolicy(time.Now().UTC())
				policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
				policy.Mode, policy.Pricing = inference.Local, nil
				if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
					t.Fatal(err)
				}
				reservation, err := store.ReserveInference(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
					t.Fatal(err)
				}
				if mode == "removed-after-start" {
					removeMember()
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
					t.Fatalf("valid fixture: %v", err)
				}
				if mode == "after-cutoff" {
					note, err = store.Append(t.Context(), noteDraft)
					if err != nil {
						t.Fatal(err)
					}
				}
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					if mode == "observed" {
						return nil
					}
					organization := "org-1"
					if mode == "other-org" {
						organization = "other-org"
					}
					if mode == "other-recipient" {
						recipient += "-unrelated"
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET organization_id=? WHERE event_id=?`, organization, note.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET recipient_scope=?,recipient_id=? WHERE event_id=?`, scope, recipient, note.EventID); err != nil {
						return err
					}
					if mode != "missing-row" {
						if _, err := tx.ExecContext(t.Context(), `INSERT INTO inbox(recipient_scope,recipient_id,event_id,organization_id,available_at) VALUES(?,?,?,?,?)`, scope, recipient, note.EventID, organization, note.CreatedAt.Format(time.RFC3339Nano)); err != nil {
							return err
						}
					}
					if mode == "moved-row" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE inbox SET recipient_id=? WHERE event_id=?`, recipient+"-moved", note.EventID); err != nil {
							return err
						}
					}
					if mode == "moved-event" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET recipient_id=? WHERE event_id=?`, recipient+"-moved", note.EventID); err != nil {
							return err
						}
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				})
				if err != nil {
					t.Fatal(err)
				}
				if err = store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				if mode == "omitted" || mode == "removed-after-start" {
					if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
						t.Fatal("full recovery accepted omitted eligible inbox input")
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if mode == "other-org" || mode == "other-recipient" || mode == "after-cutoff" || mode == "observed" || mode == "removed-before-start" {
					if err != nil {
						t.Fatalf("unrelated or valid candidate: %v", err)
					}
					return
				}
				if err == nil {
					t.Fatal("accepted execution manifest omitting eligible addressed input from other correlation")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid inbox history returned partial evidence")
				}
			})
		}
	}
}

func appendInboxTaskInference(t testing.TB, store *SQLite, agent core.Agent, config core.AgentConfig, correlationID string, extra []events.InboxRoute) inference.InferenceRequest {
	t.Helper()
	now := time.Now().UTC()
	intent := core.Intent{ID: core.ID("intent-" + correlationID), OrganizationID: "org-1", OriginalInstruction: "bounded work", NormalizedObjective: "bounded work", AcceptedFingerprint: "internal-" + correlationID, CreatedAt: now}
	work := core.Work{ID: core.ID("work-" + correlationID), IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: core.ID("task-" + correlationID), WorkID: work.ID, Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	plan := core.Plan{ID: core.ID("plan-" + correlationID), IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "agent-work", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: now}
	var err error
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID, Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	var digest string
	if _, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{
		Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlationID}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, append([]events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, extra...), func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		manifest := testAgentStartManifest(task, selection)
		manifest.Provider, manifest.Model, manifest.PromptVersion = "provider", "model", "v1"
		knowledge := make([]core.KnowledgeRecord, 0, len(selection.Knowledge))
		for _, selected := range selection.Knowledge {
			knowledge = append(knowledge, selected.Record)
		}
		var inbox []core.AgentExecutionInboxEvent
		for _, route := range selection.Inbox {
			for _, event := range route.Events {
				inbox = append(inbox, core.AgentExecutionInboxEvent{Sequence: event.Sequence, EventID: event.EventID, EventType: event.EventType, SourceActorID: event.SourceActorID, RecipientScope: event.RecipientScope, RecipientID: event.RecipientID, TaskID: event.TaskID, CreatedAt: event.CreatedAt, Payload: event.Payload})
			}
		}
		sort.Slice(inbox, func(i, j int) bool { return inbox[i].Sequence < inbox[j].Sequence })
		manifest.EventRefs = nil
		for _, event := range inbox {
			manifest.EventRefs = append(manifest.EventRefs, event.EventID)
		}
		input, err := core.BindCurrentAgentExecutionInput("org-1", manifest.ExecutionID, core.AgentExecutionInputContext{Task: task, Knowledge: knowledge, InboxEvents: inbox, Blueprint: core.AgentBlueprint{ID: config.BlueprintID, OrganizationID: "org-1", Version: config.BlueprintVersion, Role: "worker", OperatingInstructions: "bounded work"}})
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		body, err := input.Request().Canonical()
		if err != nil {
			return core.ExecutionContextManifest{}, err
		}
		digest = core.FingerprintExecutionInput(string(body))
		manifest.ExecutionInputSHA256 = digest
		return manifest, nil
	}); err != nil {
		t.Fatal(err)
	}
	executionID := "execution-" + string(task.ID) + "-v2"
	return inference.InferenceRequest{Scope: inference.Scope{OrganizationID: "org-1", Purpose: inference.PurposeTaskExecution, RequestID: executionID, ExecutionID: executionID, TaskID: string(task.ID), CorrelationID: correlationID}, Descriptor: execution.ModelDescriptor{Provider: "provider", Model: "model", ExecutionProfileVersion: config.ProfileVersion}, PromptSHA256: digest}
}

func TestIncidentInboxGrowth(t *testing.T) {
	for _, n := range []int{1, 8} {
		for _, unrelated := range []int{0, 500} {
			t.Run(fmt.Sprintf("starts-%d/unrelated-%d", n, unrelated), func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "shared", true)
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					for i := 0; i < unrelated; i++ {
						if _, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: "org-1", SourceActorID: "runtime", CorrelationID: "unrelated", EventType: "AUDIT_NOTE", RecipientScope: events.RecipientAgent, RecipientID: "other-agent", Payload: map[string]string{"text": "unrelated"}}); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < n; i++ {
					input, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", SourceActorID: "runtime", CorrelationID: "input", EventType: "AUDIT_NOTE", RecipientScope: events.RecipientAgent, RecipientID: string(agent.ID), Payload: map[string]string{"text": "shared inbox input"}})
					if err != nil {
						t.Fatal(err)
					}
					correlation := fmt.Sprintf("run-%d", i)
					request := appendInboxTaskInference(t, store, agent, config, correlation, nil)
					refs := []string{input.EventID}
					if _, err := store.ObserveInbox(t.Context(), events.TrustedDraft{OrganizationID: "org-1", SourceActorID: string(agent.ID), SourceExecutionID: request.Scope.ExecutionID, TaskID: request.Scope.TaskID, CorrelationID: correlation, EventType: "INBOX_EVENTS_OBSERVED", RecipientScope: events.RecipientAgent, RecipientID: string(agent.ID), Payload: map[string]any{"event_ids": refs}}, events.RecipientAgent, string(agent.ID), refs); err != nil {
						t.Fatal(err)
					}
				}
				started := time.Now()
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", fmt.Sprintf("run-%d", n-1), 256)
				if err != nil {
					t.Fatal(err)
				}
				starts := 0
				for _, event := range append(append([]events.Event{}, snapshot.Work.Events...), snapshot.DependencyEvents...) {
					if event.EventType == "EXECUTION_STARTED" {
						starts++
					}
				}
				if starts != n {
					t.Fatalf("selected starts=%d, want %d", starts, n)
				}
				t.Logf("public read %s: %d public, %d dependencies", time.Since(started), len(snapshot.Work.Events), len(snapshot.DependencyEvents))
				for _, event := range snapshot.DependencyEvents {
					if event.CorrelationID == "unrelated" {
						t.Fatal("unrelated addressed event selected")
					}
				}
			})
		}
	}
}
