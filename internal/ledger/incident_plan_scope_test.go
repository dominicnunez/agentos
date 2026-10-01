package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentPlanScope(t *testing.T) {
	parallelIncidentTest(t)
	for _, private := range []bool{false, true} {
		for _, variant := range []string{"foreign-task", "foreign-empty-task", "foreign-other-task", "same-tenant", "unrelated", "opaque-note"} {
			name := variant
			if private {
				name += "/private"
			}
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "plans.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				task := incidentTestExecution(t, store)
				selected := "stop-work"
				if private {
					stream, err := store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					var start string
					for _, event := range stream {
						if event.EventType == "EXECUTION_STARTED" {
							start = event.EventID
						}
					}
					if start == "" {
						t.Fatal("fixture lacks execution start")
					}
					selected = "knowledge-private-plan"
					_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
						Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: selected},
						ProjectionKind: "knowledge", RecordID: "private-plan", Version: 1,
						Value: core.KnowledgeRecord{KnowledgeID: "private-plan", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Execution observation", Content: "Recorded execution", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{start}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256); err != nil {
					t.Fatalf("valid baseline: %v", err)
				}
				organization, correlation, taskID, kind := "foreign", "stop-work", string(task.ID), "PLAN_CREATED"
				switch variant {
				case "foreign-empty-task":
					taskID = ""
				case "foreign-other-task":
					taskID = "other-task"
				case "same-tenant":
					organization = "org-1"
				case "unrelated":
					correlation, taskID = "unrelated", "other-task"
				case "opaque-note":
					kind = "AUDIT_NOTE"
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					_, err := tx.ExecContext(t.Context(), `INSERT INTO events(event_id,sequence,organization_id,event_type,source_actor_id,source_execution_id,recipient_scope,recipient_id,task_id,authorization_refs,artifact_refs,payload,correlation_id,created_at,schema_version)
SELECT 'extra-plan',(SELECT MAX(sequence)+1 FROM events),?,?,source_actor_id,source_execution_id,recipient_scope,recipient_id,?,authorization_refs,artifact_refs,payload,?,created_at,schema_version FROM events WHERE event_type='PLAN_CREATED' AND correlation_id='stop-work'`, organization, kind, taskID, correlation)
					if err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				}); err != nil {
					t.Fatal(err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				valid := variant == "unrelated" || variant == "opaque-note"
				if !valid && (fullErr == nil || !strings.Contains(fullErr.Error(), "Plan")) {
					t.Fatalf("full history must reject conflicting Plan: %v", fullErr)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
				if valid {
					if fullErr != nil {
						t.Fatalf("unrelated history changed full Plan validation: %v", fullErr)
					}
					if err != nil {
						t.Fatalf("unrelated history poisoned incident: %v", err)
					}
					for _, event := range snapshot.DependencyEvents {
						if event.EventID == "extra-plan" {
							t.Fatal("unrelated Plan or opaque note selected")
						}
					}
					return
				}
				if err == nil {
					t.Fatal("incident omitted conflicting Plan")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("failed incident returned partial evidence")
				}
			})
		}
	}
}

func TestIncidentUnstartedPlanScope(t *testing.T) {
	for _, private := range []bool{false, true} {
		for _, variant := range []string{"foreign-plan-only", "local-and-foreign-plans", "foreign-start", "mixed-correlations"} {
			if variant == "mixed-correlations" && !private {
				continue
			}
			name := variant + "/public"
			if private {
				name = variant + "/private"
			}
			t.Run(name, func(t *testing.T) { checkUnstartedPlanScope(t, private, variant) })
		}
	}
}

func checkUnstartedPlanScope(t *testing.T, private bool, variant string) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if variant == "mixed-correlations" {
		incidentTestExecution(t, store)
	}
	localIntent, _ := planScopeParents(t, store, "org-1", "shared", "unstarted-work")
	foreignIntent, foreignWork := planScopeParents(t, store, "foreign", "shared", "foreign-work")
	appendPlan := func(organization string, intent core.Intent) events.Event {
		t.Helper()
		plan := core.Plan{ID: "plan-shared", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: "pending work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden}}, CreatedAt: time.Now().UTC()}
		plan.Fingerprint, err = core.FingerprintPlan(plan)
		if err != nil {
			t.Fatal(err)
		}
		event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-shared", CorrelationID: "shared", Payload: plan})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	foreign := appendPlan("foreign", foreignIntent)
	var localPlan events.Event
	if variant == "local-and-foreign-plans" || variant == "mixed-correlations" {
		localPlan = appendPlan("org-1", localIntent)
	}
	if variant == "foreign-start" {
		task := core.Task{ID: "task-shared", WorkID: foreignWork.ID, Description: "pending work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
		for version, kind := range []string{"TASK_CREATED", "EXECUTION_STARTED"} {
			if version == 1 {
				task.Status = core.TaskRunning
			}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "foreign", EventType: kind, SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "shared", Payload: events.ExecutionStartDetail{InboxCutoffSequence: 0}}, ProjectionKind: "task", RecordID: string(task.ID), Version: version + 1, Value: task}); err != nil {
				t.Fatal(err)
			}
		}
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	selected := "shared"
	var required []string
	if localPlan.EventID != "" {
		required = append(required, localPlan.EventID)
	}
	if private {
		var evidence []string
		for _, event := range stream {
			if event.EventType == "WORK_CREATED" && event.OrganizationID == "org-1" && event.CorrelationID == "shared" || variant == "mixed-correlations" && event.EventType == "EXECUTION_STARTED" && event.OrganizationID == "org-1" {
				evidence = append(evidence, event.EventID)
			}
		}
		if len(evidence) == 0 || variant == "mixed-correlations" && len(evidence) != 2 {
			t.Fatal("missing exact public/private fixture boundaries")
		}
		required = append(required, evidence...)
		selected = "knowledge-unstarted-plan"
		_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: selected},
			ProjectionKind: "knowledge", RecordID: "unstarted-plan", Version: 1,
			Value: core.KnowledgeRecord{KnowledgeID: "unstarted-plan", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Work observation", Content: "Recorded Work", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: evidence, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
		})
		if err != nil {
			t.Fatal(err)
		}
		stream, err = store.Events(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
	}
	graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
	if err != nil {
		t.Fatalf("valid scoped histories: %v", err)
	}
	if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
		t.Fatalf("valid completion applicability: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
	if err != nil {
		t.Fatalf("unconsumed foreign Plan blocked valid incident: %v", err)
	}
	seen := make(map[string]bool)
	for _, part := range [][]events.Event{snapshot.Work.Events, snapshot.DependencyEvents} {
		for _, event := range part {
			if event.EventID == foreign.EventID {
				t.Fatal("unconsumed foreign Plan selected")
			}
			seen[event.EventID] = true
		}
	}
	for _, ref := range required {
		if !seen[ref] {
			t.Fatalf("supporting boundary %s omitted", ref)
		}
	}
}

func planScopeParents(t *testing.T, store *SQLite, organizationID, correlationID, workID string) (core.Intent, core.Work) {
	t.Helper()
	now := time.Now().UTC()
	records, err := store.Records(t.Context(), "organization", organizationID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 {
		organization := core.Organization{ID: core.ID(organizationID), Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organizationID, EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-" + organizationID}, ProjectionKind: "organization", RecordID: organizationID, Version: 1, Value: organization}); err != nil {
			t.Fatal(err)
		}
	}
	intent := core.Intent{ID: core.ID("intent-" + workID), OrganizationID: core.ID(organizationID), OriginalInstruction: "pending work", NormalizedObjective: "pending work", AcceptedFingerprint: core.FingerprintExecutionInput(organizationID + "/" + correlationID), CreatedAt: now}
	work := core.Work{ID: core.ID(workID), IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: organizationID, EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: organizationID, EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlationID}, ProjectionKind: "work", RecordID: workID, Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	return intent, work
}

// Structured Human completion can advance a blocked Task without an execution
// start. Its aggregate Work completion is therefore an independent Plan consumer.
func TestIncidentCompletedPlanScope(t *testing.T) {
	for _, private := range []bool{false, true} {
		for _, foreignCompletion := range []bool{false, true} {
			name := "local-completion/public"
			if foreignCompletion {
				name = "foreign-completion/public"
			}
			if private {
				name = strings.Replace(name, "/public", "/private", 1)
			}
			t.Run(name, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				organization := "org-1"
				if foreignCompletion {
					organization = "foreign"
					planScopeParents(t, store, "org-1", "human-shared", "pending-work")
				}
				completion, plan := completedHumanPlanScope(t, store, organization)
				selected := "human-shared"
				if private {
					ref := completion.EventID
					if foreignCompletion {
						stream, err := store.Events(t.Context(), "human-shared")
						if err != nil {
							t.Fatal(err)
						}
						ref = ""
						for _, event := range stream {
							if event.EventType == "WORK_CREATED" && event.OrganizationID == "org-1" {
								ref = event.EventID
							}
						}
						if ref == "" {
							t.Fatal("missing pending local Work")
						}
					}
					selected = "knowledge-completed-plan"
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: selected}, ProjectionKind: "knowledge", RecordID: "completed-plan", Version: 1, Value: core.KnowledgeRecord{KnowledgeID: "completed-plan", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Work observation", Content: "Recorded Work", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{ref}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}}); err != nil {
						t.Fatal(err)
					}
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range stream {
					if event.EventType == "EXECUTION_STARTED" {
						t.Fatal("fixture does not isolate Work completion applicability")
					}
				}
				graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
				if err != nil {
					t.Fatalf("valid Human projection history: %v", err)
				}
				if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
					t.Fatalf("valid Human Work completion: %v", err)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
				if err != nil {
					t.Fatalf("valid completion scope: %v", err)
				}
				if foreignCompletion {
					for _, event := range snapshot.DependencyEvents {
						if event.EventID == plan.EventID || event.EventID == completion.EventID {
							t.Fatal("foreign Work completion activated local Plan applicability")
						}
					}
					return
				}
				seenCompletion, seenPlan := false, false
				for _, part := range [][]events.Event{snapshot.Work.Events, snapshot.DependencyEvents} {
					for _, event := range part {
						seenCompletion = seenCompletion || event.EventID == completion.EventID
						seenPlan = seenPlan || event.EventID == plan.EventID
					}
				}
				if !seenCompletion || !seenPlan {
					t.Fatal("incident omitted the independently completed Work or its Plan")
				}
				foreignIntent, _ := planScopeParents(t, store, "foreign", "human-shared", "foreign-work")
				foreignPlan := core.Plan{ID: "plan-human-shared", IntentID: foreignIntent.ID, IntentFingerprint: foreignIntent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: "pending work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden}}, CreatedAt: time.Now().UTC()}
				foreignPlan.Fingerprint, err = core.FingerprintPlan(foreignPlan)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "foreign", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-human-shared", CorrelationID: "human-shared", Payload: foreignPlan}); err != nil {
					t.Fatal(err)
				}
				stream, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				graph, err = events.ValidateProjectionHistory(stream, nil, nil, nil)
				if err != nil {
					t.Fatalf("only aggregate completion should consume the conflicting Plan: %v", err)
				}
				if err := events.ValidateProjectionCompletions(graph, stream, nil); err == nil {
					t.Fatal("full completion validation omitted conflicting Plan")
				}
				snapshot, err = store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
				if err == nil || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatalf("completed Work incident omitted conflict: snapshot=%+v err=%v", snapshot, err)
				}
			})
		}
	}
}

func completedHumanPlanScope(t *testing.T, store *SQLite, organization string) (events.Event, events.Event) {
	t.Helper()
	const correlation = "human-shared"
	now := time.Now().UTC()
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-" + organization}, ProjectionKind: "organization", RecordID: organization, Version: 1, Value: core.Organization{ID: core.ID(organization), Name: "Organization", PolicyVersion: "v1", CreatedAt: now}}); err != nil {
		t.Fatal(err)
	}
	draft := appendReviewedIntent(t, t.Context(), store, organization, correlation, "intent-human-shared", "provide response", core.IntentModeStandard, core.ExecutionHuman, now)
	confirmation := events.IntentConfirmedPayload{IntentID: string(draft.ID), Version: 1, Fingerprint: draft.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "confirmation-human"}
	if _, err := store.AppendIntentConfirmation(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-human-shared", CorrelationID: correlation, Payload: confirmation}, "", ""); err != nil {
		t.Fatal(err)
	}
	intent := core.Intent{ID: draft.ID, OrganizationID: core.ID(organization), OriginalInstruction: "provide response", NormalizedObjective: draft.Objective, CompletionCriteria: draft.CompletionCriteria, AcceptedFingerprint: draft.Fingerprint, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "source-" + correlation, CreatedAt: now}
	work := core.Work{ID: "human-work", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	for _, projection := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: organization, EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: organization, EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
	} {
		if _, err := store.AppendProjection(t.Context(), projection); err != nil {
			t.Fatal(err)
		}
	}
	plan := core.Plan{ID: "plan-human-shared", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "root", Description: "provide response", ExecutionKind: core.ExecutionHuman, ModelInferencePolicy: core.InferenceForbidden}}, CreatedAt: now}
	var err error
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	planEvent, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-human-shared", CorrelationID: correlation, Payload: plan})
	if err != nil {
		t.Fatal(err)
	}
	contract := core.StructuredUserCompletionContract("task-human-shared")
	task := core.Task{ID: "task-human-shared", WorkID: work.ID, Description: "provide response", AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionHuman, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", CompletionContract: &contract, Status: core.TaskPending}
	for version, kind := range []string{"TASK_CREATED", "TASK_BLOCKED"} {
		if version == 1 {
			task.Status = core.TaskBlocked
		}
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: kind, SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation}, ProjectionKind: "task", RecordID: string(task.ID), Version: version + 1, Value: task}); err != nil {
			t.Fatal(err)
		}
	}
	publish := func(kind, actor, execution string, payload any) events.Event {
		t.Helper()
		event, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: kind, SourceActorID: actor, SourceExecutionID: execution, TaskID: string(task.ID), CorrelationID: correlation, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	fields := map[string]string{"response": "provided"}
	submission := publish("HUMAN_TASK_COMPLETION_SUBMITTED", "user-1", "", events.HumanTaskCompletionSubmittedPayload{MessageID: "human-response", Fields: fields, SourcePrincipalID: "user-1", SourceChannel: "HUMAN_DIRECT"})
	execution := "human-completion-" + submission.EventID
	outcome := core.HumanTaskCompletionOutcome(submission.EventID, nil, time.Now().UTC())
	outcomeEvent := publish("TOOL_OUTCOME_RECORDED", "runtime", execution, outcome)
	summary, err := core.ToolOutcomeSummary(outcome)
	if err != nil {
		t.Fatal(err)
	}
	result := publish("RESULT_PUBLISHED", "runtime", execution, events.ResultPublishedPayload{Summary: summary})
	publish("CANDIDATE_COMPLETE", "runtime", execution, events.CandidateCompletePayload{ToolInvocationID: string(outcome.ToolInvocationID), ResultEventID: result.EventID})
	decision := events.CompletionDecisionPayload{Contract: contract, Result: core.EvaluateHumanTaskCompletion(contract, core.HumanTaskSubmission{MessageID: "human-response", Fields: fields}), OutcomeEventRef: outcomeEvent.EventID, SubmissionEventRef: submission.EventID}
	verification := publish("COMPLETION_VERIFIED", "runtime", execution, decision)
	task.Status = core.TaskCompleted
	completed, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "TASK_VERIFIED_COMPLETE", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation, Payload: decision}, ProjectionKind: "task", RecordID: string(task.ID), Version: 3, Value: task})
	if err != nil {
		t.Fatal(err)
	}
	evidence := events.WorkCompletionEvidencePayload{WorkID: work.ID, WorkVersion: 2, IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, PlanID: plan.ID, PlanVersion: 1, Criteria: intent.CompletionCriteria, Tasks: []events.WorkCompletionTaskEvidencePayload{{TaskID: task.ID, TaskVersion: 3, VerificationEventRef: verification.EventID, CompletionEventRef: completed.EventID}}, CreatedAt: time.Now().UTC()}
	evidence.Fingerprint, err = evidence.ExpectedFingerprint()
	if err != nil {
		t.Fatal(err)
	}
	evidenceEvent, err := store.AppendWorkCompletionEvidence(t.Context(), events.TrustedDraft{OrganizationID: organization, EventType: "WORK_COMPLETION_EVALUATED", SourceActorID: "runtime", CorrelationID: correlation, Payload: evidence})
	if err != nil {
		t.Fatal(err)
	}
	work.Status = core.WorkCompleted
	completion, err := store.AppendWorkCompletion(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "WORK_COMPLETED", SourceActorID: "runtime", CorrelationID: correlation, Payload: events.WorkCompletionTransitionPayload{EvidenceEventRef: evidenceEvent.EventID, Fingerprint: evidence.Fingerprint}}, ProjectionKind: "work", RecordID: string(work.ID), Version: 2, Value: work})
	if err != nil {
		t.Fatal(err)
	}
	return completion, planEvent
}
