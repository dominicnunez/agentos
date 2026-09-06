package app

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/ledger/recovery"
	"github.com/dominicnunez/agentos/internal/projections"
)

func TestTaskCompletionRejectsKnowledgeInvalidatedAfterOutcome(t *testing.T) {
	for _, name := range []string{"current", "before_task_completion", "before_work_completion", "before_work_transition", "after_work_transition", "goal_current", "goal_before_progress", "goal_after_progress", "inference_current", "inference_stale", "inference_later_stale"} {
		t.Run(name, func(t *testing.T) {
			invalidate := name == "before_task_completion"
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "knowledge-completion.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			gateway := events.NewGateway(store)
			repository := projections.New(gateway)
			now := time.Now().UTC()
			org := core.Organization{ID: "org-1", Name: "Inventory", PolicyVersion: "v1", CreatedAt: now}
			if err := repository.SaveOrganization(ctx, "ORGANIZATION_CREATED", "runtime", "setup", 1, org, nil); err != nil {
				t.Fatal(err)
			}
			correlationID := "request-1"
			active := seedCompletionFactualKnowledge(t, store, gateway)
			agents := seedTestAgents(t, ctx, repository, correlationID, org.ID, execution.FakeModel{}.Descriptor(), "agent-1")
			intent := acceptedTestIntent("intent-1", org.ID, "report inventory")
			work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive}
			goalCase := strings.HasPrefix(name, "goal_")
			if goalCase {
				mission := core.Mission{ID: "mission-1", OrganizationID: org.ID, Statement: "Maintain inventory records", Status: core.MissionActive, CreatedAt: now}
				if err := repository.SaveMission(ctx, "MISSION_CREATED", "runtime", "mission-1", 1, mission, nil); err != nil {
					t.Fatal(err)
				}
				goal := core.Goal{ID: "goal-1", OrganizationID: org.ID, MissionID: mission.ID, Objective: intent.NormalizedObjective, Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "The requested outcome is produced and independently evaluated.", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}
				if err := repository.SaveGoal(ctx, "GOAL_CREATED", "runtime", "goal-1", 1, goal, nil); err != nil {
					t.Fatal(err)
				}
				in := confirmedGoalSubmit(t, ctx, gateway, "goal-inventory", string(org.ID), goal.ID, intent.NormalizedObjective, core.ExecutionAgent)
				var found bool
				correlationID, found, err = gateway.ResolveExternalWork(ctx, string(org.ID), in.RequestID)
				if err != nil || !found {
					t.Fatalf("resolve confirmed Goal Work: %v", err)
				}
				draft := *in.NormalizedIntent
				intent = core.Intent{ID: draft.ID, OrganizationID: org.ID, GoalID: goal.ID, OriginalInstruction: in.Statement, NormalizedObjective: draft.Objective, SourcePrincipalID: in.SourcePrincipalID, SourcePrincipalKind: in.SourcePrincipalKind, SourceHumanID: in.SourcePrincipalID, SourceChannel: in.SourceChannel, ExternalRequestID: in.RequestID, SourceMessageID: in.MessageID, Context: draft.Context, Deliverables: draft.Deliverables, CompletionCriteria: draft.CompletionCriteria, ResolvedDecisions: draft.ResolvedDecisions, AcceptedFingerprint: draft.Fingerprint, CreatedAt: now}
				work.IntentID, work.GoalID = intent.ID, goal.ID
			}
			task := core.Task{ID: core.ID("task-" + correlationID), WorkID: work.ID, Description: intent.NormalizedObjective, AcceptanceCriteria: intent.CompletionCriteria, ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agents[0].ID, AgentConfig: testAgentConfig(agents[0]), TaskContractVersion: "1", Status: core.TaskPending}
			bindTestAgentExecutionBriefs(t, correlationID, intent, &task)
			var goalPlan core.Plan
			if goalCase {
				goalPlan, err = buildTestPlan(correlationID, intent, task)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := gateway.Events(ctx, "")
				if err != nil {
					t.Fatal(err)
				}
				_, goalPlan.StrategicEventRefs, goalPlan.StrategicContextRefs, err = events.ResolveStrategicContext(string(org.ID), work, stream, 0)
				if err != nil {
					t.Fatal(err)
				}
				goalPlan.Fingerprint, err = core.FingerprintPlan(goalPlan)
				if err != nil {
					t.Fatal(err)
				}
				task.ExecutionBrief, err = core.AgentTaskExecutionBrief(intent, goalPlan.Tasks[0], goalPlan.Fingerprint)
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := saveTestTaskGraph(ctx, repository, org.ID, correlationID, intent, work, task); err != nil {
				t.Fatal(err)
			}
			if goalCase {
				_, err = gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: string(org.ID), EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), Payload: goalPlan, CorrelationID: correlationID})
			} else {
				err = saveTestPlan(ctx, gateway, correlationID, intent, task)
			}
			if err != nil {
				t.Fatal(err)
			}
			invalidateKnowledge := func() error {
				stale := active
				stale.Version = 3
				stale.Status = core.KnowledgeStale
				prior := 2
				stale.SupersedesVersion = &prior
				_, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_STALE", SourceActorID: "runtime", CorrelationID: "knowledge-inventory"}, ProjectionKind: "knowledge", RecordID: "inventory", Version: 3, Value: stale})
				return err
			}
			beforeCompletion := func() error {
				if invalidate {
					return invalidateKnowledge()
				}
				return nil
			}
			inferenceCase := strings.HasPrefix(name, "inference_")
			stopBoundary := errors.New("inference boundary checked")
			afterStart := func() error {
				if !inferenceCase {
					return nil
				}
				stream, err := gateway.Events(ctx, correlationID)
				if err != nil {
					t.Fatal(err)
				}
				var manifest core.ExecutionContextManifest
				for _, event := range stream {
					if event.EventType == "EXECUTION_CONTEXT_MANIFESTED" {
						if err := json.Unmarshal(event.Payload, &manifest); err != nil {
							t.Fatal(err)
						}
					}
				}
				if len(manifest.KnowledgeRefs) != 1 || manifest.KnowledgeRefs[0].ID != "inventory" {
					t.Fatal("inference fixture did not select Knowledge")
				}
				if name == "inference_stale" {
					if err := invalidateKnowledge(); err != nil {
						t.Fatal(err)
					}
				}
				policy := inference.Policy{Version: inference.PolicyVersion, OrganizationID: "org-1", Provider: manifest.Provider, Model: manifest.Model, ExecutionProfileVersion: manifest.ExecutionProfileVersion, Mode: inference.Local, MaxInputTokensPerRequest: 10000, MaxOutputTokensPerRequest: 1000, MaxTokensPerWindow: 30000, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "reviewer", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour)}
				if err := store.ActivateInferencePolicy(ctx, policy); err != nil {
					t.Fatal(err)
				}
				request := inference.InferenceRequest{Scope: inference.Scope{OrganizationID: "org-1", Purpose: inference.PurposeTaskExecution, RequestID: string(manifest.ExecutionID), ExecutionID: string(manifest.ExecutionID), TaskID: string(task.ID), CorrelationID: correlationID}, Descriptor: execution.FakeModel{}.Descriptor(), PromptSHA256: manifest.ExecutionInputSHA256}
				reservation, err := store.ReserveInference(ctx, request)
				if name == "inference_stale" {
					if err == nil {
						t.Fatal("inference admitted Knowledge invalidated after execution start")
					}
					if !strings.Contains(strings.ToLower(err.Error()), "knowledge") {
						t.Fatalf("unexpected inference rejection: %v", err)
					}
				} else {
					if err != nil {
						t.Fatalf("current Knowledge inference rejected: %v", err)
					}
					if _, err := store.ReconcileInference(ctx, reservation, nil, inference.ReconciliationNotSent); err != nil {
						t.Fatal(err)
					}
					if name == "inference_later_stale" {
						if err := invalidateKnowledge(); err != nil {
							t.Fatal(err)
						}
					}
				}
				return stopBoundary
			}
			err = saveTestVerifiedTaskAtBoundaries(ctx, gateway, repository, org.ID, correlationID, projections.Versioned[core.Task]{Version: 1, CorrelationID: correlationID, Value: task}, afterStart, beforeCompletion)
			if inferenceCase {
				if !errors.Is(err, stopBoundary) {
					t.Fatalf("inference boundary was not reached: %v", err)
				}
			} else if invalidate {
				if err == nil || !strings.Contains(err.Error(), "knowledge is invalid at completion admission") {
					t.Fatalf("expected current Knowledge rejection, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("valid completion rejected: %v", err)
			}
			snapshot, err := repository.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := core.TaskCompleted
			if invalidate || inferenceCase {
				want = core.TaskRunning
			}
			if snapshot.Tasks[task.ID].Value.Status != want {
				t.Fatalf("unexpected Task status: %s", snapshot.Tasks[task.ID].Value.Status)
			}
			if !invalidate && !inferenceCase {
				if name == "before_work_completion" {
					if err := invalidateKnowledge(); err != nil {
						t.Fatal(err)
					}
				}
				stream, err := gateway.Events(ctx, correlationID)
				if err != nil {
					t.Fatal(err)
				}
				var plan core.Plan
				for _, event := range stream {
					if event.EventType == "PLAN_CREATED" {
						if err := json.Unmarshal(event.Payload, &plan); err != nil {
							t.Fatal(err)
						}
					}
				}
				taskEvidence, err := completedWorkTaskEvidence(stream, snapshot, work.ID, org.ID, correlationID, plan)
				if err != nil {
					t.Fatal(err)
				}
				evidenceEvent, evidence, evidenceErr := New(gateway).ensureWorkCompletionEvidence(ctx, stream, snapshot.Works[work.ID], intent, plan, taskEvidence)
				err = evidenceErr
				if name == "before_work_completion" {
					if err == nil || !strings.Contains(err.Error(), "knowledge is invalid at completion admission") {
						t.Fatalf("expected Work completion Knowledge rejection, got %v", err)
					}
				} else if err != nil {
					t.Fatalf("valid Work evidence rejected: %v", err)
				}
				if name == "current" || name == "before_work_transition" || name == "after_work_transition" || goalCase {
					if name == "before_work_transition" {
						if err := invalidateKnowledge(); err != nil {
							t.Fatal(err)
						}
					}
					completed := snapshot.Works[work.ID].Value
					completed.Status = core.WorkCompleted
					err := repository.SaveCompletedWork(ctx, org.ID, "runtime", correlationID, snapshot.Works[work.ID].Version+1, completed, events.WorkCompletionTransitionPayload{EvidenceEventRef: evidenceEvent.EventID, Fingerprint: evidence.Fingerprint})
					if name == "before_work_transition" {
						if err == nil || !strings.Contains(err.Error(), "knowledge is invalid at completion admission") {
							t.Fatalf("expected final Work transition Knowledge rejection, got %v", err)
						}
					} else if err != nil {
						t.Fatalf("valid Work transition rejected: %v", err)
					}
					if name == "after_work_transition" {
						if err := invalidateKnowledge(); err != nil {
							t.Fatal(err)
						}
					}
					if goalCase {
						if name == "goal_before_progress" {
							if err := invalidateKnowledge(); err != nil {
								t.Fatal(err)
							}
						}
						progress, err := store.AppendGoalProgress(ctx, string(org.ID), intent.GoalID)
						if name == "goal_before_progress" {
							if err == nil || !strings.Contains(err.Error(), "goal Work completion lacks authoritative evidence") {
								t.Fatalf("expected Goal evidence rejection after Knowledge invalidation: %v", err)
							}
						} else {
							if err != nil {
								t.Fatalf("valid Goal progress rejected: %v", err)
							}
							if progress.Evaluation.Result != events.GoalProgressTargetAchieved {
								t.Fatalf("Goal was not achieved: %s", progress.Evaluation.Result)
							}
						}
						if name == "goal_after_progress" {
							if err := invalidateKnowledge(); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := recovery.Verify(ctx, path); err != nil {
				t.Fatalf("recovery rejected admitted history: %v", err)
			}

		})
	}
}

func seedCompletionFactualKnowledge(t *testing.T, store *ledger.SQLite, gateway *events.Gateway) core.KnowledgeRecord {
	t.Helper()
	ctx := t.Context()
	setup, err := gateway.Events(ctx, "setup")
	if err != nil || len(setup) != 1 {
		t.Fatalf("organization evidence: %v", err)
	}
	candidate := core.KnowledgeRecord{KnowledgeID: "inventory", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeClaim, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Inventory", Content: "The inventory contains three records.", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{setup[0].EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
	if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-inventory"}, ProjectionKind: "knowledge", RecordID: "inventory", Version: 1, Value: candidate}); err != nil {
		t.Fatal(err)
	}
	lease := core.CapabilityLease{ID: "lease-validator", ActorID: "reviewer", ActorKind: core.PrincipalHuman, Action: "knowledge.validate", Resource: "inventory", Scope: "org-1", OriginTaskID: "validation-task"}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "runtime", "validation-task", []string{"approval-validator"}, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		t.Fatal(err)
	}
	trace := core.AuthorizationTrace{Allowed: true, LeaseID: lease.ID, ActorID: lease.ActorID, ActorKind: lease.ActorKind, TaskID: lease.OriginTaskID, Action: lease.Action, Resource: lease.Resource, Scope: lease.Scope, Reason: "exact capability lease matched"}
	check, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "CAPABILITY_CHECKED", SourceActorID: "reviewer", TaskID: "validation-task", AuthorizationRefs: []string{string(lease.ID)}, Payload: trace})
	if err != nil {
		t.Fatal(err)
	}
	judgment, err := gateway.PublishTrusted(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", SourceActorID: "reviewer", TaskID: "validation-task", CorrelationID: "validation", Payload: events.KnowledgeJudgmentPayload{KnowledgeID: "inventory", CandidateVersion: 1, ContextUse: core.KnowledgeFactualReference, Decision: events.KnowledgeJudgmentValidated, Statement: "I independently validate this exact candidate as factual reference evidence.", CapabilityCheckEventID: check.EventID, SourcePrincipalID: "reviewer", SourcePrincipalKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", ArtifactRefs: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	active := candidate
	active.Version = 2
	active.Status = core.KnowledgeActive
	active.ContextUse = core.KnowledgeFactualReference
	active.ValidationMethod = core.KnowledgeValidationHuman
	active.ValidationRefs = []string{check.EventID, judgment.EventID}
	active.ValidatedBy, active.ValidatedByKind = "reviewer", core.PrincipalHuman
	verified := time.Now().UTC()
	active.LastVerifiedAt = &verified
	prior := 1
	active.SupersedesVersion = &prior
	if _, err := gateway.PublishProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_ACTIVATED", SourceActorID: "runtime", CorrelationID: "knowledge-inventory"}, ProjectionKind: "knowledge", RecordID: "inventory", Version: 2, Value: active}); err != nil {
		t.Fatal(err)
	}
	return active
}
