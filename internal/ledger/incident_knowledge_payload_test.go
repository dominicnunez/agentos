package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentKnowledgePayloadRefs(t *testing.T) {
	for _, family := range []string{"agent-proposal", "deterministic-validation"} {
		for _, mode := range []string{"foreign", "event-only", "record-only", "same-org", "unrelated", "unconsumed"} {
			t.Run(family+"/"+mode, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "knowledge.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"message": "Selected evidence"}})
				if err != nil {
					t.Fatal(err)
				}
				var raw events.Event
				if family == "agent-proposal" {
					raw = appendIncidentAgentCandidate(t, store, mode != "unconsumed")
				} else {
					raw = appendIncidentDeterministicKnowledge(t, store, mode != "unconsumed")
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
					t.Fatalf("writer full replay: %v", err)
				}
				invalid := mode == "foreign" || mode == "event-only" || mode == "record-only"
				if invalid || mode == "unconsumed" {
					field, value := "outcome_event_ref", any(selected.EventID)
					if family == "agent-proposal" {
						field, value = "occurrence_event_refs", []string{selected.EventID}
					}
					changeKnowledgePayloadRef(t, store, raw, field, value)
				}
				if mode == "event-only" || mode == "record-only" {
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						query := `DELETE FROM records WHERE kind='knowledge'`
						if mode == "record-only" {
							query = `DELETE FROM events WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' ORDER BY version DESC LIMIT 1)`
						}
						if _, err := tx.ExecContext(t.Context(), query); err != nil {
							return err
						}
						if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
							return err
						}
						return rebuildEventIntegrity(t.Context(), tx)
					}); err != nil {
						t.Fatal(err)
					}
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				stream, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				// Event-only recovery still sees the consuming admission. A record
				// orphan has no admission to replay and must fail exact backing in
				// the storage-aware reader instead.
				if mode != "record-only" && (fullErr != nil) != invalid {
					t.Fatalf("full owner applicability: %v", fullErr)
				}
				organization, correlation := "org-2", "selected"
				if mode == "same-org" {
					organization, correlation = "org-1", raw.CorrelationID
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), organization, correlation, 256)
				if invalid {
					if err == nil {
						t.Fatal("incoming consumed Knowledge payload reference omitted")
					}
					if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatal("invalid evidence returned partial snapshot")
					}
				} else if err != nil {
					t.Fatalf("valid control: %v", err)
				}
			})
		}
	}
}

func appendIncidentAgentCandidate(t *testing.T, store *SQLite, consumed bool) events.Event {
	t.Helper()
	task := incidentTestExecution(t, store)
	id := core.ID("agent-knowledge")
	title, content, applicability := "Execution observation", "An observation from an admitted execution.", ""
	basis := core.KnowledgeBasisSingleExperience
	proposal, err := events.NewGateway(store).PublishAgentDraft(t.Context(), "org-1", string(task.AssigneeID), "execution-stop-task-v2", "stop-work", events.Draft{EventType: "KNOWLEDGE_PROPOSED", TaskID: string(task.ID), Payload: events.KnowledgeProposedPayload{KnowledgeID: &id, KnowledgeType: core.KnowledgeLesson, Title: &title, Content: content, BasisType: &basis, Applicability: &applicability}})
	if err != nil {
		t.Fatal(err)
	}
	if consumed {
		candidate := core.KnowledgeRecord{KnowledgeID: id, OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: title, Content: content, Basis: basis, ProvenanceEventRefs: []string{proposal.EventID}, CreatedBy: task.AssigneeID, CreatedByKind: core.PrincipalAgent, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-agent-knowledge"}, ProjectionKind: "knowledge", RecordID: string(id), Version: 1, Value: candidate}); err != nil {
			t.Fatal(err)
		}
	}
	return proposal
}

func appendIncidentDeterministicKnowledge(t *testing.T, store *SQLite, consumed bool) events.Event {
	t.Helper()
	now := time.Now().UTC()
	organization, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "Knowledge", PolicyVersion: "v1", CreatedAt: now}})
	if err != nil {
		t.Fatal(err)
	}
	candidate := core.KnowledgeRecord{KnowledgeID: "validated", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Validated knowledge", Content: "Deterministic evidence", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{organization.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-validated"}, ProjectionKind: "knowledge", RecordID: "validated", Version: 1, Value: candidate}); err != nil {
		t.Fatal(err)
	}
	now = time.Now().UTC()
	intent := core.Intent{ID: "validation-intent", OrganizationID: "org-1", OriginalInstruction: "echo validated", NormalizedObjective: "echo validated", AcceptedFingerprint: "internal-validation", CreatedAt: now}
	work := core.Work{ID: "validation-work", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	task := core.Task{ID: "validation-task", WorkID: work.ID, Description: "echo validated", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, RuntimeHandlerRef: "builtin.echo", TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "validation"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "validation"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "validation"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	plan := core.Plan{ID: "plan-validation", IntentID: intent.ID, IntentFingerprint: intent.AcceptedFingerprint, Version: 1, Tasks: []core.PlanTask{{Key: "validate", Description: task.Description, ExecutionKind: task.ExecutionKind, ModelInferencePolicy: task.ModelInferencePolicy}}, CreatedAt: now}
	plan.Fingerprint, err = core.FingerprintPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "PLAN_CREATED", SourceActorID: "runtime", TaskID: "task-validation", CorrelationID: "validation", Payload: plan}); err != nil {
		t.Fatal(err)
	}
	task.Status = core.TaskRunning
	if _, _, err := store.AppendExecutionStart(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "validation", Payload: events.ExecutionStartDetail{InboxCutoffSequence: 0}}, ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task}, nil, nil); err != nil {
		t.Fatal(err)
	}
	outcome := core.ToolOutcome{ToolInvocationID: "validation", ToolID: "builtin.echo", ToolVersion: "1", ObservedEffect: "validated", Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified, Retryability: core.NotRetryable, StartedAt: now, FinishedAt: now}
	evidence, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", SourceActorID: "runtime", SourceExecutionID: "execution-validation-task-v2", TaskID: string(task.ID), CorrelationID: "validation", Payload: outcome})
	if err != nil {
		t.Fatal(err)
	}
	validation, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_VALIDATION_RECORDED", SourceActorID: "runtime", SourceExecutionID: "execution-validation-task-v2", TaskID: string(task.ID), CorrelationID: "validation", Payload: events.KnowledgeDeterministicValidationPayload{KnowledgeID: candidate.KnowledgeID, CandidateVersion: 1, OutcomeEventRef: evidence.EventID, ArtifactRefs: []string{}}})
	if err != nil {
		t.Fatal(err)
	}
	if consumed {
		active := candidate
		active.Version, active.Status, active.ValidationMethod = 2, core.KnowledgeActive, core.KnowledgeValidationDeterministic
		active.ValidationRefs = []string{validation.EventID}
		active.ValidatedBy, active.ValidatedByKind = "runtime", core.PrincipalRuntime
		verifiedAt, previous := time.Now().UTC(), 1
		active.LastVerifiedAt, active.SupersedesVersion = &verifiedAt, &previous
		if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_ACTIVATED", SourceActorID: "runtime", CorrelationID: "knowledge-validated"}, ProjectionKind: "knowledge", RecordID: "validated", Version: 2, Value: active}); err != nil {
			t.Fatal(err)
		}
	}
	return validation
}

func changeKnowledgePayloadRef(t *testing.T, store *SQLite, event events.Event, field string, target any) {
	t.Helper()
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	var err error
	payload[field], err = json.Marshal(target)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}
