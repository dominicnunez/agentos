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
	parallelIncidentTest(t)
	for _, family := range []string{"agent-proposal", "deterministic-validation", "human-judgment"} {
		for _, mode := range []string{"foreign", "event-only", "record-only", "record-kind", "event-counterpart-kind", "record-counterpart-kind", "event-label", "event-status", "record-status", "event-method", "record-method", "both-method", "candidate-refs", "same-org", "unrelated", "unconsumed"} {
			if family == "agent-proposal" && (mode == "event-status" || mode == "record-status" || mode == "event-method" || mode == "record-method" || mode == "both-method" || mode == "candidate-refs") {
				// Agent proposals use creator applicability, not activation status.
				continue
			}
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
				raw := appendIncidentKnowledgeStatement(t, store, family, mode != "unconsumed")
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				leases, freezes, err := store.KnowledgeAuthorityAdmissions(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err != nil {
					t.Fatalf("writer full replay: %v", err)
				}
				invalid := mode != "same-org" && mode != "unrelated" && mode != "unconsumed"
				if invalid || mode == "unconsumed" {
					field, value := "outcome_event_ref", any(selected.EventID)
					switch family {
					case "agent-proposal":
						field, value = "occurrence_event_refs", []string{selected.EventID}
					case "human-judgment":
						field = "capability_check_event_id"
					}
					changeKnowledgePayloadRef(t, store, raw, field, value)
				}
				if mode == "both-method" || mode == "candidate-refs" {
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						var admission string
						if err := tx.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='knowledge' ORDER BY version DESC LIMIT 1`).Scan(&admission); err != nil {
							return err
						}
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_remove(json_set(payload,'$.projection.value.validation_method','UNVALIDATED'),'$.projection.value.validated_by_kind') AS BLOB) WHERE event_id=?`, admission); err != nil {
							return err
						}
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_remove(json_set(body,'$.value.validation_method','UNVALIDATED'),'$.value.validated_by_kind') AS BLOB) WHERE admission_event_id=?`, admission); err != nil {
							return err
						}
						if mode == "candidate-refs" {
							if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='KNOWLEDGE_PROPOSED',payload=CAST(json_set(payload,'$.projection.value.status','CANDIDATE') AS BLOB) WHERE event_id=?`, admission); err != nil {
								return err
							}
							if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_set(body,'$.value.status','CANDIDATE') AS BLOB) WHERE admission_event_id=?`, admission); err != nil {
								return err
							}
						}
						if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
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
				if mode == "event-counterpart-kind" || mode == "record-counterpart-kind" || mode == "event-label" || mode == "event-status" || mode == "record-status" || mode == "event-method" || mode == "record-method" {
					var admission string
					if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='knowledge' ORDER BY version DESC LIMIT 1`).Scan(&admission); err != nil {
						t.Fatal(err)
					}
					switch mode {
					case "event-method":
						if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_remove(json_set(payload,'$.projection.value.validation_method','UNVALIDATED'),'$.projection.value.validated_by_kind') AS BLOB) WHERE event_id=?`, admission); err != nil {
							t.Fatal(err)
						}
					case "record-method":
						if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_remove(json_set(body,'$.value.validation_method','UNVALIDATED'),'$.value.validated_by_kind') AS BLOB) WHERE admission_event_id=?`, admission); err != nil {
							t.Fatal(err)
						}
					case "event-label":
						if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE' WHERE event_id=?`, admission); err != nil {
							t.Fatal(err)
						}
					case "event-status":
						if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=CAST(json_set(payload,'$.projection.value.status','CANDIDATE') AS BLOB) WHERE event_id=?`, admission); err != nil {
							t.Fatal(err)
						}
					case "record-status":
						if _, err := store.db.ExecContext(t.Context(), `UPDATE records SET body=CAST(json_set(body,'$.value.status','CANDIDATE') AS BLOB) WHERE admission_event_id=?`, admission); err != nil {
							t.Fatal(err)
						}
					case "event-counterpart-kind":
						changeIncidentSourceKind(t, store, admission, "counterpart-kind", false)
					case "record-counterpart-kind":
						ChangeIncidentRecordKindForTest(t, store, admission, true)
					}
					field := "provenance_event_refs"
					if family != "agent-proposal" {
						field = "validation_refs"
					}
					// Only the damaged side still consumes the raw statement.
					// The counterpart retains independent ownership evidence.
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						query, fieldPath := `UPDATE records SET body=CAST(json_set(body,?,json('[]')) AS BLOB) WHERE admission_event_id=?`, "$.value."+field
						if mode == "record-counterpart-kind" || mode == "record-status" || mode == "record-method" {
							query, fieldPath = `UPDATE events SET payload=CAST(json_set(payload,?,json('[]')) AS BLOB) WHERE event_id=?`, "$.projection.value."+field
						}
						if _, err := tx.ExecContext(t.Context(), query, fieldPath, admission); err != nil {
							return err
						}
						if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
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
				if mode == "event-only" || mode == "record-only" || mode == "record-kind" {
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						query := `DELETE FROM records WHERE kind='knowledge'`
						if mode == "record-only" || mode == "record-kind" {
							query = `DELETE FROM events WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' ORDER BY version DESC LIMIT 1)`
						}
						if _, err := tx.ExecContext(t.Context(), query); err != nil {
							return err
						}
						if mode == "record-kind" {
							// Retain the typed body and admission metadata, while losing
							// the physical kind and the consuming event independently.
							if _, err := tx.ExecContext(t.Context(), `UPDATE records SET kind='authorization_trace' WHERE kind='knowledge' AND version=(SELECT MAX(version) FROM records WHERE kind='knowledge')`); err != nil {
								return err
							}
							if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
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
				_, fullErr := events.ValidateProjectionHistory(stream, nil, leases, freezes)
				// Event-only recovery still sees the consuming admission. A record
				// orphan has no admission to replay and must fail exact backing in
				// the storage-aware reader instead.
				if mode != "record-only" && mode != "record-kind" && (fullErr != nil) != invalid {
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

func appendIncidentKnowledgeStatement(t *testing.T, store *SQLite, family string, consumed bool) events.Event {
	t.Helper()
	switch family {
	case "agent-proposal":
		return appendIncidentAgentCandidate(t, store, consumed)
	case "deterministic-validation":
		return appendIncidentDeterministicKnowledge(t, store, consumed)
	case "human-judgment":
		appendIncidentLeaseKnowledge(t, store, "org-1", "human-validation", true)
		stream, err := store.Events(t.Context(), "")
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range stream {
			if event.EventType != "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED" || event.CorrelationID != "judgment-human-validation" {
				continue
			}
			if !consumed {
				// Retain the writer's raw judgment, candidate, and authorization,
				// removing only the complete activation and its exact backing.
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id IN (SELECT admission_event_id FROM records WHERE kind='knowledge' AND record_id='human-validation' AND version=2)`); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='knowledge' AND record_id='human-validation' AND version=2`); err != nil {
						return err
					}
					if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
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
			return event
		}
	}
	t.Fatalf("missing writer statement for %s", family)
	return events.Event{}
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

// Keep direct storage mutation in the owning package; the external test compares
// the public incident reader with the independent storage-aware recovery reader.
func IncidentKnowledgeOwnerFixtureForTest(t *testing.T, family string, opaque bool) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "knowledge-owner.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"message": "Selected evidence"}})
	if err != nil {
		t.Fatal(err)
	}
	var raw events.Event
	if family == "agent-proposal" {
		raw = appendIncidentAgentCandidate(t, store, true)
	} else {
		raw = appendIncidentDeterministicKnowledge(t, store, true)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("writer full replay: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256); err != nil {
		t.Fatalf("healthy incident: %v", err)
	}
	field, value := "outcome_event_ref", any(selected.EventID)
	if family == "agent-proposal" {
		field, value = "occurrence_event_refs", []string{selected.EventID}
	}
	changeKnowledgePayloadRef(t, store, raw, field, value)
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var admission string
		if err := tx.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='knowledge' ORDER BY version DESC LIMIT 1`).Scan(&admission); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_id=?`, admission); err != nil {
			return err
		}
		query := `UPDATE records SET kind='authorization_trace' WHERE admission_event_id=?`
		if opaque {
			query = `UPDATE records SET kind='authorization_trace',admission_event_id='',admission_fingerprint='' WHERE admission_event_id=?`
		}
		if _, err := tx.ExecContext(t.Context(), query, admission); err != nil {
			return err
		}
		if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
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
	return path
}
