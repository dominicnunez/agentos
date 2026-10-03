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
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentManifestRecordRefs(t *testing.T) {
	parallelIncidentTest(t)
	for _, scenario := range []string{"knowledge", "task", "mission", "goal", "start-mission", "start-goal", "start-input", "task-identity", "agent"} {
		t.Run(scenario, func(t *testing.T) {
			kind, start := strings.CutPrefix(scenario, "start-")
			if scenario == "start-input" || scenario == "task-identity" {
				kind = "task"
			}
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			correlation, reference := appendManifestTarget(t, store, kind)
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "incoming", true)
			request := appendBenchmarkTaskInference(t, store, agent, config, "incoming")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("writer-created inference history failed full replay: %v", err)
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", correlation, 256)
			if err != nil {
				t.Fatalf("independent selected Work failed incident validation: %v", err)
			}
			var manifestID string
			var body []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT event_id,payload FROM events WHERE organization_id='org-1' AND correlation_id='incoming' AND event_type='EXECUTION_CONTEXT_MANIFESTED'`).Scan(&manifestID, &body); err != nil {
				t.Fatal(err)
			}
			for _, event := range baseline.DependencyEvents {
				if event.EventID == manifestID {
					t.Fatal("unrelated execution manifest was already selected")
				}
			}
			var manifest core.ExecutionContextManifest
			if err := json.Unmarshal(body, &manifest); err != nil {
				t.Fatal(err)
			}
			ref := []core.VersionedRef{{ID: reference, Version: "1", MaterializationState: core.MaterializedFull}}
			if start {
				var startID string
				if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE organization_id='org-1' AND correlation_id='incoming' AND event_type='EXECUTION_STARTED'`).Scan(&startID); err != nil {
					t.Fatal(err)
				}
				if scenario == "start-input" {
					var selectedID string
					for _, event := range baseline.Work.Events {
						if event.EventType == "WORK_CREATED" {
							selectedID = event.EventID
						}
					}
					if selectedID == "" {
						t.Fatal("missing selected event")
					}
					ChangeIncidentDetailForTest(t, store, startID, "input_event_refs", []string{selectedID})
				} else {
					ChangeIncidentDetailForTest(t, store, startID, "strategic_context_refs", ref)
				}
			} else {
				switch {
				case scenario == "task-identity":
					manifest.TaskID = core.ID(reference)
				case kind == "agent":
					manifest.AgentID = core.ID(reference)
				case kind == "knowledge":
					manifest.KnowledgeRefs = ref
				case kind == "task":
					manifest.CoordinationRefs = ref
				default:
					manifest.AdditionalContextRefs = ref
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					changed, err := json.Marshal(manifest)
					if err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, changed, manifestID); err != nil {
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
			if scenario == "start-input" {
				if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
					t.Fatalf("shape-only start refs changed replay semantics: %v", err)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", correlation, 256)
				if err != nil {
					t.Fatalf("shape-only start refs poisoned unrelated incident: %v", err)
				}
				for _, event := range snapshot.DependencyEvents {
					if event.EventID == manifestID {
						t.Fatal("shape-only start refs selected unrelated execution")
					}
				}
				return
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("full replay accepted substituted manifest record reference")
			} else {
				t.Logf("owner rejection: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", correlation, 256)
			if err == nil {
				t.Fatal("incident omitted foreign manifest record reference")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed incident returned partial evidence")
			}
		})
	}
}

func TestIncidentManifestRefGrammar(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, tc := range []struct {
		name, event, kind, body string
		want                    int
	}{
		{"task-identity", "EXECUTION_CONTEXT_MANIFESTED", "task", `{"task_id":"selected"}`, 1},
		{"agent-identity", "EXECUTION_CONTEXT_MANIFESTED", "agent", `{"agent_id":"selected"}`, 1},
		{"knowledge", "EXECUTION_CONTEXT_MANIFESTED", "knowledge", `{"knowledge_refs":[{"id":"selected"}]}`, 1},
		{"coordination", "EXECUTION_CONTEXT_MANIFESTED", "task", `{"coordination_refs":[{"id":"selected"}]}`, 1},
		{"mission", "EXECUTION_CONTEXT_MANIFESTED", "mission", `{"additional_context_refs":[{"id":"mission/selected"}]}`, 1},
		{"goal", "EXECUTION_CONTEXT_MANIFESTED", "goal", `{"additional_context_refs":[{"id":"goal/selected"}]}`, 1},
		{"start", "EXECUTION_STARTED", "goal", `{"projection":{"projection_kind":"task","record_id":"unrelated"},"detail":{"strategic_context_refs":[{"id":"goal/selected"}]}}`, 1},
		{"wrong-kind", "EXECUTION_CONTEXT_MANIFESTED", "goal", `{"additional_context_refs":[{"id":"mission/selected"}]}`, 0},
		{"bare-id", "EXECUTION_CONTEXT_MANIFESTED", "goal", `{"additional_context_refs":[{"id":"selected"}]}`, 0},
		{"unknown-prefix", "EXECUTION_CONTEXT_MANIFESTED", "goal", `{"additional_context_refs":[{"id":"other/selected"}]}`, 0},
		{"prefix-case", "EXECUTION_CONTEXT_MANIFESTED", "goal", `{"additional_context_refs":[{"id":"Goal/selected"}]}`, 0},
		{"artifact", "EXECUTION_CONTEXT_MANIFESTED", "knowledge", `{"artifact_refs":[{"id":"selected"}]}`, 0},
		{"ordinary-note", "AUDIT_NOTE", "knowledge", `{"knowledge_refs":[{"id":"selected"}]}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var match, legacy int
			if err := store.db.QueryRowContext(t.Context(), `SELECT agentos_incident_link_match_v2(0,?,?,?,'selected'),agentos_incident_link_match_v1(0,?,?,?,'selected')`, tc.event, tc.body, tc.kind, tc.event, tc.body, tc.kind).Scan(&match, &legacy); err != nil {
				t.Fatal(err)
			}
			if match != tc.want || legacy != 0 {
				t.Fatalf("match=%d legacy=%d want=%d", match, legacy, tc.want)
			}
		})
	}
}

func appendManifestTarget(t *testing.T, store *SQLite, kind string) (string, string) {
	t.Helper()
	_, work := planScopeParents(t, store, "selected-org", "selected", "selected-work")
	now := time.Now().UTC()
	correlation, recordID, eventType := "selected", "selected-"+kind, ""
	var value any
	switch kind {
	case "agent":
		agent, _ := appendTaskAssignmentAgent(t, t.Context(), store, "selected-org", "selected", false)
		return "roster-selected", string(agent.ID)
	case "knowledge":
		stream, err := store.Events(t.Context(), "selected")
		if err != nil {
			t.Fatal(err)
		}
		correlation = "knowledge-" + recordID
		eventType = "KNOWLEDGE_PROPOSED"
		value = core.KnowledgeRecord{KnowledgeID: core.ID(recordID), OrganizationID: "selected-org", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "selected-org", Status: core.KnowledgeCandidate, Title: "Selected source", Content: "Recorded evidence", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{stream[0].EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: now, ValidationMethod: core.KnowledgeValidationUnvalidated}
	case "task":
		eventType = "TASK_CREATED"
		value = core.Task{ID: core.ID(recordID), WorkID: work.ID, Description: "pending task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
	case "mission", "goal":
		mission := core.Mission{ID: "selected-mission", OrganizationID: "selected-org", Statement: "selected mission", Status: core.MissionActive, CreatedAt: now}
		if kind == "mission" {
			eventType = "MISSION_CREATED"
			value = mission
		} else {
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "selected-org", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "selected"}, ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission}); err != nil {
				t.Fatal(err)
			}
			eventType = "GOAL_CREATED"
			value = core.Goal{ID: core.ID(recordID), OrganizationID: "selected-org", MissionID: mission.ID, Objective: "selected goal", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "verified result", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}
		}
	}
	draft := events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "selected-org", EventType: eventType, SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: kind, RecordID: recordID, Version: 1, Value: value}
	if kind == "task" {
		draft.Event.TaskID = recordID
	}
	if _, err := store.AppendProjection(t.Context(), draft); err != nil {
		t.Fatal(err)
	}
	if kind == "mission" || kind == "goal" {
		return correlation, kind + "/" + recordID
	}
	return correlation, recordID
}
