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
	"github.com/dominicnunez/agentos/internal/modelinput"
)

// Task organization ownership is routing.organization_id. Unlike Work's
// parent-derived organization, this retained claim can establish the tenant
// of an orphan sharing the selected correlation without a selected Work link.
func TestIncidentOrphanTaskRoutingScope(t *testing.T) {
	parallelIncidentTest(t)
	for _, mode := range []string{
		"healthy-control", "unrelated-control", "foreign-tenant-control", "work-parent-control",
		"nested-note-control", "forbidden-direct-org-control",
		"selected-canonical", "selected-later-correlation", "selected-escaped-correlation",
		"selected-later-routing-org", "selected-duplicate-routing", "selected-escaped-routing-org", "selected-escaped-value",
	} {
		t.Run(mode, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "selected", "selected-work")
			selectedTask := core.Task{ID: "selected-task", WorkID: "selected-work", Description: "selected work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(selectedTask.ID), CorrelationID: "selected"}, ProjectionKind: "task", RecordID: string(selectedTask.ID), Version: 1, Value: selectedTask}); err != nil {
				t.Fatal(err)
			}
			organization := "org-1"
			if mode == "foreign-tenant-control" || mode == "selected-later-routing-org" || mode == "selected-duplicate-routing" || mode == "nested-note-control" || mode == "forbidden-direct-org-control" {
				organization = "org-2"
				appendTaskProjectionParents(t, t.Context(), store, organization, "unrelated", "unrelated-work")
			} else {
				now := time.Now().UTC()
				intent := core.Intent{ID: "unrelated-intent", OrganizationID: core.ID(organization), OriginalInstruction: "test task", NormalizedObjective: "test task", CreatedAt: now}
				work := core.Work{ID: "unrelated-work", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
				for _, draft := range []events.ProjectionDraft{
					{Event: events.TrustedDraft{OrganizationID: organization, EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
					{Event: events.TrustedDraft{OrganizationID: organization, EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
				} {
					if _, err := store.AppendProjection(t.Context(), draft); err != nil {
						t.Fatal(err)
					}
				}
			}
			appendTaskScopeAgent(t, store, organization)
			if err := incomingRosterFullValidation(t, store); err != nil {
				t.Fatalf("writer-derived complete history: %v", err)
			}
			if _, err := admittedProjectionRecordsBounded(t.Context(), store.db, 4<<20, ""); err != nil {
				t.Fatalf("healthy complete record owner: %v", err)
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
			if err != nil {
				t.Fatalf("healthy unrelated routed Task: %v", err)
			}
			if mode == "healthy-control" {
				return
			}
			kind, templateID, orphanID := "task", "template-task", "orphan-task"
			if mode == "work-parent-control" {
				kind, templateID, orphanID = "work", "unrelated-work", "orphan-work"
			}
			var body []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind=? AND record_id=?`, kind, templateID).Scan(&body); err != nil {
				t.Fatal(err)
			}
			var record events.ProjectionRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			var value map[string]json.RawMessage
			if err := json.Unmarshal(record.Value, &value); err != nil {
				t.Fatal(err)
			}
			if _, exists := value["organization_id"]; exists {
				t.Fatal("writer fixture has a forbidden direct organization claim")
			}
			if mode == "nested-note-control" {
				value["note"] = json.RawMessage(`{"routing":{"organization_id":"org-1"}}`)
			}
			if mode == "forbidden-direct-org-control" {
				value["organization_id"] = json.RawMessage(`"org-1"`)
			}
			value["id"], err = json.Marshal(orphanID)
			if err != nil {
				t.Fatal(err)
			}
			record.RecordID, record.CorrelationID = orphanID, "selected"
			if mode == "unrelated-control" || strings.Contains(mode, "correlation") {
				record.CorrelationID = "unrelated-orphan"
			}
			record.Value, err = json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			body, err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(mode, "correlation") {
				const first = `"correlation_id":"unrelated-orphan"`
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its first unrelated correlation")
				}
				key := `"correlation_id"`
				if mode == "selected-escaped-correlation" {
					key = `"correlation\u005fid"`
				}
				body = []byte(strings.Replace(string(body), first, first+`,`+key+`:"selected"`, 1))
			}
			switch mode {
			case "selected-later-routing-org":
				const first = `"organization_id":"org-2"`
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its first foreign routing organization")
				}
				body = []byte(strings.Replace(string(body), first, first+`,"organization_id":"org-1"`, 1))
			case "selected-duplicate-routing":
				routing := string(value["routing"])
				const foreign = `"organization_id":"org-2"`
				if strings.Count(routing, foreign) != 1 {
					t.Fatal("fixture lacks its original foreign routing container")
				}
				first := `"routing":` + routing
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its first routing container")
				}
				selected := strings.Replace(routing, foreign, `"organization_id":"org-1"`, 1)
				body = []byte(strings.Replace(string(body), first, first+`,"routing":`+selected, 1))
			case "selected-escaped-routing-org":
				const first = `"organization_id":"org-1"`
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its owned routing organization")
				}
				body = []byte(strings.Replace(string(body), first, `"organization\u005fid":"org-1"`, 1))
			case "selected-escaped-value":
				const first = `"value":`
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its root value container")
				}
				body = []byte(strings.Replace(string(body), first, `"val\u0075e":`, 1))
			}
			assertTaskScopeUnlinked(t, store, baseline, kind, orphanID, body)
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES(?,?,1,?,?)`, kind, orphanID, body, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			// Recovery validates every admitted projection record against its exact
			// event. A healthy event graph alone cannot detect this retained orphan.
			if err := incomingRosterFullValidation(t, store); err != nil {
				t.Fatalf("orphan unexpectedly altered the healthy event graph: %v", err)
			}
			ownerErr := store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, "")
				return err
			})
			if ownerErr == nil {
				t.Fatal("complete record owner accepted orphan admission")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
			if !strings.HasPrefix(mode, "selected-") {
				if err != nil {
					t.Fatalf("unrelated orphan affected selected incident: %v", err)
				}
				if !reflect.DeepEqual(snapshot, baseline) {
					t.Fatal("unrelated orphan changed the selected snapshot")
				}
				return
			}
			if err == nil {
				t.Fatalf("incident omitted orphan Task with selected correlation and owned routing organization: public=%d private=%d complete-owner=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid orphan returned a partial snapshot")
			}
		})
	}
}

func assertTaskScopeUnlinked(t *testing.T, store *SQLite, baseline events.IncidentSnapshot, kind, orphanID string, body []byte) {
	t.Helper()
	keys := map[incidentKey]bool{}
	for _, stream := range [][]events.Event{baseline.Work.Events, baseline.RelatedEvents, baseline.DependencyEvents} {
		for _, event := range stream {
			keys[incidentKey{"event", event.EventID}] = true
			payload, present, err := events.AdmittedProjection(event)
			if err != nil {
				t.Fatal(err)
			}
			if present {
				keys[incidentKey{payload.Projection.ProjectionKind, payload.Projection.RecordID}] = true
			}
		}
	}
	if keys[incidentKey{kind, orphanID}] || keys[incidentKey{"work", "unrelated-work"}] || keys[incidentKey{"task", "template-task"}] {
		t.Fatal("orphan shares a selected physical, owned or Work identity")
	}
	for key := range keys {
		var linked bool
		query := `WITH source(kind,body) AS (VALUES (?,?)) SELECT ` + incidentLinkMatch(true, "source", "?", "?") + ` FROM source`
		if err := store.db.QueryRowContext(t.Context(), query, kind, body, key.kind, key.id).Scan(&linked); err != nil {
			t.Fatal(err)
		}
		if linked {
			t.Fatalf("orphan has an incoming backstop link to selected %s/%s", key.kind, key.id)
		}
	}
}

func appendTaskScopeAgent(t *testing.T, store *SQLite, organization string) {
	t.Helper()
	now := time.Now().UTC()
	blueprint := core.AgentBlueprint{ID: "scope-blueprint", OrganizationID: core.ID(organization), Version: "v1", Role: "worker", OperatingInstructions: "bounded work", Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "scope-profile", OrganizationID: core.ID(organization), Version: "v1", ConnectionID: "scope-account", ModelProvider: "provider", Model: "model", PromptVersion: "v1", Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "scope-agent", OrganizationID: core.ID(organization), BlueprintID: blueprint.ID, BlueprintVersion: "v1", ExecutionProfileID: profile.ID, ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
	config := core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: "v1", ProfileID: profile.ID, ProfileVersion: "v1", RuntimeAdapter: "local"}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: organization, EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated-roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: organization, EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated-roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: organization, EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "unrelated-roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
	} {
		if _, err := store.AppendProjection(t.Context(), draft); err != nil {
			t.Fatal(err)
		}
	}
	routing := modelinput.RouteRequirements{OrganizationID: organization, Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 1, OutputTokens: 1, Locality: modelinput.CloudAllowed, DataClass: "internal"}
	fingerprint, err := routing.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	decision := modelinput.RouteDecision{Version: 1, RequirementsFingerprint: fingerprint, PolicyFingerprint: strings.Repeat("0", 64), SelectedAt: now, SnapshotSequence: 1, ConnectionID: profile.ConnectionID, Provider: profile.ModelProvider, Model: profile.Model, ExecutionProfileVersion: profile.Version, Reason: modelinput.RouteOrdered, ReservedInputTokens: 1, ReservedOutputTokens: 1}
	task := core.Task{ID: "template-task", WorkID: "unrelated-work", Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending, Routing: &routing, RoutingDecision: &decision}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: organization, EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "unrelated"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task}); err != nil {
		t.Fatal(err)
	}
}
