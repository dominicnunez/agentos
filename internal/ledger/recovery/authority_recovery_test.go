package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	_ "modernc.org/sqlite"
)

func recoveryTestManifest(task core.Task, selection events.ExecutionStartSelection) core.ExecutionContextManifest {
	refs := make([]string, 0)
	for _, inbox := range selection.Inbox {
		for _, event := range inbox.Events {
			refs = append(refs, event.EventID)
		}
	}
	coordinationRefs := make([]core.VersionedRef, 0, len(selection.Coordination))
	for _, peer := range selection.Coordination {
		coordinationRefs = append(coordinationRefs, core.VersionedRef{ID: string(peer.Task.ID), Version: fmt.Sprintf("%d", peer.Version), MaterializationState: core.MaterializedFull})
	}
	return core.ExecutionContextManifest{
		ExecutionID: "execution-" + task.ID + "-v2", AgentID: task.AssigneeID,
		AgentBlueprintVersion: task.AgentConfig.BlueprintVersion, ExecutionProfileVersion: task.AgentConfig.ProfileVersion,
		RuntimeAdapter: task.AgentConfig.RuntimeAdapter, Provider: "test", Model: "test", TaskID: task.ID,
		TaskContractVersion: task.TaskContractVersion, PromptVersion: "test", PolicyVersion: "v1", EventRefs: refs, CoordinationRefs: coordinationRefs,
		ContextBuilderVersion: "v3", ExecutionInputSHA256: core.FingerprintExecutionInput("test"), CreatedAt: selection.Started.CreatedAt,
	}
}

func TestVerifyRejectsAgentEvidenceDetachedFromItsExecution(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent-evidence.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now}
	intent := core.Intent{ID: "intent-1", OrganizationID: organization.ID, OriginalInstruction: "publish evidence", NormalizedObjective: "publish evidence", CreatedAt: now}
	work := core.Work{ID: "work-1", IntentID: intent.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
	blueprint := core.AgentBlueprint{ID: "blueprint-1", OrganizationID: organization.ID, Version: "v1", Role: "worker", OperatingInstructions: "bounded work", RequiredCapabilityClasses: []string{}, Status: "ACTIVE", CreatedAt: now}
	profile := core.ExecutionProfile{ID: "profile-1", OrganizationID: organization.ID, Version: "v1", ModelProvider: "provider", Model: "model", PromptVersion: "v1", ToolRefs: []string{}, Status: "ACTIVE", CreatedAt: now}
	agent := core.Agent{ID: "agent-1", OrganizationID: organization.ID, BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ExecutionProfileID: profile.ID, ExecutionProfileVersion: profile.Version, RuntimeAdapter: "local", Status: "ACTIVE"}
	config := core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: blueprint.Version, ProfileID: profile.ID, ProfileVersion: profile.Version, RuntimeAdapter: agent.RuntimeAdapter}
	task := core.Task{ID: "task-1", WorkID: work.ID, Description: "publish evidence", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config, TaskContractVersion: "1", Status: core.TaskPending}
	for _, draft := range []events.ProjectionDraft{
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: string(organization.ID), Version: 1, Value: organization},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "work-1"}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
		{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
	} {
		if _, err := store.AppendProjection(ctx, draft); err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
	}
	task.Status = core.TaskRunning
	if _, _, err := store.AppendExecutionStart(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "EXECUTION_STARTED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: "work-1"},
		ProjectionKind: "task", RecordID: string(task.ID), Version: 2, Value: task,
	}, []events.InboxRoute{{Scope: events.RecipientTask, ID: string(task.ID)}, {Scope: events.RecipientAgent, ID: string(agent.ID)}}, func(selection events.ExecutionStartSelection) (core.ExecutionContextManifest, error) {
		return recoveryTestManifest(task, selection), nil
	}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	evidence, err := store.AppendAgentEvidence(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "EVIDENCE_PUBLISHED", SourceActorID: string(agent.ID), SourceExecutionID: "execution-task-1-v2",
		TaskID: string(task.ID), ArtifactRefs: []string{"artifact-1"}, CorrelationID: "work-1",
		Payload: events.EvidencePublishedPayload{Summary: "bounded evidence", ArtifactRefs: []string{"artifact-1"}},
	})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid Agent evidence failed recovery: %v", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET task_id='task-other' WHERE event_id=?; DROP TABLE legacy_knowledge_quarantine; DROP TABLE event_integrity; DROP INDEX records_knowledge_organization_idx`, evidence.EventID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	fingerprint, err := testStorageSchemaFingerprint(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE agentos_storage SET storage_version=5,schema_fingerprint=?; PRAGMA user_version=5`, fingerprint); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "Agent evidence") {
		t.Fatalf("execution-detached Agent evidence passed recovery: %v", err)
	}
}

func TestVerifyRejectsAuthorityEventWithoutItsExactRecord(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orphaned-authority.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "actor-1", ActorKind: core.PrincipalAgent, OriginTaskID: "task-1", Action: "write", Resource: "record-1", Scope: "org-1"}
	if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "user-1", "task-1", nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err != nil {
		t.Fatalf("valid authority history failed recovery: %v", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM records WHERE kind='capability_lease' AND record_id='lease-1'`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "authority admission event") {
		t.Fatalf("orphaned authority event passed recovery: %v", err)
	}
}

func TestVerifyRejectsMalformedAuthorityRevisions(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		mutate func(*sql.DB) error
	}{
		{name: "noncontiguous lease", mutate: func(db *sql.DB) error {
			_, err := db.ExecContext(ctx, `UPDATE records SET version=3 WHERE kind='capability_lease' AND record_id='lease-1' AND version=2`)
			return err
		}},
		{name: "changed revocation", mutate: func(db *sql.DB) error {
			var body []byte
			if err := db.QueryRowContext(ctx, `SELECT body FROM records WHERE kind='capability_lease' AND record_id='lease-1' AND version=2`).Scan(&body); err != nil {
				return err
			}
			var lease core.CapabilityLease
			if err := json.Unmarshal(body, &lease); err != nil {
				return err
			}
			lease.Resource = "expanded-resource"
			body, err := json.Marshal(lease)
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, `UPDATE records SET body=? WHERE kind='capability_lease' AND record_id='lease-1' AND version=2`, body)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "authority.db")
			store, err := ledger.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			lease := core.CapabilityLease{ID: "lease-1", ActorID: "actor-1", ActorKind: core.PrincipalAgent, OriginTaskID: "task-1", Action: "write", Resource: "record-1", Scope: "org-1"}
			if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_GRANTED", "user-1", "task-1", nil, nil, "capability_lease", string(lease.ID), 1, lease); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			revokedAt := time.Now().UTC()
			lease.RevokedAt = &revokedAt
			if err := store.AppendRecord(ctx, "org-1", "CAPABILITY_REVOKED", "user-1", "task-1", nil, nil, "capability_lease", string(lease.ID), 2, lease); err != nil {
				_ = store.Close()
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Verify(ctx, path); err != nil {
				t.Fatalf("valid authority history failed recovery: %v", err)
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if err := test.mutate(db); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "authority record") {
				t.Fatalf("malformed authority history passed recovery: %v", err)
			}
		})
	}
}

func TestVerifyRejectsOrganizationMismatchedFreezeRecord(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "freeze.db")
	store, err := ledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	state := map[string]any{"organization_id": "org-1", "frozen": true, "reason": "incident", "updated_at": time.Now().UTC()}
	if err := store.AppendRecord(ctx, "org-1", "FREEZE_SET", "user-1", "task-1", nil, nil, "organization_freeze", "org-1", 1, state); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	state["organization_id"] = "org-2"
	body, err := json.Marshal(state)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE records SET body=? WHERE kind='organization_freeze' AND record_id='org-1'`, body); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, path); err == nil || !strings.Contains(err.Error(), "authority record") {
		t.Fatalf("organization-mismatched freeze passed recovery: %v", err)
	}
}
