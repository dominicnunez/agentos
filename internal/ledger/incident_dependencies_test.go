package ledger

import (
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentDependencyRoster(t *testing.T) {
	for _, mutation := range []string{"valid", "untrusted-note", "missing-profile-record", "orphan-profile-revision"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "dependencies.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "incident-work", "work-1")
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", false)
			appendPendingAgentExecutionTask(t, t.Context(), store, "incident-work", "task-1", agent, config)
			// A same-tenant sibling roster is not a dependency of this Task.
			appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "unrelated", false)
			switch mutation {
			case "untrusted-note":
				_, err = store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "incident-work", TaskID: "unrelated-task", EventType: "AUDIT_NOTE", Payload: map[string]any{"agent_id": "agent-unrelated", "goal_id": "missing-goal", "parent_id": "missing-task", "nested": map[string]string{"execution_profile_id": "profile-unrelated"}}})
				if err == nil {
					_, err = store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='execution_profile' AND record_id='profile-unrelated'`)
				}
			case "missing-profile-record":
				_, err = store.db.ExecContext(t.Context(), `DELETE FROM records WHERE kind='execution_profile' AND record_id=?`, string(config.ProfileID))
			case "orphan-profile-revision":
				_, err = store.db.ExecContext(t.Context(), `INSERT INTO records SELECT kind,record_id,version+1,body,'','',created_at FROM records WHERE kind='execution_profile' AND record_id=?`, string(config.ProfileID))
			}
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
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-work", 256)
			if mutation != "valid" && mutation != "untrusted-note" {
				if err == nil {
					t.Fatal("malformed dependency backing accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			kinds := map[string]int{}
			for _, event := range snapshot.DependencyEvents {
				payload, present, err := events.AdmittedProjection(event)
				if err != nil {
					t.Fatal(err)
				}
				if present {
					kinds[payload.Projection.ProjectionKind]++
					if payload.Projection.RecordID == "agent-unrelated" || payload.Projection.RecordID == "profile-unrelated" || payload.Projection.RecordID == "blueprint-unrelated" {
						t.Fatal("unrelated roster entered dependency closure")
					}
				}
			}
			for _, kind := range []string{"agent", "agent_blueprint", "execution_profile"} {
				if kinds[kind] != 1 {
					t.Fatalf("dependency %s count=%d", kind, kinds[kind])
				}
			}
			t.Logf("public=%d dependency=%d authority=%d", len(snapshot.Work.Events), len(snapshot.DependencyEvents), len(snapshot.AuthorityRecords))
		})
	}
}
