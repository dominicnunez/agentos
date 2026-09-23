package ledger

import (
	"database/sql"
	"testing"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEvidenceActorBinding(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	task := incidentTestExecution(t, store)
	evidence, err := store.AppendAgentEvidence(t.Context(), events.TrustedDraft{
		OrganizationID: "org-1", EventType: "EVIDENCE_PUBLISHED", SourceActorID: string(task.AssigneeID),
		SourceExecutionID: "execution-stop-task-v2", TaskID: string(task.ID), CorrelationID: "stop-work", ArtifactRefs: []string{"artifact-1"},
		Payload: events.EvidencePublishedPayload{Summary: "bounded evidence", ArtifactRefs: []string{"artifact-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.Work.Events {
		if snapshot.Work.Events[i].EventID == evidence.EventID {
			snapshot.Work.Events[i].SourceActorID = "wrong-agent"
		}
	}
	if _, err := events.ValidateIncidentHistory(snapshot); err == nil {
		t.Error("direct incident validation accepted evidence from the wrong actor")
	}
	err = store.withTx(t.Context(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET source_actor_id='wrong-agent' WHERE event_id=?`, evidence.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256); err == nil {
		t.Fatal("incident reader accepted evidence from the wrong actor")
	}
}
