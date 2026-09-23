package ledger

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentClosureDiamond(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "diamond.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	want := appendIncidentDiamond(t, store)
	var previous events.IncidentSnapshot
	for read := range 2 {
		snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-diamond-tip", 256)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, stream := range [][]events.Event{snapshot.Work.Events, snapshot.RelatedEvents, snapshot.DependencyEvents} {
			for _, event := range stream {
				if seen[event.EventID] {
					t.Fatalf("shared ancestor selected twice: %s", event.EventID)
				}
				seen[event.EventID] = true
			}
		}
		if !reflect.DeepEqual(seen, want) {
			t.Fatalf("closure IDs = %v, want %v", seen, want)
		}
		if read > 0 && !reflect.DeepEqual(snapshot, previous) {
			t.Fatal("repeated read changed the selected evidence")
		}
		previous = snapshot
	}
}

func TestIncidentClosureBrokenMiddle(t *testing.T) {
	for _, mutation := range []string{"missing-record", "moved-record", "missing-event", "moved-event"} {
		t.Run(mutation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "broken.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendIncidentDiamond(t, store)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-diamond-tip", 256); err != nil {
				t.Fatalf("valid fixture: %v", err)
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				var query string
				switch mutation {
				case "missing-record":
					query = `DELETE FROM records WHERE kind='knowledge' AND record_id='diamond-shared'`
				case "moved-record":
					query = `UPDATE records SET record_id='moved-shared' WHERE kind='knowledge' AND record_id='diamond-shared'`
				case "missing-event":
					query = `UPDATE events SET event_id='absent-original-event' WHERE correlation_id='knowledge-diamond-shared'`
				case "moved-event":
					query = `UPDATE events SET organization_id='other-org' WHERE correlation_id='knowledge-diamond-shared'`
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
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "knowledge-diamond-tip", 256)
			if err == nil {
				t.Fatal("broken middle ancestor was accepted")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed closure returned partial evidence")
			}
		})
	}
}

func appendIncidentDiamond(t *testing.T, store *SQLite) map[string]bool {
	t.Helper()
	appendIncidentLineage(t, store, 0)
	want := map[string]bool{}
	var root string
	if err := store.db.QueryRowContext(t.Context(), `SELECT event_id FROM events WHERE correlation_id='setup'`).Scan(&root); err != nil {
		t.Fatal(err)
	}
	want[root] = true
	appendNode := func(id string, refs ...string) string {
		event, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
			Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-" + id},
			ProjectionKind: "knowledge", RecordID: id, Version: 1,
			Value: core.KnowledgeRecord{KnowledgeID: core.ID(id), OrganizationID: "org-1", Version: 1,
				Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate,
				Title: "Shared evidence", Content: `{"knowledge_id":"missing-opaque-knowledge","event_ref":"missing-opaque-event"}`,
				Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: refs, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime,
				CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated},
		})
		if err != nil {
			t.Fatal(err)
		}
		want[event.EventID] = true
		return event.EventID
	}
	shared := appendNode("diamond-shared", root)
	left := appendNode("diamond-left", shared)
	right := appendNode("diamond-right", shared)
	appendNode("diamond-tip", left, right, shared)
	return want
}
