package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentMovedIntakeAbandonment(t *testing.T) {
	for _, name := range []string{"unrelated", "foreign-message", "selected-message", "alternate-field"} {
		selected := name == "selected-message" || name == "alternate-field"
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "intake.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			appendIncidentReplacements(t, store, 2)
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "replacement-1", 256); err != nil {
				t.Fatal(err)
			}
			message := "unrelated-message"
			if name == "selected-message" || name == "foreign-message" {
				message = "source-replacement-1"
			}
			organization := "org-1"
			if name == "foreign-message" {
				organization = "other"
			}
			var payload any = events.IntakeAbandonedPayload{MessageID: message, SourcePrincipalID: "user-1", SourcePrincipalKind: "HUMAN", SourceChannel: "HUMAN_DIRECT"}
			if name == "alternate-field" {
				payload = map[string]string{"message_id": message, "source_message_id": "source-replacement-1", "source_principal_id": "user-1", "source_principal_kind": "HUMAN", "source_channel": "HUMAN_DIRECT"}
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, err := appendEvent(t.Context(), tx, events.TrustedDraft{OrganizationID: organization, EventType: "INTAKE_ABANDONED", SourceActorID: "user-1", CorrelationID: "moved-intake", TaskID: "task-moved-intake", Payload: payload})
				return err
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
			t.Cleanup(func() { _ = store.Close() })
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "intake abandonment") {
				t.Fatalf("full history accepted invalid abandonment: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "replacement-1", 256)
			if !selected {
				if err != nil {
					t.Fatalf("unrelated intake entered selected history: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "intake abandonment") {
				t.Fatalf("selected message lost moved abandonment: %v", err)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("returned partial incident")
			}
		})
	}
}

func TestIncidentIntakeMessageSelectors(t *testing.T) {
	for _, kind := range []string{"INTAKE_MESSAGE_RECORDED", "INTAKE_ABANDONED", "INTENT_DRAFTED", "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", "INTENT_CONFIRMED"} {
		t.Run(kind, func(t *testing.T) {
			// Both identity fields must be visible if malformed retained evidence has
			// them both; choosing one could hide the conflicting selected identity.
			selectors, err := incidentDocumentSelectors(events.Event{EventType: kind, OrganizationID: "org-1", CorrelationID: "intake", Payload: []byte(`{"message_id":"message","source_message_id":"source"}`)})
			if err != nil {
				t.Fatal(err)
			}
			found := map[string]bool{}
			for _, selector := range selectors {
				if selector.Kind == "intake_message" {
					found[selector.ID] = true
				}
			}
			if !found["message"] || !found["source"] {
				t.Fatalf("intake message identities omitted: %+v", selectors)
			}
		})
	}
	selectors, err := incidentDocumentSelectors(events.Event{EventType: "AUDIT_NOTE", OrganizationID: "org-1", Payload: []byte(`{"message_id":"message","source_message_id":"source"}`)})
	if err != nil || len(selectors) != 0 {
		t.Fatalf("opaque note introduced intake dependency: %+v, %v", selectors, err)
	}
}

func TestIncidentIntakeUnrelatedGrowth(t *testing.T) {
	for _, depth := range []int{8, 32} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendIncidentReplacements(t, store, depth)
			var baseline events.IncidentSnapshot
			for _, unrelated := range []int{0, 256} {
				for i := 0; i < unrelated; i++ {
					correlation := fmt.Sprintf("unrelated-intake-%d", i)
					if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "INTAKE_MESSAGE_RECORDED", SourceActorID: "user-1", CorrelationID: correlation, TaskID: "task-" + correlation, Payload: events.IntakeMessageRecordedPayload{MessageID: correlation, Text: "An unrelated request", SourcePrincipalID: "user-1", SourcePrincipalKind: "HUMAN", SourceChannel: "HUMAN_DIRECT"}}); err != nil {
						t.Fatal(err)
					}
				}
				started := time.Now()
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "replacement-0", 256)
				t.Logf("selected intake identities=%d unrelated=%d elapsed=%s", depth-1, unrelated, time.Since(started))
				if err != nil {
					t.Fatal(err)
				}
				if unrelated == 0 {
					baseline = snapshot
				} else if !reflect.DeepEqual(snapshot.Work.Events, baseline.Work.Events) || !reflect.DeepEqual(snapshot.DependencyEvents, baseline.DependencyEvents) || !reflect.DeepEqual(snapshot.RelatedEvents, baseline.RelatedEvents) {
					t.Fatal("unrelated intake changed selected evidence")
				}
			}
		})
	}
}
