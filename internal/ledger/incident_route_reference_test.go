package ledger

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingRouteOrigin(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	planScopeParents(t, store, "selected-org", "selected", "selected-work")
	planScopeParents(t, store, "foreign-org", "foreign", "foreign-work")
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var selected, origin events.Event
	for _, event := range stream {
		if event.EventType == "WORK_CREATED" {
			if event.OrganizationID == "selected-org" {
				selected = event
			} else {
				origin = event
			}
		}
	}
	if selected.EventID == "" || origin.EventID == "" {
		t.Fatal("missing writer-owned Work origins")
	}
	payload := events.InferenceRouteRejectedPayload{Version: 1, Purpose: "PLANNING", OriginEventRef: origin.EventID, Category: "NO_ELIGIBLE_ACCOUNT"}
	rejection, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: origin.OrganizationID, CorrelationID: origin.CorrelationID, SourceActorID: "runtime", EventType: "INFERENCE_ROUTE_REJECTED", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("valid full inference admission history: %v", err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("valid projection history: %v", err)
	}
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range baseline.DependencyEvents {
		if event.EventID == rejection.EventID {
			t.Fatal("unrelated routing diagnostic selected before reference changed")
		}
	}
	payload.OriginEventRef = selected.EventID
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, rejection.EventID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
		t.Fatal("full inference replay accepted cross-organization diagnostic origin")
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
	if err == nil {
		t.Fatal("incident omitted incoming routing diagnostic origin")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed incident returned partial evidence")
	}
}

func TestIncidentRouteValidation(t *testing.T) {
	for _, private := range []bool{false, true} {
		name := "public"
		if private {
			name = "private"
		}
		t.Run(name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			planScopeParents(t, store, "org-1", "origin", "origin-work")
			stream, err := store.Events(t.Context(), "origin")
			if err != nil {
				t.Fatal(err)
			}
			var origin events.Event
			for _, event := range stream {
				if event.EventType == "WORK_CREATED" {
					origin = event
				}
			}
			payload := events.InferenceRouteRejectedPayload{Version: 1, Purpose: "PLANNING", OriginEventRef: origin.EventID, Category: "NO_ELIGIBLE_ACCOUNT"}
			rejection, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "origin", SourceActorID: "runtime", EventType: "INFERENCE_ROUTE_REJECTED", Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			selected := "origin"
			if private {
				selected = "knowledge-route-observation"
				knowledge := core.KnowledgeRecord{KnowledgeID: "route-observation", OrganizationID: "org-1", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeCandidate, Title: "Routing observation", Content: "Observed a routing decision", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{rejection.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: selected}, ProjectionKind: "knowledge", RecordID: string(knowledge.KnowledgeID), Version: 1, Value: knowledge}); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("valid full inference history: %v", err)
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
			if err != nil {
				t.Fatalf("valid diagnostic incident: %v", err)
			}
			foundOrigin, foundRejection := false, false
			for _, part := range [][]events.Event{baseline.Work.Events, baseline.DependencyEvents} {
				for _, event := range part {
					foundOrigin = foundOrigin || event.EventID == origin.EventID
					foundRejection = foundRejection || event.EventID == rejection.EventID
				}
			}
			if !foundOrigin || !foundRejection {
				t.Fatal("incident omitted diagnostic or its exact origin")
			}
			if _, err := events.ValidateIncidentHistory(baseline); err != nil {
				t.Fatalf("valid direct snapshot: %v", err)
			}
			payload.Category = "UNKNOWN_CATEGORY"
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, part := range [][]events.Event{baseline.Work.Events, baseline.DependencyEvents} {
				for index := range part {
					if part[index].EventID == rejection.EventID {
						part[index].Payload = body
					}
				}
			}
			if _, err := events.ValidateIncidentHistory(baseline); err == nil {
				t.Fatal("direct snapshot accepted malformed routing diagnostic")
			}
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, rejection.EventID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatal("full replay accepted malformed routing diagnostic")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selected, 256)
			if err == nil || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("incident accepted malformed routing diagnostic or returned partial evidence")
			}
		})
	}
}
