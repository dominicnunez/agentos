package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingKnowledgeEvidence(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{"provenance_event_refs", "occurrence_event_refs"} {
		for _, side := range []string{"both", "event", "record", "same-org", "unrelated", "opaque-note", "missing-kind", "nontext-kind", "wrong-kind"} {
			t.Run(field+"/"+side, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "knowledge-evidence.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				for _, id := range []string{"org-1", "org-2"} {
					_, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: id, EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-" + id}, ProjectionKind: "organization", RecordID: id, Version: 1, Value: core.Organization{ID: core.ID(id), Name: id, PolicyVersion: "v1", CreatedAt: time.Now().UTC()}})
					if err != nil {
						t.Fatal(err)
					}
				}
				selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"summary": "Selected observation"}})
				if err != nil {
					t.Fatal(err)
				}
				org := "org-2"
				if side == "same-org" {
					org = "org-1"
				}
				independent, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: org, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "independent", Payload: map[string]string{"summary": "Independent observation"}})
				if err != nil {
					t.Fatal(err)
				}
				value := core.KnowledgeRecord{KnowledgeID: "incoming", OrganizationID: core.ID(org), Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: core.ID(org), Status: core.KnowledgeCandidate, Title: "Observation", Content: "Recorded observation", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{independent.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
				if side == "same-org" {
					if field == "provenance_event_refs" {
						value.ProvenanceEventRefs = []string{selected.EventID}
					} else {
						value.OccurrenceEventRefs = []string{selected.EventID}
					}
				}
				incoming, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-incoming"}, ProjectionKind: "knowledge", RecordID: "incoming", Version: 1, Value: value})
				if err != nil {
					t.Fatal(err)
				}
				if side == "opaque-note" {
					if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: org, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "opaque", Payload: map[string]any{field: []string{selected.EventID}}}); err != nil {
						t.Fatal(err)
					}
				}
				if side != "same-org" {
					if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
						t.Fatalf("valid baseline: %v", err)
					}
				}
				missingKind := strings.HasSuffix(side, "-kind")
				invalid := side == "both" || side == "event" || side == "record" || missingKind
				if invalid {
					changeSide := side
					if missingKind {
						changeSide = "event"
					}
					changeKnowledgeEvidence(t, store, incoming.EventID, field, selected.EventID, changeSide)
					if missingKind {
						changeIncidentSourceKind(t, store, incoming.EventID, side, true)
					}
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				if side == "both" || !invalid || missingKind {
					stream, err := store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
					if missingKind && fullErr == nil {
						t.Fatal("full replay accepted malformed runtime Knowledge source")
					}
					if side == "both" && (fullErr == nil || !strings.Contains(fullErr.Error(), "cross-organization evidence")) {
						t.Fatalf("full replay did not reject evidence ownership: %v", fullErr)
					}
					if !invalid && fullErr != nil {
						t.Fatalf("valid full replay: %v", fullErr)
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if invalid {
					if err == nil {
						t.Fatal("incident omitted incoming cross-organization Knowledge evidence")
					}
					if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatal("failed incident returned partial evidence")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, event := range snapshot.DependencyEvents {
					found = found || event.EventID == incoming.EventID
				}
				if found != (side == "same-org") {
					t.Fatalf("incoming evidence selection = %v", found)
				}
			})
		}
	}
}

func TestIncidentIncomingValidationRefs(t *testing.T) {
	parallelIncidentTest(t)
	for _, side := range []string{"both", "event", "record", "same-org", "unrelated"} {
		t.Run(side, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "validation-evidence.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-2"}, ProjectionKind: "organization", RecordID: "org-2", Version: 1, Value: core.Organization{ID: "org-2", Name: "Other", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
				t.Fatal(err)
			}
			selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"summary": "Independent evidence"}})
			if err != nil {
				t.Fatal(err)
			}
			correlation := appendDerivedIncidentChain(t, store, 1)
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
			if err != nil {
				t.Fatalf("valid Knowledge activation: %v", err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			var incoming string
			organization, selectedCorrelation := "org-2", "selected"
			for _, event := range stream {
				if event.EventType == "KNOWLEDGE_ACTIVATED" {
					incoming = event.EventID
				}
				if side == "same-org" && event.EventType == "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED" {
					organization, selectedCorrelation = event.OrganizationID, event.CorrelationID
				}
			}
			if incoming == "" {
				t.Fatal("missing admitted activation")
			}
			invalid := side == "both" || side == "event" || side == "record"
			if invalid {
				changeKnowledgeEvidence(t, store, incoming, "validation_refs", selected.EventID, side)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if side == "both" || !invalid {
				stream, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				leases, freezes, err := events.ResolveAuthorityAdmissions(stream, baseline.AuthorityRecords)
				if err != nil {
					t.Fatal(err)
				}
				_, fullErr := events.ValidateProjectionHistory(stream, nil, leases, freezes)
				if side == "both" && (fullErr == nil || !strings.Contains(fullErr.Error(), "cross-organization evidence")) {
					t.Fatalf("full replay did not reject validation evidence ownership: %v", fullErr)
				}
				if !invalid && fullErr != nil {
					t.Fatalf("valid full activation replay: %v", fullErr)
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), organization, selectedCorrelation, 256)
			if invalid {
				if err == nil {
					t.Fatal("incident omitted incoming validation reference")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("failed incident returned partial evidence")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, event := range snapshot.DependencyEvents {
				found = found || event.EventID == incoming
			}
			if found != (side == "same-org") {
				t.Fatalf("incoming validation selection = %v", found)
			}
		})
	}
}

func changeKnowledgeEvidence(t *testing.T, store *SQLite, id, field, target, side string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return fmt.Errorf("read Knowledge event: %w", err)
		}
		if !found {
			return fmt.Errorf("Knowledge event is missing")
		}
		payload, _, err := events.AdmittedProjection(event)
		if err != nil {
			return err
		}
		var value map[string]json.RawMessage
		if err := json.Unmarshal(payload.Projection.Value, &value); err != nil {
			return err
		}
		value[field], err = json.Marshal([]string{target})
		if err != nil {
			return err
		}
		payload.Projection.Value, err = json.Marshal(value)
		if err != nil {
			return err
		}
		sealed, err := events.SealProjectionEvent(event, payload.Projection, payload.Detail)
		if err != nil {
			return err
		}
		body, err := json.Marshal(sealed)
		if err != nil {
			return err
		}
		if side != "record" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, id); err != nil {
				return err
			}
		}
		body, err = json.Marshal(payload.Projection)
		if err != nil {
			return err
		}
		if side != "event" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, id); err != nil {
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
