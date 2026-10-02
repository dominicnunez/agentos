package ledger

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

// A later duplicate can claim selected evidence even when both source channels
// keep an unrelated first value. Selection must reach exact admission checking.
func TestIncidentDuplicateReferences(t *testing.T) {
	parallelIncidentTest(t)
	for _, field := range []string{"provenance_event_refs", "occurrence_event_refs", "value", "projection"} {
		for _, side := range []string{"both", "event", "record"} {
			if field == "projection" && side == "record" {
				continue
			}
			t.Run(field+"/"+side, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				for _, org := range []string{"selected-org", "foreign-org"} {
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"}, ProjectionKind: "organization", RecordID: org, Version: 1, Value: core.Organization{ID: core.ID(org), Name: org, PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
						t.Fatal(err)
					}
				}
				selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "selected-org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"note": "Selected evidence"}})
				if err != nil {
					t.Fatal(err)
				}
				independent, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "foreign-org", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "independent", Payload: map[string]string{"note": "Independent evidence"}})
				if err != nil {
					t.Fatal(err)
				}
				value := core.KnowledgeRecord{KnowledgeID: "duplicate-knowledge", OrganizationID: "foreign-org", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "foreign-org", Status: core.KnowledgeCandidate, Title: "Observation", Content: "Independent observation", Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{independent.EventID}, OccurrenceEventRefs: []string{independent.EventID}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
				incoming, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "foreign-org", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-duplicate-knowledge"}, ProjectionKind: "knowledge", RecordID: "duplicate-knowledge", Version: 1, Value: value})
				if err != nil {
					t.Fatal(err)
				}
				before, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(before, nil, nil, nil); err != nil {
					t.Fatalf("writer-produced full baseline: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256); err != nil {
					t.Fatalf("valid baseline: %v", err)
				}
				ref, err := json.Marshal([]string{selected.EventID})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if side != "record" {
						body := duplicateKnowledgeRef(t, incoming.Payload, true, field, ref)
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, incoming.EventID); err != nil {
							return err
						}
					}
					if side != "event" {
						var body []byte
						if err := tx.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='knowledge' AND record_id='duplicate-knowledge'`).Scan(&body); err != nil {
							return err
						}
						body = duplicateKnowledgeRef(t, body, false, field, ref)
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='knowledge' AND record_id='duplicate-knowledge'`, body); err != nil {
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
				if side != "record" {
					stream, err := store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					_, fullErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
					if fullErr == nil {
						t.Fatalf("full owner did not reject duplicate fields: %v", fullErr)
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected-org", "selected", 256)
				if err == nil {
					t.Fatal("incident omitted later duplicate reference to selected evidence")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("duplicate claim returned partial evidence")
				}
			})
		}
	}
}

func duplicateKnowledgeRef(t *testing.T, body []byte, event bool, field string, ref []byte) []byte {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	projection := root
	if event {
		projection = nil
		if err := json.Unmarshal(root["projection"], &projection); err != nil {
			t.Fatal(err)
		}
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(projection["value"], &value); err != nil {
		t.Fatal(err)
	}
	if field == "projection" || field == "value" {
		value["provenance_event_refs"] = ref
	} else {
		value[field] = ref
	}
	second, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if field != "projection" && field != "value" {
		projection["value"] = duplicateJSONMember(t, projection["value"], field, ref)
	} else if field == "value" {
		if event {
			root["projection"] = duplicateJSONMember(t, root["projection"], "value", second)
			encoded, err := json.Marshal(root)
			if err != nil {
				t.Fatal(err)
			}
			return encoded
		}
		return duplicateJSONMember(t, body, "value", second)
	} else {
		projection["value"] = second
		encoded, err := json.Marshal(projection)
		if err != nil {
			t.Fatal(err)
		}
		if event {
			return duplicateJSONMember(t, body, "projection", encoded)
		}
		return duplicateJSONMember(t, body, "value", second)
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	if !event {
		return encoded
	}
	root["projection"] = encoded
	encoded, err = json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func duplicateJSONMember(t *testing.T, body []byte, name string, value []byte) []byte {
	t.Helper()
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[len(body)-1] != '}' {
		t.Fatal("duplicate fixture requires object")
	}
	result := append([]byte(nil), body[:len(body)-1]...)
	return append(result, []byte(fmt.Sprintf(",%q:%s}", name, value))...)
}
