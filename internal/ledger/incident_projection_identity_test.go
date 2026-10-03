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
)

func TestIncidentIndependentProjectionValueID(t *testing.T) {
	parallelIncidentTest(t)
	for _, kind := range []string{"team", "knowledge"} {
		for _, source := range []string{"event", "record", "both"} {
			t.Run(kind+"/"+source, func(t *testing.T) {
				store := projectionScopeFixture(t)
				selectedID, selectedCorrelation, otherID, label := "team-2", "foreign-team", "other-team", "TEAM_CREATED"
				var value any = core.Team{ID: core.ID(otherID), OrganizationID: "org-2", Name: "Independent", Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				if kind == "knowledge" {
					selectedID, selectedCorrelation, otherID, label = "selected-knowledge", "knowledge-selected-knowledge", "other-knowledge", "KNOWLEDGE_PROPOSED"
					var parent string
					if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='organization' AND record_id='org-2'`).Scan(&parent); err != nil {
						t.Fatal(err)
					}
					knowledge := core.KnowledgeRecord{KnowledgeID: core.ID(selectedID), OrganizationID: "org-2", Version: 1, Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeOrganization, ScopeID: "org-2", Status: core.KnowledgeCandidate, Title: "Observed lesson", Content: "Preserve evidence.", Basis: core.KnowledgeBasisHumanInput, ProvenanceEventRefs: []string{parent}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated}
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: label, SourceActorID: "runtime", CorrelationID: selectedCorrelation}, ProjectionKind: kind, RecordID: selectedID, Version: 1, Value: knowledge}); err != nil {
						t.Fatal(err)
					}
					knowledge.KnowledgeID = core.ID(otherID)
					provenance, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "independent-provenance", Payload: map[string]string{"text": "Independent observation."}})
					if err != nil {
						t.Fatal(err)
					}
					knowledge.ProvenanceEventRefs = []string{provenance.EventID}
					knowledge.CreatedAt = time.Now().UTC()
					value = knowledge
				}
				otherCorrelation := "independent-other"
				if kind == "knowledge" {
					otherCorrelation = "knowledge-" + otherID
				}
				incoming, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: label, SourceActorID: "runtime", CorrelationID: otherCorrelation}, ProjectionKind: kind, RecordID: otherID, Version: 1, Value: value})
				if err != nil {
					t.Fatal(err)
				}
				full, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(full, nil, nil, nil); err != nil {
					t.Fatalf("healthy writer history: %v", err)
				}
				before, err := store.VerifiedIncidentEvents(t.Context(), "org-2", selectedCorrelation, 256)
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range before.DependencyEvents {
					if event.EventID == incoming.EventID {
						t.Fatal("healthy unrelated projection expanded dependencies")
					}
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					event, _, err := eventByID(t.Context(), tx, incoming.EventID)
					if err != nil {
						return err
					}
					projection, _, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					var body map[string]any
					if err := json.Unmarshal(projection.Projection.Value, &body); err != nil {
						return err
					}
					field := "id"
					if kind == "knowledge" {
						field = "knowledge_id"
					}
					body[field] = selectedID
					projection.Projection.Value, err = json.Marshal(body)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(event, projection.Projection, projection.Detail)
					if err != nil {
						return err
					}
					eventBody, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					recordBody, err := json.Marshal(projection.Projection)
					if err != nil {
						return err
					}
					if source != "record" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, eventBody, event.EventID); err != nil {
							return err
						}
					}
					if source != "event" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, event.EventID); err != nil {
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
				if source == "record" {
					err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind=?`, kind)
						return err
					})
					if err == nil {
						t.Fatal("record owner accepted mismatched value ID")
					}
				} else {
					full, err = store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := events.ValidateProjectionHistory(full, nil, nil, nil); err == nil {
						t.Fatal("full owner accepted mismatched value ID")
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", selectedCorrelation, 256)
				if err == nil {
					t.Fatal("incident omitted independent selected value ID")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("returned partial evidence")
				}
			})
		}
	}
}

func TestProjectionIdentityOwnedPaths(t *testing.T) {
	parallelIncidentTest(t)
	for _, kind := range []string{"organization", "mission", "goal", "team", "agent_blueprint", "execution_profile", "agent", "intent", "work", "task", "lab_experiment", "lab_promotion_candidate", "knowledge"} {
		for _, source := range []string{"event", "record"} {
			t.Run(kind+"/"+source, func(t *testing.T) {
				store := projectionScopeFixture(t)
				field := "id"
				if kind == "knowledge" {
					field = "knowledge_id"
				}
				body := `{"projection_kind":"` + kind + `","record_id":"unrelated-id","value":{"` + field + `":"selected-id"}}`
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if source == "event" {
						_, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=? WHERE event_type='TEAM_CREATED'`, []byte(`{"projection":`+body+`}`))
						return err
					}
					_, err := tx.ExecContext(t.Context(), `UPDATE records SET kind=?,body=? WHERE kind='team'`, kind, []byte(body))
					return err
				}); err != nil {
					t.Fatal(err)
				}
				assertIdentityConflict(t, store, incidentKey{kind, "selected-id"}, true)
				assertIdentityConflict(t, store, incidentKey{kind, "other-unrelated-id"}, false)
			})
		}
	}
}

func assertIdentityConflict(t *testing.T, store *SQLite, key incidentKey, want bool) {
	t.Helper()
	err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		return validateIncidentProjectionIDs(t.Context(), tx, map[incidentKey]bool{key: true})
	})
	if (err != nil) != want {
		t.Fatalf("identity conflict: %v, want rejection=%v", err, want)
	}
	if err != nil && !strings.Contains(err.Error(), "conflicting selected identity") {
		t.Fatalf("unexpected rejection: %v", err)
	}
}

func TestProjectionIdentityOccurrenceClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, source := range []string{"event", "record"} {
		for _, sample := range []struct{ name, body string }{
			{"duplicate-own-id", `{"projection_kind":"team","record_id":"other-id","value":{"id":"other-id","id":"selected-id"}}`},
			{"escaped-own-id", `{"projection_kind":"team","record_id":"other-id","value":{"i\u0064":"selected-id"}}`},
			{"duplicate-value", `{"projection_kind":"team","record_id":"other-id","value":{"id":"other-id"},"value":{"id":"selected-id"}}`},
			{"duplicate-key", `{"projection_kind":"team","record_id":"other-id","record_id":"selected-id","value":{"id":"other-id"}}`},
			{"missing-key", `{"projection_kind":"team","value":{"id":"selected-id"}}`},
			{"nontext-key", `{"projection_kind":"team","record_id":[],"value":{"id":"selected-id"}}`},
			{"missing-value-id", `{"projection_kind":"team","record_id":"selected-id","value":{}}`},
			{"nontext-value-id", `{"projection_kind":"team","record_id":"selected-id","value":{"id":[]}}`},
			{"escaped-value", `{"projection_kind":"team","record_id":"other-id","val\u0075e":{"id":"selected-id"}}`},
			{"duplicate-kind", `{"projection_kind":"work","projection_kind":"team","record_id":"other-id","value":{"id":"selected-id"}}`},
			{"escaped-kind", `{"projection_kin\u0064":"team","record_id":"other-id","value":{"id":"selected-id"}}`},
		} {
			t.Run(source+"/"+sample.name, func(t *testing.T) {
				store := projectionScopeFixture(t)
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if source == "event" {
						_, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='TEAM_CREATED'`, []byte(`{"projection":`+sample.body+`}`))
						return err
					}
					_, err := tx.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='team'`, []byte(sample.body))
					return err
				}); err != nil {
					t.Fatal(err)
				}
				assertIdentityConflict(t, store, incidentKey{"team", "selected-id"}, true)
			})
		}
	}
	for _, container := range []string{"duplicate", "escaped"} {
		t.Run("event-projection/"+container, func(t *testing.T) {
			store := projectionScopeFixture(t)
			body := `{"projection":{"projection_kind":"team","record_id":"other-id","value":{"id":"other-id"}},"projection":{"projection_kind":"team","record_id":"other-id","value":{"id":"selected-id"}}}`
			if container == "escaped" {
				body = `{"pr\u006fjection":{"projection_kind":"team","record_id":"other-id","value":{"id":"selected-id"}}}`
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='TEAM_CREATED'`, []byte(body)); err != nil {
				t.Fatal(err)
			}
			assertIdentityConflict(t, store, incidentKey{"team", "selected-id"}, true)
		})
	}
}

func TestProjectionIdentityAdmissionClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, mode := range []string{"plain", "duplicate-leaf", "duplicate-container", "escaped-container", "escaped-leaf", "missing-projection", "opaque-nested"} {
		t.Run(mode, func(t *testing.T) {
			store := projectionScopeFixture(t)
			var selected string
			if err := store.db.QueryRowContext(t.Context(), `SELECT admission_event_id FROM records WHERE kind='organization' AND record_id='org-1'`).Scan(&selected); err != nil {
				t.Fatal(err)
			}
			claim, err := json.Marshal(selected)
			if err != nil {
				t.Fatal(err)
			}
			admission := `"admission":{"event_ref":` + string(claim) + `}`
			switch mode {
			case "duplicate-leaf":
				admission = `"admission":{"event_ref":"other-event","event_ref":` + string(claim) + `}`
			case "duplicate-container":
				admission = `"admission":{"event_ref":"other-event"},` + admission
			case "escaped-container":
				admission = `"admissio\u006e":{"event_ref":` + string(claim) + `}`
			case "escaped-leaf":
				admission = `"admission":{"event_re\u0066":` + string(claim) + `}`
			case "opaque-nested":
				admission = `"text":{` + admission + `}`
			}
			projection := `"projection":{"projection_kind":"team","record_id":"team-2","value":{"id":"team-2","organization_id":"org-2"}},`
			if mode == "missing-projection" || mode == "opaque-nested" {
				projection = ""
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='TEAM_CREATED'`, []byte(`{`+projection+admission+`}`)); err != nil {
				t.Fatal(err)
			}
			assertIdentityConflict(t, store, incidentKey{"event", selected}, mode != "opaque-nested")
		})
	}
}

func TestProjectionIdentityLeaseAndOpaqueClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, mode := range []string{"lease", "duplicate-lease", "escaped-lease", "unrelated-lease", "opaque-record"} {
		t.Run(mode, func(t *testing.T) {
			store := projectionScopeFixture(t)
			if mode == "opaque-record" {
				if err := store.AppendRecord(t.Context(), "org-2", "AUDIT_NOTE", "runtime", "", nil, nil, "authorization_trace", "opaque-identity", 1, json.RawMessage(`{"projection_kind":"mission","record_id":"selected-id","value":{"id":"selected-id"}}`)); err != nil {
					t.Fatal(err)
				}
				assertIdentityConflict(t, store, incidentKey{"mission", "selected-id"}, false)
				return
			}
			body := `{"id":"selected-id"}`
			switch mode {
			case "duplicate-lease":
				body = `{"id":"other-id","id":"selected-id"}`
			case "escaped-lease":
				body = `{"i\u0064":"selected-id"}`
			case "unrelated-lease":
				body = `{"id":"other-id"}`
			}
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES('capability_lease','displaced-key',1,?,?)`, []byte(body), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			assertIdentityConflict(t, store, incidentKey{"capability_lease", "selected-id"}, mode != "unrelated-lease")
		})
	}
}
