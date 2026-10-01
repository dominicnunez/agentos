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
	"github.com/dominicnunez/agentos/internal/modelinput"
)

func projectionScopeFixture(t *testing.T) *SQLite {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "scope.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, organization := range []string{"org-1", "org-2"} {
		appendTaskProjectionParents(t, t.Context(), store, organization, "incident-"+organization, "work-"+organization)
	}
	team := core.Team{ID: "team-2", OrganizationID: "org-2", Name: "Team", Status: "ACTIVE", CreatedAt: time.Now().UTC()}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-2", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "foreign-team"},
		ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team,
	}); err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
		t.Fatalf("writer fixture: %v", err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-org-1", 256); err != nil {
		t.Fatalf("healthy foreign Team: %v", err)
	}
	return store
}

func mutateScopeTeam(t *testing.T, store *SQLite, mode string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var body []byte
		var id string
		if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='team'`).Scan(&body, &id); err != nil {
			return err
		}
		var record events.ProjectionRecord
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		var team core.Team
		if err := json.Unmarshal(record.Value, &team); err != nil {
			return err
		}
		team.OrganizationID = "org-1"
		var err error
		record.Value, err = json.Marshal(team)
		if err != nil {
			return err
		}
		event, _, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return err
		}
		sealed, err := events.SealProjectionEvent(event, record, nil)
		if err != nil {
			return err
		}
		payload, err := json.Marshal(sealed)
		if err != nil {
			return err
		}
		body, err = json.Marshal(record)
		if err != nil {
			return err
		}
		if mode != "record-only" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, payload, id); err != nil {
				return err
			}
		}
		if mode != "event-only" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, id); err != nil {
				return err
			}
		}
		if mode == "missing-record" {
			if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE admission_event_id=?`, id); err != nil {
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

func TestIncidentProjectionOrganizationScope(t *testing.T) {
	for _, mode := range []string{"both", "event-only", "record-only", "missing-record"} {
		t.Run(mode, func(t *testing.T) {
			store := projectionScopeFixture(t)
			if mode == "opaque-note" {
				if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "opaque-note", Payload: json.RawMessage(`{"organization_id":"org-1","text":{"organization_id":"org-1"}}`)}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "opaque-record" {
				if err := store.AppendRecord(t.Context(), "org-2", "AUDIT_NOTE", "runtime", "", nil, nil, "authorization_trace", "foreign-trace", 1, json.RawMessage(`{"projection_kind":"organization","record_id":"org-2","value":{"id":"org-1"}}`)); err != nil {
					t.Fatal(err)
				}
			}
			mutateScopeTeam(t, store, mode)
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			if mode != "record-only" {
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
					t.Fatal("full owner accepted cross-envelope Team")
				}
			} else {
				err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind='team'`)
					return err
				})
				if err == nil {
					t.Fatal("record owner accepted mismatched Team")
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-org-1", 256)
			if err == nil {
				t.Fatal("incident omitted selected organization claim in foreign Team")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("returned partial evidence")
			}
		})
	}
}

// The paths below come from the durable value types and their admission
// validators, independently of the guard's kind table. Raw invalid sources
// exercise discovery; the sealed Team regression above exercises an admitted
// writer history whose organization claim alone becomes contradictory.
func TestProjectionScopeOwnedClaims(t *testing.T) {
	for _, owner := range []struct{ kind, event, value string }{
		{"organization", "ORGANIZATION_CREATED", `{"id":"org-1"}`},
		{"mission", "MISSION_CREATED", `{"organization_id":"org-1"}`},
		{"goal", "GOAL_CREATED", `{"organization_id":"org-1"}`},
		{"team", "TEAM_CREATED", `{"organization_id":"org-1"}`},
		{"agent_blueprint", "AGENT_BLUEPRINT_CREATED", `{"organization_id":"org-1"}`},
		{"execution_profile", "EXECUTION_PROFILE_CREATED", `{"organization_id":"org-1"}`},
		{"agent", "AGENT_CREATED", `{"organization_id":"org-1"}`},
		{"intent", "INTENT_CREATED", `{"organization_id":"org-1"}`},
		{"lab_experiment", "LAB_EXPERIMENT_STARTED", `{"organization_id":"org-1"}`},
		{"lab_promotion_candidate", "LAB_PROMOTION_CANDIDATE_CREATED", `{"organization_id":"org-1"}`},
		{"knowledge", "KNOWLEDGE_PROPOSED", `{"organization_id":"org-1"}`},
		{"task", "TASK_CREATED", `{"routing":{"organization_id":"org-1"}}`},
	} {
		for _, source := range []string{"event", "record"} {
			for _, applicability := range []string{"typed", "discriminator-missing", "discriminator-corrupt", "discriminator-duplicate"} {
				t.Run(owner.kind+"/"+source+"/"+applicability, func(t *testing.T) {
					store := projectionScopeFixture(t)
					kind := `"projection_kind":"` + owner.kind + `",`
					switch applicability {
					case "discriminator-missing":
						kind = ""
					case "discriminator-corrupt":
						kind = `"projection_kind":"unknown",`
					case "discriminator-duplicate":
						kind = `"projection_kind":"unknown",` + kind
					}
					projection := `{` + kind + `"record_id":"foreign-record","value":` + owner.value + `}`
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						if source == "event" {
							label := owner.event
							if applicability == "typed" || applicability == "discriminator-duplicate" {
								label = "AUDIT_NOTE"
							}
							if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type=?,payload=? WHERE event_type='TEAM_CREATED'`, label, []byte(`{"projection":`+projection+`}`)); err != nil {
								return err
							}
							if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='team'`); err != nil {
								return err
							}
						} else {
							physical := owner.kind
							if applicability == "typed" || applicability == "discriminator-duplicate" {
								physical = "unknown"
							}
							if _, err := tx.ExecContext(t.Context(), `UPDATE records SET kind=?,body=? WHERE kind='team'`, physical, []byte(projection)); err != nil {
								return err
							}
							// A retained envelope independently establishes record scope,
							// despite losing the event's typed projection channel.
							if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload='{}' WHERE event_type='TEAM_CREATED'`); err != nil {
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
					assertScopeConflict(t, store, true)
				})
			}
		}
	}
}

func assertScopeConflict(t *testing.T, store *SQLite, want bool) {
	t.Helper()
	err := store.withTx(t.Context(), func(tx *sql.Tx) error { return validateIncidentProjectionScope(t.Context(), tx, "org-1") })
	if (err != nil) != want {
		t.Fatalf("organization conflict: %v, want rejection=%v", err, want)
	}
	if err != nil && !strings.Contains(err.Error(), "organization claim conflicts") {
		t.Fatalf("unexpected rejection: %v", err)
	}
}

func TestProjectionScopeOccurrenceClaims(t *testing.T) {
	for _, source := range []string{"event", "record"} {
		for _, projection := range []struct{ name, json string }{
			{"duplicate-leaf", `{"projection_kind":"team","value":{"organization_id":"org-2","organization_id":"org-1"}}`},
			{"escaped-leaf", `{"projection_kind":"team","value":{"organizatio\u006e_id":"org-1"}}`},
			{"duplicate-value", `{"projection_kind":"team","value":{"organization_id":"org-2"},"value":{"organization_id":"org-1"}}`},
			{"escaped-value", `{"projection_kind":"team","val\u0075e":{"organization_id":"org-1"}}`},
			{"duplicate-routing", `{"projection_kind":"task","value":{"routing":{"organization_id":"org-2"},"routing":{"organization_id":"org-1"}}}`},
			{"escaped-routing", `{"projection_kind":"task","value":{"routin\u0067":{"organization_id":"org-1"}}}`},
			{"duplicate-discriminator", `{"projection_kind":"work","projection_kind":"team","value":{"organization_id":"org-1"}}`},
			{"escaped-discriminator", `{"projection_kin\u0064":"team","value":{"organization_id":"org-1"}}`},
			{"organization-key", `{"projection_kind":"organization","record_id":"org-1","value":{"id":"org-2"}}`},
		} {
			t.Run(source+"/"+projection.name, func(t *testing.T) {
				store := projectionScopeFixture(t)
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					if source == "event" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='TEAM_CREATED'`, []byte(`{"projection":`+projection.json+`}`)); err != nil {
							return err
						}
					} else {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=? WHERE kind='team'`, []byte(projection.json)); err != nil {
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
				assertScopeConflict(t, store, true)
			})
		}
	}
}

func TestProjectionScopeOrphanApplicability(t *testing.T) {
	for _, mode := range []string{"team-orphan", "organization-orphan-value", "organization-orphan-key", "opaque-note", "opaque-record", "work-extra-field"} {
		t.Run(mode, func(t *testing.T) {
			store := projectionScopeFixture(t)
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				switch mode {
				case "team-orphan":
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM events WHERE event_type='TEAM_CREATED'`); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=json_set(body,'$.value.organization_id','org-1') WHERE kind='team'`); err != nil {
						return err
					}
				case "organization-orphan-value", "organization-orphan-key":
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET admission_event_id='missing-organization-event' WHERE kind='organization' AND record_id='org-2'`); err != nil {
						return err
					}
					path := "$.value.id"
					if mode == "organization-orphan-key" {
						path = "$.record_id"
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=json_set(body,?,'org-1') WHERE kind='organization' AND record_id='org-2'`, path); err != nil {
						return err
					}
				case "work-extra-field":
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=json_set(payload,'$.projection.value.organization_id','org-1') WHERE event_type='WORK_CREATED' AND organization_id='org-2'`); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=json_set(body,'$.value.organization_id','org-1') WHERE kind='work' AND record_id='work-org-2'`); err != nil {
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
			assertScopeConflict(t, store, strings.HasPrefix(mode, "organization-orphan"))
			if mode == "opaque-note" || mode == "opaque-record" {
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
					t.Fatalf("healthy opaque writer history: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-org-1", 256); err != nil {
					t.Fatalf("healthy opaque source affected incident: %v", err)
				}
			}
		})
	}
}

func TestIncidentProjectionDistinctOrganizationClaims(t *testing.T) {
	for _, claim := range []string{"organization-value", "organization-record-id", "organization-key", "task-routing"} {
		for _, source := range []string{"both", "event-only", "record-only"} {
			if claim == "organization-key" && source != "record-only" {
				continue
			}
			t.Run(claim+"/"+source, func(t *testing.T) {
				store := projectionScopeFixture(t)
				kind, recordID := "organization", "org-2"
				if claim == "task-routing" {
					kind, recordID = "task", "routed-task-2"
					now := time.Now().UTC()
					blueprint := core.AgentBlueprint{ID: "scope-blueprint", OrganizationID: "org-2", Version: "v1", Role: "worker", OperatingInstructions: "bounded work", Status: "ACTIVE", CreatedAt: now}
					profile := core.ExecutionProfile{ID: "scope-profile", OrganizationID: "org-2", Version: "v1", ConnectionID: "scope-account", ModelProvider: "provider", Model: "model", PromptVersion: "v1", Status: "ACTIVE", CreatedAt: now}
					agent := core.Agent{ID: "scope-agent", OrganizationID: "org-2", BlueprintID: blueprint.ID, BlueprintVersion: "v1", ExecutionProfileID: profile.ID, ExecutionProfileVersion: "v1", RuntimeAdapter: "local", Status: "ACTIVE"}
					config := core.AgentConfig{BlueprintID: blueprint.ID, BlueprintVersion: "v1", ProfileID: profile.ID, ProfileVersion: "v1", RuntimeAdapter: "local"}
					for _, draft := range []events.ProjectionDraft{
						{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "AGENT_BLUEPRINT_CREATED", SourceActorID: "runtime", CorrelationID: "scope-roster"}, ProjectionKind: "agent_blueprint", RecordID: string(blueprint.ID), Version: 1, Value: blueprint},
						{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "EXECUTION_PROFILE_CREATED", SourceActorID: "runtime", CorrelationID: "scope-roster"}, ProjectionKind: "execution_profile", RecordID: string(profile.ID), Version: 1, Value: profile},
						{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "scope-roster"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent},
					} {
						if _, err := store.AppendProjection(t.Context(), draft); err != nil {
							t.Fatal(err)
						}
					}
					routing := modelinput.RouteRequirements{OrganizationID: "org-2", Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 1, OutputTokens: 1, Locality: modelinput.CloudAllowed, DataClass: "internal"}
					fingerprint, err := routing.Fingerprint()
					if err != nil {
						t.Fatal(err)
					}
					decision := modelinput.RouteDecision{Version: 1, RequirementsFingerprint: fingerprint, PolicyFingerprint: strings.Repeat("0", 64), SelectedAt: now, SnapshotSequence: 1, ConnectionID: profile.ConnectionID, Provider: profile.ModelProvider, Model: profile.Model, ExecutionProfileVersion: profile.Version, Reason: modelinput.RouteOrdered, ReservedInputTokens: 1, ReservedOutputTokens: 1}
					task := core.Task{ID: core.ID(recordID), WorkID: "work-org-2", Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent,
						ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: agent.ID, AgentConfig: &config,
						TaskContractVersion: "1", Status: core.TaskPending,
						Routing: &routing, RoutingDecision: &decision}
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: recordID, CorrelationID: "incident-org-2"}, ProjectionKind: kind, RecordID: recordID, Version: 1, Value: task}); err != nil {
						t.Fatal(err)
					}
				}
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err != nil {
					t.Fatalf("writer fixture: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-org-1", 256); err != nil {
					t.Fatalf("healthy distinct fixture: %v", err)
				}
				if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
					var body []byte
					var eventID string
					if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind=? AND record_id=?`, kind, recordID).Scan(&body, &eventID); err != nil {
						return err
					}
					if claim == "organization-key" {
						_, err := tx.ExecContext(t.Context(), `UPDATE records SET record_id='org-1',version=2 WHERE kind=? AND record_id=?`, kind, recordID)
						return err
					}
					var record events.ProjectionRecord
					if err := json.Unmarshal(body, &record); err != nil {
						return err
					}
					var value map[string]any
					if err := json.Unmarshal(record.Value, &value); err != nil {
						return err
					}
					switch claim {
					case "organization-value":
						value["id"] = "org-1"
					case "organization-record-id":
						record.RecordID = "org-1"
					case "task-routing":
						routing, ok := value["routing"].(map[string]any)
						if !ok {
							return fmt.Errorf("Task fixture has no routing object")
						}
						routing["organization_id"] = "org-1"
					}
					var err error
					record.Value, err = json.Marshal(value)
					if err != nil {
						return err
					}
					event, _, err := eventByID(t.Context(), tx, eventID)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(event, record, nil)
					if err != nil {
						return err
					}
					payload, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					body, err = json.Marshal(record)
					if err != nil {
						return err
					}
					if source != "record-only" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, payload, eventID); err != nil {
							return err
						}
					}
					if source != "event-only" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, eventID); err != nil {
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
				stream, err = store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if source != "record-only" {
					if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
						t.Fatal("full owner accepted contradictory organization claim")
					}
				} else {
					lookup, version := recordID, 1
					if claim == "organization-key" {
						lookup, version = "org-1", 2
					}
					err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind=? AND r.record_id=? AND r.version=?`, kind, lookup, version)
						return err
					})
					if err == nil {
						t.Fatal("record owner accepted contradictory identity")
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "incident-org-1", 256)
				if err == nil {
					t.Fatal("incident omitted contradictory owned identity")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("returned partial evidence")
				}
			})
		}
	}
}

func TestProjectionScopeCounterpartApplicability(t *testing.T) {
	for _, source := range []string{"event", "record"} {
		t.Run(source, func(t *testing.T) {
			store := projectionScopeFixture(t)
			if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
				body := []byte(`{"projection_kind":"unknown","value":{"organization_id":"org-1"}}`)
				if source == "event" {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=? WHERE event_type='TEAM_CREATED'`, []byte(`{"projection":`+string(body)+`}`)); err != nil {
						return err
					}
				} else {
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET kind='unknown',body=? WHERE kind='team'`, body); err != nil {
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
			assertScopeConflict(t, store, true)
		})
	}
}

func TestIncidentProjectionEmptyCorrelation(t *testing.T) {
	store := projectionScopeFixture(t)
	if snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "absent", 256); err != nil || len(snapshot.Work.Events) != 0 {
		t.Fatalf("healthy empty incident: %v", err)
	}
	mutateScopeTeam(t, store, "both")
	if snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "absent", 256); err == nil || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatalf("empty correlation omitted contradictory organization claim: %v", err)
	}
}

func TestProjectionScopeRootOccurrences(t *testing.T) {
	for _, mode := range []string{"duplicate", "escaped"} {
		t.Run(mode, func(t *testing.T) {
			store := projectionScopeFixture(t)
			body := `{"projection":{"projection_kind":"team","value":{"organization_id":"org-2"}},"projection":{"projection_kind":"team","value":{"organization_id":"org-1"}}}`
			if mode == "escaped" {
				body = `{"pr\u006fjection":{"projection_kind":"team","value":{"organization_id":"org-1"}}}`
			}
			if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_type='TEAM_CREATED'`, []byte(body)); err != nil {
				t.Fatal(err)
			}
			assertScopeConflict(t, store, true)
		})
	}
}

func BenchmarkIncidentProjectionScope(b *testing.B) {
	for _, selected := range []int{1, 8} {
		for _, foreign := range []int{8, 256} {
			b.Run(fmt.Sprintf("selected-%d/foreign-%d", selected, foreign), func(b *testing.B) {
				store, err := Open(":memory:")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = store.Close() })
				correlation := appendIncidentLineage(b, store, selected)
				now := time.Now().UTC()
				if _, err := store.AppendProjection(b.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "foreign-setup"}, ProjectionKind: "organization", RecordID: "org-2", Version: 1, Value: core.Organization{ID: "org-2", Name: "Foreign", PolicyVersion: "v1", CreatedAt: now}}); err != nil {
					b.Fatal(err)
				}
				for n := range foreign {
					id := fmt.Sprintf("foreign-team-%d", n)
					if _, err := store.AppendProjection(b.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: id}, ProjectionKind: "team", RecordID: id, Version: 1, Value: core.Team{ID: core.ID(id), OrganizationID: "org-2", Name: "Foreign Team", Status: "ACTIVE", CreatedAt: now}}); err != nil {
						b.Fatal(err)
					}
				}
				if snapshot, err := store.VerifiedIncidentEvents(b.Context(), "org-1", correlation, 256); err != nil || len(snapshot.Work.Events) != 1 {
					b.Fatalf("healthy selected operation: %v", err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := store.VerifiedIncidentEvents(b.Context(), "org-1", correlation, 256); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
