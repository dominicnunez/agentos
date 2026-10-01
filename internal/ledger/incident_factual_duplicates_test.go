package ledger

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentDuplicateFactualEligibility(t *testing.T) {
	for _, field := range []string{"status", "context_use", "scope", "scope_id", "organization_id", "value", "projection", "member_agent_ids"} {
		t.Run(field, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "factual.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "factual", true)
			if field == "member_agent_ids" {
				team := core.Team{ID: "factual-team", OrganizationID: "org-1", Name: "Factual team", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "factual-roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team}); err != nil {
					t.Fatal(err)
				}
			}
			// The original title does not match this task's consumed context. After
			// admission, make the same factual identity relevant independently of
			// the omitted manifest, then place each relevant value after a decoy.
			appendFactualInferenceKnowledge(t, store, "hidden-fact", "Revenue", "Sales increased three percent.")
			if field == "member_agent_ids" {
				rewriteIncidentFact(t, store, func(record *core.KnowledgeRecord) {
					record.Scope = core.KnowledgeScopeTeam
					record.ScopeID = "factual-team"
				})
			}
			var request inference.InferenceRequest
			if field == "member_agent_ids" {
				request = appendInboxTaskInference(t, store, agent, config, "factual", []events.InboxRoute{{Scope: events.RecipientTeam, ID: "factual-team"}})
			} else {
				request = appendBenchmarkTaskInference(t, store, agent, config, "factual")
			}
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			reservation, err := store.ReserveInference(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReconcileInference(t.Context(), reservation, nil, inference.ReconciliationNotSent); err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "factual", 256); err != nil {
				t.Fatalf("valid fixture: %v", err)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			err = store.withTx(t.Context(), func(tx *sql.Tx) error {
				for _, event := range stream {
					payload, present, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					fact := payload.Projection.ProjectionKind == "knowledge" && payload.Projection.RecordID == "hidden-fact"
					team := field == "member_agent_ids" && payload.Projection.ProjectionKind == "team" && payload.Projection.RecordID == "factual-team"
					if !present || (!fact && !team) {
						continue
					}
					if payload.Projection.ProjectionKind == "knowledge" {
						var record core.KnowledgeRecord
						if err := json.Unmarshal(payload.Projection.Value, &record); err != nil {
							return err
						}
						record.Title = "Bounded Agent work"
						payload.Projection.Value, err = json.Marshal(record)
						if err != nil {
							return err
						}
					}
					if field == "member_agent_ids" && payload.Projection.ProjectionKind == "team" {
						payload.Projection.Value = json.RawMessage(strings.Replace(string(payload.Projection.Value), `"member_agent_ids":`, `"member_agent_ids":[],"member_agent_ids":`, 1))
					} else if field != "value" && field != "projection" && field != "member_agent_ids" {
						key := `"` + field + `":"`
						payload.Projection.Value = json.RawMessage(strings.Replace(string(payload.Projection.Value), key, `"`+field+`":"unrelated",`+key, 1))
					}
					sealed, err := events.SealProjectionEvent(event, payload.Projection, payload.Detail)
					if err != nil {
						return err
					}
					body, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					if field == "value" {
						body = []byte(strings.Replace(string(body), `"value":`, `"value":{},"value":`, 1))
					}
					if field == "projection" {
						body = []byte(strings.Replace(string(body), `"projection":`, `"projection":{},"projection":`, 1))
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
						return err
					}
					body, err = json.Marshal(payload.Projection)
					if err != nil {
						return err
					}
					if field == "value" || field == "projection" {
						body = []byte(strings.Replace(string(body), `"value":`, `"value":{},"value":`, 1))
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, body, sealed.Admission.Fingerprint, event.EventID); err != nil {
						return err
					}
				}
				if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
					return err
				}
				return rebuildEventIntegrity(t.Context(), tx)
			})
			if err != nil {
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
			if err := store.ValidateInferenceAdmissions(t.Context()); err == nil {
				t.Fatalf("full recovery must detect omitted factual input: %v", err)
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "factual", 256)
			if err == nil {
				t.Fatal("incident accepted manifest omitting eligible factual context")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid factual context returned partial evidence")
			}
		})
	}
}
