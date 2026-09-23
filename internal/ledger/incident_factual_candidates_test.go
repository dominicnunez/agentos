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
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentOmittedFactualContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "factual.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "factual", true)
	appendFactualInferenceKnowledge(t, store, "hidden-fact", "Revenue", "Sales increased three percent.")
	request := appendBenchmarkTaskInference(t, store, agent, config, "factual")
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
			if !present || payload.Projection.ProjectionKind != "knowledge" || payload.Projection.RecordID != "hidden-fact" {
				continue
			}
			var record core.KnowledgeRecord
			if err := json.Unmarshal(payload.Projection.Value, &record); err != nil {
				return err
			}
			record.Title = "Bounded Agent work"
			payload.Projection.Value, err = json.Marshal(record)
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
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
				return err
			}
			body, err = json.Marshal(payload.Projection)
			if err != nil {
				return err
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
	if err := store.ValidateInferenceAdmissions(t.Context()); err == nil || !strings.Contains(err.Error(), "knowledge references") {
		t.Fatalf("full recovery must detect omitted factual input: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "factual", 256)
	if err == nil {
		t.Fatal("incident accepted manifest omitting eligible factual context")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("invalid factual context returned partial evidence")
	}
}

func TestIncidentFactualScopeAndTime(t *testing.T) {
	for _, scope := range []core.KnowledgeScope{core.KnowledgeScopeOrganization, core.KnowledgeScopeAgent, core.KnowledgeScopeTeam} {
		for _, mode := range []string{"omitted", "unchanged", "late", "stale-before", "stale-after", "removed-before", "removed-after", "invalid-removal"} {
			if strings.Contains(mode, "remov") && scope != core.KnowledgeScopeTeam {
				continue
			}
			t.Run(string(scope)+"/"+mode, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "factual", true)
				team := core.Team{ID: "factual-team", OrganizationID: "org-1", Name: "Factual team", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				if scope == core.KnowledgeScopeTeam {
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "factual-roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team}); err != nil {
						t.Fatal(err)
					}
				}
				remove := func() {
					team.MemberAgentIDs = []core.ID{}
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_REVISED", SourceActorID: "runtime", CorrelationID: "factual-roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 2, Value: team}); err != nil {
						t.Fatal(err)
					}
				}
				var fact core.KnowledgeRecord
				addFact := func() {
					fact = appendFactualInferenceKnowledge(t, store, "hidden-fact", "Revenue", "Sales increased three percent.")
					rewriteIncidentFact(t, store, func(record *core.KnowledgeRecord) {
						record.Scope = scope
						switch scope {
						case core.KnowledgeScopeOrganization:
							record.ScopeID = "org-1"
						case core.KnowledgeScopeAgent:
							record.ScopeID = agent.ID
						case core.KnowledgeScopeTeam:
							record.ScopeID = team.ID
						}
					})
					fact.Scope = scope
					switch scope {
					case core.KnowledgeScopeOrganization:
						fact.ScopeID = "org-1"
					case core.KnowledgeScopeAgent:
						fact.ScopeID = agent.ID
					case core.KnowledgeScopeTeam:
						fact.ScopeID = team.ID
					}
				}
				stale := func() {
					previous := 2
					fact.Version = 3
					fact.Status = core.KnowledgeStale
					fact.SupersedesVersion = &previous
					if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_STALE", SourceActorID: "runtime", CorrelationID: "knowledge-hidden-fact", ArtifactRefs: fact.EvidenceArtifactRefs}, ProjectionKind: "knowledge", RecordID: "hidden-fact", Version: 3, Value: fact}); err != nil {
						t.Fatal(err)
					}
				}
				if mode != "late" {
					addFact()
				}
				if mode == "stale-before" {
					stale()
				}
				if mode == "removed-before" || mode == "invalid-removal" {
					remove()
				}
				var routes []events.InboxRoute
				if scope == core.KnowledgeScopeTeam && mode != "removed-before" && mode != "invalid-removal" {
					routes = []events.InboxRoute{{Scope: events.RecipientTeam, ID: string(team.ID)}}
				}
				request := appendInboxTaskInference(t, store, agent, config, "factual", routes)
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
				if mode == "late" {
					addFact()
				}
				if mode == "stale-after" {
					stale()
				}
				if mode == "removed-after" {
					remove()
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "factual", 256); err != nil {
					t.Fatalf("valid scope/time fixture: %v", err)
				}
				if mode != "unchanged" {
					rewriteIncidentFact(t, store, func(record *core.KnowledgeRecord) { record.Title = "Bounded Agent work" })
				}
				if mode == "invalid-removal" {
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE kind='team' AND version=2`); err != nil {
							return err
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				wantReject := mode == "omitted" || mode == "stale-after" || mode == "removed-after" || mode == "invalid-removal"
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "factual", 256)
				if wantReject {
					if err == nil || !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
						t.Fatalf("accepted omitted or invalid candidate history: %v", err)
					}
				} else if err != nil {
					t.Fatalf("ineligible fact changed historical input: %v", err)
				}
				if mode != "invalid-removal" {
					err = store.ValidateInferenceAdmissions(t.Context())
					if (err != nil) != wantReject {
						t.Fatalf("full inference replay disagreement: %v", err)
					}
				}
			})
		}
	}
}

func rewriteIncidentFact(t *testing.T, store *SQLite, change func(*core.KnowledgeRecord)) {
	t.Helper()
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
			if !present || payload.Projection.ProjectionKind != "knowledge" || payload.Projection.RecordID != "hidden-fact" {
				continue
			}
			var record core.KnowledgeRecord
			if err := json.Unmarshal(payload.Projection.Value, &record); err != nil {
				return err
			}
			change(&record)
			payload.Projection.Value, err = json.Marshal(record)
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
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, event.EventID); err != nil {
				return err
			}
			body, err = json.Marshal(payload.Projection)
			if err != nil {
				return err
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
}

func TestIncidentFactualGrowth(t *testing.T) {
	for _, count := range []int{4, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "factual-growth", true)
			for i := 0; i < count; i++ {
				appendFactualInferenceKnowledge(t, store, fmt.Sprintf("fact-%d", i), "Revenue", "Sales increased three percent.")
			}
			request := appendBenchmarkTaskInference(t, store, agent, config, "factual-growth")
			policy := testInferencePolicy(time.Now().UTC())
			policy.OrganizationID, policy.Provider, policy.Model, policy.ExecutionProfileVersion = "org-1", "provider", "model", config.ProfileVersion
			policy.Mode, policy.Pricing = inference.Local, nil
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			var baseline events.IncidentSnapshot
			for _, unrelated := range []int{0, 128} {
				for i := 0; i < unrelated; i++ {
					if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "other-org", EventType: "AUDIT_NOTE", CorrelationID: "unrelated", Payload: map[string]string{"summary": "unrelated"}}); err != nil {
						t.Fatal(err)
					}
				}
				started := time.Now()
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "factual-growth", 256)
				t.Logf("factual candidates=%d unrelated=%d elapsed=%s", count, unrelated, time.Since(started))
				if err != nil {
					t.Fatal(err)
				}
				if unrelated == 0 {
					baseline = snapshot
				} else if !reflect.DeepEqual(snapshot.DependencyEvents, baseline.DependencyEvents) {
					t.Fatal("unrelated history changed factual evidence")
				}
			}
		})
	}
}
