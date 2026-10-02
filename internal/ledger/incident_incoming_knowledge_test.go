package ledger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingDerivedKnowledge(t *testing.T) {
	parallelIncidentTest(t)
	for _, side := range []string{"unrelated", "same-org", "event", "record", "both"} {
		t.Run(side, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incoming-knowledge.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			selectedCorrelation := appendDerivedIncidentChain(t, store, 1)
			org := core.ID("org-2")
			if side == "same-org" {
				org = "org-1"
			} else {
				_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
					Event:          events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "other-setup"},
					ProjectionKind: "organization", RecordID: "org-2", Version: 1,
					Value: core.Organization{ID: "org-2", Name: "Other", PolicyVersion: "v1", CreatedAt: time.Now().UTC()},
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			artifact := "artifact-incoming"
			evidence, err := store.Append(t.Context(), events.TrustedDraft{
				OrganizationID: string(org), EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "incoming-evidence",
				ArtifactRefs: []string{artifact}, Payload: map[string]string{"summary": "Independent recorded observation."},
			})
			if err != nil {
				t.Fatal(err)
			}
			candidate := core.KnowledgeRecord{
				KnowledgeID: "incoming-knowledge", OrganizationID: org, Version: 1, Type: core.KnowledgeLesson,
				Scope: core.KnowledgeScopeOrganization, ScopeID: org, Status: core.KnowledgeCandidate,
				Title: "Incoming observation", Content: "Preserve the independent observation.",
				Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{evidence.EventID},
				EvidenceArtifactRefs: []string{artifact}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime,
				CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated,
			}
			if side == "same-org" {
				candidate.Basis = core.KnowledgeBasisDerived
				candidate.DerivedKnowledgeRefs = []core.VersionedRef{{ID: "derived-0", Version: "2", MaterializationState: core.MaterializedFull}}
			}
			_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
				Event:          events.TrustedDraft{OrganizationID: string(org), EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-incoming-knowledge", ArtifactRefs: []string{artifact}},
				ProjectionKind: "knowledge", RecordID: "incoming-knowledge", Version: 1, Value: candidate,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			if side == "event" || side == "record" || side == "both" {
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					var body []byte
					var id string
					if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='knowledge' AND record_id='incoming-knowledge'`).Scan(&body, &id); err != nil {
						return err
					}
					event, found, err := eventByID(t.Context(), tx, id)
					if err != nil {
						return err
					}
					if !found {
						return fmt.Errorf("missing incoming Knowledge event")
					}
					payload, _, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					var record events.ProjectionRecord
					if err := json.Unmarshal(body, &record); err != nil {
						return err
					}
					candidate.Basis = core.KnowledgeBasisDerived
					candidate.DerivedKnowledgeRefs = []core.VersionedRef{{ID: "derived-0", Version: "2", MaterializationState: core.MaterializedFull}}
					record.Value, err = json.Marshal(candidate)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(event, record, payload.Detail)
					if err != nil {
						return err
					}
					eventBody, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					recordBody, err := json.Marshal(record)
					if err != nil {
						return err
					}
					if side != "record" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, eventBody, id); err != nil {
							return err
						}
					}
					if side != "event" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, id); err != nil {
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
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if side == "both" {
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
					t.Fatal("full history accepted a cross-organization derived Knowledge link")
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", selectedCorrelation, 256)
			switch side {
			case "unrelated":
				if err != nil {
					t.Fatal(err)
				}
			case "same-org":
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range snapshot.DependencyEvents {
					if event.EventType == "KNOWLEDGE_PROPOSED" && event.CorrelationID == "knowledge-incoming-knowledge" {
						return
					}
				}
				t.Fatal("valid incoming derived Knowledge omitted from incident dependencies")
			default:
				if err == nil {
					t.Fatal("incident accepted incoming cross-organization derived Knowledge link")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("partial snapshot")
				}
			}
		})
	}
}

func TestIncidentIncomingKnowledgeRoster(t *testing.T) {
	parallelIncidentTest(t)
	for _, relation := range []string{"agent-scope", "team-scope", "agent-creator"} {
		for _, side := range []string{"same-org", "event", "record", "both"} {
			if side == "same-org" && relation == "agent-creator" {
				continue
			}
			t.Run(relation+"/"+side, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "knowledge-scope.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				selected, _ := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
				other, _ := appendTaskAssignmentAgent(t, t.Context(), store, "org-2", "other", true)
				selectedScope, otherScope := selected.ID, other.ID
				if relation == "team-scope" {
					selectedScope, otherScope = "team-selected", "team-other"
					for _, item := range []struct {
						org, correlation string
						id, member       core.ID
					}{{"org-1", "roster-selected", selectedScope, selected.ID}, {"org-2", "roster-other", otherScope, other.ID}} {
						_, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
							Event:          events.TrustedDraft{OrganizationID: item.org, EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: item.correlation},
							ProjectionKind: "team", RecordID: string(item.id), Version: 1,
							Value: core.Team{ID: item.id, OrganizationID: core.ID(item.org), Name: "Scoped team", MemberAgentIDs: []core.ID{item.member}, Status: "ACTIVE", CreatedAt: time.Now().UTC()},
						})
						if err != nil {
							t.Fatal(err)
						}
					}
				}
				knowledgeOrg, knowledgeScope := "org-2", otherScope
				if side == "same-org" {
					knowledgeOrg, knowledgeScope = "org-1", selectedScope
				}
				artifact := "artifact-scope"
				evidence, err := store.Append(t.Context(), events.TrustedDraft{
					OrganizationID: knowledgeOrg, EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "scope-evidence",
					ArtifactRefs: []string{artifact}, Payload: map[string]string{"summary": "Scoped observation."},
				})
				if err != nil {
					t.Fatal(err)
				}
				candidate := core.KnowledgeRecord{
					KnowledgeID: "scoped-knowledge", OrganizationID: core.ID(knowledgeOrg), Version: 1, Type: core.KnowledgeLesson,
					Scope: core.KnowledgeScopeAgent, ScopeID: knowledgeScope, Status: core.KnowledgeCandidate,
					Title: "Scoped observation", Content: "Preserve the scoped observation.",
					Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{evidence.EventID},
					EvidenceArtifactRefs: []string{artifact}, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime,
					CreatedAt: time.Now().UTC(), ValidationMethod: core.KnowledgeValidationUnvalidated,
				}
				switch relation {
				case "team-scope":
					candidate.Scope = core.KnowledgeScopeTeam
				case "agent-creator":
					candidate.Scope, candidate.ScopeID = core.KnowledgeScopeOrganization, "org-2"
				}
				_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{
					Event:          events.TrustedDraft{OrganizationID: knowledgeOrg, EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-scoped-knowledge", ArtifactRefs: []string{artifact}},
					ProjectionKind: "knowledge", RecordID: "scoped-knowledge", Version: 1, Value: candidate,
				})
				if err != nil {
					t.Fatal(err)
				}
				baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "roster-selected", 256)
				if err != nil {
					t.Fatalf("valid baseline: %v", err)
				}
				if side == "same-org" {
					for _, event := range baseline.DependencyEvents {
						if event.EventType == "KNOWLEDGE_PROPOSED" && event.CorrelationID == "knowledge-scoped-knowledge" {
							return
						}
					}
					t.Fatal("valid scoped Knowledge omitted from incident dependencies")
				}
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					var body []byte
					var id string
					if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='knowledge' AND record_id='scoped-knowledge'`).Scan(&body, &id); err != nil {
						return err
					}
					event, found, err := eventByID(t.Context(), tx, id)
					if err != nil {
						return err
					}
					if !found {
						return fmt.Errorf("missing scoped Knowledge event")
					}
					payload, _, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					var record events.ProjectionRecord
					if err := json.Unmarshal(body, &record); err != nil {
						return err
					}
					if relation == "agent-creator" {
						candidate.CreatedBy, candidate.CreatedByKind = selectedScope, core.PrincipalAgent
					} else {
						candidate.ScopeID = selectedScope
					}
					record.Value, err = json.Marshal(candidate)
					if err != nil {
						return err
					}
					sealed, err := events.SealProjectionEvent(event, record, payload.Detail)
					if err != nil {
						return err
					}
					eventBody, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					recordBody, err := json.Marshal(record)
					if err != nil {
						return err
					}
					if side != "record" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, eventBody, id); err != nil {
							return err
						}
					}
					if side != "event" {
						if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, id); err != nil {
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
				if side == "both" {
					stream, err := store.Events(t.Context(), "")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := events.ValidateProjectionHistory(stream, nil, nil, nil); err == nil {
						t.Fatal("full history accepted invalid incoming Knowledge roster link")
					}
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "roster-selected", 256)
				if err == nil {
					t.Fatal("incident accepted incoming invalid Knowledge roster link")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("partial snapshot")
				}
			})
		}
	}
}
