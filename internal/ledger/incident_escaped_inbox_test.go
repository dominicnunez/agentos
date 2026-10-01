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
	"github.com/dominicnunez/agentos/internal/inference"
)

// Escaped member names have the same decoded meaning and preserve the admitted
// Team. They must not erase another correlation's eligible inbox input.
func TestIncidentEscapedTeamInbox(t *testing.T) {
	for _, field := range []string{"member_agent_ids", "projection", "value", "record_id"} {
		for _, mode := range []string{"omitted", "other-recipient", "after-cutoff"} {
			t.Run(field+"/"+mode, func(t *testing.T) {
				store, err := Open(":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = store.Close() }()
				agent, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", true)
				team := core.Team{ID: "escaped-inbox-team", OrganizationID: "org-1", Name: "Inbox team", MemberAgentIDs: []core.ID{agent.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				teamEvent, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "roster"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team})
				if err != nil {
					t.Fatal(err)
				}
				noteDraft := events.TrustedDraft{OrganizationID: "org-1", CorrelationID: "other-work", SourceActorID: "runtime", EventType: "AUDIT_NOTE", Payload: map[string]string{"text": "inbox input"}}
				note, err := store.Append(t.Context(), noteDraft)
				if err != nil {
					t.Fatal(err)
				}
				request := appendInboxTaskInference(t, store, agent, config, "selected", []events.InboxRoute{{Scope: events.RecipientTeam, ID: string(team.ID)}})
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
				if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
					t.Fatalf("healthy full owner: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
					t.Fatalf("healthy incident: %v", err)
				}
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					payload, present, err := events.AdmittedProjection(teamEvent)
					if err != nil {
						return err
					}
					if !present {
						t.Fatal("Team admission missing")
					}
					escaped := func(body []byte) []byte {
						return []byte(strings.Replace(string(body), `"`+field+`":`, `"`+field[:1]+`\u`+map[string]string{"member_agent_ids": "0065mber_agent_ids", "projection": "0072ojection", "value": "0061lue", "record_id": "0065cord_id"}[field]+`":`, 1))
					}
					if field == "member_agent_ids" {
						payload.Projection.Value = escaped(payload.Projection.Value)
					}
					sealed, err := events.SealProjectionEvent(teamEvent, payload.Projection, payload.Detail)
					if err != nil {
						return err
					}
					body, err := json.Marshal(sealed)
					if err != nil {
						return err
					}
					if field != "member_agent_ids" {
						body = escaped(body)
					}
					// Assert semantic equivalence independently of the SQL selector.
					equivalent := teamEvent
					equivalent.Payload = body
					decoded, present, err := events.AdmittedProjection(equivalent)
					if err != nil {
						return err
					}
					var actual core.Team
					if !present {
						t.Fatal("escaped Team lost admission")
					}
					if err := json.Unmarshal(decoded.Projection.Value, &actual); err != nil {
						return err
					}
					if !reflect.DeepEqual(actual, team) {
						t.Fatal("escaping changed Team")
					}
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, teamEvent.EventID); err != nil {
						return err
					}
					recordBody, err := json.Marshal(payload.Projection)
					if err != nil {
						return err
					}

					if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, teamEvent.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
					t.Fatalf("equivalent escaped Team changed full owner: %v", err)
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
					t.Fatalf("equivalent escaped Team changed incident: %v", err)
				}
				if mode == "after-cutoff" {
					note, err = store.Append(t.Context(), noteDraft)
					if err != nil {
						t.Fatal(err)
					}
				}
				recipient := string(team.ID)
				if mode == "other-recipient" {
					recipient += "-unrelated"
				}
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(t.Context(), `UPDATE events SET recipient_scope=?,recipient_id=? WHERE event_id=?`, events.RecipientTeam, recipient, note.EventID); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `INSERT INTO inbox(recipient_scope,recipient_id,event_id,organization_id,available_at) VALUES(?,?,?,?,?)`, events.RecipientTeam, recipient, note.EventID, "org-1", note.CreatedAt.Format(time.RFC3339Nano)); err != nil {
						return err
					}
					if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
						return err
					}
					return rebuildEventIntegrity(t.Context(), tx)
				})
				if err != nil {
					t.Fatal(err)
				}
				ownerErr := store.ValidateInferenceAdmissions(t.Context())
				snapshot, incidentErr := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if mode != "omitted" {
					if ownerErr != nil || incidentErr != nil {
						t.Fatalf("unrelated route/time: full=%v incident=%v", ownerErr, incidentErr)
					}
					return
				}
				if ownerErr == nil {
					t.Fatal("full owner accepted omitted eligible Team inbox input")
				}
				if !strings.Contains(ownerErr.Error(), "execution context references do not match durable runtime selection") {
					t.Fatalf("unrelated full owner rejection: %v", ownerErr)
				}
				t.Logf("full owner rejected omitted Team inbox input: %v", ownerErr)
				if incidentErr == nil {
					t.Fatal("incident omitted eligible Team inbox input behind valid escaped Team key")
				}
				t.Logf("incident rejected omitted Team inbox input: %v", incidentErr)
				var rawMembers sql.NullString
				if err := store.db.QueryRowContext(t.Context(), `SELECT json_extract(payload,'$.projection.value.member_agent_ids') FROM events WHERE event_id=?`, teamEvent.EventID).Scan(&rawMembers); err != nil {
					t.Fatal(err)
				}
				t.Logf("SQL canonical path finds escaped members: %v", rawMembers.Valid)
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("failure returned partial snapshot")
				}
			})
		}
	}
}
