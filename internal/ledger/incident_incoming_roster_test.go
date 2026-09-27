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

func TestIncidentIncomingRosterLinks(t *testing.T) {
	for _, link := range []struct {
		name, kind, recordID, field, target string
	}{
		{"team_member_agent", "team", "team-foreign", "member_agent_ids", "agent-selected"},
		{"task_assignee_agent", "task", "task-foreign-agent", "assignee_id", "agent-selected"},
		{"task_assignee_team", "task", "task-foreign-team", "assignee_id", "team-selected"},
		{"agent_blueprint", "agent", "agent-foreign", "blueprint_id", "blueprint-selected"},
		{"task_blueprint", "task", "task-foreign-agent", "agent_config.blueprint_id", "blueprint-selected"},
		{"agent_profile", "agent", "agent-foreign", "execution_profile_id", "profile-selected"},
		{"task_profile", "task", "task-foreign-agent", "agent_config.profile_id", "profile-selected"},
	} {
		for _, side := range []string{"unrelated", "event", "record", "both"} {
			t.Run(link.name+"/"+side, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "roster.db")
				store, err := Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				appendTaskProjectionParents(t, t.Context(), store, "org-1", "selected", "work-1")
				selected, selectedConfig := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", false)
				appendPendingAgentExecutionTask(t, t.Context(), store, "selected", "task-selected", selected, selectedConfig)
				selectedTeam := core.Team{ID: "team-selected", OrganizationID: "org-1", Name: "Selected team", MemberAgentIDs: []core.ID{selected.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				appendIncomingRosterTeam(t, store, "org-1", "selected", selectedTeam)
				appendIncomingRosterTeamTask(t, store, "org-1", "selected", "work-1", "task-selected-team", selectedTeam.ID)

				appendTaskProjectionParents(t, t.Context(), store, "org-2", "foreign", "foreign-work")
				foreign, foreignConfig := appendTaskAssignmentAgent(t, t.Context(), store, "org-2", "foreign", false)
				foreignTeam := core.Team{ID: "team-foreign", OrganizationID: "org-2", Name: "Foreign team", MemberAgentIDs: []core.ID{foreign.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				appendIncomingRosterTeam(t, store, "org-2", "foreign", foreignTeam)
				foreignTask := core.Task{ID: "task-foreign-agent", WorkID: "foreign-work", Description: "bounded Agent work", ExecutionKind: core.ExecutionAgent, ModelInferencePolicy: core.InferenceAllowed, AssigneeType: "AGENT", AssigneeID: foreign.ID, AgentConfig: &foreignConfig, TaskContractVersion: "1", Status: core.TaskPending}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(foreignTask.ID), CorrelationID: "foreign"}, ProjectionKind: "task", RecordID: string(foreignTask.ID), Version: 1, Value: foreignTask}); err != nil {
					t.Fatal(err)
				}
				appendIncomingRosterTeamTask(t, store, "org-2", "foreign", "foreign-work", "task-foreign-team", foreignTeam.ID)
				if err := incomingRosterFullValidation(t, store); err != nil {
					t.Fatalf("valid full recovery: %v", err)
				}
				baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if err != nil {
					t.Fatalf("valid incident baseline: %v", err)
				}
				selectedRecords := map[string]bool{}
				for _, stream := range [][]events.Event{baseline.Work.Events, baseline.DependencyEvents, baseline.RelatedEvents} {
					for _, event := range stream {
						payload, present, err := events.AdmittedProjection(event)
						if err != nil {
							t.Fatal(err)
						}
						if present {
							selectedRecords[payload.Projection.RecordID] = true
						}
					}
				}
				for _, id := range []string{"agent-selected", "team-selected", "blueprint-selected", "profile-selected"} {
					if !selectedRecords[id] {
						t.Fatalf("baseline omitted selected roster %s", id)
					}
				}
				if side != "unrelated" {
					if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
						var body []byte
						var eventID string
						if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind=? AND record_id=?`, link.kind, link.recordID).Scan(&body, &eventID); err != nil {
							return err
						}
						event, found, err := eventByID(t.Context(), tx, eventID)
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("missing %s event", link.recordID)
						}
						payload, _, err := events.AdmittedProjection(event)
						if err != nil {
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
						switch link.field {
						case "member_agent_ids":
							value[link.field] = []string{link.target}
						case "agent_config.blueprint_id", "agent_config.profile_id":
							config, ok := value["agent_config"].(map[string]any)
							if !ok {
								return fmt.Errorf("Task fixture lacks pinned Agent configuration")
							}
							if link.field == "agent_config.blueprint_id" {
								config["blueprint_id"] = link.target
							} else {
								config["profile_id"] = link.target
							}
						default:
							value[link.field] = link.target
						}
						record.Value, err = json.Marshal(value)
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
							if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, eventBody, eventID); err != nil {
								return err
							}
						}
						if side != "event" {
							if _, err := tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, eventID); err != nil {
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
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = Open(path)
				if err != nil {
					t.Fatal(err)
				}
				fullErr := incomingRosterFullValidation(t, store)
				if side == "event" || side == "both" {
					if fullErr == nil {
						t.Fatal("full recovery accepted invalid event relationship")
					}
				} else if fullErr != nil {
					t.Fatalf("unaltered event history: %v", fullErr)
				}
				snapshot, incidentErr := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if side == "unrelated" {
					if incidentErr != nil {
						t.Fatalf("valid unrelated roster: %v", incidentErr)
					}
					return
				}
				if incidentErr == nil {
					t.Fatal("incident accepted incoming invalid roster relationship")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("partial incident snapshot")
				}
			})
		}
	}
}

func TestIncidentIncomingRosterGrowth(t *testing.T) {
	for _, count := range []int{4, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTaskProjectionParents(t, t.Context(), store, "org-1", "selected", "work-1")
			selected, config := appendTaskAssignmentAgent(t, t.Context(), store, "org-1", "selected", false)
			appendPendingAgentExecutionTask(t, t.Context(), store, "selected", "task-selected", selected, config)
			for i := range count {
				agent := selected
				agent.ID = core.ID(fmt.Sprintf("agent-shared-%d", i))
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "AGENT_CREATED", SourceActorID: "runtime", CorrelationID: "roster-shared"}, ProjectionKind: "agent", RecordID: string(agent.ID), Version: 1, Value: agent}); err != nil {
					t.Fatal(err)
				}
				team := core.Team{ID: core.ID(fmt.Sprintf("team-shared-%d", i)), OrganizationID: "org-1", Name: "Shared team", MemberAgentIDs: []core.ID{selected.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				appendIncomingRosterTeam(t, store, "org-1", "roster-shared", team)
			}
			check := func(unrelated int) {
				t.Helper()
				started := time.Now()
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
				if err != nil {
					t.Fatal(err)
				}
				sharedAgents, sharedTeams := 0, 0
				for _, event := range snapshot.DependencyEvents {
					if event.OrganizationID != "org-1" {
						t.Fatal("unrelated tenant selected")
					}
					payload, present, err := events.AdmittedProjection(event)
					if err != nil {
						t.Fatal(err)
					}
					if !present {
						continue
					}
					switch payload.Projection.ProjectionKind {
					case "agent":
						if payload.Projection.RecordID != string(selected.ID) {
							sharedAgents++
						}
					case "team":
						sharedTeams++
					}
				}
				if sharedAgents != count || sharedTeams != count {
					t.Fatalf("selected shared Agents=%d Teams=%d, want %d each", sharedAgents, sharedTeams, count)
				}
				t.Logf("shared=%d unrelated=%d read=%s", count, unrelated, time.Since(started))
			}
			check(0)
			foreign, _ := appendTaskAssignmentAgent(t, t.Context(), store, "org-2", "growth-foreign", true)
			for i := range 64 {
				team := core.Team{ID: core.ID(fmt.Sprintf("team-foreign-%d", i)), OrganizationID: "org-2", Name: "Foreign team", MemberAgentIDs: []core.ID{foreign.ID}, Status: "ACTIVE", CreatedAt: time.Now().UTC()}
				appendIncomingRosterTeam(t, store, "org-2", "roster-foreign", team)
			}
			check(64)
		})
	}
}

func appendIncomingRosterTeam(t *testing.T, store *SQLite, org, correlation string, team core.Team) {
	t.Helper()
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team}); err != nil {
		t.Fatal(err)
	}
}

func appendIncomingRosterTeamTask(t *testing.T, store *SQLite, org, correlation, work, taskID string, teamID core.ID) {
	t.Helper()
	task := core.Task{ID: core.ID(taskID), WorkID: core.ID(work), Description: "bounded Team work", ExecutionKind: core.ExecutionTeam, ModelInferencePolicy: core.InferenceForbidden, AssigneeType: "TEAM", AssigneeID: teamID, TaskContractVersion: "1", Status: core.TaskPending}
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: taskID, CorrelationID: correlation}, ProjectionKind: "task", RecordID: taskID, Version: 1, Value: task}); err != nil {
		t.Fatal(err)
	}
}

func incomingRosterFullValidation(t *testing.T, store *SQLite) error {
	t.Helper()
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		return err
	}
	graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
	if err != nil {
		return err
	}
	return core.ValidateDurableGraph(graph)
}
