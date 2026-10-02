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

func TestIncidentOrphanRecordCorrelationClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, test := range []struct {
		name                       string
		private, selected, escaped bool
	}{
		{name: "public/unrelated-control"},
		{name: "public/later-selected-claim", selected: true},
		{name: "public/escaped-selected-claim", selected: true, escaped: true},
		{name: "private/unrelated-control", private: true},
		{name: "private/later-selected-claim", private: true, selected: true},
		{name: "private/escaped-selected-claim", private: true, selected: true, escaped: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendTestMission(t, t.Context(), store, "org-correlation", "selected-mission", time.Now().UTC())
			correlation := "selected-mission"
			var privateEvent events.Event
			if test.private {
				// The Mission selects its Goal and the Goal's reviewed Work and
				// Task through incoming owned links. Work/Task own their private
				// correlation; selecting a Goal alone does not own its correlation.
				correlation = "private-work"
				now := time.Now().UTC()
				goal := core.Goal{ID: "private-goal", OrganizationID: "org-correlation", MissionID: "selected-mission", Objective: "test", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "test", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-correlation", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "private-goal"}, ProjectionKind: "goal", RecordID: string(goal.ID), Version: 1, Value: goal}); err != nil {
					t.Fatal(err)
				}
				reviewed := appendReviewedGoalIntent(t, t.Context(), store, "org-correlation", correlation, "intent-"+correlation, string(goal.ID), "test", core.ExecutionDeterministic, now)
				confirmation := events.IntentConfirmedPayload{IntentID: string(reviewed.ID), GoalID: string(goal.ID), Version: 1, Fingerprint: reviewed.Fingerprint, ConfirmingActorID: "user-1", ConfirmingActorKind: string(core.PrincipalHuman), SourceChannel: "HUMAN_DIRECT", MessageID: "private-confirmation"}
				if _, err := store.AppendIntentConfirmation(t.Context(), events.TrustedDraft{OrganizationID: "org-correlation", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-private-work", Payload: confirmation, CorrelationID: correlation}, goal.ID, ""); err != nil {
					t.Fatal(err)
				}
				intent := core.Intent{ID: reviewed.ID, OrganizationID: "org-correlation", GoalID: goal.ID, OriginalInstruction: "test under private-goal", NormalizedObjective: reviewed.Objective, AcceptedFingerprint: reviewed.Fingerprint, SourcePrincipalID: "user-1", SourcePrincipalKind: core.PrincipalHuman, SourceChannel: "HUMAN_DIRECT", SourceMessageID: "source-" + correlation, CreatedAt: now}
				work := core.Work{ID: "private-work-record", IntentID: intent.ID, GoalID: goal.ID, Objective: intent.NormalizedObjective, Status: core.WorkActive, CreatedAt: now}
				task := core.Task{ID: "task-private-work", WorkID: work.ID, Description: "bounded private work", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
				for _, draft := range []events.ProjectionDraft{
					{Event: events.TrustedDraft{OrganizationID: "org-correlation", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "intent", RecordID: string(intent.ID), Version: 1, Value: intent},
					{Event: events.TrustedDraft{OrganizationID: "org-correlation", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
					{Event: events.TrustedDraft{OrganizationID: "org-correlation", EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: string(task.ID), CorrelationID: correlation}, ProjectionKind: "task", RecordID: string(task.ID), Version: 1, Value: task},
				} {
					event, err := store.AppendProjection(t.Context(), draft)
					if err != nil {
						t.Fatal(err)
					}
					if draft.ProjectionKind == "task" {
						privateEvent = event
					}
				}
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-correlation", "selected-mission", 256)
			if err != nil {
				t.Fatalf("valid writer history: %v", err)
			}
			if test.private {
				found := false
				for _, event := range baseline.DependencyEvents {
					found = found || event.EventID == privateEvent.EventID
				}
				if !found {
					t.Fatal("fixture did not select the private Task")
				}
				owner := incidentDependencies{organization: "org-correlation", stream: map[string]events.Event{}, keys: map[incidentKey]bool{}, correlations: map[string]bool{}, reverse: map[incidentKey]bool{}, refs: map[string]bool{}, executions: map[string]bool{}}
				if err := owner.add(privateEvent); err != nil {
					t.Fatalf("private Task correlation owner: %v", err)
				}
				if _, owned := owner.correlations[correlation]; !owned {
					t.Fatal("private Task did not establish its owned correlation")
				}
			}
			var body []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='mission' AND record_id='selected-mission'`).Scan(&body); err != nil {
				t.Fatal(err)
			}
			var record events.ProjectionRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			var mission core.Mission
			if err := json.Unmarshal(record.Value, &mission); err != nil {
				t.Fatal(err)
			}
			mission.ID = "orphan-mission"
			record.RecordID = string(mission.ID)
			record.CorrelationID = "unrelated"
			record.Value, err = json.Marshal(mission)
			if err != nil {
				t.Fatal(err)
			}
			body, err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if test.selected {
				const first = `"correlation_id":"unrelated"`
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its original unrelated correlation")
				}
				key := `"correlation_id"`
				if test.escaped {
					key = `"correlation\u005fid"`
				}
				claim, err := json.Marshal(correlation)
				if err != nil {
					t.Fatal(err)
				}
				body = []byte(strings.Replace(string(body), first, first+`,`+key+`:`+string(claim), 1))
			}
			if test.private {
				for _, event := range append(append([]events.Event(nil), baseline.Work.Events...), baseline.DependencyEvents...) {
					payload, present, err := events.AdmittedProjection(event)
					if err != nil {
						t.Fatal(err)
					}
					if !present {
						continue
					}
					if payload.Projection.ProjectionKind == "mission" && payload.Projection.RecordID == record.RecordID {
						t.Fatal("orphan shares a selected physical or owned Mission identity")
					}
					var linked bool
					query := `WITH source(kind,body) AS (VALUES ('mission',?)) SELECT ` + incidentLinkMatch(true, "source", "?", "?") + ` FROM source`
					if err := store.db.QueryRowContext(t.Context(), query, body, payload.Projection.ProjectionKind, payload.Projection.RecordID).Scan(&linked); err != nil {
						t.Fatal(err)
					}
					if linked {
						t.Fatal("private orphan has an accidental selected identity backstop")
					}
				}
			}
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES('mission','orphan-mission',1,?,?)`, body, mission.CreatedAt.Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			ownerErr := store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind='mission' AND r.record_id='orphan-mission'`)
				return err
			})
			if ownerErr == nil {
				t.Fatal("complete record owner accepted an orphan admission")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-correlation", "selected-mission", 256)
			if !test.selected {
				if err != nil {
					t.Fatalf("unrelated orphan affected selected incident: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("accepted orphan record with a later selected correlation claim: public=%d private=%d owner=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid record returned a partial snapshot")
			}
		})
	}
}

func TestIncidentOrphanRecordOrganizationClaims(t *testing.T) {
	parallelIncidentTest(t)
	for _, mode := range []string{"foreign-control", "canonical-selected", "later-selected"} {
		t.Run(mode, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			now := time.Now().UTC()
			appendTestMission(t, t.Context(), store, "org-correlation", "selected-mission", now)
			team := core.Team{ID: "template-team", OrganizationID: "org-correlation", Name: "Team", Status: "ACTIVE", CreatedAt: now}
			if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-correlation", EventType: "TEAM_CREATED", SourceActorID: "runtime", CorrelationID: "template-team"}, ProjectionKind: "team", RecordID: string(team.ID), Version: 1, Value: team}); err != nil {
				t.Fatalf("Team writer: %v", err)
			}
			if _, err := store.VerifiedIncidentEvents(t.Context(), "org-correlation", "selected-mission", 256); err != nil {
				t.Fatalf("valid writer history: %v", err)
			}
			var body []byte
			if err := store.db.QueryRowContext(t.Context(), `SELECT body FROM records WHERE kind='team' AND record_id='template-team'`).Scan(&body); err != nil {
				t.Fatal(err)
			}
			var record events.ProjectionRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			team.ID = "orphan-team"
			team.OrganizationID = "foreign-org"
			if mode == "canonical-selected" {
				team.OrganizationID = "org-correlation"
			}
			record.RecordID, record.CorrelationID = string(team.ID), "selected-mission"
			record.Value, err = json.Marshal(team)
			if err != nil {
				t.Fatal(err)
			}
			body, err = json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "later-selected" {
				const first = `"organization_id":"foreign-org"`
				if strings.Count(string(body), first) != 1 {
					t.Fatal("fixture lacks its foreign organization claim")
				}
				body = []byte(strings.Replace(string(body), first, first+`,"organization_id":"org-correlation"`, 1))
			}
			if _, err := store.db.ExecContext(t.Context(), `INSERT INTO records(kind,record_id,version,body,created_at) VALUES('team','orphan-team',1,?,?)`, body, now.Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			ownerErr := store.withTx(t.Context(), func(tx *sql.Tx) error {
				_, err := admittedProjectionRecordsBounded(t.Context(), tx, 4<<20, `WHERE r.kind='team' AND r.record_id='orphan-team'`)
				return err
			})
			if ownerErr == nil {
				t.Fatal("complete record owner accepted an orphan Team")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-correlation", "selected-mission", 256)
			if mode == "foreign-control" {
				if err != nil {
					t.Fatalf("foreign orphan affected selected incident: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("accepted selected orphan Team organization: public=%d private=%d owner=%v", len(snapshot.Work.Events), len(snapshot.DependencyEvents), ownerErr)
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("invalid Team returned a partial snapshot")
			}
		})
	}
}
