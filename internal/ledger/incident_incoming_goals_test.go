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

func TestIncidentIncomingGoalLinks(t *testing.T) {
	parallelIncidentTest(t)
	for _, side := range []string{"unrelated", "event", "record", "both"} {
		t.Run(side, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "goals.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendPrivateInferenceGoal(t, store)
			now := time.Now().UTC()
			appendTestMission(t, t.Context(), store, "org-2", "mission-2", now)
			goal := core.Goal{ID: "foreign-goal", OrganizationID: "org-2", MissionID: "mission-2", Objective: "test", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "test", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}
			_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: "foreign-goal"}, ProjectionKind: "goal", RecordID: "foreign-goal", Version: 1, Value: goal})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256); err != nil {
				t.Fatalf("valid baseline: %v", err)
			}
			if side != "unrelated" {
				err = store.withTx(t.Context(), func(tx *sql.Tx) error {
					var body []byte
					var id string
					if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE kind='goal' AND record_id='foreign-goal'`).Scan(&body, &id); err != nil {
						return err
					}
					event, found, err := eventByID(t.Context(), tx, id)
					if err != nil {
						return err
					}
					if !found {
						return fmt.Errorf("missing Goal event")
					}
					payload, _, err := events.AdmittedProjection(event)
					if err != nil {
						return err
					}
					var record events.ProjectionRecord
					if err := json.Unmarshal(body, &record); err != nil {
						return err
					}
					goal.MissionID = "mission-1"
					record.Value, err = json.Marshal(goal)
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
			if err = store.Close(); err != nil {
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
					t.Fatal("full history accepted cross-tenant Mission link")
				}
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
			if side == "unrelated" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("incident accepted incoming cross-tenant Mission link")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("partial snapshot")
			}
		})
	}
}

func TestIncidentIncomingGoalGrowth(t *testing.T) {
	parallelIncidentTest(t)
	for _, count := range []int{4, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendPrivateInferenceGoal(t, store)
			now := time.Now().UTC()
			appendTestMission(t, t.Context(), store, "org-2", "mission-2", now)
			addGoals := func(org, mission string, n int) {
				var drafts []events.ProjectionDraft
				for i := range n {
					id := fmt.Sprintf("%s-goal-%d", org, i)
					goal := core.Goal{ID: core.ID(id), OrganizationID: core.ID(org), MissionID: core.ID(mission), Objective: "test", Mode: core.GoalTarget, SuccessCriteria: []core.IntentValue{{Value: "test", Origin: "RUNTIME_DEFAULT"}}, Status: core.GoalActive, CreatedAt: now}
					drafts = append(drafts, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "GOAL_CREATED", SourceActorID: "runtime", CorrelationID: id}, ProjectionKind: "goal", RecordID: id, Version: 1, Value: goal})
				}
				if _, err := store.AppendProjections(t.Context(), drafts); err != nil {
					t.Fatal(err)
				}
			}
			addGoals("org-1", "mission-1", count-1)
			for _, unrelated := range []int{0, 64} {
				if unrelated > 0 {
					addGoals("org-2", "mission-2", unrelated)
				}
				start := time.Now()
				for range 2 {
					snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "model-stop", 256)
					if err != nil {
						t.Fatal(err)
					}
					goals := 0
					for _, event := range snapshot.DependencyEvents {
						if event.OrganizationID != "org-1" {
							t.Fatal("unrelated tenant selected")
						}
						if event.EventType == "GOAL_CREATED" {
							goals++
						}
					}
					if goals != count {
						t.Fatalf("selected %d Goals, want %d", goals, count)
					}
				}
				t.Logf("selected Goals=%d unrelated Goals=%d two reads=%s", count, unrelated, time.Since(start))
			}
		})
	}
}
