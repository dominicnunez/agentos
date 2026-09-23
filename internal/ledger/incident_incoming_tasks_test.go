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

func TestIncidentIncomingTaskLinks(t *testing.T) {
	for _, org := range []string{"org-1", "org-2"} {
		for _, field := range []string{"unrelated", "work_id", "lab_work", "parent_id", "depends_on"} {
			for _, side := range []string{"both", "event", "record"} {
				t.Run(org+"/"+field+"/"+side, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "incoming.db")
					store, err := Open(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					appendTaskProjectionParents(t, t.Context(), store, "org-1", "selected", "selected-work")
					if org == "org-2" {
						appendTaskProjectionParents(t, t.Context(), store, org, "other", "other-work")
					} else {
						_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: "other"}, ProjectionKind: "intent", RecordID: "intent-other-work", Version: 1, Value: core.Intent{ID: "intent-other-work", OrganizationID: core.ID(org), OriginalInstruction: "test task", NormalizedObjective: "test task", CreatedAt: time.Now().UTC()}})
						if err != nil {
							t.Fatal(err)
						}
						_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: "other"}, ProjectionKind: "work", RecordID: "other-work", Version: 1, Value: core.Work{ID: "other-work", IntentID: "intent-other-work", Objective: "test task", Status: core.WorkActive, CreatedAt: time.Now().UTC()}})
						if err != nil {
							t.Fatal(err)
						}
					}
					for _, id := range []string{"selected-task", "other-task"} {
						work, tenant, correlation := "selected-work", "org-1", "selected"
						if id == "other-task" {
							work, tenant, correlation = "other-work", org, "other"
						}
						if id == "other-task" && field == "lab_work" {
							_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: tenant, EventType: "LAB_EXPERIMENT_STARTED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "lab_experiment", RecordID: id, Version: 1, Value: core.Experiment{ID: core.ID(id), OrganizationID: core.ID(tenant), WorkID: core.ID(work), Objective: "test task", SandboxRef: "sandbox", CapabilityProfileRef: "profile", Budget: core.ExperimentBudget{MaxExecutions: 1, MaxUsageUnits: 1, MaxWallTimeSeconds: 1, AllowedInferencePools: []string{"test"}}, Status: core.ExperimentRunning, TrustLabel: core.ExperimentTrustUnverified, StartedAt: time.Now().UTC()}})
							if err != nil {
								t.Fatal(err)
							}
							continue
						}
						_, err = store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: tenant, EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: id, CorrelationID: correlation}, ProjectionKind: "task", RecordID: id, Version: 1, Value: core.Task{ID: core.ID(id), WorkID: core.ID(work), Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}})
						if err != nil {
							t.Fatal(err)
						}
					}
					if _, err = store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256); err != nil {
						t.Fatal(err)
					}
					if field != "unrelated" {
						err = store.withTx(t.Context(), func(tx *sql.Tx) error {
							var body []byte
							var id string
							if err := tx.QueryRowContext(t.Context(), `SELECT body,admission_event_id FROM records WHERE record_id='other-task'`).Scan(&body, &id); err != nil {
								return err
							}
							event, found, err := eventByID(t.Context(), tx, id)
							if err != nil {
								return err
							}
							if !found {
								return fmt.Errorf("missing event")
							}
							payload, _, err := events.AdmittedProjection(event)
							if err != nil {
								return err
							}
							var record events.ProjectionRecord
							if err = json.Unmarshal(body, &record); err != nil {
								return err
							}
							var value map[string]any
							if err = json.Unmarshal(record.Value, &value); err != nil {
								return err
							}
							switch field {
							case "work_id", "lab_work":
								value["work_id"] = "selected-work"
							case "parent_id":
								value[field] = "selected-task"
							case "depends_on":
								value[field] = []string{"selected-task"}
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
								if _, err = tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, eventBody, id); err != nil {
									return err
								}
							}
							if side != "event" {
								if _, err = tx.ExecContext(t.Context(), `UPDATE records SET body=?,admission_fingerprint=? WHERE admission_event_id=?`, recordBody, sealed.Admission.Fingerprint, id); err != nil {
									return err
								}
							}
							if _, err = tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
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
					if side == "both" && field != "unrelated" {
						stream, err := store.Events(t.Context(), "")
						if err != nil {
							t.Fatal(err)
						}
						graph, historyErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
						if historyErr == nil {
							historyErr = core.ValidateDurableGraph(graph)
						}
						if historyErr == nil {
							t.Fatal("full recovery accepted invalid incoming relationship")
						}
					}
					snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "selected", 256)
					invalid := field != "unrelated"
					if invalid {
						if err == nil {
							t.Fatal("accepted incoming invalid Task link")
						}
						if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
							t.Fatal("partial snapshot")
						}
					} else if err != nil {
						t.Fatalf("valid unrelated/same Work link: %v", err)
					}
				})
			}
		}
	}
}

func TestIncidentIncomingTaskGrowth(t *testing.T) {
	for _, count := range []int{4, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			for _, org := range []string{"selected", "other"} {
				appendTaskProjectionParents(t, t.Context(), store, org, org, org+"-work")
			}
			addTasks := func(org string, n int) {
				drafts := make([]events.ProjectionDraft, 0, n)
				for i := range n {
					id := fmt.Sprintf("%s-task-%d", org, i)
					task := core.Task{ID: core.ID(id), WorkID: core.ID(org + "-work"), Description: "bounded task", ExecutionKind: core.ExecutionDeterministic, ModelInferencePolicy: core.InferenceForbidden, TaskContractVersion: "1", Status: core.TaskPending}
					if i > 0 {
						task.ParentID = core.ID(fmt.Sprintf("%s-task-%d", org, i-1))
						task.DependsOn = []core.ID{task.ParentID}
					}
					drafts = append(drafts, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: org, EventType: "TASK_CREATED", SourceActorID: "runtime", TaskID: id, CorrelationID: org}, ProjectionKind: "task", RecordID: id, Version: 1, Value: task})
				}
				if _, err := store.AppendProjections(t.Context(), drafts); err != nil {
					t.Fatal(err)
				}
			}
			addTasks("selected", count)
			for _, unrelated := range []int{0, 64} {
				if unrelated > 0 {
					addTasks("other", unrelated)
				}
				started := time.Now()
				for range 2 {
					snapshot, err := store.VerifiedIncidentEvents(t.Context(), "selected", "selected", 256)
					if err != nil {
						t.Fatal(err)
					}
					tasks := 0
					for _, stream := range [][]events.Event{snapshot.Work.Events, snapshot.DependencyEvents, snapshot.RelatedEvents} {
						for _, event := range stream {
							if event.OrganizationID != "selected" {
								t.Fatal("unrelated tenant selected")
							}
							if event.EventType == "TASK_CREATED" {
								tasks++
							}
						}
					}
					if tasks != count {
						t.Fatalf("selected %d Tasks, want %d", tasks, count)
					}
				}
				t.Logf("selected=%d unrelated Tasks=%d two reads=%s", count, unrelated, time.Since(started))
			}
		})
	}
}
