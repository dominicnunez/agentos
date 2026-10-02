package ledger_test

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestIncidentIncomingAggregateIdentities(t *testing.T) {
	ledger.ParallelIncidentTestForTest(t)
	for _, field := range []string{"work_id", "intent_id", "tasks.task_id", "mission_id"} {
		t.Run(field, func(t *testing.T) {
			store, err := ledger.Open(filepath.Join(t.TempDir(), "aggregate-identities.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			appendAggregateGoal(t, store)
			selected, err := app.New(events.NewGateway(store)).Submit(t.Context(), app.Submit{
				RequestID: "selected-identity", OrganizationID: "org-2", Statement: "echo selected", Kind: core.ExecutionDeterministic,
			})
			if err != nil {
				t.Fatal(err)
			}
			selectedCorrelation := selected.Events[0].CorrelationID
			target := ""
			switch field {
			case "work_id":
				target = string(selected.Work.ID)
			case "intent_id":
				target = string(selected.Intent.ID)
			case "tasks.task_id":
				target = string(selected.Task.ID)
			case "mission_id":
				mission := core.Mission{ID: "selected-mission", OrganizationID: "org-2", Statement: "selected mission", Status: core.MissionActive, CreatedAt: time.Now().UTC()}
				if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
					Event:          events.TrustedDraft{OrganizationID: "org-2", EventType: "MISSION_CREATED", SourceActorID: "runtime", CorrelationID: "selected-mission"},
					ProjectionKind: "mission", RecordID: string(mission.ID), Version: 1, Value: mission,
				}); err != nil {
					t.Fatal(err)
				}
				target, selectedCorrelation = string(mission.ID), "selected-mission"
			}
			if target == "" {
				t.Fatal("selected writer identity is empty")
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
				t.Fatalf("healthy aggregate owner: %v", err)
			}
			incomingType := "WORK_COMPLETION_EVALUATED"
			if field == "mission_id" {
				incomingType = "GOAL_PROGRESS_EVALUATED"
			}
			var incoming events.Event
			for _, event := range stream {
				if event.OrganizationID == "org-1" && event.EventType == incomingType {
					incoming = event
				}
			}
			if incoming.EventID == "" {
				t.Fatal("writer aggregate identity evidence is missing")
			}
			baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-2", selectedCorrelation, 256)
			if err != nil {
				t.Fatalf("independent selected incident: %v", err)
			}
			for _, event := range baseline.DependencyEvents {
				if event.EventID == incoming.EventID {
					t.Fatal("unrelated aggregate selected before identity mutation")
				}
			}
			ledger.ChangeAggregateIdentityForTest(t, store, incoming.EventID, field, target)
			stream, err = store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			graph, err = events.ValidateProjectionHistory(stream, nil, nil, nil)
			if err != nil {
				t.Fatalf("identity mutation invalidated projection structure: %v", err)
			}
			if err := events.ValidateProjectionCompletions(graph, stream, nil); err == nil {
				t.Fatal("full aggregate owner accepted substituted global identity")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", selectedCorrelation, 256)
			if err == nil {
				t.Fatal("incident omitted aggregate claiming selected global identity")
			}
			if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
				t.Fatal("failed incident returned partial evidence")
			}
		})
	}
}
