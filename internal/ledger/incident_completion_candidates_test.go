package ledger_test

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
)

func TestIncidentCompletionCandidates(t *testing.T) {
	for _, kind := range []string{"COMPLETION_VERIFIED", "RESULT_PUBLISHED", "CANDIDATE_COMPLETE"} {
		modes := []string{"foreign", "other-task", "other-correlation", "after-window"}
		if kind == "RESULT_PUBLISHED" || kind == "CANDIDATE_COMPLETE" {
			modes = append(modes, "before-window")
		}
		for _, mode := range modes {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "completion-candidates.db")
				store, err := ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				result, err := app.New(events.NewGateway(store)).Submit(t.Context(), app.Submit{RequestID: "selected-completion", OrganizationID: "org-1", Statement: "echo selected", Kind: core.ExecutionDeterministic})
				if err != nil {
					t.Fatal(err)
				}
				var source events.Event
				for _, event := range result.Events {
					if event.EventType == kind {
						source = event
					}
				}
				if source.EventID == "" || source.TaskID == "" {
					t.Fatal("healthy writer did not admit completion candidate")
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", source.CorrelationID, 256); err != nil {
					t.Fatalf("healthy incident: %v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
					t.Fatalf("healthy full recovery: %v", err)
				}
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				organization, task, correlation := "foreign-org", source.TaskID, source.CorrelationID
				if mode == "other-task" {
					task = "unrelated-task"
				}
				if mode == "other-correlation" {
					correlation = "unrelated-correlation"
				}
				insertion := source.Sequence + 1
				boundary := ""
				if mode == "after-window" {
					boundary = "COMPLETION_VERIFIED"
					if kind == "COMPLETION_VERIFIED" {
						boundary = "TASK_VERIFIED_COMPLETE"
					}
				}
				if mode == "before-window" {
					boundary = "RESULT_PUBLISHED"
					if kind == "RESULT_PUBLISHED" {
						boundary = "TOOL_OUTCOME_RECORDED"
					}
				}
				if boundary != "" {
					insertion = 0
					for _, event := range result.Events {
						if event.EventType == boundary {
							insertion = event.Sequence
							if mode == "after-window" {
								insertion++
							}
						}
					}
					if insertion == 0 {
						t.Fatal("writer fixture missing temporal boundary")
					}
				}
				ledger.InsertCompletionCandidateForTest(t, store, source, insertion, organization, task, correlation)
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				graph, err := events.ValidateProjectionHistory(stream, nil, nil, nil)
				ownerErr := err
				if ownerErr == nil {
					ownerErr = events.ValidateProjectionCompletions(graph, stream, nil)
				}
				snapshot, incidentErr := store.VerifiedIncidentEvents(t.Context(), "org-1", source.CorrelationID, 256)
				if mode != "foreign" {
					if ownerErr != nil || incidentErr != nil {
						t.Fatalf("unrelated candidate changed selected completion: owner=%v incident=%v", ownerErr, incidentErr)
					}
					for _, event := range snapshot.DependencyEvents {
						if event.EventID == "extra-completion-candidate" {
							if mode == "other-task" || mode == "other-correlation" {
								t.Fatal("unrelated candidate selected")
							}
						}
					}
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
						t.Fatalf("unrelated candidate changed full recovery: %v", err)
					}
					return
				}
				if ownerErr == nil {
					t.Fatal("full completion owner accepted foreign candidate in selected task window")
				}
				want := map[string]string{
					"COMPLETION_VERIFIED": "verified Task result completion verification is invalid",
					"RESULT_PUBLISHED":    "verified Task result publication is invalid",
					"CANDIDATE_COMPLETE":  "verified Task result completion candidate is invalid",
				}[kind]
				if !strings.Contains(ownerErr.Error(), want) {
					t.Fatalf("unexpected completion owner rejection: %v", ownerErr)
				}
				t.Logf("full completion owner rejected candidate: %v", ownerErr)
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := ledgerrecovery.Verify(t.Context(), path); err == nil {
					t.Fatal("full recovery accepted foreign completion candidate")
				} else if !strings.Contains(err.Error(), want) {
					t.Fatalf("full recovery rejected unrelated damage: %v", err)
				} else {
					t.Logf("full recovery rejection: %v", err)
				}
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, incidentErr = store.VerifiedIncidentEvents(t.Context(), "org-1", source.CorrelationID, 256)
				if incidentErr == nil {
					t.Fatal("incident omitted foreign completion candidate with selected Task and correlation")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("failed incident returned partial evidence")
				}
			})
		}
	}
}

func TestIncidentCompletionAnchors(t *testing.T) {
	for _, anchor := range []struct{ kind, candidate, want string }{
		{"TOOL_OUTCOME_RECORDED", "RESULT_PUBLISHED", "task completion outcome evidence is invalid"},
		{"COMPLETION_VERIFIED", "RESULT_PUBLISHED", "task completion lacks its exact verification decision"},
		{"RESULT_PUBLISHED", "CANDIDATE_COMPLETE", "verified Task result lacks its exact publication"},
	} {
		for _, damage := range []string{"missing", "invalid"} {
			t.Run(anchor.kind+"/"+damage, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "completion-anchors.db")
				store, err := ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				result, err := app.New(events.NewGateway(store)).Submit(t.Context(), app.Submit{RequestID: "selected-anchor", OrganizationID: "org-1", Statement: "echo selected", Kind: core.ExecutionDeterministic})
				if err != nil {
					t.Fatal(err)
				}
				var canonical, candidate events.Event
				for _, event := range result.Events {
					if event.EventType == anchor.kind {
						canonical = event
					}
					if event.EventType == anchor.candidate {
						candidate = event
					}
				}
				if canonical.EventID == "" || candidate.EventID == "" {
					t.Fatal("actual writer missing canonical completion evidence")
				}
				if _, err := store.VerifiedIncidentEvents(t.Context(), "org-1", canonical.CorrelationID, 256); err != nil {
					t.Fatalf("healthy incident: %v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := ledgerrecovery.Verify(t.Context(), path); err != nil {
					t.Fatalf("healthy full recovery: %v", err)
				}
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				ledger.InsertCompletionCandidateForTest(t, store, candidate, candidate.Sequence+1, "foreign-org", candidate.TaskID, candidate.CorrelationID)
				ledger.DamageCompletionAnchorForTest(t, store, canonical.EventID, damage)
				stream, err := store.Events(t.Context(), "")
				if err != nil {
					t.Fatal(err)
				}
				retained := false
				for _, event := range stream {
					if event.EventID == "extra-completion-candidate" {
						retained = true
					}
				}
				if !retained {
					t.Fatal("foreign completion candidate disappeared with canonical anchor")
				}
				graph, ownerErr := events.ValidateProjectionHistory(stream, nil, nil, nil)
				if ownerErr == nil {
					ownerErr = events.ValidateProjectionCompletions(graph, stream, nil)
				}
				if ownerErr == nil || !strings.Contains(ownerErr.Error(), anchor.want) {
					t.Fatalf("full owner did not reject damaged canonical evidence: %v", ownerErr)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := ledgerrecovery.Verify(t.Context(), path); err == nil || !strings.Contains(err.Error(), anchor.want) {
					t.Fatalf("full recovery did not reject canonical anchor: %v", err)
				}
				store, err = ledger.Open(path)
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", canonical.CorrelationID, 256)
				if err == nil {
					t.Fatal("incident accepted terminal completion without valid canonical evidence")
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("invalid canonical completion evidence returned partial snapshot")
				}
			})
		}
	}
}
