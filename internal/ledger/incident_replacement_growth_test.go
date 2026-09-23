package ledger

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentReplacementGrowth(t *testing.T) {
	baseline := map[string]int64{}
	for _, depth := range []int{8, 32} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "replacement.db")
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if store != nil {
					_ = store.Close()
				}
			})
			appendIncidentReplacements(t, store, depth)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.db.Close(); err != nil {
				t.Fatal(err)
			}
			var count atomic.Int64
			store.db = sql.OpenDB(&incidentCountConnector{path: path, count: &count, inner: store.db.Driver()})
			store.db.SetMaxOpenConns(1)
			for _, root := range []struct {
				name  string
				index int
			}{{"first", 0}, {"last", depth - 1}} {
				t.Run(root.name, func(t *testing.T) {
					for read := range 2 {
						count.Store(0)
						started := time.Now()
						snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", fmt.Sprintf("replacement-%d", root.index), 256)
						if err != nil {
							t.Fatalf("valid replacement lineage: %v", err)
						}
						queries := count.Load()
						t.Logf("depth=%d root=%s read=%d statements=%d elapsed=%s", depth, root.name, read, queries, time.Since(started))
						counts := map[string]int{}
						ids := map[string]bool{}
						for _, stream := range [][]events.Event{snapshot.Work.Events, snapshot.DependencyEvents} {
							for _, event := range stream {
								if ids[event.EventID] {
									t.Fatalf("duplicate evidence %s", event.EventID)
								}
								ids[event.EventID] = true
								counts[event.EventType]++
							}
						}
						for eventType, want := range map[string]int{"INTENT_CREATED": depth, "WORK_CREATED": depth, "WORK_FAILED": depth, "INTAKE_MESSAGE_RECORDED": depth - 1, "INTENT_DRAFTED": depth - 1, "INTENT_CONFIRMED": depth - 1} {
							if counts[eventType] != want {
								t.Fatalf("%s evidence=%d want=%d", eventType, counts[eventType], want)
							}
						}
						if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
							t.Fatalf("returned evidence failed shared validation: %v", err)
						}
						if depth == 8 && read == 0 {
							baseline[root.name] = queries
						}
						if queries > baseline[root.name]+12 {
							t.Errorf("replacement depth drives SQL round trips: small=%d actual=%d", baseline[root.name], queries)
						}
					}
				})
			}
		})
	}
}

func appendIncidentReplacements(t *testing.T, store *SQLite, depth int) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC()
	if _, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup"},
		ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "v1", CreatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	var predecessor core.ID
	for i := range depth {
		correlation := fmt.Sprintf("replacement-%d", i)
		intentID := core.ID("intent-" + correlation)
		objective := fmt.Sprintf("echo replacement %d", i)
		intent := core.Intent{ID: intentID, OrganizationID: "org-1", OriginalInstruction: objective, NormalizedObjective: objective, SourcePrincipalID: "runtime", SourcePrincipalKind: core.PrincipalRuntime, SourceChannel: "INTERNAL", AcceptedFingerprint: "internal-root", CreatedAt: now}
		if predecessor != "" {
			draft := appendReviewedReplacementIntent(t, ctx, store, "org-1", correlation, string(intentID), predecessor, objective, now)
			confirmation := replacementConfirmation(draft, "confirmation-"+correlation, predecessor)
			if _, err := store.AppendIntentConfirmation(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CONFIRMED", SourceActorID: "user-1", TaskID: "task-" + correlation, CorrelationID: correlation, Payload: confirmation}, "", predecessor); err != nil {
				t.Fatal(err)
			}
			intent.ReplacesWorkID = predecessor
			intent.OriginalInstruction = "Replace " + string(predecessor) + " with " + objective
			intent.SourcePrincipalID, intent.SourcePrincipalKind, intent.SourceChannel = "user-1", core.PrincipalHuman, "HUMAN_DIRECT"
			intent.SourceMessageID = "source-" + correlation
			intent.ExternalRequestID = "request-" + correlation
			intent.AcceptedFingerprint = draft.Fingerprint
		}
		work := core.Work{ID: core.ID("work-" + correlation), IntentID: intentID, ReplacesWorkID: predecessor, Objective: objective, Status: core.WorkActive, CreatedAt: now}
		if _, err := store.AppendProjections(ctx, []events.ProjectionDraft{
			{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "INTENT_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "intent", RecordID: string(intentID), Version: 1, Value: intent},
			{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_CREATED", SourceActorID: "runtime", CorrelationID: correlation}, ProjectionKind: "work", RecordID: string(work.ID), Version: 1, Value: work},
		}); err != nil {
			t.Fatal(err)
		}
		work.Status = core.WorkFailed
		if _, err := store.AppendProjection(ctx, events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-1", EventType: "WORK_FAILED", SourceActorID: "runtime", CorrelationID: correlation, Payload: map[string]string{"reason": "bounded failure"}}, ProjectionKind: "work", RecordID: string(work.ID), Version: 2, Value: work}); err != nil {
			t.Fatal(err)
		}
		predecessor = work.ID
	}
}
