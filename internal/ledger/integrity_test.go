package ledger

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestEventIntegrityChainsOrdinaryAndProjectionEvents(t *testing.T) {
	ctx := context.Background()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ordinary, err := store.Append(ctx, events.TrustedDraft{
		OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime",
		CorrelationID: "audit-1", Payload: map[string]string{"result": "recorded"},
	})
	if err != nil {
		t.Fatal(err)
	}
	organization := core.Organization{ID: "org-1", Name: "Organization", PolicyVersion: "policy-v1", CreatedAt: time.Now().UTC()}
	projection, err := store.AppendProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: "org-1", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime",
			CorrelationID: "setup-1",
		},
		ProjectionKind: "organization", RecordID: "org-1", Version: 1, Value: organization,
	})
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.Integrity(ctx)
	if err != nil || head.Algorithm != EventIntegrityAlgorithm || head.EventCount != 2 || head.Sequence != projection.Sequence || head.EventID != projection.EventID || head.SHA256 == "" || ordinary.Sequence != 1 {
		t.Fatalf("event integrity head=%+v ordinary=%+v err=%v", head, ordinary, err)
	}
	var firstHash, secondPrevious string
	if err := store.db.QueryRowContext(ctx, `SELECT event_hash FROM event_integrity WHERE sequence=1`).Scan(&firstHash); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(ctx, `SELECT previous_hash FROM event_integrity WHERE sequence=2`).Scan(&secondPrevious); err != nil {
		t.Fatal(err)
	}
	if firstHash == "" || secondPrevious != firstHash {
		t.Fatalf("event integrity link first=%q previous=%q", firstHash, secondPrevious)
	}
}

func TestEventIntegrityRejectsStoredEventMutationDeletionAndReordering(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate string
		want   string
	}{
		{name: "payload mutation", mutate: `UPDATE events SET payload='{"changed":true}' WHERE sequence=1`, want: "hash does not match"},
		{name: "authorization mutation", mutate: `UPDATE events SET authorization_refs='["forged"]' WHERE sequence=1`, want: "hash does not match"},
		{name: "actor mutation", mutate: `UPDATE events SET source_actor_id='forged' WHERE sequence=1`, want: "hash does not match"},
		{name: "event deletion", mutate: `DELETE FROM events WHERE sequence=1`, want: "sequence 2 is not contiguous"},
		{name: "integrity deletion", mutate: `DELETE FROM event_integrity WHERE sequence=1`, want: "lacks its exact integrity record"},
		{name: "integrity rewrite", mutate: `UPDATE event_integrity SET event_hash=lower(hex(randomblob(32))) WHERE sequence=1`, want: "hash does not match"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			for index := 1; index <= 2; index++ {
				if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "audit", Payload: map[string]int{"index": index}}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.db.ExecContext(ctx, test.mutate); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Integrity(ctx); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("integrity error=%v want %q", err, test.want)
			}
		})
	}
}

func TestEventIntegrityRejectsUnsealedAppendAndStartupTampering(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/agentos.db"
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", Payload: map[string]string{"state": "sealed"}}); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO events(event_id,organization_id,event_type,source_actor_id,authorization_refs,artifact_refs,payload,created_at,schema_version) VALUES('unsealed','org-1','AUDIT_NOTE','runtime','[]','[]','{}','2026-08-25T00:00:00Z',?)`, events.SchemaVersion); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil || !strings.Contains(err.Error(), "lacks its exact integrity record") {
		t.Fatalf("startup integrity error=%v", err)
	}
}

func TestVerifiedReplayEventsUsesOneTenantScopedIntegritySnapshot(t *testing.T) {
	ctx := t.Context()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, draft := range []events.TrustedDraft{
		{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1", Payload: map[string]int{"step": 1}},
		{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1", Payload: map[string]int{"step": 2}},
		{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-2", Payload: map[string]int{"step": 3}},
		{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1", Payload: map[string]int{"step": 4}},
	} {
		if _, err := store.Append(ctx, draft); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := store.VerifiedReplayEvents(ctx, "org-1", "work-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.OrganizationID != "org-1" || snapshot.CorrelationID != "work-1" || snapshot.Algorithm != EventIntegrityAlgorithm || snapshot.LedgerEvents != 4 || snapshot.LedgerSequence != 4 || snapshot.LedgerSHA256 == "" || len(snapshot.Events) != 2 || snapshot.Events[0].Sequence != 1 || snapshot.Events[1].Sequence != 4 {
		t.Fatalf("verified replay snapshot=%+v", snapshot)
	}
	if _, err := store.VerifiedReplayEvents(ctx, "org-1", "work-1", 1); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unbounded replay error=%v", err)
	}
	missing, err := store.VerifiedReplayEvents(ctx, "org-1", "missing", 2)
	if err != nil || len(missing.Events) != 0 || missing.LedgerSHA256 != snapshot.LedgerSHA256 {
		t.Fatalf("missing replay snapshot=%+v err=%v", missing, err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload='{"step":99}' WHERE sequence=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedReplayEvents(ctx, "org-1", "work-1", 2); err == nil || !strings.Contains(err.Error(), "integrity hash does not match") {
		t.Fatalf("tampered replay error=%v", err)
	}
}

func TestVerifiedOrganizationEventsUsesOneBoundedTenantIntegritySnapshot(t *testing.T) {
	ctx := t.Context()
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, draft := range []events.TrustedDraft{
		{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-1", Payload: map[string]int{"step": 1}},
		{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-other", Payload: map[string]int{"step": 2}},
		{OrganizationID: "org-1", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "work-2", Payload: map[string]int{"step": 3}},
	} {
		if _, err := store.Append(ctx, draft); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := store.VerifiedOrganizationEvents(ctx, "org-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.OrganizationID != "org-1" || snapshot.CorrelationID != "" || snapshot.Algorithm != EventIntegrityAlgorithm || snapshot.LedgerEvents != 3 || snapshot.LedgerSequence != 3 || snapshot.LedgerSHA256 == "" || len(snapshot.Events) != 2 || snapshot.Events[0].Sequence != 1 || snapshot.Events[1].Sequence != 3 {
		t.Fatalf("verified organization snapshot=%+v", snapshot)
	}
	if _, err := store.VerifiedOrganizationEvents(ctx, "org-1", 1); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unbounded organization snapshot error=%v", err)
	}
	if _, err := store.verifiedEventSnapshot(ctx, "org-1", "", 2, 2, 1); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("oversized organization snapshot error=%v", err)
	}
	if _, err := store.VerifiedOrganizationEvents(ctx, "", 2); err == nil {
		t.Fatal("tenantless organization snapshot was accepted")
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE events SET payload='{"step":99}' WHERE sequence=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedOrganizationEvents(ctx, "org-1", 2); err == nil || !strings.Contains(err.Error(), "integrity hash does not match") {
		t.Fatalf("tampered organization snapshot error=%v", err)
	}
}
