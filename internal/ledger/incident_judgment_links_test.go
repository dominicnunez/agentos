package ledger

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentIncomingJudgmentAuthority(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "judgment.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.AppendProjection(t.Context(), events.ProjectionDraft{Event: events.TrustedDraft{OrganizationID: "org-2", EventType: "ORGANIZATION_CREATED", SourceActorID: "runtime", CorrelationID: "setup-2"}, ProjectionKind: "organization", RecordID: "org-2", Version: 1, Value: core.Organization{ID: "org-2", Name: "Other", PolicyVersion: "v1", CreatedAt: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	selected, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-2", EventType: "AUDIT_NOTE", SourceActorID: "runtime", CorrelationID: "selected", Payload: map[string]string{"summary": "Independent evidence"}})
	if err != nil {
		t.Fatal(err)
	}
	correlation := appendDerivedIncidentChain(t, store, 1)
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", correlation, 256)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256); err != nil {
		t.Fatal(err)
	}
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var judgment events.Event
	for _, event := range stream {
		if event.EventType == "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED" {
			judgment = event
		}
	}
	if judgment.EventID == "" {
		t.Fatal("missing admitted judgment")
	}
	changeIncidentStopRef(t, store, judgment, "capability_check_event_id", selected.EventID)
	stream, err = store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := events.ResolveAuthorityAdmissions(stream, baseline.AuthorityRecords)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := events.ValidateProjectionHistory(stream, nil, leases, freezes); err == nil {
		t.Fatal("full recovery accepted invalid judgment authority")
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-2", "selected", 256)
	if err == nil {
		t.Fatal("incident omitted foreign judgment referring to selected event")
	}
	if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
		t.Fatal("failed incident returned partial evidence")
	}
}
