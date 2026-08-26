package audit

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type reader struct{ events []events.Event }

func (r reader) Events(context.Context, string) ([]events.Event, error) { return r.events, nil }
func TestFindsCompletionWithoutVerification(t *testing.T) {
	f, e := New(reader{[]events.Event{{EventID: "e", Sequence: 1, EventType: "TASK_VERIFIED_COMPLETE", TaskID: "t"}}}).Run(context.Background())
	if e != nil || len(f) != 1 {
		t.Fatalf("findings=%v err=%v", f, e)
	}
}

func TestAuditsActiveKnowledgeStalenessWithoutChoosingDeploymentPolicy(t *testing.T) {
	created := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	verified := created.Add(time.Hour)
	stream := []events.Event{
		{EventID: "source", Sequence: 1, OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", CreatedAt: created, SchemaVersion: events.SchemaVersion},
		{EventID: "validation", Sequence: 2, OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", CreatedAt: verified, SchemaVersion: events.SchemaVersion},
	}
	active := activeKnowledge("knowledge-1", created, verified, []string{"source"}, []string{"validation"})
	stream = append(stream, knowledgeProjectionEvent(t, 3, active))
	now := verified.Add(12 * time.Hour)

	findings, err := New(reader{stream}).RunAt(context.Background(), now)
	if err != nil || len(findings) != 1 || findings[0].Rule != "knowledge_staleness_policy_missing" {
		t.Fatalf("missing policy findings=%+v err=%v", findings, err)
	}
	findings, err = New(reader{stream}).WithKnowledgeVerificationMaxAge(24*time.Hour).RunAt(context.Background(), now)
	if err != nil || len(findings) != 0 {
		t.Fatalf("fresh governed knowledge findings=%+v err=%v", findings, err)
	}
	findings, err = New(reader{stream}).WithKnowledgeVerificationMaxAge(6*time.Hour).RunAt(context.Background(), now)
	if err != nil || len(findings) != 1 || findings[0].Rule != "knowledge_revalidation_due" {
		t.Fatalf("stale knowledge findings=%+v err=%v", findings, err)
	}
}

func TestAuditsActiveKnowledgeAgainstCurrentDerivedLineage(t *testing.T) {
	created := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	verified := created.Add(time.Hour)
	stream := []events.Event{
		{EventID: "source", Sequence: 1, OrganizationID: "org-1", EventType: "RESULT_PUBLISHED", CreatedAt: created, SchemaVersion: events.SchemaVersion},
		{EventID: "validation", Sequence: 2, OrganizationID: "org-1", EventType: "TOOL_OUTCOME_RECORDED", CreatedAt: verified, SchemaVersion: events.SchemaVersion},
	}
	source := activeKnowledge("knowledge-source", created, verified, []string{"source"}, []string{"validation"})
	stream = append(stream, knowledgeProjectionEvent(t, 3, source))
	stale := source
	stale.Version = 3
	stale.Status = core.KnowledgeStale
	stale.SupersedesVersion = intPointer(2)
	stream = append(stream, knowledgeProjectionEvent(t, 4, stale))
	derived := activeKnowledge("knowledge-derived", created, verified, []string{"source"}, []string{"validation"})
	derived.Basis = core.KnowledgeBasisDerived
	derived.DerivedKnowledgeRefs = []core.VersionedRef{{ID: string(source.KnowledgeID), Version: "2", MaterializationState: core.MaterializedFull}}
	stream = append(stream, knowledgeProjectionEvent(t, 5, derived))

	findings, err := New(reader{stream}).WithKnowledgeVerificationMaxAge(24*time.Hour).RunAt(context.Background(), verified.Add(2*time.Hour))
	if err != nil || len(findings) != 1 || findings[0].Rule != "knowledge_lineage_invalidated" || !slices.Contains(findings[0].EvidenceRefs, "knowledge-knowledge-source-v2") {
		t.Fatalf("lineage findings=%+v err=%v", findings, err)
	}
}

func activeKnowledge(id core.ID, created, verified time.Time, provenance, validation []string) core.KnowledgeRecord {
	return core.KnowledgeRecord{
		KnowledgeID: id, OrganizationID: "org-1", Version: 2, Type: core.KnowledgeLesson,
		Scope: core.KnowledgeScopeOrganization, ScopeID: "org-1", Status: core.KnowledgeActive,
		Title: "Bounded lesson", Content: "Use current verified evidence.", Basis: core.KnowledgeBasisSingleExperience,
		ProvenanceEventRefs: provenance, CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: created,
		LastVerifiedAt: &verified, ValidationMethod: core.KnowledgeValidationDeterministic, ValidationRefs: validation,
		ValidatedBy: "runtime", ValidatedByKind: core.PrincipalRuntime, SupersedesVersion: intPointer(1),
	}
}

func knowledgeProjectionEvent(t *testing.T, sequence int64, value core.KnowledgeRecord) events.Event {
	t.Helper()
	eventType := "KNOWLEDGE_ACTIVATED"
	if value.Status == core.KnowledgeStale {
		eventType = "KNOWLEDGE_STALE"
	}
	event := events.Event{
		EventID: "knowledge-" + string(value.KnowledgeID) + "-v" + string(rune('0'+value.Version)), Sequence: sequence,
		OrganizationID: string(value.OrganizationID), EventType: eventType, SourceActorID: "runtime",
		CorrelationID: "knowledge-" + string(value.KnowledgeID), CreatedAt: value.LastVerifiedAt.Add(time.Duration(sequence) * time.Minute), SchemaVersion: events.SchemaVersion,
	}
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := events.SealProjectionEvent(event, events.ProjectionRecord{
		ProjectionKind: "knowledge", RecordID: string(value.KnowledgeID), Version: value.Version,
		CorrelationID: event.CorrelationID, Value: body,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	event.Payload, err = json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func intPointer(value int) *int { return &value }
