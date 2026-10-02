package ledger

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

func TestIncidentEffectDependencyPromotion(t *testing.T) {
	store := sharedBudgetStore(t)
	attempt := incidentEffectFixture(t, store)
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil {
		t.Fatalf("healthy effect writer baseline: %v", err)
	}
	var pending events.Event
	for _, event := range baseline.RelatedEvents {
		if event.EventType != "EFFECT_OBLIGATION_TRANSITIONED" {
			continue
		}
		if pending.EventID == "" || event.Sequence < pending.Sequence {
			pending = event
		}
	}
	if pending.EventID == "" || len(baseline.RelatedEvents) != 2 {
		t.Fatal("fixture lacks both pending and attempted effect revisions")
	}
	_, task := latestTestProjection[core.Task](t, t.Context(), store, "task", attempt.TaskID)
	candidate := core.KnowledgeRecord{
		KnowledgeID: "effect-provenance", OrganizationID: "org-1", Version: 1,
		Type: core.KnowledgeLesson, Scope: core.KnowledgeScopeAgent, ScopeID: task.AssigneeID,
		Status: core.KnowledgeCandidate, Title: "Pending effect observation", Content: "Preserve the pending effect evidence.",
		Basis: core.KnowledgeBasisExternalEvidence, ProvenanceEventRefs: []string{pending.EventID},
		CreatedBy: "runtime", CreatedByKind: core.PrincipalRuntime, CreatedAt: time.Now().UTC(),
		ValidationMethod: core.KnowledgeValidationUnvalidated,
	}
	knowledge, err := store.AppendProjection(t.Context(), events.ProjectionDraft{
		Event:          events.TrustedDraft{OrganizationID: "org-1", EventType: "KNOWLEDGE_PROPOSED", SourceActorID: "runtime", CorrelationID: "knowledge-effect-provenance"},
		ProjectionKind: "knowledge", RecordID: string(candidate.KnowledgeID), Version: 1, Value: candidate,
	})
	if err != nil {
		t.Fatal(err)
	}
	incidentOverlapFullOwners(t, store)
	// The independent full effect owner checks the same exact two-record history;
	// Knowledge's provenance owner accepts the prior same-organization event.
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		tasks := map[string]int64{}
		for _, event := range baseline.Work.Events {
			payload, present, err := events.AdmittedProjection(event)
			if err != nil {
				return err
			}
			if present && payload.Projection.ProjectionKind == "task" && payload.Projection.Version == 1 {
				tasks[payload.Projection.RecordID] = event.Sequence
			}
		}
		return validateIncidentEffects(t.Context(), tx, "org-1", tasks, []string{string(attempt.ID)}, baseline.RelatedEvents, nil)
	}); err != nil {
		t.Fatalf("full effect owner rejected healthy history: %v", err)
	}
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "stop-work", 256)
	if err != nil {
		t.Fatalf("valid Knowledge/effect overlap rejected: %v", err)
	}
	related, dependency, knowledgeSelected := 0, 0, false
	for _, event := range snapshot.RelatedEvents {
		if event.EventID == pending.EventID {
			related++
		}
	}
	for _, event := range snapshot.DependencyEvents {
		if event.EventID == pending.EventID {
			dependency++
		}
		if event.EventID == knowledge.EventID {
			knowledgeSelected = true
		}
	}
	if !knowledgeSelected || related != 1 || dependency != 0 {
		t.Fatalf("Knowledge selected=%v pending effect related=%d dependency=%d want true/1/0", knowledgeSelected, related, dependency)
	}
	if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
		t.Fatalf("returned overlap snapshot rejected by shared history consumer: %v", err)
	}
}

func TestIncidentPublicPrivateByteAllowances(t *testing.T) {
	store := sharedBudgetStore(t)
	appendPrivateInferenceGoal(t, store)
	const privatePayload = (15 << 20) + (600 << 10)
	for range 2 {
		if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "model-stop", Payload: map[string]string{"text": strings.Repeat("p", privatePayload)}}); err != nil {
			t.Fatal(err)
		}
	}
	incidentOverlapFullOwners(t, store)
	baseline, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
	if err != nil {
		t.Fatalf("healthy private-byte baseline: %v", err)
	}
	privateNotes := 0
	for _, event := range baseline.DependencyEvents {
		if event.EventType == "AUDIT_NOTE" && event.CorrelationID == "model-stop" {
			privateNotes++
		}
	}
	if privateNotes != 2 {
		t.Fatalf("private notes selected=%d want2", privateNotes)
	}
	public, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "goal", Payload: map[string]string{"text": strings.Repeat("u", (1<<20)+(512<<10))}})
	if err != nil {
		t.Fatal(err)
	}
	incidentOverlapFullOwners(t, store)
	var publicCount, supportCount int
	var publicBytes, privateBytes, recordBytes, noteBytes int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),COALESCE(SUM(`+incidentEventBytes+`),0) FROM events WHERE organization_id='org-1' AND correlation_id='goal'`).Scan(&publicCount, &publicBytes); err != nil {
		t.Fatal(err)
	}
	// All nonpublic event sources and all physical records are a conservative
	// upper bound: the actual private selection is a subset. No reservations,
	// policies, leases, inbox bindings or admission annotations exist here.
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),COALESCE(SUM(`+incidentEventBytes+`),0) FROM events WHERE NOT (organization_id='org-1' AND correlation_id='goal')`).Scan(&supportCount, &privateBytes); err != nil {
		t.Fatal(err)
	}
	var recordCount int
	if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),COALESCE(SUM(`+incidentProjectionRecordBytes+`),0) FROM records r`).Scan(&recordCount, &recordBytes); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(t.Context(), `SELECT SUM(length(CAST(payload AS BLOB))) FROM events WHERE event_type='AUDIT_NOTE'`).Scan(&noteBytes); err != nil {
		t.Fatal(err)
	}
	if publicCount != 2 || publicBytes > 2<<20 || supportCount+recordCount > events.MaximumIncidentEvidence || privateBytes+recordBytes >= events.MaximumIncidentEvidenceBytes || noteBytes <= events.MaximumIncidentEvidenceBytes {
		t.Fatalf("fixture allowances: public%d/%d private upper%d/%d all-note payload%d", publicCount, publicBytes, supportCount+recordCount, privateBytes+recordBytes, noteBytes)
	}
	t.Logf("independent public bytes%d private+record upper%d combined note payload%d", publicBytes, privateBytes+recordBytes, noteBytes)
	snapshot, err := store.VerifiedIncidentEvents(t.Context(), "org-1", "goal", 256)
	if err != nil {
		t.Fatalf("independently bounded public and private evidence rejected: %v", err)
	}
	if len(snapshot.Work.Events) != 2 || snapshot.Work.Events[1].EventID != public.EventID {
		t.Fatal("public byte control changed selected timeline")
	}
	privateNotes = 0
	for _, event := range snapshot.DependencyEvents {
		if event.EventType == "AUDIT_NOTE" && event.CorrelationID == "model-stop" {
			privateNotes++
		}
	}
	if privateNotes != 2 {
		t.Fatalf("private notes selected=%d want2", privateNotes)
	}
	if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
		t.Fatalf("shared consumer contradicted separate byte allowances: %v", err)
	}
}

func incidentOverlapFullOwners(t *testing.T, store *SQLite) {
	t.Helper()
	sharedBudgetFullOwner(t, store)
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	leases, freezes, err := authorityAdmissionsSnapshot(t.Context(), store.db)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := events.ValidateProjectionHistory(stream, nil, leases, freezes)
	if err != nil {
		t.Fatalf("full projection owner rejected healthy fixture: %v", err)
	}
	if err := events.ValidateProjectionCompletions(graph, stream, nil); err != nil {
		t.Fatal(fmt.Errorf("full completion owner rejected healthy fixture: %w", err))
	}
}

func TestIncidentPreviouslyPublicBytes(t *testing.T) {
	store := sharedBudgetStore(t)
	public, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "public", Payload: map[string]string{"text": strings.Repeat("u", (1<<20)+(512<<10))}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "org-1", EventType: "AUDIT_NOTE", CorrelationID: "private", Payload: map[string]string{"text": strings.Repeat("p", (15<<20)+(600<<10))}}); err != nil {
			t.Fatal(err)
		}
	}
	var privateBytes, allBytes int64
	if err := store.db.QueryRowContext(t.Context(), `SELECT SUM(CASE WHEN correlation_id='private' THEN `+incidentEventBytes+` ELSE 0 END),SUM(`+incidentEventBytes+`) FROM events`).Scan(&privateBytes, &allBytes); err != nil {
		t.Fatal(err)
	}
	if privateBytes >= events.MaximumIncidentEvidenceBytes || allBytes <= events.MaximumIncidentEvidenceBytes {
		t.Fatal("fixture does not separate private and previously public raw bytes")
	}
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	support := newIncidentSupport()
	publicBudget := incidentBudget{events: 256, bytes: 2 << 20, public: true, support: support}
	if _, err := incidentEvents(t.Context(), tx, &publicBudget, `event_id=?`, public.EventID); err != nil {
		t.Fatal(err)
	}
	privateBudget := incidentBudget{events: events.MaximumIncidentEvidence, bytes: events.MaximumIncidentEvidenceBytes, support: support}
	// Independent consumers can require a public source again. Its raw bytes
	// stay in the public allowance while the private sources consume support.
	stream, err := incidentEvents(t.Context(), tx, &privateBudget, `organization_id=?`, "org-1")
	if err != nil {
		t.Fatalf("bounded mixed source batch rejected: %v", err)
	}
	if len(stream) != 3 || support.items != events.MaximumIncidentEvidence-2 || support.bytes != int64(events.MaximumIncidentEvidenceBytes)-privateBytes {
		t.Fatal("source reuse changed selection or charged public bytes to support")
	}
}
