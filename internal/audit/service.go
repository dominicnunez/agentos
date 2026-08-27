// Package audit contains deterministic checks; findings never mutate authority.
package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

type Severity string

const High Severity = "HIGH"

type Finding struct {
	ID           string    `json:"id"`
	Rule         string    `json:"rule"`
	Severity     Severity  `json:"severity"`
	Scope        string    `json:"scope"`
	EvidenceRefs []string  `json:"evidence_refs"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
}

type Reader interface {
	Events(context.Context, string) ([]events.Event, error)
}

type Service struct {
	reader                      Reader
	now                         func() time.Time
	knowledgeVerificationMaxAge time.Duration
}

func New(reader Reader) *Service { return &Service{reader: reader, now: time.Now} }

// WithKnowledgeVerificationMaxAge configures deployment policy without
// embedding a governance choice in the runtime. Nonpositive values leave the
// policy unset and active knowledge fails closed into an audit finding.
func (s *Service) WithKnowledgeVerificationMaxAge(maximum time.Duration) *Service {
	if s != nil {
		s.knowledgeVerificationMaxAge = maximum
	}
	return s
}

func (s *Service) Run(ctx context.Context) ([]Finding, error) {
	if s == nil || s.now == nil {
		return nil, fmt.Errorf("audit reader and clock are required")
	}
	return s.RunAt(ctx, s.now().UTC())
}

// RunAt makes one audit pass reproducible for tests, incident review, and
// deterministic replay. It observes state and never repairs it.
func (s *Service) RunAt(ctx context.Context, now time.Time) ([]Finding, error) {
	if s == nil || s.reader == nil || now.IsZero() {
		return nil, fmt.Errorf("audit reader and timestamp are required")
	}
	now = now.UTC()
	stream, err := s.reader.Events(ctx, "")
	if err != nil {
		return nil, err
	}
	findings := auditEventIntegrity(stream, now)
	return append(findings, s.auditKnowledge(stream, now)...), nil
}

func auditEventIntegrity(stream []events.Event, now time.Time) []Finding {
	seenCompletion := map[string]bool{}
	var findings []Finding
	for _, event := range stream {
		if event.EventID == "" || event.Sequence < 1 {
			findings = append(findings, newFinding(fmt.Sprintf("integrity-%d", event.Sequence), "ledger_reference_integrity", "", nil, now))
		}
		if event.EventType == "COMPLETION_VERIFIED" {
			seenCompletion[event.TaskID] = true
		}
		if event.EventType == "TASK_VERIFIED_COMPLETE" && !seenCompletion[event.TaskID] {
			findings = append(findings, newFinding("completion-"+event.TaskID, "completion_evidence_order", event.TaskID, []string{event.EventID}, now))
		}
	}
	return findings
}

type auditedKnowledgeRevision struct {
	value     core.KnowledgeRecord
	admission events.Event
}

func (s *Service) auditKnowledge(stream []events.Event, now time.Time) []Finding {
	eventIndex := make(map[string]events.Event, len(stream))
	revisions := make(map[core.ID]map[int]auditedKnowledgeRevision)
	latest := make(map[core.ID]auditedKnowledgeRevision)
	var findings []Finding
	for _, event := range stream {
		eventIndex[event.EventID] = event
		if !knowledgeLifecycleEvent(event.EventType) {
			continue
		}
		projection, present, err := events.AdmittedProjection(event)
		if err != nil {
			findings = append(findings, newFinding("knowledge-projection-"+event.EventID, "knowledge_projection_integrity", event.OrganizationID, []string{event.EventID}, now))
			continue
		}
		if !present {
			if event.EventType != "KNOWLEDGE_PROPOSED" || event.SourceActorID == "runtime" {
				findings = append(findings, newFinding("knowledge-projection-"+event.EventID, "knowledge_projection_integrity", event.OrganizationID, []string{event.EventID}, now))
			}
			continue
		}
		if projection.Projection.ProjectionKind != "knowledge" || event.SourceActorID != "runtime" ||
			event.SourceExecutionID != "" || event.RecipientScope != "" || event.RecipientID != "" || event.TaskID != "" ||
			event.CorrelationID != "knowledge-"+projection.Projection.RecordID {
			findings = append(findings, newFinding("knowledge-projection-"+event.EventID, "knowledge_projection_integrity", event.OrganizationID, []string{event.EventID}, now))
			continue
		}
		var knowledge core.KnowledgeRecord
		if decodeExact(projection.Projection.Value, &knowledge) != nil || !core.ValidKnowledgeRecord(knowledge) ||
			knowledge.KnowledgeID != core.ID(projection.Projection.RecordID) || knowledge.Version != projection.Projection.Version ||
			knowledge.OrganizationID != core.ID(event.OrganizationID) {
			findings = append(findings, newFinding("knowledge-record-"+event.EventID, "knowledge_projection_integrity", projection.Projection.RecordID, []string{event.EventID}, now))
			continue
		}
		revision := auditedKnowledgeRevision{value: knowledge, admission: event}
		if revisions[knowledge.KnowledgeID] == nil {
			revisions[knowledge.KnowledgeID] = make(map[int]auditedKnowledgeRevision)
		}
		revisions[knowledge.KnowledgeID][knowledge.Version] = revision
		if current, found := latest[knowledge.KnowledgeID]; !found || knowledge.Version > current.value.Version {
			latest[knowledge.KnowledgeID] = revision
		}
	}
	knowledgeIDs := make([]core.ID, 0, len(latest))
	for knowledgeID := range latest {
		knowledgeIDs = append(knowledgeIDs, knowledgeID)
	}
	slices.Sort(knowledgeIDs)
	for _, knowledgeID := range knowledgeIDs {
		revision := latest[knowledgeID]
		if revision.value.Status == core.KnowledgeActive {
			findings = append(findings, auditActiveKnowledge(knowledgeID, revision, revisions, latest, eventIndex, now, s.knowledgeVerificationMaxAge)...)
		}
	}
	return findings
}

func knowledgeLifecycleEvent(eventType string) bool {
	switch eventType {
	case "KNOWLEDGE_PROPOSED", "KNOWLEDGE_ACTIVATED", "KNOWLEDGE_SUPERSEDED", "KNOWLEDGE_STALE", "KNOWLEDGE_QUARANTINED":
		return true
	default:
		return false
	}
}

func auditActiveKnowledge(knowledgeID core.ID, revision auditedKnowledgeRevision, revisions map[core.ID]map[int]auditedKnowledgeRevision, latest map[core.ID]auditedKnowledgeRevision, eventIndex map[string]events.Event, now time.Time, maximumAge time.Duration) []Finding {
	value := revision.value
	var findings []Finding
	refs := append(append(append([]string{}, value.ProvenanceEventRefs...), value.OccurrenceEventRefs...), value.ValidationRefs...)
	for _, ref := range refs {
		evidence, found := eventIndex[ref]
		if !found || evidence.OrganizationID != string(value.OrganizationID) || evidence.Sequence >= revision.admission.Sequence {
			findings = append(findings, newFinding("knowledge-provenance-"+string(knowledgeID)+"-"+ref, "knowledge_provenance_invalid", string(knowledgeID), []string{revision.admission.EventID, ref}, now))
		}
	}
	for _, ref := range value.DerivedKnowledgeRefs {
		version, err := strconv.Atoi(ref.Version)
		source, found := revisions[core.ID(ref.ID)][version]
		current, currentFound := latest[core.ID(ref.ID)]
		if err != nil || !found || source.value.OrganizationID != value.OrganizationID || source.value.Status != core.KnowledgeActive ||
			!currentFound || current.value.Version != version || current.value.Status != core.KnowledgeActive {
			evidenceRefs := []string{revision.admission.EventID}
			if source.admission.EventID != "" {
				evidenceRefs = append(evidenceRefs, source.admission.EventID)
			}
			findings = append(findings, newFinding("knowledge-lineage-"+string(knowledgeID)+"-"+ref.ID+"-"+ref.Version, "knowledge_lineage_invalidated", string(knowledgeID), evidenceRefs, now))
		}
	}
	if value.LastVerifiedAt == nil || value.LastVerifiedAt.After(now) {
		findings = append(findings, newFinding("knowledge-verification-"+string(knowledgeID), "knowledge_verification_timestamp_invalid", string(knowledgeID), []string{revision.admission.EventID}, now))
	} else if maximumAge <= 0 {
		findings = append(findings, newFinding("knowledge-staleness-policy-"+string(knowledgeID), "knowledge_staleness_policy_missing", string(knowledgeID), []string{revision.admission.EventID}, now))
	} else if now.Sub(*value.LastVerifiedAt) > maximumAge {
		findings = append(findings, newFinding("knowledge-revalidation-"+string(knowledgeID), "knowledge_revalidation_due", string(knowledgeID), []string{revision.admission.EventID}, now))
	}
	return findings
}

func newFinding(id, rule, scope string, refs []string, now time.Time) Finding {
	if refs == nil {
		refs = []string{}
	}
	return Finding{ID: id, Rule: rule, Severity: High, Scope: scope, EvidenceRefs: refs, Status: "OPEN", CreatedAt: now}
}

func decodeExact(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}
