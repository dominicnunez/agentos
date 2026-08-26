// Package knowledge implements bounded, versioned institutional knowledge.
// Knowledge is curated context, never authority, approval, or completion proof.
package knowledge

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	maximumSearchBytes      = 4096
	maximumSearchResults    = 64
	maximumSearchScan       = 4096
	knowledgeCorrelationKey = "knowledge-"
)

type Store struct{ gateway *events.Gateway }

func New(gateway *events.Gateway) *Store { return &Store{gateway: gateway} }

func (s *Store) Propose(ctx context.Context, record core.KnowledgeRecord) (events.Event, error) {
	if record.Status != core.KnowledgeCandidate || !core.ValidKnowledgeRecord(record) {
		return events.Event{}, fmt.Errorf("knowledge proposal must be a valid candidate revision")
	}
	return s.publish(ctx, "KNOWLEDGE_PROPOSED", record)
}

func (s *Store) Activate(ctx context.Context, record core.KnowledgeRecord) (events.Event, error) {
	if record.Status != core.KnowledgeActive || !core.ValidKnowledgeRecord(record) {
		return events.Event{}, fmt.Errorf("knowledge activation requires a valid active revision")
	}
	return s.publish(ctx, "KNOWLEDGE_ACTIVATED", record)
}

func (s *Store) Supersede(ctx context.Context, record core.KnowledgeRecord) (events.Event, error) {
	if record.Status != core.KnowledgeSuperseded || !core.ValidKnowledgeRecord(record) {
		return events.Event{}, fmt.Errorf("knowledge supersession requires a valid superseded revision")
	}
	return s.publish(ctx, "KNOWLEDGE_SUPERSEDED", record)
}

func (s *Store) MarkStale(ctx context.Context, record core.KnowledgeRecord) (events.Event, error) {
	if record.Status != core.KnowledgeStale || !core.ValidKnowledgeRecord(record) {
		return events.Event{}, fmt.Errorf("knowledge staleness requires a valid stale revision")
	}
	return s.publish(ctx, "KNOWLEDGE_MARKED_STALE", record)
}

func (s *Store) Quarantine(ctx context.Context, record core.KnowledgeRecord) (events.Event, error) {
	if record.Status != core.KnowledgeQuarantined || !core.ValidKnowledgeRecord(record) {
		return events.Event{}, fmt.Errorf("knowledge quarantine requires a valid quarantined revision")
	}
	return s.publish(ctx, "KNOWLEDGE_QUARANTINED", record)
}

func (s *Store) publish(ctx context.Context, eventType string, record core.KnowledgeRecord) (events.Event, error) {
	if s == nil || s.gateway == nil {
		return events.Event{}, fmt.Errorf("knowledge store requires an event gateway")
	}
	return s.gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: string(record.OrganizationID),
			EventType:      eventType,
			SourceActorID:  "runtime",
			ArtifactRefs:   append([]string(nil), record.EvidenceArtifactRefs...),
			CorrelationID:  knowledgeCorrelationKey + string(record.KnowledgeID),
			Payload: map[string]any{
				"submitted_by_id":   record.CreatedBy,
				"submitted_by_kind": record.CreatedByKind,
			},
		},
		ProjectionKind: "knowledge",
		RecordID:       string(record.KnowledgeID),
		Version:        record.Version,
		Value:          record,
	})
}

// Search returns a deterministic bounded prefix of active knowledge in one
// exact tenant/scope. It performs no inference and grants no authority.
func (s *Store) Search(ctx context.Context, organizationID core.ID, scope core.KnowledgeScope, scopeID core.ID, text string, limit int) ([]core.KnowledgeRecord, error) {
	if s == nil || s.gateway == nil {
		return nil, fmt.Errorf("knowledge store requires an event gateway")
	}
	if organizationID == "" || scopeID == "" || strings.TrimSpace(text) == "" || strings.TrimSpace(text) != text ||
		len(text) > maximumSearchBytes || !utf8.ValidString(text) || limit < 1 || limit > maximumSearchResults {
		return nil, fmt.Errorf("complete tenant/scope, canonical bounded search text, and result limit from 1 through 64 are required")
	}
	if scope != core.KnowledgeScopeAgent && scope != core.KnowledgeScopeTeam && scope != core.KnowledgeScopeOrganization {
		return nil, fmt.Errorf("knowledge scope is unsupported")
	}
	if scope == core.KnowledgeScopeOrganization && scopeID != organizationID {
		return nil, fmt.Errorf("organization knowledge scope crosses its tenant boundary")
	}
	needle := strings.ToLower(text)
	rows, err := s.gateway.ActiveKnowledgeRecords(ctx, string(organizationID), string(scope), string(scopeID), maximumSearchScan+1)
	if err != nil {
		return nil, err
	}
	if len(rows) > maximumSearchScan {
		return nil, fmt.Errorf("active knowledge scope exceeds the deterministic search bound")
	}
	results := make([]core.KnowledgeRecord, 0, limit)
	previousRecordID := ""
	for _, body := range rows {
		var projection events.ProjectionRecord
		var record core.KnowledgeRecord
		if json.Unmarshal(body, &projection) != nil || json.Unmarshal(projection.Value, &record) != nil ||
			projection.ProjectionKind != "knowledge" || projection.RecordID != string(record.KnowledgeID) || projection.RecordID <= previousRecordID || projection.Version != record.Version ||
			record.OrganizationID != organizationID || record.Scope != scope || record.ScopeID != scopeID || record.Status != core.KnowledgeActive ||
			!core.ValidKnowledgeRecord(record) {
			return nil, fmt.Errorf("active knowledge projection is invalid")
		}
		previousRecordID = projection.RecordID
		if knowledgeContains(record, needle) {
			results = append(results, record)
			if len(results) == limit {
				return results, nil
			}
		}
	}
	return results, nil
}

func knowledgeContains(record core.KnowledgeRecord, needle string) bool {
	if strings.Contains(strings.ToLower(record.Title), needle) || strings.Contains(strings.ToLower(record.Content), needle) ||
		strings.Contains(strings.ToLower(record.Applicability), needle) {
		return true
	}
	for _, tag := range record.Tags {
		if strings.Contains(strings.ToLower(tag), needle) {
			return true
		}
	}
	return false
}

// PatternCandidate enforces the default minimum without confusing frequency
// with validation or truth.
func PatternCandidate(refs []string) error {
	distinct := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		canonical := strings.TrimSpace(ref)
		if canonical == "" {
			return fmt.Errorf("occurrence event refs must be non-empty")
		}
		if canonical != ref {
			return fmt.Errorf("occurrence event refs must not contain surrounding whitespace")
		}
		distinct[canonical] = struct{}{}
	}
	if len(distinct) < 3 {
		return fmt.Errorf("at least three distinct concrete occurrence event refs are required")
	}
	return nil
}
