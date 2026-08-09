// Package knowledge implements the minimal versioned institutional store.
package knowledge

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dominicnunez/agentos/internal/core"
)

type Records interface {
	AppendRecord(context.Context, string, string, string, string, []string, []string, string, string, int, any) error
	Records(context.Context, string, string) ([][]byte, error)
}
type Store struct{ records Records }

func New(records Records) *Store { return &Store{records: records} }
func (s *Store) Propose(ctx context.Context, r core.KnowledgeRecord) error {
	if r.Status != core.KnowledgeCandidate {
		return fmt.Errorf("new knowledge must be CANDIDATE")
	}
	if len(r.ProvenanceEventRefs) == 0 {
		return fmt.Errorf("knowledge provenance is required")
	}
	return s.records.AppendRecord(ctx, "", "KNOWLEDGE_RECORD_TRANSITIONED", string(r.CreatedBy), "", nil, r.EvidenceArtifactRefs, "knowledge", string(r.KnowledgeID), r.Version, r)
}
func (s *Store) Search(ctx context.Context, scope, text string) ([]core.KnowledgeRecord, error) {
	rows, err := s.records.Records(ctx, "knowledge", "")
	if err != nil {
		return nil, err
	}
	var out []core.KnowledgeRecord
	for _, b := range rows {
		var r core.KnowledgeRecord
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, err
		}
		if r.Status == core.KnowledgeActive && r.Scope == scope && strings.Contains(strings.ToLower(r.Content), strings.ToLower(text)) {
			out = append(out, r)
		}
	}
	return out, nil
}

// PatternCandidate enforces the default minimum without confusing frequency with validation.
func PatternCandidate(refs []string) error {
	distinct := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if strings.TrimSpace(ref) == "" {
			return fmt.Errorf("occurrence event refs must be non-empty")
		}
		distinct[ref] = struct{}{}
	}
	if len(distinct) < 3 {
		return fmt.Errorf("at least three distinct concrete occurrence event refs are required")
	}
	return nil
}
