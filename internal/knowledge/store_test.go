package knowledge

import (
	"context"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestStorePreservesVersionsAndRequiresProvenance(t *testing.T) {
	l, e := ledger.Open(":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	s := New(l)
	r := core.KnowledgeRecord{KnowledgeID: "k", Version: 1, Scope: "ORGANIZATION", Status: core.KnowledgeCandidate, Content: "lesson", CreatedAt: time.Now()}
	if s.Propose(context.Background(), r) == nil {
		t.Fatal("missing provenance accepted")
	}
	r.ProvenanceEventRefs = []string{"e"}
	if e = s.Propose(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	if e = s.Propose(context.Background(), r); e == nil {
		t.Fatal("version overwritten")
	}
	events, err := l.Events(context.Background(), "")
	if err != nil || len(events) != 1 || events[0].EventType != "KNOWLEDGE_RECORD_TRANSITIONED" {
		t.Fatalf("knowledge transition was not ledgered: events=%+v err=%v", events, err)
	}
	if PatternCandidate([]string{"1", "2"}) == nil {
		t.Fatal("two occurrences accepted")
	}
	if PatternCandidate([]string{"1", "1", "1"}) == nil {
		t.Fatal("one repeated occurrence accepted as a pattern")
	}
	if PatternCandidate([]string{"1", " 1", "1 "}) == nil {
		t.Fatal("padded forms of one occurrence accepted as distinct")
	}
	if PatternCandidate([]string{"1", "2", ""}) == nil {
		t.Fatal("empty occurrence reference accepted")
	}
	if err := PatternCandidate([]string{"1", "2", "3"}); err != nil {
		t.Fatalf("three distinct occurrences rejected: %v", err)
	}
}
