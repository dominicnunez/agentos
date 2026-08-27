// Package audit contains deterministic checks; findings never mutate authority.
package audit

import (
	"context"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
)

type Severity string

const (
	High Severity = "HIGH"
)

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
	reader Reader
	now    func() time.Time
}

func New(r Reader) *Service { return &Service{reader: r, now: time.Now} }

func (s *Service) Run(ctx context.Context) ([]Finding, error) {
	return s.RunAt(ctx, s.now().UTC())
}

// RunAt evaluates one immutable audit pass at an explicit observation time.
// Every finding from the pass carries exactly that timestamp, so replaying the
// same event stream with the same observation time is byte-reproducible.
func (s *Service) RunAt(ctx context.Context, observedAt time.Time) ([]Finding, error) {
	if observedAt.IsZero() {
		return nil, fmt.Errorf("audit observation time is required")
	}
	observedAt = observedAt.UTC()
	es, e := s.reader.Events(ctx, "")
	if e != nil {
		return nil, e
	}
	seen := map[string]bool{}
	var out []Finding
	for _, event := range es {
		if event.EventID == "" || event.Sequence < 1 {
			out = append(out, Finding{ID: fmt.Sprintf("integrity-%d", event.Sequence), Rule: "ledger_reference_integrity", Severity: High, Status: "OPEN", CreatedAt: observedAt})
		}
		if event.EventType == "COMPLETION_VERIFIED" {
			seen[event.TaskID] = true
		}
		if event.EventType == "TASK_VERIFIED_COMPLETE" && !seen[event.TaskID] {
			out = append(out, Finding{ID: "completion-" + event.TaskID, Rule: "completion_evidence_order", Severity: High, Scope: event.TaskID, EvidenceRefs: []string{event.EventID}, Status: "OPEN", CreatedAt: observedAt})
		}
	}
	return out, nil
}
