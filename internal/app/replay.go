package app

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/replay"
)

// IncidentReplay reconstructs one durable Work stream from a verified ledger
// snapshot. It is an observation path only and cannot schedule or resume work.
func (s *Service) IncidentReplay(ctx context.Context, organizationID core.ID, conversationID string) (replay.Report, bool, error) {
	if organizationID == "" || conversationID == "" {
		return replay.Report{}, false, fmt.Errorf("incident replay identity is required")
	}
	correlationID, found, err := s.gateway.ResolveExternalWork(ctx, string(organizationID), conversationID)
	if err != nil {
		return replay.Report{}, false, fmt.Errorf("resolve incident conversation: %w", err)
	}
	if !found {
		return replay.Report{}, false, nil
	}
	snapshot, err := s.gateway.VerifiedReplayEvents(ctx, string(organizationID), correlationID, replay.MaximumEvents)
	if err != nil {
		return replay.Report{}, false, err
	}
	if len(snapshot.Events) == 0 {
		return replay.Report{}, false, nil
	}
	report, err := replay.Project(snapshot, conversationID)
	if err != nil {
		return replay.Report{}, false, fmt.Errorf("project incident replay: %w", err)
	}
	return report, true, nil
}
