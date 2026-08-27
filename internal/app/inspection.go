package app

import (
	"context"
	"fmt"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/inspection"
)

// GovernanceInspection returns a bounded, deterministic, tenant-scoped report
// tied to one verified ledger head. It is read-only and cannot schedule work,
// repair findings, or grant authority.
func (s *Service) GovernanceInspection(ctx context.Context, organizationID core.ID) (inspection.Report, bool, error) {
	if s == nil || s.state == nil || s.gateway == nil || organizationID == "" {
		return inspection.Report{}, false, fmt.Errorf("governance inspection boundary is required")
	}
	snapshot, err := s.state.Load(ctx)
	if err != nil {
		return inspection.Report{}, false, err
	}
	if _, found := snapshot.Organizations[organizationID]; !found {
		return inspection.Report{}, false, nil
	}
	verified, err := s.gateway.VerifiedOrganizationEvents(ctx, string(organizationID), inspection.MaximumEvents)
	if err != nil {
		return inspection.Report{}, false, err
	}
	observedAt := time.Now().UTC()
	report, err := inspection.Project(snapshot, verified, organizationID, observedAt, 0)
	return report, true, err
}
