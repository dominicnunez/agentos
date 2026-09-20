package events

import (
	"context"
	"fmt"
)

// IncidentSnapshot contains private evidence from one verified read transaction.
// RelatedEvents carry the organization's freeze chain and exact task-bound
// effect histories; their presence grants no authority or execution linkage.
type IncidentSnapshot struct {
	Work          VerifiedEventSnapshot `json:"-"`
	RelatedEvents []Event               `json:"-"`
	FreezeRecords []AuthorityRecord     `json:"-"`
	Admissions    []IncidentAdmission   `json:"-"`
}

// IncidentAdmission identifies a validated durable boundary, never an observed
// remote dispatch, successful effect or authorization to perform it again.
type IncidentAdmission struct {
	EventRef    string
	Kind        string
	TaskID      string
	ExecutionID string
}

type incidentReader interface {
	VerifiedIncidentEvents(context.Context, string, string, int) (IncidentSnapshot, error)
}

func (g *Gateway) VerifiedIncidentEvents(ctx context.Context, organization, correlation string, limit int) (IncidentSnapshot, error) {
	reader, ok := g.ledger.(incidentReader)
	if !ok {
		return IncidentSnapshot{}, fmt.Errorf("verified incident evidence is unavailable")
	}
	return reader.VerifiedIncidentEvents(ctx, organization, correlation, limit)
}
