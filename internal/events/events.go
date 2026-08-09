package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const SchemaVersion = 1

type Draft struct {
	EventType      string   `json:"event_type"`
	RecipientScope string   `json:"recipient_scope,omitempty"`
	RecipientID    string   `json:"recipient_id,omitempty"`
	TaskID         string   `json:"task_id,omitempty"`
	ArtifactRefs   []string `json:"artifact_refs,omitempty"`
	Payload        any      `json:"payload"`
}
type TrustedDraft struct {
	OrganizationID    string
	EventType         string
	SourceActorID     string
	SourceExecutionID string
	TaskID            string
	AuthorizationRefs []string
	ArtifactRefs      []string
	Payload           any
	CorrelationID     string
}

// ProjectionDraft couples one trusted event with one rebuildable projection
// update. The ledger persists both atomically; callers never publish the
// projection before its authoritative event exists.
type ProjectionDraft struct {
	Event          TrustedDraft
	ProjectionKind string
	RecordID       string
	Version        int
	Value          any
}

// ProjectionRecord is the canonical event/record representation used to
// rebuild current state. Value remains raw until a bounded projection module
// decodes it into its domain type.
type ProjectionRecord struct {
	ProjectionKind string          `json:"projection_kind"`
	RecordID       string          `json:"record_id"`
	Version        int             `json:"version"`
	CorrelationID  string          `json:"correlation_id,omitempty"`
	Value          json.RawMessage `json:"value"`
}

// ProjectionEventPayload preserves transition detail while carrying the
// complete versioned record needed for deterministic replay.
type ProjectionEventPayload struct {
	Projection ProjectionRecord `json:"projection"`
	Detail     json.RawMessage  `json:"detail,omitempty"`
}
type Event struct {
	EventID           string          `json:"event_id"`
	Sequence          int64           `json:"sequence"`
	OrganizationID    string          `json:"organization_id"`
	EventType         string          `json:"event_type"`
	SourceActorID     string          `json:"source_actor_id,omitempty"`
	SourceExecutionID string          `json:"source_execution_id,omitempty"`
	TaskID            string          `json:"task_id,omitempty"`
	AuthorizationRefs []string        `json:"authorization_refs"`
	ArtifactRefs      []string        `json:"artifact_refs"`
	CreatedAt         time.Time       `json:"created_at"`
	SchemaVersion     int             `json:"schema_version"`
	Payload           json.RawMessage `json:"payload"`
	CorrelationID     string          `json:"-"`
}
type Appender interface {
	Append(context.Context, TrustedDraft) (Event, error)
}
type Reader interface {
	Events(context.Context, string) ([]Event, error)
}
type ProjectionAppender interface {
	AppendProjection(context.Context, ProjectionDraft) (Event, error)
}
type ProjectionReader interface {
	Records(context.Context, string, string) ([][]byte, error)
}

type Gateway struct {
	ledger interface {
		Appender
		Reader
	}
}

func NewGateway(ledger interface {
	Appender
	Reader
}) *Gateway {
	return &Gateway{ledger: ledger}
}

var agentTypes = map[string]bool{"MESSAGE": true, "TASK_BLOCKED": true, "EVIDENCE_PUBLISHED": true, "RESULT_PUBLISHED": true, "CANDIDATE_COMPLETE": true, "KNOWLEDGE_PROPOSED": true, "SKILL_PROPOSED": true}

func (g *Gateway) PublishAgentDraft(ctx context.Context, organizationID, actorID, executionID, correlationID string, draft Draft) (Event, error) {
	if !agentTypes[draft.EventType] {
		return Event{}, fmt.Errorf("event type %s is not agent-proposable", draft.EventType)
	}
	return g.ledger.Append(ctx, TrustedDraft{OrganizationID: organizationID, EventType: draft.EventType, SourceActorID: actorID, SourceExecutionID: executionID, TaskID: draft.TaskID, ArtifactRefs: draft.ArtifactRefs, Payload: draft.Payload, CorrelationID: correlationID})
}
func (g *Gateway) PublishTrusted(ctx context.Context, draft TrustedDraft) (Event, error) {
	return g.ledger.Append(ctx, draft)
}
func (g *Gateway) PublishProjection(ctx context.Context, draft ProjectionDraft) (Event, error) {
	if draft.Event.EventType == "" || draft.ProjectionKind == "" || draft.RecordID == "" || draft.Version < 1 {
		return Event{}, fmt.Errorf("event type, projection kind, record id, and positive version are required")
	}
	store, ok := g.ledger.(ProjectionAppender)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support durable projections")
	}
	return store.AppendProjection(ctx, draft)
}
func (g *Gateway) ProjectionRecords(ctx context.Context, kind, id string) ([][]byte, error) {
	store, ok := g.ledger.(ProjectionReader)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support durable projections")
	}
	return store.Records(ctx, kind, id)
}
func (g *Gateway) Events(ctx context.Context, correlationID string) ([]Event, error) {
	return g.ledger.Events(ctx, correlationID)
}
