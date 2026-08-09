package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const SchemaVersion = 2

const (
	RecipientAgent = "AGENT"
	RecipientTeam  = "TEAM"
	RecipientTask  = "TASK"
)

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
	RecipientScope    string
	RecipientID       string
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
	RecipientScope    string          `json:"recipient_scope,omitempty"`
	RecipientID       string          `json:"recipient_id,omitempty"`
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
type InboxReader interface {
	Inbox(context.Context, string, string) ([]Event, error)
}
type InboxObserver interface {
	ObserveInbox(context.Context, TrustedDraft, string, string, []string) (Event, error)
}

type MessageRoute struct {
	OrganizationID string
	SourceActorID  string
	RecipientScope string
	RecipientID    string
	TaskID         string
}

type RouteValidator interface {
	ValidateMessageRoute(context.Context, MessageRoute) error
}

type Gateway struct {
	ledger interface {
		Appender
		Reader
	}
	routeValidator RouteValidator
}

func NewGateway(ledger interface {
	Appender
	Reader
}) *Gateway {
	return &Gateway{ledger: ledger}
}

// SetRouteValidator wires the organization/task identity projection into the
// gateway at the composition root. MESSAGE publication fails closed until the
// runtime supplies this validator.
func (g *Gateway) SetRouteValidator(validator RouteValidator) {
	g.routeValidator = validator
}

var agentTypes = map[string]bool{"MESSAGE": true, "TASK_BLOCKED": true, "EVIDENCE_PUBLISHED": true, "RESULT_PUBLISHED": true, "CANDIDATE_COMPLETE": true, "KNOWLEDGE_PROPOSED": true, "SKILL_PROPOSED": true}

func (g *Gateway) PublishAgentDraft(ctx context.Context, organizationID, actorID, executionID, correlationID string, draft Draft) (Event, error) {
	if !agentTypes[draft.EventType] {
		return Event{}, fmt.Errorf("event type %s is not agent-proposable", draft.EventType)
	}
	trusted := TrustedDraft{OrganizationID: organizationID, EventType: draft.EventType, SourceActorID: actorID, SourceExecutionID: executionID, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, TaskID: draft.TaskID, ArtifactRefs: draft.ArtifactRefs, Payload: draft.Payload, CorrelationID: correlationID}
	if err := g.validateMessage(ctx, trusted); err != nil {
		return Event{}, err
	}
	return g.ledger.Append(ctx, trusted)
}
func (g *Gateway) PublishTrusted(ctx context.Context, draft TrustedDraft) (Event, error) {
	if err := g.validateMessage(ctx, draft); err != nil {
		return Event{}, err
	}
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

func (g *Gateway) Inbox(ctx context.Context, recipientScope, recipientID string) ([]Event, error) {
	if !validRecipient(recipientScope) || recipientID == "" {
		return nil, fmt.Errorf("valid recipient scope and id are required")
	}
	reader, ok := g.ledger.(InboxReader)
	if !ok {
		return nil, fmt.Errorf("event ledger does not support durable inboxes")
	}
	return reader.Inbox(ctx, recipientScope, recipientID)
}

func (g *Gateway) ObserveInbox(ctx context.Context, organizationID, actorID, executionID, taskID, correlationID, recipientScope, recipientID string, eventIDs []string) (Event, error) {
	if !validRecipient(recipientScope) || recipientID == "" || len(eventIDs) == 0 {
		return Event{}, fmt.Errorf("recipient and event ids are required")
	}
	distinct := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		if eventID == "" {
			return Event{}, fmt.Errorf("event ids must be non-empty")
		}
		distinct[eventID] = struct{}{}
	}
	if len(distinct) != len(eventIDs) {
		return Event{}, fmt.Errorf("event ids must be distinct")
	}
	observer, ok := g.ledger.(InboxObserver)
	if !ok {
		return Event{}, fmt.Errorf("event ledger does not support durable inbox observations")
	}
	draft := TrustedDraft{OrganizationID: organizationID, EventType: "INBOX_EVENTS_OBSERVED", SourceActorID: actorID, SourceExecutionID: executionID, RecipientScope: recipientScope, RecipientID: recipientID, TaskID: taskID, CorrelationID: correlationID, Payload: map[string]any{"event_ids": eventIDs}}
	return observer.ObserveInbox(ctx, draft, recipientScope, recipientID, eventIDs)
}

func (g *Gateway) validateMessage(ctx context.Context, draft TrustedDraft) error {
	if draft.EventType != "MESSAGE" {
		return nil
	}
	if draft.OrganizationID == "" || draft.SourceActorID == "" || !validRecipient(draft.RecipientScope) || draft.RecipientID == "" {
		return fmt.Errorf("message organization, source actor, and valid recipient are required")
	}
	var content struct {
		Body string `json:"body"`
	}
	payload, err := json.Marshal(draft.Payload)
	if err != nil {
		return fmt.Errorf("encode message payload: %w", err)
	}
	if err := json.Unmarshal(payload, &content); err != nil || content.Body == "" {
		return fmt.Errorf("message payload requires a non-empty body")
	}
	if g.routeValidator == nil {
		return fmt.Errorf("message route validator is required")
	}
	return g.routeValidator.ValidateMessageRoute(ctx, MessageRoute{OrganizationID: draft.OrganizationID, SourceActorID: draft.SourceActorID, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, TaskID: draft.TaskID})
}

func validRecipient(scope string) bool {
	return scope == RecipientAgent || scope == RecipientTeam || scope == RecipientTask
}
