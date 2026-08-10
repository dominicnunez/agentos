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

type TaskBlockedPayload struct {
	Reason        string   `json:"reason"`
	Missing       string   `json:"missing"`
	WhyNeeded     string   `json:"why_needed"`
	WorkCompleted string   `json:"work_completed"`
	RemainingWork string   `json:"remaining_work,omitempty"`
	EvidenceRefs  []string `json:"evidence_refs,omitempty"`
	Urgency       string   `json:"urgency,omitempty"`
}

type ResultPublishedPayload struct {
	Summary      string   `json:"summary"`
	ArtifactRefs []string `json:"artifact_refs,omitempty"`
}

// OperatorInputReceivedPayload is the durable, untrusted-content contract for
// one continuation message from any authenticated operator channel. MessageID
// makes delivery retries idempotent; the trusted envelope identity remains
// authoritative.
type OperatorInputReceivedPayload struct {
	MessageID           string `json:"message_id"`
	Text                string `json:"text"`
	SourcePrincipalID   string `json:"source_principal_id"`
	SourcePrincipalKind string `json:"source_principal_kind"`
	SourceChannel       string `json:"source_channel"`
}

type OperatorWorkAcceptedPayload struct {
	MessageID           string `json:"message_id"`
	SourcePrincipalID   string `json:"source_principal_id"`
	SourcePrincipalKind string `json:"source_principal_kind"`
	SourceChannel       string `json:"source_channel"`
}

func (p ResultPublishedPayload) ValidFor(artifactRefs []string) bool {
	return p.Summary != "" && sameStrings(p.ArtifactRefs, artifactRefs)
}

type InferenceUsageRecordedPayload struct {
	Source       string   `json:"source"`
	Provider     string   `json:"provider"`
	Model        string   `json:"model"`
	InputTokens  int      `json:"input_tokens"`
	OutputTokens int      `json:"output_tokens"`
	TotalTokens  int      `json:"total_tokens"`
	CostUSD      *float64 `json:"cost_usd,omitempty"`
}

func (p InferenceUsageRecordedPayload) Valid() bool {
	return p.Source != "" && p.Provider != "" && p.Model != "" &&
		p.InputTokens >= 0 && p.OutputTokens >= 0 &&
		p.TotalTokens == p.InputTokens+p.OutputTokens &&
		(p.CostUSD == nil || *p.CostUSD >= 0)
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
type ExternalWorkResolver interface {
	ResolveExternalWork(context.Context, string, string) (string, bool, error)
	ResolveExternalRequest(context.Context, string, string) (string, bool, error)
	ResolveExternalTask(context.Context, string, string) (string, string, bool, error)
}
type ExternalWorkAllocator interface {
	ReserveExternalWork(context.Context, string, string) (string, error)
}
type InboxReader interface {
	Inbox(context.Context, string, string) ([]Event, error)
}
type InboxObserver interface {
	ObserveInbox(context.Context, TrustedDraft, string, string, []string) (Event, error)
}

type AddressedRoute struct {
	OrganizationID string
	EventType      string
	SourceActorID  string
	ValidateSource bool
	RecipientScope string
	RecipientID    string
	TaskID         string
}

type RouteValidator interface {
	ValidateAddressedRoute(context.Context, AddressedRoute) error
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

func (g *Gateway) ResolveExternalWork(ctx context.Context, organizationID, requestID string) (string, bool, error) {
	resolver, ok := g.ledger.(ExternalWorkResolver)
	if !ok {
		return "", false, nil
	}
	return resolver.ResolveExternalWork(ctx, organizationID, requestID)
}

func (g *Gateway) ResolveExternalRequest(ctx context.Context, organizationID, correlationID string) (string, bool, error) {
	resolver, ok := g.ledger.(ExternalWorkResolver)
	if !ok {
		return "", false, nil
	}
	return resolver.ResolveExternalRequest(ctx, organizationID, correlationID)
}

func (g *Gateway) ResolveExternalTask(ctx context.Context, organizationID, taskID string) (string, string, bool, error) {
	resolver, ok := g.ledger.(ExternalWorkResolver)
	if !ok {
		return "", "", false, nil
	}
	return resolver.ResolveExternalTask(ctx, organizationID, taskID)
}

func (g *Gateway) ReserveExternalWork(ctx context.Context, organizationID, requestID string) (string, error) {
	allocator, ok := g.ledger.(ExternalWorkAllocator)
	if !ok {
		return "", fmt.Errorf("external work allocator is unavailable")
	}
	return allocator.ReserveExternalWork(ctx, organizationID, requestID)
}

var agentTypes = map[string]bool{"MESSAGE": true, "TASK_BLOCKED": true, "EVIDENCE_PUBLISHED": true, "RESULT_PUBLISHED": true, "CANDIDATE_COMPLETE": true, "KNOWLEDGE_PROPOSED": true, "SKILL_PROPOSED": true}

func (g *Gateway) PublishAgentDraft(ctx context.Context, organizationID, actorID, executionID, correlationID string, draft Draft) (Event, error) {
	if !agentTypes[draft.EventType] {
		return Event{}, fmt.Errorf("event type %s is not agent-proposable", draft.EventType)
	}
	if draft.EventType == "TASK_BLOCKED" && (draft.TaskID == "" || draft.RecipientScope != RecipientTask || draft.RecipientID == "") {
		return Event{}, fmt.Errorf("task blocked draft requires a source child task and parent task recipient")
	}
	if draft.EventType == "RESULT_PUBLISHED" {
		var result ResultPublishedPayload
		if draft.TaskID == "" || decodePayload(draft.Payload, &result) != nil || !result.ValidFor(draft.ArtifactRefs) {
			return Event{}, fmt.Errorf("result published draft requires a task, summary, and matching artifact refs")
		}
	}
	trusted := TrustedDraft{OrganizationID: organizationID, EventType: draft.EventType, SourceActorID: actorID, SourceExecutionID: executionID, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, TaskID: draft.TaskID, ArtifactRefs: draft.ArtifactRefs, Payload: draft.Payload, CorrelationID: correlationID}
	if err := g.validateAddressed(ctx, trusted, true); err != nil {
		return Event{}, err
	}
	return g.ledger.Append(ctx, trusted)
}
func (g *Gateway) PublishTrusted(ctx context.Context, draft TrustedDraft) (Event, error) {
	if err := g.validateAddressed(ctx, draft, false); err != nil {
		return Event{}, err
	}
	return g.ledger.Append(ctx, draft)
}
func (g *Gateway) PublishProjection(ctx context.Context, draft ProjectionDraft) (Event, error) {
	if draft.Event.EventType == "" || draft.ProjectionKind == "" || draft.RecordID == "" || draft.Version < 1 {
		return Event{}, fmt.Errorf("event type, projection kind, record id, and positive version are required")
	}
	if err := g.validateAddressed(ctx, draft.Event, false); err != nil {
		return Event{}, err
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

func (g *Gateway) validateAddressed(ctx context.Context, draft TrustedDraft, sourceRequired bool) error {
	addressed := draft.EventType == "MESSAGE" || draft.RecipientScope != "" || draft.RecipientID != ""
	if !addressed {
		return nil
	}
	validateSource := sourceRequired || draft.EventType == "MESSAGE"
	if draft.OrganizationID == "" || !validRecipient(draft.RecipientScope) || draft.RecipientID == "" {
		return fmt.Errorf("addressed event organization and valid recipient are required")
	}
	if validateSource && draft.SourceActorID == "" {
		return fmt.Errorf("addressed agent event requires an authenticated source")
	}
	if draft.EventType == "MESSAGE" {
		var content struct {
			Body string `json:"body"`
		}
		if err := decodePayload(draft.Payload, &content); err != nil || content.Body == "" {
			return fmt.Errorf("message payload requires a non-empty body")
		}
	}
	if draft.EventType == "TASK_BLOCKED" {
		var content TaskBlockedPayload
		if err := decodePayload(draft.Payload, &content); err != nil || content.Reason == "" || content.Missing == "" || content.WhyNeeded == "" || content.WorkCompleted == "" {
			return fmt.Errorf("task blocked payload requires reason, missing, why_needed, and work_completed")
		}
	}
	if g.routeValidator == nil {
		return fmt.Errorf("addressed event route validator is required")
	}
	return g.routeValidator.ValidateAddressedRoute(ctx, AddressedRoute{OrganizationID: draft.OrganizationID, EventType: draft.EventType, SourceActorID: draft.SourceActorID, ValidateSource: validateSource, RecipientScope: draft.RecipientScope, RecipientID: draft.RecipientID, TaskID: draft.TaskID})
}

func decodePayload(value any, target any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(payload, target)
}

func validRecipient(scope string) bool {
	return scope == RecipientAgent || scope == RecipientTeam || scope == RecipientTask
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
