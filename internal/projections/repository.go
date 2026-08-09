// Package projections materializes durable organizational and work state from
// versioned Event Contracts. It contains no scheduling or execution policy.
package projections

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
)

const (
	KindOrganization = "organization"
	KindTeam         = "team"
	KindAgent        = "agent"
	KindIntent       = "intent"
	KindGoal         = "goal"
	KindTask         = "task"
)

type Versioned[T any] struct {
	Version       int
	CorrelationID string
	Value         T
}

type Snapshot struct {
	Organizations map[core.ID]Versioned[core.Organization]
	Teams         map[core.ID]Versioned[core.Team]
	Agents        map[core.ID]Versioned[core.Agent]
	Intents       map[core.ID]Versioned[core.Intent]
	Goals         map[core.ID]Versioned[core.Goal]
	Tasks         map[core.ID]Versioned[core.Task]
}

type Repository struct{ gateway *events.Gateway }

func New(gateway *events.Gateway) *Repository { return &Repository{gateway: gateway} }

func (r *Repository) SaveOrganization(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Organization, detail any) error {
	return r.save(ctx, string(value.ID), eventType, actorID, "", correlationID, KindOrganization, value.ID, version, value, detail)
}

func (r *Repository) SaveTeam(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Team, detail any) error {
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindTeam, value.ID, version, value, detail)
}

func (r *Repository) SaveAgent(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Agent, detail any) error {
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindAgent, value.ID, version, value, detail)
}

func (r *Repository) SaveIntent(ctx context.Context, eventType, actorID, correlationID string, version int, value core.Intent, detail any) error {
	return r.save(ctx, string(value.OrganizationID), eventType, actorID, "", correlationID, KindIntent, value.ID, version, value, detail)
}

func (r *Repository) SaveGoal(ctx context.Context, organizationID core.ID, eventType, actorID, correlationID string, version int, value core.Goal, detail any) error {
	return r.save(ctx, string(organizationID), eventType, actorID, "", correlationID, KindGoal, value.ID, version, value, detail)
}

func (r *Repository) SaveTask(ctx context.Context, organizationID core.ID, eventType, actorID, correlationID string, version int, value core.Task, detail any) error {
	return r.save(ctx, string(organizationID), eventType, actorID, string(value.ID), correlationID, KindTask, value.ID, version, value, detail)
}

func (r *Repository) save(ctx context.Context, organizationID, eventType, actorID, taskID, correlationID, kind string, id core.ID, version int, value, detail any) error {
	if r == nil || r.gateway == nil {
		return fmt.Errorf("durable projection gateway is required")
	}
	_, err := r.gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: organizationID,
			EventType:      eventType,
			SourceActorID:  actorID,
			TaskID:         taskID,
			CorrelationID:  correlationID,
			Payload:        detail,
		},
		ProjectionKind: kind,
		RecordID:       string(id),
		Version:        version,
		Value:          value,
	})
	return err
}

func (r *Repository) Load(ctx context.Context) (Snapshot, error) {
	if r == nil || r.gateway == nil {
		return Snapshot{}, fmt.Errorf("durable projection gateway is required")
	}
	return r.loadFromRecords(ctx)
}

// Rebuild ignores the records table and deterministically replays projection
// records embedded in the authoritative event stream.
func (r *Repository) Rebuild(ctx context.Context) (Snapshot, error) {
	if r == nil || r.gateway == nil {
		return Snapshot{}, fmt.Errorf("durable projection gateway is required")
	}
	stream, err := r.gateway.Events(ctx, "")
	if err != nil {
		return Snapshot{}, err
	}
	records := make(map[string][][]byte)
	for _, event := range stream {
		var payload events.ProjectionEventPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.Projection.ProjectionKind == "" {
			continue
		}
		body, err := json.Marshal(payload.Projection)
		if err != nil {
			return Snapshot{}, err
		}
		kind := payload.Projection.ProjectionKind
		records[kind] = append(records[kind], body)
	}
	return decodeSnapshot(records)
}

func (r *Repository) loadFromRecords(ctx context.Context) (Snapshot, error) {
	records := make(map[string][][]byte)
	for _, kind := range []string{KindOrganization, KindTeam, KindAgent, KindIntent, KindGoal, KindTask} {
		rows, err := r.gateway.ProjectionRecords(ctx, kind, "")
		if err != nil {
			return Snapshot{}, err
		}
		records[kind] = rows
	}
	return decodeSnapshot(records)
}

func decodeSnapshot(records map[string][][]byte) (Snapshot, error) {
	snapshot := Snapshot{
		Organizations: make(map[core.ID]Versioned[core.Organization]),
		Teams:         make(map[core.ID]Versioned[core.Team]),
		Agents:        make(map[core.ID]Versioned[core.Agent]),
		Intents:       make(map[core.ID]Versioned[core.Intent]),
		Goals:         make(map[core.ID]Versioned[core.Goal]),
		Tasks:         make(map[core.ID]Versioned[core.Task]),
	}
	if err := decodeKind(records[KindOrganization], snapshot.Organizations); err != nil {
		return Snapshot{}, fmt.Errorf("decode organizations: %w", err)
	}
	if err := decodeKind(records[KindTeam], snapshot.Teams); err != nil {
		return Snapshot{}, fmt.Errorf("decode teams: %w", err)
	}
	if err := decodeKind(records[KindAgent], snapshot.Agents); err != nil {
		return Snapshot{}, fmt.Errorf("decode agents: %w", err)
	}
	if err := decodeKind(records[KindIntent], snapshot.Intents); err != nil {
		return Snapshot{}, fmt.Errorf("decode intents: %w", err)
	}
	if err := decodeKind(records[KindGoal], snapshot.Goals); err != nil {
		return Snapshot{}, fmt.Errorf("decode goals: %w", err)
	}
	if err := decodeKind(records[KindTask], snapshot.Tasks); err != nil {
		return Snapshot{}, fmt.Errorf("decode tasks: %w", err)
	}
	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func decodeKind[T any](bodies [][]byte, target map[core.ID]Versioned[T]) error {
	for _, body := range bodies {
		var record events.ProjectionRecord
		if err := json.Unmarshal(body, &record); err != nil {
			return err
		}
		id := core.ID(record.RecordID)
		previous, exists := target[id]
		wantVersion := 1
		if exists {
			wantVersion = previous.Version + 1
		}
		if record.Version != wantVersion {
			return fmt.Errorf("record %s version %d follows %d", id, record.Version, previous.Version)
		}
		var value T
		if err := json.Unmarshal(record.Value, &value); err != nil {
			return err
		}
		target[id] = Versioned[T]{Version: record.Version, CorrelationID: record.CorrelationID, Value: value}
	}
	return nil
}

func validateSnapshot(snapshot Snapshot) error {
	for id, state := range snapshot.Organizations {
		if id == "" || state.Value.ID != id {
			return fmt.Errorf("organization record %s has mismatched identity %s", id, state.Value.ID)
		}
	}
	for id, state := range snapshot.Agents {
		if state.Value.ID != id {
			return fmt.Errorf("agent record %s has mismatched identity %s", id, state.Value.ID)
		}
		if _, ok := snapshot.Organizations[state.Value.OrganizationID]; !ok {
			return fmt.Errorf("agent %s references missing organization %s", id, state.Value.OrganizationID)
		}
	}
	for id, state := range snapshot.Teams {
		if state.Value.ID != id {
			return fmt.Errorf("team record %s has mismatched identity %s", id, state.Value.ID)
		}
		if _, ok := snapshot.Organizations[state.Value.OrganizationID]; !ok {
			return fmt.Errorf("team %s references missing organization %s", id, state.Value.OrganizationID)
		}
		for _, memberID := range state.Value.MemberAgentIDs {
			member, ok := snapshot.Agents[memberID]
			if !ok || member.Value.OrganizationID != state.Value.OrganizationID {
				return fmt.Errorf("team %s references invalid member agent %s", id, memberID)
			}
		}
	}
	for id, state := range snapshot.Intents {
		if state.Value.ID != id {
			return fmt.Errorf("intent record %s has mismatched identity %s", id, state.Value.ID)
		}
		if _, ok := snapshot.Organizations[state.Value.OrganizationID]; !ok {
			return fmt.Errorf("intent %s references missing organization %s", id, state.Value.OrganizationID)
		}
	}
	for id, state := range snapshot.Goals {
		if state.Value.ID != id {
			return fmt.Errorf("goal record %s has mismatched identity %s", id, state.Value.ID)
		}
		if _, ok := snapshot.Intents[state.Value.IntentID]; !ok {
			return fmt.Errorf("goal %s references missing intent %s", id, state.Value.IntentID)
		}
	}
	for id, state := range snapshot.Tasks {
		task := state.Value
		if task.ID != id {
			return fmt.Errorf("task record %s has mismatched identity %s", id, task.ID)
		}
		goal, ok := snapshot.Goals[task.GoalID]
		if !ok {
			return fmt.Errorf("task %s references missing goal %s", id, task.GoalID)
		}
		intent := snapshot.Intents[goal.Value.IntentID]
		switch task.AssigneeType {
		case "":
		case "AGENT":
			agent, ok := snapshot.Agents[task.AssigneeID]
			if !ok || agent.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("task %s references invalid assignee agent %s", id, task.AssigneeID)
			}
		case "TEAM":
			team, ok := snapshot.Teams[task.AssigneeID]
			if !ok || team.Value.OrganizationID != intent.Value.OrganizationID {
				return fmt.Errorf("task %s references invalid assignee team %s", id, task.AssigneeID)
			}
		default:
			return fmt.Errorf("task %s has unsupported assignee type %s", id, task.AssigneeType)
		}
	}
	return nil
}
