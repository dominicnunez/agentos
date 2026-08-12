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
	// Generic instantiations read Value throughout app and projection code, but
	// Gallow cannot currently connect those reads to this generic declaration.
	// gallow-ignore-next-line unused-field
	Value T
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

// SaveNewTasks atomically creates a complete Task DAG. Every Task starts at
// version one; later transitions continue through SaveTask.
func (r *Repository) SaveNewTasks(ctx context.Context, organizationID core.ID, actorID, correlationID string, values []core.Task) error {
	if r == nil || r.gateway == nil || organizationID == "" || actorID == "" || correlationID == "" || len(values) == 0 {
		return fmt.Errorf("complete Task-DAG projection identity is required")
	}
	drafts := make([]events.ProjectionDraft, 0, len(values))
	for _, value := range values {
		if value.ID == "" {
			return fmt.Errorf("Task-DAG projection contains an empty task identity")
		}
		drafts = append(drafts, events.ProjectionDraft{
			Event: events.TrustedDraft{
				OrganizationID: string(organizationID), EventType: "TASK_CREATED", SourceActorID: actorID,
				TaskID: string(value.ID), CorrelationID: correlationID,
			},
			ProjectionKind: KindTask, RecordID: string(value.ID), Version: 1, Value: value,
		})
	}
	_, err := r.gateway.PublishProjections(ctx, drafts)
	return err
}

// SaveBlockedTask atomically persists the blocked child projection and makes
// the same Event Contract available to its parent Task for remediation.
func (r *Repository) SaveBlockedTask(ctx context.Context, organizationID core.ID, actorID, correlationID string, version int, value core.Task, detail events.TaskBlockedPayload, parentTaskID core.ID) error {
	if value.Status != core.TaskBlocked || value.ParentID == "" || value.ParentID != parentTaskID {
		return fmt.Errorf("blocked child task and exact parent are required")
	}
	return r.saveAddressed(ctx, string(organizationID), "TASK_BLOCKED", actorID, string(value.ID), correlationID, KindTask, value.ID, version, value, detail, events.RecipientTask, string(parentTaskID))
}

func (r *Repository) save(ctx context.Context, organizationID, eventType, actorID, taskID, correlationID, kind string, id core.ID, version int, value, detail any) error {
	return r.saveAddressed(ctx, organizationID, eventType, actorID, taskID, correlationID, kind, id, version, value, detail, "", "")
}

func (r *Repository) saveAddressed(ctx context.Context, organizationID, eventType, actorID, taskID, correlationID, kind string, id core.ID, version int, value, detail any, recipientScope, recipientID string) error {
	if r == nil || r.gateway == nil {
		return fmt.Errorf("durable projection gateway is required")
	}
	_, err := r.gateway.PublishProjection(ctx, events.ProjectionDraft{
		Event: events.TrustedDraft{
			OrganizationID: organizationID,
			EventType:      eventType,
			SourceActorID:  actorID,
			RecipientScope: recipientScope,
			RecipientID:    recipientID,
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
		if err := validateIdentity("organization", id, state.Value.ID); err != nil {
			return err
		}
	}
	organized := make([]organizedIdentity, 0, len(snapshot.Agents)+len(snapshot.Teams)+len(snapshot.Intents))
	for id, state := range snapshot.Agents {
		organized = append(organized, organizedIdentity{"agent", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.Teams {
		organized = append(organized, organizedIdentity{"team", id, state.Value.ID, state.Value.OrganizationID})
	}
	for id, state := range snapshot.Intents {
		organized = append(organized, organizedIdentity{"intent", id, state.Value.ID, state.Value.OrganizationID})
	}
	for _, record := range organized {
		if err := validateOrganizedIdentity(record.kind, record.recordID, record.valueID, record.organizationID, snapshot.Organizations); err != nil {
			return err
		}
	}
	for id, state := range snapshot.Teams {
		for _, memberID := range state.Value.MemberAgentIDs {
			member, ok := snapshot.Agents[memberID]
			if !ok || member.Value.OrganizationID != state.Value.OrganizationID {
				return fmt.Errorf("team %s references invalid member agent %s", id, memberID)
			}
		}
	}
	for id, state := range snapshot.Goals {
		if err := validateIdentity("goal", id, state.Value.ID); err != nil {
			return err
		}
		intent, ok := snapshot.Intents[state.Value.IntentID]
		if !ok {
			return fmt.Errorf("goal %s references missing intent %s", id, state.Value.IntentID)
		}
		if state.CorrelationID == "" || intent.CorrelationID != state.CorrelationID {
			return fmt.Errorf("goal %s crosses its intent correlation boundary", id)
		}
	}
	for id, state := range snapshot.Tasks {
		task := state.Value
		if err := validateIdentity("task", id, task.ID); err != nil {
			return err
		}
		goal, ok := snapshot.Goals[task.GoalID]
		if !ok {
			return fmt.Errorf("task %s references missing goal %s", id, task.GoalID)
		}
		if state.CorrelationID == "" || goal.CorrelationID != state.CorrelationID {
			return fmt.Errorf("task %s crosses its goal correlation boundary", id)
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
		if task.ParentID != "" {
			parent, ok := snapshot.Tasks[task.ParentID]
			if !ok || parent.Value.GoalID != task.GoalID || parent.CorrelationID != state.CorrelationID || task.ParentID == id {
				return fmt.Errorf("task %s references invalid parent %s", id, task.ParentID)
			}
		}
		for _, dependencyID := range task.DependsOn {
			dependency, ok := snapshot.Tasks[dependencyID]
			if !ok || dependency.Value.GoalID != task.GoalID || dependency.CorrelationID != state.CorrelationID || dependencyID == id {
				return fmt.Errorf("task %s references invalid dependency %s", id, dependencyID)
			}
		}
	}
	return nil
}

type organizedIdentity struct {
	kind           string
	recordID       core.ID
	valueID        core.ID
	organizationID core.ID
}

func validateIdentity(kind string, recordID, valueID core.ID) error {
	if recordID == "" || valueID != recordID {
		return fmt.Errorf("%s record %s has mismatched identity %s", kind, recordID, valueID)
	}
	return nil
}

func validateOrganizedIdentity(kind string, recordID, valueID, organizationID core.ID, organizations map[core.ID]Versioned[core.Organization]) error {
	if err := validateIdentity(kind, recordID, valueID); err != nil {
		return err
	}
	if _, ok := organizations[organizationID]; !ok {
		return fmt.Errorf("%s %s references missing organization %s", kind, recordID, organizationID)
	}
	return nil
}
