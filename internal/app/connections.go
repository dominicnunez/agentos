package app

import (
	"fmt"
	"github.com/dominicnunez/agentos/internal/assignment"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
)

// TaskConnectionRouting is trusted installation policy keyed by exact planned
// task keys. It selects accounts only; capability and budget checks still apply.
type TaskConnectionRouting struct {
	Default   string
	ByTaskKey map[string]string
}

// NewWithConnections composes all configured guarded accounts. The default is
// used for new assignments; an existing task always resolves its pinned profile.
func NewWithConnections(g *events.Gateway, registry *inference.ConnectionRegistry, routing TaskConnectionRouting, planner planning.Planner) (*Service, error) {
	if g == nil || planner == nil {
		return nil, fmt.Errorf("gateway and planner are required")
	}
	model, err := registry.Adapter(routing.Default)
	if err != nil {
		return nil, err
	}
	routes := make(map[string]*execution.AgentExecution)
	for _, id := range registry.Connections() {
		adapter, err := registry.Adapter(id)
		if err != nil {
			return nil, err
		}
		routes[id] = execution.NewAgentExecution(adapter)
	}
	taskConnections := make(map[string]string, len(routing.ByTaskKey))
	if len(routing.ByTaskKey) > 1024 {
		return nil, fmt.Errorf("too many task connection rules")
	}
	for key, connection := range routing.ByTaskKey {
		if !planning.ValidTaskKey(key) {
			return nil, fmt.Errorf("task connection rule key is invalid")
		}
		if _, err := registry.Adapter(connection); err != nil {
			return nil, err
		}
		taskConnections[key] = connection
	}
	service := NewWithModelAndPlanner(g, model, planner)
	service.agentRoutes = routes
	service.taskConnections = taskConnections
	return service, nil
}

func (s *Service) resolveAssigned(snapshot projections.Snapshot, organizationID core.ID, task core.Task) (assignment.Selection, error) {
	requirement := s.assignmentRequirement(organizationID, task.ExecutionKind)
	if task.ExecutionKind == core.ExecutionAgent && s.agentRoutes != nil {
		if task.AgentConfig == nil {
			return assignment.Selection{}, fmt.Errorf("task has no pinned execution profile")
		}
		profile, ok := snapshot.ExecutionProfiles[task.AgentConfig.ProfileID]
		if !ok {
			return assignment.Selection{}, fmt.Errorf("task execution profile is unavailable")
		}
		route, ok := s.agentRoutes[profile.Value.ConnectionID]
		if !ok {
			return assignment.Selection{}, fmt.Errorf("task connection is not configured")
		}
		descriptor := route.Descriptor()
		requirement.ConnectionID = profile.Value.ConnectionID
		requirement.ModelProvider, requirement.Model = descriptor.Provider, descriptor.Model
		requirement.ExecutionProfileVersion = descriptor.ExecutionProfileVersion
	}
	return assignment.ResolveAssigned(assignmentRoster(snapshot), task, requirement)
}

func (s *Service) plannedAssignmentRequirement(organizationID core.ID, task core.PlanTask) (assignment.Requirement, error) {
	requirement := s.assignmentRequirement(organizationID, task.ExecutionKind)
	if task.ExecutionKind != core.ExecutionAgent || s.agentRoutes == nil {
		return requirement, nil
	}
	connection := s.agentConnection
	if selected, ok := s.taskConnections[task.Key]; ok {
		connection = selected
	}
	route, ok := s.agentRoutes[connection]
	if !ok {
		return assignment.Requirement{}, fmt.Errorf("planned task connection is not configured")
	}
	descriptor := route.Descriptor()
	requirement.ConnectionID = connection
	requirement.ModelProvider, requirement.Model = descriptor.Provider, descriptor.Model
	requirement.ExecutionProfileVersion = descriptor.ExecutionProfileVersion
	return requirement, nil
}
