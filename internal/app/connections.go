package app

import (
	"context"
	"fmt"
	"github.com/dominicnunez/agentos/internal/assignment"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/projections"
)

// TaskConnectionRouting is trusted installation policy keyed by exact planned
// task keys. Requirements enable broker selection; per-task entries are complete
// requirements, and explicit account rules remain hard constraints. Without
// requirements, the existing explicit routing contract applies.
type TaskConnectionRouting struct {
	Default          string
	ByTaskKey        map[string]string
	Requirements     *modelinput.RouteRequirements
	TaskRequirements map[string]modelinput.RouteRequirements
}

// NewWithConnections composes all configured guarded accounts. The default is
// preferred for broker assignments unless requirements provide preferences.
// Without requirements it is selected directly. Existing tasks retain their
// pinned profiles and requirements.
func NewWithConnections(g *events.Gateway, registry *inference.ConnectionRegistry, routing TaskConnectionRouting, planner planning.Planner) (*Service, error) {
	if g == nil || planner == nil {
		return nil, fmt.Errorf("gateway and planner are required")
	}
	model, err := registry.Adapter(routing.Default)
	if err != nil {
		return nil, err
	}
	catalogs := make(map[string]bool)
	for _, metadata := range registry.Catalog() {
		catalogs[metadata.ConnectionID] = true
	}
	requiresRouting, err := registry.RequiresRouting(context.Background(), routing.Default)
	if err != nil {
		return nil, err
	}
	if requiresRouting && routing.Requirements == nil {
		return nil, fmt.Errorf("governed task default requires routing requirements")
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
	service.connectionRegistry = registry
	service.taskRouting = modelinput.CloneRouteRequirements(routing.Requirements)
	service.taskRoutingRules = make(map[string]modelinput.RouteRequirements, len(routing.TaskRequirements))
	if len(routing.TaskRequirements) > 1024 {
		return nil, fmt.Errorf("too many task requirement rules")
	}
	validate := func(requirements modelinput.RouteRequirements) error {
		if len(catalogs) == 0 {
			return fmt.Errorf("task routing requirements need a catalog-backed account")
		}
		if _, err := requirements.Canonical(); err != nil {
			return err
		}
		ids := append([]string(nil), requirements.PreferredConnections...)
		if requirements.ConnectionID != "" {
			if !catalogs[requirements.ConnectionID] {
				return fmt.Errorf("hard task requirements need a catalog-backed account")
			}
			ids = append(ids, requirements.ConnectionID)
		}
		for _, id := range ids {
			if _, err := registry.Adapter(id); err != nil {
				return err
			}
		}
		return nil
	}
	if service.taskRouting != nil {
		if err := validate(*service.taskRouting); err != nil {
			return nil, err
		}
	}
	for key, requirements := range routing.TaskRequirements {
		if !planning.ValidTaskKey(key) {
			return nil, fmt.Errorf("task requirement rule key is invalid")
		}
		if err := validate(requirements); err != nil {
			return nil, err
		}
		service.taskRoutingRules[key] = requirements.Clone()
	}
	for key, connection := range taskConnections {
		requirements := service.taskRouting
		if specific, ok := service.taskRoutingRules[key]; ok {
			requirements = &specific
		}
		if requirements == nil {
			required, err := registry.RequiresRouting(context.Background(), connection)
			if err != nil {
				return nil, err
			}
			if required {
				return nil, fmt.Errorf("governed task route requires routing requirements")
			}
			continue
		}
		if !catalogs[connection] || (requirements.ConnectionID != "" && requirements.ConnectionID != connection) {
			return nil, fmt.Errorf("task connection conflicts with catalog routing requirements")
		}
	}
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

type plannedAssignment struct {
	RoutingDecision *modelinput.RouteDecision
	Requirement     assignment.Requirement
	Routing         *modelinput.RouteRequirements
}

func (s *Service) plannedAssignmentRoute(ctx context.Context, organizationID core.ID, task core.PlanTask) (plannedAssignment, error) {
	requirement := s.assignmentRequirement(organizationID, task.ExecutionKind)
	if task.ExecutionKind != core.ExecutionAgent || s.agentRoutes == nil {
		return plannedAssignment{Requirement: requirement}, nil
	}
	connection := s.agentConnection
	explicit, hasExplicit := s.taskConnections[task.Key]
	if selected, ok := s.taskConnections[task.Key]; ok {
		connection = selected
	}
	constraints := modelinput.CloneRouteRequirements(s.taskRouting)
	var decision *modelinput.RouteDecision
	if specific, ok := s.taskRoutingRules[task.Key]; ok {
		constraints = modelinput.CloneRouteRequirements(&specific)
	}
	if constraints != nil {
		if constraints.OrganizationID != string(organizationID) {
			return plannedAssignment{}, fmt.Errorf("task routing requirements belong to another organization")
		}
		if hasExplicit {
			if constraints.ConnectionID != "" && constraints.ConnectionID != explicit {
				return plannedAssignment{}, fmt.Errorf("task connection conflicts with routing requirements")
			}
			constraints.ConnectionID = explicit
		} else if len(constraints.PreferredConnections) == 0 {
			constraints.PreferredConnections = []string{connection}
		}
		selected, err := s.connectionRegistry.Select(ctx, *constraints)
		if err != nil {
			return plannedAssignment{}, err
		}
		connection = selected.ConnectionID
		decision = modelinput.CloneRouteDecision(&selected.Decision)
	}
	route, ok := s.agentRoutes[connection]
	if !ok {
		return plannedAssignment{}, fmt.Errorf("planned task connection is not configured")
	}
	descriptor := route.Descriptor()
	requirement.ConnectionID = connection
	requirement.ModelProvider, requirement.Model = descriptor.Provider, descriptor.Model
	requirement.ExecutionProfileVersion = descriptor.ExecutionProfileVersion
	return plannedAssignment{Requirement: requirement, Routing: constraints, RoutingDecision: decision}, nil
}
