package main

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
)

// auxiliaryRoute holds immutable installation requirements. Selection returns
// an attempt-local adapter; it never changes a shared planner or normalizer.
type auxiliaryRoute struct {
	registry     *inference.ConnectionRegistry
	requirements modelinput.RouteRequirements
}

func newAuxiliaryRoute(registry *inference.ConnectionRegistry, preferred string, requirements modelinput.RouteRequirements) (auxiliaryRoute, error) {
	if _, err := registry.Adapter(preferred); err != nil {
		return auxiliaryRoute{}, err
	}
	requirements = requirements.Clone()
	if len(requirements.PreferredConnections) == 0 {
		requirements.PreferredConnections = []string{preferred}
	}
	if _, err := requirements.Canonical(); err != nil {
		return auxiliaryRoute{}, err
	}
	return auxiliaryRoute{registry: registry, requirements: requirements}, nil
}

func (r auxiliaryRoute) selectAdapter(ctx context.Context, organization string) (*inference.GuardedAdapter, *modelinput.RouteBinding, error) {
	if organization != r.requirements.OrganizationID {
		return nil, nil, fmt.Errorf("auxiliary routing requirements belong to another organization")
	}
	requirements := r.requirements.Clone()
	selected, err := r.registry.Select(ctx, requirements)
	if err != nil {
		return nil, nil, err
	}
	adapter, err := r.registry.Adapter(selected.ConnectionID)
	if err != nil {
		return nil, nil, err
	}
	return adapter, &modelinput.RouteBinding{Requirements: requirements, Decision: selected.Decision}, nil
}

type routedPlanner struct {
	planning.Planner
	route auxiliaryRoute
}

func (p routedPlanner) SelectPlanner(ctx context.Context, organization string) (planning.Planner, *modelinput.RouteBinding, error) {
	adapter, requirements, err := p.route.selectAdapter(ctx, organization)
	if err != nil {
		return nil, nil, err
	}
	selected, err := planning.NewModelPlanner(planningModel{adapter: adapter})
	return selected, requirements, err
}

type routedNormalizer struct {
	intake.Normalizer
	route auxiliaryRoute
}

func (n routedNormalizer) SelectNormalizer(ctx context.Context, organization string) (intake.Normalizer, *modelinput.RouteBinding, error) {
	adapter, requirements, err := n.route.selectAdapter(ctx, organization)
	if err != nil {
		return nil, nil, err
	}
	selected, err := intake.NewModelNormalizer(intakeModel{adapter: adapter})
	return selected, requirements, err
}
