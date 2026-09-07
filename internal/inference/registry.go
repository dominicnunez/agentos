package inference

import (
	"context"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/execution"
)

// Connection is supplied by trusted runtime composition after resolving the
// connection's own credential source. Credentials never enter this registry's
// public metadata or the routing identity.
type Connection struct {
	ID       string
	Adapter  execution.ModelAdapter
	Metadata *RouteMetadata
}

// ConnectionRegistry owns a fixed set of guarded adapters. Lookups require an
// exact configured connection ID; provider/model aliases cannot select another
// account, and there is no implicit default or fallback connection.
type ConnectionRegistry struct {
	adapters map[string]*GuardedAdapter
	metadata map[string]RouteMetadata
	selector RouteSelector
	store    Store
}

// RouteSelector reads authoritative policy and accounting for advisory selection.
// Dispatch must still reserve atomically through the guarded adapter's store.
type RouteSelector interface {
	SelectInferenceRoute(context.Context, *ConnectionRegistry, RouteRequirements) (RouteSelection, error)
}

func NewConnectionRegistry(store Store, connections []Connection) (*ConnectionRegistry, error) {
	if store == nil || len(connections) == 0 || len(connections) > 1024 {
		return nil, fmt.Errorf("connection registry requires a store and 1 to 1024 connections")
	}
	registry := &ConnectionRegistry{adapters: make(map[string]*GuardedAdapter, len(connections)), metadata: make(map[string]RouteMetadata, len(connections))}
	registry.selector, _ = store.(RouteSelector)
	registry.store = store
	for _, connection := range connections {
		if !ValidConnectionID(connection.ID) || registry.adapters[connection.ID] != nil {
			return nil, fmt.Errorf("connection registry contains an invalid or duplicate identity")
		}
		adapter, err := NewGuardedConnectionAdapter(store, connection.Adapter, connection.ID)
		if err != nil {
			return nil, err
		}
		registry.adapters[connection.ID] = adapter
		if connection.Metadata != nil {
			metadata := *connection.Metadata
			if metadata.Validate() != nil || metadata.ConnectionID != connection.ID || metadata.Descriptor != adapter.Descriptor() {
				return nil, fmt.Errorf("connection metadata does not identify its configured adapter")
			}
			// The current adapter contract exposes text completion only. Catalog
			// configuration cannot activate tools, vision, or streaming transports.
			for _, capability := range metadata.Capabilities {
				if capability != Text {
					return nil, fmt.Errorf("connection declares an unsupported adapter capability")
				}
			}
			registry.metadata[connection.ID] = cloneRouteMetadata(metadata)
		}
	}
	return registry, nil
}

// RequiresRouting reports catalog or durable governance requirements, including
// accounts that have governance policy but no capability catalog.
func (r *ConnectionRegistry) RequiresRouting(ctx context.Context, connectionID string) (bool, error) {
	if _, err := r.Adapter(connectionID); err != nil {
		return false, err
	}
	if _, ok := r.metadata[connectionID]; ok {
		return true, nil
	}
	reader, ok := r.store.(interface {
		InferenceConnectionRequiresRouting(context.Context, string) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("inference store cannot establish account routing prerequisites")
	}
	return reader.InferenceConnectionRequiresRouting(ctx, connectionID)
}

// Select uses the same authority that admits this registry's provider calls.
// Stores without routing support can serve explicit legacy connections, but
// cannot silently substitute an unverified snapshot for capability selection.
func (r *ConnectionRegistry) Select(ctx context.Context, requirements RouteRequirements) (selection RouteSelection, resultErr error) {
	fingerprint, _ := requirements.Fingerprint()
	defer func() {
		if resultErr != nil {
			resultErr = &routeFailure{code: RouteFailureCategory(resultErr), cause: resultErr, requirementsFingerprint: fingerprint}
		}
	}()
	if r == nil || r.selector == nil {
		return RouteSelection{}, &routeFailure{code: RouteSelectionUnavailable}
	}
	if err := ctx.Err(); err != nil {
		return RouteSelection{}, &routeFailure{code: RouteCanceled, cause: err}
	}
	if _, err := requirements.Canonical(); err != nil {
		return RouteSelection{}, &routeFailure{code: RouteInvalidRequirements, cause: err}
	}
	selected, err := r.selector.SelectInferenceRoute(ctx, r, requirements.Clone())
	if err != nil {
		return RouteSelection{}, &routeFailure{code: RouteFailureCategory(err), cause: err}
	}
	if err := selected.ValidateFor(requirements); err != nil {
		return RouteSelection{}, &routeFailure{code: RouteSelectionUnavailable, cause: err}
	}
	return selected, nil
}

// Catalog returns independent, sorted metadata snapshots. Connections without
// reviewed metadata remain usable by legacy explicit routing but are absent from
// capability selection. A caller cannot add capabilities by editing this result.
func (r *ConnectionRegistry) Catalog() []RouteMetadata {
	if r == nil {
		return nil
	}
	result := make([]RouteMetadata, 0, len(r.metadata))
	for _, id := range r.Connections() {
		if metadata, ok := r.metadata[id]; ok {
			result = append(result, cloneRouteMetadata(metadata))
		}
	}
	return result
}

func cloneRouteMetadata(metadata RouteMetadata) RouteMetadata {
	metadata.Capabilities = append([]Capability(nil), metadata.Capabilities...)
	metadata.DataClasses = append([]string(nil), metadata.DataClasses...)
	return metadata
}

func (r *ConnectionRegistry) Adapter(connectionID string) (*GuardedAdapter, error) {
	if r == nil || !ValidConnectionID(connectionID) {
		return nil, fmt.Errorf("configured inference connection is required")
	}
	adapter := r.adapters[connectionID]
	if adapter == nil {
		return nil, fmt.Errorf("inference connection is not configured")
	}
	return adapter, nil
}

// Connections returns a fresh sorted list, without exposing adapters or secrets.
func (r *ConnectionRegistry) Connections() []string {
	if r == nil {
		return nil
	}
	ids := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
