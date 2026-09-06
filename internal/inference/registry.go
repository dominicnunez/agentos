package inference

import (
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/execution"
)

// Connection is supplied by trusted runtime composition after resolving the
// connection's own credential source. Credentials never enter this registry's
// public metadata or the routing identity.
type Connection struct {
	ID      string
	Adapter execution.ModelAdapter
}

// ConnectionRegistry owns a fixed set of guarded adapters. Lookups require an
// exact configured connection ID; provider/model aliases cannot select another
// account, and there is no implicit default or fallback connection.
type ConnectionRegistry struct {
	adapters map[string]*GuardedAdapter
}

func NewConnectionRegistry(store Store, connections []Connection) (*ConnectionRegistry, error) {
	if store == nil || len(connections) == 0 || len(connections) > 1024 {
		return nil, fmt.Errorf("connection registry requires a store and 1 to 1024 connections")
	}
	registry := &ConnectionRegistry{adapters: make(map[string]*GuardedAdapter, len(connections))}
	for _, connection := range connections {
		if !ValidConnectionID(connection.ID) || registry.adapters[connection.ID] != nil {
			return nil, fmt.Errorf("connection registry contains an invalid or duplicate identity")
		}
		adapter, err := NewGuardedConnectionAdapter(store, connection.Adapter, connection.ID)
		if err != nil {
			return nil, err
		}
		registry.adapters[connection.ID] = adapter
	}
	return registry, nil
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
