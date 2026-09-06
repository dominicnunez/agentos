package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/secrets"
)

type providerFactory func(context.Context, bootstrap.Provider, string, secrets.Source) (execution.ModelAdapter, func() error, error)

// providerCredentialSource prevents an adapter from resolving another configured
// account's secret reference through its runtime credential source.
type providerCredentialSource struct {
	source secrets.Source
	ref    secrets.Ref
}

func (s providerCredentialSource) Resolve(ctx context.Context, ref secrets.Ref) (secrets.Value, error) {
	if s.source == nil || ref != s.ref {
		return "", fmt.Errorf("provider credential reference is not authorized")
	}
	return s.source.Resolve(ctx, ref)
}

// composeProviderConnections creates every configured account before returning
// the registry. Partial initialization closes all acquired resources in reverse
// order; successful composition returns an idempotent shutdown function.
func composeProviderConnections(ctx context.Context, providers []bootstrap.Provider, runtimeDir string, source secrets.Source, store inference.Store, factory providerFactory) (*inference.ConnectionRegistry, func() error, error) {
	if ctx == nil || source == nil || store == nil || factory == nil || len(providers) == 0 || len(providers) > 1024 {
		return nil, nil, fmt.Errorf("provider composition dependencies are incomplete")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	policies := make([]inference.Policy, len(providers))
	credentialStores := map[string]bool{}
	for i, provider := range providers {
		if err := provider.Validate(); err != nil {
			return nil, nil, err
		}
		policies[i] = provider.InferencePolicy
		if provider.CodexCredential != "" {
			if credentialStores[provider.CodexCredential] {
				return nil, nil, fmt.Errorf("connections cannot share a mutable Codex credential store")
			}
			credentialStores[provider.CodexCredential] = true
		}
	}
	if err := inference.ValidatePolicySet(policies); err != nil {
		return nil, nil, err
	}
	if err := ensureOwnedRuntimeDirectory(runtimeDir, effectiveUID(), 0o700); err != nil {
		return nil, nil, err
	}
	parent := filepath.Join(runtimeDir, "connections")
	if err := ensureOwnedRuntimeDirectory(parent, effectiveUID(), 0o700); err != nil {
		return nil, nil, err
	}
	var closers []func() error
	var once sync.Once
	var closeErr error
	closeAll := func() error {
		once.Do(func() {
			for i := len(closers) - 1; i >= 0; i-- {
				closeErr = errors.Join(closeErr, closers[i]())
			}
		})
		return closeErr
	}
	fail := func(err error) (*inference.ConnectionRegistry, func() error, error) {
		return nil, nil, errors.Join(err, closeAll())
	}
	connections := make([]inference.Connection, 0, len(providers))
	for _, provider := range providers {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		id := provider.InferencePolicy.ConnectionID
		directory := filepath.Join(parent, id)
		if err := ensureOwnedRuntimeDirectory(directory, effectiveUID(), 0o700); err != nil {
			return fail(err)
		}
		adapter, closeAdapter, err := factory(ctx, provider, directory, providerCredentialSource{source: source, ref: secrets.Ref(provider.SecretRef)})
		if closeAdapter != nil {
			closers = append(closers, closeAdapter)
		}
		if err != nil {
			return fail(err)
		}
		if adapter == nil || closeAdapter == nil {
			return fail(fmt.Errorf("provider factory returned incomplete ownership"))
		}
		descriptor := adapter.Descriptor()
		policy := provider.InferencePolicy
		if descriptor.Provider != policy.Provider || descriptor.Model != policy.Model || descriptor.ExecutionProfileVersion != policy.ExecutionProfileVersion {
			return fail(fmt.Errorf("provider adapter does not match its reviewed policy"))
		}
		connections = append(connections, inference.Connection{ID: id, Adapter: adapter})
	}
	registry, err := inference.NewConnectionRegistry(store, connections)
	if err != nil {
		return fail(err)
	}
	return registry, closeAll, nil
}

type runtimeProviderModels struct {
	task, planning, normalization execution.ModelAdapter
	registry                      *inference.ConnectionRegistry
	close                         func() error
}

func composeRuntimeModels(ctx context.Context, config bootstrap.Config, source secrets.Source, store inference.Store, factory providerFactory) (runtimeProviderModels, error) {
	if err := config.Routing.Validate(config.Providers); err != nil {
		return runtimeProviderModels{}, err
	}
	if config.Routing == nil {
		raw, closeModel, err := factory(ctx, config.Providers[0], providerRuntimeDirectory(config), source)
		if err != nil {
			if closeModel != nil {
				err = errors.Join(err, closeModel())
			}
			return runtimeProviderModels{}, err
		}
		if closeModel == nil {
			return runtimeProviderModels{}, fmt.Errorf("provider cleanup is required")
		}
		guarded, err := inference.NewGuardedAdapter(store, raw)
		if err != nil {
			return runtimeProviderModels{}, errors.Join(err, closeModel())
		}
		return runtimeProviderModels{task: guarded, planning: guarded, normalization: guarded, close: closeModel}, nil
	}
	registry, closeAll, err := composeProviderConnections(ctx, config.Providers, providerRuntimeDirectory(config), source, store, factory)
	if err != nil {
		return runtimeProviderModels{}, err
	}
	selected := runtimeProviderModels{registry: registry, close: closeAll}
	selected.task, err = registry.Adapter(config.Routing.TaskDefault)
	if err == nil {
		selected.planning, err = registry.Adapter(config.Routing.Planning)
	}
	if err == nil {
		selected.normalization, err = registry.Adapter(config.Routing.Normalization)
	}
	if err != nil {
		return runtimeProviderModels{}, errors.Join(err, closeAll())
	}
	return selected, nil
}
