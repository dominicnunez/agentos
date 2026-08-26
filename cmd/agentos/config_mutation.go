package main

import (
	"errors"
	"fmt"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/fileguard"
)

func lockAndReloadReadyConfig(configPath string, initial bootstrap.Config) (bootstrap.Config, *fileguard.ProcessLock, error) {
	lock, err := acquireConfigurationLock(initial)
	if err != nil {
		return bootstrap.Config{}, nil, fmt.Errorf("acquire configuration mutation lock: %w", err)
	}
	config, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		return bootstrap.Config{}, nil, errors.Join(fmt.Errorf("reload installation configuration: %w", err), lock.Close())
	}
	if config.Mode != initial.Mode || config.Owner != initial.Owner || config.Organization != initial.Organization || config.Paths.RuntimeDir != initial.Paths.RuntimeDir || config.Paths.ConfigDir != initial.Paths.ConfigDir {
		return bootstrap.Config{}, nil, errors.Join(fmt.Errorf("installation identity or lock path changed while waiting for exclusive access"), lock.Close())
	}
	if err := config.ValidateReady(); err != nil {
		return bootstrap.Config{}, nil, errors.Join(fmt.Errorf("reloaded installation configuration is invalid: %w", err), lock.Close())
	}
	return config, lock, nil
}
