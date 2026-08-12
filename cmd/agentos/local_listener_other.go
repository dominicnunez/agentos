//go:build !linux

package main

import (
	"context"
	"fmt"
	"net"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func listenLocalHuman(_ context.Context, _ string, _, _ int) (net.Listener, error) {
	return nil, fmt.Errorf("Agent OS V1 local user access is supported on Linux")
}

func localConnContext(ctx context.Context, _ net.Conn) context.Context { return ctx }
func effectiveUID() int                                                { return -1 }
func validateRuntimeBoundary(bootstrap.Config) error {
	return fmt.Errorf("Agent OS V1 runtime is supported on Linux")
}
