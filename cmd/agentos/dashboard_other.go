//go:build !linux

package main

import (
	"context"
	"fmt"
	"io"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func runDashboard(context.Context, bootstrap.Config, io.Writer) error {
	return fmt.Errorf("Agent OS V1 dashboard access is supported on Linux")
}
