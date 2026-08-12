//go:build !linux

package main

import (
	"fmt"
	"net/http"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func localHTTPClient(_ bootstrap.Config) (*http.Client, error) {
	return nil, fmt.Errorf("Agent OS V1 local user access is supported on Linux")
}
