//go:build linux

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func localHTTPClient(config bootstrap.Config) (*http.Client, error) {
	socket := config.Paths.UserSocket
	if !filepath.IsAbs(socket) {
		return nil, fmt.Errorf("local user socket path must be absolute")
	}
	if err := validateLocalGatewaySocket(config); err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		DisableCompression:    true,
		MaxIdleConns:          2,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "unix", socket)
		},
	}
	return &http.Client{Transport: transport, Timeout: 35 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

func validateLocalGatewaySocket(config bootstrap.Config) error {
	info, err := os.Lstat(config.Paths.UserSocket)
	if err != nil {
		return fmt.Errorf("inspect local user socket: %w", err)
	}
	uid, mode, err := fileOwner(config.Paths.UserSocket)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || mode&os.ModeSocket == 0 || mode.Perm() != 0o600 || uid != config.Owner.UID {
		return fmt.Errorf("local user socket ownership, type, or permissions are invalid")
	}
	if config.Mode == bootstrap.ModeUser {
		return validateUserRuntimeBase(config)
	}
	uid, mode, err = fileOwner(config.Paths.RuntimeDir)
	if err != nil || mode&os.ModeSymlink != 0 || !mode.IsDir() || mode.Perm() != 0o711 || uid != 0 {
		return fmt.Errorf("system local user socket directory is invalid")
	}
	return nil
}
