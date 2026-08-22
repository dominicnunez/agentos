//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/dashboard"
)

func runDashboard(ctx context.Context, config bootstrap.Config, output io.Writer) error {
	if effectiveUID() != config.Owner.UID {
		return fmt.Errorf("dashboard must be launched by installation owner UID %d", config.Owner.UID)
	}
	upstream, err := localHTTPClient(config)
	if err != nil {
		return err
	}
	defer upstream.CloseIdleConnections()
	assets, err := dashboard.New()
	if err != nil {
		return fmt.Errorf("load embedded dashboard: %w", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen on dashboard loopback boundary: %w", err)
	}
	host := listener.Addr().String()
	bootstrapToken, err := randomDashboardToken()
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("create dashboard bootstrap token: %w", err)
	}
	bootstrapPath, err := writeDashboardBootstrap(host, bootstrapToken)
	if err != nil {
		_ = listener.Close()
		return err
	}
	defer func() { _ = os.Remove(bootstrapPath) }()
	bridge, err := newDashboardBridge(upstream, assets, config.Organization, config.Mode, version, host, bootstrapToken, time.Now, func() { _ = os.Remove(bootstrapPath) })
	if err != nil {
		_ = listener.Close()
		return err
	}
	server := newHTTPServer(host, bridge, nil)
	if _, err := fmt.Fprintf(output, "Agent OS dashboard listening at http://%s/\n", host); err != nil {
		_ = listener.Close()
		return err
	}
	if _, err := fmt.Fprintf(output, "Private manual launch: open this file in a local browser:\n%s\n", bootstrapPath); err != nil {
		_ = listener.Close()
		return err
	}
	if err := startDashboardBrowser(ctx, bootstrapPath); err != nil {
		_, _ = fmt.Fprintln(output, "Automatic browser launch unavailable; use the private manual launch path above.")
	}
	return serveAll(ctx, []serverBinding{{server: server, listener: listener}})
}

func startDashboardBrowser(ctx context.Context, bootstrapPath string) error {
	opener, err := exec.LookPath("xdg-open")
	if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, opener, bootstrapPath)
	if err := command.Start(); err != nil {
		return err
	}
	go func() { _ = command.Wait() }()
	return nil
}

func writeDashboardBootstrap(host, token string) (path string, err error) {
	if !strings.HasPrefix(host, "127.0.0.1:") || token == "" || strings.ContainsAny(host+token, "\"'<>\\\r\n") {
		return "", fmt.Errorf("dashboard bootstrap values are invalid")
	}
	script := `location.replace("http://` + host + `/#bootstrap=` + token + `");`
	digest := sha256.Sum256([]byte(script))
	policy := "default-src 'none'; base-uri 'none'; script-src 'sha256-" + base64.StdEncoding.EncodeToString(digest[:]) + "'"
	body := "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"referrer\" content=\"no-referrer\"><meta http-equiv=\"Content-Security-Policy\" content=\"" + policy + "\"><title>Open Agent OS</title></head><body><script>" + script + "</script></body></html>\n"
	file, err := os.CreateTemp("", "agentos-dashboard-*.html")
	if err != nil {
		return "", fmt.Errorf("create private dashboard bootstrap: %w", err)
	}
	path = file.Name()
	defer func() {
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(path)
			path = ""
		}
	}()
	if err = file.Chmod(0o600); err != nil {
		return path, fmt.Errorf("seal dashboard bootstrap permissions: %w", err)
	}
	if _, err = io.WriteString(file, body); err != nil {
		return path, fmt.Errorf("write dashboard bootstrap: %w", err)
	}
	if err = file.Sync(); err != nil {
		return path, fmt.Errorf("sync dashboard bootstrap: %w", err)
	}
	return path, nil
}
