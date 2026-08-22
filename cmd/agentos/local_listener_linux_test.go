//go:build linux

package main

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func TestLocalUserSocketDerivesOwnerFromKernelPeerCredentials(t *testing.T) {
	uid, gid := syscall.Geteuid(), syscall.Getegid()
	runtimeBase := t.TempDir()
	if err := os.Chmod(runtimeBase, 0o700); err != nil {
		t.Fatal(err)
	}
	runtimeDir := filepath.Join(runtimeBase, "agentos")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(runtimeDir, "user.sock")
	listener, err := listenLocalHuman(t.Context(), socketPath, uid, gid)
	if err != nil {
		t.Fatal(err)
	}

	store, err := ledger.Open(":memory:")
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := intake.New(app.New(events.NewGateway(store)))
	owner, err := gateway.NewHuman(service, gateway.LocalHuman{
		UID: uid, ID: "local-owner", OrganizationID: "org-1", MaxConcurrent: 2, RequestsPerMinute: 10,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	otherOwner, err := gateway.NewHuman(service, gateway.LocalHuman{
		UID: uid + 1, ID: "different-owner", OrganizationID: "org-1", MaxConcurrent: 2, RequestsPerMinute: 10,
	})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := r.Clone(r.Context())
		request.URL.Path = "/identity-probe"
		switch r.URL.Path {
		case "/owner":
			owner.ServeHTTP(w, request)
		case "/other-owner":
			otherOwner.ServeHTTP(w, request)
		default:
			http.NotFound(w, r)
		}
	})
	server := &http.Server{
		Handler:           handler,
		ConnContext:       localConnContext,
		ReadHeaderTimeout: 2 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-serveErrors; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, os.ErrClosed) {
			t.Errorf("serve local user socket: %v", err)
		}
	})

	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{
		Username: "current-user", UID: uid, GID: gid,
	}, bootstrap.Paths{RuntimeDir: runtimeDir, UserSocket: socketPath}, time.Now())
	client, err := localHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.CloseIdleConnections() })

	response := localUserSocketRequest(t, client, "/owner", "")
	if response.StatusCode != http.StatusNotFound {
		_ = response.Body.Close()
		t.Fatalf("kernel-authenticated owner status=%d", response.StatusCode)
	}
	_ = response.Body.Close()

	response = localUserSocketRequest(t, client, "/other-owner", strconv.Itoa(uid+1))
	if response.StatusCode != http.StatusUnauthorized {
		_ = response.Body.Close()
		t.Fatalf("forged owner header status=%d", response.StatusCode)
	}
	_ = response.Body.Close()
}

func localUserSocketRequest(t *testing.T, client *http.Client, path, forgedUID string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://agentos.local"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if forgedUID != "" {
		request.Header.Set("X-AgentOS-UID", forgedUID)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
