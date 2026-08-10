package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/secrets"
)

type testSecrets map[secrets.Ref]secrets.Value

func (s testSecrets) Resolve(_ context.Context, ref secrets.Ref) (secrets.Value, error) {
	value, ok := s[ref]
	if !ok {
		return "", fmt.Errorf("secret unavailable")
	}
	return value, nil
}

func TestConfiguredExternalActorsResolvesDistinctServerOwnedIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actors.json")
	config := `{"actors":[{"id":"agent-1","organization_id":"org-1","status":"ACTIVE","role":"COLLABORATOR","work_scope":"OWN","token_ref":"AGENT_1_TOKEN","authorization_ref":"security-review-1","expires_at":"2099-01-01T00:00:00Z","max_concurrent":2,"requests_per_minute":30}]}`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	const token = "configured-external-agent-token-0001"
	registry, err := configuredExternalActors(context.Background(), path, testSecrets{"AGENT_1_TOKEN": token})
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Acquire(token)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	if session.Principal.ID != "agent-1" || session.Principal.OrganizationID != "org-1" || session.Principal.WorkScope != intake.WorkScopeOwn || !session.Principal.Allowed(intake.CapabilityProvideInput) || session.Principal.Allowed(intake.CapabilityReadResult) {
		t.Fatalf("principal=%+v", session.Principal)
	}
	if registry.HasCredential("different-external-agent-token-001") {
		t.Fatal("unknown credential matched registry")
	}
}

func TestConfiguredExternalActorsDisablesA2AWithoutRegistry(t *testing.T) {
	registry, err := configuredExternalActors(context.Background(), "", testSecrets{})
	if err != nil || registry != nil {
		t.Fatalf("registry=%v err=%v", registry, err)
	}
}

func TestConfiguredListenAddressIsLoopbackByDefaultAndRemoteIsExplicit(t *testing.T) {
	t.Setenv("AGENTOS_LISTEN_ADDR", "")
	t.Setenv("AGENTOS_ALLOW_REMOTE", "")
	address, remote, err := configuredListenAddress()
	if err != nil || address != "127.0.0.1:8080" || remote {
		t.Fatalf("default address=%q remote=%t err=%v", address, remote, err)
	}
	t.Setenv("AGENTOS_LISTEN_ADDR", "0.0.0.0:8080")
	if _, _, err := configuredListenAddress(); err == nil {
		t.Fatal("remote listener was enabled implicitly")
	}
	t.Setenv("AGENTOS_ALLOW_REMOTE", "true")
	address, remote, err = configuredListenAddress()
	if err != nil || address != "0.0.0.0:8080" || !remote {
		t.Fatalf("explicit address=%q remote=%t err=%v", address, remote, err)
	}
	t.Setenv("AGENTOS_ALLOW_REMOTE", "1")
	if _, _, err := configuredListenAddress(); err == nil {
		t.Fatal("noncanonical remote-listener switch was accepted")
	}
}

func TestRemoteA2ARequiresAnHTTPSPublicOrigin(t *testing.T) {
	for _, test := range []struct {
		name       string
		url        string
		remote     bool
		a2aEnabled bool
		wantError  bool
	}{
		{name: "loopback HTTP", url: "http://127.0.0.1:8080", a2aEnabled: true},
		{name: "remote HTTPS", url: "https://agentos.example", remote: true, a2aEnabled: true},
		{name: "remote HTTP", url: "http://agentos.example", remote: true, a2aEnabled: true, wantError: true},
		{name: "remote missing origin", remote: true, a2aEnabled: true, wantError: true},
		{name: "human only remote", remote: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePublicURL(test.url, test.remote, test.a2aEnabled)
			if (err != nil) != test.wantError {
				t.Fatalf("err=%v wantError=%t", err, test.wantError)
			}
		})
	}
}
