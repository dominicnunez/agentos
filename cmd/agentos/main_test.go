package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/core"
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
	config := `{"actors":[{"id":"agent-1","organization_id":"org-1","status":"ACTIVE","role":"COLLABORATOR","work_scope":"OWN","token_ref":"AGENT_1_TOKEN","review_ref":"security-review-1","expires_at":"2099-01-01T00:00:00Z","max_concurrent":2,"requests_per_minute":30}]}`
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

func TestPrintVersionDoesNotStartRuntime(t *testing.T) {
	var output bytes.Buffer
	handled, err := printVersion([]string{"--version"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if !handled || output.String() != version+"\n" {
		t.Fatalf("handled=%t output=%q", handled, output.String())
	}
	if _, err := printVersion([]string{"serve"}, &output); err == nil {
		t.Fatal("unknown command was accepted")
	}
}

func TestConfiguredProviderRequiresTypedRealProviderAndServerOwnedSecret(t *testing.T) {
	provider := bootstrap.Provider{Kind: bootstrap.ProviderOpenAIAPI, Model: "gpt-test-2026-01-01", SecretRef: "OPENAI_PROJECT_KEY"}
	model, closeModel, err := configuredProvider(context.Background(), provider, t.TempDir(), testSecrets{"OPENAI_PROJECT_KEY": "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor := model.Descriptor(); descriptor.Provider != "openai-api" || descriptor.Model != "gpt-test-2026-01-01" || descriptor.ExecutionProfileVersion != "v1-openai-responses-model-only" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if err := closeModel(); err != nil {
		t.Fatal(err)
	}

	provider.SecretRef = "MISSING"
	if _, _, err := configuredProvider(context.Background(), provider, t.TempDir(), testSecrets{}); err == nil {
		t.Fatal("unavailable OpenAI API credential was accepted")
	}
	provider.Kind = "fake"
	if _, _, err := configuredProvider(context.Background(), provider, t.TempDir(), testSecrets{}); err == nil {
		t.Fatal("fake provider was accepted by production runtime")
	}
}

func TestRuntimeCredentialDirectoryFailsClosed(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("CREDENTIALS_DIRECTORY", directory)
	want := filepath.Join(directory, "a2a-actors.json")
	if got := runtimeCredentialFile("/configured/actors.json", "a2a-actors.json"); got != want {
		t.Fatalf("runtime credential path=%q want=%q", got, want)
	}
}

func TestConfiguredExternalActorsDisablesA2AWithoutRegistry(t *testing.T) {
	registry, err := configuredExternalActors(context.Background(), "", testSecrets{})
	if err != nil || registry != nil {
		t.Fatalf("registry=%v err=%v", registry, err)
	}
}

func TestConfiguredEffectReconcilersResolveExactServerOwnedStatusChecker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reconcilers.json")
	config := `{"reconcilers":[{"organization_id":"org-1","action":"send","resource":"destination-1","status":"ACTIVE","status_url":"https://status.example/effects","token_ref":"STATUS_TOKEN","review_ref":"security-review-2","expires_at":"2099-01-01T00:00:00Z"}]}`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	registry, err := configuredEffectReconcilers(context.Background(), path, testSecrets{"STATUS_TOKEN": "configured-effect-status-token-000001"})
	if err != nil {
		t.Fatal(err)
	}
	obligation := core.EffectObligation{OrganizationID: "org-1", Action: "send", Resource: "destination-1"}
	if reconciler, ok := registry.ReconcilerFor(obligation); !ok || reconciler == nil {
		t.Fatal("configured status checker was not resolved")
	}
	obligation.Resource = "other-destination"
	if _, ok := registry.ReconcilerFor(obligation); ok {
		t.Fatal("status checker escaped its exact resource binding")
	}
}

func TestConfiguredEffectReconcilersDisableChecksWithoutRegistry(t *testing.T) {
	registry, err := configuredEffectReconcilers(context.Background(), "", testSecrets{})
	if err != nil || registry != nil {
		t.Fatalf("registry=%v err=%v", registry, err)
	}
}

func TestConfiguredA2AAddressIsLoopbackByDefaultAndRemoteIsExplicit(t *testing.T) {
	address, remote, err := configuredA2AAddress(bootstrap.A2A{})
	if err != nil || address != "127.0.0.1:8080" || remote {
		t.Fatalf("default address=%q remote=%t err=%v", address, remote, err)
	}
	if _, _, err := configuredA2AAddress(bootstrap.A2A{ListenAddress: "0.0.0.0:8080"}); err == nil {
		t.Fatal("remote listener was enabled implicitly")
	}
	address, remote, err = configuredA2AAddress(bootstrap.A2A{ListenAddress: "0.0.0.0:8080", AllowRemote: true})
	if err != nil || address != "0.0.0.0:8080" || !remote {
		t.Fatalf("explicit address=%q remote=%t err=%v", address, remote, err)
	}
}

func TestRemoteExposureRequiresTLSAndAnHTTPSPublicOrigin(t *testing.T) {
	for _, test := range []struct {
		name       string
		url        string
		remote     bool
		a2aEnabled bool
		tlsEnabled bool
		wantError  bool
	}{
		{name: "loopback HTTP", url: "http://127.0.0.1:8080", a2aEnabled: true},
		{name: "remote HTTPS", url: "https://agentos.example", remote: true, a2aEnabled: true, tlsEnabled: true},
		{name: "remote HTTP", url: "http://agentos.example", remote: true, a2aEnabled: true, wantError: true},
		{name: "remote missing origin", remote: true, a2aEnabled: true, wantError: true},
		{name: "TLS listener with HTTP origin", url: "http://127.0.0.1:8080", tlsEnabled: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePublicURL(test.url, test.remote, test.a2aEnabled, test.tlsEnabled)
			if (err != nil) != test.wantError {
				t.Fatalf("err=%v wantError=%t", err, test.wantError)
			}
		})
	}
}

func TestConfiguredTLSFailsClosedForRemoteListeners(t *testing.T) {
	if _, err := configuredTLSValues(true, "", ""); err == nil {
		t.Fatal("remote listener accepted without TLS")
	}
	if _, err := configuredTLSValues(false, "server.crt", ""); err == nil {
		t.Fatal("one-sided TLS configuration was accepted")
	}
	config, err := configuredTLSValues(true, "server.crt", "server.key")
	if err != nil || config == nil || config.MinVersion != tls.VersionTLS13 {
		t.Fatalf("TLS config=%+v err=%v", config, err)
	}
}

func TestServeStopsCleanlyWhenRuntimeContextIsCancelled(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }),
		ReadHeaderTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, server, listener, "", "")
	}()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String(), nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request) //nolint:gosec // loopback listener owned by this test
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		cancel()
		t.Fatalf("status=%d", response.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned error during graceful shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop after runtime cancellation")
	}
}

func TestServeAllStopsBothListenersWhenRuntimeContextIsCancelled(t *testing.T) {
	listeners := make([]net.Listener, 0, 2)
	bindings := make([]serverBinding, 0, 2)
	for range 2 {
		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		bindings = append(bindings, serverBinding{
			server:   &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }), ReadHeaderTimeout: time.Second},
			listener: listener,
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveAll(ctx, bindings) }()
	for _, listener := range listeners {
		response, err := http.Get("http://" + listener.Addr().String()) //nolint:gosec,noctx // loopback listener owned by this test
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			cancel()
			t.Fatalf("status=%d", response.StatusCode)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveAll returned error during graceful shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("servers did not stop after runtime cancellation")
	}
}
