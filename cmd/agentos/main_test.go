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

func TestConfiguredModelIsFakeUnlessExplicitlySelected(t *testing.T) {
	t.Setenv("AGENTOS_MODEL_PROVIDER", "")
	model, closeModel, err := configuredModel(context.Background(), testSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor := model.Descriptor(); descriptor.Provider != "fake" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if err := closeModel(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGENTOS_MODEL_PROVIDER", "fake-review")
	model, closeModel, err = configuredModel(context.Background(), testSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor := model.Descriptor(); descriptor.Provider != "fake-review" || descriptor.Model != "fake-review-model/v1" || descriptor.ExecutionProfileVersion != "v1-fake-review" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if err := closeModel(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGENTOS_MODEL_PROVIDER", "unreviewed")
	if _, _, err := configuredModel(context.Background(), testSecrets{}); err == nil {
		t.Fatal("unknown provider was accepted")
	}
}

func TestConfiguredCodexProviderFailsClosedWithoutExactFiles(t *testing.T) {
	t.Setenv("AGENTOS_MODEL_PROVIDER", "codex-subscription")
	t.Setenv("AGENTOS_CODEX_BINARY", "")
	t.Setenv("AGENTOS_CODEX_CREDENTIALS_FILE", "")
	t.Setenv("AGENTOS_CODEX_MODEL", "gpt-test")
	if _, _, err := configuredModel(context.Background(), testSecrets{}); err == nil {
		t.Fatal("incomplete Codex provider configuration was accepted")
	}
}

func TestFakeReviewProviderIsRestrictedToLoopback(t *testing.T) {
	if err := validateModelExposure("fake-review", false); err != nil {
		t.Fatal(err)
	}
	if err := validateModelExposure("fake-review", true); err == nil {
		t.Fatal("fake-review provider was accepted for remote exposure")
	}
	for _, provider := range []string{"", "fake", "codex-subscription", "openai-api"} {
		if err := validateModelExposure(provider, true); err != nil {
			t.Fatalf("provider %q was rejected by the fake-review exposure guard: %v", provider, err)
		}
	}
}

func TestConfiguredOpenAIAPIProviderUsesNamedServerOwnedSecret(t *testing.T) {
	t.Setenv("AGENTOS_MODEL_PROVIDER", "openai-api")
	t.Setenv("AGENTOS_OPENAI_API_KEY_REF", "OPENAI_PROJECT_KEY")
	t.Setenv("AGENTOS_OPENAI_MODEL", "gpt-test-2026-01-01")
	model, closeModel, err := configuredModel(context.Background(), testSecrets{"OPENAI_PROJECT_KEY": "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if descriptor := model.Descriptor(); descriptor.Provider != "openai-api" || descriptor.Model != "gpt-test-2026-01-01" || descriptor.ExecutionProfileVersion != "v1-openai-responses-model-only" {
		t.Fatalf("descriptor=%+v", descriptor)
	}
	if err := closeModel(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGENTOS_OPENAI_API_KEY_REF", "MISSING")
	if _, _, err := configuredModel(context.Background(), testSecrets{}); err == nil {
		t.Fatal("unavailable OpenAI API credential was accepted")
	}
	t.Setenv("AGENTOS_OPENAI_API_KEY_REF", "")
	if _, _, err := configuredModel(context.Background(), testSecrets{}); err == nil {
		t.Fatal("missing OpenAI API credential reference was accepted")
	}
}

func TestConfiguredHumanActorsResolvesReviewedRole(t *testing.T) {
	path := filepath.Join(t.TempDir(), "human-actors.json")
	config := `{"actors":[{"id":"human-1","organization_id":"org-1","status":"ACTIVE","role":"OPERATOR","work_scope":"ORGANIZATION","token_ref":"HUMAN_1_TOKEN","review_ref":"security-review-2","expires_at":"2099-01-01T00:00:00Z","max_concurrent":2,"requests_per_minute":30}]}`
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	const token = "configured-human-operator-token-00001"
	registry, err := configuredHumanActors(context.Background(), path, testSecrets{"HUMAN_1_TOKEN": token})
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Acquire(token)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	if session.Principal.ID != "human-1" || session.Principal.Kind != core.PrincipalHuman || !session.Principal.Allowed(intake.CapabilityReadResult) {
		t.Fatalf("principal=%+v", session.Principal)
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
	t.Setenv("AGENTOS_TLS_CERT_FILE", "")
	t.Setenv("AGENTOS_TLS_KEY_FILE", "")
	if _, err := configuredTLS(true); err == nil {
		t.Fatal("remote listener accepted without TLS")
	}
	t.Setenv("AGENTOS_TLS_CERT_FILE", "server.crt")
	if _, err := configuredTLS(false); err == nil {
		t.Fatal("one-sided TLS configuration was accepted")
	}
	t.Setenv("AGENTOS_TLS_KEY_FILE", "server.key")
	config, err := configuredTLS(true)
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
