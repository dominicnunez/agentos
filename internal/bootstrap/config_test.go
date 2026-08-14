package bootstrap

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func testProviderPolicy(organization string, uid int, provider, model, profile string, mode inference.AccessMode) inference.Policy {
	now := time.Now().UTC()
	policy := inference.Policy{
		Version: inference.PolicyVersion, OrganizationID: organization, Provider: provider, Model: model,
		ExecutionProfileVersion: profile, Mode: mode, MaxInputTokensPerRequest: 100,
		MaxOutputTokensPerRequest: 100, MaxTokensPerWindow: 10_000, ContinuityReserveTokens: 100,
		WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1,
		AuthorizedBy: "local-uid-" + strconv.Itoa(uid), AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour),
	}
	if mode == inference.MeteredAPI {
		policy.Pricing = &inference.Pricing{InputNanoUSDPerMillionTokens: 1, OutputNanoUSDPerMillionTokens: 1, MaxCostNanoUSDPerRequest: 2, MaxCostNanoUSDPerWindow: 100, ExpiresAt: now.Add(time.Hour)}
	}
	return policy
}

func testOpenAIProvider(config Config, model string) Provider {
	return Provider{
		Kind: ProviderOpenAIAPI, Model: model, SecretRef: "openai-api-key",
		InferencePolicy: testProviderPolicy(config.Organization, config.Owner.UID, "openai-api", model, "v1-openai-responses-model-only", inference.MeteredAPI),
	}
}

func TestSystemAndUserPathsAreStable(t *testing.T) {
	system := SystemPaths()
	if system.ConfigDir != "/etc/agentos" || system.Database != "/var/lib/agentos/agentos.db" || system.UserSocket != "/run/agentos/user.sock" {
		t.Fatalf("unexpected system paths: %#v", system)
	}
	home := filepath.Join(t.TempDir(), "home", "alice")
	runtime := filepath.Join(t.TempDir(), "run", "1000")
	user, err := UserPaths(home, runtime, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if user.ConfigDir != filepath.Join(home, ".config", "agentos") || user.UserSocket != filepath.Join(runtime, "agentos", "user.sock") {
		t.Fatalf("unexpected user paths: %#v", user)
	}
}

func TestUserPathsRequireSecureRuntimeDirectory(t *testing.T) {
	if _, err := UserPaths(filepath.Join(t.TempDir(), "home", "alice"), "", 1000); err == nil || !strings.Contains(err.Error(), "XDG_RUNTIME_DIR") {
		t.Fatalf("shared temporary runtime fallback was accepted: %v", err)
	}
}

func TestReadyConfigurationRequiresRealTypedProvider(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home", "alice"), filepath.Join(t.TempDir(), "run", "1000"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, time.Now())
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "real model provider") {
		t.Fatalf("expected provider blocker, got %v", err)
	}
	fake := testOpenAIProvider(config, "fake")
	fake.Kind = "fake"
	config.Providers = []Provider{fake}
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "codex-subscription or openai-api") {
		t.Fatalf("expected fake provider rejection, got %v", err)
	}
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-5.4-2026-06-01")}
	if err := config.ValidateReady(); err != nil {
		t.Fatal(err)
	}
}

func TestSystemOwnerMayBeVerifiedRootAccount(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "root"), filepath.Join(t.TempDir(), "run"), 0)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeSystem, Owner{Username: "root", UID: 0, GID: 0}, paths, time.Now())
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-test-2026-01-01")}
	if err := config.ValidateReady(); err != nil {
		t.Fatalf("verified root owner was rejected: %v", err)
	}
}

func TestConfigurationRejectsUnitAndCredentialInjection(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice\nUser=root", UID: 1000, GID: 1000}, paths, time.Now())
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-test-2026-01-01")}
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "verified Linux user") {
		t.Fatalf("unsafe owner was accepted: %v", err)
	}
	config.Owner.Username = "alice"
	config.Providers[0].SecretRef = "key:../../override"
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "secret reference is invalid") {
		t.Fatalf("unsafe credential reference was accepted: %v", err)
	}
}

func TestConfigurationRejectsImplicitRemoteA2A(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, time.Now())
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-test-2026-01-01")}
	config.A2A.ListenAddress = "0.0.0.0:8080"
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "enabled explicitly") {
		t.Fatalf("implicit remote A2A was accepted: %v", err)
	}
	config.A2A.AllowRemote = true
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "actor registry and TLS") {
		t.Fatalf("remote A2A without identity and TLS was accepted: %v", err)
	}
}

func TestConfigurationConfinesReviewedA2ASources(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, time.Now())
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-test-2026-01-01")}
	config.A2A.ActorsFile = filepath.Join(t.TempDir(), "actors.json")
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "inside the configuration directory") {
		t.Fatalf("external actor registry path was accepted: %v", err)
	}
	config.A2A.ActorsFile = filepath.Join(paths.ConfigDir, "a2a-actors.json")
	config.A2A.TLSCertFile = filepath.Join(paths.ConfigDir, "tls", "server.crt")
	config.A2A.TLSKeyFile = filepath.Join(paths.ConfigDir, "tls", "server.key")
	config.A2A.PublicURL = "https://127.0.0.1:8080"
	if err := config.ValidateReady(); err != nil {
		t.Fatalf("configuration-confined A2A sources were rejected: %v", err)
	}
}

func TestCodexCredentialStoreCannotEscapeStateDirectory(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, time.Now())
	config.Providers = []Provider{{
		Kind: ProviderCodexSubscription, Model: "gpt-codex-test", SecretRef: "codex-store-key-test",
		CodexBinary: filepath.Join(paths.DataDir, "bin", "codex"), CodexCredential: filepath.Join(paths.DataDir, "outside.enc"),
		InferencePolicy: testProviderPolicy(config.Organization, config.Owner.UID, "codex-subscription", "gpt-codex-test", "v1-codex-subscription-restricted", inference.Subscription),
	}}
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "inside the state directory") {
		t.Fatalf("escaping Codex credential store was accepted: %v", err)
	}
	config.Providers[0].CodexCredential = filepath.Join(paths.StateDir, "providers", "codex.enc")
	if err := config.ValidateReady(); err != nil {
		t.Fatalf("state-confined Codex credential store was rejected: %v", err)
	}
}

func TestConfigurationRoundTripIsStrictAndRefusesSymlink(t *testing.T) {
	directory := t.TempDir()
	paths, err := UserPaths(filepath.Join(directory, "home"), filepath.Join(directory, "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, time.Now())
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-5.4-2026-06-01")}
	path := filepath.Join(directory, "config.json")
	if err := SaveConfig(path, config); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil || loaded.Owner.UID != 1000 {
		t.Fatalf("round trip failed: %#v %v", loaded, err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("unknown configuration field was accepted")
	}
	if os.Symlink("missing", filepath.Join(directory, "link.json")) == nil {
		if err := SaveConfig(filepath.Join(directory, "link.json"), config); err == nil {
			t.Fatal("symlink target was replaced")
		}
	}
}
