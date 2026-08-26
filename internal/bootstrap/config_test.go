package bootstrap

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/inference"
)

func testIntegrity(config Config) IntegrityAnchor {
	publicKey := make([]byte, 32)
	digest := sha256.Sum256(publicKey)
	return IntegrityAnchor{
		InstallationID: "install-" + strings.Repeat("ab", 32),
		CheckpointFile: filepath.Join(config.Paths.StateDir, "ledger-anchor.json"),
		PublicKey:      base64.StdEncoding.EncodeToString(publicKey), KeyID: fmt.Sprintf("%x", digest[:]),
		SecretRef: "ledger-anchor-signing-key", SignatureAlgorithm: "Ed25519",
	}
}

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
	config.Integrity = testIntegrity(config)
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

func TestReadyConfigurationReservesLedgerAnchorCredentialNamespace(t *testing.T) {
	config := NewConfig(ModeSystem, Owner{Username: "root", UID: 0, GID: 0}, SystemPaths(), time.Now())
	config.Integrity = testIntegrity(config)
	provider := testOpenAIProvider(config, "gpt-test-2026-01-01")
	provider.SecretRef = "ledger-anchor-provider-confusion"
	config.Providers = []Provider{provider}
	if err := config.ValidateReady(); err == nil || !strings.Contains(err.Error(), "reserved ledger-anchor namespace") {
		t.Fatalf("reserved credential namespace was accepted: %v", err)
	}
}

func TestReadyConfigurationAllowsOnlyReviewedRestoreCheckpointNames(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, time.Now())
	config.Integrity = testIntegrity(config)
	config.Providers = []Provider{testOpenAIProvider(config, "gpt-test-2026-01-01")}
	config.Integrity.CheckpointFile = filepath.Join(config.Paths.StateDir, "ledger-anchor-restore-incident-42.json")
	if err := config.ValidateReady(); err != nil {
		t.Fatalf("reviewed restore checkpoint name was rejected: %v", err)
	}
	for _, name := range []string{"other.json", "ledger-anchor.pending", "ledger-anchor-restore-../escape.json", "ledger-anchor-transition.json"} {
		config.Integrity.CheckpointFile = filepath.Join(config.Paths.StateDir, name)
		if err := config.ValidateReady(); err == nil {
			t.Fatalf("unsafe checkpoint name %q was accepted", name)
		}
	}
}

func TestSystemOwnerMayBeVerifiedRootAccount(t *testing.T) {
	paths, err := UserPaths(filepath.Join(t.TempDir(), "root"), filepath.Join(t.TempDir(), "run"), 0)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig(ModeSystem, Owner{Username: "root", UID: 0, GID: 0}, paths, time.Now())
	config.Integrity = testIntegrity(config)
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
	config.Integrity = testIntegrity(config)
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
	config.Integrity = testIntegrity(config)
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
	config.Integrity = testIntegrity(config)
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
	config.Integrity = testIntegrity(config)
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
	config.Integrity = testIntegrity(config)
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

func TestVersion1CheckpointUpgradeRequiresReconfirmedProviderPolicy(t *testing.T) {
	now := time.Now().UTC()
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	legacy := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, now)
	legacy.Version = legacyConfigVersion
	provider := testOpenAIProvider(legacy, "gpt-test-2026-01-01")
	provider.InferencePolicy = inference.Policy{}
	legacy.Providers = []Provider{provider}
	state := State{Version: legacyConfigVersion, Mode: ModeUser, Stage: StageReady, UpdatedAt: now}

	upgraded, upgradedState, err := UpgradeVersion1Checkpoint(legacy, state)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Version != ConfigVersion || upgradedState.Version != ConfigVersion || upgradedState.Stage != StageProvider || len(upgraded.Providers) != 0 {
		t.Fatalf("upgrade did not require provider policy confirmation: config=%+v state=%+v", upgraded, upgradedState)
	}
	legacy.Owner.Username = "../root"
	if _, _, err := UpgradeVersion1Checkpoint(legacy, state); err == nil {
		t.Fatal("unsafe version-1 checkpoint was upgraded")
	}
	if _, _, err := UpgradeVersion1Checkpoint(upgraded, upgradedState); err == nil {
		t.Fatal("current checkpoint was accepted by the one-way version-1 upgrader")
	}
}

func TestVersion2CheckpointUpgradePreservesReviewedBoundaryAndRequiresAnchorEnrollment(t *testing.T) {
	now := time.Now().UTC()
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	legacy := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, now)
	legacy.Version = previousConfigVersion
	legacy.Providers = []Provider{testOpenAIProvider(legacy, "gpt-test-2026-01-01")}
	legacy.A2A.ListenAddress = "127.0.0.1:9443"
	state := State{Version: previousConfigVersion, Mode: ModeUser, Stage: StageReady, UpdatedAt: now}

	upgraded, upgradedState, err := UpgradeVersion2Checkpoint(legacy, state)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Version != ConfigVersion || upgraded.Integrity != (IntegrityAnchor{}) || len(upgraded.Providers) != 1 || upgraded.A2A != legacy.A2A || upgradedState.Version != ConfigVersion || upgradedState.Stage != StageAnchor {
		t.Fatalf("config=%+v state=%+v", upgraded, upgradedState)
	}
	if err := upgraded.ValidateReady(); err == nil || !strings.Contains(err.Error(), "ledger anchor") {
		t.Fatalf("upgraded configuration ran without anchor enrollment: %v", err)
	}

	legacy.Providers[0].SecretRef = "ledger-anchor-provider-confusion"
	if _, _, err := UpgradeVersion2Checkpoint(legacy, state); err == nil {
		t.Fatal("version-2 upgrade preserved a credential in the reserved anchor namespace")
	}
}

func TestVersion2CheckpointUpgradePreservesIncompleteSetupStage(t *testing.T) {
	now := time.Now().UTC()
	paths, err := UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []Stage{StageWorkspace, StageProvider} {
		legacy := NewConfig(ModeUser, Owner{Username: "alice", UID: 1000, GID: 1000}, paths, now)
		legacy.Version = previousConfigVersion
		state := State{Version: previousConfigVersion, Mode: ModeUser, Stage: stage, UpdatedAt: now}
		upgraded, upgradedState, err := UpgradeVersion2Checkpoint(legacy, state)
		if err != nil {
			t.Fatalf("stage %s: %v", stage, err)
		}
		if upgraded.Version != ConfigVersion || len(upgraded.Providers) != 0 || upgradedState.Stage != stage {
			t.Fatalf("stage %s was not preserved: config=%+v state=%+v", stage, upgraded, upgradedState)
		}
	}
}

func TestLoadStateRecognizesVersion1OnlyForExplicitUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "init.json")
	state := State{Version: legacyConfigVersion, Mode: ModeUser, Stage: StageReady, UpdatedAt: time.Now().UTC()}
	if err := SaveState(path, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadState(path)
	if err != nil || loaded.Version != legacyConfigVersion {
		t.Fatalf("version-1 state was not discoverable for upgrade: %+v %v", loaded, err)
	}
	state.Version = 0
	if err := SaveState(path, state); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadState(path); err == nil {
		t.Fatal("unsupported initialization state was accepted")
	}
}
