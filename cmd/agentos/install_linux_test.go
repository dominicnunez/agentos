//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func TestSystemdUnitsQuoteConfiguredPathsAndPercentSpecifiers(t *testing.T) {
	config := bootstrap.NewConfig(bootstrap.ModeSystem, bootstrap.Owner{Username: "root", UID: 0, GID: 0}, bootstrap.SystemPaths(), time.Now())
	config.Paths.Workspace = "/var/lib/agentos/work spaces/%n"
	config.Providers = []bootstrap.Provider{{Kind: bootstrap.ProviderOpenAIAPI, Model: "gpt-test-2026-01-01", SecretRef: "openai-api-key"}}
	unit, err := systemServiceUnit(config)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unit, `"/var/lib/agentos/work spaces/%%n"`) || strings.Contains(unit, `"/var/lib/agentos/work spaces/%n"`) {
		t.Fatalf("configured path was not safely quoted:\n%s", unit)
	}
	if !strings.Contains(unit, `LoadCredentialEncrypted="openai-api-key:/etc/agentos/credentials/openai-api-key.cred"`) {
		t.Fatalf("credential directive was not quoted:\n%s", unit)
	}
	if !strings.Contains(unit, "RuntimeDirectory=agentos-private\nRuntimeDirectoryMode=0700") || !strings.Contains(unit, `"/run/agentos-private"`) {
		t.Fatalf("private provider runtime directory is missing:\n%s", unit)
	}
	if !strings.Contains(unit, "UMask=0077") {
		t.Fatal("system service does not enforce a private file-creation mask")
	}
	if !strings.Contains(systemSocketUnit(config), "DirectoryMode=0711") {
		t.Fatal("socket parent directory mode is not explicit")
	}
}

func TestUserServiceLoadsReviewedA2ACredentials(t *testing.T) {
	home := t.TempDir()
	paths, err := bootstrap.UserPaths(home, filepath.Join(home, "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{Username: "alice", UID: effectiveUID(), GID: effectiveUID()}, paths, time.Now())
	config.Providers = []bootstrap.Provider{{Kind: bootstrap.ProviderOpenAIAPI, Model: "gpt-test-2026-01-01", SecretRef: "openai-api-key"}}
	config.A2A.ActorsFile = filepath.Join(paths.ConfigDir, "a2a-actors.json")
	if err := os.MkdirAll(filepath.Join(paths.ConfigDir, "credentials"), 0o700); err != nil {
		t.Fatal(err)
	}
	actor := `{"actors":[{"id":"agent-1","organization_id":"default","status":"ACTIVE","role":"SUBMITTER","work_scope":"OWN","token_ref":"agent-1-token","review_ref":"review-1","expires_at":"2099-01-01T00:00:00Z","max_concurrent":2,"requests_per_minute":30}]}`
	if err := os.WriteFile(config.A2A.ActorsFile, []byte(actor), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(paths.ConfigDir, "credentials", "agent-1-token.cred"), []byte("encrypted"), 0o600); err != nil {
		t.Fatal(err)
	}
	unit, err := userServiceUnit(config, filepath.Join(home, ".local", "bin", "agentos"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(unit, `LoadCredential="a2a-actors.json:`) || !strings.Contains(unit, `LoadCredentialEncrypted="agent-1-token:`) {
		t.Fatalf("A2A credential directives are missing:\n%s", unit)
	}
	if !strings.Contains(unit, "UMask=0077") {
		t.Fatal("user service does not enforce a private file-creation mask")
	}
}

func TestEnsureOwnedRuntimeDirectoryRejectsBroadModesAndLinks(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureOwnedRuntimeDirectory(directory, effectiveUID(), 0o700); err == nil {
		t.Fatal("broad runtime directory mode was accepted")
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ensureOwnedRuntimeDirectory(directory, effectiveUID(), 0o700); err != nil {
		t.Fatalf("private owned runtime directory was rejected: %v", err)
	}
	link := filepath.Join(t.TempDir(), "runtime-link")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatal(err)
	}
	if err := ensureOwnedRuntimeDirectory(link, effectiveUID(), 0o700); err == nil {
		t.Fatal("runtime directory symlink was accepted")
	}
}

func TestReadSetupCredentialRequiresPrivateOwnedStableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, []byte(`{"tokens":{"access_token":"test"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	body, err := readSetupCredential(path, effectiveUID())
	if err != nil || len(body) == 0 {
		t.Fatalf("body=%q err=%v", body, err)
	}
	clearBytes(body)
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := readSetupCredential(path, effectiveUID()); err == nil {
		t.Fatal("group-readable credential was accepted")
	}
	link := filepath.Join(t.TempDir(), "auth-link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readSetupCredential(link, effectiveUID()); err == nil {
		t.Fatal("credential symlink was accepted")
	}
}

func TestPrivilegedDirectoryPreparationRejectsSymlinkedParents(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "redirect")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := rejectSymlinkDirectoryChain(filepath.Join(link, "workspace")); err == nil {
		t.Fatal("symlinked privileged directory chain was accepted")
	}
}

func TestSystemdCredentialArgumentsMatchServiceScope(t *testing.T) {
	system, err := systemdCredentialArguments(bootstrap.ModeSystem, "encrypt", "provider-key", "-", "/tmp/provider.cred")
	if err != nil || strings.Contains(strings.Join(system, " "), "--user") {
		t.Fatalf("system arguments=%v err=%v", system, err)
	}
	user, err := systemdCredentialArguments(bootstrap.ModeUser, "decrypt", "provider-key", "/tmp/provider.cred", "-")
	if err != nil || !strings.Contains(strings.Join(user, " "), "--user") {
		t.Fatalf("user arguments=%v err=%v", user, err)
	}
}

func TestServiceCredentialsRejectMissingProvider(t *testing.T) {
	config := bootstrap.NewConfig(bootstrap.ModeSystem, bootstrap.Owner{Username: "root", UID: 0, GID: 0}, bootstrap.SystemPaths(), time.Now())
	if _, err := serviceCredentialDirectives(config); err == nil {
		t.Fatal("service credential generation accepted a missing provider")
	}
}
