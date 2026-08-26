//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func testIntegrityAnchor(config bootstrap.Config) bootstrap.IntegrityAnchor {
	publicKey := make([]byte, 32)
	digest := sha256.Sum256(publicKey)
	return bootstrap.IntegrityAnchor{
		InstallationID:     "install-" + strings.Repeat("ab", 32),
		CheckpointFile:     filepath.Join(config.Paths.StateDir, "ledger-anchor.json"),
		PublicKey:          base64.StdEncoding.EncodeToString(publicKey),
		KeyID:              fmt.Sprintf("%x", digest[:]),
		SecretRef:          "ledger-anchor-signing-key",
		SignatureAlgorithm: "Ed25519",
	}
}

func TestSystemdUnitsQuoteConfiguredPathsAndPercentSpecifiers(t *testing.T) {
	config := bootstrap.NewConfig(bootstrap.ModeSystem, bootstrap.Owner{Username: "root", UID: 0, GID: 0}, bootstrap.SystemPaths(), time.Now())
	config.Paths.Workspace = "/var/lib/agentos/work spaces/%n"
	config.Providers = []bootstrap.Provider{testOpenAIProvider(config, "gpt-test-2026-01-01", "openai-api-key")}
	config.Integrity = testIntegrityAnchor(config)
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
	if !strings.Contains(unit, `LoadCredentialEncrypted="ledger-anchor-signing-key:/etc/agentos/credentials/ledger-anchor-signing-key.cred"`) {
		t.Fatalf("ledger anchor credential directive was not quoted:\n%s", unit)
	}
	if !strings.Contains(unit, "RuntimeDirectory=agentos-private\nRuntimeDirectoryMode=0700") || !strings.Contains(unit, `"/run/agentos-private"`) {
		t.Fatalf("private provider runtime directory is missing:\n%s", unit)
	}
	if !strings.Contains(unit, "UMask=0077") {
		t.Fatal("system service does not enforce a private file-creation mask")
	}
	if !strings.Contains(unit, `ExecStartPre=/usr/bin/test ! -e "/run/agentos/integrity-maintenance.lock"`) {
		t.Fatal("system service can start during ledger integrity maintenance")
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
	config.Providers = []bootstrap.Provider{testOpenAIProvider(config, "gpt-test-2026-01-01", "openai-api-key")}
	config.Integrity = testIntegrityAnchor(config)
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
	if !strings.Contains(unit, `LoadCredential="a2a-actors.json:`) || !strings.Contains(unit, `LoadCredentialEncrypted="agent-1-token:`) || !strings.Contains(unit, `LoadCredentialEncrypted="ledger-anchor-signing-key:`) {
		t.Fatalf("A2A credential directives are missing:\n%s", unit)
	}
	if !strings.Contains(unit, "UMask=0077") {
		t.Fatal("user service does not enforce a private file-creation mask")
	}
	if !strings.Contains(unit, "ExecStartPre=/usr/bin/test ! -e ") || !strings.Contains(unit, "integrity-maintenance.lock") {
		t.Fatal("user service can start during ledger integrity maintenance")
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

func TestUserRuntimeBaseMustBePrivateAndOwned(t *testing.T) {
	base := filepath.Join(t.TempDir(), "runtime")
	if err := os.Mkdir(base, 0o700); err != nil {
		t.Fatal(err)
	}
	config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{Username: "owner", UID: effectiveUID(), GID: effectiveUID()}, bootstrap.Paths{RuntimeDir: filepath.Join(base, "agentos")}, time.Now())
	if err := validateUserRuntimeBase(config); err != nil {
		t.Fatalf("private owned runtime base was rejected: %v", err)
	}
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateUserRuntimeBase(config); err == nil {
		t.Fatal("broad user runtime base was accepted")
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

func TestSystemWorkspaceRejectsMissingParents(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "missing", "workspace")
	err := prepareSystemWorkspace(workspace, bootstrap.Owner{Username: "root", UID: effectiveUID(), GID: effectiveUID()}, effectiveUID(), effectiveUID())
	if err == nil {
		t.Fatalf("workspace with missing parent was accepted: %v", err)
	}
	if _, statErr := os.Stat(filepath.Dir(workspace)); !os.IsNotExist(statErr) {
		t.Fatalf("workspace parent was created: %v", statErr)
	}
}

func TestSystemWorkspaceRejectsUntrustedExistingParent(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "untrusted")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	err := prepareSystemWorkspace(filepath.Join(parent, "workspace"), bootstrap.Owner{Username: "owner", UID: effectiveUID() + 1, GID: effectiveUID() + 1}, effectiveUID()+2, effectiveUID()+2)
	if err == nil || !strings.Contains(err.Error(), "owned by root") {
		t.Fatalf("workspace with an untrusted parent was accepted: %v", err)
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

func TestUserCheckpointAccessIsPrivateAndRejectsLinks(t *testing.T) {
	directory := t.TempDir()
	checkpoint := filepath.Join(directory, "ledger-anchor.json")
	if err := os.WriteFile(checkpoint, []byte("checkpoint"), 0o640); err != nil {
		t.Fatal(err)
	}
	uid, gid, err := fileIdentity(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	config := bootstrap.Config{
		Mode: bootstrap.ModeUser, Owner: bootstrap.Owner{Username: "owner", UID: uid, GID: gid},
		Integrity: bootstrap.IntegrityAnchor{CheckpointFile: checkpoint},
	}
	if err := prepareIntegrityCheckpointAccess(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("checkpoint mode=%v", info.Mode().Perm())
	}
	if err := doctorIntegrityCheckpointAccess(context.Background(), config); err != nil {
		t.Fatalf("doctor rejected protected checkpoint: %v", err)
	}
	if err := os.Chmod(checkpoint, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := doctorIntegrityCheckpointAccess(context.Background(), config); err == nil {
		t.Fatal("doctor accepted group-readable checkpoint")
	}
	if err := os.Chmod(checkpoint, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "ledger-anchor-link.json")
	if err := os.Symlink(checkpoint, link); err != nil {
		t.Fatal(err)
	}
	config.Integrity.CheckpointFile = link
	if err := prepareIntegrityCheckpointAccess(context.Background(), config); err == nil {
		t.Fatal("checkpoint symlink was accepted")
	}
	if err := doctorIntegrityCheckpointAccess(context.Background(), config); err == nil {
		t.Fatal("doctor accepted checkpoint symlink")
	}
}

func TestUserDatabaseAccessIsPrivateAndRejectsLinks(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "agentos.db")
	if err := os.WriteFile(database, []byte("database"), 0o640); err != nil {
		t.Fatal(err)
	}
	wal := database + "-wal"
	if err := os.WriteFile(wal, []byte("wal"), 0o640); err != nil {
		t.Fatal(err)
	}
	uid, gid, err := fileIdentity(database)
	if err != nil {
		t.Fatal(err)
	}
	config := bootstrap.Config{
		Mode: bootstrap.ModeUser, Owner: bootstrap.Owner{Username: "owner", UID: uid, GID: gid},
		Paths: bootstrap.Paths{Database: database},
	}
	if err := prepareLedgerDatabaseAccess(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{database, wal} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode=%v", path, info.Mode().Perm())
		}
	}
	link := filepath.Join(directory, "agentos-link.db")
	if err := os.Symlink(database, link); err != nil {
		t.Fatal(err)
	}
	config.Paths.Database = link
	if err := prepareLedgerDatabaseAccess(context.Background(), config); err == nil {
		t.Fatal("database symlink was accepted")
	}
}

func fileIdentity(path string) (int, int, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("file ownership is unavailable")
	}
	return int(stat.Uid), int(stat.Gid), nil
}
