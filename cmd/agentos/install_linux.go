//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/fileguard"
	"github.com/dominicnunez/agentos/internal/gateway"
	ledgeranchor "github.com/dominicnunez/agentos/internal/ledger/anchor"
	"github.com/dominicnunez/agentos/internal/secrets"
)

func ensureInitPrivileges(ctx context.Context, mode bootstrap.Mode, ui *terminalUI) (bool, error) {
	if mode != bootstrap.ModeSystem || effectiveUID() == 0 {
		return false, nil
	}
	return runAdministratorSetup(ctx, ui, "administrator setup failed", "init", "--system")
}

func ensureProviderSetupPrivileges(ctx context.Context, config bootstrap.Config, ui *terminalUI) (bool, error) {
	if config.Mode != bootstrap.ModeSystem || effectiveUID() == 0 {
		return false, nil
	}
	return runAdministratorSetup(ctx, ui, "administrator provider setup failed", "setup", "provider")
}

func ensureIntegrityMaintenancePrivileges(ctx context.Context, config bootstrap.Config, ui *terminalUI, action string) (bool, error) {
	if config.Mode != bootstrap.ModeSystem || effectiveUID() == 0 {
		return false, nil
	}
	return runAdministratorSetup(ctx, ui, "administrator ledger integrity maintenance failed", "integrity", action)
}

func integrityMaintenanceAuthority(ctx context.Context, config bootstrap.Config) (string, error) {
	if config.Mode == bootstrap.ModeUser {
		if effectiveUID() != config.Owner.UID {
			return "", fmt.Errorf("user installation integrity maintenance requires its configured Linux owner")
		}
		return keyTransitionAuthority(config.Owner.UID), nil
	}
	owner, err := invokingSystemOwner(ctx)
	if err != nil {
		return "", err
	}
	if owner.UID != 0 && owner.UID != config.Owner.UID {
		return "", fmt.Errorf("system integrity maintenance requires root or the configured Linux owner")
	}
	return keyTransitionAuthority(owner.UID), nil
}

func requireIntegrityServiceStopped(ctx context.Context, config bootstrap.Config) error {
	arguments := []string{"is-active", "agentos.service"}
	if config.Mode == bootstrap.ModeUser {
		arguments = append([]string{"--user"}, arguments...)
	}
	command := exec.CommandContext(ctx, "/usr/bin/systemctl", arguments...)
	output, err := command.CombinedOutput()
	state := strings.TrimSpace(string(output))
	if err == nil || state == "active" || state == "activating" || state == "reloading" {
		return fmt.Errorf("stop agentos.service before ledger anchor key maintenance")
	}
	if state != "" && state != "inactive" && state != "failed" && state != "unknown" {
		return fmt.Errorf("cannot prove agentos.service is stopped: %s", state)
	}
	return nil
}

func keyTransitionAuthority(uid int) string { return "local-uid-" + strconv.Itoa(uid) }

func prepareIntegrityCheckpointAccess(ctx context.Context, config bootstrap.Config) error {
	info, err := os.Lstat(config.Integrity.CheckpointFile)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > ledgeranchor.MaximumFileBytes {
		return fmt.Errorf("ledger anchor checkpoint must be a bounded regular file, not a link")
	}
	if config.Mode == bootstrap.ModeSystem {
		if effectiveUID() != 0 {
			return fmt.Errorf("system checkpoint access requires administrator access")
		}
		uid, gid, identityErr := lookupNumericIdentity(ctx, "agentos")
		if identityErr != nil {
			return identityErr
		}
		if err := os.Chown(config.Integrity.CheckpointFile, uid, gid); err != nil {
			return err
		}
	} else if effectiveUID() != config.Owner.UID {
		return fmt.Errorf("user checkpoint access requires the configured Linux owner")
	} else if uid, _, ownerErr := fileOwner(config.Integrity.CheckpointFile); ownerErr != nil || uid != config.Owner.UID {
		return fmt.Errorf("user checkpoint must be owned by the configured Linux owner")
	}
	return os.Chmod(config.Integrity.CheckpointFile, 0o600)
}

func doctorIntegrityCheckpointAccess(ctx context.Context, config bootstrap.Config) error {
	info, err := os.Lstat(config.Integrity.CheckpointFile)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > ledgeranchor.MaximumFileBytes {
		return fmt.Errorf("ledger anchor checkpoint must be a bounded regular file, not a link")
	}
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("ledger anchor checkpoint permissions must be 0600")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("ledger anchor checkpoint ownership is unavailable")
	}
	expectedUID, expectedGID := config.Owner.UID, config.Owner.GID
	if config.Mode == bootstrap.ModeSystem {
		expectedUID, expectedGID, err = lookupNumericIdentity(ctx, "agentos")
		if err != nil {
			return fmt.Errorf("resolve Agent OS service identity: %w", err)
		}
	}
	if int(stat.Uid) != expectedUID || int(stat.Gid) != expectedGID {
		return fmt.Errorf("ledger anchor checkpoint is not owned by its service identity")
	}
	return nil
}

func runAdministratorSetup(ctx context.Context, ui *terminalUI, failure string, arguments ...string) (bool, error) {
	selected, err := ui.selectOne("Administrator access required:", []string{"Continue", "Exit"})
	if err != nil {
		return false, err
	}
	if selected == 1 {
		return false, errSetupExited
	}
	executable, err := os.Executable()
	if err != nil {
		return false, err
	}
	commandArguments := append([]string{"--", executable}, arguments...)
	command := exec.CommandContext(ctx, "/usr/bin/sudo", commandArguments...)
	command.Stdin = ui.input
	command.Stdout = ui.output
	command.Stderr = ui.output
	if err := command.Run(); err != nil {
		return false, fmt.Errorf("%s: %w", failure, err)
	}
	return true, nil
}

func invokingSystemOwner(ctx context.Context) (bootstrap.Owner, error) {
	if effectiveUID() != 0 {
		return bootstrap.Owner{}, fmt.Errorf("system setup must resume with administrator access")
	}
	uidText := strings.TrimSpace(os.Getenv("SUDO_UID"))
	gidText := strings.TrimSpace(os.Getenv("SUDO_GID"))
	username := strings.TrimSpace(os.Getenv("SUDO_USER"))
	if uidText == "" && gidText == "" && username == "" {
		uidText, gidText, username = "0", "0", "root"
	}
	uid, uidErr := strconv.Atoi(uidText)
	gid, gidErr := strconv.Atoi(gidText)
	if uidErr != nil || gidErr != nil || uid < 0 || uid >= 65534 || gid < 0 || username == "" || username == "agentos" {
		return bootstrap.Owner{}, fmt.Errorf("cannot verify the Linux user who started setup")
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/getent", "passwd", strconv.Itoa(uid)).Output()
	if err != nil {
		return bootstrap.Owner{}, fmt.Errorf("verify the Linux user who started setup: %w", err)
	}
	fields := strings.Split(strings.TrimSpace(string(output)), ":")
	if len(fields) != 7 || fields[0] != username || fields[2] != uidText || fields[3] != gidText || !filepath.IsAbs(fields[5]) || (uid != 0 && (strings.HasSuffix(fields[6], "/nologin") || strings.HasSuffix(fields[6], "/false"))) {
		return bootstrap.Owner{}, fmt.Errorf("the Linux user who started setup is not a valid login account")
	}
	return bootstrap.Owner{Username: username, UID: uid, GID: gid}, nil
}

func canonicalCodexBinary(mode bootstrap.Mode, path string) (string, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("codex path must be absolute")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve Codex path: %w", err)
	}
	info, err := os.Lstat(resolved)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Size() <= 0 || info.Size() > 512<<20 {
		return "", fmt.Errorf("codex path must resolve to a bounded executable regular file")
	}
	if mode != bootstrap.ModeSystem {
		return resolved, nil
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || info.Mode().Perm()&0o022 != 0 {
		return "", fmt.Errorf("system mode requires a root-owned Codex executable that is not group- or world-writable")
	}
	for directory := filepath.Dir(resolved); ; directory = filepath.Dir(directory) {
		parent, parentErr := os.Lstat(directory)
		if parentErr != nil {
			return "", fmt.Errorf("inspect system Codex path: %w", parentErr)
		}
		parentStat, parentOK := parent.Sys().(*syscall.Stat_t)
		if !parentOK || parent.Mode()&os.ModeSymlink != 0 || !parent.IsDir() || parentStat.Uid != 0 || parent.Mode().Perm()&0o022 != 0 {
			return "", fmt.Errorf("system Codex path must have a root-owned non-writable directory chain")
		}
		if directory == string(filepath.Separator) {
			break
		}
	}
	for _, protected := range []string{"/home", "/root", "/run/user", "/tmp", "/var/tmp"} {
		if resolved == protected || strings.HasPrefix(resolved, protected+string(filepath.Separator)) {
			return "", fmt.Errorf("system mode requires a service-readable Codex installation outside user and temporary directories")
		}
	}
	return resolved, nil
}

func readSetupCredential(path string, ownerUID int) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || ownerUID < 0 {
		return nil, fmt.Errorf("credential path or owner is invalid")
	}
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > secrets.MaximumSealedBytes || before.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("credential must be a private bounded regular file")
	}
	stat, ok := before.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != ownerUID {
		return nil, fmt.Errorf("credential owner does not match the setup user")
	}
	return readUnchangedBoundedFile(path, before, secrets.MaximumSealedBytes, "credential")
}

func storeEncryptedCredential(ctx context.Context, config bootstrap.Config, name string, secret []byte) error {
	return storeEncryptedCredentialMode(ctx, config, name, secret, false)
}

func storeEncryptedCredentialNew(ctx context.Context, config bootstrap.Config, name string, secret []byte) error {
	return storeEncryptedCredentialMode(ctx, config, name, secret, true)
}

func storeEncryptedCredentialMode(ctx context.Context, config bootstrap.Config, name string, secret []byte, exclusive bool) error {
	if name == "" || filepath.Base(name) != name || len(secret) == 0 {
		return fmt.Errorf("credential name and value are required")
	}
	directory := filepath.Join(config.Paths.ConfigDir, "credentials")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create encrypted credential directory: %w", err)
	}
	path := filepath.Join(directory, name+".cred")
	temporary, err := os.CreateTemp(directory, ".credential-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	defer func() { _ = os.Remove(temporaryPath) }()
	arguments, err := systemdCredentialArguments(config.Mode, "encrypt", name, "-", temporaryPath)
	if err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "/usr/bin/systemd-creds", arguments...)
	command.Stdin = bytes.NewReader(secret)
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		return fmt.Errorf("encrypt provider credential with systemd: %w", err)
	}
	if info, err := os.Stat(temporaryPath); err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("systemd did not produce an encrypted credential")
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return err
	}
	if exclusive {
		if err := os.Link(temporaryPath, path); err != nil {
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("encrypted credential already exists")
			}
			return err
		}
	} else if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	if err := errors.Join(directoryFile.Sync(), directoryFile.Close()); err != nil {
		return err
	}
	return nil
}

func installRuntime(ctx context.Context, config bootstrap.Config, serviceChoice int) error {
	if err := config.ValidateReady(); err != nil {
		return err
	}
	if serviceChoice < 0 || serviceChoice > 2 {
		return fmt.Errorf("service choice is invalid")
	}
	if config.Mode == bootstrap.ModeSystem {
		return installSystemRuntime(ctx, config, serviceChoice)
	}
	return installUserRuntime(ctx, config, serviceChoice)
}

func applyProviderRuntime(ctx context.Context, config bootstrap.Config) error {
	if err := config.ValidateReady(); err != nil {
		return err
	}
	if config.Mode == bootstrap.ModeSystem {
		if effectiveUID() != 0 {
			return fmt.Errorf("system provider setup requires administrator access")
		}
		serviceUID, serviceGID, err := lookupNumericIdentity(ctx, "agentos")
		if err != nil {
			return err
		}
		if err := prepareSystemProviderState(config, serviceUID, serviceGID); err != nil {
			return err
		}
		unit, err := systemServiceUnit(config)
		if err != nil {
			return err
		}
		if err := writeRestrictedFile("/etc/systemd/system/agentos.service", []byte(unit), 0o644); err != nil {
			return err
		}
		if err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
			return err
		}
		return runCommand(ctx, "systemctl", "try-restart", "agentos.service")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	binary := filepath.Join(home, ".local", "bin", "agentos")
	unit := filepath.Join(home, ".config", "systemd", "user", "agentos.service")
	serviceUnit, err := userServiceUnit(config, binary)
	if err != nil {
		return err
	}
	if err := writeRestrictedFile(unit, []byte(serviceUnit), 0o600); err != nil {
		return err
	}
	if err := runCommand(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	return runCommand(ctx, "systemctl", "--user", "try-restart", "agentos.service")
}

func installSystemRuntime(ctx context.Context, config bootstrap.Config, serviceChoice int) error {
	if effectiveUID() != 0 {
		return fmt.Errorf("system installation requires administrator access")
	}
	if err := ensureServiceAccount(ctx); err != nil {
		return err
	}
	serviceUID, serviceGID, err := lookupNumericIdentity(ctx, "agentos")
	if err != nil {
		return err
	}
	if err := prepareSystemDirectory(config.Paths.ConfigDir, 0, 0, 0o755); err != nil {
		return err
	}
	if err := prepareSystemDirectory(config.Paths.DataDir, serviceUID, serviceGID, 0o751); err != nil {
		return err
	}
	for _, directory := range []string{config.Paths.StateDir, config.Paths.CacheDir} {
		if err := prepareSystemDirectory(directory, serviceUID, serviceGID, 0o750); err != nil {
			return err
		}
	}
	if err := prepareIntegrityCheckpointAccess(ctx, config); err != nil {
		return err
	}
	if err := prepareSystemProviderState(config, serviceUID, serviceGID); err != nil {
		return err
	}
	if err := prepareSystemDirectory(config.Paths.RuntimeDir, 0, 0, 0o711); err != nil {
		return err
	}
	if err := prepareSystemWorkspace(config.Paths.Workspace, config.Owner, serviceUID, serviceGID); err != nil {
		return err
	}
	if err := installExecutable("/usr/local/bin/agentos"); err != nil {
		return err
	}
	servicePath := "/etc/systemd/system/agentos.service"
	socketPath := "/etc/systemd/system/agentos-user.socket"
	serviceUnit, err := systemServiceUnit(config)
	if err != nil {
		return err
	}
	if err := writeRestrictedFile(servicePath, []byte(serviceUnit), 0o644); err != nil {
		return err
	}
	if err := writeRestrictedFile(socketPath, []byte(systemSocketUnit(config)), 0o644); err != nil {
		return err
	}
	if err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	switch serviceChoice {
	case 0:
		return runCommand(ctx, "systemctl", "enable", "--now", "agentos-user.socket", "agentos.service")
	case 1:
		return runCommand(ctx, "systemctl", "start", "agentos-user.socket", "agentos.service")
	default:
		return nil
	}
}

func prepareSystemProviderState(config bootstrap.Config, serviceUID, serviceGID int) error {
	provider := config.Providers[0]
	if provider.Kind != bootstrap.ProviderCodexSubscription {
		return nil
	}
	relative, err := filepath.Rel(config.Paths.StateDir, provider.CodexCredential)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("codex credential store must remain inside the Agent OS state directory")
	}
	directory := filepath.Dir(provider.CodexCredential)
	if err := prepareSystemDirectory(directory, serviceUID, serviceGID, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(provider.CodexCredential)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("codex credential store must be a regular file")
	}
	if err := os.Chown(provider.CodexCredential, serviceUID, serviceGID); err != nil {
		return err
	}
	return os.Chmod(provider.CodexCredential, 0o600)
}

func prepareSystemDirectory(path string, uid, gid int, mode os.FileMode) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("system directory is invalid")
	}
	if err := rejectSymlinkDirectoryChain(path); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("system path %s must be a directory, not a link", path)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	if err := os.Chown(path, uid, gid); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func prepareSystemWorkspace(path string, owner bootstrap.Owner, serviceUID, serviceGID int) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("workspace is invalid")
	}
	if err := rejectSymlinkDirectoryChain(path); err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if err := validateSystemWorkspaceParents(parent, owner.UID, serviceUID); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("workspace %s must be a directory, not a link", path)
		}
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			return readErr
		}
		if len(entries) != 0 {
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || int(stat.Uid) != owner.UID || int(stat.Gid) != serviceGID || info.Mode().Perm() != 0o770 {
				return fmt.Errorf("non-empty workspace %s does not already have the required owner and access mode", path)
			}
			return nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parentInfo, err := os.Lstat(parent)
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("workspace parent %s must already exist", parent)
	}
	if err != nil {
		return err
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() || parentInfo.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("workspace parent %s must be a non-writable directory, not a link", parent)
	}
	if err := os.Mkdir(path, 0o770); err != nil {
		return err
	}
	if err := os.Chown(path, owner.UID, serviceGID); err != nil {
		return err
	}
	return os.Chmod(path, 0o770)
}

func validateSystemWorkspaceParents(path string, ownerUID, serviceUID int) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || ownerUID < 0 || serviceUID < 0 {
		return fmt.Errorf("workspace parent chain is invalid")
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("workspace parent %s must already exist", current)
		}
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 || (int(stat.Uid) != 0 && int(stat.Uid) != ownerUID && int(stat.Uid) != serviceUID) {
			return fmt.Errorf("workspace parent %s must be a non-writable directory owned by root, the configured Linux user, or the service", current)
		}
	}
	return nil
}

func rejectSymlinkDirectoryChain(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("directory chain must be a canonical absolute path")
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("directory chain component %s must be a directory, not a link", current)
		}
	}
	return nil
}

func installUserRuntime(ctx context.Context, config bootstrap.Config, serviceChoice int) error {
	if effectiveUID() != config.Owner.UID {
		return fmt.Errorf("user installation must run as its configured Linux owner")
	}
	if err := validateUserRuntimeBase(config); err != nil {
		return err
	}
	current, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	for _, directory := range []string{config.Paths.ConfigDir, config.Paths.DataDir, config.Paths.StateDir, config.Paths.CacheDir, config.Paths.RuntimeDir, config.Paths.Workspace} {
		if err := ensureOwnedRuntimeDirectory(directory, config.Owner.UID, 0o700); err != nil {
			return err
		}
	}
	if err := prepareIntegrityCheckpointAccess(ctx, config); err != nil {
		return err
	}
	binary := filepath.Join(current, ".local", "bin", "agentos")
	if err := installExecutable(binary); err != nil {
		return err
	}
	unitDirectory := filepath.Join(current, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDirectory, 0o700); err != nil {
		return err
	}
	serviceUnit, err := userServiceUnit(config, binary)
	if err != nil {
		return err
	}
	if err := writeRestrictedFile(filepath.Join(unitDirectory, "agentos.service"), []byte(serviceUnit), 0o600); err != nil {
		return err
	}
	if err := runCommand(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	switch serviceChoice {
	case 0:
		return runCommand(ctx, "systemctl", "--user", "enable", "--now", "agentos.service")
	case 1:
		return runCommand(ctx, "systemctl", "--user", "start", "agentos.service")
	default:
		return nil
	}
}

func validateUserRuntimeBase(config bootstrap.Config) error {
	if config.Mode != bootstrap.ModeUser {
		return nil
	}
	base := filepath.Dir(config.Paths.RuntimeDir)
	uid, mode, err := fileOwner(base)
	if err != nil || mode&os.ModeSymlink != 0 || !mode.IsDir() || uid != config.Owner.UID || mode.Perm() != 0o700 {
		return fmt.Errorf("user runtime base must be a private directory owned by the configured Linux user")
	}
	return nil
}

func ensureServiceAccount(ctx context.Context) error {
	if err := exec.CommandContext(ctx, "/usr/bin/id", "-u", "agentos").Run(); err != nil {
		if err := runCommand(ctx, "useradd", "--system", "--home-dir", "/var/lib/agentos", "--shell", "/usr/sbin/nologin", "--user-group", "agentos"); err != nil {
			return err
		}
	}
	entry, err := exec.CommandContext(ctx, "/usr/bin/getent", "passwd", "agentos").Output()
	if err != nil {
		return fmt.Errorf("verify Agent OS service account: %w", err)
	}
	fields := strings.Split(strings.TrimSpace(string(entry)), ":")
	if len(fields) != 7 {
		return fmt.Errorf("existing agentos account is not the required restricted service account")
	}
	validShell := fields[6] == "/usr/sbin/nologin" || fields[6] == "/sbin/nologin" || fields[6] == "/bin/false" || fields[6] == "/usr/bin/false"
	if fields[0] != "agentos" || fields[5] != "/var/lib/agentos" || !validShell {
		return fmt.Errorf("existing agentos account is not the required restricted service account")
	}
	uid, uidErr := strconv.Atoi(fields[2])
	gid, gidErr := strconv.Atoi(fields[3])
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 {
		return fmt.Errorf("agent OS service account identity is invalid")
	}
	group, err := exec.CommandContext(ctx, "/usr/bin/getent", "group", strconv.Itoa(gid)).Output()
	groupFields := strings.Split(strings.TrimSpace(string(group)), ":")
	if err != nil || len(groupFields) != 4 || groupFields[0] != "agentos" || groupFields[2] != strconv.Itoa(gid) || strings.TrimSpace(groupFields[3]) != "" {
		return fmt.Errorf("agent OS service group is invalid")
	}
	groups, err := exec.CommandContext(ctx, "/usr/bin/id", "-G", "agentos").Output()
	if err != nil || strings.TrimSpace(string(groups)) != strconv.Itoa(gid) {
		return fmt.Errorf("agent OS service account must not have supplementary groups")
	}
	return nil
}

func lookupNumericIdentity(ctx context.Context, name string) (int, int, error) {
	uidOutput, err := exec.CommandContext(ctx, "/usr/bin/id", "-u", name).Output()
	if err != nil {
		return 0, 0, err
	}
	gidOutput, err := exec.CommandContext(ctx, "/usr/bin/id", "-g", name).Output()
	if err != nil {
		return 0, 0, err
	}
	uid, uidErr := strconv.Atoi(strings.TrimSpace(string(uidOutput)))
	gid, gidErr := strconv.Atoi(strings.TrimSpace(string(gidOutput)))
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 {
		return 0, 0, fmt.Errorf("service account identity is invalid")
	}
	return uid, gid, nil
}

func installExecutable(destination string) error {
	source, err := os.Executable()
	if err != nil {
		return err
	}
	if same, _ := filepath.EvalSymlinks(destination); same == source {
		return nil
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".agentos-bin-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if _, err := io.Copy(temporary, input); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o755); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, destination)
}

func writeRestrictedFile(path string, body []byte, mode os.FileMode) error {
	return fileguard.WriteAtomically(path, body, mode, 0o755)
}

func systemServiceUnit(config bootstrap.Config) (string, error) {
	credentials, err := serviceCredentialDirectives(config)
	if err != nil {
		return "", err
	}
	return `[Unit]
Description=Agent OS
Requires=agentos-user.socket
After=network-online.target agentos-user.socket
Wants=network-online.target

[Service]
Type=simple
User=agentos
Group=agentos
UMask=0077
RuntimeDirectory=agentos-private
RuntimeDirectoryMode=0700
ExecStart=/usr/local/bin/agentos serve --config ` + systemdQuote(bootstrap.ConfigPath(config.Paths)) + `
Restart=on-failure
RestartSec=5s
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectSystem=strict
ProtectHome=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
ReadWritePaths=` + systemdPathList(config.Paths.DataDir, config.Paths.StateDir, config.Paths.CacheDir, config.Paths.RuntimeDir, systemProviderRuntimeDirectory, config.Paths.Workspace) + "\n" + credentials + `
[Install]
WantedBy=multi-user.target
`, nil
}

func systemSocketUnit(config bootstrap.Config) string {
	return `[Unit]
Description=Agent OS private user gateway

[Socket]
ListenStream=` + systemdQuote(config.Paths.UserSocket) + `
DirectoryMode=0711
SocketMode=0600
SocketUser=` + systemdQuote(config.Owner.Username) + `
SocketGroup=` + strconv.Itoa(config.Owner.GID) + `
RemoveOnStop=yes
Service=agentos.service

[Install]
WantedBy=sockets.target
`
}

func userServiceUnit(config bootstrap.Config, binary string) (string, error) {
	credentials, err := serviceCredentialDirectives(config)
	if err != nil {
		return "", err
	}
	return `[Unit]
Description=Agent OS
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
UMask=0077
ExecStart=` + systemdQuote(binary) + ` serve --config ` + systemdQuote(bootstrap.ConfigPath(config.Paths)) + `
Restart=on-failure
RestartSec=5s
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ReadWritePaths=` + systemdPathList(config.Paths.DataDir, config.Paths.StateDir, config.Paths.CacheDir, config.Paths.RuntimeDir, config.Paths.Workspace) + "\n" + credentials + `
[Install]
WantedBy=default.target
`, nil
}

func serviceCredentialDirectives(config bootstrap.Config) (string, error) {
	if len(config.Providers) != 1 {
		return "", fmt.Errorf("exactly one configured provider is required")
	}
	provider := config.Providers[0]
	references := map[string]struct{}{provider.SecretRef: {}}
	var directives strings.Builder
	switch provider.Kind {
	case bootstrap.ProviderOpenAIAPI:
		directives.WriteString("LoadCredentialEncrypted=" + systemdQuote(provider.SecretRef+":"+filepath.Join(config.Paths.ConfigDir, "credentials", provider.SecretRef+".cred")) + "\n")
	case bootstrap.ProviderCodexSubscription:
		directives.WriteString("LoadCredentialEncrypted=" + systemdQuote(provider.SecretRef+":"+filepath.Join(config.Paths.ConfigDir, "credentials", provider.SecretRef+".cred")) + "\n")
	}
	if config.Integrity.SecretRef == "" {
		return "", fmt.Errorf("ledger anchor signing credential is required")
	}
	if _, duplicate := references[config.Integrity.SecretRef]; duplicate {
		return "", fmt.Errorf("ledger anchor signing credential reference is duplicated")
	}
	references[config.Integrity.SecretRef] = struct{}{}
	directives.WriteString("LoadCredentialEncrypted=" + systemdQuote(config.Integrity.SecretRef+":"+filepath.Join(config.Paths.ConfigDir, "credentials", config.Integrity.SecretRef+".cred")) + "\n")
	if config.A2A.ActorsFile == "" {
		return directives.String(), nil
	}
	if err := validateReviewedServiceFile(config, config.A2A.ActorsFile, false); err != nil {
		return "", fmt.Errorf("external actor registry is unsafe: %w", err)
	}
	actors, err := decodeConfigFile(config.A2A.ActorsFile, "external actor registry", gateway.DecodeExternalActorConfig)
	if err != nil {
		return "", err
	}
	if _, exists := references["a2a-actors.json"]; exists {
		return "", fmt.Errorf("service credential reference a2a-actors.json is reserved")
	}
	references["a2a-actors.json"] = struct{}{}
	directives.WriteString("LoadCredential=" + systemdQuote("a2a-actors.json:"+config.A2A.ActorsFile) + "\n")
	if config.A2A.TLSCertFile != "" {
		for name, source := range map[string]struct {
			path    string
			private bool
		}{
			"a2a-tls-cert": {path: config.A2A.TLSCertFile},
			"a2a-tls-key":  {path: config.A2A.TLSKeyFile, private: true},
		} {
			if _, exists := references[name]; exists {
				return "", fmt.Errorf("service credential reference %s is reserved", name)
			}
			if err := validateReviewedServiceFile(config, source.path, source.private); err != nil {
				return "", fmt.Errorf("A2A TLS source is unsafe: %w", err)
			}
			references[name] = struct{}{}
		}
		directives.WriteString("LoadCredential=" + systemdQuote("a2a-tls-cert:"+config.A2A.TLSCertFile) + "\n")
		directives.WriteString("LoadCredential=" + systemdQuote("a2a-tls-key:"+config.A2A.TLSKeyFile) + "\n")
	}
	for _, actor := range actors {
		if !validServiceCredentialName(actor.TokenRef) {
			return "", fmt.Errorf("external Agent %s has an invalid credential reference", actor.ID)
		}
		if _, exists := references[actor.TokenRef]; exists {
			return "", fmt.Errorf("service credential reference %s is duplicated", actor.TokenRef)
		}
		references[actor.TokenRef] = struct{}{}
		path := filepath.Join(config.Paths.ConfigDir, "credentials", actor.TokenRef+".cred")
		if err := validateReviewedServiceFile(config, path, true); err != nil {
			return "", fmt.Errorf("external Agent credential %s is unavailable or unsafe", actor.TokenRef)
		}
		directives.WriteString("LoadCredentialEncrypted=" + systemdQuote(actor.TokenRef+":"+path) + "\n")
	}
	return directives.String(), nil
}

func validateReviewedServiceFile(config bootstrap.Config, path string, private bool) error {
	relative, err := filepath.Rel(config.Paths.ConfigDir, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("source must remain inside the configuration directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return fmt.Errorf("source must not traverse a link")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 || info.Mode().Perm()&0o022 != 0 || (private && info.Mode().Perm()&0o077 != 0) {
		return fmt.Errorf("source must be a bounded protected regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	expectedUID := config.Owner.UID
	if config.Mode == bootstrap.ModeSystem {
		expectedUID = 0
	}
	if !ok || int(stat.Uid) != expectedUID {
		return fmt.Errorf("source owner does not match the installation authority")
	}
	return nil
}

func validServiceCredentialName(name string) bool {
	if name == "" || len(name) > 128 || filepath.Base(name) != name {
		return false
	}
	for _, character := range name {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func systemdPathList(paths ...string) string {
	quoted := make([]string, 0, len(paths))
	for _, path := range paths {
		quoted = append(quoted, systemdQuote(path))
	}
	return strings.Join(quoted, " ")
}

func systemdQuote(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, `%`, `%%`)
	return `"` + replacer.Replace(value) + `"`
}

func runCommand(ctx context.Context, name string, arguments ...string) error {
	paths := map[string]string{
		"systemctl": "/usr/bin/systemctl",
		"useradd":   "/usr/sbin/useradd",
	}
	path, ok := paths[name]
	if !ok {
		return fmt.Errorf("unsupported privileged command %s", name)
	}
	command := exec.CommandContext(ctx, path, arguments...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}

func decryptProviderCredential(ctx context.Context, mode bootstrap.Mode, path, name string) ([]byte, error) {
	if filepath.Base(name) != name || name == "" {
		return nil, fmt.Errorf("credential name is invalid")
	}
	arguments, err := systemdCredentialArguments(mode, "decrypt", name, path, "-")
	if err != nil {
		return nil, err
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/systemd-creds", arguments...).Output()
	if err != nil || len(output) == 0 || len(output) > 64<<10 {
		clearBytes(output)
		return nil, fmt.Errorf("decrypt provider credential failed")
	}
	return output, nil
}

func systemdCredentialArguments(mode bootstrap.Mode, operation, name, input, output string) ([]string, error) {
	if mode != bootstrap.ModeSystem && mode != bootstrap.ModeUser {
		return nil, fmt.Errorf("credential scope is invalid")
	}
	if operation != "encrypt" && operation != "decrypt" {
		return nil, fmt.Errorf("credential operation is invalid")
	}
	if !validServiceCredentialName(name) || input == "" || output == "" {
		return nil, fmt.Errorf("credential operation arguments are invalid")
	}
	arguments := []string{"--name=" + name}
	if mode == bootstrap.ModeUser {
		arguments = append(arguments, "--user")
	}
	return append(arguments, operation, input, output), nil
}

func doctorUserSocket(config bootstrap.Config) (string, string) {
	info, err := os.Lstat(config.Paths.UserSocket)
	if errors.Is(err, os.ErrNotExist) {
		return "INFO", "not present while the service is stopped"
	}
	if err != nil {
		return "BLOCKED", err.Error()
	}
	uid, mode, ownerErr := fileOwner(config.Paths.UserSocket)
	if ownerErr != nil || info.Mode()&os.ModeSymlink != 0 || mode&os.ModeSocket == 0 || mode.Perm() != 0o600 || uid != config.Owner.UID {
		return "BLOCKED", "socket ownership, type, or permissions are invalid"
	}
	return "PASS", config.Paths.UserSocket
}

func doctorService(ctx context.Context, config bootstrap.Config) (string, string) {
	arguments := []string{"is-active", "agentos.service"}
	if config.Mode == bootstrap.ModeUser {
		arguments = append([]string{"--user"}, arguments...)
	}
	output, err := exec.CommandContext(ctx, "/usr/bin/systemctl", arguments...).CombinedOutput()
	state := strings.TrimSpace(string(output))
	if err != nil {
		if state == "" {
			state = "not active"
		}
		return "INFO", state
	}
	if state != "active" {
		return "INFO", state
	}
	if config.Mode == bootstrap.ModeSystem {
		socketOutput, socketErr := exec.CommandContext(ctx, "/usr/bin/systemctl", "is-active", "agentos-user.socket").CombinedOutput()
		if socketErr != nil || strings.TrimSpace(string(socketOutput)) != "active" {
			return "BLOCKED", "service is active without the private user socket"
		}
	}
	return "PASS", "active"
}

func fileOwner(path string) (int, os.FileMode, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("file ownership is unavailable")
	}
	return int(stat.Uid), info.Mode(), nil
}
