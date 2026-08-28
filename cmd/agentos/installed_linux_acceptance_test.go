//go:build linux && installedlinux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/ledger"
)

const (
	installedTestBinary   = "AGENTOS_TEST_PACKAGED_BINARY"
	installedTestRecovery = "AGENTOS_TEST_PACKAGED_RECOVERY"
	installedUserHelper   = "AGENTOS_TEST_USER_HELPER"
)

func TestInstalledLinuxSystemLifecycle(t *testing.T) {
	if effectiveUID() != 0 {
		t.Fatal("installed Linux acceptance requires a disposable root host")
	}
	binary := requirePackagedExecutable(t, installedTestBinary)
	recoveryBinary := requirePackagedExecutable(t, installedTestRecovery)
	assertCleanSystemInstallation(t)
	exerciseInstalledUserLifecycle(t, binary, recoveryBinary)

	ctx := t.Context()
	now := time.Now().UTC()
	config := bootstrap.NewConfig(bootstrap.ModeSystem, bootstrap.Owner{Username: "root", UID: 0, GID: 0}, bootstrap.SystemPaths(), now)
	config.Providers = []bootstrap.Provider{testOpenAIProvider(config, "gpt-5.2-2025-12-11", "openai-api-key")}
	state := bootstrap.State{Version: bootstrap.ConfigVersion, Mode: bootstrap.ModeSystem, Stage: bootstrap.StageService, UpdatedAt: now}
	if err := checkpoint(bootstrap.ConfigPath(config.Paths), bootstrap.StatePath(config.Paths), &config, &state); err != nil {
		t.Fatalf("persist resumable setup checkpoint: %v", err)
	}
	loaded, err := bootstrap.LoadState(bootstrap.StatePath(config.Paths))
	if err != nil || loaded.Stage != bootstrap.StageService {
		t.Fatalf("resumable setup state=%+v err=%v", loaded, err)
	}
	if err := storeEncryptedCredential(ctx, config, config.Providers[0].SecretRef, []byte("installed-test-not-a-live-key")); err != nil {
		t.Fatalf("store offline provider credential: %v", err)
	}
	if output, err := runPackagedSetupResume(ctx, binary); err != nil {
		t.Fatalf("resume packaged system setup: %v\n%s", err, output)
	}
	loaded, err = bootstrap.LoadState(bootstrap.StatePath(config.Paths))
	if err != nil || loaded.Stage != bootstrap.StageReady {
		t.Fatalf("packaged system resume state=%+v err=%v", loaded, err)
	}
	installOfflineTestConfinement(t)
	runSystemctl(t, "enable", "agentos-user.socket", "agentos.service")
	runSystemctl(t, "start", "agentos-user.socket")
	waitForSystemUnit(t, "agentos-user.socket", config.Paths.UserSocket)
	runSystemctl(t, "start", "agentos.service")
	waitForSystemUnit(t, "agentos.service", config.Paths.UserSocket)
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = exec.CommandContext(cleanupContext, "/usr/bin/systemctl", "disable", "--now", "agentos-user.socket", "agentos.service").Run()
	})
	assertInstalledSystemLayout(t, config)
	assertDoctorReady(t, binary)
	assertDifferentUIDCannotConnect(t, config.Paths.UserSocket)
	bootstrapOrganizationThroughPackagedService(t, config)

	runSystemctl(t, "restart", "agentos.service")
	assertDoctorReady(t, binary)
	assertOrganizationSurvivesRestart(t, config, "mission-installed")

	backup := filepath.Join(config.Paths.DataDir, "installed-acceptance-backup.db")
	restored := filepath.Join(config.Paths.DataDir, "installed-acceptance-restored.db")
	runRecovery(t, recoveryBinary, "backup", "--database", config.Paths.Database, "--output", backup)
	runRecovery(t, recoveryBinary, "verify", "--database", backup)
	runRecovery(t, recoveryBinary, "restore", "--backup", backup, "--output", restored)
	assertRestoredOrganization(t, restored)
	assertProductionRejectsUnsupportedProvider(t, binary, config)
}

func TestInstalledLinuxUserHelper(t *testing.T) {
	action := os.Getenv(installedUserHelper)
	if action == "" {
		t.Skip("invoked only by the disposable-host acceptance test")
	}
	if effectiveUID() == 0 {
		t.Fatal("user-mode helper must not run as root")
	}
	home := os.Getenv("HOME")
	runtimeBase := os.Getenv("XDG_RUNTIME_DIR")
	paths, err := bootstrap.UserPaths(home, runtimeBase, effectiveUID())
	if err != nil {
		t.Fatal(err)
	}
	configPath := bootstrap.ConfigPath(paths)
	switch action {
	case "layout":
		now := time.Now().UTC()
		config := bootstrap.NewConfig(bootstrap.ModeUser, bootstrap.Owner{Username: "agentos-owner", UID: effectiveUID(), GID: os.Getgid()}, paths, now)
		config.Providers = []bootstrap.Provider{testOpenAIProvider(config, "gpt-5.2-2025-12-11", "openai-api-key")}
		state := bootstrap.State{Version: bootstrap.ConfigVersion, Mode: bootstrap.ModeUser, Stage: bootstrap.StageService, UpdatedAt: now}
		if err := checkpoint(configPath, bootstrap.StatePath(paths), &config, &state); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(runtimeBase, 0o700); err != nil {
			t.Fatal(err)
		}
		credentialDirectory := filepath.Join(config.Paths.ConfigDir, "credentials")
		if err := os.MkdirAll(credentialDirectory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(credentialDirectory, "openai-api-key.cred"), []byte("not-a-systemd-encrypted-credential"), 0o600); err != nil {
			t.Fatal(err)
		}
		if output, err := runPackagedSetupResume(t.Context(), requirePackagedExecutable(t, installedTestBinary)); err != nil {
			t.Fatalf("resume packaged user setup: %v\n%s", err, output)
		}
		loaded, err := bootstrap.LoadState(bootstrap.StatePath(paths))
		if err != nil || loaded.Stage != bootstrap.StageReady {
			t.Fatalf("packaged user resume state=%+v err=%v", loaded, err)
		}
		assertInstalledUserLayout(t, config)
		assertOnlineDoctorRejectsFakeCredential(t, filepath.Join(home, ".local", "bin", "agentos"))
	case "bootstrap", "verify":
		config, err := bootstrap.LoadConfig(configPath)
		if err != nil {
			t.Fatal(err)
		}
		if action == "bootstrap" {
			body := `{"request_id":"installed-user-strategy","mission_id":"mission-installed-user","mission_statement":"Exercise the installed user organization","goal_id":"goal-installed-user","goal_objective":"Retain user-owned governed state across restart","goal_mode":"TARGET","success_criteria":["the user installation survives restart"]}`
			response := localRequest(t, config, http.MethodPost, "/v1/user/strategy/bootstrap", body, map[string]string{"X-AgentOS-User-ID": "local-uid-0"})
			defer func() { _ = response.Body.Close() }()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("user strategy status=%d", response.StatusCode)
			}
			assertRequestIdentity(t, config.Paths.Database, "installed-user-strategy", "local-uid-0", fmt.Sprintf("local-uid-%d", effectiveUID()))
		} else {
			assertOrganizationSurvivesRestart(t, config, "mission-installed-user")
		}
		assertDoctorReady(t, filepath.Join(home, ".local", "bin", "agentos"))
	default:
		t.Fatalf("unknown user helper action %q", action)
	}
}

func runPackagedSetupResume(ctx context.Context, binary string) ([]byte, error) {
	const script = `import errno, os, pty, sys
pid, fd = pty.fork()
if pid == 0:
    os.execve(sys.argv[1], [sys.argv[1]], os.environ)
os.write(fd, b"\x1b[B\x1b[B\r")
output = bytearray()
while True:
    try:
        chunk = os.read(fd, 4096)
        if not chunk:
            break
        output.extend(chunk)
    except OSError as error:
        if error.errno == errno.EIO:
            break
        raise
_, status = os.waitpid(pid, 0)
sys.stdout.buffer.write(output)
sys.exit(os.waitstatus_to_exitcode(status))
`
	command := exec.CommandContext(ctx, "/usr/bin/python3", "-c", script, binary)
	command.Env = make([]string, 0, len(os.Environ()))
	for _, value := range os.Environ() {
		if strings.HasPrefix(value, "SUDO_USER=") || strings.HasPrefix(value, "SUDO_UID=") || strings.HasPrefix(value, "SUDO_GID=") {
			continue
		}
		command.Env = append(command.Env, value)
	}
	return command.CombinedOutput()
}

func exerciseInstalledUserLifecycle(t *testing.T, binary, recoveryBinary string) {
	t.Helper()
	const username = "agentos-owner"
	toolDirectory := "/usr/local/libexec/agentos-installed-acceptance"
	if err := os.MkdirAll(toolDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	packagedSource := filepath.Join(toolDirectory, "agentos-package-source")
	recoverySource := filepath.Join(toolDirectory, "agentos-recovery-package-source")
	testSource := filepath.Join(toolDirectory, "agentos-installed-linux.test")
	testExecutable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct{ source, destination string }{
		{binary, packagedSource}, {recoveryBinary, recoverySource}, {testExecutable, testSource},
	} {
		if err := installExecutable(item.source, item.destination); err != nil {
			t.Fatal(err)
		}
	}
	if output, err := exec.CommandContext(t.Context(), "/usr/sbin/useradd", "--create-home", "--shell", "/bin/bash", username).CombinedOutput(); err != nil {
		t.Fatalf("create disposable user owner: %v\n%s", err, output)
	}
	uid, gid, err := lookupNumericIdentity(t.Context(), username)
	if err != nil {
		t.Fatal(err)
	}
	home := "/home/" + username
	runtimeBase := fmt.Sprintf("/run/user/%d", uid)
	userUnit := fmt.Sprintf("user@%d.service", uid)
	runSystemctl(t, "start", userUnit)
	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "/usr/bin/systemctl", "stop", userUnit).Run()
	})
	if err := os.MkdirAll(runtimeBase, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(runtimeBase, uid, gid); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(runtimeBase, 0o700); err != nil {
		t.Fatal(err)
	}
	runUserHelper(t, username, home, runtimeBase, packagedSource, testSource, "layout")

	paths, err := bootstrap.UserPaths(home, runtimeBase, uid)
	if err != nil {
		t.Fatal(err)
	}
	credentialDirectory := filepath.Join(paths.RuntimeDir, "test-credentials")
	if err := os.MkdirAll(credentialDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(credentialDirectory, uid, gid); err != nil {
		t.Fatal(err)
	}
	credentialPath := filepath.Join(credentialDirectory, "openai-api-key")
	if err := os.WriteFile(credentialPath, []byte("installed-user-test-not-a-live-key"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(credentialPath, uid, gid); err != nil {
		t.Fatal(err)
	}

	start := func() (*exec.Cmd, *bytes.Buffer) {
		var diagnostics bytes.Buffer
		command := exec.CommandContext(t.Context(),
			"/usr/bin/unshare", "--net", "/usr/bin/setpriv",
			fmt.Sprintf("--reuid=%d", uid), fmt.Sprintf("--regid=%d", gid), "--clear-groups",
			filepath.Join(home, ".local", "bin", "agentos"), "serve", "--config", bootstrap.ConfigPath(paths),
		)
		command.Env = append(os.Environ(), "HOME="+home, "XDG_RUNTIME_DIR="+runtimeBase, "CREDENTIALS_DIRECTORY="+credentialDirectory)
		command.Stdout = &diagnostics
		command.Stderr = &diagnostics
		if err := command.Start(); err != nil {
			t.Fatalf("start offline user runtime: %v", err)
		}
		waitForSocket(t, paths.UserSocket, command, &diagnostics)
		return command, &diagnostics
	}
	stop := func(command *exec.Cmd, diagnostics *bytes.Buffer) {
		if err := command.Process.Signal(os.Interrupt); err != nil {
			t.Fatalf("signal user runtime: %v", err)
		}
		if err := command.Wait(); err != nil {
			t.Fatalf("stop user runtime: %v\n%s", err, diagnostics.String())
		}
	}

	command, diagnostics := start()
	runUserHelper(t, username, home, runtimeBase, packagedSource, testSource, "bootstrap")
	config, err := bootstrap.LoadConfig(bootstrap.ConfigPath(paths))
	if err != nil {
		t.Fatal(err)
	}
	response := localRequest(t, config, http.MethodGet, "/v1/user/organization", "", nil)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("root peer crossed user gateway boundary: status=%d", response.StatusCode)
	}
	stop(command, diagnostics)

	command, diagnostics = start()
	runUserHelper(t, username, home, runtimeBase, packagedSource, testSource, "verify")
	userBackup := filepath.Join(paths.DataDir, "installed-user-backup.db")
	userRestore := filepath.Join(paths.DataDir, "installed-user-restored.db")
	runAsUser(t, username, home, runtimeBase, recoverySource, "backup", "--database", paths.Database, "--output", userBackup)
	runAsUser(t, username, home, runtimeBase, recoverySource, "restore", "--backup", userBackup, "--output", userRestore)
	stop(command, diagnostics)
	assertRestoredOrganizationID(t, userRestore, "default", "goal-installed-user")
}

func runUserHelper(t *testing.T, username, home, runtimeBase, binary, testBinary, action string) {
	t.Helper()
	arguments := []string{
		"-u", username, "--", "/usr/bin/env",
		"HOME=" + home, "XDG_RUNTIME_DIR=" + runtimeBase, "DBUS_SESSION_BUS_ADDRESS=unix:path=" + filepath.Join(runtimeBase, "bus"),
		installedUserHelper + "=" + action, installedTestBinary + "=" + binary,
		testBinary, "-test.run", "^TestInstalledLinuxUserHelper$", "-test.v", "-test.timeout", "30s",
	}
	output, err := exec.CommandContext(t.Context(), "/usr/sbin/runuser", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("user helper %s: %v\n%s", action, err, output)
	}
}

func runAsUser(t *testing.T, username, home, runtimeBase, binary string, arguments ...string) {
	t.Helper()
	commandArguments := []string{
		"-u", username, "--", "/usr/bin/env", "HOME=" + home, "XDG_RUNTIME_DIR=" + runtimeBase, "DBUS_SESSION_BUS_ADDRESS=unix:path=" + filepath.Join(runtimeBase, "bus"),
		binary,
	}
	commandArguments = append(commandArguments, arguments...)
	output, err := exec.CommandContext(t.Context(), "/usr/sbin/runuser", commandArguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("run %s as %s: %v\n%s", filepath.Base(binary), username, err, output)
	}
}

func waitForSocket(t *testing.T, path string, command *exec.Cmd, diagnostics *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		if err := command.Process.Signal(syscall.Signal(0)); err != nil {
			t.Fatalf("user runtime exited before creating its socket: %v\n%s", err, diagnostics.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("user runtime did not create %s\n%s", path, diagnostics.String())
}

func assertInstalledUserLayout(t *testing.T, config bootstrap.Config) {
	t.Helper()
	home := os.Getenv("HOME")
	checks := []struct {
		path string
		mode os.FileMode
	}{
		{config.Paths.ConfigDir, 0o700}, {config.Paths.DataDir, 0o700}, {config.Paths.StateDir, 0o700},
		{config.Paths.CacheDir, 0o700}, {config.Paths.RuntimeDir, 0o700}, {config.Paths.Workspace, 0o700},
		{bootstrap.ConfigPath(config.Paths), 0o600}, {bootstrap.StatePath(config.Paths), 0o600},
		{filepath.Join(config.Paths.ConfigDir, "credentials", "openai-api-key.cred"), 0o600},
		{filepath.Join(home, ".local", "bin", "agentos"), 0o755},
		{filepath.Join(home, ".config", "systemd", "user", "agentos.service"), 0o600},
	}
	for _, check := range checks {
		uid, mode, err := fileOwner(check.path)
		if err != nil || uid != effectiveUID() || mode.Perm() != check.mode.Perm() {
			t.Fatalf("user layout %s uid=%d mode=%v err=%v", check.path, uid, mode, err)
		}
	}
}

func assertRequestIdentity(t *testing.T, database, correlationID, forbidden, expected string) {
	t.Helper()
	store, err := ledger.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	stream, err := app.New(events.NewGateway(store)).Events(t.Context(), correlationID)
	if err != nil || len(stream) == 0 {
		t.Fatalf("strategy events=%d err=%v", len(stream), err)
	}
	encoded, err := json.Marshal(stream)
	if err != nil || bytes.Contains(encoded, []byte(forbidden)) || !bytes.Contains(encoded, []byte(expected)) {
		t.Fatalf("request header influenced durable identity: %s err=%v", encoded, err)
	}
}

func assertRestoredOrganizationID(t *testing.T, database, organizationID, goalID string) {
	t.Helper()
	store, err := ledger.Open(database)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	service := app.New(events.NewGateway(store))
	if _, err := service.Recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot, found, err := service.OrganizationState(t.Context(), core.ID(organizationID))
	if err != nil || !found || len(snapshot.Goals) != 1 || snapshot.Goals[0].ID != core.ID(goalID) {
		t.Fatalf("restored organization found=%v snapshot=%+v err=%v", found, snapshot, err)
	}
}

func installOfflineTestConfinement(t *testing.T) {
	t.Helper()
	directory := "/run/systemd/system/agentos.service.d"
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	body := []byte("[Service]\nIPAddressDeny=any\nRestrictAddressFamilies=AF_UNIX\n")
	if err := os.WriteFile(filepath.Join(directory, "installed-acceptance.conf"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	runSystemctl(t, "daemon-reload")
}

func requirePackagedExecutable(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Clean(os.Getenv(name))
	if !filepath.IsAbs(path) {
		t.Fatalf("%s must name an absolute packaged executable", name)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Size() <= 0 {
		t.Fatalf("%s is not an executable regular file: %v", name, err)
	}
	return path
}

func assertCleanSystemInstallation(t *testing.T) {
	t.Helper()
	for _, path := range []string{
		"/etc/agentos", "/var/lib/agentos", "/var/cache/agentos", "/run/agentos",
		"/usr/local/bin/agentos", "/etc/systemd/system/agentos.service", "/etc/systemd/system/agentos-user.socket",
	} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("disposable host is not clean at %s: %v", path, err)
		}
	}
	if err := exec.CommandContext(t.Context(), "/usr/bin/id", "-u", "agentos").Run(); err == nil {
		t.Fatal("disposable host already has an agentos service account")
	}
}

func assertInstalledSystemLayout(t *testing.T, config bootstrap.Config) {
	t.Helper()
	serviceUID, serviceGID, err := lookupNumericIdentity(t.Context(), "agentos")
	if err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		path string
		uid  int
		gid  int
		mode os.FileMode
	}{
		{config.Paths.ConfigDir, 0, 0, os.ModeDir | 0o755},
		{bootstrap.ConfigPath(config.Paths), 0, 0, 0o644},
		{bootstrap.StatePath(config.Paths), 0, 0, 0o644},
		{filepath.Join(config.Paths.ConfigDir, "credentials"), 0, 0, os.ModeDir | 0o700},
		{filepath.Join(config.Paths.ConfigDir, "credentials", "openai-api-key.cred"), 0, 0, 0o600},
		{config.Paths.DataDir, serviceUID, serviceGID, os.ModeDir | 0o751},
		{config.Paths.StateDir, serviceUID, serviceGID, os.ModeDir | 0o750},
		{config.Paths.CacheDir, serviceUID, serviceGID, os.ModeDir | 0o750},
		{config.Paths.Workspace, 0, serviceGID, os.ModeDir | 0o770},
		{config.Paths.RuntimeDir, 0, 0, os.ModeDir | 0o711},
		{"/usr/local/bin/agentos", 0, 0, 0o755},
		{"/etc/systemd/system/agentos.service", 0, 0, 0o644},
		{"/etc/systemd/system/agentos-user.socket", 0, 0, 0o644},
		{config.Paths.UserSocket, 0, 0, os.ModeSocket | 0o600},
	}
	for _, check := range checks {
		info, statErr := os.Lstat(check.path)
		if statErr != nil {
			t.Fatalf("inspect %s: %v", check.path, statErr)
		}
		uid, mode, ownerErr := fileOwner(check.path)
		if ownerErr != nil {
			t.Fatalf("inspect owner for %s: %v", check.path, ownerErr)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || uid != check.uid || int(stat.Gid) != check.gid || mode.Perm() != check.mode.Perm() || mode.Type() != check.mode.Type() {
			t.Fatalf("layout %s uid=%d gid=%d mode=%v; want uid=%d gid=%d mode=%v", check.path, uid, stat.Gid, mode, check.uid, check.gid, check.mode)
		}
	}
}

func assertDoctorReady(t *testing.T, binary string) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), binary, "doctor", "--json").CombinedOutput()
	if err != nil {
		t.Fatalf("packaged doctor: %v\n%s", err, output)
	}
	var result struct {
		Ready bool `json:"ready"`
	}
	if err := json.Unmarshal(output, &result); err != nil || !result.Ready {
		t.Fatalf("packaged doctor result=%s err=%v", output, err)
	}
}

func assertOnlineDoctorRejectsFakeCredential(t *testing.T, binary string) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), binary, "doctor", "--online").CombinedOutput()
	if err == nil || !strings.Contains(string(output), "decrypt provider credential failed") {
		t.Fatalf("online doctor accepted fake encrypted credential: err=%v output=%s", err, output)
	}
}

func assertDifferentUIDCannotConnect(t *testing.T, socket string) {
	t.Helper()
	script := "import socket,sys\ns=socket.socket(socket.AF_UNIX)\ntry:\n s.connect(sys.argv[1])\nexcept PermissionError:\n sys.exit(0)\nexcept OSError as error:\n print(error, file=sys.stderr)\n sys.exit(2)\nsys.exit(1)\n"
	output, err := exec.CommandContext(t.Context(), "/usr/sbin/runuser", "-u", "nobody", "--", "/usr/bin/python3", "-c", script, socket).CombinedOutput()
	if err != nil {
		t.Fatalf("different UID reached the private gateway: %v %s", err, output)
	}
}

func bootstrapOrganizationThroughPackagedService(t *testing.T, config bootstrap.Config) {
	t.Helper()
	body := `{"request_id":"installed-strategy","mission_id":"mission-installed","mission_statement":"Exercise the installed organization","goal_id":"goal-installed","goal_objective":"Retain exact governed state across restart and recovery","goal_mode":"TARGET","success_criteria":["restored state retains this organization"]}`
	response := localRequest(t, config, http.MethodPost, "/v1/user/strategy/bootstrap", body, map[string]string{"X-AgentOS-User-ID": "local-uid-65534"})
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("strategy bootstrap status=%d", response.StatusCode)
	}
	assertRequestIdentity(t, config.Paths.Database, "installed-strategy", "local-uid-65534", "local-uid-0")
}

func assertOrganizationSurvivesRestart(t *testing.T, config bootstrap.Config, missionID core.ID) {
	t.Helper()
	response := localRequest(t, config, http.MethodGet, "/v1/user/organization", "", nil)
	defer func() { _ = response.Body.Close() }()
	var snapshot app.OrganizationSnapshot
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&snapshot) != nil || snapshot.Organization.ID != "default" || len(snapshot.Missions) != 1 || snapshot.Missions[0].ID != missionID {
		t.Fatalf("organization did not survive restart: status=%d snapshot=%+v", response.StatusCode, snapshot)
	}
}

func localRequest(t *testing.T, config bootstrap.Config, method, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	client, err := localHTTPClient(config)
	if err != nil {
		t.Fatal(err)
	}
	client.Timeout = 15 * time.Second
	t.Cleanup(client.CloseIdleConnections)
	request, err := http.NewRequestWithContext(t.Context(), method, "http://agentos.local"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := client.Do(request)
	if err != nil {
		if config.Mode == bootstrap.ModeSystem {
			status, _ := exec.CommandContext(t.Context(), "/usr/bin/systemctl", "status", "--no-pager", "agentos-user.socket", "agentos.service").CombinedOutput()
			journal, _ := exec.CommandContext(t.Context(), "/usr/bin/journalctl", "--no-pager", "--unit", "agentos.service", "--lines", "100").CombinedOutput()
			t.Fatalf("local request: %v\nstatus:\n%s\njournal:\n%s", err, status, journal)
		}
		t.Fatal(err)
	}
	return response
}

func runSystemctl(t *testing.T, arguments ...string) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), "/usr/bin/systemctl", arguments...).CombinedOutput()
	if err != nil {
		verification, _ := exec.CommandContext(t.Context(), "/usr/bin/systemd-analyze", "verify", "/etc/systemd/system/agentos-user.socket", "/etc/systemd/system/agentos.service").CombinedOutput()
		status, _ := exec.CommandContext(t.Context(), "/usr/bin/systemctl", "status", "--no-pager", "agentos-user.socket", "agentos.service").CombinedOutput()
		t.Fatalf("systemctl %s: %v\n%s\nverification:\n%s\nstatus:\n%s", strings.Join(arguments, " "), err, output, verification, status)
	}
}

func waitForSystemUnit(t *testing.T, unit, socket string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		active := exec.CommandContext(t.Context(), "/usr/bin/systemctl", "is-active", "--quiet", unit).Run() == nil
		_, socketErr := os.Lstat(socket)
		if active && socketErr == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	status, _ := exec.CommandContext(t.Context(), "/usr/bin/systemctl", "status", "--no-pager", unit).CombinedOutput()
	journal, _ := exec.CommandContext(t.Context(), "/usr/bin/journalctl", "--no-pager", "--unit", unit, "--lines", "50").CombinedOutput()
	t.Fatalf("systemd unit %s did not become ready with %s\nstatus:\n%s\njournal:\n%s", unit, socket, status, journal)
}

func runRecovery(t *testing.T, binary string, arguments ...string) {
	t.Helper()
	output, err := exec.CommandContext(t.Context(), binary, arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("recovery %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	var result map[string]any
	if err := json.Unmarshal(output, &result); err != nil || result["sha256"] == "" {
		t.Fatalf("recovery result=%s err=%v", output, err)
	}
}

func assertRestoredOrganization(t *testing.T, database string) {
	assertRestoredOrganizationID(t, database, "default", "goal-installed")
}

func assertProductionRejectsUnsupportedProvider(t *testing.T, binary string, ready bootstrap.Config) {
	t.Helper()
	invalid := ready
	invalid.Providers = append([]bootstrap.Provider(nil), ready.Providers...)
	root := t.TempDir()
	invalid.Paths = bootstrap.Paths{
		ConfigDir: filepath.Join(root, "config"), DataDir: filepath.Join(root, "data"), StateDir: filepath.Join(root, "state"),
		CacheDir: filepath.Join(root, "cache"), RuntimeDir: filepath.Join(root, "run"), Workspace: filepath.Join(root, "workspace"),
		Database: filepath.Join(root, "data", "agentos.db"), UserSocket: filepath.Join(root, "run", "user.sock"),
	}
	invalid.Providers[0].Kind = bootstrap.ProviderKind("fake")
	path := filepath.Join(t.TempDir(), "invalid.json")
	if err := bootstrap.SaveConfig(path, invalid); err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(t.Context(), binary, "serve", "--config", path).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "provider must be codex-subscription or openai-api") {
		t.Fatalf("production binary accepted unsupported provider: err=%v output=%s", err, output)
	}
	if _, statErr := os.Lstat(invalid.Paths.Database); !os.IsNotExist(statErr) {
		t.Fatalf("invalid provider reached runtime storage: %v", statErr)
	}
}
