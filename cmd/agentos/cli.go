package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	ledgerrecovery "github.com/dominicnunez/agentos/internal/ledger/recovery"
	"github.com/dominicnunez/agentos/internal/secrets"
)

func execute(ctx context.Context, args []string, input *os.File, output, errorOutput io.Writer) error {
	if ctx == nil || input == nil || output == nil || errorOutput == nil {
		return fmt.Errorf("command context and streams are required")
	}
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		_, err := printVersion(args, output)
		return err
	}
	if len(args) == 0 {
		configPath, config, state, err := discoverInstallation()
		switch {
		case err == nil && state.Stage == bootstrap.StageReady:
			return runTUI(ctx, configPath, config, input, output)
		case err == nil:
			return runInit(ctx, config.Mode, true, input, output)
		case errors.Is(err, os.ErrNotExist):
			return runInit(ctx, bootstrap.ModeSystem, false, input, output)
		default:
			return fmt.Errorf("installation is invalid; run agentos doctor: %w", err)
		}
	}
	switch args[0] {
	case "init":
		mode, err := parseInitMode(args[1:])
		if err != nil {
			return err
		}
		return runInit(ctx, mode, false, input, output)
	case "serve":
		configPath, err := parseConfigPath("serve", args[1:])
		if err != nil {
			return err
		}
		config, err := bootstrap.LoadConfig(configPath)
		if err != nil {
			return err
		}
		return runServer(ctx, config, secrets.CredentialDirectory{})
	case "doctor":
		return runDoctor(ctx, args[1:], output)
	case "setup":
		if len(args) != 2 || args[1] != "provider" {
			return fmt.Errorf("use agentos setup provider")
		}
		return runProviderSetup(ctx, input, output)
	case "help", "--help", "-h":
		return printHelp(output)
	default:
		return fmt.Errorf("unknown command %q; use agentos help", args[0])
	}
}

func parseInitMode(args []string) (bootstrap.Mode, error) {
	mode := bootstrap.ModeSystem
	for _, argument := range args {
		switch argument {
		case "--system":
			mode = bootstrap.ModeSystem
		case "--user":
			mode = bootstrap.ModeUser
		default:
			return "", fmt.Errorf("use agentos init, agentos init --system, or agentos init --user")
		}
	}
	return mode, nil
}

func parseConfigPath(command string, args []string) (string, error) {
	set := flag.NewFlagSet(command, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	path := set.String("config", "", "configuration path")
	if err := set.Parse(args); err != nil || len(set.Args()) != 0 {
		return "", fmt.Errorf("use agentos %s --config <path>", command)
	}
	if *path == "" {
		discovered, _, state, err := discoverInstallation()
		if err != nil {
			return "", err
		}
		if state.Stage != bootstrap.StageReady {
			return "", fmt.Errorf("initialization is incomplete at %s", state.Stage)
		}
		return discovered, nil
	}
	cleaned := filepath.Clean(*path)
	if !filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("configuration path must be absolute")
	}
	return cleaned, nil
}

func discoverInstallation() (string, bootstrap.Config, bootstrap.State, error) {
	paths := []bootstrap.Paths{bootstrap.SystemPaths()}
	current, err := user.Current()
	if err == nil {
		uid, parseErr := strconv.Atoi(current.Uid)
		if parseErr == nil && uid >= 0 {
			userPaths, pathErr := bootstrap.UserPaths(current.HomeDir, os.Getenv("XDG_RUNTIME_DIR"), uid)
			if pathErr == nil {
				paths = append(paths, userPaths)
			}
		}
	}
	for _, candidate := range paths {
		state, stateErr := bootstrap.LoadState(bootstrap.StatePath(candidate))
		config, configErr := bootstrap.LoadConfig(bootstrap.ConfigPath(candidate))
		if stateErr == nil && configErr == nil {
			return bootstrap.ConfigPath(candidate), config, state, nil
		}
		if !errors.Is(stateErr, os.ErrNotExist) || !errors.Is(configErr, os.ErrNotExist) {
			if stateErr != nil && !errors.Is(stateErr, os.ErrNotExist) {
				return "", bootstrap.Config{}, bootstrap.State{}, stateErr
			}
			if configErr != nil && !errors.Is(configErr, os.ErrNotExist) {
				return "", bootstrap.Config{}, bootstrap.State{}, configErr
			}
			return "", bootstrap.Config{}, bootstrap.State{}, fmt.Errorf("installation has only one of config or state")
		}
	}
	return "", bootstrap.Config{}, bootstrap.State{}, os.ErrNotExist
}

func runProviderSetup(ctx context.Context, input *os.File, output io.Writer) error {
	configPath, config, state, err := discoverInstallation()
	if err != nil {
		return fmt.Errorf("initialize Agent OS first: %w", err)
	}
	if state.Stage != bootstrap.StageReady {
		return fmt.Errorf("initialization is incomplete; run agentos")
	}
	if err := config.ValidateReady(); err != nil {
		return fmt.Errorf("installation configuration is invalid: %w", err)
	}
	ui := newTerminalUI(input, output)
	completed, err := ensureProviderSetupPrivileges(ctx, config, ui)
	if err != nil || completed {
		return err
	}
	previous := config.Providers[0]
	provider, err := collectProvider(ctx, config, input, output)
	if err != nil {
		return err
	}
	config.Providers = []bootstrap.Provider{provider}
	config.UpdatedAt = nowUTC()
	if err := bootstrap.SaveConfig(configPath, config); err != nil {
		return err
	}
	if config.Mode == bootstrap.ModeSystem {
		if err := os.Chmod(configPath, 0o644); err != nil {
			return err
		}
	}
	if err := applyProviderRuntime(ctx, config); err != nil {
		return fmt.Errorf("apply provider to the installed service: %w", err)
	}
	if err := removeObsoleteProviderCredentials(config, previous, provider); err != nil {
		return fmt.Errorf("remove replaced provider credential: %w", err)
	}
	_, err = fmt.Fprintln(output, "Provider ready.")
	return err
}

func removeObsoleteProviderCredentials(config bootstrap.Config, previous, current bootstrap.Provider) error {
	paths := make([]string, 0, 2)
	if previous.SecretRef != "" && previous.SecretRef != current.SecretRef {
		paths = append(paths, filepath.Join(config.Paths.ConfigDir, "credentials", previous.SecretRef+".cred"))
	}
	if previous.Kind == bootstrap.ProviderCodexSubscription && previous.CodexCredential != "" && previous.CodexCredential != current.CodexCredential {
		relative, err := filepath.Rel(config.Paths.StateDir, previous.CodexCredential)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("previous Codex credential store is outside the state directory")
		}
		paths = append(paths, previous.CodexCredential)
	}
	for _, path := range paths {
		if info, err := os.Lstat(path); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return fmt.Errorf("refuse to remove non-regular credential %s", path)
			}
			if err := os.Remove(path); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func printHelp(output io.Writer) error {
	_, err := fmt.Fprintln(output, `Agent OS

  agentos                 Resume setup or open the organization console
  agentos init            Set up a system installation (default)
  agentos init --user     Set up an installation for the current Linux user
  agentos doctor          Inspect local health without changing anything
  agentos setup provider  Replace and test the configured model provider
  agentos serve           Run the configured service
  agentos version         Print the version`)
	return err
}

type doctorCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

func runDoctor(ctx context.Context, args []string, output io.Writer) error {
	set := flag.NewFlagSet("doctor", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	jsonOutput := set.Bool("json", false, "JSON output")
	online := set.Bool("online", false, "perform external provider check")
	if err := set.Parse(args); err != nil || len(set.Args()) != 0 {
		return fmt.Errorf("use agentos doctor [--json] [--online]")
	}
	configPath, config, state, err := discoverInstallation()
	checks := make([]doctorCheck, 0, 12)
	if err != nil {
		checks = append(checks, doctorCheck{Name: "installation", Status: "BLOCKED", Detail: "No valid installation. Run agentos to start or resume setup."})
		return writeDoctor(output, checks, *jsonOutput, true)
	}
	checks = append(checks, doctorCheck{Name: "initialization", Status: status(state.Stage == bootstrap.StageReady), Detail: "stage=" + string(state.Stage)})
	validationErr := config.ValidateReady()
	checks = append(checks, doctorCheck{Name: "configuration", Status: status(validationErr == nil), Detail: detail(validationErr, configPath)})
	policyErr := doctorInferencePolicy(config, nowUTC())
	checks = append(checks, doctorCheck{Name: "inference authorization", Status: status(policyErr == nil), Detail: detail(policyErr, "current reviewed provider budget")})
	for _, configuredPath := range []struct {
		name string
		path string
	}{
		{name: "data", path: config.Paths.DataDir},
		{name: "state", path: config.Paths.StateDir},
		{name: "workspace", path: config.Paths.Workspace},
	} {
		name, path := configuredPath.name, configuredPath.path
		info, pathErr := os.Stat(path)
		ok := pathErr == nil && info.IsDir()
		checks = append(checks, doctorCheck{Name: name + " path", Status: status(ok), Detail: detail(pathErr, path)})
	}
	if _, ledgerErr := os.Lstat(config.Paths.Database); errors.Is(ledgerErr, os.ErrNotExist) {
		checks = append(checks, doctorCheck{Name: "event ledger", Status: "INFO", Detail: "not created until the service starts"})
	} else if ledgerErr != nil {
		checks = append(checks, doctorCheck{Name: "event ledger", Status: "BLOCKED", Detail: ledgerErr.Error()})
	} else if config.Mode == bootstrap.ModeSystem && effectiveUID() != 0 {
		checks = append(checks, doctorCheck{Name: "event ledger", Status: "INFO", Detail: "administrator access is required to verify private storage"})
	} else if result, verifyErr := ledgerrecovery.Verify(ctx, config.Paths.Database); verifyErr != nil {
		checks = append(checks, doctorCheck{Name: "event ledger", Status: "BLOCKED", Detail: verifyErr.Error()})
	} else {
		checks = append(checks, doctorCheck{Name: "event ledger", Status: "PASS", Detail: fmt.Sprintf("%d events; sha256 %s", result.EventCount, result.SHA256[:12])})
	}
	credentialErr := error(nil)
	if config.Mode == bootstrap.ModeSystem && effectiveUID() != 0 {
		checks = append(checks, doctorCheck{Name: "credential", Status: "INFO", Detail: "administrator access is required to inspect encrypted provider storage"})
	} else {
		credentialErr = doctorProviderCredential(config)
		if _, serviceCredentialErr := serviceCredentialDirectives(config); serviceCredentialErr != nil {
			credentialErr = errors.Join(credentialErr, serviceCredentialErr)
		}
		checks = append(checks, doctorCheck{Name: "credential", Status: status(credentialErr == nil), Detail: detail(credentialErr, "configured service credentials are present and protected")})
	}
	socketStatus, socketDetail := doctorUserSocket(config)
	checks = append(checks, doctorCheck{Name: "user gateway", Status: socketStatus, Detail: socketDetail})
	serviceStatus, serviceDetail := doctorService(ctx, config)
	checks = append(checks, doctorCheck{Name: "service", Status: serviceStatus, Detail: serviceDetail})
	if *online && validationErr == nil {
		providerErr := error(nil)
		if config.Mode == bootstrap.ModeSystem && effectiveUID() != 0 {
			providerErr = fmt.Errorf("administrator access is required for the system provider check")
		} else {
			providerErr = doctorProviderOnline(ctx, config)
		}
		checks = append(checks, doctorCheck{Name: "provider", Status: status(providerErr == nil), Detail: detail(providerErr, string(config.Providers[0].Kind))})
	} else {
		checks = append(checks, doctorCheck{Name: "provider", Status: "INFO", Detail: "configuration only; use --online for an external check"})
	}
	blocked := false
	for _, check := range checks {
		if check.Status == "BLOCKED" {
			blocked = true
			break
		}
	}
	return writeDoctor(output, checks, *jsonOutput, blocked)
}

func doctorInferencePolicy(config bootstrap.Config, now time.Time) error {
	if len(config.Providers) != 1 {
		return fmt.Errorf("exactly one inference policy is required")
	}
	policy := config.Providers[0].InferencePolicy
	if err := policy.Validate(); err != nil {
		return err
	}
	if now.Before(policy.AuthorizedAt) || !now.Before(policy.AuthorizationExpiresAt) {
		return fmt.Errorf("inference authorization is not currently valid")
	}
	if policy.Pricing != nil && !now.Before(policy.Pricing.ExpiresAt) {
		return fmt.Errorf("inference pricing is stale")
	}
	return nil
}

func doctorProviderCredential(config bootstrap.Config) error {
	if len(config.Providers) != 1 {
		return fmt.Errorf("exactly one active provider is required")
	}
	provider := config.Providers[0]
	paths := []string{filepath.Join(config.Paths.ConfigDir, "credentials", provider.SecretRef+".cred")}
	if provider.Kind == bootstrap.ProviderCodexSubscription {
		paths = append(paths, provider.CodexCredential)
	}
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 65<<10 {
			return fmt.Errorf("provider credential is not a bounded regular file")
		}
		if info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("provider credential permissions are too broad")
		}
	}
	return nil
}

func doctorProviderOnline(ctx context.Context, config bootstrap.Config) error {
	if err := doctorProviderCredential(config); err != nil {
		return err
	}
	provider := config.Providers[0]
	if provider.Kind == bootstrap.ProviderOpenAIAPI {
		secret, err := decryptProviderCredential(ctx, config.Mode, filepath.Join(config.Paths.ConfigDir, "credentials", provider.SecretRef+".cred"), provider.SecretRef)
		if err != nil {
			return err
		}
		defer clearBytes(secret)
		return probeOpenAIModel(ctx, provider.Model, string(secret))
	}
	encodedKey, err := decryptProviderCredential(ctx, config.Mode, filepath.Join(config.Paths.ConfigDir, "credentials", provider.SecretRef+".cred"), provider.SecretRef)
	if err != nil {
		return err
	}
	defer clearBytes(encodedKey)
	model, closeModel, err := configuredProvider(ctx, provider, providerRuntimeDirectory(config), secrets.Values{secrets.Ref(provider.SecretRef): secrets.Value(encodedKey)})
	if err != nil {
		return err
	}
	_ = model
	return closeModel()
}

func writeDoctor(output io.Writer, checks []doctorCheck, asJSON, blocked bool) error {
	if asJSON {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(struct {
			Ready  bool          `json:"ready"`
			Checks []doctorCheck `json:"checks"`
		}{Ready: !blocked, Checks: checks}); err != nil {
			return err
		}
	} else {
		for _, check := range checks {
			if _, err := fmt.Fprintf(output, "%-8s  %-18s %s\n", safeTerminalText(check.Status), safeTerminalText(check.Name), safeTerminalText(check.Detail)); err != nil {
				return err
			}
		}
	}
	if blocked {
		return fmt.Errorf("agent OS has blocking health findings")
	}
	return nil
}

func status(ok bool) string {
	if ok {
		return "PASS"
	}
	return "BLOCKED"
}

func detail(err error, fallback string) string {
	if err != nil {
		return err.Error()
	}
	return fallback
}

func canonicalInput(value string) string { return strings.TrimSpace(value) }
