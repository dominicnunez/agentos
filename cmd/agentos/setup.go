package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/secrets"
)

func nowUTC() time.Time { return time.Now().UTC() }

func runInit(ctx context.Context, mode bootstrap.Mode, resume bool, input *os.File, output io.Writer) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("agent OS V1 setup is supported on Linux")
	}
	ui := newTerminalUI(input, output)
	completed, err := ensureInitPrivileges(ctx, mode, ui)
	if err != nil || completed {
		return err
	}
	paths, currentOwner, err := initialPaths(ctx, mode)
	if err != nil {
		return err
	}
	configPath := bootstrap.ConfigPath(paths)
	statePath := bootstrap.StatePath(paths)
	config, state, err := loadOrBeginInit(mode, currentOwner, paths, configPath, statePath)
	if err != nil {
		return err
	}
	if resume {
		if _, err := fmt.Fprintf(output, "Agent OS setup\n\nResuming: %s\n\n", state.Stage); err != nil {
			return err
		}
	} else if _, err := fmt.Fprintln(output, "Agent OS setup"); err != nil {
		return err
	}

	if state.Stage == bootstrap.StageWorkspace {
		selected, selectErr := ui.selectOne("Change default workspace:", []string{"No", "Yes"})
		if selectErr != nil {
			return selectErr
		}
		if selected == 1 {
			workspace, lineErr := ui.line("Workspace:", true)
			if lineErr != nil {
				return lineErr
			}
			workspace = filepath.Clean(workspace)
			if !filepath.IsAbs(workspace) || workspace == string(filepath.Separator) {
				return fmt.Errorf("workspace must be an absolute non-root path")
			}
			config.Paths.Workspace = workspace
		}
		state.Stage = bootstrap.StageProvider
		if err := checkpoint(configPath, statePath, &config, &state); err != nil {
			return err
		}
	}
	if state.Stage == bootstrap.StageProvider {
		provider, collectErr := collectProvider(ctx, config, input, output)
		if collectErr != nil {
			return collectErr
		}
		config.Providers = []bootstrap.Provider{provider}
		state.Stage = bootstrap.StageService
		if err := checkpoint(configPath, statePath, &config, &state); err != nil {
			return err
		}
	}
	if state.Stage == bootstrap.StageService {
		selected, selectErr := ui.selectOne("Service:", []string{"Enable and start", "Start once", "Leave stopped"})
		if selectErr != nil {
			return selectErr
		}
		if err := installRuntime(ctx, config, selected); err != nil {
			return err
		}
		state.Stage = bootstrap.StageReady
		if err := checkpoint(configPath, statePath, &config, &state); err != nil {
			return err
		}
	}
	if err := config.ValidateReady(); err != nil {
		return fmt.Errorf("setup did not produce a ready installation: %w", err)
	}
	_, err = fmt.Fprintln(output, "\nAgent OS is ready. Run agentos to open the organization console.")
	return err
}

func initialPaths(ctx context.Context, mode bootstrap.Mode) (bootstrap.Paths, bootstrap.Owner, error) {
	if mode == bootstrap.ModeSystem {
		owner, err := invokingSystemOwner(ctx)
		return bootstrap.SystemPaths(), owner, err
	}
	current, err := user.Current()
	if err != nil {
		return bootstrap.Paths{}, bootstrap.Owner{}, err
	}
	uid, err := strconv.Atoi(current.Uid)
	if err != nil {
		return bootstrap.Paths{}, bootstrap.Owner{}, fmt.Errorf("current Linux UID is invalid")
	}
	gid, err := strconv.Atoi(current.Gid)
	if err != nil {
		return bootstrap.Paths{}, bootstrap.Owner{}, fmt.Errorf("current Linux GID is invalid")
	}
	owner := bootstrap.Owner{Username: current.Username, UID: uid, GID: gid}
	if uid < 0 || gid < 0 {
		return bootstrap.Paths{}, bootstrap.Owner{}, fmt.Errorf("current Linux account is invalid")
	}
	paths, err := bootstrap.UserPaths(current.HomeDir, os.Getenv("XDG_RUNTIME_DIR"), uid)
	return paths, owner, err
}

func loadOrBeginInit(mode bootstrap.Mode, owner bootstrap.Owner, paths bootstrap.Paths, configPath, statePath string) (bootstrap.Config, bootstrap.State, error) {
	state, stateErr := bootstrap.LoadState(statePath)
	config, configErr := bootstrap.LoadConfig(configPath)
	if stateErr == nil || configErr == nil {
		if stateErr != nil || configErr != nil {
			return bootstrap.Config{}, bootstrap.State{}, fmt.Errorf("setup checkpoint is incomplete or damaged")
		}
		if state.Mode != mode || config.Mode != mode {
			return bootstrap.Config{}, bootstrap.State{}, fmt.Errorf("an existing %s installation cannot be resumed as %s", config.Mode, mode)
		}
		if config.Owner != owner {
			return bootstrap.Config{}, bootstrap.State{}, fmt.Errorf("installation belongs to Linux user %s (UID %d), not the user who started this command", config.Owner.Username, config.Owner.UID)
		}
		return config, state, nil
	}
	if !os.IsNotExist(stateErr) || !os.IsNotExist(configErr) {
		return bootstrap.Config{}, bootstrap.State{}, fmt.Errorf("load setup checkpoint: %w", errorsJoin(stateErr, configErr))
	}
	now := nowUTC()
	config = bootstrap.NewConfig(mode, owner, paths, now)
	state = bootstrap.State{Version: bootstrap.ConfigVersion, Mode: mode, Stage: bootstrap.StageWorkspace, UpdatedAt: now}
	if err := checkpoint(configPath, statePath, &config, &state); err != nil {
		return bootstrap.Config{}, bootstrap.State{}, err
	}
	return config, state, nil
}

func checkpoint(configPath, statePath string, config *bootstrap.Config, state *bootstrap.State) error {
	now := nowUTC()
	config.UpdatedAt = now
	state.UpdatedAt = now
	if config.Mode == bootstrap.ModeSystem {
		directory := filepath.Dir(configPath)
		if info, err := os.Lstat(directory); err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("system configuration path must be a directory, not a link")
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
		if err := os.Chmod(directory, 0o755); err != nil {
			return err
		}
	}
	if err := bootstrap.SaveConfig(configPath, *config); err != nil {
		return err
	}
	if err := bootstrap.SaveState(statePath, *state); err != nil {
		return err
	}
	if config.Mode == bootstrap.ModeSystem {
		if err := os.Chmod(configPath, 0o644); err != nil {
			return err
		}
		if err := os.Chmod(statePath, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func collectProvider(ctx context.Context, config bootstrap.Config, input *os.File, output io.Writer) (bootstrap.Provider, error) {
	ui := newTerminalUI(input, output)
	selected, err := ui.selectOne("Select provider:", []string{"Codex subscription", "OpenAI API"})
	if err != nil {
		return bootstrap.Provider{}, err
	}
	switch selected {
	case 0:
		return collectCodexProvider(ctx, config, ui)
	case 1:
		return collectOpenAIProvider(ctx, config, ui)
	default:
		return bootstrap.Provider{}, fmt.Errorf("provider selection is invalid")
	}
}

func collectCodexProvider(ctx context.Context, config bootstrap.Config, ui *terminalUI) (bootstrap.Provider, error) {
	binaryOptions := detectedCodexBinaries(config)
	binary, err := selectDetectedPath(ui, "Select Codex:", "Codex path:", binaryOptions)
	if err != nil {
		return bootstrap.Provider{}, err
	}
	binary, err = canonicalCodexBinary(config.Mode, binary)
	if err != nil {
		return bootstrap.Provider{}, err
	}
	credentialOptions := detectedCodexCredentials(config.Owner)
	credential, err := selectDetectedPath(ui, "Select Codex credential:", "Credential path:", credentialOptions)
	if err != nil {
		return bootstrap.Provider{}, err
	}
	credential = filepath.Clean(credential)
	adapter, err := execution.NewCodexSubscription(ctx, execution.CodexSubscriptionConfig{BinaryPath: binary, CredentialsPath: credential, Model: "model-discovery"})
	if err != nil {
		return bootstrap.Provider{}, fmt.Errorf("codex connection test failed: %w", err)
	}
	choices, err := adapter.AvailableModels(ctx)
	if err != nil {
		_ = adapter.Close()
		return bootstrap.Provider{}, err
	}
	labels := make([]string, 0, len(choices)+1)
	for _, choice := range choices {
		label := choice.DisplayName
		if label == "" || label == choice.ID {
			label = choice.ID
		} else {
			label += " (" + choice.ID + ")"
		}
		if choice.Default {
			label += " - Default"
		}
		labels = append(labels, label)
	}
	labels = append(labels, "Enter another model...")
	selected, selectErr := ui.selectOne("Select model:", labels)
	if selectErr != nil {
		_ = adapter.Close()
		return bootstrap.Provider{}, selectErr
	}
	model := ""
	if selected == len(choices) {
		model, err = ui.line("Model:", true)
	} else {
		model = choices[selected].ID
	}
	if err != nil {
		_ = adapter.Close()
		return bootstrap.Provider{}, err
	}
	if err := adapter.Close(); err != nil {
		return bootstrap.Provider{}, fmt.Errorf("close Codex connection test: %w", err)
	}
	body, err := readSetupCredential(credential, config.Owner.UID)
	if err != nil {
		return bootstrap.Provider{}, fmt.Errorf("read Codex credential for sealed storage: %w", err)
	}
	defer clearBytes(body)
	key := make([]byte, 32)
	defer clearBytes(key)
	var identity [8]byte
	if _, err := rand.Read(key); err != nil {
		return bootstrap.Provider{}, err
	}
	if _, err := rand.Read(identity[:]); err != nil {
		return bootstrap.Provider{}, err
	}
	suffix := hex.EncodeToString(identity[:])
	secretRef := "codex-store-key-" + suffix
	credentialStore := filepath.Join(config.Paths.StateDir, "providers", "codex-auth-"+suffix+".enc")
	provider := bootstrap.Provider{
		Kind: bootstrap.ProviderCodexSubscription, Model: model, SecretRef: secretRef,
		CodexBinary: binary, CodexCredential: credentialStore,
	}
	if err := provider.Validate(); err != nil {
		return bootstrap.Provider{}, err
	}
	if err := secrets.SealFile(credentialStore, "codex-auth-v1", key, body); err != nil {
		return bootstrap.Provider{}, fmt.Errorf("seal Codex credential: %w", err)
	}
	encodedKey := []byte(base64.StdEncoding.EncodeToString(key))
	defer clearBytes(encodedKey)
	if err := storeEncryptedCredential(ctx, config, secretRef, encodedKey); err != nil {
		_ = os.Remove(credentialStore)
		return bootstrap.Provider{}, err
	}
	return provider, nil
}

type pathSetupUI interface {
	selectOne(string, []string) (int, error)
	line(string, bool) (string, error)
}

func selectDetectedPath(ui pathSetupUI, selectLabel, inputLabel string, detected []string) (string, error) {
	if len(detected) == 0 {
		return ui.line(inputLabel, true)
	}
	options := append(append([]string{}, detected...), "Enter another path...")
	selected, err := ui.selectOne(selectLabel, options)
	if err != nil {
		return "", err
	}
	if selected < 0 || selected >= len(options) {
		return "", fmt.Errorf("path selection is invalid")
	}
	if selected == len(detected) {
		return ui.line(inputLabel, true)
	}
	return detected[selected], nil
}

func collectOpenAIProvider(ctx context.Context, config bootstrap.Config, ui *terminalUI) (bootstrap.Provider, error) {
	secret, err := ui.secret("API key:")
	if err != nil {
		return bootstrap.Provider{}, err
	}
	defer clearBytes(secret)
	models, err := listOpenAIModels(ctx, string(secret))
	if err != nil {
		return bootstrap.Provider{}, fmt.Errorf("OpenAI API connection test failed: %w", err)
	}
	labels := append(append([]string{}, models...), "Enter another model...")
	selected, err := ui.selectOne("Select model:", labels)
	if err != nil {
		return bootstrap.Provider{}, err
	}
	model := ""
	if selected == len(models) {
		model, err = ui.line("Model snapshot:", true)
		if err != nil {
			return bootstrap.Provider{}, err
		}
	} else {
		model = models[selected]
	}
	provider := bootstrap.Provider{Kind: bootstrap.ProviderOpenAIAPI, Model: model, SecretRef: "openai-api-key"}
	if err := provider.Validate(); err != nil {
		return bootstrap.Provider{}, err
	}
	if err := probeOpenAIModel(ctx, model, string(secret)); err != nil {
		return bootstrap.Provider{}, fmt.Errorf("OpenAI API connection test failed: %w", err)
	}
	if err := storeEncryptedCredential(ctx, config, provider.SecretRef, secret); err != nil {
		return bootstrap.Provider{}, err
	}
	return provider, nil
}

func listOpenAIModels(ctx context.Context, key string) ([]string, error) {
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return nil, fmt.Errorf("credential is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.openai.com/v1/models", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+key)
	client := setupHTTPClient()
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request did not complete")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return nil, fmt.Errorf("provider response was not JSON")
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	if err := decoder.Decode(&body); err != nil || len(body.Data) > 10_000 {
		return nil, fmt.Errorf("provider model list is invalid")
	}
	seen := make(map[string]struct{})
	models := make([]string, 0, len(body.Data))
	for _, model := range body.Data {
		candidate := bootstrap.Provider{Kind: bootstrap.ProviderOpenAIAPI, Model: model.ID, SecretRef: "model-discovery"}
		if candidate.Validate() != nil {
			continue
		}
		if _, exists := seen[model.ID]; exists {
			continue
		}
		seen[model.ID] = struct{}{}
		models = append(models, model.ID)
	}
	if len(models) == 0 {
		return nil, fmt.Errorf("provider returned no dated model snapshots")
	}
	sort.Sort(sort.Reverse(sort.StringSlice(models)))
	return models, nil
}

func probeOpenAIModel(ctx context.Context, model, key string) error {
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return fmt.Errorf("credential is invalid")
	}
	endpoint := "https://api.openai.com/v1/models/" + url.PathEscape(model)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+key)
	client := setupHTTPClient()
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("request did not complete")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return fmt.Errorf("provider response was not JSON")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 64<<10))
	var body struct {
		ID string `json:"id"`
	}
	if err := decoder.Decode(&body); err != nil || body.ID != model {
		return fmt.Errorf("configured model is not available to this credential")
	}
	return nil
}

func setupHTTPClient() *http.Client {
	transport, _ := http.DefaultTransport.(*http.Transport)
	if transport == nil {
		transport = &http.Transport{}
	} else {
		transport = transport.Clone()
	}
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return &http.Client{Transport: transport, Timeout: 20 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func detectedCodexBinaries(config bootstrap.Config) []string {
	seen := make(map[string]struct{})
	paths := make([]string, 0, 4)
	add := func(path string) {
		path, err := canonicalCodexBinary(config.Mode, path)
		if err != nil {
			return
		}
		if _, exists := seen[path]; !exists {
			seen[path] = struct{}{}
			paths = append(paths, path)
		}
	}
	if path, err := exec.LookPath("codex"); err == nil {
		if absolute, absoluteErr := filepath.Abs(path); absoluteErr == nil {
			add(absolute)
		}
	}
	if account, err := user.LookupId(strconv.Itoa(config.Owner.UID)); err == nil {
		add(filepath.Join(account.HomeDir, ".local", "bin", "codex"))
		add(filepath.Join(account.HomeDir, ".npm-global", "bin", "codex"))
	}
	return paths
}

func detectedCodexCredentials(owner bootstrap.Owner) []string {
	current, err := user.LookupId(strconv.Itoa(owner.UID))
	if err != nil {
		return nil
	}
	candidate := filepath.Join(current.HomeDir, ".codex", "auth.json")
	if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
		return []string{candidate}
	}
	return nil
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func errorsJoin(errors ...error) error {
	parts := make([]string, 0, len(errors))
	for _, err := range errors {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return fmt.Errorf("%s", strings.Join(parts, "; "))
}
