// Package bootstrap owns installation configuration and readiness validation.
// It does not start the Agent OS runtime or make policy decisions.
package bootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/fileguard"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/modelid"
)

const (
	legacyConfigVersion = 1
	ConfigVersion       = 2
)

type Mode string

const (
	ModeSystem Mode = "system"
	ModeUser   Mode = "user"
)

type Stage string

const (
	StageWorkspace Stage = "workspace"
	StageProvider  Stage = "provider"
	StageService   Stage = "service"
	StageReady     Stage = "ready"
)

type ProviderKind string

const (
	ProviderCodexSubscription ProviderKind = "codex-subscription"
	ProviderOpenAIAPI         ProviderKind = "openai-api"
)

type Paths struct {
	ConfigDir  string `json:"config_dir"`
	DataDir    string `json:"data_dir"`
	StateDir   string `json:"state_dir"`
	CacheDir   string `json:"cache_dir"`
	RuntimeDir string `json:"runtime_dir"`
	Workspace  string `json:"workspace"`
	Database   string `json:"database"`
	UserSocket string `json:"user_socket"`
}

type Owner struct {
	Username string `json:"username"`
	UID      int    `json:"uid"`
	GID      int    `json:"gid"`
}

type Provider struct {
	Kind            ProviderKind     `json:"kind"`
	Model           string           `json:"model"`
	SecretRef       string           `json:"secret_ref,omitempty"`
	CodexBinary     string           `json:"codex_binary,omitempty"`
	CodexCredential string           `json:"codex_credential_store,omitempty"`
	InferencePolicy inference.Policy `json:"inference_policy"`
}

type A2A struct {
	ListenAddress string `json:"listen_address"`
	AllowRemote   bool   `json:"allow_remote"`
	PublicURL     string `json:"public_url,omitempty"`
	ActorsFile    string `json:"actors_file,omitempty"`
	TLSCertFile   string `json:"tls_cert_file,omitempty"`
	TLSKeyFile    string `json:"tls_key_file,omitempty"`
}

type Config struct {
	Version      int        `json:"version"`
	Mode         Mode       `json:"mode"`
	Owner        Owner      `json:"owner"`
	Organization string     `json:"organization"`
	Paths        Paths      `json:"paths"`
	Providers    []Provider `json:"providers"`
	A2A          A2A        `json:"a2a"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

type State struct {
	Version   int       `json:"version"`
	Mode      Mode      `json:"mode"`
	Stage     Stage     `json:"stage"`
	UpdatedAt time.Time `json:"updated_at"`
}

func SystemPaths() Paths {
	return Paths{
		ConfigDir: "/etc/agentos", DataDir: "/var/lib/agentos", StateDir: "/var/lib/agentos/state",
		CacheDir: "/var/cache/agentos", RuntimeDir: "/run/agentos", Workspace: "/var/lib/agentos/workspaces",
		Database: "/var/lib/agentos/agentos.db", UserSocket: "/run/agentos/user.sock",
	}
}

func UserPaths(home, runtimeDir string, _ int) (Paths, error) {
	home = filepath.Clean(home)
	if !filepath.IsAbs(home) || home == string(filepath.Separator) {
		return Paths{}, fmt.Errorf("user home must be an absolute non-root path")
	}
	if runtimeDir == "" {
		return Paths{}, fmt.Errorf("user runtime directory must be supplied by XDG_RUNTIME_DIR")
	}
	runtimeDir = filepath.Clean(runtimeDir)
	if !filepath.IsAbs(runtimeDir) || runtimeDir == string(filepath.Separator) {
		return Paths{}, fmt.Errorf("user runtime directory must be an absolute non-root path")
	}
	data := filepath.Join(home, ".local", "share", "agentos")
	state := filepath.Join(home, ".local", "state", "agentos")
	return Paths{
		ConfigDir: filepath.Join(home, ".config", "agentos"), DataDir: data, StateDir: state,
		CacheDir: filepath.Join(home, ".cache", "agentos"), RuntimeDir: filepath.Join(runtimeDir, "agentos"),
		Workspace: filepath.Join(data, "workspaces"), Database: filepath.Join(data, "agentos.db"),
		UserSocket: filepath.Join(runtimeDir, "agentos", "user.sock"),
	}, nil
}

func ConfigPath(paths Paths) string { return filepath.Join(paths.ConfigDir, "config.json") }
func StatePath(paths Paths) string  { return filepath.Join(paths.ConfigDir, "init.json") }

func NewConfig(mode Mode, owner Owner, paths Paths, now time.Time) Config {
	return Config{
		Version: ConfigVersion, Mode: mode, Owner: owner, Organization: "default", Paths: paths,
		A2A: A2A{ListenAddress: "127.0.0.1:8080"}, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
}

func (c Config) ValidateReady() error {
	var problems []error
	if c.Version != ConfigVersion {
		problems = append(problems, fmt.Errorf("unsupported configuration version %d", c.Version))
	}
	problems = append(problems, configurationIdentityProblems(c.Mode, c.Owner, c.Organization, c.Paths, "owner must be the verified Linux user who started setup")...)
	if len(c.Providers) == 0 {
		problems = append(problems, fmt.Errorf("at least one real model provider is required"))
	} else if len(c.Providers) != 1 {
		problems = append(problems, fmt.Errorf("V1 requires exactly one active model provider"))
	}
	for index, provider := range c.Providers {
		if err := provider.Validate(); err != nil {
			problems = append(problems, fmt.Errorf("provider %d: %w", index+1, err))
		}
		if provider.Kind == ProviderCodexSubscription && !pathWithin(c.Paths.StateDir, provider.CodexCredential) {
			problems = append(problems, fmt.Errorf("provider %d: Codex credential store must remain inside the state directory", index+1))
		}
		if provider.InferencePolicy.OrganizationID != c.Organization || provider.InferencePolicy.AuthorizedBy != "local-uid-"+strconv.Itoa(c.Owner.UID) {
			problems = append(problems, fmt.Errorf("provider %d: inference policy must be approved for this organization by the installation owner", index+1))
		}
	}
	if err := validateA2A(c.A2A); err != nil {
		problems = append(problems, err)
	}
	for _, configuredPath := range []struct {
		label string
		path  string
	}{
		{label: "actor registry", path: c.A2A.ActorsFile},
		{label: "TLS certificate", path: c.A2A.TLSCertFile},
		{label: "TLS private key", path: c.A2A.TLSKeyFile},
	} {
		label, path := configuredPath.label, configuredPath.path
		if path != "" && !pathWithin(c.Paths.ConfigDir, path) {
			problems = append(problems, fmt.Errorf("A2A %s must remain inside the configuration directory", label))
		}
	}
	return errors.Join(problems...)
}

// UpgradeVersion1Checkpoint validates the one prior setup format and converts
// it into an incomplete current checkpoint. Provider configuration is cleared
// deliberately because version 1 contains no reviewed inference policy; setup
// must collect and verify that policy before the installation can run again.
func UpgradeVersion1Checkpoint(config Config, state State) (Config, State, error) {
	if config.Version != legacyConfigVersion || state.Version != legacyConfigVersion || config.Mode != state.Mode {
		return Config{}, State{}, fmt.Errorf("only a matching version-1 checkpoint can be upgraded")
	}
	if err := validateVersion1Config(config); err != nil {
		return Config{}, State{}, fmt.Errorf("validate version-1 configuration: %w", err)
	}
	if !validStage(state.Stage) || state.UpdatedAt.IsZero() || state.UpdatedAt.Location() != time.UTC {
		return Config{}, State{}, fmt.Errorf("version-1 initialization state is invalid")
	}
	upgradedConfig := Config{
		Version: ConfigVersion, Mode: config.Mode, Owner: config.Owner, Organization: config.Organization,
		Paths: config.Paths, A2A: config.A2A, CreatedAt: config.CreatedAt, UpdatedAt: config.UpdatedAt,
	}
	upgradedState := State{Version: ConfigVersion, Mode: state.Mode, Stage: StageProvider, UpdatedAt: state.UpdatedAt}
	return upgradedConfig, upgradedState, nil
}

func validateVersion1Config(config Config) error {
	problems := configurationIdentityProblems(config.Mode, config.Owner, config.Organization, config.Paths, "owner must be a verified Linux user")
	if len(config.Providers) > 1 {
		problems = append(problems, fmt.Errorf("version-1 configuration has multiple providers"))
	}
	for _, provider := range config.Providers {
		if err := validateVersion1Provider(provider); err != nil {
			problems = append(problems, err)
		}
		if provider.Kind == ProviderCodexSubscription && !pathWithin(config.Paths.StateDir, provider.CodexCredential) {
			problems = append(problems, fmt.Errorf("codex credential store must remain inside the state directory"))
		}
	}
	if err := validateA2A(config.A2A); err != nil {
		problems = append(problems, err)
	}
	for _, path := range []string{config.A2A.ActorsFile, config.A2A.TLSCertFile, config.A2A.TLSKeyFile} {
		if path != "" && !pathWithin(config.Paths.ConfigDir, path) {
			problems = append(problems, fmt.Errorf("A2A source must remain inside the configuration directory"))
		}
	}
	if config.CreatedAt.IsZero() || config.UpdatedAt.IsZero() || config.CreatedAt.Location() != time.UTC || config.UpdatedAt.Location() != time.UTC || config.UpdatedAt.Before(config.CreatedAt) {
		problems = append(problems, fmt.Errorf("configuration timestamps are invalid"))
	}
	return errors.Join(problems...)
}

func configurationIdentityProblems(mode Mode, owner Owner, organization string, paths Paths, ownerProblem string) []error {
	var problems []error
	if mode != ModeSystem && mode != ModeUser {
		problems = append(problems, fmt.Errorf("mode must be system or user"))
	}
	if !validLinuxAccountName(owner.Username) || owner.Username == "agentos" || owner.UID < 0 || owner.GID < 0 {
		problems = append(problems, errors.New(ownerProblem))
	}
	if !validIdentifier(organization) {
		problems = append(problems, fmt.Errorf("organization is required"))
	}
	if err := validatePaths(paths); err != nil {
		problems = append(problems, err)
	}
	return problems
}

func validateVersion1Provider(provider Provider) error {
	switch provider.Kind {
	case ProviderOpenAIAPI:
		if !validModelIdentifier(provider.Model) || !modelid.HasDatedSnapshot(provider.Model) || !validCredentialRef(provider.SecretRef) || provider.CodexBinary != "" || provider.CodexCredential != "" {
			return fmt.Errorf("version-1 OpenAI API provider is invalid")
		}
	case ProviderCodexSubscription:
		if !validModelIdentifier(provider.Model) || !canonicalAbsolutePath(provider.CodexBinary) || !canonicalAbsolutePath(provider.CodexCredential) || !validCredentialRef(provider.SecretRef) {
			return fmt.Errorf("version-1 Codex subscription provider is invalid")
		}
	default:
		return fmt.Errorf("version-1 provider kind is invalid")
	}
	return nil
}

func pathWithin(root, target string) bool {
	if !canonicalAbsolutePath(root) || !canonicalAbsolutePath(target) {
		return false
	}
	relative, err := filepath.Rel(root, target)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (p Provider) Validate() error {
	if err := p.InferencePolicy.Validate(); err != nil {
		return fmt.Errorf("inference policy: %w", err)
	}
	if p.InferencePolicy.Model != p.Model {
		return fmt.Errorf("inference policy model does not match the provider")
	}
	switch p.Kind {
	case ProviderOpenAIAPI:
		if strings.TrimSpace(p.Model) == "" || strings.TrimSpace(p.SecretRef) == "" {
			return fmt.Errorf("OpenAI API model and secret reference are required")
		}
		if !validCredentialRef(p.SecretRef) {
			return fmt.Errorf("OpenAI API secret reference is invalid")
		}
		if !validModelIdentifier(p.Model) || !modelid.HasDatedSnapshot(p.Model) {
			return fmt.Errorf("OpenAI API model must identify an exact dated snapshot")
		}
		if p.CodexBinary != "" || p.CodexCredential != "" {
			return fmt.Errorf("OpenAI API provider cannot contain Codex settings")
		}
		if p.InferencePolicy.Provider != "openai-api" || p.InferencePolicy.ExecutionProfileVersion != "v1-openai-responses-model-only" || p.InferencePolicy.Mode != inference.MeteredAPI {
			return fmt.Errorf("OpenAI API inference policy classification is invalid")
		}
	case ProviderCodexSubscription:
		if !validModelIdentifier(p.Model) || !canonicalAbsolutePath(p.CodexBinary) || !canonicalAbsolutePath(p.CodexCredential) || !validCredentialRef(p.SecretRef) {
			return fmt.Errorf("codex model, binary, sealed credential store, and key reference are required")
		}
		if p.InferencePolicy.Provider != "codex-subscription" || p.InferencePolicy.ExecutionProfileVersion != "v1-codex-subscription-restricted" || p.InferencePolicy.Mode != inference.Subscription {
			return fmt.Errorf("codex subscription inference policy classification is invalid")
		}
	default:
		return fmt.Errorf("provider must be codex-subscription or openai-api")
	}
	return nil
}

func validModelIdentifier(model string) bool {
	return model != "" && len(model) <= 128 && strings.TrimSpace(model) == model && !strings.ContainsAny(model, "\r\n\t ") && strings.IndexFunc(model, func(character rune) bool {
		return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
	}) < 0
}

func validateA2A(config A2A) error {
	host, port, err := net.SplitHostPort(config.ListenAddress)
	if err != nil || port == "" {
		return fmt.Errorf("A2A listen address must be a host:port address")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("A2A listen port is invalid")
	}
	parsedIP := net.ParseIP(host)
	remote := host == "" || (!strings.EqualFold(host, "localhost") && (parsedIP == nil || !parsedIP.IsLoopback()))
	if remote && !config.AllowRemote {
		return fmt.Errorf("remote A2A listening must be enabled explicitly")
	}
	if config.ActorsFile != "" && !canonicalAbsolutePath(config.ActorsFile) {
		return fmt.Errorf("A2A actor registry must be a canonical absolute path")
	}
	if (config.TLSCertFile == "") != (config.TLSKeyFile == "") {
		return fmt.Errorf("A2A TLS certificate and key must be configured together")
	}
	if config.TLSCertFile != "" && (!canonicalAbsolutePath(config.TLSCertFile) || !canonicalAbsolutePath(config.TLSKeyFile)) {
		return fmt.Errorf("A2A TLS files must use canonical absolute paths")
	}
	if remote && (config.ActorsFile == "" || config.TLSCertFile == "") {
		return fmt.Errorf("remote A2A requires a reviewed actor registry and TLS")
	}
	if config.PublicURL == "" {
		if remote && config.ActorsFile != "" {
			return fmt.Errorf("remote A2A requires an HTTPS public URL")
		}
		return nil
	}
	parsed, err := url.Parse(config.PublicURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("A2A public URL must be an absolute HTTP(S) origin")
	}
	if (remote || config.TLSCertFile != "") && parsed.Scheme != "https" {
		return fmt.Errorf("remote or TLS-enabled A2A requires an HTTPS public URL")
	}
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' || character == ':' {
			continue
		}
		return false
	}
	return true
}

func canonicalAbsolutePath(value string) bool {
	cleaned := filepath.Clean(value)
	return filepath.IsAbs(cleaned) && cleaned != string(filepath.Separator) && cleaned == value && strings.IndexFunc(cleaned, func(character rune) bool {
		return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
	}) < 0
}

func validLinuxAccountName(name string) bool {
	if name == "" || len(name) > 32 {
		return false
	}
	for index, character := range name {
		if character == '$' && index > 0 && index == len(name)-1 {
			continue
		}
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') || character == '_' || (index > 0 && ((character >= '0' && character <= '9') || character == '-')) {
			continue
		}
		return false
	}
	return true
}

func validCredentialRef(name string) bool {
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

func validatePaths(paths Paths) error {
	values := []struct {
		name, value string
	}{
		{"config directory", paths.ConfigDir}, {"data directory", paths.DataDir}, {"state directory", paths.StateDir},
		{"cache directory", paths.CacheDir}, {"runtime directory", paths.RuntimeDir}, {"workspace", paths.Workspace},
		{"database", paths.Database}, {"user socket", paths.UserSocket},
	}
	var problems []error
	for _, value := range values {
		if !canonicalAbsolutePath(value.value) {
			problems = append(problems, fmt.Errorf("%s must be a canonical absolute non-root path", value.name))
		}
	}
	return errors.Join(problems...)
}

func LoadConfig(path string) (Config, error) {
	var config Config
	if err := decodeFile(path, "configuration", &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func LoadState(path string) (State, error) {
	var state State
	if err := decodeFile(path, "initialization state", &state); err != nil {
		return State{}, err
	}
	if (state.Version != legacyConfigVersion && state.Version != ConfigVersion) || (state.Mode != ModeSystem && state.Mode != ModeUser) || !validStage(state.Stage) {
		return State{}, fmt.Errorf("initialization state is invalid")
	}
	return state, nil
}

func validStage(stage Stage) bool {
	return stage == StageWorkspace || stage == StageProvider || stage == StageService || stage == StageReady
}

func SaveConfig(path string, config Config) error { return writeJSON(path, config, 0o600) }
func SaveState(path string, state State) error    { return writeJSON(path, state, 0o600) }

func decodeFile(path, label string, target any) error {
	file, err := openRegularNoSymlink(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", label, err)
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return fmt.Errorf("read %s: %w", label, err)
	}
	if len(body) > 1<<20 {
		return fmt.Errorf("decode %s: file exceeds limit", label)
	}
	if err := boundaryjson.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode %s: %w", label, err)
	}

	return nil
}

func openRegularNoSymlink(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file, not a link", path)
	}
	return os.Open(path)
}

func writeJSON(path string, value any, mode os.FileMode) error {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(bytes.TrimSpace(encoded), '\n')
	return fileguard.WriteAtomically(path, encoded, mode, 0o700)
}
