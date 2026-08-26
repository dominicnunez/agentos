package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/artifacts"
	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/effectstatus"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/fileguard"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
	ledgeranchor "github.com/dominicnunez/agentos/internal/ledger/anchor"
	"github.com/dominicnunez/agentos/internal/planning"
	"github.com/dominicnunez/agentos/internal/secrets"
)

var version = "1.0.0-dev"

const systemProviderRuntimeDirectory = "/run/agentos-private"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
}

func printVersion(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	if len(args) != 1 || (args[0] != "--version" && args[0] != "version") {
		return false, fmt.Errorf("unsupported argument; use agentos help")
	}
	if output == nil {
		return false, fmt.Errorf("version output is required")
	}
	_, err := fmt.Fprintln(output, version)
	return true, err
}

func runServer(ctx context.Context, config bootstrap.Config, source secrets.Source) (err error) {
	if ctx == nil {
		return fmt.Errorf("runtime context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := config.ValidateReady(); err != nil {
		return fmt.Errorf("agent OS is not ready: %w", err)
	}
	if err := validateRuntimeBoundary(config); err != nil {
		return fmt.Errorf("runtime boundary is unsafe: %w", err)
	}
	if err := ensureNoIntegrityMaintenance(config); err != nil {
		return err
	}
	writerLock, err := fileguard.AcquireProcessLock(filepath.Join(config.Paths.StateDir, "ledger-writer.lock"), 0o700)
	if err != nil {
		return fmt.Errorf("acquire exclusive ledger writer: %w", err)
	}
	defer func() { err = errors.Join(err, writerLock.Close()) }()
	l, err := ledger.OpenCurrent(config.Paths.Database)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, l.Close())
	}()
	anchorState, err := l.IntegrityAnchorState(ctx)
	if err != nil {
		return err
	}
	anchorStore, err := configuredLedgerAnchor(ctx, config, source, anchorState)
	if err != nil {
		return err
	}
	if err := l.AttachIntegrityAnchor(ctx, anchorStore); err != nil {
		return err
	}
	publicURL := config.A2A.PublicURL
	actorFile := runtimeCredentialFile(config.A2A.ActorsFile, "a2a-actors.json")
	tlsCertFile := runtimeCredentialFile(config.A2A.TLSCertFile, "a2a-tls-cert")
	tlsKeyFile := runtimeCredentialFile(config.A2A.TLSKeyFile, "a2a-tls-key")
	listenAddress, remote, err := configuredA2AAddress(config.A2A)
	if err != nil {
		return err
	}
	tlsConfig, err := configuredTLSValues(remote, tlsCertFile, tlsKeyFile)
	if err != nil {
		return err
	}
	externalActors, err := configuredExternalActors(ctx, actorFile, source)
	if err != nil {
		return err
	}
	reconcilers, err := configuredEffectReconcilers(ctx, os.Getenv("AGENTOS_EFFECT_RECONCILERS_FILE"), source)
	if err != nil {
		return err
	}
	if err := validatePublicURL(publicURL, remote, externalActors != nil, tlsConfig != nil); err != nil {
		return err
	}
	recoveredInference, err := prepareInferenceAdmissions(ctx, l, config.Providers[0].InferencePolicy)
	if err != nil {
		return err
	}
	if recoveredInference > 0 {
		log.Printf("inference reservations require conservative reconciliation: count=%d", recoveredInference)
	}
	rawModel, closeModel, err := configuredProvider(ctx, config.Providers[0], providerRuntimeDirectory(config), source)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, closeModel())
	}()
	model, err := inference.NewGuardedAdapter(l, rawModel)
	if err != nil {
		return err
	}
	planner, err := planning.NewModelPlanner(planningModel{adapter: model})
	if err != nil {
		return err
	}
	service := app.NewWithModelAndPlanner(events.NewGateway(l), model, planner)
	if _, err := service.Recover(ctx); err != nil {
		return fmt.Errorf("recover durable runtime before serving: %w", err)
	}
	effectRecovery, err := effects.NewReconciliationService(l).Recover(ctx, reconcilers)
	if err != nil {
		return fmt.Errorf("recover effect obligations before serving: %w", err)
	}
	for _, item := range effectRecovery {
		log.Printf("effect requires reconciliation: effect_id=%s task_id=%s reason=%s", item.EffectID, item.TaskID, item.Reason)
	}
	normalizer, err := intake.NewModelNormalizer(intakeModel{adapter: model})
	if err != nil {
		return err
	}
	operator := intake.NewWithNormalizer(service, normalizer)
	owner := gateway.LocalHuman{
		UID: config.Owner.UID, ID: core.ID("local-uid-" + strconv.Itoa(config.Owner.UID)),
		OrganizationID: core.ID(config.Organization),
	}
	userGateway, err := gateway.NewHuman(operator, owner, artifacts.Store{Root: filepath.Join(config.Paths.DataDir, "artifacts")})
	if err != nil {
		return err
	}
	approvalService := approvals.New(l, nil, approvals.OwnerAuthorizer{OrganizationID: owner.OrganizationID, HumanID: owner.ID})
	approvalControl, err := gateway.NewApprovalControl(approvalService, owner)
	if err != nil {
		return err
	}
	userMux := http.NewServeMux()
	userMux.Handle("/v1/user/", userGateway)
	userMux.Handle("/v1/control/approvals", approvalControl)
	userMux.Handle("/v1/control/approvals/", approvalControl)
	userServer := newHTTPServer("", userMux, nil)
	userServer.ConnContext = localConnContext
	userListener, err := listenLocalHuman(ctx, config.Paths.UserSocket, config.Owner.UID, config.Owner.GID)
	if err != nil {
		return err
	}
	log.Printf("Agent OS local user gateway ready at %s", config.Paths.UserSocket)
	bindings := []serverBinding{{server: userServer, listener: userListener}}
	if externalActors != nil {
		a2aMux := http.NewServeMux()
		a2aMux.Handle("/", gateway.NewA2A(operator, externalActors, publicURL, version))
		a2aServer := newHTTPServer(listenAddress, a2aMux, tlsConfig)
		a2aListener, listenErr := (&net.ListenConfig{}).Listen(ctx, "tcp", a2aServer.Addr)
		if listenErr != nil {
			_ = userListener.Close()
			return fmt.Errorf("listen on A2A endpoint %s: %w", a2aServer.Addr, listenErr)
		}
		log.Printf("Agent OS A2A gateway listening on %s", a2aListener.Addr())
		bindings = append(bindings, serverBinding{server: a2aServer, listener: a2aListener, certFile: tlsCertFile, keyFile: tlsKeyFile})
	}
	return serveAll(ctx, bindings)
}

func configuredLedgerAnchor(ctx context.Context, config bootstrap.Config, source secrets.Source, state ledgeranchor.LedgerState) (*ledgeranchor.Store, error) {
	if ctx == nil || source == nil {
		return nil, fmt.Errorf("ledger anchor credential source is required")
	}
	value, err := source.Resolve(ctx, secrets.Ref(config.Integrity.SecretRef))
	if err != nil {
		return nil, fmt.Errorf("resolve ledger anchor signing key: %w", err)
	}
	var credential ledgerAnchorCredential
	decoder := json.NewDecoder(bytes.NewReader([]byte(value)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&credential); err != nil {
		return nil, fmt.Errorf("decode ledger anchor signing credential: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("ledger anchor signing credential has trailing content")
	}
	privateKey, err := base64.StdEncoding.DecodeString(credential.PrivateKey)
	if err != nil || len(privateKey) != ed25519.PrivateKeySize || credential.Version != 1 || credential.InstallationID != config.Integrity.InstallationID {
		clear(privateKey)
		return nil, fmt.Errorf("ledger anchor signing credential is invalid")
	}
	defer clear(privateKey)
	publicKey, err := ledgeranchor.DecodePublicKey(config.Integrity.PublicKey)
	if err != nil {
		return nil, err
	}
	store, err := ledgeranchor.Open(config.Integrity.CheckpointFile, config.Integrity.InstallationID, publicKey, ed25519.PrivateKey(privateKey), state, time.Now)
	if err != nil {
		return nil, fmt.Errorf("open external ledger anchor: %w", err)
	}
	return store, nil
}

type inferenceAdmissionStore interface {
	ValidateInferenceAdmissions(context.Context) error
	RecoverInferenceReservations(context.Context, string) (int, error)
	ActivateInferencePolicy(context.Context, inference.Policy) error
}

func prepareInferenceAdmissions(ctx context.Context, store inferenceAdmissionStore, policy inference.Policy) (int, error) {
	if store == nil {
		return 0, fmt.Errorf("inference admission store is required")
	}
	if err := store.ValidateInferenceAdmissions(ctx); err != nil {
		return 0, fmt.Errorf("validate durable inference accounting before startup: %w", err)
	}
	recovered, err := store.RecoverInferenceReservations(ctx, policy.OrganizationID)
	if err != nil {
		return 0, fmt.Errorf("recover incomplete inference reservations: %w", err)
	}
	if err := store.ActivateInferencePolicy(ctx, policy); err != nil {
		return 0, fmt.Errorf("activate reviewed inference policy: %w", err)
	}
	if err := store.ValidateInferenceAdmissions(ctx); err != nil {
		return 0, fmt.Errorf("validate durable inference accounting after startup recovery: %w", err)
	}
	return recovered, nil
}

type intakeModel struct{ adapter execution.ModelAdapter }

type planningModel struct{ adapter execution.ModelAdapter }

func (m planningModel) Descriptor() planning.Descriptor {
	descriptor := m.adapter.Descriptor()
	return planning.Descriptor{
		Provider: descriptor.Provider, Model: descriptor.Model,
		ExecutionProfileVersion: descriptor.ExecutionProfileVersion,
	}
}

func (m planningModel) CompleteText(ctx context.Context, prompt string) (planning.TextCompletion, error) {
	response, err := m.adapter.Complete(ctx, prompt)
	if err != nil {
		return planning.TextCompletion{}, err
	}
	return planning.TextCompletion{Text: response.Text, Usage: response.Usage}, nil
}

func (m intakeModel) Descriptor() intake.NormalizerDescriptor {
	descriptor := m.adapter.Descriptor()
	return intake.NormalizerDescriptor{
		Provider: descriptor.Provider, Model: descriptor.Model,
		ExecutionProfileVersion: descriptor.ExecutionProfileVersion,
	}
}

func (m intakeModel) CompleteText(ctx context.Context, prompt string) (intake.TextCompletion, error) {
	response, err := m.adapter.Complete(ctx, prompt)
	if err != nil {
		return intake.TextCompletion{}, err
	}
	return intake.TextCompletion{Text: response.Text, Usage: response.Usage}, nil
}

func configuredProvider(ctx context.Context, provider bootstrap.Provider, runtimeDir string, source secrets.Source) (execution.ModelAdapter, func() error, error) {
	if err := provider.Validate(); err != nil {
		return nil, nil, err
	}
	switch provider.Kind {
	case bootstrap.ProviderCodexSubscription:
		if source == nil || !filepath.IsAbs(runtimeDir) {
			return nil, nil, fmt.Errorf("codex sealed credential source and runtime directory are required")
		}
		encodedKey, err := source.Resolve(ctx, secrets.Ref(provider.SecretRef))
		if err != nil {
			return nil, nil, fmt.Errorf("resolve Codex credential key: %w", err)
		}
		key, err := base64.StdEncoding.DecodeString(string(encodedKey))
		if err != nil || len(key) != 32 {
			clear(key)
			return nil, nil, fmt.Errorf("codex credential key is invalid")
		}
		credential, err := secrets.OpenSealedFile(provider.CodexCredential, "codex-auth-v1", key)
		if err != nil {
			clear(key)
			return nil, nil, fmt.Errorf("open Codex credential store: %w", err)
		}
		defer clear(credential)
		credentialDirectory := filepath.Join(runtimeDir, "providers")
		if err := os.MkdirAll(credentialDirectory, 0o700); err != nil {
			clear(key)
			return nil, nil, err
		}
		if info, err := os.Lstat(credentialDirectory); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			clear(key)
			return nil, nil, fmt.Errorf("codex runtime credential directory is invalid")
		}
		temporary, err := os.CreateTemp(credentialDirectory, ".codex-auth-*")
		if err != nil {
			clear(key)
			return nil, nil, err
		}
		credentialPath := temporary.Name()
		removeCredential := func() { _ = os.Remove(credentialPath) }
		if err := temporary.Chmod(0o600); err != nil {
			_ = temporary.Close()
			removeCredential()
			clear(key)
			return nil, nil, err
		}
		if _, err := temporary.Write(credential); err != nil {
			_ = temporary.Close()
			removeCredential()
			clear(key)
			return nil, nil, err
		}
		if err := temporary.Close(); err != nil {
			removeCredential()
			clear(key)
			return nil, nil, err
		}
		adapter, err := execution.NewCodexSubscription(ctx, execution.CodexSubscriptionConfig{
			BinaryPath: provider.CodexBinary, CredentialsPath: credentialPath, Model: provider.Model,
			PersistCredentials: func(encoded []byte) error {
				return secrets.SealFile(provider.CodexCredential, "codex-auth-v1", key, encoded)
			},
		})
		if err != nil {
			removeCredential()
			clear(key)
			return nil, nil, fmt.Errorf("configure Codex subscription provider: %w", err)
		}
		return adapter, func() error {
			closeErr := adapter.Close()
			removeCredential()
			clear(key)
			return closeErr
		}, nil
	case bootstrap.ProviderOpenAIAPI:
		if source == nil {
			return nil, nil, fmt.Errorf("OpenAI API secret source is required")
		}
		adapter, err := execution.NewOpenAIAPI(ctx, execution.OpenAIAPIConfig{Model: provider.Model, APIKey: func(resolveCtx context.Context) (string, error) {
			value, resolveErr := source.Resolve(resolveCtx, secrets.Ref(provider.SecretRef))
			return string(value), resolveErr
		}})
		if err != nil {
			return nil, nil, fmt.Errorf("configure OpenAI API provider: %w", err)
		}
		return adapter, func() error { return nil }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported model provider")
	}
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, certFile, keyFile string) error {
	return serveAll(ctx, []serverBinding{{server: server, listener: listener, certFile: certFile, keyFile: keyFile}})
}

type serverBinding struct {
	server            *http.Server
	listener          net.Listener
	certFile, keyFile string
}

func serveAll(ctx context.Context, bindings []serverBinding) error {
	if ctx == nil || len(bindings) == 0 {
		closeListeners(bindings)
		return fmt.Errorf("runtime context, server, and listener are required")
	}
	if err := ctx.Err(); err != nil {
		closeListeners(bindings)
		return err
	}
	for _, binding := range bindings {
		if binding.server == nil || binding.listener == nil {
			closeListeners(bindings)
			return fmt.Errorf("runtime context, server, and listener are required")
		}
	}
	results := make(chan error, len(bindings))
	for _, binding := range bindings {
		binding := binding
		go func() {
			if binding.server.TLSConfig != nil {
				results <- binding.server.ServeTLS(binding.listener, binding.certFile, binding.keyFile)
				return
			}
			results <- binding.server.Serve(binding.listener)
		}()
	}
	var result error
	completed := 0
	select {
	case serveErr := <-results:
		completed = 1
		if !errors.Is(serveErr, http.ErrServerClosed) {
			result = serveErr
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, binding := range bindings {
		shutdownErr := binding.server.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, binding.server.Close())
		}
		result = errors.Join(result, shutdownErr)
	}
	for completed < len(bindings) {
		serveErr := <-results
		if !errors.Is(serveErr, http.ErrServerClosed) {
			result = errors.Join(result, serveErr)
		}
		completed++
	}
	return result
}

func closeListeners(bindings []serverBinding) {
	for _, binding := range bindings {
		if binding.listener != nil {
			_ = binding.listener.Close()
		}
	}
}

func newHTTPServer(address string, handler http.Handler, tlsConfig *tls.Config) *http.Server {
	return &http.Server{Addr: address, Handler: handler, TLSConfig: tlsConfig, MaxHeaderBytes: 32 << 10, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute}
}

func configuredEffectReconcilers(ctx context.Context, path string, source secrets.Source) (*effectstatus.HTTPReconcilerRegistry, error) {
	return configuredRegistry(ctx, path, source, "effect reconciler registry", "effect reconciler", effectstatus.DecodeHTTPReconcilerConfig,
		func(binding effectstatus.HTTPReconcilerBinding) (string, string) {
			return fmt.Sprintf("%s/%s/%s", binding.OrganizationID, binding.Action, binding.Resource), binding.TokenRef
		},
		func(binding *effectstatus.HTTPReconcilerBinding, token string) { binding.BearerToken = token },
		func(bindings []effectstatus.HTTPReconcilerBinding) (*effectstatus.HTTPReconcilerRegistry, error) {
			return effectstatus.NewHTTPReconcilerRegistry(bindings, nil)
		})
}

func configuredExternalActors(ctx context.Context, path string, source secrets.Source) (*gateway.ExternalActorRegistry, error) {
	return configuredRegistry(ctx, path, source, "external actor registry", "external actor", gateway.DecodeExternalActorConfig,
		func(actor gateway.ExternalActor) (string, string) { return actor.ID, actor.TokenRef },
		func(actor *gateway.ExternalActor, token string) { actor.BearerToken = token }, gateway.NewExternalActorRegistry)
}

func configuredRegistry[T, R any](ctx context.Context, path string, source secrets.Source, registryName, entryName string, decode func(io.Reader) ([]T, error), identity func(T) (string, string), set func(*T, string), validate func([]T) (R, error)) (R, error) {
	var zero R
	if path == "" {
		return zero, nil
	}
	entries, err := decodeConfigFile(path, registryName, decode)
	if err != nil {
		return zero, err
	}
	if err := resolveRegistryCredentials(ctx, source, entries, identity, set); err != nil {
		return zero, fmt.Errorf("resolve %s credential: %w", entryName, err)
	}
	registry, err := validate(entries)
	if err != nil {
		return zero, fmt.Errorf("validate %s: %w", registryName, err)
	}
	return registry, nil
}

func resolveRegistryCredentials[T any](ctx context.Context, source secrets.Source, entries []T, identity func(T) (string, string), set func(*T, string)) error {
	for index := range entries {
		name, tokenRef := identity(entries[index])
		if tokenRef == "" {
			return fmt.Errorf("%s token_ref is required", name)
		}
		value, err := source.Resolve(ctx, secrets.Ref(tokenRef))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		set(&entries[index], string(value))
	}
	return nil
}

func decodeConfigFile[T any](path, name string, decode func(io.Reader) ([]T, error)) ([]T, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 || (runtime.GOOS == "linux" && info.Mode().Perm()&0o022 != 0) {
		return nil, fmt.Errorf("%s must be a bounded, non-writable regular file", name)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	defer func() {
		_ = file.Close()
	}()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("%s changed while it was opened", name)
	}
	return decode(file)
}

func runtimeCredentialFile(configuredPath, name string) string {
	if configuredPath == "" {
		return ""
	}
	directory := os.Getenv("CREDENTIALS_DIRECTORY")
	if !filepath.IsAbs(directory) {
		return configuredPath
	}
	// When a service manager declares a credential directory, its runtime copy
	// is authoritative. Returning the configured source on a missing or invalid
	// copy would silently weaken the service boundary instead of failing closed.
	return filepath.Join(directory, name)
}

func providerRuntimeDirectory(config bootstrap.Config) string {
	if config.Mode == bootstrap.ModeSystem {
		return systemProviderRuntimeDirectory
	}
	return config.Paths.RuntimeDir
}

func configuredA2AAddress(config bootstrap.A2A) (string, bool, error) {
	address := config.ListenAddress
	if address == "" {
		address = "127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return "", false, fmt.Errorf("A2A listen address must be a host:port address")
	}
	parsedIP := net.ParseIP(host)
	loopback := strings.EqualFold(host, "localhost") || (parsedIP != nil && parsedIP.IsLoopback())
	remote := host == "" || !loopback
	if remote && !config.AllowRemote {
		return "", false, fmt.Errorf("remote A2A listening must be enabled explicitly")
	}
	return address, remote, nil
}

func configuredTLSValues(remote bool, certFile, keyFile string) (*tls.Config, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("A2A TLS certificate and key must be configured together")
	}
	if certFile == "" {
		if remote {
			return nil, fmt.Errorf("remote A2A listening requires TLS certificate and key files")
		}
		return nil, nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS13}, nil
}

func validatePublicURL(publicURL string, remote, a2aEnabled, tlsEnabled bool) error {
	if publicURL == "" {
		if remote && a2aEnabled {
			return fmt.Errorf("remote A2A exposure requires a2a.public_url")
		}
		return nil
	}
	parsed, err := url.Parse(publicURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("a2a.public_url must be an absolute HTTP(S) origin")
	}
	if remote && parsed.Scheme != "https" {
		return fmt.Errorf("remote exposure requires an HTTPS a2a.public_url")
	}
	if tlsEnabled && parsed.Scheme != "https" {
		return fmt.Errorf("TLS listeners require an HTTPS a2a.public_url")
	}
	return nil
}
