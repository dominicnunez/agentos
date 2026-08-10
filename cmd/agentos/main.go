package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/approvals"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/effectstatus"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/secrets"
)

var version = "1.0.0-dev"

func main() {
	handled, err := printVersion(os.Args[1:], os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	if handled {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = run(ctx); err != nil {
		log.Fatal(err)
	}
}

func printVersion(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	if len(args) != 1 || (args[0] != "--version" && args[0] != "version") {
		return false, fmt.Errorf("unsupported argument; use --version or configure the runtime through AGENTOS_* environment variables")
	}
	if output == nil {
		return false, fmt.Errorf("version output is required")
	}
	_, err := fmt.Fprintln(output, version)
	return true, err
}

func run(ctx context.Context) (err error) {
	if ctx == nil {
		return fmt.Errorf("runtime context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := os.Getenv("AGENTOS_DB")
	if path == "" {
		path = "agentos.db"
	}
	l, err := ledger.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, l.Close())
	}()
	publicURL := os.Getenv("AGENTOS_PUBLIC_URL")
	listenAddress, remote, err := configuredListenAddress()
	if err != nil {
		return err
	}
	if err := validateModelExposure(os.Getenv("AGENTOS_MODEL_PROVIDER"), remote); err != nil {
		return err
	}
	tlsConfig, err := configuredTLS(remote)
	if err != nil {
		return err
	}
	externalActors, err := configuredExternalActors(ctx, os.Getenv("AGENTOS_A2A_ACTORS_FILE"), secrets.Environment{})
	if err != nil {
		return err
	}
	humanActors, err := configuredHumanActors(ctx, os.Getenv("AGENTOS_HUMAN_ACTORS_FILE"), secrets.Environment{})
	if err != nil {
		return err
	}
	approvalActors, err := configuredApprovalActors(ctx, os.Getenv("AGENTOS_APPROVAL_ACTORS_FILE"), secrets.Environment{})
	if err != nil {
		return err
	}
	if approvalActors == nil && approvalControlEnvironmentConfigured() {
		return fmt.Errorf("approval control configuration requires AGENTOS_APPROVAL_ACTORS_FILE")
	}
	if externalActors == nil && humanActors == nil {
		return fmt.Errorf("at least one reviewed operator registry is required")
	}
	reconcilers, err := configuredEffectReconcilers(ctx, os.Getenv("AGENTOS_EFFECT_RECONCILERS_FILE"), secrets.Environment{})
	if err != nil {
		return err
	}
	if gateway.OperatorRegistriesOverlap(humanActors, externalActors, approvalActors) {
		return fmt.Errorf("human, external-agent, and approval-control identities and credentials must be distinct")
	}
	if err := validatePublicURL(publicURL, remote, externalActors != nil, tlsConfig != nil); err != nil {
		return err
	}
	model, closeModel, err := configuredModel(ctx, secrets.Environment{})
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, closeModel())
	}()
	service := app.NewWithModel(events.NewGateway(l), model)
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
	operator := intake.New(service)
	mux := http.NewServeMux()
	if humanActors != nil {
		mux.Handle("/v1/human/", gateway.NewHuman(operator, humanActors))
	}
	if externalActors != nil {
		mux.Handle("/", gateway.NewA2A(operator, externalActors, publicURL, version))
	}
	s := newHTTPServer(listenAddress, mux, tlsConfig)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.Addr, err)
	}
	log.Printf("Agent OS listening on %s", listener.Addr())
	bindings := []serverBinding{{server: s, listener: listener, certFile: os.Getenv("AGENTOS_TLS_CERT_FILE"), keyFile: os.Getenv("AGENTOS_TLS_KEY_FILE")}}
	if approvalActors != nil {
		controlAddress, controlRemote, configErr := configuredControlListenAddress()
		if configErr != nil {
			_ = listener.Close()
			return configErr
		}
		controlTLS, configErr := configuredControlTLS(controlRemote)
		if configErr != nil {
			_ = listener.Close()
			return configErr
		}
		approvalService := approvals.New(l, nil, approvalActors)
		controlMux := http.NewServeMux()
		controlMux.Handle("/v1/control/approvals/", gateway.NewApprovalControl(approvalService, approvalActors))
		controlServer := newHTTPServer(controlAddress, controlMux, controlTLS)
		controlListener, listenErr := (&net.ListenConfig{}).Listen(ctx, "tcp", controlServer.Addr)
		if listenErr != nil {
			_ = listener.Close()
			return fmt.Errorf("listen on approval control %s: %w", controlServer.Addr, listenErr)
		}
		log.Printf("Agent OS approval control listening on %s", controlListener.Addr())
		bindings = append(bindings, serverBinding{
			server: controlServer, listener: controlListener,
			certFile: os.Getenv("AGENTOS_CONTROL_TLS_CERT_FILE"), keyFile: os.Getenv("AGENTOS_CONTROL_TLS_KEY_FILE"),
		})
	}
	return serveAll(ctx, bindings)
}

func configuredModel(ctx context.Context, source secrets.Source) (execution.ModelAdapter, func() error, error) {
	provider := os.Getenv("AGENTOS_MODEL_PROVIDER")
	switch provider {
	case "", "fake":
		return execution.FakeModel{}, func() error { return nil }, nil
	case "fake-review":
		return execution.ReviewFakeModel{}, func() error { return nil }, nil
	case "codex-subscription":
		adapter, err := execution.NewCodexSubscription(ctx, execution.CodexSubscriptionConfig{
			BinaryPath:      os.Getenv("AGENTOS_CODEX_BINARY"),
			CredentialsPath: os.Getenv("AGENTOS_CODEX_CREDENTIALS_FILE"),
			Model:           os.Getenv("AGENTOS_CODEX_MODEL"),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("configure Codex subscription provider: %w", err)
		}
		return adapter, adapter.Close, nil
	case "openai-api":
		keyRef := os.Getenv("AGENTOS_OPENAI_API_KEY_REF")
		if len(keyRef) == 0 || len(keyRef) > 128 || strings.TrimSpace(keyRef) != keyRef || strings.IndexFunc(keyRef, func(character rune) bool { return character < 0x21 || character > 0x7e }) >= 0 {
			return nil, nil, fmt.Errorf("AGENTOS_OPENAI_API_KEY_REF must name one canonical server-owned secret")
		}
		if source == nil {
			return nil, nil, fmt.Errorf("OpenAI API secret source is required")
		}
		adapter, err := execution.NewOpenAIAPI(ctx, execution.OpenAIAPIConfig{
			Model: os.Getenv("AGENTOS_OPENAI_MODEL"),
			APIKey: func(resolveCtx context.Context) (string, error) {
				value, resolveErr := source.Resolve(resolveCtx, secrets.Ref(keyRef))
				return string(value), resolveErr
			},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("configure OpenAI API provider: %w", err)
		}
		return adapter, func() error { return nil }, nil
	default:
		return nil, nil, fmt.Errorf("AGENTOS_MODEL_PROVIDER must be fake, fake-review, codex-subscription, or openai-api")
	}
}

func validateModelExposure(provider string, remote bool) error {
	if provider == "fake-review" && remote {
		return fmt.Errorf("fake-review model provider is restricted to loopback release testing")
	}
	return nil
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

func configuredHumanActors(ctx context.Context, path string, source secrets.Source) (*gateway.HumanActorRegistry, error) {
	return configuredRegistry(ctx, path, source, "human actor registry", "human actor", gateway.DecodeHumanActorConfig,
		func(actor gateway.HumanActor) (string, string) { return actor.ID, actor.TokenRef },
		func(actor *gateway.HumanActor, token string) { actor.BearerToken = token }, gateway.NewHumanActorRegistry)
}

func configuredApprovalActors(ctx context.Context, path string, source secrets.Source) (*gateway.ApprovalActorRegistry, error) {
	return configuredRegistry(ctx, path, source, "approval actor registry", "approval actor", gateway.DecodeApprovalActorConfig,
		func(actor gateway.ApprovalActor) (string, string) { return actor.ID, actor.TokenRef },
		func(actor *gateway.ApprovalActor, token string) { actor.BearerToken = token }, gateway.NewApprovalActorRegistry)
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
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	defer func() {
		_ = file.Close()
	}()
	return decode(file)
}

func configuredListenAddress() (string, bool, error) {
	return configuredAddress("AGENTOS_LISTEN_ADDR", "AGENTOS_ALLOW_REMOTE", "127.0.0.1:8080", "")
}

func configuredControlListenAddress() (string, bool, error) {
	return configuredAddress("AGENTOS_CONTROL_LISTEN_ADDR", "AGENTOS_CONTROL_ALLOW_REMOTE", "127.0.0.1:8082", "approval control ")
}

func configuredAddress(addressVariable, remoteVariable, defaultAddress, label string) (string, bool, error) {
	address := os.Getenv(addressVariable)
	if address == "" {
		address = defaultAddress
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return "", false, fmt.Errorf("%s must be a host:port address", addressVariable)
	}
	parsedIP := net.ParseIP(host)
	loopback := strings.EqualFold(host, "localhost") || (parsedIP != nil && parsedIP.IsLoopback())
	remote := host == "" || !loopback
	allowRemote := false
	switch configured := os.Getenv(remoteVariable); configured {
	case "", "false":
	case "true":
		allowRemote = true
	default:
		return "", false, fmt.Errorf("%s must be true or false", remoteVariable)
	}
	if remote && !allowRemote {
		return "", false, fmt.Errorf("%sremote listening is disabled; set %s=true deliberately", label, remoteVariable)
	}
	return address, remote, nil
}

func configuredTLS(remote bool) (*tls.Config, error) {
	return configuredTLSFiles(remote, "AGENTOS_TLS_CERT_FILE", "AGENTOS_TLS_KEY_FILE", "")
}

func configuredControlTLS(remote bool) (*tls.Config, error) {
	return configuredTLSFiles(remote, "AGENTOS_CONTROL_TLS_CERT_FILE", "AGENTOS_CONTROL_TLS_KEY_FILE", "approval control ")
}

func configuredTLSFiles(remote bool, certVariable, keyVariable, label string) (*tls.Config, error) {
	certFile := os.Getenv(certVariable)
	keyFile := os.Getenv(keyVariable)
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("%s and %s must be configured together", certVariable, keyVariable)
	}
	if certFile == "" {
		if remote {
			return nil, fmt.Errorf("%sremote listening requires TLS certificate and key files", label)
		}
		return nil, nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS13}, nil
}

func approvalControlEnvironmentConfigured() bool {
	for _, variable := range []string{
		"AGENTOS_CONTROL_LISTEN_ADDR", "AGENTOS_CONTROL_ALLOW_REMOTE",
		"AGENTOS_CONTROL_TLS_CERT_FILE", "AGENTOS_CONTROL_TLS_KEY_FILE",
	} {
		if os.Getenv(variable) != "" {
			return true
		}
	}
	return false
}

func validatePublicURL(publicURL string, remote, a2aEnabled, tlsEnabled bool) error {
	if publicURL == "" {
		if remote && a2aEnabled {
			return fmt.Errorf("remote A2A exposure requires AGENTOS_PUBLIC_URL")
		}
		return nil
	}
	parsed, err := url.Parse(publicURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("AGENTOS_PUBLIC_URL must be an absolute HTTP(S) origin")
	}
	if remote && parsed.Scheme != "https" {
		return fmt.Errorf("remote exposure requires an HTTPS AGENTOS_PUBLIC_URL")
	}
	if tlsEnabled && parsed.Scheme != "https" {
		return fmt.Errorf("TLS listeners require an HTTPS AGENTOS_PUBLIC_URL")
	}
	return nil
}
