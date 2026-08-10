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
	if externalActors == nil && humanActors == nil {
		return fmt.Errorf("at least one reviewed operator registry is required")
	}
	reconcilers, err := configuredEffectReconcilers(ctx, os.Getenv("AGENTOS_EFFECT_RECONCILERS_FILE"), secrets.Environment{})
	if err != nil {
		return err
	}
	if gateway.OperatorRegistriesOverlap(humanActors, externalActors) {
		return fmt.Errorf("human and external-agent credentials must be distinct")
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
		mux.Handle("/", gateway.NewA2A(operator, externalActors, publicURL))
	}
	s := &http.Server{Addr: listenAddress, Handler: mux, TLSConfig: tlsConfig, MaxHeaderBytes: 32 << 10, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.Addr, err)
	}
	log.Printf("Agent OS listening on %s", listener.Addr())
	return serve(ctx, s, listener, os.Getenv("AGENTOS_TLS_CERT_FILE"), os.Getenv("AGENTOS_TLS_KEY_FILE"))
}

func configuredModel(ctx context.Context, source secrets.Source) (execution.ModelAdapter, func() error, error) {
	provider := os.Getenv("AGENTOS_MODEL_PROVIDER")
	switch provider {
	case "", "fake":
		return execution.FakeModel{}, func() error { return nil }, nil
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
		return nil, nil, fmt.Errorf("AGENTOS_MODEL_PROVIDER must be fake, codex-subscription, or openai-api")
	}
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, certFile, keyFile string) error {
	if listener == nil {
		return fmt.Errorf("runtime context, server, and listener are required")
	}
	if ctx == nil || server == nil {
		_ = listener.Close()
		return fmt.Errorf("runtime context, server, and listener are required")
	}
	if err := ctx.Err(); err != nil {
		_ = listener.Close()
		return err
	}
	result := make(chan error, 1)
	go func() {
		if server.TLSConfig != nil {
			result <- server.ServeTLS(listener, certFile, keyFile)
			return
		}
		result <- server.Serve(listener)
	}()

	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, server.Close())
		}
		serveErr := <-result
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

func configuredHumanActors(ctx context.Context, path string, source secrets.Source) (*gateway.HumanActorRegistry, error) {
	return configuredRegistry(ctx, path, source, "human actor registry", "human actor", gateway.DecodeHumanActorConfig,
		func(actor gateway.HumanActor) (string, string) { return actor.ID, actor.TokenRef },
		func(actor *gateway.HumanActor, token string) { actor.BearerToken = token }, gateway.NewHumanActorRegistry)
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
	address := os.Getenv("AGENTOS_LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:8080"
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return "", false, fmt.Errorf("AGENTOS_LISTEN_ADDR must be a host:port address")
	}
	parsedIP := net.ParseIP(host)
	loopback := strings.EqualFold(host, "localhost") || (parsedIP != nil && parsedIP.IsLoopback())
	remote := host == "" || !loopback
	allowRemote := false
	switch configured := os.Getenv("AGENTOS_ALLOW_REMOTE"); configured {
	case "", "false":
	case "true":
		allowRemote = true
	default:
		return "", false, fmt.Errorf("AGENTOS_ALLOW_REMOTE must be true or false")
	}
	if remote && !allowRemote {
		return "", false, fmt.Errorf("remote listening is disabled; set AGENTOS_ALLOW_REMOTE=true deliberately")
	}
	return address, remote, nil
}

func configuredTLS(remote bool) (*tls.Config, error) {
	certFile := os.Getenv("AGENTOS_TLS_CERT_FILE")
	keyFile := os.Getenv("AGENTOS_TLS_KEY_FILE")
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("AGENTOS_TLS_CERT_FILE and AGENTOS_TLS_KEY_FILE must be configured together")
	}
	if certFile == "" {
		if remote {
			return nil, fmt.Errorf("remote listening requires TLS certificate and key files")
		}
		return nil, nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS13}, nil
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
