package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/effects"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/secrets"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
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
	humanToken := os.Getenv("AGENTOS_HUMAN_TOKEN")
	if len(humanToken) < 32 {
		return fmt.Errorf("AGENTOS_HUMAN_TOKEN must contain at least 32 characters (fail closed)")
	}
	orgID := os.Getenv("AGENTOS_ORGANIZATION_ID")
	if orgID == "" {
		orgID = "org-default"
	}
	publicURL := os.Getenv("AGENTOS_PUBLIC_URL")
	listenAddress, remote, err := configuredListenAddress()
	if err != nil {
		return err
	}
	registry, err := configuredExternalActors(context.Background(), os.Getenv("AGENTOS_A2A_ACTORS_FILE"), secrets.Environment{})
	if err != nil {
		return err
	}
	if registry != nil && registry.HasCredential(humanToken) {
		return fmt.Errorf("human and external-agent credentials must be distinct")
	}
	if err := validatePublicURL(publicURL, remote, registry != nil); err != nil {
		return err
	}
	service := app.New(events.NewGateway(l))
	if _, err := service.Recover(context.Background()); err != nil {
		return fmt.Errorf("recover durable runtime before serving: %w", err)
	}
	effectRecovery, err := effects.NewReconciliationService(l).Recover(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("recover effect obligations before serving: %w", err)
	}
	for _, item := range effectRecovery {
		log.Printf("effect requires reconciliation: effect_id=%s task_id=%s reason=%s", item.EffectID, item.TaskID, item.Reason)
	}
	operator := intake.New(service)
	capabilities := []string{intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityReadResult, intake.CapabilityProvideInput}
	human := gateway.NewHuman(operator, gateway.HumanActor{ID: "human-primary", OrganizationID: orgID, BearerToken: humanToken, Capabilities: capabilities})
	mux := http.NewServeMux()
	mux.Handle("/v1/human/", human)
	if registry != nil {
		mux.Handle("/", gateway.NewA2A(operator, registry, publicURL))
	}
	s := &http.Server{Addr: listenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute}
	log.Printf("Agent OS listening on %s", s.Addr)
	return s.ListenAndServe()
}

func configuredExternalActors(ctx context.Context, path string, source secrets.Source) (*gateway.ExternalActorRegistry, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open external actor registry: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()
	actors, err := gateway.DecodeExternalActorConfig(file)
	if err != nil {
		return nil, err
	}
	for i := range actors {
		if actors[i].TokenRef == "" {
			return nil, fmt.Errorf("external actor %q token_ref is required", actors[i].ID)
		}
		value, err := source.Resolve(ctx, secrets.Ref(actors[i].TokenRef))
		if err != nil {
			return nil, fmt.Errorf("resolve external actor %q credential: %w", actors[i].ID, err)
		}
		actors[i].BearerToken = string(value)
	}
	registry, err := gateway.NewExternalActorRegistry(actors)
	if err != nil {
		return nil, fmt.Errorf("validate external actor registry: %w", err)
	}
	return registry, nil
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

func validatePublicURL(publicURL string, remote, a2aEnabled bool) error {
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
	return nil
}
