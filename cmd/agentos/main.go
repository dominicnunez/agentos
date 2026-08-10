package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
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
	token := os.Getenv("AGENTOS_OPERATOR_TOKEN")
	if token == "" {
		return fmt.Errorf("AGENTOS_OPERATOR_TOKEN is required (fail closed)")
	}
	humanToken := os.Getenv("AGENTOS_HUMAN_TOKEN")
	if humanToken == "" {
		return fmt.Errorf("AGENTOS_HUMAN_TOKEN is required (fail closed)")
	}
	if humanToken == token {
		return fmt.Errorf("human and external-agent credentials must be distinct")
	}
	orgID := os.Getenv("AGENTOS_ORGANIZATION_ID")
	if orgID == "" {
		orgID = "org-default"
	}
	publicURL := os.Getenv("AGENTOS_PUBLIC_URL")
	if publicURL != "" {
		parsed, parseErr := url.Parse(publicURL)
		if parseErr != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("AGENTOS_PUBLIC_URL must be an absolute HTTP(S) origin")
		}
	}
	service := app.New(events.NewGateway(l))
	if _, err := service.Recover(context.Background()); err != nil {
		return fmt.Errorf("recover durable runtime before serving: %w", err)
	}
	operator := intake.New(service)
	capabilities := []string{intake.CapabilitySubmitWork, intake.CapabilityReadStatus, intake.CapabilityReadResult, intake.CapabilityProvideInput}
	a2a := gateway.NewA2A(operator, gateway.ExternalActor{ID: "external-agent-primary", OrganizationID: orgID, BearerToken: token, PublicURL: publicURL, Capabilities: capabilities})
	human := gateway.NewHuman(operator, gateway.HumanActor{ID: "human-primary", OrganizationID: orgID, BearerToken: humanToken, Capabilities: capabilities})
	mux := http.NewServeMux()
	mux.Handle("/v1/human/", human)
	mux.Handle("/", a2a)
	s := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5e9}
	log.Printf("Agent OS listening on %s", s.Addr)
	return s.ListenAndServe()
}
