package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/gateway"
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
	orgID := os.Getenv("AGENTOS_ORGANIZATION_ID")
	if orgID == "" {
		orgID = "org-default"
	}
	service := app.New(events.NewGateway(l))
	if _, err := service.Recover(context.Background()); err != nil {
		return fmt.Errorf("recover durable runtime before serving: %w", err)
	}
	h := gateway.NewA2A(service, gateway.ExternalActor{ID: "hermes-primary", OrganizationID: orgID, BearerToken: token, Capabilities: []string{"submit_work", "read_status", "provide_input"}})
	s := &http.Server{Addr: ":8080", Handler: h, ReadHeaderTimeout: 5e9}
	log.Printf("Agent OS listening on %s", s.Addr)
	return s.ListenAndServe()
}
