package main

import (
	"log"
	"net/http"
	"os"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/gateway"
	"github.com/dominicnunez/agentos/internal/ledger"
)

func main() {
	path := os.Getenv("AGENTOS_DB")
	if path == "" {
		path = "agentos.db"
	}
	l, err := ledger.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	token := os.Getenv("AGENTOS_OPERATOR_TOKEN")
	if token == "" {
		log.Fatal("AGENTOS_OPERATOR_TOKEN is required (fail closed)")
	}
	orgID := os.Getenv("AGENTOS_ORGANIZATION_ID")
	if orgID == "" {
		orgID = "org-default"
	}
	h := gateway.NewA2A(app.New(events.NewGateway(l)), gateway.ExternalActor{ID: "hermes-primary", OrganizationID: orgID, BearerToken: token})
	s := &http.Server{Addr: ":8080", Handler: h, ReadHeaderTimeout: 5e9}
	log.Printf("Agent OS listening on %s", s.Addr)
	log.Fatal(s.ListenAndServe())
}
