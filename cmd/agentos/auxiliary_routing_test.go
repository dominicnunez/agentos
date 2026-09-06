package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/app"
	"github.com/dominicnunez/agentos/internal/core"
	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/intake"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/modelinput"
	"github.com/dominicnunez/agentos/internal/planning"
)

type failingAuxiliaryModel struct {
	execution.FakeModel
	calls atomic.Int32
}

type mismatchedPlanner struct {
	planning.Planner
	descriptor planning.Descriptor
}

func (p mismatchedPlanner) Descriptor() (planning.Descriptor, bool) { return p.descriptor, true }

type mismatchedPlannerSelector struct {
	routedPlanner
	mutate        func(*planning.Descriptor)
	mutateBinding func(*modelinput.RouteBinding)
}

func (p mismatchedPlannerSelector) SelectPlanner(ctx context.Context, organization string) (planning.Planner, *modelinput.RouteBinding, error) {
	selected, binding, err := p.routedPlanner.SelectPlanner(ctx, organization)
	if err != nil {
		return nil, nil, err
	}
	descriptor, _ := selected.Descriptor()
	if p.mutate != nil {
		p.mutate(&descriptor)
	}
	if p.mutateBinding != nil {
		p.mutateBinding(binding)
	}
	return mismatchedPlanner{Planner: selected, descriptor: descriptor}, binding, nil
}

func (m *failingAuxiliaryModel) CompleteRequest(context.Context, modelinput.Request) (execution.ModelResponse, error) {
	m.calls.Add(1)
	return execution.ModelResponse{}, execution.RequestNotSent(errors.New("synthetic provider failure"))
}

func TestAuxiliaryBrokerBindsSelectedAccountAndRequirements(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	first, second := &failingAuxiliaryModel{}, &failingAuxiliaryModel{}
	var policies []inference.Policy
	var connections []inference.Connection
	for i, id := range []string{"first", "second"} {
		policy := inference.Policy{Version: inference.ConnectionPolicyVersion, ConnectionID: id,
			OrganizationID: "org-1", Provider: "fake", Model: "fake-model/v1", ExecutionProfileVersion: "v1-fake", Mode: inference.Local,
			MaxInputTokensPerRequest: 10000, MaxOutputTokensPerRequest: 1000, MaxTokensPerWindow: 100000, WindowDurationSeconds: 3600,
			MaxConcurrentRequests: 2, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now.Add(-time.Minute), AuthorizationExpiresAt: now.Add(time.Hour),
			OrganizationBudget: &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 100000, MaxConcurrentRequests: 4},
			Routing:            &inference.RoutePolicy{OrganizationID: "org-1", Locality: inference.LocalOnly, DataClasses: []string{"internal"}},
			Catalog:            &inference.CatalogDefinition{Capabilities: []inference.Capability{inference.Text}, Local: true, ContextTokens: 11000, OutputTokens: 1000, DataClasses: []string{"internal"}, ValidUntil: now.Add(time.Hour)},
		}
		metadata, err := policy.Catalog.Metadata(policy)
		if err != nil {
			t.Fatal(err)
		}
		policies = append(policies, policy)
		connections = append(connections, inference.Connection{ID: id, Adapter: []*failingAuxiliaryModel{first, second}[i], Metadata: &metadata})
	}
	if err := store.ActivateInferencePolicies(t.Context(), policies); err != nil {
		t.Fatal(err)
	}
	registry, err := inference.NewConnectionRegistry(store, connections)
	if err != nil {
		t.Fatal(err)
	}
	requirements := modelinput.RouteRequirements{OrganizationID: "org-1", Capabilities: []modelinput.Capability{modelinput.Text}, InputTokens: 10000, OutputTokens: 1000, Locality: modelinput.LocalOnly, DataClass: "internal", PreferredConnections: []string{"second"}}
	route, err := newAuxiliaryRoute(registry, "first", requirements)
	if err != nil {
		t.Fatal(err)
	}
	requirements.PreferredConnections[0] = "first"
	requirements.DataClass = "changed"
	if _, _, err := route.selectAdapter(t.Context(), "other-org"); err == nil {
		t.Fatal("cross-organization auxiliary selection accepted")
	}
	adapter, selectedRequirements, err := route.selectAdapter(t.Context(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	selectedRequirements.Requirements.DataClass = "changed"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := route.selectAdapter(ctx, "org-1"); !errors.Is(err, context.Canceled) {
		t.Fatal("auxiliary route ignored cancellation", err)
	}
	for _, mutate := range []func(*modelinput.RouteRequirements){
		func(r *modelinput.RouteRequirements) { r.DataClass = "secret" },
		func(r *modelinput.RouteRequirements) {
			r.Capabilities = []modelinput.Capability{modelinput.Text, modelinput.Vision}
		},
		func(r *modelinput.RouteRequirements) { r.DeniedProviders = []string{"fake"} },
	} {
		constraints := route.requirements.Clone()
		mutate(&constraints)
		denied, err := newAuxiliaryRoute(registry, "first", constraints)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := denied.selectAdapter(t.Context(), "org-1"); err == nil {
			t.Fatal("auxiliary selection weakened requirements to use default")
		}
	}
	if first.calls.Load() != 0 || second.calls.Load() != 0 {
		t.Fatal("selection contacted provider")
	}
	basePlanner, err := planning.NewModelPlanner(planningModel{adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	if composed, err := app.NewWithConnections(events.NewGateway(store), registry, app.TaskConnectionRouting{Default: "first", Requirements: &route.requirements}, basePlanner); err == nil || composed != nil {
		t.Fatal("governed planner without selector accepted by constructor")
	}
	unguardedComposition := app.NewWithModelAndPlanner(events.NewGateway(store), adapter, basePlanner)
	if _, err := unguardedComposition.Submit(t.Context(), app.Submit{RequestID: "missing-planner-selector", OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}); err == nil || !strings.Contains(err.Error(), "requires a route selector") {
		t.Fatalf("direct planner without selector was not rejected: %v", err)
	}
	unselectedStream, err := unguardedComposition.ExternalEvents(t.Context(), "org-1", "missing-planner-selector")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range unselectedStream {
		if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INFERENCE_RESERVED" {
			t.Fatal("unselected planner published model context")
		}
	}
	for name, mutate := range map[string]func(*modelinput.RouteBinding){
		"missing-cutoff":  func(b *modelinput.RouteBinding) { b.Decision.SnapshotSequence += 1000000 },
		"inactive-policy": func(b *modelinput.RouteBinding) { b.Decision.PolicyFingerprint = modelinput.TextDigest("inactive") },
	} {
		t.Run(name, func(t *testing.T) {
			selector := mismatchedPlannerSelector{routedPlanner: routedPlanner{Planner: basePlanner, route: route}, mutateBinding: mutate}
			service, err := app.NewWithConnections(events.NewGateway(store), registry, app.TaskConnectionRouting{Default: "first", Requirements: &route.requirements}, selector)
			if err != nil {
				t.Fatal(err)
			}
			submission := app.Submit{RequestID: name, OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}
			if _, err := service.Submit(t.Context(), submission); err == nil || !strings.Contains(err.Error(), "validate planning route provenance") {
				t.Fatalf("forged binding was not rejected before publication: %v", err)
			}
			stream, err := service.ExternalEvents(t.Context(), "org-1", name)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range stream {
				if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INFERENCE_RESERVED" {
					t.Fatal("invalid binding published", event.EventType)
				}
			}
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatal("rejected binding poisoned ledger", err)
			}
			if first.calls.Load() != 0 || second.calls.Load() != 0 {
				t.Fatal("invalid binding reached provider")
			}
		})
	}
	for name, mutate := range map[string]func(*planning.Descriptor){
		"connection": func(d *planning.Descriptor) { d.ConnectionID = "first" },
		"provider":   func(d *planning.Descriptor) { d.Provider = "other" },
		"model":      func(d *planning.Descriptor) { d.Model = "other" },
		"profile":    func(d *planning.Descriptor) { d.ExecutionProfileVersion = "other" },
	} {
		t.Run("mismatched-"+name, func(t *testing.T) {
			selector := mismatchedPlannerSelector{routedPlanner: routedPlanner{Planner: basePlanner, route: route}, mutate: mutate}
			service, err := app.NewWithConnections(events.NewGateway(store), registry, app.TaskConnectionRouting{Default: "first", Requirements: &route.requirements}, selector)
			if err != nil {
				t.Fatal(err)
			}
			submission := app.Submit{RequestID: "mismatched-" + name, OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}
			if _, err := service.Submit(t.Context(), submission); err == nil || !strings.Contains(err.Error(), "identity differs from routing decision") {
				t.Fatalf("mismatched planner was not rejected at selection: %v", err)
			}
			stream, err := service.ExternalEvents(t.Context(), "org-1", submission.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range stream {
				if event.EventType == "PLANNING_CONTEXT_MANIFESTED" || event.EventType == "INFERENCE_RESERVED" {
					t.Fatal("mismatched planner persisted inference context or reservation")
				}
			}
			if first.calls.Load() != 0 || second.calls.Load() != 0 {
				t.Fatal("mismatched planner contacted provider")
			}
		})
	}
	service, err := app.NewWithConnections(events.NewGateway(store), registry, app.TaskConnectionRouting{Default: "first", Requirements: &route.requirements}, routedPlanner{Planner: basePlanner, route: route})
	if err != nil {
		t.Fatal(err)
	}
	submission := app.Submit{RequestID: "routed-planning", OrganizationID: "org-1", Statement: "perform adaptive work", Kind: core.ExecutionAgent}
	if _, err := service.Submit(t.Context(), submission); err == nil {
		t.Fatal("synthetic planning failure was ignored")
	}
	stream, err := service.ExternalEvents(t.Context(), "org-1", submission.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	assertAuxiliaryRouteEvidence(t, stream, "PLANNING_CONTEXT_MANIFESTED", "second")
	if _, err := service.Submit(t.Context(), submission); err == nil {
		t.Fatal("failed planning replay was accepted")
	}
	if second.calls.Load() != 1 {
		t.Fatalf("planning attempt called provider %d times", second.calls.Load())
	}

	normalizationRequirements := route.requirements.Clone()
	normalizationRequirements.PreferredConnections = []string{"first"}
	normalizationRoute, err := newAuxiliaryRoute(registry, "second", normalizationRequirements)
	if err != nil {
		t.Fatal(err)
	}
	baseNormalizer, err := intake.NewModelNormalizer(intakeModel{adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	operator := intake.NewWithNormalizer(service, routedNormalizer{Normalizer: baseNormalizer, route: normalizationRoute})
	principal := intake.Principal{ID: "human-1", OrganizationID: "org-1", Kind: core.PrincipalHuman, Channel: intake.ChannelHumanDirect, WorkScope: intake.WorkScopeOrganization, Capabilities: []string{intake.CapabilitySubmitWork}}
	message := intake.Message{ConversationID: "routed-normalization", MessageID: "message-1", Text: "prepare an analysis"}
	unselectedOperator := intake.NewWithNormalizer(service, baseNormalizer)
	unselectedMessage := message
	unselectedMessage.ConversationID = "missing-normalizer-selector"
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := unselectedOperator.Handle(t.Context(), principal, unselectedMessage); err == nil {
			t.Fatal("governed normalizer without selector accepted")
		}
	}
	unselectedStream, err = service.ExternalEvents(t.Context(), "org-1", unselectedMessage.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range unselectedStream {
		if event.EventType == "INTENT_NORMALIZATION_CONTEXT_MANIFESTED" || event.EventType == "INFERENCE_RESERVED" {
			t.Fatal("unselected normalizer published model context")
		}
	}
	if first.calls.Load() != 0 || second.calls.Load() != 1 {
		t.Fatal("unselected normalizer called provider")
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := operator.Handle(t.Context(), principal, message); err == nil {
		t.Fatal("synthetic normalization failure was ignored")
	}
	stream, err = service.ExternalEvents(t.Context(), "org-1", message.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	assertAuxiliaryRouteEvidence(t, stream, "INTENT_NORMALIZATION_CONTEXT_MANIFESTED", "first")
	if first.calls.Load() != 1 || second.calls.Load() != 1 {
		t.Fatalf("wrong account calls: first=%d second=%d", first.calls.Load(), second.calls.Load())
	}
	if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
		t.Fatalf("auxiliary routing replay failed: %v", err)
	}
}

func assertAuxiliaryRouteEvidence(t *testing.T, stream []events.Event, contextType, connection string) {
	t.Helper()
	var contextRequirements *modelinput.RouteRequirements
	var contextDecision *modelinput.RouteDecision
	contexts, reservations := 0, 0
	for _, event := range stream {
		if event.EventType == contextType {
			var payload struct {
				RoutingDecision *modelinput.RouteDecision     `json:"routing_decision"`
				ConnectionID    string                        `json:"connection_id"`
				Routing         *modelinput.RouteRequirements `json:"routing"`
			}
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.ConnectionID != connection || payload.Routing == nil || payload.Routing.DataClass != "internal" || payload.Routing.Locality != modelinput.LocalOnly {
				t.Fatalf("context lost selected route: %+v", payload)
			}
			contextRequirements = payload.Routing
			if payload.RoutingDecision == nil || payload.RoutingDecision.ValidateFor(*payload.Routing) != nil || payload.RoutingDecision.ConnectionID != connection || payload.RoutingDecision.SnapshotSequence <= 0 || payload.RoutingDecision.SnapshotSequence >= event.Sequence {
				t.Fatal("auxiliary context lost selected decision")
			}
			contextDecision = payload.RoutingDecision
			contexts++
		}
		if event.EventType == "INFERENCE_RESERVED" {
			var payload events.InferenceReservedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.ConnectionID != connection || !modelinput.SameRouteRequirements(contextRequirements, payload.Routing) || !modelinput.SameRouteDecision(contextDecision, payload.RoutingDecision) {
				t.Fatal("reservation changed manifested route")
			}
			reservations++
		}
	}
	if contexts != 1 || reservations != 1 {
		t.Fatalf("context count=%d reservation count=%d", contexts, reservations)
	}
}
