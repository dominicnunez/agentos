package inference

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/modelinput"
)

const PolicyVersion = 1

// ConnectionPolicyVersion binds an independently configured connection. Legacy
// policies retain their original version and fingerprint serialization.
const ConnectionPolicyVersion = 2

const reconciliationTimeout = 5 * time.Second

type AccessMode string

const (
	Subscription AccessMode = "SUBSCRIPTION"
	MeteredAPI   AccessMode = "METERED_API"
	Local        AccessMode = "LOCAL"
)

type Purpose string

const (
	PurposeIntentNormalization Purpose = "INTENT_NORMALIZATION"
	PurposePlanning            Purpose = "PLANNING"
	PurposeTaskExecution       Purpose = "TASK_EXECUTION"
)

type Pricing struct {
	InputNanoUSDPerMillionTokens  int64     `json:"input_nano_usd_per_million_tokens"`
	OutputNanoUSDPerMillionTokens int64     `json:"output_nano_usd_per_million_tokens"`
	MaxCostNanoUSDPerRequest      int64     `json:"max_cost_nano_usd_per_request"`
	MaxCostNanoUSDPerWindow       int64     `json:"max_cost_nano_usd_per_window"`
	ExpiresAt                     time.Time `json:"expires_at"`
}

// Policy is one reviewed organization budget for one exact provider, model,
// and execution profile. It contains no credentials or provider-controlled
// values. Metered prices are operator-reviewed inputs and expire independently
// from the authorization itself.
type Policy struct {
	Routing                   *RoutePolicy        `json:"routing,omitempty"`
	Catalog                   *CatalogDefinition  `json:"catalog,omitempty"`
	Version                   int                 `json:"version"`
	ConnectionID              string              `json:"connection_id,omitempty"`
	OrganizationBudget        *OrganizationBudget `json:"organization_budget,omitempty"`
	OrganizationID            string              `json:"organization_id"`
	Provider                  string              `json:"provider"`
	Model                     string              `json:"model"`
	ExecutionProfileVersion   string              `json:"execution_profile_version"`
	Mode                      AccessMode          `json:"mode"`
	MaxInputTokensPerRequest  int64               `json:"max_input_tokens_per_request"`
	MaxOutputTokensPerRequest int64               `json:"max_output_tokens_per_request"`
	MaxTokensPerWindow        int64               `json:"max_tokens_per_window"`
	ContinuityReserveTokens   int64               `json:"continuity_reserve_tokens"`
	WindowDurationSeconds     int64               `json:"window_duration_seconds"`
	MaxConcurrentRequests     int                 `json:"max_concurrent_requests"`
	MaxAttemptsPerRequest     int                 `json:"max_attempts_per_request"`
	AuthorizedBy              string              `json:"authorized_by"`
	AuthorizedAt              time.Time           `json:"authorized_at"`
	AuthorizationExpiresAt    time.Time           `json:"authorization_expires_at"`
	Pricing                   *Pricing            `json:"pricing,omitempty"`
}

func (p Policy) Validate() error {
	if p.Version != PolicyVersion && p.Version != ConnectionPolicyVersion || !validValue(p.OrganizationID) || !validValue(p.Provider) || !validValue(p.Model) || !validValue(p.ExecutionProfileVersion) || !validValue(p.AuthorizedBy) {
		return fmt.Errorf("inference policy identity is incomplete")
	}
	if p.Version == PolicyVersion && p.ConnectionID != "" || p.Version == ConnectionPolicyVersion && !ValidConnectionID(p.ConnectionID) {
		return fmt.Errorf("inference policy connection identity is invalid")
	}
	if p.Version == PolicyVersion && p.OrganizationBudget != nil {
		return fmt.Errorf("legacy inference policy cannot carry connection budget semantics")
	}
	if p.Version == ConnectionPolicyVersion {
		if p.OrganizationBudget == nil || p.OrganizationBudget.Validate() != nil {
			return fmt.Errorf("connection policy requires a valid organization inference budget")
		}
	}
	if p.Mode != Subscription && p.Mode != MeteredAPI && p.Mode != Local {
		return fmt.Errorf("inference access mode is invalid")
	}
	if p.MaxInputTokensPerRequest < 1 || p.MaxOutputTokensPerRequest < 1 || p.MaxInputTokensPerRequest > int64(math.MaxInt) || p.MaxOutputTokensPerRequest > int64(math.MaxInt) || p.MaxInputTokensPerRequest > math.MaxInt64-p.MaxOutputTokensPerRequest {
		return fmt.Errorf("inference request token bounds are invalid")
	}
	requestTokens := p.MaxInputTokensPerRequest + p.MaxOutputTokensPerRequest
	if p.MaxTokensPerWindow < requestTokens || p.ContinuityReserveTokens < 0 || p.ContinuityReserveTokens > p.MaxTokensPerWindow-requestTokens {
		return fmt.Errorf("inference window cannot preserve its configured reserve")
	}
	if p.WindowDurationSeconds < 60 || p.WindowDurationSeconds > int64((366*24*time.Hour)/time.Second) || p.MaxConcurrentRequests < 1 || p.MaxConcurrentRequests > 1024 || p.MaxAttemptsPerRequest != 1 {
		return fmt.Errorf("inference concurrency, retry, or window bounds are invalid")
	}
	if p.AuthorizedAt.IsZero() || p.AuthorizationExpiresAt.IsZero() || !p.AuthorizationExpiresAt.After(p.AuthorizedAt) || p.AuthorizedAt.Location() != time.UTC || p.AuthorizationExpiresAt.Location() != time.UTC {
		return fmt.Errorf("inference authorization times must be ordered UTC values")
	}
	if p.Mode == MeteredAPI {
		if p.Pricing == nil || p.Pricing.InputNanoUSDPerMillionTokens < 1 || p.Pricing.OutputNanoUSDPerMillionTokens < 1 || p.Pricing.MaxCostNanoUSDPerRequest < 1 || p.Pricing.MaxCostNanoUSDPerWindow < p.Pricing.MaxCostNanoUSDPerRequest || p.Pricing.ExpiresAt.IsZero() || p.Pricing.ExpiresAt.Location() != time.UTC || !p.Pricing.ExpiresAt.After(p.AuthorizedAt) {
			return fmt.Errorf("metered inference requires bounded, expiring pricing")
		}
		reserved, err := p.ReservedCostNanoUSD()
		if err != nil || reserved > p.Pricing.MaxCostNanoUSDPerRequest {
			return fmt.Errorf("metered inference request exceeds its cost bound")
		}
	} else if p.Pricing != nil {
		return fmt.Errorf("non-metered inference cannot carry monetary pricing")
	}
	if p.Catalog != nil {
		if p.Version != ConnectionPolicyVersion {
			return fmt.Errorf("catalog requires a named connection policy")
		}
		if _, err := p.Catalog.validateForPolicy(p); err != nil {
			return err
		}
		if p.Routing == nil {
			return fmt.Errorf("catalog requires reviewed organization routing policy")
		}
	}
	if p.Routing != nil {
		if p.Version != ConnectionPolicyVersion || p.Routing.Validate() != nil || p.Routing.OrganizationID != p.OrganizationID {
			return fmt.Errorf("routing policy does not match account authorization")
		}
	}
	return nil
}

func (p Policy) Fingerprint() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	body, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode inference policy: %w", err)
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func (p Policy) ReservedCostNanoUSD() (int64, error) {
	if p.Mode != MeteredAPI {
		return 0, nil
	}
	if p.Pricing == nil {
		return 0, fmt.Errorf("metered inference pricing is missing")
	}
	input, err := tokenCostNanoUSD(p.MaxInputTokensPerRequest, p.Pricing.InputNanoUSDPerMillionTokens)
	if err != nil {
		return 0, err
	}
	output, err := tokenCostNanoUSD(p.MaxOutputTokensPerRequest, p.Pricing.OutputNanoUSDPerMillionTokens)
	if err != nil || input > math.MaxInt64-output {
		return 0, fmt.Errorf("inference cost exceeds the supported range")
	}
	return input + output, nil
}

func (p Policy) ActualCostNanoUSD(usage events.InferenceUsageRecordedPayload) (int64, error) {
	if p.Mode != MeteredAPI {
		return 0, nil
	}
	if p.Pricing == nil || !usage.Valid() {
		return 0, fmt.Errorf("metered inference usage or pricing is invalid")
	}
	input, err := tokenCostNanoUSD(int64(usage.InputTokens), p.Pricing.InputNanoUSDPerMillionTokens)
	if err != nil {
		return 0, err
	}
	output, err := tokenCostNanoUSD(int64(usage.OutputTokens), p.Pricing.OutputNanoUSDPerMillionTokens)
	if err != nil || input > math.MaxInt64-output {
		return 0, fmt.Errorf("inference cost exceeds the supported range")
	}
	return input + output, nil
}

func tokenCostNanoUSD(tokens, pricePerMillion int64) (int64, error) {
	if tokens < 0 || pricePerMillion < 0 || tokens != 0 && pricePerMillion > math.MaxInt64/tokens {
		return 0, fmt.Errorf("inference cost exceeds the supported range")
	}
	product := tokens * pricePerMillion
	const million = int64(1_000_000)
	if product > math.MaxInt64-(million-1) {
		return 0, fmt.Errorf("inference cost exceeds the supported range")
	}
	return (product + million - 1) / million, nil
}

type Scope struct {
	RoutingDecision *modelinput.RouteDecision
	Routing         *modelinput.RouteRequirements
	OrganizationID  string
	Purpose         Purpose
	RequestID       string
	IntentID        string
	TaskID          string
	ExecutionID     string
	CorrelationID   string
}

func (s Scope) Validate() error {
	if s.RoutingDecision != nil && (s.Routing == nil || s.RoutingDecision.ValidateFor(*s.Routing) != nil || s.RoutingDecision.SnapshotSequence <= 0) {
		return fmt.Errorf("inference routing decision lacks valid requirements or a ledger snapshot")
	}
	if s.Routing != nil {
		if _, err := s.Routing.Canonical(); err != nil {
			return err
		}
		if s.Routing.OrganizationID != s.OrganizationID {
			return fmt.Errorf("routing requirements belong to another organization")
		}
	}
	if !validValue(s.OrganizationID) || !validValue(s.RequestID) || !validValue(s.ExecutionID) || !validValue(s.CorrelationID) {
		return fmt.Errorf("inference request scope is incomplete")
	}
	if s.Purpose != PurposeIntentNormalization && s.Purpose != PurposePlanning && s.Purpose != PurposeTaskExecution {
		return fmt.Errorf("inference purpose is invalid")
	}
	if s.IntentID == "" && s.TaskID == "" || s.IntentID != "" && !validValue(s.IntentID) || s.TaskID != "" && !validValue(s.TaskID) {
		return fmt.Errorf("inference request must be bound to an Intent or Task")
	}
	return nil
}

type scopeKey struct{}

func WithScope(ctx context.Context, scope Scope) (context.Context, error) {
	if ctx == nil {
		return nil, fmt.Errorf("inference context is required")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	ctx, err := modelinput.WithInvocation(ctx, scope.OrganizationID, scope.ExecutionID)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, scopeKey{}, cloneScope(scope)), nil
}

func scopeFromContext(ctx context.Context) (Scope, error) {
	if ctx == nil {
		return Scope{}, fmt.Errorf("inference context is required")
	}
	scope, ok := ctx.Value(scopeKey{}).(Scope)
	if !ok {
		return Scope{}, fmt.Errorf("durable inference scope is missing")
	}
	return cloneScope(scope), scope.Validate()
}

func cloneScope(scope Scope) Scope {
	scope.RoutingDecision = modelinput.CloneRouteDecision(scope.RoutingDecision)
	if scope.Routing != nil {
		routing := scope.Routing.Clone()
		scope.Routing = &routing
	}
	return scope
}

type InferenceRequest struct {
	ConnectionID string
	Scope        Scope
	Descriptor   execution.ModelDescriptor
	PromptSHA256 string
}

type Reservation struct {
	ID                   string
	PolicyFingerprint    string
	Mode                 AccessMode
	Request              InferenceRequest
	ReservedInputTokens  int64
	ReservedOutputTokens int64
	ReservedCostNanoUSD  int64
	WindowStartedAt      time.Time
	WindowExpiresAt      time.Time
}

type Reconciliation string

const (
	ReconciliationCompleted                 Reconciliation = "COMPLETED"
	ReconciliationNotSent                   Reconciliation = "NOT_SENT"
	ReconciliationUncertain                 Reconciliation = "UNCERTAIN"
	ReconciliationViolation                 Reconciliation = "VIOLATION"
	ReconciliationTerminalFailed            Reconciliation = "TERMINAL_FAILED"
	ReconciliationTerminalIncomplete        Reconciliation = "TERMINAL_INCOMPLETE"
	ReconciliationTerminalFailedNoUsage     Reconciliation = "TERMINAL_FAILED_NO_USAGE"
	ReconciliationTerminalIncompleteNoUsage Reconciliation = "TERMINAL_INCOMPLETE_NO_USAGE"
)

type Store interface {
	ActivateInferencePolicy(context.Context, Policy) error
	ReserveInference(context.Context, InferenceRequest) (Reservation, error)
	ReconcileInference(context.Context, Reservation, *events.InferenceUsageRecordedPayload, Reconciliation) (int64, error)
}

// containmentStore is required at construction, including for wrappers of Store.
// Both live cancellation and committed-generation checks protect provider calls.
type containmentStore interface {
	BeginInferenceContext(context.Context, string) (context.Context, func(), error)
	CheckInferenceContext(context.Context, string) error
}

// GuardedAdapter is the single production provider boundary. It admits a
// provider call only after a durable reservation and returns a response only
// after the reservation is durably reconciled.
type GuardedAdapter struct {
	store        Store
	containment  containmentStore
	adapter      execution.ModelAdapter
	connectionID string
}

// NewGuardedConnectionAdapter binds connection identity at trusted composition,
// independently of model/provider responses and request content.
func NewGuardedConnectionAdapter(store Store, adapter execution.ModelAdapter, connectionID string) (*GuardedAdapter, error) {
	if !ValidConnectionID(connectionID) {
		return nil, fmt.Errorf("inference connection identity is invalid")
	}
	guard, err := NewGuardedAdapter(store, adapter)
	if err != nil {
		return nil, err
	}
	guard.connectionID = connectionID
	return guard, nil
}

func NewGuardedAdapter(store Store, adapter execution.ModelAdapter) (*GuardedAdapter, error) {
	if store == nil || adapter == nil {
		return nil, fmt.Errorf("inference store and model adapter are required")
	}
	containment, ok := store.(containmentStore)
	if !ok {
		return nil, fmt.Errorf("inference store requires live containment and committed hold checks")
	}
	descriptor := adapter.Descriptor()
	if !validValue(descriptor.Provider) || !validValue(descriptor.Model) || !validValue(descriptor.ExecutionProfileVersion) {
		return nil, fmt.Errorf("model adapter descriptor is incomplete")
	}
	return &GuardedAdapter{store: store, containment: containment, adapter: adapter}, nil
}

func (a *GuardedAdapter) Name() string { return a.adapter.Name() }

// ConnectionID is set by trusted composition, never by the model response.
func (a *GuardedAdapter) ConnectionID() string { return a.connectionID }

func (a *GuardedAdapter) Descriptor() execution.ModelDescriptor { return a.adapter.Descriptor() }

func (a *GuardedAdapter) Complete(ctx context.Context, prompt string) (execution.ModelResponse, error) {
	digest := sha256.Sum256([]byte(prompt))
	return a.complete(ctx, hex.EncodeToString(digest[:]), func(callCtx context.Context) (execution.ModelResponse, error) {
		return a.adapter.Complete(callCtx, prompt)
	})
}

func (a *GuardedAdapter) complete(ctx context.Context, fingerprint string, call func(context.Context) (execution.ModelResponse, error)) (execution.ModelResponse, error) {
	scope, err := scopeFromContext(ctx)
	if err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, err)
	}
	request := InferenceRequest{ConnectionID: a.connectionID, Scope: scope, Descriptor: a.adapter.Descriptor(), PromptSHA256: fingerprint}
	callCtx, release, containmentErr := a.containment.BeginInferenceContext(ctx, scope.OrganizationID)
	if containmentErr != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, containmentErr)
	}
	defer release()
	ctx = callCtx
	reservation, err := a.store.ReserveInference(ctx, request)
	if err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceDenied, err)
	}
	var response execution.ModelResponse
	var providerErr error
	checkContainment := func() error {
		// Caller cancellation may win before a committed freeze. Preserve the
		// generation while checking durable history independently of that cause.
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconciliationTimeout)
		defer cancel()
		holdErr := a.containment.CheckInferenceContext(checkCtx, scope.OrganizationID)
		return errors.Join(context.Cause(ctx), holdErr)
	}
	if containmentErr := checkContainment(); containmentErr != nil {
		// The runtime has not invoked the adapter; this is definite not-sent
		// evidence, unlike cancellation after control reaches the provider.
		providerErr = execution.RequestNotSent(containmentErr)
	} else {
		response, providerErr = call(ctx)
	}
	if providerErr != nil {
		result := ReconciliationUncertain
		var usage *events.InferenceUsageRecordedPayload
		if execution.WasRequestNotSent(providerErr) {
			result = ReconciliationNotSent
		} else if terminal, ok := execution.TerminalResponseOutcome(providerErr); ok {
			result, usage = terminalReconciliation(terminal, reservation)
		}
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconciliationTimeout)
		_, reconcileErr := a.store.ReconcileInference(reconcileCtx, reservation, usage, result)
		cancel()
		code := execution.ModelCallFailed
		if reconcileErr != nil {
			code = execution.InferenceRecordFailed
		}
		return execution.ModelResponse{}, execution.SafeModelError(code, errors.Join(providerErr, reconcileErr, checkContainment()))
	}
	// Account attribution belongs to runtime composition, not provider output.
	response.Usage.ConnectionID = a.connectionID
	if !response.Usage.Valid() || response.Usage.Provider != request.Descriptor.Provider || response.Usage.Model != request.Descriptor.Model || int64(response.Usage.InputTokens) > reservation.ReservedInputTokens || int64(response.Usage.OutputTokens) > reservation.ReservedOutputTokens {
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconciliationTimeout)
		_, reconcileErr := a.store.ReconcileInference(reconcileCtx, reservation, &response.Usage, ReconciliationViolation)
		cancel()
		return execution.ModelResponse{}, execution.SafeModelError(execution.ModelContractFailed, errors.Join(fmt.Errorf("provider usage exceeded its authorized inference reservation"), reconcileErr, checkContainment()))
	}
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconciliationTimeout)
	costNanoUSD, err := a.store.ReconcileInference(reconcileCtx, reservation, &response.Usage, ReconciliationCompleted)
	cancel()
	if err != nil {
		return execution.ModelResponse{}, execution.SafeModelError(execution.InferenceRecordFailed, errors.Join(err, checkContainment()))
	}
	if reservation.Mode == MeteredAPI {
		costUSD := float64(costNanoUSD) / 1_000_000_000
		response.Usage.CostUSD = &costUSD
	}
	if containmentErr := checkContainment(); containmentErr != nil {
		return execution.ModelResponse{}, execution.WithReconciledUsage(execution.SafeModelError(execution.ModelCallFailed, containmentErr), response.Usage)
	}
	return response, nil
}

func validValue(value string) bool {
	return value != "" && len(value) <= 512 && strings.TrimSpace(value) == value && utf8.ValidString(value) && strings.IndexFunc(value, func(character rune) bool {
		return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
	}) < 0
}

// ValidConnectionID accepts a bounded configuration identifier, never a URL,
// credential, path, or provider-returned model name.
func ValidConnectionID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}
