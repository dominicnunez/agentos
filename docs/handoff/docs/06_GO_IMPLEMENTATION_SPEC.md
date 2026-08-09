# Go Implementation Specification

## 1. Architecture

Start with one Go module/binary and one SQLite database.

Suggested modules:

```text
internal/
  actors/
  organizations/
  teams/
  tasks/
  events/
  inbox/
  scheduler/
  runtimeadapter/
  models/
  tools/
  policy/
  capabilities/
  approvals/
  completion/
  ledger/
  projections/
  api/
```

Do not create modules for future systems until promoted by `IMPLEMENTATION_SCOPE.yaml`.

## 2. Core Go types

Illustrative only; implementation may refine names.

```go
type EventType string

type Event struct {
    ID                string
    Sequence          int64
    OrganizationID    string
    Type              EventType
    SourceActorID     *string
    SourceExecutionID *string
    RecipientScope    *string
    RecipientID       *string
    TaskID            *string
    CollaborationID   *string
    AuthorizationRefs []string
    ArtifactRefs      []string
    CreatedAt         time.Time
    SchemaVersion     int
    PayloadJSON       []byte
}
```

```go
type EventDraft struct {
    Type              EventType
    RecipientScope    *string
    RecipientID       *string
    TaskID            *string
    CollaborationID   *string
    ArtifactRefs      []string
    PayloadJSON       []byte
    ProposedPriority  *string
}
```

`EventDraft` deliberately lacks authoritative identity/time/sequence/approval/attestation fields.

## 3. Event gateway

```go
type EventGateway interface {
    PublishDraft(ctx context.Context, exec ExecutionIdentity, d EventDraft) (Event, error)
    PublishRuntimeEvent(ctx context.Context, authority RuntimeAuthority, e RuntimeEventRequest) (Event, error)
}
```

Responsibilities:

- validate allowed draft event types;
- validate payload schema;
- validate routing/task relationship;
- apply policy/capability checks;
- stamp trusted metadata;
- persist atomically;
- update/enqueue projections after persistence.

## 4. Ledger

V1 SQLite tables may include:

```text
events
organizations
agents
teams
goals
tasks
capability_leases
approvals
completion_contracts
idempotency_actions
projection_offsets
```

The event table is append-only through application interfaces.

## 5. RuntimeAdapter

```go
type RuntimeAdapter interface {
    Execute(ctx context.Context, req ExecutionRequest) (ExecutionResult, error)
}
```

V1 adapters:

- `FakeAdapter` for deterministic tests;
- one real provider adapter.

Hermes as an **internal RuntimeAdapter** and arbitrary remote-agent worker adapters are later features. The minimal inbound Hermes A2A Operator Gateway is V1 and is implemented separately under `internal/operator/a2a`.

## 6. Agent identity split

Keep these concepts separate:

### AgentBlueprint

Role/operating instructions/default capability classes. Minimal in V1.

### ExecutionProfile

Model/provider/reasoning/tool/prompt version configuration.

### RuntimeAdapter

How execution is invoked.

### Agent

Durable logical identity referring to versions of the above.

Do not turn every profile/role into a separate persistent Agent.

## 7. Scheduler

Basic deterministic scheduler:

- ready/waiting/running states;
- dependency checks;
- wake on inbox/dependency/approval events;
- retries/timeouts;
- cancellation;
- basic budget checks.

MLFQ/context hibernation/resource lanes are future considerations.

## 8. Model context

Context builder supplies only needed slices:

- TaskContract;
- relevant prior messages/events;
- artifact summaries/references;
- current capability/tool descriptions;
- relevant team/task state.

Do not send the entire ledger or organization history by default.

V1 format: JSON + natural language. TOON may be benchmarked later behind a `ContextCodec` interface.

## 9. Tools/actions

Tool gateway validates:

- exact action/resource/scope;
- originating Task/Intent references;
- current capability lease;
- human consequence boundary;
- freeze/revoke state;
- data/provider boundary where applicable.

Runtime records actual tool outcome as attested evidence/event.

## 10. Testing

Use deterministic fakes for:

- model adapter;
- clock;
- notifier;
- tool effects;
- completion verifiers where possible.

Run race tests, architecture checks, event ordering/restart/idempotency tests, and adversarial safety cases.


## v4.1.1 Go modules

Add small bounded packages/interfaces:

```text
internal/knowledge   # records/versioning/simple retrieval
internal/skills      # instruction/reference packages + promotion state
internal/audit       # deterministic rules/schedules/findings
internal/inference   # pools, availability/reserve policy, selection telemetry
```

Do not add a vector database, plugin loader, LLM audit persona, or predictive optimizer to V1.
## v4.1.2 — Minimal workflow/execution mechanism

Do not add a generic workflow DSL.

Extend V1 Task/TaskContract concepts with an execution kind such as:

```go
type ExecutionKind string

const (
    ExecutionDeterministic ExecutionKind = "DETERMINISTIC"
    ExecutionTool          ExecutionKind = "TOOL"
    ExecutionAgent         ExecutionKind = "AGENT"
    ExecutionTeam          ExecutionKind = "TEAM"
    ExecutionHuman         ExecutionKind = "HUMAN"
    ExecutionMixed         ExecutionKind = "MIXED"
)
```

and a model inference policy:

```go
type ModelInferencePolicy string

const (
    ModelInferenceDisallowed        ModelInferencePolicy = "DISALLOWED"
    ModelInferenceAllowedIfJustified ModelInferencePolicy = "ALLOWED_IF_JUSTIFIED"
    ModelInferenceRequired          ModelInferencePolicy = "REQUIRED"
)
```

These are task execution metadata, not a workflow language.

For Agent-owned Tasks, default model inference policy should normally be `ALLOWED_IF_JUSTIFIED`. Do not create an AgentExecution until the workflow actually reaches a step requiring model capability.
## v4.2 — boundary/runtime modules

Add logical modules/interfaces:

```text
internal/operator/a2a
internal/contextmanifest
internal/tooloutcome
internal/effects
internal/secrets
```

Suggested interfaces:

```go
type SecretSource interface {
    Resolve(ctx context.Context, ref SecretRef) (SecretValue, error)
}

type EffectStore interface {
    Create(ctx context.Context, o EffectObligation) error
    RecordAttempt(ctx context.Context, id EffectObligationID, outcome ToolOutcome) error
    Confirm(ctx context.Context, id EffectObligationID, evidence []ArtifactRef) error
}
```

The A2A adapter imports protocol-specific generated/types packages; core `tasks`, `events`, `capability`, and `actors` packages do not.

Context builder writes an immutable ExecutionContextManifest before/with execution start.

Tool adapters should verify deterministic postconditions where practical.
