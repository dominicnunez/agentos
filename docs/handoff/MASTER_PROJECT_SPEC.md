# Agent OS — Consolidated Master Project Specification v4.2

**Authority:** Normative implementation handoff. v4.2 supersedes v4.1.2 and all earlier handoffs.

> Start with `QUICK_START.md`; use this master file as the consolidated reference.


---

<!-- SOURCE: docs/00_START_HERE.md -->

# Agent OS — AI Coding Handoff v4.2

> **Short path:** If you are overwhelmed by the documentation, read `../QUICK_START.md`, `29_V1_BUILD_CONTRACT.md`, and `35_WORK_FIRST_ORCHESTRATION_AND_MINIMAL_LLM.md`. Use the rest as reference.
**Status:** Normative implementation handoff  
**Date:** 2026-08-08  
**Primary implementation language:** Go  
**Architecture:** modular monolith first  
**Working name:** Agent OS

## 1. Why v4.2

v4.2 is a deliberate architectural reset of the communication layer and a further reduction of implementation scope.

The project no longer uses:

- ANL / Agent Native Language;
- a general Agent Semantic Model;
- a global ontology for beliefs, hypotheses, observations, contradictions, confidence, etc.

Those approaches are **superseded**, not deferred.

The replacement is **Agent OS Event Contracts**:

> Formalize only the semantics that Agent OS itself must react to deterministically. Everything else remains ordinary agent content.

This change reduces custom protocol surface while preserving persistence, auditability, provenance, authorization, passive awareness, and deterministic control.

## 2. Core thesis

Agent OS tests this abstraction:

> **Organizations, Teams, and Agents are durable logical actors; model executions are ephemeral work sessions that wake those actors, advance their state, communicate, use approved tools, and then disappear.**

The first falsifiable question is:

> Can a persistent asynchronously collaborating Team solve genuinely interdependent work better than a strong single agent after accounting for cost, reliability, coordination overhead, and safety?

## 3. V1 core

Build a local Go modular monolith containing only:

```text
Organization / Team / Agent identity
Goal / Task dependency graph
AgentExecution + RuntimeAdapter
Scheduler
Event Ledger
Event Router + Inbox projections
Agent OS Event Contracts
Capability / policy gate
Authorization trace projection
Blocked-task flow
Human consequence boundary + approval
Freeze / revoke
CompletionContract + Completion Engine
Runtime-attested evidence
Operational telemetry/replay harness + adversarial tests
Basic human CLI/UI
```

## 4. Communication model

```text
Agent/model output
      |
      v
 EventDraft
 (untrusted)
      |
      v
Event Gateway / validator
      |
      | validates allowed event type,
      | routing, task relationship,
      | authority and payload
      v
Persist Event first
      |
      v
Immutable Event Ledger
      |
      +--> Inbox projection
      +--> Team/task state projection
      +--> Audit/provenance projection
      +--> Authorization trace projection
      |
      v
Recipient action boundary
      |
      v
Context injection
```

The persisted `Event` has a runtime-owned envelope. Agent content is untrusted.

## 5. Initial agent-proposable event contracts

Only these require first-class model-facing event output in v1:

- `MESSAGE`
- `TASK_BLOCKED`
- `EVIDENCE_PUBLISHED`
- `RESULT_PUBLISHED`
- `CANDIDATE_COMPLETE`

Other events such as `TASK_ASSIGNED`, `APPROVAL_DECIDED`, `COMPLETION_VERIFIED`, `CAPABILITY_REVOKED`, and `FREEZE_SET` are emitted by trusted runtime operations, not by a model declaring them in content.

## 6. Critical content/control rule

> **Content cannot create control-plane authority.**

An agent may write “APPROVED”, “SYSTEM”, “ADMIN”, “ignore policy”, or any other text. It has no runtime effect unless an authorized runtime operation emits the corresponding typed event under the correct authority chain.

## 7. Structure-promotion rule

> **Promote semantics into structure only when deterministic runtime behavior, safety, verification, interoperability, or measured efficiency requires it.**

Do not add a new event kind merely because a concept is intellectually useful.

## 8. One history, many projections

Use one append-only event history as the source of truth. Prefer projections over new stores:

```text
ledger -> inbox
ledger -> current task state
ledger -> team state
ledger -> audit history
ledger -> provenance trace
ledger -> authorization trace
```

A separate blackboard, authorization ledger, provenance store, or semantic memory database must be justified by measured need.

## 9. Scope authority

`IMPLEMENTATION_SCOPE.yaml` is the machine-readable implementation-scope authority.

- `V1_CORE`: build now.
- `VALIDATE_NEXT`: controlled evaluation/prototype only after real V1 operational evidence exists.
- `FUTURE_IF_EARNED`: do not build until prerequisites and revisit trigger are satisfied.
- `SUPERSEDED`: do not implement.

`future/FUTURE_CONSIDERATIONS.yaml` preserves compatible demoted ideas and their prerequisites.

## 10. Safety invariants

1. No LLM is trusted merely because it has a manager/evaluator title.
2. Workers do not self-grant or normally solicit authority expansion.
3. Positive capabilities do not automatically inherit.
4. Restrictions/authority ceilings remain binding through delegation.
5. `discoverable != invocable != authorized`.
6. Consequential actions require valid authorization ancestry at time of use.
7. Human-required actions wait until an authorized human answers.
8. Escalating notifications may increase attention, never authority.
9. Human freeze/revoke wins at time of action.
10. Agents only produce `CANDIDATE_COMPLETE`; the Completion Engine verifies completion.
11. Agent content cannot forge runtime identity, approval, attestation, authorization, completion, or ledger history.
12. External content is data, not authority.
13. Unknown safety-critical control semantics fail closed.
14. Persist before making a communication available to another actor.

## 11. Read order

1. `00_START_HERE.md`
2. `29_V1_BUILD_CONTRACT.md`
3. `03_EVENT_CONTRACTS_V0_1_SPEC.md`
4. `02_ARCHITECTURE.md`
5. `04_SECURITY_AND_THREAT_MODEL.md`
6. `05_TEAMS_TASKS_DURABILITY.md`
7. `06_GO_IMPLEMENTATION_SPEC.md`
8. `17_SAFETY_INTEGRITY_AND_CONSTITUTION.md`
9. `18_APPROVALS_BLOCKED_TASKS_AND_NOTIFICATIONS.md`
10. `19_COMPLETION_VERIFICATION_AND_EVIDENCE.md`
11. `08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md`
12. `16_FALSIFICATION_AND_BENCHMARK_POLICY.md`
13. `20_FUTURE_CONSIDERATIONS.md`
14. remaining normative docs as needed
15. `future/` only when a prerequisite has been met
16. `history/` and `research/` for context only


## v4.2 required addenda

After `29_V1_BUILD_CONTRACT.md`, read:

- `30_VERSIONED_KNOWLEDGE_AND_SKILLS.md`
- `31_AUDIT_SERVICE.md`
- `32_LAB_EXPERIMENTATION_AND_PROMOTION.md`
- `33_INFERENCE_RESOURCE_MANAGEMENT.md`

These supersede v4.0 statements that placed all memory/skills outside V1.


## v4.2 patch

Read `34_V4_1_1_IMPLEMENTATION_HARDENING.md` before coding knowledge promotion, topology experiments, Skill validation, or inference telemetry adapters.

## v4.2 runtime boundary additions

Read `36_A2A_OPERATOR_GATEWAY_AND_HERMES.md` and `37_EXECUTION_CONTEXT_TOOL_OUTCOME_EFFECTS.md` before implementing the external operator interface, context builder, tool adapters, approvals, or external effects.


---

<!-- SOURCE: docs/01_VISION_AND_PRODUCT.md -->

# Vision and Product Direction

## 1. Product thesis

Agent OS is infrastructure for building and operating persistent artificial organizations.

It does **not** need to be sold as a standalone product to be valuable. A successful Agent OS can be an internal capability used to build and operate multiple products, services, or businesses.

```text
                         Agent OS
                            |
          +-----------------+-----------------+
          |                 |                 |
          v                 v                 v
    Organization A    Organization B    Organization C
     product/service   product/service   product/service
```

The success criterion is operational leverage, not framework adoption.

## 2. Durable abstractions

### Organization

Owns mission/goals, teams, policy boundary, budget envelope, and durable history.

### Team

A durable collaborative actor with members, mission, task participation, inbox/state projections, and history.

### Agent

A durable logical identity. It can survive model/provider/profile changes.

### AgentExecution

An ephemeral invocation/session used to advance an Agent or Team. No LLM process needs to stay alive while the organization sleeps.

## 3. Human UX

Humans should be able to express goals in ordinary language and inspect:

- organization/team structure;
- active/waiting work;
- messages/events;
- blocked work;
- approvals;
- verified completion;
- audit/history;
- cost/resource usage.

Technical IDs and event schemas belong in Advanced/Audit views.

## 4. Build organizations, not bureaucracy

Human company metaphors are useful UX, not mandatory machine structure.

Do not create CEO/VP/manager hierarchies merely because human companies have them. The system should prefer the smallest useful number of persistent actors and coordination layers.

## 5. Long-term vision

If earned through real operational evidence and controlled evaluation, Agent OS may later support:

- richer institutional knowledge;
- SOP/skills;
- organization evaluation/health;
- controlled organizational experiments;
- model/reasoning/team optimization;
- research/self-improvement organizations;
- external federation;
- multi-organization operation.

Those are future capabilities, not V1 assumptions. See `../future/FUTURE_CONSIDERATIONS.md`.
## v4.1.2 — Work-first operating philosophy

Agent OS is infrastructure for getting real organizational work done.

The system does not seek to maximize the number of Agents, Teams, model calls, or agentic steps.

Actual objectives determine whether work is handled by normal software, tools/APIs, one Agent, a Team, a human operator, or a mixture.

Persistent Agents represent durable responsibility/experience; they need not invoke an LLM for every step.
## v4.2 — Hermes operator relationship

Agent OS may remain headless/infrastructure-oriented while Hermes serves as a human-facing operator/chief-of-staff layer.

```text
Human
  -> Hermes
      -> A2A Operator Gateway
          -> Agent OS
              -> businesses/organizations
```

This avoids duplicating Hermes interaction surfaces while keeping Agent OS authority, audit, persistence, and organizational execution independent.


---

<!-- SOURCE: docs/02_ARCHITECTURE.md -->

# Architecture

## 1. V1 architecture

Agent OS begins as one Go modular monolith and one SQLite database.

```text
Human/API
   |
   v
Application / Organization control
   |
   +-----------> Tasks / Scheduler
   |                   |
   |                   v
   |             AgentExecution
   |                   |
   |                   v
   |             RuntimeAdapter
   |                   |
   |                   v
   |             model / tools
   |
   +-----------> Event Gateway
                       |
                       v
                 Event Ledger
                       |
        +--------------+--------------+
        |              |              |
        v              v              v
      Inbox          Task state     Audit/trace
        |
        v
  next action boundary
```

## 2. Event sourcing

The immutable event ledger is authoritative for meaningful runtime history.

Current state is a materialized projection.

Persist before:

- delivery/availability acknowledgement;
- consequential effect acknowledgement;
- completion transition.

At-least-once delivery is acceptable; consequential effects require idempotency/action records.

## 3. Event Gateway

Responsibilities:

1. accept `EventDraft` or trusted runtime event request;
2. validate event type and payload;
3. authenticate actual actor/runtime source;
4. resolve/validate recipient and task/collaboration relation;
5. enforce capability/policy checks where the event implies an effect;
6. assign runtime-owned ID/sequence/timestamp/source metadata;
7. persist;
8. project to inbox/state;
9. make available at action boundaries.

A model never supplies authoritative sender identity, timestamp, approval state, runtime attestation, or ledger sequence.

## 4. Communication/content boundary

The runtime formally understands **coordination and control events**.

Inside `MESSAGE` or result content, agents may use natural language, JSON, code, files, images, tables, or artifact references.

Agent OS does not attempt to formalize beliefs, hypotheses, critiques, or internal cognition unless a later concrete requirement earns a dedicated event contract.

## 5. Task graph

Use Tasks with `ParentID` and `DependsOn[]`. That already creates a DAG.

Do not build a separate generic PlanGraph engine in V1.

## 6. Scheduler

The scheduler is deterministic software. V1 responsibilities:

- runnable/waiting state;
- dependency readiness;
- basic priority;
- retries/timeouts;
- cancellation;
- budget checks;
- wake on relevant event;
- suspend while waiting for human/dependency.

No LLM decides runtime scheduling truth.

## 7. Model/tool boundary

Core code depends on provider-neutral interfaces.

V1:

- deterministic fake model adapter for tests;
- one real model adapter;
- tool calls pass through capability/consequence enforcement;
- runtime attests actual tool results/effects.

## 8. Conceptual planes

Organization, Runtime, Safety/Integrity, and Evaluation/Optimization remain useful conceptual views. They are **not** required services or deployment boundaries.

Only V1 modules are built now. Mature-plane functions are future considerations unless explicitly V1-scoped.


## v4.1.1 learning, audit, and inference additions

These are bounded modules inside the modular monolith, not new distributed planes:

```text
Event Ledger -> Versioned Knowledge/Skills -> Context Builder
           \-> AuditService -> AuditFinding
Scheduler -> InferenceResourceManager -> RuntimeAdapter/Model
```

The future Lab composes Task + sandbox + ephemeral executions + resource budget + experimental trust labels. It does not bypass authority or promotion gates.
## v4.1.2 — Work execution architecture

The V1 Task dependency graph doubles as the minimal workflow representation.

```text
Goal
  -> Task DAG
       -> deterministic handler
       -> tool/API
       -> Agent responsibility -> AgentExecution only when needed
       -> Team
       -> human operator
       -> mixed child Tasks
```

Do not create a separate workflow language in V1.

The scheduler/runtime should distinguish durable responsibility from inference execution. A durable Agent may progress work through deterministic handlers/tools without creating a model invocation until adaptive intelligence is justified.

Inference Resource Manager is downstream of the decision that an LLM is needed; it is not the universal dispatcher for all work.
## v4.2 — External operator and runtime-evidence boundaries

```text
Hermes / external A2A peer
        |
     A2A v1.0
        |
A2A Operator Gateway
        |
authenticate / authorize / translate
        |
Intent / Goal / Task DAG
        |
Event Contracts + Ledger
```

A2A wire objects never become the internal domain model.

Each `AgentExecution` has an `ExecutionContextManifest` capturing what actually entered context.

Tool/effect path:

```text
Task/Agent
 -> Tool Adapter
 -> deterministic recovery/postcondition checks
 -> ToolOutcome
 -> if consequential external effect:
      EffectObligation persisted
      effect attempted
      confirmation/reconciliation recorded
```

This strengthens auditability without adding semantic cognition types.


---

<!-- SOURCE: docs/03_EVENT_CONTRACTS_V0_1_SPEC.md -->

# Agent OS Event Contracts v0.1

## 1. Decision

Agent OS v4.x replaces ANL and the Agent Semantic Model with **Event Contracts**.

Event Contracts are not a cognition language. They are a small set of structured coordination/control records used where deterministic runtime behavior is required.

## 2. Design principle

> **Structure only what software must understand. Leave the rest as content.**

A distinction earns a typed event contract when at least one is true:

- runtime behavior changes deterministically because of it;
- safety/authority depends on it;
- completion/verification depends on it;
- interoperability requires it;
- measured efficiency/reliability justifies it.

## 3. Trust boundary

Every persisted event is conceptually:

```text
+--------------------------------------------------+
| Trusted runtime-owned envelope                   |
| ID / sequence / source identity / time / routing |
| task/collaboration refs / auth refs / type       |
+-------------------------+------------------------+
                          |
                          v
+--------------------------------------------------+
| Payload/content                                    |
| model-generated portions are untrusted             |
| text / structured data / artifact references       |
+--------------------------------------------------+
```

A model may propose content. It cannot mint authoritative envelope fields.

## 4. EventDraft vs Event

### EventDraft

Untrusted proposal produced by an AgentExecution.

V1 allowed model-proposable kinds:

- `MESSAGE`
- `TASK_BLOCKED`
- `EVIDENCE_PUBLISHED`
- `RESULT_PUBLISHED`
- `CANDIDATE_COMPLETE`
- `KNOWLEDGE_PROPOSED`
- `SKILL_PROPOSED`

Draft contains no authoritative sender identity, event ID, ledger sequence, timestamp, approval, capability grant, or attestation.

### Event

Validated, runtime-stamped, persisted record.

Only `Event` enters the ledger.

## 5. V1 event families

### Agent-proposable coordination/content

#### `MESSAGE`

Purpose: lateral or directed communication.

Payload:

```text
body: string
optional structured_content: object
optional artifact_refs[]
```

`body` has no control-plane authority.

#### `TASK_BLOCKED`

Purpose: return control upward when work cannot continue within current assignment.

Required payload:

```text
reason
missing
why_needed
work_completed
remaining_work?
evidence_refs[]?
urgency?
```

The worker describes the gap; it does not request its own authority expansion.

#### `EVIDENCE_PUBLISHED`

Purpose: publish evidence with explicit artifact/provenance references.

Payload:

```text
summary
artifact_refs[]
```

The summary is an agent claim. Actual tool/file provenance is runtime-attested when available.

#### `RESULT_PUBLISHED`

Purpose: publish a work product/result before completion certification.

Payload:

```text
summary
artifact_refs[]?
```

#### `CANDIDATE_COMPLETE`

Purpose: ask the Completion Engine to verify the task against its CompletionContract.

Payload:

```text
result_event_id?
artifact_refs[]?
notes?
```

It cannot set task state directly to verified complete.

#### `KNOWLEDGE_PROPOSED`

Purpose: propose a versioned institutional knowledge/experience/lesson/procedure candidate.

The proposal is untrusted content and cannot become ACTIVE without runtime validation/promotion.

#### `SKILL_PROPOSED`

Purpose: propose a reusable instruction/reference-based skill or revision.

The proposal cannot activate itself, grant capabilities, or introduce trusted executable code.

### Trusted runtime/control events

These are emitted by runtime operations, not created by model text:

- `INTENT_CREATED`
- `GOAL_CREATED`
- `TASK_CREATED`
- `TASK_ASSIGNED`
- `TASK_CANCELLED`
- `EXECUTION_STARTED`
- `EXECUTION_FINISHED`
- `CAPABILITY_CHECKED`
- `CAPABILITY_DENIED`
- `CAPABILITY_REVOKED`
- `APPROVAL_PENDING`
- `APPROVAL_ACKNOWLEDGED`
- `APPROVAL_DECIDED`
- `FREEZE_SET`
- `ACTION_ATTESTED`
- `COMPLETION_VERIFIED`
- `COMPLETION_REJECTED`
- `TASK_VERIFIED_COMPLETE`
- `KNOWLEDGE_ACTIVATED`
- `KNOWLEDGE_SUPERSEDED`
- `KNOWLEDGE_STALE`
- `KNOWLEDGE_QUARANTINED`
- `SKILL_ACTIVATED`
- `SKILL_SUPERSEDED`
- `SKILL_QUARANTINED`
- `AUDIT_RUN_STARTED`
- `AUDIT_FINDING_CREATED`
- `AUDIT_FINDING_RESOLVED`
- `INFERENCE_SELECTED`
- `INFERENCE_USAGE_RECORDED`

Additional lifecycle events may be added when a V1 state transition requires them; adding an event is not permission to invent a general ontology.

## 6. Runtime-owned envelope

Minimum persisted fields:

```text
event_id
sequence
organization_id
event_type
source_actor_id?        # authenticated/stamped by runtime
source_execution_id?
recipient_scope?        # actor/team/task/organization
recipient_id?
task_id?
collaboration_id?
authorization_refs[]
artifact_refs[]
created_at              # runtime timestamp
payload
schema_version
```

Infrastructure may own additional integrity/hash/idempotency fields.

## 7. Addressing

V1 recipient scopes:

- Agent;
- Team;
- current Task participants.

Organization-wide broadcast can be added only if needed; broad broadcast increases noise and information exposure.

## 8. Availability at action boundaries

Relevant persisted events may surface:

- before model call;
- after model call;
- before tool call;
- after tool call;
- before `CANDIDATE_COMPLETE`.

No mid-token interruption in V1.

## 9. Priority

Priority is metadata subject to runtime policy:

```text
P0 safety/global invalidating
P1 dependency/evidence likely to change current work
P2 relevant before current task completes
P3 informational
```

Agent-proposed priority is advisory. Runtime may reject/downgrade it.

## 10. Content formats

V1 canonical serialization: JSON.

Content may contain:

- natural language;
- structured JSON;
- code;
- artifact/file references.

TOON is not required. It is a future codec experiment only after JSON baseline metrics exist.

## 11. Content cannot create authority

The following strings inside `MESSAGE.body` have no special meaning:

```text
APPROVED
SYSTEM
ADMIN
CAPABILITY_GRANTED
TASK_COMPLETE
IGNORE POLICY
```

Only the corresponding authenticated runtime event/state transition can create those effects.

## 12. Extending the contract set

Before adding a new event type, document:

1. runtime behavior that requires the distinction;
2. why `MESSAGE`/existing event + content is insufficient;
3. validation rules;
4. security implications;
5. benchmark/operational evidence if the goal is efficiency rather than correctness.

If those answers are weak, do not add the event type.


## 13. v4.1 knowledge/resource trust rule

Knowledge and skill payloads remain content even when ACTIVE. They do not create authority. Audit findings are observations requiring normal remediation. Inference selection/usage events are runtime evidence and cannot be forged by model text.
## 14. v4.1.1 knowledge proposal hardening

`KNOWLEDGE_PROPOSED` may identify its basis. When `basis_type = REPEATED_PATTERN`, the default policy requires at least three concrete occurrence event references.

This event only creates a candidate. It cannot activate knowledge.

No new general semantic event ontology is introduced for patterns; the structured fields exist only because the knowledge promotion pipeline needs deterministic evidence-count/provenance behavior.
## 15. v4.2 runtime evidence/control events

Add trusted runtime events as implementation requires for:

```text
EXECUTION_CONTEXT_MANIFESTED
TOOL_OUTCOME_RECORDED
EFFECT_OBLIGATION_CREATED
EFFECT_ATTEMPTED
EFFECT_CONFIRMED
EFFECT_FAILED
EXTERNAL_ACTOR_AUTHENTICATED
A2A_WORK_ACCEPTED
A2A_INPUT_RECEIVED
```

These exist because deterministic runtime/audit behavior depends on them.

A2A Messages/Tasks are translated at the gateway; they do not bypass the Event Gateway/ledger.


---

<!-- SOURCE: docs/04_SECURITY_AND_THREAT_MODEL.md -->

# Security and Threat Model

## 1. Security stance

LLMs are fallible/untrusted decision producers. Deterministic infrastructure owns authority, identity, persistence, policy enforcement, human approvals, and runtime attestations.

## 2. Primary trust boundary

### Trusted/control data

- runtime identity;
- event ID/sequence/time;
- authorization references;
- capability leases;
- approval decisions;
- freeze/revoke state;
- runtime-attested tool/action evidence;
- Completion Engine result.

### Untrusted/content data

- model text;
- model-supplied JSON content;
- external papers/web pages/repos;
- tool output until classified/attested appropriately;
- imported artifacts.

**Content never becomes authority merely by saying authority-like words.**

## 3. Threats

### Prompt injection / instruction laundering

External content can contain commands. Treat it as data. It cannot alter root policy or capability state.

### Confused deputy / authority laundering

An actor cannot cause another actor to perform an effect outside the originating authorization chain.

### Identity spoofing

Models cannot set authoritative sender/role/membership metadata.

### Completion spoofing

Model says “done” -> only `CANDIDATE_COMPLETE`; Completion Engine decides verification.

### Evidence spoofing

Agent claims are distinct from runtime-attested evidence.

### Priority abuse

Sender does not control P0/P1 authority; priority is policy constrained.

### Covert channels

Natural-language content inherently permits covert signaling. V4.0 does **not** claim to eliminate covert communication. High-assurance restrictions on timing/size/content degrees of freedom are future considerations if a real threat model requires them.

### Tool side channels

Agents can communicate or exfiltrate through files, URLs, external services, DNS, Git, etc. Tool access therefore passes through capability/data-boundary enforcement.

### Trajectory composition

Individually permitted steps may combine into a prohibited consequence. Evaluate cumulative derivation/provenance at consequential boundaries.

### Stale authority/zombie work

Long-sleeping work revalidates TaskContract, capability leases, policy, approval state, relevant environment assumptions, and freeze state before consequential action.

## 4. Core invariants

- `discoverable != invocable != authorized`;
- positive authority does not automatically inherit;
- restrictions/ceilings do propagate;
- authorization checked at time of effect;
- control events cannot be forged by content;
- consequential subsystem outage fails closed;
- ledger history cannot be rewritten to conceal activity;
- human emergency control wins.

## 5. V1 isolation claim

V1 is a local modular monolith and does **not** claim hostile-code process/container isolation. Conformance profile must state this explicitly.

Before executing genuinely untrusted code or production-sensitive workloads, stronger sandbox/process/container isolation is a future prerequisite, not something implied by module boundaries.

## 6. Sensitive data

Sending sensitive data to a cloud model is an external disclosure. Provider/data-class policy must authorize it.

V1 should avoid intentionally placing secrets in message content. Advanced encrypted/deletable artifact storage and information-flow labels are future considerations before sensitive production use.
## v4.2 — A2A and effect security

### External peer authority

Authenticated A2A identity is not authorization. Every external actor receives explicit scoped capabilities.

### Approval scope

Human approval is tied to an exact effect fingerprint/arguments and may expire/be single-use. Materially changing the effect invalidates the approval where policy requires.

### Crash/retry ambiguity

Consequential external effects use persisted EffectObligations. Retry only under idempotency/reconciliation policy. Never infer success solely because a model/tool attempted the action.

### Context audit

ExecutionContextManifest is trusted runtime evidence. Agent content cannot forge what was actually materialized.

### Secrets

Prefer adapter-side secret resolution. Do not put long-lived credentials into model context when the runtime/tool can use them without disclosure.


---

<!-- SOURCE: docs/05_TEAMS_TASKS_DURABILITY.md -->

# Teams, Tasks, and Durability

## 1. Durable identity

`Agent` and `Team` are durable logical entities. `AgentExecution` is ephemeral.

The system may sleep with no LLM process alive and later resume from persisted state/events.

## 2. Team

V1 Team fields:

```text
ID
OrganizationID
Name
Mission?
MemberAgentIDs[]
Status
CreatedAt
```

Team state is a projection over events/tasks/artifacts, not a separate semantic world model.

## 3. Task

V1 Task:

```text
ID
GoalID
ParentID?
DependsOn[]
AssigneeType = AGENT | TEAM
AssigneeID
TaskContractVersion
Status
```

This graph is sufficient for V1 planning/dependencies.

## 4. TaskContract

Versioned immutable contract containing:

- objective;
- success criteria;
- hard constraints;
- forbidden effects/actions;
- allowed resources/capability requirements;
- budget;
- approval requirements;
- expected evidence/artifacts.

Changing the contract creates a new version. Workers cannot silently redefine success.

## 5. Blocked task

A worker that cannot continue publishes `TASK_BLOCKED`.

The delegating/governing actor may:

- provide information;
- rescope;
- split;
- reassign;
- cancel;
- create a new separately authorized assignment.

No ordinary worker self-service permission escalation.

## 6. Passive awareness

Messages/evidence are persisted first, routed to recipient projections, then surfaced at deterministic action boundaries.

No planner relay is required for every lateral communication.

## 7. Restart

After process restart:

- rebuild projections from events (or future checkpoints);
- preserve pending approvals;
- preserve inbox availability state;
- preserve Task status/dependencies;
- avoid duplicating consequential effects through idempotency/action records.


---

<!-- SOURCE: docs/06_GO_IMPLEMENTATION_SPEC.md -->

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


---

<!-- SOURCE: docs/07_UI_API_FEDERATION.md -->

# Human UI, API, and External Integration

## 1. V1 UI

A basic CLI/web UI is sufficient. Show:

- organizations/teams/agents;
- tasks and dependencies;
- event/message timeline;
- blocked work;
- pending approvals;
- completion status/evidence;
- freeze/revoke controls;
- audit metadata.

## 2. Plain-language labels

Default examples:

| UI | Internal |
|---|---|
| Can't continue | `TASK_BLOCKED` |
| Needs your decision | `APPROVAL_PENDING` |
| Work submitted for checking | `CANDIDATE_COMPLETE` |
| Work verified | `TASK_VERIFIED_COMPLETE` |
| Work history | Event ledger/audit projection |
| Put agent on hold | Actor status/dormancy (future if needed) |

## 3. Event inspection

Human view should clearly separate:

- trusted runtime envelope;
- agent-generated content;
- artifacts/provenance;
- authorization references;
- runtime attestation.

There is no separate semantic-language inspector.

## 4. API

REST + SSE is sufficient for V1 control/read/event streaming.

WebSockets may be added later if bidirectional UI needs justify them.

## 5. External integrations

V1 has no **general federation** requirement; v4.2 requires only the minimal inbound A2A Operator Gateway for Hermes/external operator use.

The V1 inbound A2A Operator Gateway maps external task/session transport to internal work. Outbound A2A discovery/delegation remains later. Do not define an ANL/ASM federation payload.

MCP may be used behind the tool/capability gateway for compatible tools/resources; it is not the internal communication substrate.


## v4.1 UI additions

Default human views should include:

- Team/Agent knowledge with version history and “Why do we believe this?” provenance;
- active Skills with version, last verified, dependencies/capabilities required;
- open Audit Findings;
- inference resource status: subscription estimate/reset, metered budget, local capacity, reserve state;
- simple explanation of why a model/pool was selected when useful.

Keep exact IDs/evidence/profile details in Advanced/Audit views.
## v4.2 — A2A Operator Gateway is V1

Prior deferral of all A2A/federation no longer applies to the **minimal inbound operator interface**.

V1 includes:

- A2A v1.0 Agent Card;
- authenticated external actor mapping;
- work submission/continuation;
- task status/progress;
- blocked/input-needed mapping;
- result Artifact mapping;
- correlation to internal Intent/Goal/Task IDs;
- Hermes interoperability/conformance tests.

Still deferred:

- arbitrary outbound remote-agent discovery/delegation;
- federation marketplace;
- cross-organization trust federation;
- dynamic remote capability negotiation.

Do not expose root control operations through A2A merely because the protocol can carry messages.


---

<!-- SOURCE: docs/08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md -->

# Implementation Roadmap and Acceptance

## Phase 0 — repository/kernel skeleton

- Go modular monolith;
- module boundaries/Archguard;
- SQLite migrations;
- deterministic fake adapter;
- event ledger append/read/replay;
- basic projections.

## Phase 1 — actors/tasks/execution

- Organization, Team, Agent, AgentExecution;
- Goal/Task dependency graph;
- TaskContract;
- RuntimeAdapter;
- basic scheduler.

## Phase 2 — Event Contracts + async collaboration

- EventDraft/Event gateway;
- `MESSAGE`, `TASK_BLOCKED`, `EVIDENCE_PUBLISHED`, `RESULT_PUBLISHED`, `CANDIDATE_COMPLETE`;
- persist-before-availability;
- inbox/team/task projections;
- action-boundary message surfacing;
- priority policy.

## Phase 3 — authority and human boundary

- capabilities;
- authorization trace projection;
- no positive inheritance;
- blocked-task parent remediation;
- human consequence classifier;
- pending approval;
- freeze/revoke/time-of-use check.

## Phase 4 — completion/evidence

- CompletionContract;
- candidate completion;
- deterministic verifiers;
- runtime-attested evidence;
- verified completion state.

## Phase 5 — representative real work + measurement

Operate on representative actual organizational work.

For each real task/workflow:

- represent it with the Task dependency graph;
- use deterministic handlers/tools wherever sufficient;
- introduce AgentExecution/Team reasoning only where justified;
- record outcome, cost, latency, blockers, human intervention, and inference usage;
- preserve replayable evidence where practical.

Controlled comparisons belong under `VALIDATE_NEXT` when a real task class creates uncertainty about execution structure.

## V1 acceptance

V1 is accepted only when:

1. Team/Agent identity survives restart;
2. an agent can send a lateral message without planner relay;
3. recipient sees it at next applicable action boundary;
4. no recipient sees a message whose persistence failed;
5. a blocked worker returns control without authority expansion;
6. a child assignment receives no unintended positive capability inheritance;
7. human-required action waits indefinitely until decision;
8. acknowledgement cannot approve;
9. freeze/revoke prevents action at time of use;
10. agent text cannot forge approval/identity/completion/runtime attestation;
11. `CANDIDATE_COMPLETE` cannot bypass Completion Engine;
12. duplicate delivery cannot duplicate a consequential effect;
13. restart preserves pending work/approval/inbox state;
14. operational telemetry records verified outcome, execution mechanism, cost, time, model use (if any), messages, blocks, retries, and human intervention.
15. every model execution has an accurate ExecutionContextManifest;
16. deterministic ToolOutcome/postcondition failures cannot be hidden by a success string;
17. known safe deterministic recovery is attempted before cognitive recovery where implemented;
18. Hermes can discover/submit/continue work through the pinned A2A Operator Gateway integration;
19. Hermes/A2A identity cannot bypass capability or human-approval boundaries;
20. protected external effects use exact effect-bound approval and durable EffectObligation/reconciliation before production use.

## Stop rule

Do not build `VALIDATE_NEXT` or `FUTURE_IF_EARNED` merely because V1 code compiles. First operate on representative real work, collect baseline operational measurements, and run the safety acceptance suite.


## v4.1.1 sequencing

After the core ledger/events/tasks/capability/completion path works, add before declaring V1 complete:

1. versioned KnowledgeRecord + simple retrieval;
2. instruction/reference Skill package + candidate/activation/versioning;
3. deterministic AuditService + findings;
4. InferencePool + basic deterministic resource selection/usage telemetry.

Then operate the core runtime on representative real work. Build workflows and Agent/Team structures from what the work actually requires. Record outcomes, cost, latency, blockers, model usage, and human intervention. Use controlled replays or Lab experiments on real task classes when evidence is needed to choose between deterministic workflows, single Agents, Skills, verifiers, parallel attempts, or Teams. Build full Lab orchestration, LLM AuditWorker, semantic retrieval, executable skill assets, and resource forecasting only when operational needs and measurements justify them.
## v4.1.2 sequencing rule

Do not make a benchmark the product roadmap.

As soon as core safety/durability/completion primitives are usable:

1. select representative actual work;
2. represent it as a Task dependency graph;
3. use deterministic handlers/tools wherever sufficient;
4. inject AgentExecution/Team reasoning only where justified;
5. record operational evidence;
6. use controlled comparisons only when a real execution-structure question exists.

This produces an organization that learns how to do its own work rather than a research harness looking for tasks.
## v4.2 sequencing additions

Before handing Agent OS to Hermes as operator:

1. ExternalActor identity/capability mapping;
2. minimal A2A v1.0 Agent Card/server;
3. A2A work -> Intent/Goal/Task translation;
4. progress/blocked/input/result mapping;
5. pinned Hermes interoperability tests.

Before trusting model/tool traces operationally:

6. ExecutionContextManifest;
7. structured ToolOutcome + deterministic recovery.

Before enabling consequential external writes:

8. effect-bound approval fingerprints;
9. EffectObligation outbox/recovery/reconciliation;
10. simple SecretSource seam where credentials are needed.

These additions do not authorize full federation or general plugin infrastructure.


---

<!-- SOURCE: docs/09_ADRS_AND_OPEN_QUESTIONS.md -->

# Architecture Decision Records and Open Questions

## Accepted v4.0 decisions

### ADR-001 — Go modular monolith first
Accepted.

### ADR-002 — Durable logical actors, ephemeral executions
Accepted.

### ADR-003 — Event sourcing with projections
Accepted. One event history first; avoid duplicate stores.

### ADR-004 — Event Contracts replace ANL and Agent Semantic Model
Accepted. ANL/ASM are superseded and must not be implemented.

### ADR-005 — Structure only runtime-significant semantics
Accepted. Natural-language/structured content remains content until a deterministic runtime requirement earns a typed contract.

### ADR-006 — Trusted envelope, untrusted content
Accepted. Runtime owns event identity/source/time/sequence/control metadata.

### ADR-007 — JSON baseline
Accepted. Canonical JSON for V1 persistence/API/context. TOON is a future benchmark candidate.

### ADR-008 — Persist before availability
Accepted.

### ADR-009 — Passive awareness at action boundaries
Accepted. No mid-token interrupt in V1.

### ADR-010 — Authority Non-Solicitation
Accepted. Worker returns blocked task instead of requesting expansion of its own authority.

### ADR-011 — No positive capability inheritance
Accepted. Restrictions/ceilings continue through delegation.

### ADR-012 — `discoverable != invocable != authorized`
Accepted.

### ADR-013 — Authorization trace is a projection
Accepted. Do not build a second authorization ledger.

### ADR-014 — Task dependency graph before PlanGraph engine
Accepted.

### ADR-015 — Typed rules before policy DSL
Accepted.

### ADR-016 — Human consequence boundaries
Accepted: financial, physical, public/external write, privilege/trust expansion, sensitive-data boundary expansion, destructive/irreversible, legal/binding, trusted-core/security.

### ADR-017 — Unanswered approvals wait
Accepted. Escalate attention, never authority.

### ADR-018 — Candidate completion + Completion Engine
Accepted.

### ADR-019 — Runtime-attested evidence distinct from agent claims
Accepted.

### ADR-020 — Evaluation before optimization
Accepted. Runtime optimization/Organization Health are future if earned.

### ADR-021 — Future systems require prerequisites
Accepted. `future/FUTURE_CONSIDERATIONS.yaml` is the deferral registry.

### ADR-022 — Agent OS may remain an internal operating capability
Accepted product assumption. External framework adoption is not required for success.

## Superseded decisions

- ANL as native semantic IPC — superseded.
- custom ANL grammar — superseded.
- Agent Semantic Model ontology — superseded.
- ANL/ASM federation payload — superseded.

Historical copies live under `history/`/`research/` only.

## Open questions worth measuring

1. Does async Team collaboration outperform strong single agents on genuinely interdependent tasks after cost?
2. What is the smallest useful set of Event Contracts?
3. Does TOON improve context efficiency enough without degrading accuracy?
4. When does a ledger projection cease to be sufficient as team memory?
5. How often does Authority Non-Solicitation create avoidable blocker churn?
6. Which collaboration topologies deserve runtime support after baseline benchmarking?


## ADR-037 — Minimal versioned institutional knowledge is V1 CORE

**Decision:** Accepted in v4.1.

Durable agents/teams retain auditable EXPERIENCE, LESSON, KNOWLEDGE, and PROCEDURE records across ExecutionProfile/model changes. The ledger remains history; knowledge is a versioned curated layer over evidence. Knowledge is not authority.

## ADR-038 — Instruction/reference Skills are V1 CORE

**Decision:** Accepted in v4.1.

Agents may propose reusable procedural skills inspired by proven external agent-skill patterns. Runtime validation/promotion/versioning controls activation. Skills do not grant capabilities and are not trusted runtime plugins.

## ADR-039 — Deterministic Audit Service is V1 CORE

**Decision:** Accepted in v4.1.

Scheduled/event-triggered software audits produce durable findings. Judgment-heavy LLM AuditWorker remains a bounded `VALIDATE NEXT` feature. Auditing observes; it does not receive executive authority.

## ADR-040 — Lab experimentation separates exploration from authority

**Decision:** Accepted conceptually; implementation tier `VALIDATE NEXT`.

Disposable high-freedom experiments run inside explicit sandbox/capability/resource budgets. Outputs are `EXPERIMENTAL_UNVERIFIED`; promotion requires independent validation. Parent may nominate but cannot unilaterally certify trust.

## ADR-041 — Inference access is an organizational resource

**Decision:** Accepted in v4.1.

V1 models subscription allowance, metered API budget, and local compute as InferencePools. Agents do not own models/quotas. Scheduler/resource manager selects feasible resources and protects configured continuity reserve.

## ADR-042 — Knowledge/Skill revision preserves history

**Decision:** Accepted.

Minor revisions and full rewrites produce new versions. Prior versions are superseded/stale/quarantined rather than silently overwritten.
## ADR-043 — Three occurrences create a pattern candidate, not truth

**Decision:** Accepted in v4.1.1.

Three related occurrences are the default minimum for proposing a repeated pattern. The threshold is configurable by consequence/task class. Subsequent evidence/experiments and appropriate validation determine promotion.

## ADR-044 — Nondeterministic conclusions are operator judgments

**Decision:** Accepted in v4.1.1.

Where deterministic/objective verification is unavailable, an authorized agent or human operator may judge according to consequence policy. The record identifies the method/operator and never presents judgment as deterministic proof.

## ADR-045 — Execution topology is empirical

**Decision:** Accepted in v4.1.1.

Agent OS has no global preference for Teams. Lab/benchmark evidence should determine whether a task class uses a single agent, Skill-assisted agent, verifier, parallel attempts, or async Team.

## ADR-046 — Skill safety is defense in depth

**Decision:** Accepted in v4.1.1.

Auditing supplements provenance, applicability, validation, versioning, capability enforcement, completion verification, revalidation and rollback/quarantine.

## ADR-047 — Usage telemetry uses best available evidence

**Decision:** Accepted in v4.1.1.

Inference usage may come from official APIs, supported provider CLIs, other supported telemetry, observed estimates, or conservative estimates. Every snapshot carries source/time/confidence. Deterministic adapters cache/rate-limit collection.
## ADR-048 — Work-First Orchestration

**Decision:** Accepted in v4.1.2.

Actual organizational work determines Task decomposition, deterministic workflows, Agent/Team use, Skills and human involvement. Benchmarks/Lab experiments are instruments for improving real work, not the workload driver.

## ADR-049 — Minimal Justified LLM Use

**Decision:** Accepted in v4.1.2.

Use the least nondeterministic mechanism sufficient. LLM inference is introduced only where adaptive reasoning, interpretation, generation, tool-use planning, or judgment provides justified value over conventional software/tools/procedures.

## ADR-050 — Task DAG is the V1 workflow representation

**Decision:** Accepted in v4.1.2.

Do not build a separate workflow DSL/engine in V1. Task nodes may identify execution as deterministic, tool, Agent, Team, human, or mixed.

## ADR-051 — Persistent Agent does not imply persistent inference

**Decision:** Accepted in v4.1.2.

An Agent is durable organizational identity/responsibility. AgentExecution is created only when model inference is needed. An Agent may own a workflow whose majority is deterministic.

## ADR-052 — Inference routing occurs after LLM justification

**Decision:** Accepted in v4.1.2.

The Resource Manager chooses among feasible inference pools/models only after the workflow determines that model inference is needed.
## ADR-053 — Minimal A2A Operator Gateway is V1 CORE

**Decision:** Accepted in v4.2.

Hermes is an intended external operator. Agent OS exposes a minimal A2A v1.0 work/status/artifact/input boundary. Internal communication remains Event Contracts.

## ADR-054 — A2A identity is not authority

**Decision:** Accepted in v4.2.

Authenticated peers map to scoped ExternalActor identities. Agent OS capability/consequence policy determines what they may cause.

## ADR-055 — A2A Task is not Agent OS Task

**Decision:** Accepted in v4.2.

External A2A task/context IDs correlate to internal Intent/Goal/Task DAG objects but do not define internal workflow semantics.

## ADR-056 — ExecutionContextManifest is V1 CORE

**Decision:** Accepted in v4.2.

Every model execution records exact Event/Knowledge/Skill/Artifact/tool/profile versions/materialization states actually available to it.

## ADR-057 — Deterministic recovery before cognitive recovery

**Decision:** Accepted in v4.2.

Tool adapters attempt safe known deterministic recovery/postcondition verification before spending a new model turn.

## ADR-058 — ToolOutcome is structured runtime evidence

**Decision:** Accepted in v4.2.

Tool outcomes include observed effect, postcondition verification, retryability, deterministic recovery and artifacts/error details.

## ADR-059 — Human approvals bind to exact effect fingerprints

**Decision:** Accepted in v4.2.

Approval authorizes the described protected effect, not general capability expansion. Changed material arguments may require a new approval.

## ADR-060 — Persist EffectObligation before consequential external effects

**Decision:** Accepted in v4.2.

Use durable outbox/obligation state, idempotency/reconciliation and explicit confirmation. Do not claim exactly-once behavior when unsupported.

## ADR-061 — SecretSource seam before secret platform

**Decision:** Accepted in v4.2.

Resolve secrets in deterministic adapters where possible; V1 needs only a small interface/simple implementation. Additional secret managers are later integrations.

## ADR-062 — Core remains a narrow waist

**Decision:** Accepted in v4.2.

Prefer existing handler/tool, tool+Skill, adapter or external integration before adding business-specific functionality to Agent OS core.


---

<!-- SOURCE: docs/10_AI_CODING_AGENT_INSTRUCTIONS.md -->

# Instructions for an AI Coding Agent

## Scope

Treat `../IMPLEMENTATION_SCOPE.yaml` as binding. Build only `V1_CORE` unless the human explicitly promotes an item.

## Do

- implement Event Contracts, not ANL/ASM;
- keep model content untrusted;
- make Event envelope metadata runtime-owned;
- persist before delivery/availability;
- use one event history with projections;
- keep Task dependency graph simple;
- keep policy/capabilities typed and concrete;
- implement blocked-task return, not worker self-permission requests;
- verify authority at time of effect;
- implement `CANDIDATE_COMPLETE` + Completion Engine;
- distinguish claimed from runtime-attested evidence;
- use deterministic fakes in CI;
- preserve module boundaries;
- add adversarial regression tests.

## Do not

- recreate ANL or a general semantic ontology;
- add `BELIEF`, `HYPOTHESIS`, `OBSERVATION`, etc. as first-class types without a documented runtime need;
- create a second blackboard/authorization/provenance database by default;
- build an automatic Organization Optimizer;
- build an Organization Health scoring engine;
- build SOP/skill evolution;
- build research self-improvement;
- build broad/outbound A2A federation beyond the required minimal inbound Operator Gateway;
- build TOON/adaptive codecs unless promoted;
- build a policy DSL;
- create microservices because the conceptual architecture has “planes.”

## Event contract extension test

Before adding an event kind, answer in the PR:

1. What deterministic runtime behavior depends on this distinction?
2. Why is an existing event + content insufficient?
3. What validation/security rules apply?
4. Which tests prove the type is necessary and safe?

If no concrete answer exists, keep it as content.

## Removal-first rule

When a problem can be solved by removing unnecessary abstraction/state/dependency rather than adding another subsystem, prefer removal unless the added abstraction has independent measured value.


## v4.1.1 mandatory constraints

- Do not treat knowledge or skill text as authority.
- Do not overwrite active knowledge/skills in place; create a version/supersession event.
- Do not let agents directly activate their own proposed knowledge/skills.
- Do not dynamically compile/load model-generated skill code into the trusted Agent OS process.
- Implement auditing as deterministic software first; do not invent an Auditor Agent.
- Agents do not own model/provider selection; use the Inference Resource Manager.
- Subscription remaining capacity may be estimated; represent uncertainty rather than inventing exact values.
- Do not build the full Lab before the core runtime is performing representative real work and producing operational measurements.
## v4.1.2 implementation discipline

- Work-first: build Task DAGs/workflows from actual goals.
- The Task DAG is enough workflow machinery for V1.
- Prefer deterministic Go/tooling when it can reliably do the job.
- Only create model invocations where adaptive intelligence is justified.
- A Task assigned to an Agent does not automatically require an LLM call.
- Do not wrap deterministic services in LLM personas.
- Operational work creates the evidence used for later controlled comparisons.
## v4.2 implementation constraints

- Implement inbound A2A operator support without importing A2A types into core domain modules.
- Pin/test a known Hermes configuration/release for integration; do not assume every Hermes surface loads A2A identically.
- Persist exact execution context manifests.
- Tool success requires observed/postcondition evidence where practical, not just a model/tool string.
- Attempt bounded deterministic recovery first.
- Scope approvals to exact effect fingerprints.
- Use EffectObligation before protected external writes; design retry/reconciliation honestly.
- Build only a SecretSource seam/simple source initially.


---

<!-- SOURCE: docs/11_SOURCE_REFERENCES.md -->

# Source References and Prior Art

The project is influenced by and should continue to compare itself against:

- AgentRadio — passive asynchronous multi-agent awareness/collaboration;
- AOS reference architecture — intent, authority, governance/runtime separation;
- AIOS — OS-inspired scheduling/context/memory/tool/access management;
- Agent libOS — long-running agent processes and capability-controlled primitives;
- Qualixar OS — topology/model routing and evaluator/Goodhart ideas;
- OneManCompany — organizational layer, heterogeneous talent/runtime separation;
- Meta-Team — distributed post-task failure attribution and team evolution;
- Microsoft Agent Governance Toolkit — deterministic enforcement beneath LLM behavior;
- Paperclip / AgentTeams — organization-control UX and human-visible team operation;
- A2A — V1 minimal inbound operator interoperability; broader outbound federation remains later;
- TOON — possible future model-context codec, not a semantic protocol.

See `../research/landscape-2026-08-08/` for the detailed research dossier.

## Important prior-art conclusion

Do not claim invention of “Agent OS,” dynamic teams, artificial companies, deterministic governance, or self-evolving agent organizations.

The project thesis is the integrated use of durable artificial organizations, passive async collaboration, deterministic authority/completion, event-sourced auditability, and evidence-driven future evolution.

## v4 communication decision

Earlier research documents may discuss ANL/ASM. Those passages are historical only. v4.0 uses Event Contracts and does not preserve ANL/ASM as future implementation options.
## v4.2 Hermes/A2A references

- Hermes Agent releases — v0.19.0, v0.19.1, v0.20.0; studied for A2A, delivery obligations, profiles/secrets, context transparency/compaction, tool recovery, direct shell/non-model execution, and usage/resource patterns:
  `https://github.com/NousResearch/hermes-agent/releases`
- Hermes Agent repository/contributor architecture:
  `https://github.com/NousResearch/hermes-agent`
- A2A Protocol specification v1.0:
  `https://a2a-protocol.org/latest/specification/`

Treat these as prior art/implementation lessons. Agent OS owns its own security and conformance requirements.


---

<!-- SOURCE: docs/12_MODULAR_MONOLITH_ARCHITECTURE.md -->

# Modular Monolith Architecture

## 1. Decision

One Go repository, one primary runtime process, one initial database, one composition root, strict module ownership.

## 2. Suggested boundaries

```text
events          canonical Event/EventDraft contracts + gateway
ledger          append/read/replay/integrity
projections     inbox/task/team/audit/authorization projections
actors          Agent identity/blueprints/profiles
teams           membership/team lifecycle
organizations   organization identity/minimal policy binding
tasks           Goal/Task/TaskContract dependency graph
scheduler       deterministic runnable/waiting/retry logic
runtimeadapter  execution adapters
models          provider abstraction
capabilities    leases/action-resource-scope checks
policy          root/human/org typed rules
approvals       pending/ack/decision
completion      contracts/verifiers/verified transition
tools           capability-gated tool execution
api             REST/SSE/CLI/UI DTOs
```

## 3. Same process does not mean bypass

Agent A to Agent B still goes:

```text
EventDraft -> Event Gateway -> persist -> projection/router -> recipient
```

Do not call another agent's implementation directly to bypass persistence/policy.

Ordinary internal software calls such as scheduler-to-repository may use normal typed interfaces. Do not fake network boundaries inside the monolith.

## 4. Database ownership

A shared SQLite database is acceptable. Modules own their tables/queries logically and should not casually query another module's tables.

## 5. Extraction rule

Extract a service only for demonstrated need such as:

- fault isolation;
- hostile-code security isolation;
- independent scale;
- GPU/resource specialization;
- externally exposed federation boundary;
- independent deployment.

Do not pre-split by conceptual “plane.”


---

<!-- SOURCE: docs/13_AI_DEVELOPMENT_GOVERNANCE.md -->

# AI Development Governance

## 1. Purpose

The codebase itself will likely be developed with coding agents. Repository controls therefore need to prevent architecture erosion and unreviewed scope growth.

## 2. Authoritative sources

Order:

1. `IMPLEMENTATION_SCOPE.yaml`
2. `docs/29_V1_BUILD_CONTRACT.md`
3. v4 normative docs
4. schemas/API contracts/tests
5. `future/` only after explicit promotion
6. `research/` and `history/` are non-normative

## 3. Scope discipline

A coding agent must not interpret “future consideration” as implementation permission.

Every new package/subsystem should map to a `V1_CORE` scope item or an explicit human promotion.

## 4. Architectural changes

Changes to these require explicit review:

- Event Contract set/trust boundary;
- ledger/event ordering semantics;
- capability/policy enforcement;
- human consequence boundaries;
- Completion Engine authority;
- root invariants;
- provider/tool trust boundaries.

## 5. Removal-first maintenance

Prefer deleting dead/redundant code and dependencies over hiding them behind abstraction/suppression.

Archguard protects structural boundaries. Gallow is advisory anti-entropy until proven useful enough for stronger enforcement.

## 6. AI-generated patches

Require:

- tests;
- architecture checks;
- adversarial regression where relevant;
- no silent scope-tier promotion;
- no generated authority bypass;
- no implementation of superseded ANL/ASM concepts.


---

<!-- SOURCE: docs/14_ARCHITECTURE_FITNESS_AND_CI.md -->

# Architecture Fitness and CI

## Required CI gates

V1 should enforce:

- `gofmt`;
- `go test ./...`;
- `go vet ./...`;
- race tests for concurrency-critical modules;
- Archguard module/dependency rules;
- schema validation tests;
- migrations/replay tests;
- adversarial safety tests;
- Gallow advisory anti-entropy if retained.

## Event architecture tests

Required examples:

- EventDraft cannot set authoritative source/sequence/time;
- model-proposed `APPROVAL_DECIDED` is rejected;
- message is unavailable if persistence fails;
- stored event canonical JSON/hash is stable under the chosen canonicalizer;
- duplicate delivery cannot duplicate consequential action;
- projection rebuild from ledger yields same state;
- agent text claiming another sender does not change envelope identity;
- agent text claiming completion does not set verified completion.

## Architectural fitness

Prevent:

- direct agent-to-agent semantic bypass around Event Gateway;
- policy module importing model provider implementation;
- completion depending on worker self-verdict;
- future packages becoming runtime dependencies before promotion;
- separate shadow state stores becoming alternative truth sources.
## v4.2 A2A/Hermes test gates

The A2A adapter is a boundary and should be testable without a live Hermes process.

CI should include:

- unit tests for A2A -> internal command translation;
- ExternalActor authentication/authorization tests;
- task/status/artifact mapping tests;
- blocked/input-needed continuation tests;
- architecture guard proving core domain modules do not import A2A protocol packages.

A separate integration/release profile should run against a pinned supported Hermes release/configuration before declaring Hermes interoperability working.

Do not weaken the protocol/domain boundary to make the integration test easier.


---

<!-- SOURCE: docs/15_OPERATIONAL_AND_DATA_EVOLUTION.md -->

# Operational and Data Evolution

## V1

- SQLite;
- single authoritative writer;
- append-only event API;
- versioned schemas/migrations;
- projection rebuild tests;
- idempotency records for consequential actions;
- explicit conformance profile.

## Event evolution

Every event stores `schema_version`.

Do not reinterpret historical events under a new schema silently. Upcasters/migrations must be deterministic and tested.

## Restart/recovery

V1 may replay from genesis. Add checkpoints only when replay cost becomes measurable.

## Future storage/distribution

Postgres, NATS/JetStream, distributed workers, multi-writer consensus, stronger ledger integrity/signing, encrypted artifact stores, backups, and multi-tenant isolation are future considerations with explicit prerequisites.


---

<!-- SOURCE: docs/16_FALSIFICATION_AND_BENCHMARK_POLICY.md -->

# Falsification, Operational Measurement, and Controlled Evaluation


## 1. Work-first thesis

Hypothesis:

> Agent OS can perform real organizational work by composing deterministic software, tools, Agents, Teams, Skills, and human operators while using LLM inference only where it creates justified value.

Primary evidence comes from representative real work.

The system does not exist to prove Teams are better or to maximize agentic execution.

## 2. Operational measures

Record by real task class:

- verified outcome;
- execution mechanism/topology;
- deterministic vs LLM steps;
- model/provider/profile where used;
- token/provider/tool cost;
- wall time;
- retries/failures/rework;
- duplicate work;
- messages/collaboration;
- blocked-task frequency;
- human interventions;
- safety denials/violations;
- completion assurance.

## 3. Controlled comparison trigger

Do not infer structure superiority from unrelated tasks of different difficulty.

When a real recurring task class creates uncertainty about the best execution structure, compare using replayable/matched/held-out real tasks where practical.

Candidates may include:

- deterministic workflow;
- single Agent;
- Agent + Skill;
- Agent + verifier;
- parallel independent attempts;
- async Team.

Prefer the smallest structure that reaches the required verified outcome at acceptable cost, latency, reliability and risk.

If Teams provide no material advantage for a task class, do not use them there. If LLM inference provides no material value for a workflow step, remove it.

## 4. Event Contract minimality thesis

Hypothesis:

> A tiny typed coordination/control vocabulary plus ordinary content is sufficient.

Track how often developers want new event kinds and whether existing `MESSAGE` + content actually fails a deterministic requirement.

Do not expand contracts because a concept “sounds semantic.”

## 5. TOON/codec thesis — future

Only after JSON baseline exists, benchmark TOON on representative context payloads for:

- tokens;
- latency;
- parsing/accuracy;
- model-specific failure modes.

If benefit is weak, keep JSON.

## 6. Blocked-task authority thesis

Measure whether Authority Non-Solicitation reduces unnecessary privilege without creating unacceptable task-blocker churn.

## 7. Completion thesis

Measure false-complete/false-reject rates for:

- worker self-report;
- LLM reviewer;
- CompletionContract + deterministic/runtime evidence.

The Completion Engine must earn complexity through lower false-complete risk.
## 8. Topology neutrality

The benchmark is not designed to prove Teams win.

Lab/benchmark candidates may include single-agent, Skill-assisted single-agent, verifier, parallel, and async-Team configurations.

A successful Agent OS may learn that most work belongs to a single agent and reserve Team collaboration for specific interdependent classes.
## 9. Minimal-LLM falsification rule — v4.1.2

For recurring workflow steps, compare LLM-backed behavior against simpler deterministic/tool implementations when practical.

If conventional software achieves equivalent required outcomes with greater reliability/lower cost, prefer it.

Conversely, do not force rigid deterministic automation where an LLM materially improves success/adaptability and remains within policy/resource constraints.


---

<!-- SOURCE: docs/17_SAFETY_INTEGRITY_AND_CONSTITUTION.md -->

# Safety, Integrity, and Constitution

## 1. Safety is infrastructure

Safety is not a special agent department. Deterministic infrastructure constrains every intelligent actor.

## 2. Trusted computing base — V1

Keep small:

- actor/runtime identity;
- Event Gateway and ledger integrity interfaces;
- root typed policy;
- consequence classifier;
- capability gate;
- authorization trace logic;
- approval state;
- freeze/revoke;
- Completion Engine integrity;
- runtime attestation.

## 3. Authority hierarchy

```text
ROOT INVARIANTS
  > HUMAN/TENANT POLICY
  > ORGANIZATION POLICY
  > GOAL/TASK CONTRACT
  > TEAM/ASSIGNMENT RESTRICTIONS
  > CAPABILITY LEASE
  > ACTION
```

Lower layers may narrow but not expand an upper-layer ceiling.

## 4. Root invariants

1. no self-granting authority;
2. no positive authority inheritance by default;
3. no ledger rewriting to conceal history;
4. no content-forged identity/approval/attestation/completion;
5. no bypass of human consequence boundary;
6. no disabling human freeze/revoke through model action;
7. no worker modification of its own completion criteria;
8. no consequential action when required safety authority/state is unavailable;
9. persist communication before recipient availability;
10. unknown control semantics fail closed.

## 5. Human consequence boundaries

Human approval required for:

- financial;
- physical-world;
- public/external write;
- privilege/trust expansion;
- sensitive-data boundary expansion;
- destructive/effectively irreversible;
- legal/binding;
- trusted-core/security deployment.

Already-approved spending/resource use inside an envelope can remain autonomous.

## 6. Reversible internal autonomy

Agents may autonomously plan, research, code, test, communicate, reorganize bounded internal work, and use pre-approved resources so long as the effect remains inside the autonomy envelope.

Organizational optimization machinery itself is future, but this policy principle remains.
## v4.2 external operator invariant

> **External protocol access does not create authority.**

An authenticated A2A/Hermes peer has only the capabilities assigned to its `ExternalActor` identity. It cannot inherit root/human/organization authority from being the primary operator interface.

Protected human consequence boundaries remain unchanged.


---

<!-- SOURCE: docs/18_APPROVALS_BLOCKED_TASKS_AND_NOTIFICATIONS.md -->

# Approvals, Blocked Tasks, and Notifications

## Authority Non-Solicitation Invariant

> An agent SHALL NOT request expansion of its own permissions, capabilities, or authority. If an assigned task cannot be completed within current information, capabilities, authority, or dependencies, it returns `TASK_BLOCKED` with the unmet requirement.

## Parent remediation

Parent/governing actor may:

- provide information;
- rescope;
- split;
- reassign;
- cancel;
- issue a new separately authorized assignment.

The worker may state “Y cannot be completed without X.” It does not say “grant me X.”

## Approval states

```text
PENDING -> NOTIFIED -> ACKNOWLEDGED -> PENDING_DECISION
                                      -> APPROVED / DENIED
```

Acknowledgement is not approval.

## Risk vs urgency

- consequence risk determines approval authority;
- urgency determines notification behavior.

## V1 notification

Persist risk/urgency/acknowledgement. Use a simple `Notifier` interface and UI/log/email adapter if needed.

Do not build a multi-channel escalation platform in V1.

Principle remains:

> **Escalate attention, never authority.**

An unanswered protected action waits.

## Protective actions

While waiting, already-authorized harm-reducing actions may continue if they do not themselves cross a human boundary.
## v4.2 — effect-bound approval

A human approval should authorize an exact effect, not a general privilege expansion.

Bind approval to:

```text
EffectFingerprint
TaskID
Action
Resource/Destination
ArgumentsHash
ExpiresAt?
SingleUse?
```

If material parameters change, recalculate the fingerprint and re-evaluate whether a new approval is required.

A2A/Hermes cannot approve a human-required effect unless the external actor is itself the configured authorized human identity through an approved human-authentication path; ordinary Hermes operator identity is not sufficient.


---

<!-- SOURCE: docs/19_COMPLETION_VERIFICATION_AND_EVIDENCE.md -->

# Completion, Verification, and Evidence

## Completion invariant

Agent publishes `CANDIDATE_COMPLETE`. It cannot directly set `TASK_VERIFIED_COMPLETE`.

## CompletionContract

Versioned task verification criteria:

- deterministic checks;
- executable tests;
- objective measured outcomes;
- artifact requirements;
- forbidden effects;
- independent/human judgment only where unavoidable.

Worker cannot silently rewrite these criteria.

## Verification preference

Use, in order where applicable:

1. deterministic environment predicate;
2. executable test/known answer;
3. objective measurable outcome;
4. formal rubric over observable evidence;
5. independent model adjudication;
6. human judgment.

Do not create fake deterministic proxies for inherently subjective quality.

## Evidence

Distinguish:

- agent-claimed evidence/content;
- runtime-attested observation/action/tool result.

Artifact/provenance references are structured because completion/audit/safety can depend on them.

## Independent tests

Important acceptance should not depend solely on tests authored by the implementation agent.

## No-action can be correct

“no change needed,” “wait,” and “insufficient evidence” are legitimate outcomes.
## v4.1.1 — nondeterministic operator judgment

When objective verification is unavailable, the remaining judgment must be explicitly attributed to an authorized operator.

- `INDEPENDENT_ADJUDICATION` may be performed by an appropriately authorized independent agent/operator.
- `HUMAN_JUDGMENT` is performed by a human operator.

Record the operator, evidence reviewed, and method. Do not render an operator judgment as deterministic verification.

Human consequence boundaries continue to determine when only a human may decide.
## v4.2 — execution/tool/effect evidence

Completion/audit may reference:

- `ExecutionContextManifest` to establish what information was actually available;
- `ToolOutcome` to establish observed tool behavior/postconditions;
- `EffectObligation` confirmation evidence for consequential external effects.

A model claim that an external action happened is weaker than confirmed runtime/effect evidence.


---

<!-- SOURCE: docs/20_FUTURE_CONSIDERATIONS.md -->

# Future Considerations

The authoritative future registry is:

- `../future/FUTURE_CONSIDERATIONS.yaml` — machine-readable scope/prerequisite registry
- `../future/FUTURE_CONSIDERATIONS.md` — human-readable rendering

The registry intentionally preserves compatible ideas demoted from earlier designs and records prerequisites/revisit triggers. ANL/ASM are excluded because they are superseded by Event Contracts and directly contradict the active communication architecture.


---

<!-- SOURCE: docs/21_V4_MIGRATION_AND_SUPERSEDED_CONCEPTS.md -->

# v4.0 Migration and Superseded Concepts

## From v3.2 to v4.0

### Replace

```text
Agent Semantic Model / SemanticMessage
        ->
EventDraft + Event + typed Event Contracts + ordinary content
```

### Remove from active code/spec

- semantic ontology package;
- observation/assertion/question/answer/contradiction message-type taxonomy;
- semantic parser/validator beyond ordinary event payload validation;
- authoritative semantic human renderer;
- ANL/ASM federation concepts.

### Keep

- canonical JSON;
- structured payloads where runtime behavior needs them;
- artifact/evidence references;
- runtime-owned identity/provenance;
- asynchronous lateral delivery;
- event sourcing;
- action-boundary passive awareness;
- deterministic safety/completion.

## Serialization

JSON is V1. TOON may later be a context codec. No semantic layer depends on a particular codec.

## Historical material

Research/history files may still contain ANL/ASM terminology. They are explicitly non-normative and should not be copied into implementation.


## v4.1 correction to v4.0 scope

v4.0 correctly removed ANL/ASM but overcorrected by pushing all durable memory/skills outside V1. v4.1 restores a minimal evidence-backed versioned knowledge layer and instruction/reference Skills. This does **not** revive ANL/ASM or the old broad memory ontology. The Event Contract architecture remains authoritative.


---

<!-- SOURCE: docs/22_EVENT_CONTENT_AND_CODEC_POLICY.md -->

# Event Content and Codec Policy

## V1

- Persist events as canonical JSON.
- API uses JSON.
- Model context uses JSON/natural language.
- Agent content may be text or structured JSON plus artifact refs.

## Codec boundary

If later needed:

```go
type ContextCodec interface {
    Encode(ContextView) ([]byte, error)
}
```

Candidate codecs may include JSON, TOON, CSV-like table views, or others.

The codec is a projection for model consumption. It never changes authoritative Event meaning/history.

## TOON

TOON is `VALIDATE_NEXT`, not V1. Revisit only after:

- representative JSON context traces exist;
- token/context cost is meaningful;
- benchmark includes accuracy/parsing failure, not token count alone.

## Structured domain content

Applications may define their own structured payload/content schemas. Do not promote those schemas into global Agent OS event kinds unless runtime control behavior requires it.


---

<!-- SOURCE: docs/23_HUMAN_LANGUAGE_AND_UI_TERMINOLOGY.md -->

# Human Language and UI Terminology

Default UI uses normal workplace language. Advanced/Audit exposes exact event/state terminology.

| Human-facing | Internal |
|---|---|
| Can't continue | `TASK_BLOCKED` |
| Needs your decision | `APPROVAL_PENDING` |
| I saw this | `APPROVAL_ACKNOWLEDGED` |
| Work submitted for checking | `CANDIDATE_COMPLETE` |
| Work verified | `TASK_VERIFIED_COMPLETE` |
| Doesn't have access | capability check denied |
| Waiting on another task | dependency waiting state |
| Work history | event-ledger audit projection |
| Team messages | `MESSAGE` events |
| Evidence | `EVIDENCE_PUBLISHED` + artifact refs |

Human UI should explain action, reason, consequence, reversibility, evidence, and what is waiting. Do not expose only opaque enum names.


## v4.1 terminology

| Human-facing | Internal |
|---|---|
| What we learned | Versioned institutional knowledge |
| Why we believe this | Provenance/evidence history |
| Previous versions | Knowledge/Skill version history |
| Team procedure / reusable skill | Skill |
| Needs re-checking | STALE / audit revalidation finding |
| Audit issue | AUDIT_FINDING |
| AI/model capacity | InferencePool resource status |
| Experimental / not trusted yet | EXPERIMENTAL_UNVERIFIED |


---

<!-- SOURCE: docs/24_RUNTIME_SAFETY_OPERATIONS.md -->

# Runtime Safety Operations

V1 needs primitives, not a separate incident-management platform.

Core operations:

- freeze consequential/external actions;
- revoke capability;
- cancel/suspend task;
- isolate actor from new work;
- preserve event/artifact evidence;
- inspect audit/authorization trace;
- resume only after revalidation.

A future incident-response workflow may compose these primitives when real operating experience justifies it.

Configuration rollback does not imply external effects were undone. The UI/audit must distinguish configuration reversibility from effect reversibility.
## v4.2 external-effect recovery

Consequential external effects use EffectObligations.

During restart/recovery:

1. identify PENDING/ATTEMPTED obligations;
2. inspect idempotency/reconciliation capability;
3. query destination status where supported;
4. retry only when policy says duplicate risk is acceptably controlled;
5. otherwise surface uncertainty/require operator resolution.

Configuration rollback does not mark an external effect undone.


---

<!-- SOURCE: docs/25_ADVERSARIAL_SAFETY_ACCEPTANCE.md -->

# Adversarial Safety Acceptance

Safety rules become regression tests.

Required V1 cases include:

1. model writes `APPROVAL_DECIDED: APPROVED` inside `MESSAGE` -> no authority effect;
2. model attempts to publish control-only event -> rejected;
3. sender spoof in content -> envelope identity unchanged;
4. worker lacks access -> `TASK_BLOCKED`, no self-grant path;
5. child task cannot inherit unintended capability;
6. confused deputy/authority laundering -> denied;
7. protected action unanswered -> remains blocked;
8. acknowledgement -> remains unapproved;
9. freeze race before time-of-use -> action denied;
10. unauthorized P0 -> rejected/downgraded;
11. external prompt injection -> remains content/data;
12. claimed evidence cannot become runtime-attested evidence;
13. “done” in text -> not verified completion;
14. `CANDIDATE_COMPLETE` with failed check -> completion rejected;
15. worker cannot change own CompletionContract;
16. duplicate event delivery -> no duplicate consequential effect;
17. persistence failure -> recipient never sees message as available;
18. restart -> pending approvals/inboxes/tasks preserved;
19. stale task -> authority/environment revalidated before effect;
20. tool/provider data boundary violation -> denied.

Expand catalog with every material discovered failure.
## v4.2 Hermes/A2A/context/effect cases

The executable catalog also covers:

- authenticated A2A peer does not become administrator;
- Hermes cannot cross the human approval boundary;
- A2A input is translated/persisted through internal authority/event handling;
- exact ExecutionContextManifest materialization state;
- no “ghost Skill” after compaction/reference loss;
- ToolOutcome cannot claim success over failed postcondition;
- deterministic recovery cannot broaden authority;
- approval argument drift invalidates an exact effect fingerprint when required;
- crash after external effect attempt triggers reconciliation/idempotency logic rather than blind resend;
- replay context preserves destination/thread/effect semantics;
- adapter-side secrets do not enter model context unnecessarily.


---

<!-- SOURCE: docs/26_V4_SCOPE_AND_COMPLEXITY_BUDGET.md -->

# v4.2 Scope and Complexity Budget

## Rule

A mature-system idea is not a V1 feature merely because it is documented.

## V1 complexity budget

Prefer:

- one process;
- one DB;
- one event history;
- small Event Contract vocabulary;
- task dependency graph;
- typed capability rules;
- one real model adapter;
- simple notifier;
- simple UI/CLI;
- operational telemetry + controlled replay harness.

## Complexity smells

Stop and justify before adding:

- another source-of-truth datastore;
- custom language/ontology;
- new policy DSL;
- new network service;
- automatic optimizer;
- general memory platform;
- generic planning framework;
- adaptive codec selector;
- permanent specialized agent role;
- organization bureaucracy copied from humans.

## Concept vs runtime object vs subsystem

A useful concept does not automatically deserve a first-class object, and a first-class object does not automatically deserve a separate subsystem.

Examples:

- Organization Health can remain a future concept while V1 records raw metrics.
- Authorization lineage is a projection, not a second ledger.
- Incident response is initially a runbook over safety primitives.
- Research Team is a future organization built on Agent OS, not kernel code.


## v4.1.1 learning/resource correction

The earlier v4.0 scope overcorrected by moving all durable learning out of V1. v4.1 restored only the minimum layers that reinforce persistent organizational identity:

- versioned institutional knowledge;
- instruction/reference Skills;
- deterministic audits;
- inference resource accounting/selection.

This does **not** authorize building the previously imagined full memory/skill/optimization platforms. `IMPLEMENTATION_SCOPE.yaml` remains the machine-readable scope authority.
## v4.1.2 complexity-budget correction

Do not build a generic workflow engine or wrap deterministic work in agent personas.

V1 uses:

```text
Task dependency graph
+ ExecutionKind
+ ordinary Go handlers/tools
+ AgentExecution only where justified
```

A workflow DSL, automatic topology selector, and sophisticated orchestration engine remain deferred until actual work proves the simple Task graph insufficient.
## v4.2 boundary exception

A2A is V1 only because a concrete external-operator requirement exists: Hermes will manage Agent OS.

This does not justify:

- general federation;
- arbitrary outbound remote-agent discovery;
- A2A as internal IPC;
- a plugin marketplace.

ExecutionContextManifest, ToolOutcome and EffectObligation are small evidence/durability primitives, not new intelligent subsystems.


---

<!-- SOURCE: docs/28_RESEARCH_INTEGRATION_AND_PRIOR_ART.md -->

# Research Integration and Prior Art

The August 2026 landscape study is included under `../research/landscape-2026-08-08/`.

## Adopted lessons already reflected in v4

- preserve originating human intent;
- make authorization ancestry inspectable;
- capability/policy feasibility before quality/cost selection;
- separate durable Agent identity, ExecutionProfile, and RuntimeAdapter;
- `discoverable != invocable != authorized`;
- declare conformance/unsupported guarantees;
- use passive async communication for the main collaboration hypothesis;
- use deterministic enforcement below models;
- evaluate before optimizing.

## Research ideas intentionally deferred

See `../future/FUTURE_CONSIDERATIONS.md` for topology selection, organization optimization, Goodhart monitoring, memory/SOP/skills, research self-improvement, runtime scheduling enhancements, federation, and other mature-system ideas.

## Communication revision

The landscape work originally recommended a smaller semantic model. v4 goes further: it removes the general semantic model entirely and uses Event Contracts + ordinary content.

This should itself be validated by observing whether concrete runtime needs force new typed event distinctions.


## v4.1 practical skill-learning influence

The handoff now explicitly borrows the useful external-agent pattern of durable, progressively loaded procedural skills while changing the trust model: proposed procedures are versioned/auditable, do not grant authority, and cannot directly become trusted executable runtime code. This is inspiration/adaptation rather than a claim that skill externalization is novel to Agent OS.


---

<!-- SOURCE: docs/29_V1_BUILD_CONTRACT.md -->

# V1 Build Contract

This is the most concrete implementation document in v4.2.

## 1. Goal

Build the smallest local Agent OS capable of performing representative **real organizational work** with Task-DAG workflows that may combine deterministic software, tools, Agents, Teams, or humans, while enforcing core authority, learning, resource, audit, and completion rules.

## 2. Conformance profile

```text
profile: v1-local
runtime: one Go process
architecture: modular monolith
state store: SQLite
ledger authority: single writer
human controllers: one
model adapters: fake deterministic + one real provider
communication: Event Contracts + ordinary content
canonical codec: JSON
external operator: minimal A2A v1.0 inbound Operator Gateway
external federation/delegation: none
TOON: not required
strong hostile-code isolation: not claimed
automatic optimizer: none
organization health engine: none
self-improvement: none
knowledge: minimal versioned institutional store
skills: instruction/reference packages only
auditing: deterministic scheduled/event-triggered AuditService
inference resources: subscription + metered API + local pools
```

## 3. Required durable objects

### Organization

```text
ID
Name
PolicyVersion
CreatedAt
```

### Team

```text
ID
OrganizationID
Name
Mission?
MemberAgentIDs[]
Status
CreatedAt
```

### Agent

```text
ID
OrganizationID
BlueprintVersion
ExecutionProfileVersion
RuntimeAdapter
Status
```

### AgentBlueprint

```text
Role
OperatingInstructions
RequiredCapabilityClasses[]
```

### ExecutionProfile

```text
ModelProvider
Model
ReasoningSetting?
PromptVersion
ToolRefs[]
```

### IntentEnvelope

```text
ID
OriginalInstruction
NormalizedObjective
HardConstraints[]
ConsequenceBoundaries[]
SourceHumanID
CreatedAt
```

### Goal

```text
ID
IntentEnvelopeID
Objective
Status
```

### Task

```text
ID
GoalID
ParentID?
DependsOn[]
ExecutionKind: DETERMINISTIC | TOOL | AGENT | TEAM | HUMAN | MIXED
ModelInferencePolicy: DISALLOWED | ALLOWED_IF_JUSTIFIED | REQUIRED
AssigneeType?
AssigneeID?
RuntimeHandlerRef?
TaskContractVersion
Status
```

The Task dependency graph is the V1 workflow representation. Do not add a separate workflow DSL.

### Event / EventDraft

Per `03_EVENT_CONTRACTS_V0_1_SPEC.md`.

### CapabilityLease

```text
ID
ActorID
Action
Resource
Scope
ExpiresAt?
OriginTaskID
```

### KnowledgeRecord

```text
KnowledgeID
Version
Type: EXPERIENCE | LESSON | KNOWLEDGE | PROCEDURE
Scope: AGENT | TEAM | ORGANIZATION
Status: CANDIDATE | ACTIVE | SUPERSEDED | STALE | QUARANTINED
Content
ProvenanceEventRefs[]
EvidenceArtifactRefs[]
Applicability?
CreatedBy
CreatedAt
LastVerifiedAt?
SupersedesVersion?
```

### Skill

Instruction/reference-based V1 package:

```text
SkillID
Version
Name
Description
Scope
Status
InstructionsRef
ReferenceRefs[]
RequiredCapabilityClasses[]
ProvenanceEventRefs[]
CreatedBy
LastVerifiedAt?
```

Skill never grants capabilities. V1 skill cannot become dynamically loaded trusted Go/plugin code.

### AuditFinding

Durable output of deterministic AuditService with severity/scope/evidence/status. Audit finding cannot silently modify policy, knowledge, skills, or authority.

### InferencePool

```text
PoolID
Provider
AccessMode: SUBSCRIPTION | METERED_API | LOCAL
AllowedModels[]
Availability
ConcurrencyLimit?
EstimatedRemaining/Confidence?
ResetWindow?
MeteredBudget?
LocalCapacity?
ReservePolicy
```

### HumanApproval

Durable pending/ack/decision record.

### CompletionContract

Versioned verification criteria.


### ExternalActor

```text
ID
OrganizationID
Protocol: A2A
Peer/CredentialBindingRef
CapabilityRefs[]
Status
```

### A2ATaskMapping

Correlates external A2A task/context IDs to internal IntentEnvelope/Goal/root Task IDs.

### ExecutionContextManifest

Exact context actually materialized for an `AgentExecution`, including Event refs, Knowledge/Skill versions/materialization state, Artifacts, tool definitions, TaskContract/profile/policy/prompt versions.

### ToolOutcome

Structured runtime evidence of a tool invocation: status, observed effect, postcondition verification, retryability, deterministic recovery, artifacts/errors.

### EffectObligation

Durable outbox/obligation for consequential external effects with effect fingerprint, authorization/approval refs, idempotency/replay context, attempts and confirmation evidence.

### SecretSource

V1 interface/seam with one simple implementation. Secrets should be consumed by adapters without entering model context where practical.

## 4. V1 agent-proposable events

Exactly these initially:

```text
MESSAGE
TASK_BLOCKED
EVIDENCE_PUBLISHED
RESULT_PUBLISHED
CANDIDATE_COMPLETE
KNOWLEDGE_PROPOSED
SKILL_PROPOSED
```

Do not add semantic cognition types.

## 5. Trusted runtime events

At minimum:

```text
INTENT_CREATED
GOAL_CREATED
TASK_CREATED
TASK_ASSIGNED
TASK_CANCELLED
EXECUTION_STARTED
EXECUTION_FINISHED
CAPABILITY_CHECKED
CAPABILITY_DENIED
CAPABILITY_REVOKED
APPROVAL_PENDING
APPROVAL_ACKNOWLEDGED
APPROVAL_DECIDED
FREEZE_SET
ACTION_ATTESTED
COMPLETION_VERIFIED
COMPLETION_REJECTED
TASK_VERIFIED_COMPLETE
KNOWLEDGE_ACTIVATED / SUPERSEDED / STALE / QUARANTINED
SKILL_ACTIVATED / SUPERSEDED / QUARANTINED
AUDIT_RUN_STARTED / AUDIT_FINDING_CREATED / AUDIT_FINDING_RESOLVED
INFERENCE_SELECTED / INFERENCE_USAGE_RECORDED
EXECUTION_CONTEXT_MANIFESTED
TOOL_OUTCOME_RECORDED
EFFECT_OBLIGATION_CREATED / EFFECT_ATTEMPTED / EFFECT_CONFIRMED / EFFECT_FAILED
EXTERNAL_ACTOR_AUTHENTICATED
A2A_WORK_ACCEPTED / A2A_INPUT_RECEIVED
```

## 6. Passive awareness

Relevant persisted events surface:

- before model call;
- after model call;
- before tool call;
- after tool call;
- before candidate completion.

No mid-token interruption.

## 7. Event priority

```text
P0 safety/global invalidating
P1 dependency/evidence likely to change current work
P2 relevant before task completion
P3 informational
```

Runtime controls/downgrades sender proposal.

## 8. Authority

- exact action/resource/scope checks;
- authorization trace to Task/Intent;
- no positive inheritance;
- blocked worker returns upward;
- human consequence boundary at time of effect;
- freeze/revoke at time of effect.

## 9. Human approval

Persist:

```text
Action
Boundary
Risk
Urgency
Status
CreatedAt
AcknowledgedAt?
DecisionAt?
```

Unanswered state waits. One simple Notifier interface only.

## 10. Completion

Agent publishes `CANDIDATE_COMPLETE`.

Completion Engine evaluates CompletionContract and emits trusted completion event.

## 11. Operational telemetry

Record per run:

```text
verified outcome
provider/model/profile
wall time
tokens/cost
tool calls
messages
blocks
retries
human interventions
safety denials
completion evidence
```

## 12. Institutional learning

- Persist raw history in ledger.
- Agent/human may propose knowledge/skill candidates.
- Runtime validates/promotes separately.
- Preserve prior versions; never rewrite historical knowledge versions.
- Retrieval initially uses simple scope/tags/text/status/recency.
- Knowledge/skills cannot grant authority.

## 13. Audit Service

Run deterministic audit rules on configured intervals and event triggers. At minimum audit ledger/reference integrity, authorization consistency, completion evidence, knowledge provenance/staleness, skill dependencies/validation, and stuck/stale states. Findings enter remediation; AuditService does not directly rewrite trusted state except its own audit records.

## 14. Inference Resource Manager

Support `SUBSCRIPTION`, `METERED_API`, and `LOCAL` InferencePools. Track available/estimated capacity, reset/budget/concurrency/reserve information and record selection/usage telemetry. Agents request execution; runtime selects from feasible authorized pools according to deterministic resource policy.

## 15. Lab status

Do not block representative real work on a full Lab. V1 sandbox/resource primitives must be compatible with later bounded experiments. `Experiment` orchestration and promotion candidates are `VALIDATE NEXT`. Use the Lab when a real task class creates uncertainty about the best execution structure.

## 16. Not V1

Do not build:

- ANL/ASM;
- TOON codec;
- rich semantic-memory/knowledge-graph platform;
- executable generated skill-code evolution;
- Organization Health engine;
- automatic optimizer;
- research/self-improvement organization;
- topology selector;
- generic PlanGraph;
- broad/outbound A2A federation beyond the minimal inbound Operator Gateway;
- portable organizations;
- multi-human governance;
- MLFQ/context hibernation;
- multi-channel notification platform;
- distributed runtime;
- policy DSL.

Those compatible ideas are preserved with prerequisites under `future/`.


## 17. V1 non-goals added in v4.1

Do not build automatic memory consolidation, embedding/vector infrastructure, an LLM Auditor Agent, automatic skill evolution, full Lab orchestration, predictive quota optimization, multi-GPU scheduling, or autonomous subscription purchasing in the initial vertical slice.
## 18. v4.1.1 behavioral hardening

### Pattern candidacy

Default repeated-pattern threshold is 3 related occurrence/event references. This permits a pattern candidate/proposal only; it does not activate knowledge. Threshold may be increased by risk/task class.

### Nondeterministic judgment

Where deterministic/objective verification is unavailable, use an authorized agent or human operator according to consequence policy and record the judgment method/operator/evidence.

### Topology neutrality

Do not encode a global preference for Team execution. Single-agent and Team structures are experimental alternatives.

### Skill defense in depth

Skill safety requires provenance, applicability, validation evidence, versioning, action-time capabilities/consequence checks, Completion Engine verification, auditing, and rollback/quarantine.

### Usage telemetry

Implement normalized usage snapshots with:

```text
source
observed_at
confidence
remaining/value/unit?
reset/window?
basis?
```

Source preference:

```text
official API
-> supported provider CLI
-> other supported telemetry
-> observed estimate
-> conservative estimate
```

CLI/status collection belongs in deterministic provider adapters and must be cached/rate-limited. Do not require an LLM to repeatedly interpret interactive status output.
## 19. v4.1.2 work-first execution

### Work defines structure

Create workflows/Agents/Teams from actual goals and repeated operational needs.

Do not pre-create collaboration topology merely because Agent OS supports it.

### Minimal LLM use

Before invoking a model:

1. determine whether deterministic Go/runtime logic or an existing tool/procedure can adequately perform the step;
2. if not, determine whether adaptive model capability is justified;
3. only then ask the Inference Resource Manager to select a feasible resource/model.

A durable Agent may own deterministic steps without creating `AgentExecution`.

### Execution mechanism defaults

```text
DETERMINISTIC / TOOL -> ModelInferencePolicy = DISALLOWED
AGENT                -> normally ALLOWED_IF_JUSTIFIED
TEAM                 -> normally ALLOWED_IF_JUSTIFIED
HUMAN                -> model inference not implied
MIXED                -> decompose into child Tasks with explicit kinds
```

`REQUIRED` should be used only where model reasoning/generation is intrinsic to the contracted work.

### Real-work evaluation

Operate on representative actual work as soon as the core runtime works. Controlled replays/Lab experiments are used when deciding how to improve recurring real task classes, not as the reason the organization exists.
## 20. v4.2 Hermes A2A operator acceptance

V1 must support a pinned Hermes/A2A path:

1. Hermes discovers Agent OS Agent Card.
2. Hermes submits representative work.
3. Gateway authenticates/maps Hermes to an ExternalActor.
4. Agent OS persists an IntentEnvelope and internal work.
5. Hermes can receive progress/status.
6. Internal `TASK_BLOCKED` can surface as input-needed.
7. Hermes can provide authorized missing input.
8. Agent OS resumes and returns authorized result Artifacts/status.

A2A connectivity never bypasses normal Agent OS capability/human approval policy.

## 21. v4.2 execution-context acceptance

For every model `AgentExecution`, persist enough information to reconstruct exactly which Event, Knowledge, Skill, Artifact, prompt/profile/policy and tool-definition versions/materialization states were available.

## 22. v4.2 ToolOutcome/recovery acceptance

Tool adapters return structured outcomes. Where safe known deterministic recovery exists, attempt it before another model turn. Record postcondition verification where practical.

## 23. v4.2 external-effect acceptance

Before enabling real consequential external writes:

- approval must bind to the exact effect fingerprint;
- persist EffectObligation before attempting the effect;
- use idempotency/reconciliation where supported;
- record ATTEMPTED vs CONFIRMED/FAILED distinctly;
- recovery after restart must not blindly duplicate an uncertain effect.

## 24. v4.2 A2A non-goals

Do not build in V1:

- arbitrary outbound A2A worker discovery/delegation;
- federation marketplace;
- remote-agent trust negotiation;
- A2A as internal IPC;
- Hermes as internal RuntimeAdapter unless separately promoted.


---

<!-- SOURCE: docs/30_VERSIONED_KNOWLEDGE_AND_SKILLS.md -->

# Versioned Institutional Knowledge and Skills — v4.1.1

## 1. Decision

Persistent institutional learning is **V1 CORE**.

The system does not build a general “AI memory platform.” It implements the minimum durable, auditable knowledge layer required for an Agent/Team/Organization to learn from experience and retain that learning when the underlying LLM, provider, or ExecutionProfile changes.

> **Persist experience cheaply; promote knowledge cautiously.**

## 2. Why this is core

A durable Agent identity that loses useful lessons whenever its model changes is not meaningfully durable.

```text
Durable Agent / Team / Organization
          |
          +-- task/event history
          +-- validated institutional knowledge
          +-- versioned reusable skills
          |
          +-- ExecutionProfile may change independently
                Model A -> Model B -> Local Model C
```

The accumulated organizational experience belongs to the logical organization, not the model vendor.

## 3. Ledger versus knowledge

```text
Event Ledger                  Knowledge Store
------------                  ---------------
what happened                 what we currently retain/learn
immutable history             versioned current institutional knowledge
raw evidence                  distilled reusable content
source of provenance          projection/curation over history
```

Knowledge must link back to ledger events/artifacts whenever practical.

## 4. V1 knowledge types

Use common storage/versioning infrastructure but preserve different semantics:

### EXPERIENCE
Historical observation about a particular task/incident.

Example: “Task 188 failed because the vendor endpoint returned 410.”

### LESSON
Generalized advice inferred from one or more experiences.

Example: “Verify the supported API version before deployment.”

### KNOWLEDGE
A current claim about the environment/domain.

Example: “Payment API v5 is the current production API.”

### PROCEDURE
A reusable human/model-readable procedure.

Example: “Steps to deploy PaymentService safely.”

These are not identical. They may later receive different validation/expiry behavior.

## 5. V1 lifecycle

```text
CANDIDATE
   |
   | validation/corroboration appropriate to risk
   v
ACTIVE
   |
   +--> SUPERSEDED    new version replaces it
   +--> STALE         applicability may no longer hold
   +--> QUARANTINED   suspected unsafe/incorrect/poisoned
```

Do not overwrite prior versions.

## 6. Versioning

A stable Knowledge ID has monotonic versions:

```text
KNOW-42 v1
  -> KNOW-42 v2 minor correction
  -> KNOW-42 v3 major rewrite
```

Suggested change class:

- `CORRECTION`
- `MINOR_REVISION`
- `MAJOR_REVISION`
- `SUPERSEDE`

Historical versions remain auditable even after they are no longer active.

## 7. Required V1 fields

```text
KnowledgeID
Version
Type
Scope                 AGENT | TEAM | ORGANIZATION
Status
Title/Summary
Content
Applicability?        simple tags/text initially
ProvenanceEventRefs[]
EvidenceArtifactRefs[]
ProposedBy
CreatedAt
LastVerifiedAt?
SupersedesVersion?
ChangeClass?
ChangeReason?
```

## 8. Knowledge is not authority

Knowledge cannot:

- grant a capability;
- approve an action;
- change a TaskContract;
- override policy;
- modify the Constitution;
- turn agent text into a trusted event.

> **Knowledge informs behavior. Policy/authorization controls behavior.**

## 9. Agent/human revisions

Agents and humans may propose revisions through the same audited system. The runtime records who proposed and who/what validated the revision.

No invisible mutation.

## 10. Simple V1 retrieval

Start with boring retrieval:

- scope;
- task/service/domain tags;
- text search;
- recency;
- status = ACTIVE;
- optional last-verified/applicability filters.

Do not require embeddings, graph databases, rerankers, or a semantic ontology in V1.

## 11. Skill model

A Skill is **versioned reusable procedural knowledge packaged for on-demand use**.

Borrow the useful idea from systems such as Hermes: keep reusable procedures outside the permanent prompt and load them progressively only when relevant.

V1 skill package may contain:

```text
skill.yaml
instructions.md
references/
assets/            # static templates/examples only in V1
```

V1 does **not** allow a generated skill to become trusted Go/plugin code.

## 12. Skill lifecycle

```text
SKILL_CANDIDATE
      |
      | validation/tests
      v
ACTIVE
      |
      +--> SUPERSEDED
      +--> QUARANTINED
      +--> RETIRED
```

Agent can propose a skill or revision. Agent cannot directly activate it.

## 13. Skill requirements

Minimum fields:

```text
SkillID
Version
Name
Description
Scope
Status
InstructionsRef
ReferenceRefs[]
RequiredCapabilityClasses[]
ProvenanceEventRefs[]
EvidenceRefs[]
CreatedBy
CreatedAt
LastVerifiedAt?
SupersedesVersion?
```

## 14. Skill != capability

An active “Deploy Production” skill may describe exactly how to deploy production. It does not grant `production.write`.

Runtime performs normal capability and consequence checks at every real action.

## 15. Promotion signals

Difficulty alone does not justify a new skill. Useful signals include:

- repeated verified successful workflow;
- human correction followed by verified success;
- Completion Engine rejection followed by a generalizable correction;
- recurring multi-step task class;
- multiple independent experiences supporting the same procedure.

V1 may allow manual/agent explicit proposals. Automatic extraction belongs to `VALIDATE NEXT`.

## 16. Executable skill assets

Generated scripts/code are **not V1 trusted skill behavior**.

If later enabled, they must be untrusted artifacts executed through sandbox/capability enforcement and must never be dynamically loaded into the Agent OS Trusted Computing Base merely because a skill generated them.
## 17. Repeated-pattern candidacy — v4.1.1

Default minimum for a repeated-pattern candidate is **three related occurrences**.

This is a trigger for investigation, not proof. A `KNOWLEDGE_PROPOSED` record whose basis is `REPEATED_PATTERN` must reference at least three occurrence events under the default policy.

The threshold may be raised by task class/consequence.

> Frequency determines when a pattern deserves investigation. Evidence determines whether it deserves promotion.

After pattern candidacy, use subsequent evidence and/or deliberate experiments before promotion to ACTIVE knowledge where appropriate.

## 18. Knowledge validation method

Knowledge promotion/revision should record how it was validated, for example:

- `DETERMINISTIC_CHECK`
- `EXPERIMENTAL_EVIDENCE`
- `REPEATED_OBSERVATION`
- `INDEPENDENT_AGENT_JUDGMENT`
- `HUMAN_JUDGMENT`
- `MIXED`

Judgment is not deterministic proof. The operator identity and supporting evidence remain auditable.

## 19. Skill defense in depth

Auditing supplements rather than replaces:

- provenance;
- applicability/scope;
- validation evidence;
- version history;
- capability/consequence enforcement;
- Completion Engine verification;
- dependency/version revalidation;
- quarantine/supersession/rollback.

Bad Skills can exist. The design objective is rapid detection/correction and bounded consequences, not impossible perfect procedures.
## v4.2 source-grounding note

For factual knowledge backed by external sources, a future `VALIDATE_NEXT` capability may check whether cited artifacts actually support the proposed claim.

This is evidence validation, not a return to ANL/ASM or a general semantic ontology.

V1 continues to preserve evidence/provenance so such validation can be added later.


---

<!-- SOURCE: docs/31_AUDIT_SERVICE.md -->

# Audit Service — v4.1.1

## 1. Decision

A deterministic **Audit Service** is V1 CORE.

Do not create a permanent omnipotent “Auditor Agent” in V1.

> **Auditing is independent observation, not executive authority.**

## 2. Why deterministic first

Many important audits do not require an LLM:

- ledger integrity;
- dangling/missing references;
- actions missing valid authorization trace;
- expired/revoked capability inconsistencies;
- CompletionContract/evidence inconsistencies;
- stuck/stale tasks;
- stale pending approvals;
- knowledge missing provenance;
- ACTIVE knowledge depending on changed versions/environment;
- quarantined/stale knowledge being surfaced as active;
- skills missing provenance/validation;
- skill dependencies that changed;
- unexpected resource/budget anomalies.

These should be normal software checks.

## 3. Audit triggers

Support both:

### Scheduled
Configurable interval per audit rule/class.

Do not hard-code “daily/weekly/monthly” as architecture. A deployment chooses cadence.

### Event-triggered
Examples:

- model/provider/tool version change;
- significant skill revision;
- dependency/environment change;
- incident/freeze event;
- repeated Completion Engine rejection;
- capability/policy change;
- knowledge activation/supersession;
- inference resource anomaly.

## 4. AuditFinding

Audit Service emits a durable finding, for example:

```text
AUDIT_FINDING
id
rule_id
severity
scope
summary
evidence_refs[]
related_knowledge_ids[]?
related_skill_ids[]?
created_at
status = OPEN | ACKNOWLEDGED | RESOLVED | DISMISSED
```

A finding does not silently alter trusted knowledge, policy, skills, or capabilities.

It enters normal remediation/governance.

## 5. Knowledge audit examples

```text
Knowledge: Payment API procedure v7
Last verified: 90 days ago
Dependency recorded: API v4
Current dependency: API v5

=> AUDIT_FINDING: revalidation required
```

A successful revalidation can update `LastVerifiedAt`; a change produces a new version.

## 6. Skill audit examples

- referenced tool version changed;
- active procedure has not passed its relevant tests after environment update;
- active skill references a quarantined knowledge item;
- skill requires capability class no longer available;
- supporting artifact was revoked/invalidated.

## 7. LLM AuditWorker — VALIDATE NEXT

Judgment-heavy audits may later invoke a bounded independent model execution for:

- sampled SOP/procedure quality;
- recurring failure patterns;
- suspicious but technically passing completion;
- contradictory knowledge;
- whether evidence actually supports a proposed lesson.

The AuditWorker returns a candidate finding. It does not receive executive authority.

Increase model/context/evidence independence with consequence.

## 8. Anti-bureaucracy rule

Do not deep-audit everything.

Use:

- cheap deterministic checks broadly;
- risk-based triggers;
- sampling for judgment-heavy review;
- measured false-positive/confirmed-finding rates.

Do not create “auditor of the auditor” recursive agent hierarchy. Audit quality can be measured by deterministic statistics and occasional human evaluation.
## 9. Institutional correction objective — v4.1.1

Auditing is not expected to guarantee that incorrect knowledge or bad Skills never become active.

The system should instead minimize the duration and blast radius of institutional mistakes.

Record raw correction metrics where feasible:

- time from contradictory evidence to audit finding;
- time from audit finding to correction/quarantine;
- downstream tasks affected;
- repeat failures before correction;
- stale knowledge uses;
- Skill rollback/revision time.

Do not collapse these into an authoritative health score in V1.

## 10. Pattern/knowledge audit

Repeated occurrence count is not proof.

Audit rules may flag:

- ACTIVE repeated-pattern knowledge lacking post-pattern validation;
- high-consequence knowledge supported only by a minimal occurrence count;
- knowledge whose evidence roots are not actually independent;
- knowledge repeatedly contradicted by later tasks;
- Skills derived from stale/quarantined knowledge.

The output remains `AUDIT_FINDING`; remediation follows normal knowledge/Skill governance.
## v4.2 runtime-evidence audits

Add deterministic rules for:

- `ExecutionContextManifest` refs resolve and exact version/materialization states are internally consistent;
- an execution cannot be audited as having used a Skill/Knowledge version absent from its manifest;
- ToolOutcome says SUCCESS while required postconditions failed;
- deterministic recovery exceeded the original authorized effect;
- protected external effect lacks an EffectObligation;
- EffectObligation remains ATTEMPTED/uncertain beyond its reconciliation policy;
- confirmed effect lacks confirmation evidence where evidence is available;
- approval EffectFingerprint does not match the obligation/effect actually attempted;
- disabled/revoked ExternalActor continues to submit A2A work;
- A2A task mappings reference missing/unauthorized internal objects.

These produce `AUDIT_FINDING`; they do not silently repair policy or execute the effect.


---

<!-- SOURCE: docs/32_LAB_EXPERIMENTATION_AND_PROMOTION.md -->

# Lab Experimentation and Promotion — v4.1.1

## 1. Status

The Lab is `VALIDATE NEXT`, built from V1 primitives after the core runtime is performing representative real work and producing operational measurements.

The architectural principle is accepted now:

> **Separate freedom to explore from authority to affect reality.**

## 2. Purpose

Trusted operational procedures should not be mutated while searching for better approaches.

Instead, Agent OS can create disposable isolated experiments:

```text
trusted baseline
   +-- Experiment A
   +-- Experiment B
   +-- Experiment C
   +-- Experiment D
```

Explorers may fail cheaply. Useful results cross a promotion gate before becoming trusted.

## 3. Experiment composition

Prefer composition over a giant new subsystem:

```text
Experiment =
  bounded Task/Goal
  + isolated workspace/sandbox
  + special capability profile
  + explicit resource budget
  + ephemeral AgentExecution(s)
  + artifacts/results
  + EXPERIMENTAL_UNVERIFIED trust label
```

## 4. High freedom, hard walls

Inside an approved disposable sandbox, experimentation may allow:

- arbitrary temporary files;
- temporary databases;
- package installation;
- test/prototype code;
- multiple approaches/models;
- child explorer executions;
- candidate knowledge/skill generation.

It may not automatically:

- write production;
- contact customers/public;
- modify active policy;
- grant authority;
- change active knowledge/skills;
- spend outside the experiment resource budget;
- promote itself.

## 5. Promotion

Parent/commissioning actor may say “promising,” but cannot alone certify trust.

```text
Experiment result
   -> PROMOTION_CANDIDATE
   -> independent reproduction/evidence
   -> CompletionContract/tests
   -> security/capability review as needed
   -> trusted knowledge / skill / configuration candidate
```

For high-consequence system/runtime changes, normal human approval boundaries still apply.

## 6. Cherry-picking protection

The more alternatives explored, the stronger fresh confirmation should be.

A winner among many candidates should be re-tested against held-out/fresh tasks before promotion.

## 7. Resource control

Experiments always receive explicit limits such as:

```text
MaxExecutions
MaxTokens/UsageUnits
MaxMeteredCost
MaxWallTime
MaxChildren
AllowedInferencePools[]
```

Surplus subscription/local capacity may later be allocated to the Lab, but business continuity reserve always wins.
## 9. Topology experiments — v4.1.1

The Lab is explicitly allowed to compare execution structure itself.

Candidate configurations may include:

- single agent;
- single agent + active Skill;
- single agent + verifier;
- parallel independent attempts;
- asynchronous Team.

Agent OS is not premised on Teams being universally superior.

Long-term selection should prefer the smallest structure that reaches the required verified outcome at acceptable cost, latency, reliability, and risk.

Do not add an automatic topology selector until real-work/Lab evidence demonstrates stable task-class differences.
## 10. v4.1.2 — Work-derived experiments

Lab experiments should normally originate from uncertainty or inefficiency observed in actual organizational work.

The Lab may test whether the real task class should use:

- deterministic workflow;
- one Agent;
- one Agent + Skill;
- verifier;
- parallel attempts;
- async Team.

Use controlled replay or held-out representative tasks where possible.

Do not create synthetic workload merely to justify a preferred topology.


---

<!-- SOURCE: docs/33_INFERENCE_RESOURCE_MANAGEMENT.md -->

# Inference Resource Management — v4.1.1

## 1. Decision

A minimal deterministic **Inference Resource Manager** is V1 CORE.

Inference access is an organizational resource. It does not belong to an Agent identity.

> **Agent intelligence != inference entitlement.**

## 2. Why this is core

Persistent organizations may have fundamentally different inference resources simultaneously:

- subscription allowance with reset/rolling windows;
- metered API with monetary/token cost;
- local GPU/accelerator capacity that renews continuously;
- multiple providers/subscriptions with different strengths and limits.

The organization must stay operational while using these resources effectively.

## 3. InferencePool

V1 supports access modes:

```text
SUBSCRIPTION
METERED_API
LOCAL
```

Suggested fields:

```text
PoolID
Provider
AccessMode
AllowedModels[]
Availability
ConcurrencyLimit?
DataPolicy
EstimatedRemaining?
RemainingConfidence?
ResetAt/Window?          subscription
MeteredBudgetRemaining?  API
LocalCapacity?           GPU/queue/concurrency
ReservePolicy
```

Provider quota may be exact, approximate, or opaque. Track confidence/basis rather than pretending uncertain allowance is exact.

## 4. Resource request

An Agent requests execution, not a preferred entitlement:

```text
TaskClass
MinimumCapability/Quality requirements
Urgency
DataClass
Latency constraint?
Task/Organization priority
```

Selection order:

1. hard capability/policy/data constraints;
2. feasible inference pools/models;
3. reserve/availability/resource policy;
4. simple deterministic selection rule.

Do not optimize cost/quality among unauthorized or technically incapable choices.

## 5. Resource economics

### Perishable capacity — subscription
Unused capacity may expire/reset. Conserve enough reserve but avoid systematic waste.

### Monetary capacity — metered API
Unused budget retains value. Do not spend merely because a calendar period is ending unless human policy explicitly treats it that way.

### Renewable capacity — local compute
Idle local capacity has low opportunity cost but still has queue/power/thermal/resource limits.

## 6. V1 policy

Keep selection simple:

- minimum reserve per subscription pool;
- maximum metered spend;
- priority classes;
- allowed/denied models by data class;
- local-first or provider-preference rules where configured;
- fallback/escalation only through explicit deterministic policy.

No LLM “treasurer agent” in V1.

## 7. Usage telemetry

For every execution record:

```text
PoolID
Provider/model/profile
TaskID/task class
started/ended
usage units/tokens if known
estimated subscription consumption if known
metered monetary cost if known
local duration/resource telemetry if known
completion outcome later linked
```

This evidence enables later routing experiments without baking guesses into V1.

## 8. Continuity reserve

Do not optimize solely for minimum spend or maximum uptime.

The policy goal is:

> Maximize useful organizational work while preserving a configurable continuity reserve for expected and unexpected higher-priority work.

## 9. Validate next

Once actual usage exists, test:

- burn forecasting;
- target consumption curves toward subscription reset;
- local -> cloud escalation;
- task-class model routing;
- cross-provider verifier selection;
- surplus-capacity Lab allocation;
- quality/cost/usage trade-offs.

## 10. Future if earned

- predictive demand/reserve optimization;
- multi-GPU scheduling;
- energy-aware scheduling;
- provider/subscription ROI analysis;
- purchasing recommendations.

Human approval remains required for expanding paid financial commitments/subscriptions/budgets.
## 11. Usage telemetry source ladder — v4.1.1

Resource management uses the best legitimate telemetry source available:

1. official machine-readable provider API/usage endpoint;
2. supported provider CLI/status command;
3. other supported provider telemetry;
4. Agent OS estimate derived from observed usage;
5. conservative estimate.

Local runtimes may expose direct local hardware/runtime telemetry.

Every normalized `UsageSnapshot` records source, observation time, confidence, unit/basis, remaining estimate/value if known, and reset/window information if known.

## 12. Provider CLI adapters

A provider-specific deterministic adapter may invoke a supported CLI when that is the best available usage source.

The LLM should not repeatedly operate an interactive terminal merely to read quota/status if deterministic Go code can invoke, parse, cache, and normalize the information.

Provider tooling is an adapter implementation detail; the Resource Manager consumes normalized snapshots.

## 13. Telemetry caching

Do not poll usage before every inference by default.

Telemetry adapters expose cache/freshness policy. Refresh may become more frequent near:

- configured reserve threshold;
- provider limit;
- reset window;
- anomalous burn.

Provider-returned rate-limit/reset data may update the snapshot immediately.

Exact refresh cadence remains configurable and should later be tuned from evidence.
## 14. v4.1.2 — Inference is downstream of work-mechanism selection

The Resource Manager does not answer whether a Task should use an LLM.

Work orchestration first determines that model capability is justified.

Only then does the Resource Manager select among feasible subscription/API/local pools/models.

```text
work requirement
 -> deterministic/tool sufficient? yes -> no inference
 -> no
 -> model capability justified?
 -> yes
 -> Resource Manager selects inference pool/model
```

This prevents model-routing sophistication from increasing unnecessary model usage.


---

<!-- SOURCE: docs/34_V4_1_1_IMPLEMENTATION_HARDENING.md -->

# v4.1.1 Implementation Hardening

## 1. Status

This is a patch to v4.1. It does **not** introduce a new architectural plane or major subsystem.

The purpose is to remove implementation ambiguity discovered during design review.

## 2. Pattern candidacy: three occurrences is a trigger, not truth

Default V1 rule:

> **Three related occurrences are the minimum default evidence count for creating a repeated-pattern candidate.**

This means:

```text
occurrence #1 -> experience
occurrence #2 -> repeated observation
occurrence #3 -> repeated-pattern candidate may be proposed
```

It does **not** mean:

```text
three occurrences -> ACTIVE organizational knowledge
```

The threshold is configurable by task class/consequence. High-consequence business, safety, financial, legal, or public-facing claims may require more evidence before even becoming a promotion candidate.

Core distinction:

> **Frequency determines when a pattern deserves investigation. Evidence determines whether it deserves promotion.**

A repeated-pattern knowledge proposal must reference at least three concrete occurrence/event records under the default policy.

Automatic clustering/pattern discovery is not required in V1. A human or agent may propose the candidate; runtime validation checks its evidence references.

## 3. Promotion after pattern detection

Preferred flow:

```text
3+ related occurrences
        |
        v
PATTERN_CANDIDATE
        |
        v
subsequent observation and/or deliberate experiment
        |
        v
KNOWLEDGE_CANDIDATE
        |
        v
validation appropriate to consequence
        |
        v
ACTIVE / REJECTED / QUARANTINED
```

For deterministic domains, tests/environment checks should dominate.

For empirical business claims, controlled or repeated observations may be required.

For claims that remain fundamentally judgmental, do not falsely label them deterministic.

## 4. Nondeterministic judgment belongs to an operator

When objective verification is unavailable, an authorized **operator** makes the judgment.

An operator may be:

- an appropriately authorized agent for low/material internal judgment; or
- a human where consequence policy requires human authority.

Record:

```text
method
operator identity/type
evidence reviewed
time
scope
confidence/limitations if applicable
```

Existing Completion assurance categories map as:

```text
INDEPENDENT_ADJUDICATION -> agent/operator judgment
HUMAN_JUDGMENT          -> human operator judgment
```

The record must never imply that operator judgment is deterministic proof.

## 5. Auditing corrects institutional mistakes; it cannot guarantee none exist

Agent OS is allowed to be wrong.

The goal is not impossible perfect knowledge. The goal is to make institutional mistakes:

- visible;
- attributable;
- challengeable;
- auditable;
- reversible where practical;
- correctable before they compound.

Useful audit/correction telemetry includes:

```text
time_to_contradiction
time_to_audit_finding
time_to_correction
downstream_tasks_affected
repeat_failures_before_correction
stale_knowledge_usage_count
bad_skill_rollback_time
```

Do not optimize these into a single authoritative score in V1. Record raw measurements first.

## 6. Topology is empirical, not ideological

Agent OS does **not** assume Teams are better than single agents.

The system must be able to support and later experiment among execution structures such as:

```text
single agent
single agent + Skill
single agent + verifier
parallel independent agents
async Team
```

The Lab may compare these configurations for a task class.

The long-term objective is:

> **Use the smallest execution structure that produces the required verified outcome at acceptable cost, latency, reliability, and risk.**

A healthy Agent OS may assign most simple work to one agent.

## 7. Skills use defense in depth

Auditing is important but is not the sole defense against bad Skills.

V1 Skill safety relies on:

1. provenance;
2. explicit applicability/scope;
3. validation evidence;
4. version history;
5. capability/consequence enforcement at action time;
6. Completion Engine verification of work;
7. dependency/version revalidation triggers;
8. deterministic audits;
9. quarantine/supersession/rollback.

A Skill is procedural knowledge, not authority.

Bad procedures are possible just as in human organizations. Agent OS should make them easier to detect, trace, revise, and roll back.

## 8. Inference usage telemetry: best available evidence

Resource management must tolerate imperfect provider telemetry.

Use this source preference:

```text
1. official machine-readable API/usage endpoint
2. supported provider CLI/status command
3. other supported provider telemetry
4. estimate from Agent OS observed usage
5. conservative estimate
```

Local hardware telemetry may be read directly from the local runtime/system.

Every usage snapshot records:

```text
source
observed_at
confidence
remaining estimate/value if available
unit
reset/window information if available
basis/details
```

Unknown or estimated values remain explicitly uncertain.

## 9. CLI telemetry is an adapter concern

If a subscription provider exposes useful quota/rate-limit data only through a supported CLI, Agent OS may install/use that CLI through a provider-specific deterministic adapter.

Preferred flow:

```text
InferenceUsageAdapter
        |
        v
provider API / CLI / local telemetry
        |
        v
parse + normalize
        |
        v
UsageSnapshot
```

Do **not** make an LLM repeatedly open an interactive shell and reason over status output when deterministic software can invoke/parse it.

## 10. Cache/rate-limit telemetry collection

Usage polling itself may consume resources or create provider load.

Therefore:

- cache observations;
- use configurable minimum refresh intervals;
- refresh more aggressively only near meaningful thresholds/resets;
- immediately incorporate provider-reported rate-limit/reset events;
- do not query before every inference by default.

The exact cadence is configuration/empirical tuning, not a hard architectural constant.

## 11. Patch invariants

v4.1.1 adds no permission to:

- auto-promote three occurrences into trusted knowledge;
- let an operator judgment masquerade as deterministic proof;
- prefer Teams regardless of evidence;
- let Skills bypass capabilities;
- let audits silently rewrite trusted state;
- invent exact provider quota values when telemetry is uncertain;
- let agent content create trusted resource telemetry.


---

<!-- SOURCE: docs/35_WORK_FIRST_ORCHESTRATION_AND_MINIMAL_LLM.md -->

# Work-First Orchestration and Minimal Justified LLM Use — v4.2

## 1. Decision

Agent OS is built to perform **real organizational work**, not to exercise predetermined agent topologies or benchmarks.

Two principles are normative:

> **Work defines the organization. Measurement improves it.**

and:

> **Use the least nondeterministic mechanism sufficient for the work.**

## 2. Work-First Orchestration

Agents, Teams, Skills, tools, and workflows exist because the actual objective requires them.

Do not begin with:

```text
"We need a Team, so find work for the Team."
```

Begin with:

```text
"What work must be done?"
```

Then decompose the work into the smallest useful Task dependency graph.

The graph is the V1 workflow representation. Do not add a separate generic workflow DSL/engine unless real work proves the Task graph insufficient.

## 3. Execution mechanisms

A Task may be performed by:

- `DETERMINISTIC` — normal Go/runtime logic, rules, calculations, state transitions;
- `TOOL` — API/database/CLI/tool invocation with predetermined control logic;
- `AGENT` — durable Agent responsibility with LLM execution only where needed;
- `TEAM` — collaborative actors where interdependence justifies it;
- `HUMAN` — human judgment/authority;
- `MIXED` — parent Task decomposed across more than one mechanism.

This is not a ranking. The correct mechanism depends on the work.

## 4. Minimal Justified LLM Principle

LLMs are valuable but:

- nondeterministic;
- resource-consuming;
- sometimes slower;
- sometimes harder to audit;
- unnecessary for many exact operations.

Before invoking a model, ask whether reliable conventional software or an existing deterministic procedure can perform the step adequately.

### Good deterministic candidates

Do not use an LLM merely to:

- add/count/compare exact values;
- parse known structured data;
- enforce authorization;
- check capability presence;
- route a known Event Contract;
- evaluate exact dependency state;
- run known validation/tests;
- query a known database field;
- calculate budgets/remaining resources;
- schedule timers;
- collect provider CLI/API usage telemetry;
- execute a stable deterministic transformation.

### Good LLM candidates

LLM inference is justified where the work benefits from:

- ambiguous natural-language understanding;
- novel planning/decomposition;
- research/synthesis;
- code/writing generation;
- critique and alternative generation;
- interpreting messy/unstructured evidence;
- adaptive tool-use planning;
- nondeterministic judgment where an agent operator is permitted;
- candidate lesson/Skill extraction from experience.

## 5. Agent != always-running LLM

A persistent Agent is a durable organizational actor/responsibility.

It may own a workflow in which most steps are deterministic.

Example:

```text
Accounts Receivable Agent

scheduled check              deterministic
retrieve invoices            API/database
calculate aging              deterministic
apply standard policy        deterministic
ambiguous customer case      -> AgentExecution / LLM
protected external action    -> consequence policy / human if required
```

No model process needs to exist between reasoning events.

## 6. Task graph as V1 workflow

A Task should record an `ExecutionKind`.

Suggested values:

```text
DETERMINISTIC
TOOL
AGENT
TEAM
HUMAN
MIXED
```

Task/TaskContract may also record:

```text
ModelInferencePolicy:
  DISALLOWED
  ALLOWED_IF_JUSTIFIED
  REQUIRED
```

Default for Agent-owned work should be `ALLOWED_IF_JUSTIFIED`, not `REQUIRED`.

A deterministic/tool task should normally use `DISALLOWED`.

`REQUIRED` is appropriate only when model reasoning/generation is intrinsic to the contracted work.

## 7. Selecting execution structure

Do not encode a global preference for:

- one Agent;
- Teams;
- workflows;
- parallelism.

The default goal is:

> **Use the smallest execution structure that produces the required verified outcome at acceptable cost, latency, reliability, and risk.**

When the best structure is uncertain, actual operational history or Lab experiments may compare alternatives.

## 8. Real work before benchmark work

As soon as core runtime primitives function, operate on representative real work.

Record:

- verified outcome;
- task class;
- execution structure;
- deterministic vs LLM steps;
- model/provider only when used;
- tokens/cost/resource usage;
- wall time;
- tool calls;
- messages;
- blocked work;
- human interventions;
- failures/rework;
- completion assurance.

These measurements become evidence for later improvements.

Do not manufacture work merely to obtain a benchmark score.

## 9. Controlled evaluation still matters

Raw real-world results are confounded by task difficulty.

When choosing between structures, use where practical:

- replayable real tasks;
- matched tasks;
- shadow runs;
- held-out tasks sampled from real workload.

The corpus should be representative of actual organizational work.

Benchmarking is an instrument, not the product objective.

## 10. Lab relationship

The Lab answers questions such as:

```text
For this real task class, should we use:
  deterministic workflow?
  one Agent?
  one Agent + Skill?
  Agent + verifier?
  parallel attempts?
  async Team?
```

The commissioning organization nominates promising results; independent validation/promotion rules still apply.

## 11. Resource relationship

Inference Resource Manager operates only after model inference is justified.

Decision order:

```text
1. What work is required?
2. Can deterministic software/tooling adequately perform it?
3. If no, is LLM reasoning/judgment/generation justified?
4. If yes, which feasible inference resource/model is appropriate?
5. Preserve continuity reserve and consequence/data policies.
```

Do not optimize model routing for steps that should not use a model.

## 12. Anti-complexity consequence

This principle is also a complexity control.

Do not build:

- a generic workflow DSL before Task DAGs fail;
- LLM agents around deterministic services;
- permanent Agent roles for work that is a deterministic job;
- multiple-agent topology merely for architectural symmetry.

The system should become **less agentic** where normal software is superior and **more agentic** only where adaptive intelligence creates measurable value.


---

<!-- SOURCE: docs/36_A2A_OPERATOR_GATEWAY_AND_HERMES.md -->

# A2A Operator Gateway and Hermes Handoff — v4.2

## 1. Decision

A minimal **A2A v1.0 Operator Gateway** is V1 CORE because Hermes Agent is a concrete intended external operator/manager of Agent OS.

A2A is an external interoperability boundary.

> **Internal Agent OS communication remains Event Contracts.**

```text
Human
  |
  v
Hermes Agent
  |
  | A2A v1.0
  v
Agent OS A2A Operator Gateway
  |
  | authenticate -> authorize -> translate
  v
Intent / Goal / Task / Event Contracts / Ledger
```

Do not replace the internal Event Gateway, Task model, or Event Contracts with A2A objects.

## 2. External actor identity

An authenticated A2A peer maps to an Agent OS `ExternalActor`.

Example:

```text
ExternalActorID: hermes-primary
Protocol: A2A
Credential/identity binding: configured
OrganizationID: org-1
Capability refs:
  submit_work
  read_visible_status
  provide_task_input
  read_visible_artifacts
```

A2A connectivity does not imply administrator authority.

> **A2A establishes/communicates peer identity; Agent OS policy determines what that peer may cause.**

Hermes remains subordinate to all capability, data, approval, freeze/revoke, and consequence rules.

## 3. V1 inbound management surface

The V1 gateway should support the minimum operator path:

1. Agent Card discovery;
2. authenticated message/task submission;
3. map submitted work to an `IntentEnvelope`;
4. create or continue the corresponding internal Goal/Task work;
5. expose task status/progress;
6. expose result Artifacts;
7. request/provide missing task input;
8. translate internal `TASK_BLOCKED` into an input-needed/status update;
9. maintain correlation between A2A external task/context IDs and internal Intent/Goal/Task IDs.

## 4. A2A Task != Agent OS Task

Do not make the models identical.

```text
A2A Task
= external interoperability/session object

Agent OS Goal/Task DAG
= internal organizational work model
```

One A2A task may map to:

```text
A2A task
  -> IntentEnvelope
  -> Goal
      -> Task A
      -> Task B
          -> Task B1
          -> Task B2
      -> Task C
```

The gateway reports aggregate/progress status externally without leaking internal data the peer is not authorized to see.

## 5. Work-level interface, not privileged control API

A2A should primarily express:

- desired work;
- task continuation;
- new information;
- status;
- results/artifacts.

Do not expose broad remote operations such as:

```text
grant arbitrary capability
rewrite ledger
activate skill directly
change root policy
rewrite knowledge directly
disable human freeze
```

Hermes may request work whose normal execution happens to require these transitions; normal internal authorization/governance remains authoritative.

## 6. Blocked work

Example:

```text
internal:
TASK_BLOCKED
reason = MISSING_INPUT
missing = target customer segment

A2A external status/message:
"Can't continue — target customer segment is required."
```

Hermes may supply the missing information if authorized/known.

The supplied input is persisted with source provenance:

```text
source_external_actor = hermes-primary
a2a_task/context = ...
```

The parent Task may then resume.

## 7. Human approval boundary

Hermes is not a substitute for required human approval.

Example:

```text
Hermes requests production deployment
    ->
Agent OS consequence classifier
    ->
HUMAN_APPROVAL
    ->
wait until authorized human decides
```

Hermes may surface the pending decision to the human.

It may not self-approve because it is the operator.

## 8. A2A conformance/interoperability testing

V1 acceptance includes a pinned Hermes integration configuration/release and tests. The initial compatibility target from this handoff is Hermes Agent **v0.20.0** with A2A v1.0; upgrades require rerunning conformance/integration tests:

```text
Hermes discovers Agent OS Agent Card
Hermes submits work
Agent OS persists Intent/Task
Agent OS reports progress
Agent OS reports blocked/input-needed state
Hermes supplies input
Agent OS resumes
Agent OS returns result Artifact
```

Treat Hermes/A2A implementation bugs as adapter/conformance issues, not reasons to weaken Agent OS internals.

## 9. V1 directionality

Required:

```text
Hermes / external A2A peer -> Agent OS Operator Gateway
```

Agent OS may respond/status-stream as required by that task/session.

Not required in V1:

- Agent OS discovering arbitrary external A2A workers for delegation;
- remote agent marketplace;
- dynamic federation;
- cross-organization trust negotiation.

Those remain later features.

## 10. Hermes RuntimeAdapter remains separate

Using Hermes as an **internal worker runtime** is a different capability from Hermes operating Agent OS externally.

```text
External operator:
Hermes --A2A--> Agent OS

Possible later internal worker:
Agent OS --RuntimeAdapter--> Hermes execution
```

The second remains `VALIDATE_NEXT` unless explicitly promoted.

## 11. Protocol isolation

The gateway is an adapter module.

Suggested Go boundary:

```go
type OperatorGateway interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
}

type ExternalWorkSubmission struct {
    ExternalActorID string
    ExternalTaskID  string
    Content         []Part
}
```

Do not make core domain packages depend on A2A-specific wire types.

Translate at the boundary into internal application commands/Events.


---

<!-- SOURCE: docs/37_EXECUTION_CONTEXT_TOOL_OUTCOME_EFFECTS.md -->

# Execution Context, Tool Outcomes, and Durable Effects — v4.2

## 1. Why this exists

Hermes release/bug history exposed three runtime questions that Agent OS must answer precisely:

1. **What information was actually available to this model execution?**
2. **What effect did a tool actually produce, and can software recover without another model turn?**
3. **Was an approved external effect merely attempted, or durably confirmed?**

These are runtime/audit questions, not cognition semantics.

## 2. ExecutionContextManifest — V1 CORE

Every `AgentExecution` receives a durable manifest describing the exact context materialized for that execution.

Minimum fields:

```text
ExecutionID
AgentID
ExecutionProfileVersion
Model/provider/reasoning setting
TaskID
TaskContractVersion
Prompt/OperatingInstructionVersion
PolicyVersion

EventRefs[]
KnowledgeRefs[]:
  KnowledgeID + exact Version + MaterializationState
SkillRefs[]:
  SkillID + exact Version + MaterializationState
ArtifactRefs[]:
  ArtifactID + MaterializationState
ToolDefinitions[]:
  ToolID + Version
AdditionalContextRefs[]

CreatedAt
ContextBuilderVersion
```

Recommended materialization states:

```text
FULL
SUMMARY
REFERENCE_ONLY
OMITTED
UNAVAILABLE
```

The manifest should be generated by runtime/context-building software and persisted before or atomically with execution start.

### Audit question

The system must be able to answer:

> **What did this execution actually have available when it made this decision?**

This is distinct from:

> What knowledge/skills existed in the organization at the time?

## 3. Context compaction

V1 may use simple context construction.

Sophisticated compaction is `VALIDATE_NEXT`.

If compaction/summarization is introduced, the manifest must preserve whether a Skill/knowledge item was still fully materialized versus merely referenced/summarized.

A summary must not falsely imply that full procedural instructions remain present.

## 4. ToolOutcome — V1 CORE

Tool adapters return structured outcomes rather than a loose success string.

Minimum shape:

```text
ToolInvocationID
ToolID / Version
Status: SUCCESS | PARTIAL | FAILED

ObservedEffect
PostconditionStatus: VERIFIED | FAILED | NOT_CHECKED
Retryability: RETRYABLE | NOT_RETRYABLE | RETRY_AFTER_CHANGE

RecoveryAttempted
RecoveryResult?

ArtifactRefs[]
ErrorClass?
ErrorDetail?
StartedAt
FinishedAt
```

## 5. Deterministic recovery before cognitive recovery

> **Deterministic recovery before cognitive recovery.**

When a known tool failure has a safe deterministic remedy, software should attempt it before spending another model turn.

Examples:

- truncated output -> spill full result to an Artifact and return reference;
- file write -> read/hash/verify postcondition;
- patch already applied -> detect exact expected state;
- transient API failure -> bounded policy-controlled retry;
- missing directory -> create it only when the requested effect and capability allow it.

Do not let deterministic recovery broaden the authorized effect.

If deterministic recovery fails or the choice is semantic/novel, return the ToolOutcome to the responsible Agent for reasoning.

## 6. EffectObligation — V1 CORE for consequential external effects

For real external/write side effects, Agent OS must distinguish:

```text
decision/approval
attempt
confirmed effect
```

Before performing a consequential external effect, persist an `EffectObligation`.

Minimum fields:

```text
EffectObligationID
OrganizationID
TaskID
Action
Destination/Resource
CanonicalEffectDescriptor
ArgumentsHash / EffectFingerprint
AuthorizationRefs[]
ApprovalRef? 
IdempotencyKey
ReplayContext
Status:
  PENDING
  ATTEMPTED
  CONFIRMED
  FAILED
  CANCELLED

AttemptCount
LastAttemptAt?
ConfirmationEvidenceRefs[]
CreatedAt
```

## 7. Persist-before-effect

For covered external effects:

```text
authorize
  ->
persist EffectObligation
  ->
execute through effect adapter
  ->
record attempt
  ->
verify/receive confirmation
  ->
CONFIRMED
```

A process crash after persistence but before confirmation leaves a visible obligation that recovery can reconcile/retry according to policy.

Do not claim exactly-once side effects when the destination cannot guarantee them.

Use:

- idempotency keys where supported;
- reconciliation/status checks;
- at-least-once retry semantics where appropriate;
- explicit uncertainty when confirmation cannot be established.

## 8. Replay semantics must be complete

An obligation must preserve enough context to reproduce the intended effect faithfully.

For example, message delivery may require:

- channel/destination;
- reply/thread target;
- payload/artifact;
- sender identity;
- idempotency metadata.

Persisting only the text body is insufficient.

## 9. Approval fingerprint — V1 CORE

Human approval binds to the exact protected effect, not a general permission expansion.

Approval should include or derive:

```text
EffectFingerprint
TaskID
Action
Resource/Destination
ArgumentsHash
ExpiresAt?
SingleUse?
```

If material arguments change after approval, the fingerprint changes and a new decision is required when policy says the changed effect crosses the boundary.

> **Approve the effect, not authority expansion.**

## 10. Tool/Effect boundary

Not every ToolOutcome needs an EffectObligation.

Use EffectObligation for actions where retry/crash ambiguity or external consequence matters.

Examples likely covered:

- sending external/public communication;
- production deployment;
- third-party mutation;
- financial transaction;
- destructive external operation.

Local read-only/deterministic work may only need ToolOutcome and ordinary events.

## 11. SecretSource seam

V1 should expose a small secret-resolution seam without building a secret-management platform.

```go
type SecretSource interface {
    Resolve(ctx context.Context, ref SecretRef) (SecretValue, error)
}
```

A simple environment/config-backed implementation is enough initially.

Agents receive capability/tool access; avoid exposing long-lived credentials to model context when adapters can resolve/use them directly.

Additional secret managers remain future/validate-next integrations.

## 12. Narrow-waist rule

When a business capability is needed, prefer:

1. existing deterministic handler/tool;
2. existing tool + Skill;
3. new adapter/handler;
4. external integration;
5. core runtime change only if the requirement truly belongs to the OS.

Do not make every business-specific capability a core Agent OS feature.
