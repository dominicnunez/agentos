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
