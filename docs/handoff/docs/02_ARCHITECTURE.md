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
