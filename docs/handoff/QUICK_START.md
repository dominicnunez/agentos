# Agent OS v4.2 — Quick Start

This is the short version. You do **not** need to read the entire master specification before implementation.

## What Agent OS is trying to do

Agent OS is a runtime for operating persistent AI-assisted organizations and businesses.

The organization persists. Individual LLM invocations do not.

```text
Organization
  -> Goals
  -> real work
  -> Task dependency graph / workflow
  -> use ordinary software, tools, LLM-backed agents/teams, or humans where each is justified
  -> persist events/results/knowledge
  -> verify completion
  -> learn and audit
```

## Two core implementation principles

### 1. Work-First Orchestration

**Actual work determines the workflow, agents, teams, and tools.**

Do not build work merely to exercise an architecture or benchmark.

A real task may be best handled by:

- deterministic software;
- a tool/API call;
- a deterministic workflow;
- one Agent;
- one Agent + Skill;
- one Agent + verifier;
- parallel attempts;
- an asynchronous Team;
- a human operator;
- a mixture of the above.

Measurements and Lab experiments improve how recurring real work is performed.

### 2. Minimal Justified LLM Use

> **Use the least nondeterministic mechanism sufficient for the work.**

Do not invoke an LLM when ordinary software or an existing deterministic procedure can reliably do the step with better cost/reliability.

Use LLM inference when adaptive reasoning, interpretation, generation, tool-use planning, or judgment provides justified value.

Short engineering rule:

> **Don't use an LLM to do a computer's job. Don't use rigid software to do an LLM's job.**

A persistent `Agent` does not mean an LLM is constantly running. `AgentExecution` is created only when model inference is actually needed.

## V1 architecture

Build:

- Go modular monolith;
- SQLite single-writer event ledger;
- Organization / Team / Agent durable identities;
- ephemeral AgentExecution;
- Intent / Goal / Task dependency graph;
- Task graph as the minimal workflow representation;
- Event Contracts + Event Gateway;
- async lateral event availability at action boundaries;
- capabilities / authorization trace / blocked tasks;
- human approval boundaries + freeze/revoke;
- Completion Contract + Completion Engine;
- versioned institutional knowledge;
- instruction/reference Skills;
- deterministic Audit Service;
- Inference Resource Manager;
- ExecutionContextManifest for every model execution;
- structured ToolOutcome + deterministic recovery;
- scoped effect-bound approvals;
- durable EffectObligation/outbox for consequential external effects;
- minimal A2A Operator Gateway for Hermes;
- operational telemetry from real work.

Do not build a general workflow DSL in V1.

## Work execution kinds

A Task may be satisfied by:

```text
DETERMINISTIC
TOOL
AGENT
TEAM
HUMAN
MIXED
```

LLM inference is not assumed merely because an Agent owns the responsibility.

## Learning

Three related occurrences may create a **pattern candidate**, not truth.

```text
3+ occurrences
 -> investigate
 -> subsequent evidence / experiment / operator judgment
 -> knowledge candidate
 -> validation
 -> ACTIVE knowledge
```

Knowledge and Skills are versioned and auditable. They never grant authority.

## Auditing

Audit deterministic things with deterministic software first.

Audit Service checks integrity, provenance, stale knowledge/skills, authorization, completion evidence, stuck state, etc.

Judgment-heavy LLM auditing is later and bounded.

## Resources

Inference resources may be:

- subscription;
- metered API;
- local compute.

The runtime manages them. Agents do not own model entitlement.

Before asking **which model**, ask **whether a model is needed at all**.

## Lab

The Lab is for uncertainty about how real work should be performed.

Example:

```text
real recurring task class
 -> uncertain best structure
 -> Lab compares:
      deterministic workflow
      single Agent
      Agent + Skill
      verifier
      parallel attempts
      async Team
 -> controlled replay / held-out real work
 -> adopt the best justified structure
```

The Lab does not exist to prove Teams are better.

## What to read next

For implementation, read only:

1. `AGENTS.md`
2. `IMPLEMENTATION_SCOPE.yaml`
3. `docs/29_V1_BUILD_CONTRACT.md`
4. `docs/35_WORK_FIRST_ORCHESTRATION_AND_MINIMAL_LLM.md`
5. `docs/36_A2A_OPERATOR_GATEWAY_AND_HERMES.md`
6. `docs/37_EXECUTION_CONTEXT_TOOL_OUTCOME_EFFECTS.md`

Use `MASTER_PROJECT_SPEC.md` only when you need deeper rationale/details.
## Hermes as the external operator

V1 exposes a minimal **A2A v1.0 Operator Gateway** so a Hermes Agent can manage Agent OS at the work level.

```text
You -> Hermes -> A2A -> Agent OS -> Organizations/Tasks
```

Hermes can submit/continue work, read authorized status/results, provide missing input, and receive blocked/progress/result updates.

Hermes does **not** gain root/admin authority simply because it connects over A2A. Agent OS capabilities and human consequence boundaries remain authoritative.

Internal communication still uses Event Contracts.

## Runtime evidence hardening

Every model execution gets an `ExecutionContextManifest`, so audits can reconstruct the exact Event, Knowledge, Skill, Artifact, prompt/profile and tool versions actually available.

Tools return `ToolOutcome` with verified/failed postconditions and deterministic recovery status.

For consequential external writes, persist an `EffectObligation` before attempting the effect and record confirmation/reconciliation afterward.

> Deterministic recovery before cognitive recovery.
