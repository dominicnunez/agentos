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
