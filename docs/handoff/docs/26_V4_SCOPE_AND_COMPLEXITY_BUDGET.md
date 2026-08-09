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
