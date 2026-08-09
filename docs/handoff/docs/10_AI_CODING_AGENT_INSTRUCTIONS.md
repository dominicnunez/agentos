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
