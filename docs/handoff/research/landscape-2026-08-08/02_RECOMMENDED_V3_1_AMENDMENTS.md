# Recommended v3.1 Amendments

These are targeted amendments, not a v4 rewrite.

## Priority 1 — add before core public APIs freeze

### 1. `IntentEnvelope`

Add a first-class immutable object preserving:

- original human/external instruction;
- normalized objective;
- assumptions;
- hard constraints;
- consequence boundaries;
- source identity;
- created time/version;
- links to Goal/TaskContracts derived from it.

Human-facing label:

> Original request and boundaries

Why: AOS is right that structured task plans must remain traceable to the originating intent.

### 2. `AuthorizationLineage`

Make authorization ancestry a concrete object rather than only a list of IDs.

Fields:

- root intent;
- policy decisions;
- delegations;
- authority ceilings;
- capability leases;
- approvals;
- current action;
- derivation/security labels;
- revocations/expiry.

Human-facing label:

> Why this action is allowed

### 3. Capability Contract and feasibility-before-utility

Before selecting an actor/model/provider:

```text
hard capability/policy/data constraints
        ->
feasible candidates
        ->
quality/cost/latency optimization
```

Never optimize among candidates that are not authorized or technically capable.

### 4. Clarify actor composition

Use explicit internal concepts:

```text
AgentBlueprint
  role, operating principles, default skills, required capability classes

ExecutionProfile
  model, reasoning budget, skill versions, tools, prompt/policy, memory/context policy

RuntimeAdapter
  Hermes, native runtime, CLI agent, remote A2A agent, etc.

Agent
  durable logical identity bound to versioned blueprint/profile/adapter
```

This borrows the useful Talent/Container distinction without adopting OMC’s automated HR semantics.

### 5. Visibility, invocation, and authority are distinct

Add root invariant:

```text
discoverable != invocable != authorized
```

Knowing an actor, tool, artifact, or object exists grants no authority.

### 6. Conformance profile

Every deployment declares which responsibilities are actually implemented.

Example v1 profile:

```text
local modular monolith
single authoritative ledger writer
no distributed consensus
basic information-flow labels
local sandbox only
ANL/0.1
human approval single controller
```

This prevents architecture promises from being confused with production implementation.

## Priority 2 — add to Evaluation/Optimization design

### 7. Goodhart and evaluator-health monitor

Track as warning/health inputs:

- score inflation;
- evaluator disagreement/drift;
- diversity collapse;
- calibration deterioration;
- sudden reduction in uncertainty declarations;
- benchmark leakage indicators;
- task-selection skew.

Do not hard-code Qualixar’s thresholds as universal truth.

### 8. Collaborative post-task attribution

After a task, gather bounded operational evidence from each actor:

- what failed;
- what evidence changed its work;
- what dependencies blocked it;
- what teammate contribution mattered;
- which assumption was wrong.

The hidden evaluator uses this evidence. Actors do not receive comparative grades.

### 9. Held-out evolution/evaluation split

Organizational changes should be developed on one task set and evaluated on unseen held-out tasks where practical.

### 10. Organization portability package

Define a signed/versioned export format for:

- OrganizationBlueprint;
- Team/Agent blueprints;
- policies;
- SOP/skill manifests;
- ontology dependencies;
- allowed runtime adapters;
- no embedded live secrets.

Imported packages begin quarantined/untrusted.

## Priority 3 — runtime roadmap, not first vertical slice

### 11. MLFQ and zombie/stall detection

Study AgentRM after basic scheduler correctness. Add:

- priority classes;
- blocked/waiting distinction;
- stalled execution detection;
- rate-limit-aware admission;
- resource-lane accounting.

### 12. Context hibernation

Add checkpointed inactive context and selective restoration. Measure information retention and compaction cost.

### 13. Fresh spawn semantics

New child/specialist should receive:

- explicit Goal/TaskContract;
- scoped TeamSync;
- selected memory/artifacts;
- explicit capabilities.

Do not clone the full parent transcript/capabilities by default.

## Decisions to keep unchanged

- modular monolith first;
- Go core runtime;
- ANL internally;
- A2A for external federation;
- blocked tasks rather than worker permission solicitation;
- no positive capability inheritance;
- separate Completion Engine;
- hidden evaluator grades;
- dormancy before archival;
- human approval for consequential boundaries;
- external research as untrusted;
- runtime code deployment requires human approval;
- trusted-core deployment requires stricter security-admin approval.

## Public-positioning amendment

Do not claim:

- first Agent OS;
- first AI company manager;
- first dynamic agent-team system;
- first self-improving multi-agent organization;
- first deterministic agent governance layer.

A defensible claim is:

> Agent OS combines persistent artificial organizations, AgentRadio-style asynchronous collaboration, ANL semantic communication, deterministic human audit, least-authority blocked-task delegation, independent completion/evaluation, and evidence-driven reversible organization evolution.
