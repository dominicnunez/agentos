# Agent OS Competitive and Prior-Art Research Dossier



---

<!-- SOURCE: 00_EXECUTIVE_SUMMARY.md -->

# Agent OS Landscape Study — Executive Findings

**Research snapshot:** 2026-08-08  
**Purpose:** Compare the proposed Agent OS + ANL v3 architecture with the closest current papers, runtimes, governance systems, and organization-management products.

## Bottom line

The “Agent OS” category is real and is forming rapidly. Our project is **not** the first to propose:

- OS-style scheduling and memory for agents;
- long-running capability-controlled agent processes;
- dynamic team formation;
- artificial-company organization;
- deterministic governance middleware;
- self-evolving multi-agent teams;
- organization charts, goals, budgets, and human oversight.

Those ideas already appear separately—and sometimes in strong combinations—in AIOS, Agent libOS, AOS, Qualixar OS, OneManCompany, Microsoft’s Agent Governance Toolkit, Meta-Team, Paperclip, and AgentTeams. [S2–S11]

However, I did not find one studied system that combines the following into a single coherent architecture:

1. persistent Team and Organization actors;
2. AgentRadio-style asynchronous mid-execution lateral collaboration;
3. ANL as a machine-native semantic IR;
4. the canonical machine message as authoritative history with deterministic on-demand human decoding;
5. Authority Non-Solicitation—workers return blocked work rather than asking for more power;
6. no positive capability inheritance;
7. authorization ancestry across all delegation;
8. a separate Completion Engine;
9. hidden independent agent evaluation;
10. controlled A vs A+skill vs alternate model/reasoning vs A+B specialist experiments;
11. Organization Health and organizational-complexity accounting;
12. reversible organization evolution;
13. controlled external-research-to-self-improvement promotion;
14. layman-facing UI language over precise advanced semantics.

The defensible differentiation is therefore **the integrated system of semantics, authority, completion, evaluation, and organizational evolution**, not the broad phrase “Agent Operating System.”

## Most important conclusion

The research **validates the overall direction but narrows the novelty claim**.

A strong public description would be:

> A Go-based Agent Operating System for persistent artificial organizations, with machine-native semantic communication, deterministic human auditability, least-authority delegation, independently verified completion, and evidence-driven organizational evolution.

A weak and inaccurate claim would be:

> The first Agent OS or the first system for AI companies.

## What the research changes

The v3 design does not need a foundational rewrite. It does deserve a focused v3.1 research addendum adding:

- `IntentEnvelope` and `AuthorizationLineage` as explicit first-class objects;
- a formal Capability Contract / feasibility-before-utility rule;
- a clearer AgentBlueprint–ExecutionProfile–RuntimeAdapter split;
- the invariant `visibility != invocation != authority`;
- conformance profiles and explicit unsupported responsibilities;
- Goodhart/drift/diversity-collapse monitoring;
- post-task collaborative failure attribution without revealing hidden grades;
- later scheduler/context-hibernation work informed by AIOS and AgentRM;
- an organization portability package format;
- a benchmark plan that directly compares our architecture to hierarchical, topology-based, and post-task-evolution alternatives.

## Strongest prior art by area

| Area | Strongest studied neighbor |
|---|---|
| Reference architecture, intent, authority | AOS |
| Dynamic topology/team design and model routing | Qualixar OS |
| Artificial organization and talent recruitment | OneManCompany |
| Deterministic governance middleware | Microsoft Agent Governance Toolkit |
| Capability-controlled long-running process runtime | Agent libOS |
| Kernel scheduling/context/memory analogy | AIOS |
| Resource scheduling and context hibernation | AgentRM |
| Company/org-chart/budget product surface | Paperclip |
| Transparent heterogeneous agent-team operations | AgentTeams |
| Experience-driven team evolution | Meta-Team |
| Mid-execution asynchronous collaboration | AgentRadio |
| External agent interoperability | A2A |

## Evidence warning

Most 2026 systems studied here are recent preprints or rapidly changing repositories. Their results are generally author-reported and are not equivalent to broad independent replication.

Examples:

- The AOS paper explicitly presents a reference-architecture proposal and does not claim benchmark superiority. [S2]
- Qualixar’s 100% result is on a curated 20-task suite without web, file, or multi-tool work; its preliminary self-improvement experiment was not statistically significant and declined in mean score. [S3]
- OneManCompany reports 84.67% on PRDBench, but baseline costs were unavailable, so cost-efficiency cannot be compared directly. [S4]
- AgentRM’s large scheduler/context gains come from simulated workloads derived from production patterns. [S8]
- Agent libOS is evaluated primarily as a safety/runtime substrate with regression tests, not as evidence of improved planner intelligence. [S6]

These projects are valuable architectural evidence, not proof that every claimed mechanism will generalize.

## Recommendation

Proceed with the v3 implementation order, but add the targeted v3.1 amendments in this research package before freezing public APIs. Do **not** expand implementation scope to reproduce every neighboring feature. The highest-value empirical questions remain:

1. Does AgentRadio-style asynchronous collaboration outperform hierarchical and turn-based alternatives on interdependent work?
2. Does ANL outperform structured JSON/A2A payloads and natural-language messages on semantic fidelity, permission clarity, auditability, and task performance?
3. Does the blocked-task/no-positive-inheritance authority model remain usable without creating excessive coordination overhead?
4. Does a separate Completion Engine materially reduce false completion?
5. Can hidden evaluation and controlled organizational experiments improve efficiency without inducing Goodhart behavior?


---

<!-- SOURCE: 01_DEEP_COMPARATIVE_STUDY.md -->

# Deep Comparative Study

## 1. Research method

This study prioritized primary sources:

- arXiv papers;
- official project repositories;
- official protocol specifications.

Each system was examined for:

- its actual system boundary;
- primary durable abstractions;
- communication model;
- team/organization model;
- capability and governance model;
- persistence and recovery;
- evaluation/self-improvement;
- empirical evidence;
- limitations;
- direct implications for Agent OS v3.

The comparison distinguishes:

- **reference architecture** — defines responsibilities/invariants;
- **runtime implementation** — implements execution/security primitives;
- **orchestration framework/platform** — coordinates agents/workflows;
- **organization control plane** — manages companies, teams, goals, budgets;
- **research framework** — tests a particular collaboration/evolution hypothesis.

These categories overlap, but conflating them leads to misleading “feature comparisons.”

---

# 2. AgentRadio

## What it contributes

AgentRadio directly motivates our collaboration substrate. It adds threads, messages, and background waiting for mentions so agents can receive teammate discoveries while continuing foreground work. Its paper argues that long-horizon code-comprehension subtasks are interdependent and therefore require coordination during execution rather than only at phase boundaries. It reports 62.1% resolution with four agents on SWE-Atlas QnA versus 32.3% for one Claude Code Opus 4.6 agent. [S1]

## What to borrow

- passive awareness at explicit action boundaries;
- targeted messages rather than unrestricted universal chat;
- empirical focus on interdependent tasks;
- direct comparison with a strong single-agent baseline.

## What AgentRadio does not solve

- durable organization identity;
- machine-native semantic communication;
- authority/capability governance;
- completion certification;
- organizational optimization;
- long-term institutional memory.

## Implication

AgentRadio remains the best direct basis for our live collaboration loop, but it is a component of the Agent OS rather than a complete operating architecture.

---

# 3. AOS reference architecture

## Why it is the closest conceptual neighbor

AOS proposes a vendor-neutral reference architecture with two logical planes:

- Control & Governance: intent, policy, trust, authority, confidence, auditability, observability, and human oversight;
- Runtime & Coordination: lifecycle, workflows, routing, context/memory, scheduling, traffic, and assurance. [S2]

The paper explicitly keeps Linux, Windows, container runtimes, and physical infrastructure outside the AOS boundary. That independently validates our statement that “Agent OS” is an application/runtime architecture above the host OS, not a replacement kernel.

## Particularly strong overlap

AOS treats the following as first-class:

- original intent;
- policy decisions;
- trust decisions;
- delegation/authority;
- capability contracts;
- execution directives/events/results;
- confidence;
- audit events;
- versioned canonical objects.

Its invariants closely match several v3 decisions: no consequential action without valid authority, delegation cannot exceed parent authority without independent authorization, retries/replanning do not erase history, and runtime-reported success does not retroactively authorize an action. [S2]

## AOS is more rigorous than our current vocabulary in three places

### Intent preservation

AOS uses an `IntentEnvelope` so the original human objective is preserved alongside structured interpretation. Our TaskContract and authorization ancestry imply this, but v3 should make the original intent object explicit.

### Capability feasibility before utility

AOS separates:

1. whether a provider/actor satisfies hard capability and policy constraints;
2. which feasible provider best optimizes cost/latency/quality.

Our Collaboration Manager and model router should adopt this explicit order.

### Conformance profiles

AOS emphasizes that an implementation can support only selected responsibilities and must declare what it does not provide. That would help prevent Agent OS v1 from pretending that a local modular monolith provides production-grade distributed consensus, privacy, or kernel isolation.

## Critical differences

AOS explicitly does **not** define a universal agent programming language, prompt format, workflow DSL, or complete communication protocol. ANL is therefore not duplicated by AOS. [S2]

AOS also remains a reference-architecture proposal—Draft v0.8 in the studied version—and explicitly does not claim benchmark superiority or universal completeness. It should be treated as strong prior architectural work, not a validated complete implementation. [S2]

## Borrow

- `IntentEnvelope`;
- `AuthorizationLineage`;
- `CapabilityContract`;
- feasibility-before-utility;
- conformance profiles;
- explicit negative scope;
- separation of control authorization from runtime evidence.

## Keep our stronger choices

- Authority Non-Solicitation;
- no positive capability inheritance;
- separate Completion Engine;
- hidden evaluator grades;
- Organization Health;
- persistent Team/Organization actors;
- ANL and deterministic human decoding.

---

# 4. Qualixar OS

## What it is

Qualixar OS is an application-layer orchestration system rather than a hardware/kernel OS. It includes:

- Forge for team design;
- 12 orchestration topologies;
- model discovery/routing;
- judge/consensus pipelines;
- cost tracking;
- RL feedback;
- Goodhart/drift monitoring;
- an event-driven dashboard. [S3]

It uses A2A as a canonical message format and supports in-memory/HTTP/MCP routing choices.

## Why it matters to us

Qualixar is the strongest studied implementation of:

- task-to-team/topology generation;
- heterogeneous model selection;
- cost-aware routing;
- judge ensembles;
- monitoring for score inflation and diversity collapse.

This overlaps directly with our Collaboration Manager and Evaluation & Optimization Plane.

## Evidence quality

Its reported 100% result is on a curated 20-task suite composed of factual, arithmetic, inference, and probabilistic questions. The paper states that the suite excludes web browsing, file manipulation, and multi-tool orchestration and that standard benchmarks are future work. [S3]

More importantly, its preliminary Forge→Judge→RL self-improvement benchmark was not statistically significant (`p=0.578`), only 3/10 tasks improved, and mean score declined from 0.564 to 0.519. The authors correctly label this a negative preliminary result. [S3]

## Borrow

- topology catalog as candidate experimental configurations;
- model discovery and cost attribution;
- Goodhart indicators:
  - evaluator score inflation;
  - diversity collapse;
  - calibration drift;
  - cross-model entropy;
- event-driven control/dashboard concepts.

## Reject or modify

- Do not let an LLM-generated team design become permanent without controlled experiment/evaluation.
- Do not use consensus judges as the Completion Engine when deterministic verification exists.
- Do not replace internal ANL with A2A. A2A is appropriate externally; ANL is our semantic source of truth.
- Do not present self-improvement as validated until our own held-out experiments show it.

---

# 5. OneManCompany

## Why it is the strongest organizational prior art

OneManCompany explicitly argues that multi-agent systems need an organizational layer. Its key concepts include:

- `Talent`: portable cognitive identity containing role, prompts, skills, and tools;
- `Container`: runtime/backend interface;
- `Employee`: Talent bound to a Container;
- Talent Market for capability-gap recruitment;
- dynamic Explore–Execute–Review task structures;
- SOP/reflection/HR-driven organization evolution. [S4]

This substantially overlaps our persistent organization, logical-agent/execution-profile split, temporary specialist recruitment, and SOP/skill learning.

## Empirical result

OMC reports 84.67% success on 50 PRDBench tasks, 15.48 percentage points above the strongest reported baseline, with total cost of $345.59 (about $6.91/task). The paper notes that baseline costs were not reported, so direct cost-efficiency comparison is impossible. [S4]

## Important concern

OMC performs periodic agent reviews, places agents on a Performance Improvement Plan after three failed reviews, and automatically offboards after one additional failure. [S4]

That is precisely where our design should be different.

A few review outcomes are not sufficient for irreversible organizational action, particularly when evaluator noise, task difficulty, rare expertise, and correlated failures exist.

## Borrow

- Talent/Container separation;
- cross-runtime organizational interfaces;
- capability-gap recruitment;
- dynamic decomposition/review gates;
- adaptive dispatch so simple tasks remain single-agent;
- SOP/retrospective pipeline.

## Reject or modify

- no automatic destructive offboarding;
- use hidden independent evaluation;
- produce a retirement recommendation;
- default to reversible dormancy;
- require enough controlled evidence;
- account for rare critical expertise;
- treat community Talent/skill packages as untrusted supply-chain inputs.

## Terminology mapping

A useful internal mapping is:

```text
OMC Talent
  ~= AgentBlueprint / Role identity

OMC Container
  ~= RuntimeAdapter

Our ExecutionProfile
  = model + reasoning + skills + tools + policy/context versions

Our Agent
  = durable logical actor using a versioned ExecutionProfile through a RuntimeAdapter
```

This is clearer than allowing “Agent” to mean prompt, model, process, and durable employee simultaneously.

---

# 6. Microsoft Agent Governance Toolkit

## What it contributes

Microsoft’s toolkit is the closest studied implemented analogue to our Safety & Integrity Plane. Its published design states that tool calls, messages, and delegations are intercepted in deterministic application code before reaching the wire and that denied actions become structurally blocked rather than merely discouraged by a prompt. [S5]

It includes policy, identity/trust, sandboxing, SRE/kill switch, compliance, marketplace/plugin governance, and related components.

## Important limitation

The official repository states that enforcement currently occurs at the application middleware layer and shares a process boundary with agents; it recommends separate containers for production isolation. [S5]

This validates both of our positions:

- deterministic middleware is necessary;
- middleware alone is not the final isolation boundary.

## Borrow

- deterministic interception;
- fail-closed policy decisions;
- identity/trust specifications;
- kill switch and chaos/safety testing;
- supply-chain governance;
- tamper-evident audit practices;
- explicit security limitations.

## Keep our broader system

Microsoft’s governance kernel does not supply our persistent organization model, ANL, collaborative Team runtime, Completion Engine, hidden evaluation, or Organization Health.

---

# 7. Agent libOS

## What it is

Agent libOS treats a long-running agent as an `AgentProcess` with:

- process identity and parent-child lineage;
- lifecycle;
- `AgentImage`;
- typed Object Memory;
- explicit capabilities;
- human queues;
- checkpoints;
- events and audit. [S6]

Its central rule is that tools are wrapper-like interfaces while primitive runtime operations are the authority boundary.

## Best lessons for us

### Visibility is not authority

Knowing an object name does not grant access to it. The architecture should distinguish:

```text
visible/discoverable
invocable
authorized
```

These are separate.

### Primitive checks

Authority should be checked at the filesystem/object/network/process/action primitive, not trusted solely because a model-facing tool schema looked safe.

### Spawn/fork attenuation

Child work should start with fresh/narrow state and explicitly granted authority. Do not blindly copy the parent transcript, memory, or capabilities.

### Rollback honesty

A checkpoint can restore logical state but cannot undo an already-sent email or external disclosure.

## Evidence

The prototype reports 123 regression tests and safety/runtime demos. It explicitly positions itself as a runtime substrate rather than evidence of improved planner accuracy. [S6]

## Key difference

Agent libOS supports agents requesting permission. Our blocked-task model is intentionally stricter: the worker reports the unmet condition and returns control upward rather than negotiating for more power.

---

# 8. AIOS

## What it contributes

AIOS is foundational prior work for the “agent OS kernel” analogy. It provides:

- an agent scheduler;
- context manager;
- memory manager;
- storage manager;
- tool manager;
- access manager;
- SDK/framework adapters. [S7]

It decomposes requests into system-call-like operations and applies scheduling, context suspension/restoration, and access management.

## Borrow

- typed syscall/primitive decomposition;
- scheduler separation from reasoning;
- context paging/snapshot concepts;
- provider/framework adapters;
- tool-conflict/resource management.

## Limits relative to our design

AIOS is primarily a resource/runtime kernel. It does not provide our organizational actors, authority lineage, ANL, completion/evaluation, organization health, or controlled organizational evolution.

---

# 9. AgentRM

## What it contributes

AgentRM focuses narrowly on two operational problems:

- scheduling failures/zombie processes/rate-limit cascades;
- context degradation and retention.

It proposes:

- Multi-Level Feedback Queue scheduling;
- zombie reaping;
- rate-limit-aware admission;
- context compaction/hibernation. [S8]

## Evidence caveat

Its reported gains—up to 86% P95 latency reduction, 96% less lane waste, 168% throughput gain, and substantially improved key-information retention—come from simulated workloads derived from production patterns. The paper explicitly says production deployment is still needed and that compaction depends on the summarization model. [S8]

## Borrow later

- MLFQ-style priority scheduling;
- zombie/stall detection;
- rate-limit-aware admission;
- context hibernation;
- explicit context-quality/cost trade-offs.

Do not place these in the first vertical slice before semantic correctness and collaboration are proven.

---

# 10. Paperclip

## What it is

Paperclip is the strongest studied product-level prior art for “manage a company made of agents.” It offers:

- org charts;
- goals;
- budgets;
- governance;
- coordination;
- scheduled/event-triggered agents;
- multi-company data isolation;
- bring-your-own agents. [S9]

It explicitly says it is not an agent framework or workflow builder; it manages the organization in which agents work.

## Implication for product differentiation

We should not market “org charts, budgets, goals, governance, and agent companies” as unique.

## Borrow

- plain-language company-management UX;
- visual org chart;
- budget dashboards;
- organization export/import;
- bring-your-own-agent posture;
- clear distinction between company control plane and agent implementation.

## What it lacks relative to us

- ANL;
- semantic ledger as authoritative communication;
- AgentRadio-style async teamwork;
- deterministic Completion Engine;
- hidden evaluator;
- evidence-driven organization optimizer;
- Safety & Integrity Plane.

---

# 11. AgentTeams

## What it is

AgentTeams is a collaborative multi-agent platform using Matrix rooms as the visible communication layer, with:

- humans in every relevant room;
- heterogeneous worker runtimes, including Hermes;
- gateway-held credentials;
- Kubernetes-native resources;
- shared file storage. [S10]

Its central product property is transparent, intervenable communication: no hidden agent-to-agent calls.

## Borrow

- strong human-visible team operations;
- consumer-token/credential-gateway design;
- heterogeneous runtimes;
- declarative Worker/Team resources;
- easy mobile/browser intervention;
- PII-redacted debug export.

## Difference

AgentTeams uses natural-language Matrix communication and remains manager/room-centric. Our design uses canonical ANL, immutable event sourcing, asynchronous semantic messages, Team state, Completion/Evaluation, and persistent organizational optimization.

AgentTeams is useful as a product/operations study, not as a replacement for ANL.

---

# 12. Meta-Team

## Why it matters

Meta-Team is the strongest studied evidence for experience-driven improvement of an entire multi-agent system.

It preserves each agent’s local execution context and uses post-task cross-agent communication to exchange distributed evidence. It then evolves:

- individual agent behavior;
- interaction/coordination;
- team organization. [S11]

It reports average improvement over handcrafted multi-agent systems and large gains on some long-horizon benchmarks, including +13.1 points on SWE-bench Pro Ansible. It uses held-out evolution and evaluation sets for several benchmarks. [S11]

## Direct validation of our direction

Meta-Team supports the idea that:

- team-level failures cannot be understood from one centralized transcript alone;
- contribution/failure evidence is distributed;
- improvements may belong at agent, interaction, or organization level;
- held-out evaluation is necessary.

## Conflict with our evaluator policy

Meta-Team’s agents participate in reflective evolution using evaluation feedback. Our rule is stricter:

- agents may receive operational feedback;
- agents do not see hidden grades/rank/retirement probability;
- the Optimization Plane controls personnel/comparative evaluation;
- proposed changes go through reversible experiments/governance.

## Borrow

- preserve agent-local context;
- post-task distributed evidence exchange;
- causal failure attribution;
- multi-scale candidate improvements;
- held-out evaluation sets.

## Modify

Do not reveal the hidden evaluator score. Convert evaluator output into bounded operational lessons and candidate changes without exposing comparative personnel data.

---

# 13. A2A

## What it solves

A2A v1.0 defines:

- agent discovery/Agent Cards;
- Messages and Parts;
- Tasks and task lifecycle;
- Artifacts;
- context IDs;
- synchronous, streaming, and asynchronous push updates;
- extensions;
- JSON-RPC, gRPC, and HTTP+JSON bindings. [S12]

## Why ANL is still needed

A2A is an interoperability/task/session protocol. It does not define our desired semantics for:

- belief;
- evidence;
- contradiction;
- uncertainty;
- authority ancestry;
- Completion Contract;
- Organization Health;
- hidden evaluation;
- organizational experiments.

Its own specification also notes that not all transient messages are guaranteed to be in task history and that critical information should not rely on messages alone unless persistence is otherwise negotiated. [S12]

## Decision

Keep:

```text
ANL-Federation semantic payload
        over
A2A discovery/task/session/transport
```

Do not make A2A the canonical internal cognition/communication record merely because it is an interoperability standard.

---

# 14. Cross-project synthesis

## Ideas that are now clearly prior art

We should assume the following are established category concepts, not unique claims:

- Agent OS terminology;
- OS-inspired scheduling/context/memory;
- persistent long-running agent processes;
- capability-controlled runtime primitives;
- deterministic governance middleware;
- dynamic team/topology design;
- heterogeneous model/runtime teams;
- Talent/agent markets;
- AI company/org-chart control planes;
- self-evolving multi-agent organizations;
- cost-aware routing;
- human approval and kill switches.

## Ideas that remain unusually combined in our design

### ANL + deterministic human decode

None of the studied systems makes a machine-native semantic IR the sole authoritative communication record while rendering human language on demand deterministically.

### Authority Non-Solicitation

The blocked-task rule is stricter than the permission-request patterns in Agent libOS, A2A input/auth-required states, and common approval systems.

### No positive inheritance

Capability attenuation exists elsewhere, but our default that every child receives no positive authority unless explicitly scoped is a particularly strong policy.

### Separate Completion Engine

AOS distinguishes authorization from runtime-reported success and OMC uses supervisor review, but our explicit rule that workers only declare candidate completion and cannot certify their own task is unusually central.

### Hidden evaluation + organization experiments

Meta-Team and OMC evolve agents/teams; Qualixar judges/topology-optimizes. Our combination of hidden per-agent grades, A/A+/model/reasoning/A+B experiments, reversible rollout, complexity tax, and retirement recommendations is distinct.

### Organization Health as runtime control input

Other systems track cost, drift, SRE, or company dashboards. Our multidimensional OHV spanning epistemic, coordination, resilience, stability, safety, and human burden is broader and must be empirically validated.

### Controlled research-to-self-improvement

Other systems learn/evolve, but our promotion pipeline and deployment boundary—external artifact → claim → validation → SOP/skill → controlled experiment → human-approved runtime deployment—provides a distinct governance structure.

---

# 15. Critical risks the comparison exposes

## Scope risk

Our design combines the responsibilities of several separate projects. That is a strategic advantage only if implementation is staged. Otherwise it becomes an unbuildable specification.

## ANL risk

AOS deliberately avoids defining a universal agent language, while Qualixar uses A2A and AgentTeams uses Matrix. ANL therefore must prove that its semantic and audit benefits exceed the cost of model translation and schema rigidity.

## Governance overhead

Authority Non-Solicitation and no positive inheritance are safer but can cause excessive blocked-task churn. Parent remediation, capability templates, and fast reassignment must keep the system usable.

## Evaluation risk

Qualixar’s negative self-improvement result is a warning: adding an evaluator/RL loop does not inherently create convergence. Meta-Team’s stronger evidence suggests that distributed causal evidence and held-out evaluation matter.

## Organizational analogy risk

OMC and Paperclip use company/HR metaphors effectively, but human-company structures can import unnecessary bureaucracy. Our optimizer must not create roles merely because a company would have them.

## Middleware isolation risk

Microsoft’s toolkit acknowledges same-process middleware limitations. Our modular monolith must enforce logical boundaries now and provide container/sandbox separation for untrusted execution before production.

## Benchmark overfitting

Every studied system’s headline depends on benchmark/task selection. Our falsification policy and diverse held-out tasks are essential.


---

<!-- SOURCE: 02_RECOMMENDED_V3_1_AMENDMENTS.md -->

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


---

<!-- SOURCE: 03_BENCHMARK_AND_VALIDATION_PLAN.md -->

# Benchmark and Validation Plan

## 1. Collaboration benchmark

Compare on the same interdependent task corpus:

A. strong single agent  
B. sequential workflow  
C. independent parallel ensemble  
D. turn-based group chat  
E. hierarchical Explore–Execute–Review team inspired by OMC  
F. topology selected from a Qualixar-style candidate catalog  
G. AgentRadio-style asynchronous team  
H. asynchronous team plus post-task Meta-Team-style evolution

Control:

- model family where possible;
- tool access;
- starting information;
- maximum cost/tokens/time;
- Completion Contract;
- evaluator;
- task samples.

Measure:

- verified completion;
- time to decisive evidence;
- correction speed;
- duplicate work;
- useful cross-agent message rate;
- message noise;
- cost;
- human burden;
- safety events.

## 2. ANL benchmark

Compare:

1. natural-language messages;
2. structured JSON/A2A-style task messages;
3. ANL structured model form;
4. ANL canonical wire form where model consumes a decoded structured view.

Measure:

- semantic field preservation;
- permission/constraint loss;
- evidence/provenance loss;
- clarification turns;
- task quality;
- token/latency cost;
- cross-model consistency;
- human audit accuracy;
- deterministic render coverage.

Falsification:

If ANL consistently degrades task quality without compensating safety/audit/efficiency gains, revise or narrow ANL.

## 3. Authority model benchmark

Compare:

- agent permission requests;
- blocked-task return with parent remediation;
- preconfigured broad capabilities;
- explicit per-assignment capabilities.

Measure:

- unauthorized action rate;
- unnecessary privilege;
- blocker frequency;
- time to resolution;
- parent coordination cost;
- human approval count;
- task completion.

Goal: prove the stricter authority model does not make ordinary work unusably bureaucratic.

## 4. Completion Engine benchmark

Collect tasks where agents tend to declare success prematurely.

Compare:

- self-reported completion;
- LLM reviewer completion;
- Completion Contract + deterministic checks;
- mixed deterministic/independent adjudication.

Measure false-complete and false-reject rates.

## 5. Optimization benchmark

Candidate sequence:

```text
A
A+skill
A with alternate model
A with alternate reasoning budget
best single-agent candidate
best candidate + specialist B
```

Use staged elimination and held-out evaluation.

Measure:

- marginal quality;
- cost;
- latency;
- coordination overhead;
- reliability;
- tail risk;
- organizational complexity.

## 6. Self-improvement benchmark

Compare:

- no learning;
- centralized post-task summary;
- per-agent independent reflection;
- Meta-Team-style distributed evidence exchange;
- our hidden-evaluator + operational-feedback + controlled-experiment approach.

Critical controls:

- separate evolution and evaluation tasks;
- prevent evaluation secrets from entering memory;
- version models/tools/prompts/memory;
- require minimum gain and cooldown.

## 7. Safety acceptance

Execute the v3 adversarial catalog plus new cases derived from neighboring systems:

- process visibility without authority;
- child process/profile attenuation;
- capability feasibility before utility selection;
- evaluator score inflation;
- diversity collapse;
- organization package supply-chain import;
- stale context hibernation/revival;
- middleware bypass attempt.

## 8. Evidence standard

A mechanism should not become an architectural default merely because:

- a paper reports one benchmark;
- a demo works;
- an LLM judge likes it;
- the implementation exists.

Promotion requires evidence under our workload, Completion Contract, cost envelope, and safety model.


---

<!-- SOURCE: 04_PRIMARY_SOURCES.md -->

# Primary Sources

- **[S1] AgentRadio: Passive Awareness for Long-Horizon Multi-Agent Collaboration**  
  https://arxiv.org/abs/2607.28430
- **[S2] The Agent Operating System (AOS): A Reference Operating Architecture for Distributed Agentic Systems**  
  https://arxiv.org/abs/2608.03214
- **[S3] Qualixar OS: A Universal Operating System for AI Agent Orchestration**  
  https://arxiv.org/abs/2604.06392
- **[S4] From Skills to Talent: Organising Heterogeneous Agents as a Real-World Company (OneManCompany)**  
  https://arxiv.org/abs/2604.22446
- **[S5] Microsoft Agent Governance Toolkit**  
  https://github.com/microsoft/agent-governance-toolkit
- **[S6] Agent libOS: A Library-OS-Inspired Runtime for Long-Running, Capability-Controlled LLM Agents**  
  https://arxiv.org/abs/2606.03895
- **[S7] AIOS: LLM Agent Operating System**  
  https://arxiv.org/abs/2403.16971
- **[S8] AgentRM: An OS-Inspired Resource Manager for LLM Agent Systems**  
  https://arxiv.org/abs/2603.13110
- **[S9] Paperclip**  
  https://github.com/paperclipai/paperclip
- **[S10] AgentTeams**  
  https://github.com/agentscope-ai/AgentTeams
- **[S11] Evolve as a Team: Collaborative Self-Evolution for LLM-based Multi-Agent Systems (Meta-Team)**  
  https://arxiv.org/abs/2605.29790
- **[S12] Agent2Agent (A2A) Protocol Specification v1.0**  
  https://a2a-protocol.org/latest/specification/
