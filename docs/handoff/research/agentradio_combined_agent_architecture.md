# AgentRadio-Centered Multi-Agent Architecture

## Purpose

This document describes a practical multi-agent AI architecture that combines the core idea from **AgentRadio: Passive Awareness for Long-Horizon Multi-Agent Collaboration** with several complementary mechanisms borrowed from recent agent research.

The central design goal is to build an agent system that is:

- capable of long-horizon work,
- efficient with compute,
- resilient to context degradation,
- able to learn from experience without full model retraining,
- able to convert repeated successful behavior into reusable skills,
- able to verify that actions actually changed the world as intended,
- and able to coordinate without forcing every agent to stop at global synchronization barriers.

The architecture is not a reproduction of any one paper. It is a synthesis.

---

# 1. Core Design Principle

The system should not be built as:

```text
One giant agent
    + huge context
    + every tool
    + every memory
    + every responsibility
```

Nor should it be built as:

```text
Manager
  -> Worker A
  -> Worker B
  -> Worker C

Workers return final results only to the manager.
```

Instead, the architecture should treat intelligence as a distributed system composed of specialized processes.

A strong planner decomposes and replans work. Specialized workers execute. Workers communicate laterally through an asynchronous message layer. Separate systems manage memory, context, verification, and skill promotion.

The result resembles an AI operating system more than a collection of chatbots.

---

# 2. AgentRadio as the Communication Backbone

Source:

- **AgentRadio: Passive Awareness for Long-Horizon Multi-Agent Collaboration**
- arXiv:2607.28430
- https://arxiv.org/abs/2607.28430

## 2.1 The problem AgentRadio addresses

Many multi-agent frameworks coordinate through blocking synchronization.

A typical pattern looks like:

1. agents work independently,
2. agents stop,
3. agents exchange results,
4. a manager reconciles them,
5. agents resume.

This is similar to requiring a human engineering team to stop working every time someone has useful information.

AgentRadio instead introduces passive awareness.

An agent can continue working while messages from other agents become available at action boundaries. The agent does not need to explicitly stop and poll for information.

Conceptually:

```text
Worker A executing task
        |
        |<---- message from Worker C
        |
next action boundary
        |
message becomes visible
        |
Worker A incorporates it
        |
continues execution
```

This is the architectural foundation of the proposed system.

## 2.2 Why lateral communication matters

The coordinator should not be a mandatory relay for all information.

Instead:

```text
                 Coordinator
                      |
          initial decomposition/replan
                      |
       +--------------+--------------+
       |              |              |
    Worker A       Worker B       Worker C
       |              |              |
       +-------- AgentRadio ---------+
              asynchronous bus
```

Workers should be allowed to communicate directly when:

- they discover a dependency,
- they invalidate a shared assumption,
- they find evidence relevant to another workstream,
- they identify a conflict,
- they require a capability another worker owns,
- or they discover a result that changes the global plan.

The coordinator still exists, but it is no longer the communication bottleneck.

---

# 3. Strong Planner, Cheaper Workers

Borrowed idea:

- **Planner Matters! An Efficient and Unbalanced Multi-agent Collaboration Framework for Long-horizon Planning**
- arXiv:2605.02168

## 3.1 Unequal model allocation

Not every role requires the same level of intelligence.

The system should deliberately use an **unbalanced model hierarchy**.

Example:

```text
Planner / Coordinator
    strongest available model

Investigative Workers
    medium or strong models

Routine Executors
    smaller models

Memory Manager
    smaller specialized model

Context Manager
    smaller specialized model

Verification Agents
    model sized to verification difficulty
```

The planner gets disproportionate compute because planning quality affects every downstream action.

A memory cleanup task may not justify the same model that performs strategic decomposition.

## 3.2 Planner responsibilities

The planner should own:

- task decomposition,
- worker assignment,
- dependency analysis,
- replanning,
- global constraint tracking,
- escalation decisions,
- integration strategy,
- stop/continue decisions,
- and final acceptance criteria.

The planner should **not** micromanage every action.

Workers need bounded autonomy.

---

# 4. Explicit Event and State Log

Agent communication should not be the only persistent record.

Every meaningful system event should be recorded in a structured event log.

Example:

```json
{
  "event_id": "evt_018291",
  "timestamp": "...",
  "actor": "worker_security_1",
  "task_id": "task_42",
  "event_type": "evidence_discovered",
  "summary": "Authentication bypass requires legacy compatibility mode",
  "artifact_refs": ["file://...", "trace://..."],
  "confidence": 0.91
}
```

The event log provides:

- durable auditability,
- replay,
- debugging,
- memory extraction,
- skill learning,
- planner observability,
- and post-task analysis.

AgentRadio carries **live awareness**.

The event log carries **durable history**.

They serve different purposes.

---

# 5. Separate Context Management

Borrowed ideas:

- **Learning Agent-Compatible Context Management for Long-Horizon Tasks**
- arXiv:2605.30785

- **Active Context Compression: Autonomous Memory Management in LLM Agents**
- arXiv:2601.07190

## 5.1 Why agents should not manage all of their own context

A long-running worker gradually accumulates:

- stale hypotheses,
- completed subtasks,
- tool output,
- logs,
- repeated instructions,
- irrelevant branches,
- intermediate drafts,
- and old state.

Simply appending everything eventually degrades reasoning.

The architecture should therefore include a **Context Manager** separate from the worker.

```text
Worker context
     |
     v
Context Manager
     |
     +--> preserve constraints
     +--> preserve unresolved questions
     +--> preserve active plan
     +--> preserve key evidence
     +--> discard stale execution noise
     +--> replace detail with references
```

## 5.2 Context layers

A useful working context can be separated into:

### Immutable task constraints

Requirements that must never silently disappear.

### Active objective

What the agent is currently trying to accomplish.

### Current plan

The working decomposition or procedure.

### Key evidence

Only evidence relevant to current decisions.

### Recent actions

Enough recent history to preserve local continuity.

### Referenced external state

Pointers to artifacts, logs, files, traces, or memory objects.

This minimizes the need to continuously resend raw history.

## 5.3 Explicit compression events

When context pressure increases, the Context Manager can produce a checkpoint:

```text
CHECKPOINT

Goal:
...

Completed:
...

Verified facts:
...

Rejected hypotheses:
...

Open questions:
...

Dependencies:
...

Relevant artifacts:
...

Next actions:
...
```

The original raw history remains in storage, but the worker receives the compact checkpoint.

---

# 6. Hierarchical Memory Rather Than Flat Retrieval

Borrowed idea:

- **From Memory to Skills: Evidence-Grounded Co-Evolution Governance for Long-Horizon LLM Agents**
- arXiv:2607.16621

A single vector database containing every old message is not sufficient.

The system should maintain multiple memory layers.

## 6.1 L1: Episodes / Evidence

Raw or minimally processed observations.

Examples:

- execution traces,
- test output,
- discovered API behavior,
- failed approaches,
- successful tool calls,
- screenshots,
- source excerpts,
- benchmark results.

L1 answers:

> What actually happened?

## 6.2 L2: Policies / Heuristics

Patterns extracted from repeated episodes.

Examples:

- "When this compiler error appears, inspect generated bindings before changing source code."
- "Before submitting a browser workflow, verify the final page state rather than trusting the click."
- "When agents disagree on runtime behavior, favor reproducible execution evidence."

L2 answers:

> What generally seems to work?

## 6.3 L3: Environmental Knowledge

Stable knowledge about the operating environment.

Examples:

- repository structure,
- API limitations,
- tool semantics,
- organizational conventions,
- hardware topology,
- deployment characteristics.

L3 answers:

> What is true about the environment?

## 6.4 L4: Skills

Validated procedures that can be executed intentionally.

Examples:

```text
verify_repository_build
reproduce_bug
inspect_service_health
generate_release_candidate
compare_pre_and_post_gui_state
```

L4 answers:

> What procedure can the system reliably perform?

---

# 7. Skill Crystallization

Borrowed ideas:

- **Harnessing LLM Agents with Skill Programs**
- arXiv:2605.17734

- **Formal Skill: Programmable Runtime Skills for Efficient and Accurate LLM Agents**
- arXiv:2605.19604

The architecture should progressively move repeated behavior out of natural-language prompting and into executable skills.

## 7.1 Skill maturity ladder

```text
Observation
    |
Repeated successful behavior
    |
Heuristic
    |
Candidate skill
    |
Validated procedure
    |
Executable skill
    |
Monitored production skill
```

## 7.2 A skill should contain more than instructions

A mature skill could have:

```yaml
name: reproduce_http_failure
version: 3
description: Reproduce and capture a suspected HTTP failure.
eligibility:
  - service_is_running
  - target_endpoint_known
inputs:
  - endpoint
  - request_payload
actions:
  - execute_request
  - capture_response
  - capture_service_logs
verification:
  - response_is_recorded
  - logs_are_correlated
failure_modes:
  - timeout
  - service_unavailable
  - authentication_required
reliability:
  successful_runs: 84
  failed_runs: 7
evidence_refs:
  - memory://episode/...
```

That is fundamentally more useful than:

```text
Remember to reproduce the bug carefully and check logs.
```

## 7.3 Code should replace repeatedly solved reasoning

If the agent repeatedly reasons through the same deterministic process, eventually that reasoning should become code.

Examples:

- schema validation,
- file existence checks,
- test execution,
- retry logic,
- diff inspection,
- artifact checksums,
- dependency resolution,
- structured extraction,
- status polling.

Models should spend inference on ambiguity, not repeatedly rediscover deterministic procedures.

---

# 8. Verification as a First-Class Subsystem

Borrowed idea:

- **VisCritic: Visual State Comparison as Process Reward for GUI Agents**
- arXiv:2606.24525

The underlying principle generalizes beyond GUI tasks.

Every important action should follow:

```text
state_before
     |
   action
     |
state_after
     |
 verifier
     |
 success / retry / replan / escalate
```

## 8.1 Verification by environment

### Code

- compile,
- unit tests,
- integration tests,
- static analysis,
- git diff,
- runtime trace.

### Shell / operating system

- process state,
- exit codes,
- file hashes,
- filesystem state,
- service health.

### Browser

- DOM state,
- page URL,
- screenshot comparison,
- backend response.

### API

- status code,
- returned object,
- follow-up GET,
- idempotency check.

### Data pipeline

- row counts,
- schema checks,
- invariants,
- checksum,
- sampled output validation.

### Multi-agent reasoning

- independent reproduction,
- evidence comparison,
- contradiction detection.

The system should avoid treating:

> "The agent attempted the action"

as equivalent to:

> "The action succeeded."

---

# 9. Learn Workflows, Then Stop Re-Reasoning Them

Borrowed idea:

- **OS-Marathon: Benchmarking Computer-Use Agents on Long-Horizon Repetitive Tasks**
- arXiv:2601.20650

For repetitive workloads, the first few cases may require significant reasoning.

The hundredth case usually should not.

The system should detect repetitive structure:

```text
examples 1-5:
    explore and reason heavily

patterns discovered:
    extract workflow

examples 6+:
    execute distilled procedure

exceptions:
    escalate back to reasoning mode
```

This introduces two operating modes:

### Discovery mode

High reasoning cost.

### Procedure mode

Low reasoning cost with strict verification.

This is one of the most important ways to make autonomous systems economically practical.

---

# 10. Scale Experience Before Scaling Team Size

Borrowed idea:

- **Scaling Teams or Scaling Time? Memory Enabled Lifelong Learning in LLM Multi-Agent Systems**
- arXiv:2604.03295

More agents are not automatically better.

Every additional agent creates:

- inference cost,
- communication traffic,
- coordination complexity,
- duplicated exploration,
- potential disagreement,
- and additional failure modes.

The default optimization loop should therefore be:

```text
Improve memory
    ->
Improve skills
    ->
Improve context management
    ->
Improve verification
    ->
Improve communication
    ->
Only then consider adding agents
```

A four-agent team that has accumulated excellent skills may be more capable than a sixteen-agent team that starts from scratch every time.

---

# 11. Communication Protocol Layer

Borrowed idea:

- **A Comparative Study of MCP and A2A for Inter-Agent Coordination in LLM-Based Systems**
- arXiv:2607.23884

The agent system should distinguish between:

### Tool protocol

How an agent invokes resources.

Examples:

- MCP-like tool discovery and invocation,
- local RPC,
- typed function calls.

### Agent communication protocol

How agents exchange:

- messages,
- task state,
- requests,
- replies,
- subscriptions,
- mentions,
- coordination state.

These are not necessarily the same protocol.

AgentRadio-style passive awareness can sit above either.

---

# 12. Proposed Reference Architecture

```text
                         ┌──────────────────────┐
                         │   STRONG PLANNER     │
                         │ / COORDINATOR        │
                         └─────────┬────────────┘
                                   │
                       decomposition / replan
                                   │
          ┌────────────────────────┼────────────────────────┐
          │                        │                        │
          ▼                        ▼                        ▼
   ┌──────────────┐         ┌──────────────┐        ┌──────────────┐
   │   Worker A   │         │   Worker B   │        │   Worker C   │
   │ specialized  │         │ specialized  │        │ specialized  │
   └──────┬───────┘         └──────┬───────┘        └──────┬───────┘
          │                        │                        │
          └──────────── AgentRadio-style async bus ───────┘
                                   │
                                   ▼
                         ┌──────────────────┐
                         │ EVENT / STATE LOG│
                         └────────┬─────────┘
                                  │
           ┌──────────────────────┼────────────────────────┐
           │                      │                        │
           ▼                      ▼                        ▼
 ┌─────────────────┐    ┌────────────────────┐    ┌─────────────────┐
 │ Context Manager │    │ Hierarchical Memory│    │ Verification    │
 │                 │    │                    │    │ / Critics       │
 │ compress/prune  │    │ L1 Episodes        │    │                 │
 │ preserve state  │    │ L2 Policies        │    │ state-before    │
 │ checkpoint      │    │ L3 Knowledge       │    │ state-after     │
 └─────────────────┘    │ L4 Skills          │    │ accept/retry    │
                        └─────────┬──────────┘    └─────────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │  SKILL RUNTIME   │
                         │                  │
                         │ manifests        │
                         │ executable code  │
                         │ hooks            │
                         │ local state      │
                         │ eligibility      │
                         │ verification     │
                         └──────────────────┘
```

---

# 13. Suggested Agent Roles

## Planner

High-capability model.

Responsibilities:

- understand objectives,
- decompose work,
- assign workers,
- define evidence requirements,
- track dependencies,
- replan when assumptions fail,
- decide when to add/remove workers,
- decide when work is complete.

## Worker

Domain-focused execution agent.

Responsibilities:

- pursue assigned objective,
- use tools,
- communicate discoveries laterally,
- produce evidence,
- invoke skills,
- request help when blocked.

## Context Manager

Optimizes active context.

Responsibilities:

- checkpoint,
- compress,
- prune,
- maintain hard constraints,
- recover referenced historical detail.

## Memory Curator

Promotes useful experience through memory layers.

Responsibilities:

- extract episodes,
- deduplicate,
- identify reusable policies,
- maintain environmental facts,
- propose candidate skills.

## Skill Validator

Tests candidate skills.

Responsibilities:

- verify applicability,
- run regression examples,
- record success/failure,
- detect outdated procedures,
- approve promotion.

## Critic / Verifier

Confirms actual state transitions.

Responsibilities:

- evaluate action outcome,
- detect divergence,
- classify failures,
- trigger retry or replanning.

These roles do not necessarily require one dedicated model process each. Some can be services or opportunistic agent invocations.

---

# 14. Message Types for AgentRadio

Communication should be typed rather than purely conversational.

Recommended message classes:

```text
DISCOVERY
CONTRADICTION
DEPENDENCY
REQUEST
RESPONSE
PLAN_CHANGE
BLOCKER
EVIDENCE
WARNING
HANDOFF
SKILL_AVAILABLE
VERIFICATION_FAILURE
```

Example:

```json
{
  "type": "CONTRADICTION",
  "from": "worker_runtime",
  "to": ["worker_security", "planner"],
  "task_id": "auth-investigation",
  "summary": "Observed runtime path bypasses function assumed by current plan.",
  "evidence": ["trace://4821"],
  "confidence": 0.96,
  "urgency": "high"
}
```

Typed messages help routing, prioritization, observability, and later learning.

---

# 15. Interrupt Policy

Passive awareness does not mean every message should immediately hijack the recipient.

Messages should have priority.

Example:

```text
P0 - stop immediately
critical safety issue or invalid global assumption

P1 - surface at next action boundary
important evidence or dependency

P2 - surface before current subtask completes
relevant but non-critical

P3 - archive for later
informational
```

Without prioritization, asynchronous collaboration can become attention spam.

---

# 16. Evidence-Based Consensus

Agents should not resolve disagreement through simple majority vote.

Preferred hierarchy:

```text
reproducible evidence
    >
verified tool output
    >
independent corroboration
    >
reasoned inference
    >
unsupported agent opinion
```

If Worker A says:

> "The service uses configuration X."

and Worker B produces a reproducible runtime trace showing configuration Y, the system should favor the evidence.

Consensus must be evidence weighted.

---

# 17. Task Lifecycle

A complete task can follow:

## Phase 1: Intake

- normalize objective,
- record immutable constraints,
- define completion criteria.

## Phase 2: Explore

- planner and selected workers inspect environment,
- identify unknowns,
- retrieve relevant memory and skills.

## Phase 3: Decompose

- planner builds task graph,
- assign workers,
- define communication dependencies.

## Phase 4: Execute

- workers operate independently,
- AgentRadio carries passive awareness,
- actions enter event log,
- verification occurs continuously.

## Phase 5: Replan

Triggered by:

- contradiction,
- failed verification,
- dependency change,
- unexpected environment state,
- low confidence,
- worker blockage.

## Phase 6: Integrate

- merge worker results,
- resolve conflicts using evidence,
- run global verification.

## Phase 7: Learn

- extract useful episodes,
- update policies,
- update environmental knowledge,
- propose or refine skills.

## Phase 8: Close

- final acceptance checks,
- artifact delivery,
- event-log completion,
- memory checkpoint.

---

# 18. Failure Modes and Mitigations

## Communication overload

**Problem:** too many agent messages degrade focus.

**Mitigation:** typed messages, priorities, subscriptions, recipient filtering.

## Shared false belief

**Problem:** all agents converge on the same wrong assumption.

**Mitigation:** independent verification, adversarial critic, executable evidence.

## Context corruption

**Problem:** important constraints disappear during compression.

**Mitigation:** immutable constraint store separate from summarization.

## Skill ossification

**Problem:** previously successful skills become outdated.

**Mitigation:** versioning, environmental compatibility checks, reliability decay, regression tests.

## Planner bottleneck

**Problem:** coordinator becomes overloaded.

**Mitigation:** workers communicate laterally; planner handles strategy rather than message forwarding.

## Excessive agent count

**Problem:** additional agents cost more than they contribute.

**Mitigation:** measure marginal value of each worker; scale skills and experience before headcount.

## Verification recursion

**Problem:** endless verifier-of-verifier loops.

**Mitigation:** deterministic checks wherever possible and bounded verification policies.

---

# 19. Compute Strategy

The system should allocate compute according to uncertainty and leverage.

## High compute

Use strong models for:

- decomposition,
- ambiguity,
- novel reasoning,
- conflict resolution,
- strategic replanning,
- high-impact decisions.

## Medium compute

Use capable but cheaper models for:

- domain investigation,
- synthesis,
- complicated tool work.

## Low compute

Use smaller models or deterministic services for:

- memory indexing,
- schema extraction,
- routine execution,
- status classification,
- context compression,
- simple verification.

## No model

Prefer normal software for:

- hashing,
- parsing,
- tests,
- retries,
- scheduling,
- file operations,
- dependency graphs,
- state machines,
- policy enforcement.

This is important: a sophisticated agent architecture should contain **less unnecessary LLM inference**, not more.

---

# 20. Local Compute Implications

This architecture is particularly attractive for local inference systems.

With API pricing, multiple agents can multiply token cost dramatically.

With owned hardware, the principal costs become:

- hardware,
- power,
- latency,
- utilization,
- and opportunity cost.

That changes the optimization problem.

Local systems can keep:

- long-running workers,
- persistent memory services,
- dedicated critics,
- background skill validation,
- and continuous event processing

without paying an API fee for every token.

However, compute still should not be wasted. Smaller models and deterministic services remain valuable because they free the strongest accelerator capacity for the decisions that matter.

---

# 21. Minimum Viable Implementation

A first implementation does not need every component.

## Stage 1

Build:

- planner,
- 2-4 workers,
- AgentRadio-style asynchronous communication,
- shared event log,
- deterministic verification hooks.

## Stage 2

Add:

- Context Manager,
- basic hierarchical memory,
- task checkpoints.

## Stage 3

Add:

- policy extraction,
- skill candidate generation,
- executable skill manifests,
- reliability tracking.

## Stage 4

Add:

- dynamic model routing,
- adaptive worker count,
- automatic skill promotion/demotion,
- richer evidence-based consensus.

The important point is to establish the **communication, state, and verification substrate first**.

---

# 22. Core Architectural Thesis

The strongest combined lesson from these papers is:

> Long-horizon agent capability will likely come as much from system architecture as from larger models.

A capable system should distribute responsibility across:

- planning,
- execution,
- communication,
- context,
- memory,
- skills,
- and verification.

AgentRadio provides the nervous system.

The planner provides strategic direction.

Workers provide parallel specialized execution.

The context manager protects attention.

Hierarchical memory gives the system accumulated experience.

Skill crystallization turns successful reasoning into reusable machinery.

Verification ties model intentions back to actual state.

Together, these mechanisms create a system that can become more capable over time without requiring every improvement to come from a larger foundation model.

---

# 23. Source Papers

1. **AgentRadio: Passive Awareness for Long-Horizon Multi-Agent Collaboration**  
   arXiv:2607.28430  
   https://arxiv.org/abs/2607.28430

2. **From Memory to Skills: Evidence-Grounded Co-Evolution Governance for Long-Horizon LLM Agents**  
   arXiv:2607.16621  
   https://arxiv.org/abs/2607.16621

3. **Planner Matters! An Efficient and Unbalanced Multi-agent Collaboration Framework for Long-horizon Planning**  
   arXiv:2605.02168  
   https://arxiv.org/abs/2605.02168

4. **Harnessing LLM Agents with Skill Programs**  
   arXiv:2605.17734  
   https://arxiv.org/abs/2605.17734

5. **Learning Agent-Compatible Context Management for Long-Horizon Tasks**  
   arXiv:2605.30785  
   https://arxiv.org/abs/2605.30785

6. **Formal Skill: Programmable Runtime Skills for Efficient and Accurate LLM Agents**  
   arXiv:2605.19604  
   https://arxiv.org/abs/2605.19604

7. **Scaling Teams or Scaling Time? Memory Enabled Lifelong Learning in LLM Multi-Agent Systems**  
   arXiv:2604.03295  
   https://arxiv.org/abs/2604.03295

8. **A Comparative Study of MCP and A2A for Inter-Agent Coordination in LLM-Based Systems**  
   arXiv:2607.23884  
   https://arxiv.org/abs/2607.23884

9. **VisCritic: Visual State Comparison as Process Reward for GUI Agents**  
   arXiv:2606.24525  
   https://arxiv.org/abs/2606.24525

10. **OS-Marathon: Benchmarking Computer-Use Agents on Long-Horizon Repetitive Tasks**  
    arXiv:2601.20650  
    https://arxiv.org/abs/2601.20650

11. **Active Context Compression: Autonomous Memory Management in LLM Agents**  
    arXiv:2601.07190  
    https://arxiv.org/abs/2601.07190
