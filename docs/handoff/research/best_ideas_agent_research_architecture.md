# Best-Ideas Agent Architecture
## A research-derived reference architecture for practical long-horizon AI agents

**Research pass:** arXiv agent literature spanning foundational work through August 2026  
**Design objective:** combine the strongest implementable ideas rather than reproduce any single framework

---

# Executive Summary

The most useful lesson from the agent literature is that a capable autonomous system should **not** be modeled as one language model repeatedly choosing the next action from an ever-growing transcript.

The stronger architecture is a distributed execution system with explicit control planes:

1. **Intent and Safety Plane** — converts the user's request into an immutable task contract and grants only the capabilities required for that task.
2. **Strategic Planning Plane** — uses the strongest reasoning model to decompose the objective, build a dependency graph, allocate compute, and replan.
3. **Execution Kernel** — schedules ready tasks asynchronously, manages parallelism, checkpoints state, and enforces bounded recovery.
4. **Specialist Worker Pool** — executes narrow work using the cheapest model or deterministic program capable of doing it reliably.
5. **Coordination Plane** — combines AgentRadio-style passive direct messaging with a shared structured blackboard.
6. **Context Plane** — creates task-specific working views without confusing a model's current context with durable memory.
7. **Memory Plane** — separates episodic evidence, semantic/environment knowledge, policies, and executable skills.
8. **Skill Runtime** — promotes repeated successful reasoning into versioned, tested, executable procedures.
9. **Verification Plane** — checks state transitions and milestones rather than trusting that an attempted action succeeded.
10. **Provenance Plane** — records why every consequential claim and action exists and which evidence supports it.
11. **Learning Plane** — converts completed trajectories into better policies, prompts, memory behavior, and skills.
12. **Evaluation Plane** — measures actual environment outcomes, safety, constraint satisfaction, cost, and recovery behavior.

The architecture can be summarized as:

```text
                           USER OBJECTIVE
                                 |
                                 v
                      +----------------------+
                      | INTENT / TASK CONTRACT|
                      | constraints, success  |
                      | criteria, permissions |
                      +----------+-----------+
                                 |
                    policy / provenance gate
                                 |
                                 v
                    +-------------------------+
                    | STRATEGIC PLANNER       |
                    | strongest reasoning     |
                    | model / solver hybrid   |
                    +------------+------------+
                                 |
                        versioned Task DAG
                                 |
                                 v
                 +-------------------------------+
                 | ASYNC EXECUTION KERNEL        |
                 | scheduler / checkpoints /     |
                 | budgets / recovery controller |
                 +------+------------+-----------+
                        |            |
              ready task|            |replan event
                        v            |
        +---------------+------------+---------------+
        |               |            |               |
        v               v            v               v
   Specialist A    Specialist B  Specialist C   Deterministic
      worker          worker        worker        skill/tool
        |               |            |               |
        +---------------+------------+---------------+
                        |
              AgentRadio message fabric
                        |
             +----------+-----------+
             |                      |
             v                      v
       DIRECT MESSAGES        SHARED BLACKBOARD
       targeted/urgent        structured shared state
             |                      |
             +----------+-----------+
                        |
                        v
                 APPEND-ONLY EVENT LOG
                        |
        +---------------+------------------+
        |               |                  |
        v               v                  v
   CONTEXT PLANE    MEMORY PLANE      PROVENANCE GRAPH
                        |
          +-------------+--------------+
          |             |              |
          v             v              v
       Episodic       Semantic      Procedural
       evidence       knowledge      policies
                                        |
                                        v
                                  SKILL RUNTIME
                                  code + contracts
                                  tests + versions
                                        |
                                        v
                                VERIFICATION PLANE
                                state before/after
                                milestone critics
                                        |
                              success / retry / replan
```

The remainder of this document explains why each component exists and how they fit together.

---

# 1. Design Rules Derived from the Literature

## Rule 1: Context is not memory

A model's active prompt should be treated as a **temporary task view**, not as the canonical record of everything the system knows.

Long contexts accumulate stale hypotheses, repeated output, old failures, and irrelevant action traces. Research on active context compression, agent-compatible context management, dual-process memory, and unified memory control consistently points toward explicit context governance.

Therefore:

```text
canonical state != current prompt
```

The current prompt is generated from canonical state.

---

## Rule 2: Plans should exist outside the model

A plan buried inside conversation text is hard to inspect, schedule, parallelize, recover, or verify.

The system should represent work as a typed dependency graph:

```text
Task
  id
  objective
  prerequisites
  assigned_role
  required_capabilities
  input_artifacts
  success_criteria
  verification_method
  budget
  status
```

The model may create and revise plans, but the runtime owns their representation.

---

## Rule 3: Replanning should create a new plan version

Research simultaneously supports structured/immutable execution graphs and dynamic replanning.

The clean reconciliation is:

> **A plan version is immutable once execution begins. Replanning creates a new version.**

Example:

```text
Plan v17
  T1 -> T2 -> T4
     \-> T3 -/

contradiction discovered

Plan v18
  preserves completed T1
  invalidates T2
  adds T2b and T5
  records why v17 was superseded
```

This preserves auditability without sacrificing adaptability.

---

## Rule 4: Use a strong planner and cheaper execution

"Planner Matters!" provides an important compute-allocation result: the planner often benefits more from stronger model capacity than support roles.

The runtime should therefore allocate models by **decision leverage**.

Use the strongest model for:

- decomposition,
- ambiguous decisions,
- cross-domain reasoning,
- replanning,
- constraint resolution,
- conflict resolution,
- final synthesis.

Use cheaper models for:

- focused investigation,
- routine transformations,
- retrieval,
- classification,
- context compression,
- predictable tool flows.

Use no model when normal software is sufficient.

---

## Rule 5: Parallelize independent uncertainty, not everything

More agents do not automatically improve results.

Parallelism is useful when:

- tasks are genuinely independent,
- different hypotheses should be explored,
- multiple information sources can be inspected concurrently,
- specialist expertise is useful,
- verification benefits from independent replication.

Parallelism is wasteful when:

- agents duplicate the same simple task,
- every agent needs all other agents' output first,
- communication dominates work,
- the problem is deterministic and cheaply solvable with code.

---

## Rule 6: Agents need lateral communication

AgentRadio's core contribution is important because manager-mediated communication creates a bottleneck.

Workers should be able to send information directly to peers while those peers continue execution.

However, unrestricted chat is not enough.

The architecture needs three distinct coordination mechanisms:

### Direct message

For targeted, time-sensitive information.

### Blackboard

For shared structured state relevant to many agents.

### Event log

For durable historical truth and replay.

These should not be conflated.

---

# 2. Intent and Task Contract Plane

Before any agent acts, the user's natural-language objective should be normalized into a structured contract.

Example:

```yaml
task_id: task_8821
objective: "..."
success_criteria:
  - ...
hard_constraints:
  - ...
forbidden_actions:
  - ...
allowed_resources:
  - filesystem.read:/workspace/project
  - git.write:/workspace/project
  - network.read:docs.example.com
budget:
  wall_clock: ...
  model_compute: ...
  tool_calls: ...
human_approval_required:
  - destructive_external_action
```

The task contract is stored separately from agent context and cannot be silently rewritten by a worker.

## Why

Agent-safety research repeatedly shows that prompt-level safety alone is insufficient. Tool-using agents need deterministic runtime enforcement.

The contract becomes the root of:

- permission decisions,
- plan validation,
- tool-call authorization,
- provenance,
- completion checks.

---

# 3. Capability and Security Kernel

Every worker receives a **capability lease**, not unrestricted access to every tool.

Example:

```yaml
worker: repo_test_worker
capabilities:
  filesystem:
    read:
      - /workspace/project/**
    write:
      - /workspace/project/.agent/tmp/**
  process:
    execute:
      - pytest
      - python
  network:
    allowed: false
expires_when:
  - task_complete
  - worker_terminated
```

## Tool-call boundary

Every proposed consequential tool invocation passes through:

```text
agent proposes action
        |
        v
capability check
        |
        v
task-contract alignment check
        |
        v
provenance/evidence check
        |
        v
risk policy
        |
        +---- deny / request approval
        |
        v
execute in sandbox
```

This borrows from:

- mandatory/attribute-based access control approaches,
- runtime prompt-injection defenses,
- provenance-based action alignment.

The important architectural decision is:

> **Security policy is runtime code, not an instruction asking the model to behave safely.**

---

# 4. Strategic Planner

The planner operates at a higher abstraction than workers.

## Inputs

- task contract,
- available worker registry,
- available skills,
- environment summary,
- retrieved relevant memory,
- current task graph,
- unresolved contradictions,
- resource budget.

## Outputs

- task DAG,
- worker assignments,
- evidence requirements,
- verification requirements,
- uncertainty estimates,
- compute allocation,
- fallback branches.

## Planner should explicitly represent uncertainty

Example:

```yaml
task: determine_api_behavior
hypotheses:
  - id: H1
    claim: "..."
    confidence: 0.55
    falsifier: "runtime trace shows ..."
  - id: H2
    claim: "..."
    confidence: 0.45
    falsifier: "..."
```

This makes it possible to dispatch parallel workers to resolve uncertainty rather than allowing one early guess to become the team's assumed truth.

---

# 5. Solver Escalation

Not every planning problem should be solved by language-model intuition.

When a subproblem becomes formalizable, route it to a solver.

Examples:

- PDDL / classical planning,
- SAT/SMT,
- constraint programming,
- linear or mixed-integer optimization,
- graph algorithms,
- symbolic algebra,
- database query planners.

Pattern:

```text
natural-language ambiguity
        |
       LLM
        |
formal representation
        |
deterministic solver
        |
verified solution
        |
natural-language explanation
```

This adopts the lesson of LLM+P and related hybrid planning work:

> let the LLM interpret; let mature algorithms solve what they solve better.

---

# 6. Versioned Dynamic Task Graph

The planner creates an explicit DAG.

Example:

```text
             T0: inspect task
                    |
          +---------+---------+
          |                   |
          v                   v
   T1: inspect code     T2: reproduce issue
          |                   |
          +---------+---------+
                    |
                    v
             T3: form hypothesis
                    |
          +---------+---------+
          |                   |
          v                   v
    T4: implement       T5: independent
         candidate           verification
          |                   |
          +---------+---------+
                    |
                    v
              T6: final tests
```

Each node has explicit success criteria.

A node becomes runnable only when dependencies are satisfied.

---

# 7. Asynchronous Execution Kernel

The execution kernel is deliberately **not an LLM**.

It owns:

- task readiness,
- worker availability,
- priority,
- resource quotas,
- GPU/model queues,
- timeouts,
- retries,
- cancellation,
- checkpointing,
- recovery,
- task lifecycle.

Pseudo-loop:

```python
while task_not_complete:
    process_events()
    update_ready_set()

    for task in ready_tasks:
        if resources_available(task):
            dispatch(task)

    for event in new_events:
        if event.requires_replan:
            freeze_current_plan_version()
            request_new_plan()

    enforce_budgets()
    detect_stalls()
```

The language model chooses *what work should exist*.

The scheduler chooses *when executable work runs*.

---

# 8. Worker Registry and Dynamic Team Formation

Avoid a permanently fixed collection of generic personas.

Maintain a capability registry.

```yaml
worker_type: code_runtime_investigator
skills:
  - reproduce_bug
  - inspect_stack_trace
  - trace_http_requests
tool_access:
  - shell
  - debugger
preferred_models:
  - medium_reasoning_model
estimated_cost: ...
historical_success:
  reproduction: 0.91
```

When a task arrives:

```text
task requirements
       |
       v
capability matching
       |
       v
select smallest useful team
```

A blackboard-inspired alternative is to allow specialists to **volunteer** for tasks whose requirements match their capabilities.

This avoids forcing the central planner to understand every specialist in advance.

---

# 9. Worker Execution Loop

A worker receives:

- one bounded objective,
- local success criteria,
- relevant constraints,
- selected memory,
- applicable skills,
- necessary artifacts,
- subscribed AgentRadio topics.

Its basic loop is:

```text
observe local state
       |
retrieve relevant context
       |
check available skill
   /          \
 yes           no
  |             |
execute      reason/act
skill           |
  \             /
   verify result
       |
publish evidence/events
       |
continue / complete / escalate
```

---

# 10. AgentRadio Communication Fabric

AgentRadio-style passive awareness is used for direct inter-agent messages.

Messages should be typed.

Recommended classes:

```text
DISCOVERY
CONTRADICTION
DEPENDENCY
REQUEST
RESPONSE
EVIDENCE
PLAN_RISK
BLOCKER
HANDOFF
VERIFICATION_FAILURE
SKILL_DISCOVERED
SECURITY_WARNING
```

Example:

```json
{
  "message_id": "msg_91",
  "type": "CONTRADICTION",
  "from": "worker_runtime_2",
  "recipients": ["worker_architecture_1", "planner"],
  "task_id": "T14",
  "summary": "Runtime path bypasses the handler assumed by plan v22.",
  "evidence_refs": ["trace://91882"],
  "priority": "P1"
}
```

## Delivery policy

### P0

Immediate interrupt.

Examples:

- safety violation,
- destructive side effect,
- globally invalid task assumption.

### P1

Deliver at next action boundary.

Examples:

- contradiction,
- required dependency,
- strong new evidence.

### P2

Deliver before subtask completion.

### P3

Store without interrupting.

---

# 11. Shared Blackboard

Direct messages are targeted.

The blackboard represents **team-visible structured working state**.

Example:

```yaml
claims:
  claim_19:
    text: "API endpoint retries internally"
    status: verified
    evidence:
      - trace://...
    owner: worker_runtime
    confidence: 0.98

open_questions:
  - id: q7
    text: "Does retry occur before auth refresh?"
    priority: high

artifacts:
  - id: a12
    type: patch
    path: ...

task_status:
  T1: complete
  T2: complete
  T3: running

contradictions:
  - claim_19
  - claim_22
```

Workers subscribe to relevant blackboard fields rather than re-reading the whole team's conversation.

---

# 12. Append-Only Event Log

The event log records what happened.

Example events:

```text
TASK_CREATED
TASK_DISPATCHED
TOOL_CALL_PROPOSED
TOOL_CALL_ALLOWED
TOOL_CALL_DENIED
TOOL_RESULT
CLAIM_CREATED
CLAIM_REVISED
MESSAGE_SENT
VERIFICATION_PASSED
VERIFICATION_FAILED
PLAN_SUPERSEDED
MEMORY_PROMOTED
SKILL_VERSION_CREATED
```

The log supports:

- debugging,
- replay,
- audit,
- provenance,
- learning,
- metrics.

---

# 13. Provenance Graph

The event log is chronological.

The provenance graph captures **causal/support relationships**.

Example:

```text
Source document
      |
      v
Evidence E17
      |
      v
Claim C9 --------+
      |          |
      v          v
Decision D4   Claim C10
      |
      v
Tool call A8
      |
      v
Observed state S12
      |
      v
Final conclusion F1
```

Every material final claim should ideally have a path back to evidence.

Every material external action should have a path back to:

```text
user intent -> plan step -> evidence/decision -> authorized action
```

This provides the basis for both trust and safety enforcement.

---

# 14. Context Plane

The Context Manager builds a **working view** for each agent.

It does not own canonical facts.

## Context template

```text
IMMUTABLE CONSTRAINTS
...

CURRENT OBJECTIVE
...

ACTIVE PLAN SLICE
...

VERIFIED FACTS
...

OPEN QUESTIONS
...

RECENT ACTIONS
...

RELEVANT RADIO MESSAGES
...

APPLICABLE SKILLS
...

ARTIFACT REFERENCES
...
```

## What should be omitted

- old verbose tool output,
- completed branches with no ongoing relevance,
- repeated system instructions,
- obsolete hypotheses,
- full transcripts from other workers.

## Compression

Instead of "summarize the conversation", compression should be typed:

```yaml
completed:
  - ...
verified:
  - ...
rejected:
  - ...
open:
  - ...
constraints:
  - ...
artifacts:
  - ...
next:
  - ...
```

---

# 15. Hybrid Memory Governance

Agent research contains a useful tension.

Some work argues that the agent should autonomously choose memory operations. Other work shows benefits from external context/memory managers.

The reference architecture uses both:

> **Agents may propose memory operations; a memory service applies governance.**

Example:

```text
worker proposes:
  "Store this as stable environmental knowledge."

memory service checks:
  evidence?
  contradiction?
  duplicate?
  expiration?
  provenance?
  confidence?

then:
  accept / merge / quarantine / reject
```

This preserves adaptability without allowing a worker to silently rewrite the system's canonical memory.

---

# 16. Four-Layer Memory

## L1 — Episodic Evidence

What happened.

Examples:

- trajectories,
- traces,
- tool outputs,
- failures,
- screenshots,
- test results.

## L2 — Semantic / Environmental Knowledge

What is believed to be true about the environment.

Examples:

- repository architecture,
- API behavior,
- organizational conventions,
- machine configuration.

Every item carries provenance and freshness metadata.

## L3 — Policies / Heuristics

What tends to work.

Examples:

```text
If a GUI action produces no visual state change,
do not blindly repeat the click; inspect focus/window state.
```

## L4 — Skills

Executable reusable procedures.

The hierarchy is not merely storage organization.

It represents increasing abstraction and confidence.

---

# 17. Memory Consolidation

Periodic consolidation should perform:

```text
episodes
   |
deduplicate
   |
cluster recurring patterns
   |
derive candidate fact/policy
   |
check contradictions
   |
attach provenance
   |
promote
```

Memory should also support:

- revision,
- temporal validity,
- contradiction,
- confidence,
- forgetting,
- supersession.

A fact should not be stored as timeless truth when it was merely true during one session.

---

# 18. Skill Crystallization

The architecture treats skills as **software artifacts**, not prompt snippets.

Lifecycle:

```text
repeated experience
      |
      v
candidate procedure
      |
      v
natural-language policy
      |
      v
prototype skill
      |
      v
test cases
      |
      v
validated skill
      |
      v
production use
      |
      v
telemetry / maintenance / retirement
```

---

# 19. Skill Contract

A skill should declare:

```yaml
name: verify_patch
version: 4

preconditions:
  - git_repository_available
  - test_command_known

inputs:
  patch_ref:
    type: artifact
  test_command:
    type: string

outputs:
  status:
    enum: [pass, fail, inconclusive]
  evidence:
    type: list

actions:
  - inspect_diff
  - run_targeted_tests
  - run_regression_tests

verification:
  - tests_exited_successfully
  - no_unexpected_files_modified

permissions:
  filesystem: repository
  process:
    - git
    - test_runner

failure_modes:
  - tests_unavailable
  - nondeterministic_failure
  - environment_broken

provenance:
  derived_from:
    - episode://...

metrics:
  successes: ...
  failures: ...
  last_validated: ...
```

---

# 20. SkillOps Layer

Once skill libraries grow, they become a software-maintenance problem.

The architecture therefore includes skill-library operations:

- typed contracts,
- dependency graph,
- versioning,
- compatibility checks,
- duplicate detection,
- deprecation,
- regression tests,
- risk classification,
- telemetry,
- utility scores.

A successful agent system should not accumulate a thousand stale skills that nobody trusts.

---

# 21. Routine Tool-Flow Compilation

If tool sequences become predictable, the system should stop asking a large model to select every next tool.

Example history:

```text
inspect_file -> run_test -> inspect_trace -> patch -> run_test
```

may become:

```text
skill: diagnose_and_validate_patch
```

or a lightweight transition graph.

This incorporates the practical lesson from tool-selection research:

> learned workflow structure can reduce repeated inference.

---

# 22. Verification Plane

Verification is mandatory for consequential state changes.

Basic pattern:

```text
STATE BEFORE
     |
PROPOSED ACTION
     |
ACTION
     |
STATE AFTER
     |
VERIFIER
     |
+----+-----------+-----------+
|                |           |
PASS            FAIL     INCONCLUSIVE
|                |           |
continue       retry       escalate
               /replan
```

---

# 23. Deterministic Verification First

Use code whenever possible.

Examples:

### Filesystem

- expected file exists,
- checksum changed,
- unintended files did not change.

### Code

- test suite,
- compiler,
- linter,
- static analyzer,
- runtime trace.

### API

- response code,
- returned object,
- subsequent read,
- invariants.

### Data

- schema,
- row counts,
- constraints,
- checksums.

### GUI

- DOM/state inspection,
- screenshot delta,
- application state.

An LLM critic is a fallback when success cannot be deterministically evaluated.

---

# 24. Milestone Verification

Long tasks should not wait until the end to learn they failed.

Each task node can declare milestones.

Example:

```yaml
task: deploy_service

milestones:
  - id: build
    verify: artifact_exists

  - id: startup
    verify: process_healthy

  - id: network
    verify: health_endpoint_returns_200

  - id: behavior
    verify: integration_test_passes
```

This reflects work on subgoal decomposition and process rewards.

---

# 25. Reflection Is Event-Triggered, Not Constant

Research supports reflection, but repeatedly asking agents to critique themselves is expensive and can amplify noise.

Trigger reflection when:

- verification fails,
- confidence is low,
- a contradiction appears,
- execution stalls,
- a branch has high cost,
- a task completes and learning is useful.

Do not insert a generic "reflect" step after every action.

---

# 26. Anticipatory Failure Analysis

Before expensive or irreversible branches, invoke a lightweight Devil's-Advocate procedure.

Ask:

```text
What is the most likely way this action fails?
What observation would reveal that failure?
What fallback should be available?
```

Store fallback branches in the plan.

This reduces expensive complete reruns.

---

# 27. Search and Branching Only When Worth It

Tree search, multiple proposals, and debate can improve hard reasoning but can also multiply compute.

Use branch search when:

- uncertainty is high,
- mistakes are expensive,
- multiple plausible strategies exist,
- an evaluator can meaningfully discriminate branches.

Do not use Monte Carlo/tree search for deterministic routine operations.

---

# 28. Debate Is Not the Default Coordination Primitive

Broad multi-agent research contains an important caution.

Debate can help, but:

- majority pressure can suppress independent correction,
- extra rounds can become redundant,
- structural debate parameters may matter less than agent quality and diversity.

The architecture therefore prefers:

```text
independent analysis
      |
structured claims + evidence
      |
reviewer(s)
      |
meta-review / adjudication
```

over constant round-table conversation.

A MARS-like author/reviewer/meta-review structure is appropriate for high-value decisions because it preserves independence while limiting reviewer-to-reviewer chatter.

---

# 29. Evidence-Weighted Consensus

Never resolve factual conflict by simple majority if stronger evidence exists.

Recommended ordering:

```text
reproducible deterministic evidence
        >
direct environment observation
        >
independent corroborated observation
        >
trusted retrieved source
        >
reasoned inference
        >
unsupported model assertion
```

Consensus is about **evidence quality**, not vote count.

---

# 30. Recovery Controller

Failure handling should be explicit.

Failure classes:

```text
TOOL_FAILURE
ENVIRONMENT_CHANGED
PLAN_INVALID
DEPENDENCY_FAILED
VERIFICATION_FAILED
TIMEOUT
PERMISSION_DENIED
MODEL_UNCERTAIN
CONTRADICTION
BUDGET_EXCEEDED
```

Each class maps to a bounded recovery policy.

Example:

```yaml
VERIFICATION_FAILED:
  attempt_1: retry_with_local_correction
  attempt_2: alternate_worker
  attempt_3: planner_replan
  then: human_escalation
```

No unbounded "try again" loops.

---

# 31. Checkpoints and Rollback

Create checkpoints before:

- destructive actions,
- major environment modifications,
- expensive branches,
- long multi-step workflows.

A checkpoint records:

- environment state or snapshot reference,
- plan version,
- task states,
- blackboard snapshot,
- memory writes since prior checkpoint,
- permissions,
- active artifacts.

Rollback can then restore the system rather than asking agents to reason their way out of corrupted state.

---

# 32. Compute Controller

The runtime should optimize for expected value, not model prestige.

Each task receives:

```yaml
difficulty: ...
uncertainty: ...
impact_of_error: ...
parallelizability: ...
verifiability: ...
latency_budget: ...
```

From these, the controller selects:

- model size,
- reasoning budget,
- number of workers,
- number of candidate branches,
- verifier strength.

---

# 33. Escalation Ladder

Example:

```text
deterministic skill
      |
small model
      |
medium model
      |
strong model
      |
parallel strong-model attempts
      |
search / tree exploration
      |
human escalation
```

Do not begin every task at the top of the ladder.

---

# 34. Test-Time Compute as a Budget

Test-time compute should be allocated where additional search is likely to change the answer.

Possible triggers:

```text
low verifier confidence
high-value decision
disagreement between evidence-backed workers
formal constraint failure
novel problem outside skill coverage
```

Stop when marginal improvement becomes small.

---

# 35. Learning Plane

After task completion:

```text
trajectory
   |
   +--> useful episodes
   |
   +--> failure analysis
   |
   +--> policy updates
   |
   +--> memory updates
   |
   +--> candidate skills
   |
   +--> prompt/policy optimization candidates
   |
   +--> benchmark regression cases
```

---

# 36. Experience Learning

Adopt the useful lesson from Reflexion and ExpeL:

an agent can improve without changing model weights by storing structured lessons from success and failure.

But free-form reflections should not become permanent truth automatically.

They enter as **candidate policies** with evidence links.

---

# 37. Prompt/Policy Evolution

Approaches such as GEPA suggest that textual policies can be optimized from execution feedback.

A safe implementation:

```text
current policy
     |
benchmark failures
     |
candidate policy revisions
     |
offline evaluation
     |
promote only if regression suite improves
```

Never allow production prompt mutation with no evaluation gate.

---

# 38. Skill Promotion Tests

A candidate skill should demonstrate:

- repeated usefulness,
- stable preconditions,
- reproducible success,
- acceptable failure behavior,
- meaningful cost savings.

Promotion threshold example:

```yaml
minimum_examples: 8
success_rate: ">= 0.90"
regression_pass: true
known_failure_modes_documented: true
security_review: passed
```

---

# 39. Forgetting and Deprecation

Lifelong learning requires forgetting.

Deprecate or downgrade memory/skills when:

- environment version changes,
- evidence is contradicted,
- success rate drops,
- the skill has not been used,
- better skill supersedes it,
- dependency disappears.

The system should be able to say:

> this used to be reliable, but is no longer trusted.

---

# 40. Evaluation Plane

Do not evaluate the architecture primarily with "did an LLM judge the answer as good?"

Use execution-based and constraint-based metrics wherever possible.

Track:

## Outcome

- task success,
- partial completion,
- constraint satisfaction.

## Efficiency

- tokens,
- model seconds,
- wall-clock time,
- tool calls,
- parallel utilization.

## Reliability

- failed actions,
- recovery success,
- contradiction rate,
- verification catch rate.

## Safety

- denied unsafe actions,
- permission overreach,
- prompt-injection resistance,
- unintended side effects.

## Learning

- performance improvement after experience,
- skill reuse,
- memory precision,
- stale-memory rate.

---

# 41. Benchmark Portfolio

No single benchmark captures real autonomy.

A practical evaluation suite should include:

- software issue resolution,
- long-horizon software evolution,
- web navigation,
- real desktop/computer use,
- constrained planning,
- repetitive workflows,
- information discovery,
- safety/prompt injection,
- memory-heavy longitudinal tasks.

The architecture should be considered improved only when changes generalize across several task classes.

---

# 42. Canonical Data Types

## TaskContract

```text
objective
constraints
success criteria
permissions
budget
```

## PlanGraph

```text
version
nodes
dependencies
fallbacks
supersedes
```

## Worker

```text
capabilities
tools
model policy
skills
historical performance
```

## Claim

```text
statement
confidence
status
provenance
validity interval
```

## Evidence

```text
source
artifact
timestamp
method
trust level
```

## ActionProposal

```text
intent
plan node
tool
parameters
expected state change
supporting evidence
risk
```

## SkillContract

```text
preconditions
inputs
actions
outputs
verification
permissions
failure modes
versions
metrics
```

---

# 43. End-to-End Task Lifecycle

```text
1. USER REQUEST
      |
2. TASK CONTRACT
      |
3. SECURITY/CAPABILITY ENVELOPE
      |
4. MEMORY + SKILL RETRIEVAL
      |
5. PLANNING
      |
6. TASK DAG v1
      |
7. ASYNC DISPATCH
      |
8. PARALLEL WORK
      |
   AgentRadio + Blackboard
      |
9. LOCAL VERIFICATION
      |
10. CONTRADICTION / FAILURE?
      | yes
      v
11. REPLAN -> DAG v2
      |
      +------> resume execution
      |
      no
      v
12. INTEGRATION
      |
13. GLOBAL VERIFICATION
      |
14. SAFETY / INTENT CHECK
      |
15. DELIVER RESULT
      |
16. LEARN
      |
   memory / policies / skills / tests
```

---

# 44. Minimal Practical Build

## Version 0.1

Implement only:

- TaskContract,
- strong planner,
- versioned DAG,
- 2-4 workers,
- AgentRadio messaging,
- event log,
- deterministic verification.

This creates the critical execution substrate.

## Version 0.2

Add:

- blackboard,
- Context Manager,
- capability leases,
- tool-call policy gate,
- provenance edges.

## Version 0.3

Add:

- episodic + semantic memory,
- policy extraction,
- candidate skills,
- skill runtime.

## Version 0.4

Add:

- SkillOps,
- model routing,
- dynamic team creation,
- anticipatory fallback planning,
- reviewer/meta-reviewer path.

## Version 1.0

Add:

- continuous benchmark harness,
- automated memory consolidation,
- validated prompt/policy evolution,
- adaptive compute controller,
- automatic skill retirement,
- full recovery/checkpoint subsystem.

---

# 45. What Should NOT Be Built

## Do not build a universal transcript

The event log and memory stores are canonical. Models receive views.

## Do not route every message through the planner

AgentRadio exists specifically to avoid that bottleneck.

## Do not allow unrestricted shared chat

Use targeted radio plus structured blackboard.

## Do not give every worker every tool

Use scoped capability leases.

## Do not trust model-reported success

Verify environment state.

## Do not turn every successful trajectory into a skill

Require validation.

## Do not permanently retain every memory

Use consolidation, contradiction, expiry, and forgetting.

## Do not deploy debate everywhere

Use independent work plus selective review.

## Do not mutate plans invisibly

Create a new plan version.

## Do not use an LLM for deterministic orchestration

Schedulers, permissions, parsing, state machines, and retries belong in normal software.

---

# 46. The Architecture in One Sentence

> **Use LLMs as bounded reasoning components inside a versioned, asynchronous, evidence-driven, least-privilege execution system that can convert successful experience into verified reusable software.**

---

# 47. Research Synthesis: What Each Major Paper Contributes

The following papers were particularly useful during the broader arXiv pass.

## Coordination and multi-agent structure

### AgentRadio: Passive Awareness for Long-Horizon Multi-Agent Collaboration
arXiv:2607.28430  
https://arxiv.org/abs/2607.28430

Borrowed idea: passive asynchronous lateral communication.

### DynTaskMAS: A Dynamic Task Graph-driven Framework for Asynchronous and Parallel LLM-based Multi-Agent Systems
arXiv:2503.07675  
https://arxiv.org/abs/2503.07675

Borrowed idea: dynamic task graphs + async parallel scheduling.

### Exploring Advanced LLM Multi-Agent Systems Based on Blackboard Architecture
arXiv:2507.01701  
https://arxiv.org/abs/2507.01701

Borrowed idea: shared blackboard and dynamic agent selection.

### LLM-based Multi-Agent Blackboard System for Information Discovery in Data Science
arXiv:2510.01285  
https://arxiv.org/abs/2510.01285

Borrowed idea: capability-driven volunteering through shared working state.

### AutoGen: Enabling Next-Gen LLM Applications via Multi-Agent Conversation
arXiv:2308.08155  
https://arxiv.org/abs/2308.08155

Borrowed idea: programmable agent conversation infrastructure.

### MetaGPT: Meta Programming for A Multi-Agent Collaborative Framework
arXiv:2308.00352  
https://arxiv.org/abs/2308.00352

Borrowed idea: explicit SOPs and role-based workflow.

### ChatDev: Communicative Agents for Software Development
arXiv:2307.07924  
https://arxiv.org/abs/2307.07924

Borrowed idea: structured role collaboration in software workflows.

### CAMEL: Communicative Agents for "Mind" Exploration of Large Scale Language Model Society
arXiv:2303.17760  
https://arxiv.org/abs/2303.17760

Borrowed idea: role-based communication as an agent primitive.

---

# 48. Planning and Execution

### Planner Matters! An Efficient and Unbalanced Multi-agent Collaboration Framework for Long-horizon Planning
arXiv:2605.02168  
https://arxiv.org/abs/2605.02168

Borrowed idea: concentrate model capacity in the planner.

### HIPIF: Hierarchical Planning and Information Folding for Long-Horizon LLM Agent Learning
arXiv:2606.10507  
https://arxiv.org/abs/2606.10507

Borrowed idea: explicit subgoals plus folding completed histories.

### STRUCTUREDAGENT: Planning with AND/OR Trees for Long-Horizon Web Tasks
arXiv:2603.05294  
https://arxiv.org/abs/2603.05294

Borrowed idea: explicit hierarchical search structure and candidate tracking.

### From Agent Loops to Structured Graphs: A Scheduler-Theoretic Framework for LLM Agent Execution
arXiv:2604.11378  
https://arxiv.org/abs/2604.11378

Borrowed idea: move control flow into inspectable graphs; separate planning, execution, recovery.

### A Subgoal-driven Framework for Improving Long-Horizon LLM Agents
arXiv:2603.19685  
https://arxiv.org/abs/2603.19685

Borrowed idea: milestone/subgoal-based execution and reward.

### LLM+P: Empowering Large Language Models with Optimal Planning Proficiency
arXiv:2304.11477  
https://arxiv.org/abs/2304.11477

Borrowed idea: translate suitable problems into a classical planning representation.

### Language Agent Tree Search Unifies Reasoning Acting and Planning in Language Models
arXiv:2310.04406  
https://arxiv.org/abs/2310.04406

Borrowed idea: selectively use search over agent trajectories.

### DEVIL'S ADVOCATE: Anticipatory Reflection for LLM Agents
arXiv:2405.16334  
https://arxiv.org/abs/2405.16334

Borrowed idea: anticipate likely failures and fallback actions before execution.

---

# 49. Reasoning and Acting

### ReAct: Synergizing Reasoning and Acting in Language Models
arXiv:2210.03629  
https://arxiv.org/abs/2210.03629

Borrowed idea: interleave reasoning with environment interaction.

### Toolformer: Language Models Can Teach Themselves to Use Tools
arXiv:2302.04761  
https://arxiv.org/abs/2302.04761

Borrowed idea: external tools should complement model limitations.

### Language Models can Solve Computer Tasks
arXiv:2303.17491  
https://arxiv.org/abs/2303.17491

Borrowed idea: recursive critique/improvement around action execution.

### AutoTool: Efficient Tool Selection for Large Language Model Agents
arXiv:2511.14650  
https://arxiv.org/abs/2511.14650

Borrowed idea: compile predictable tool transitions into a lower-cost structure.

---

# 50. Memory and Context

### From Memory to Skills: Evidence-Grounded Co-Evolution Governance for Long-Horizon LLM Agents
arXiv:2607.16621  
https://arxiv.org/abs/2607.16621

Borrowed idea: evidence-grounded hierarchy from memory to reusable skills.

### Agentic Memory: Learning Unified Long-Term and Short-Term Memory Management for Large Language Model Agents
arXiv:2601.01885  
https://arxiv.org/abs/2601.01885

Borrowed idea: expose memory operations as first-class actions.

### Episodic-Semantic Memory Architecture for Long-Horizon Scientific Agents
arXiv:2605.17625  
https://arxiv.org/abs/2605.17625

Borrowed idea: separate immediate episodic context from consolidated semantic knowledge.

### Continuum Memory Architectures for Long-Horizon LLM Agents
arXiv:2601.09913  
https://arxiv.org/abs/2601.09913

Borrowed idea: mutable, temporally linked, consolidating memory rather than stateless RAG.

### Learning Agent-Compatible Context Management for Long-Horizon Tasks
arXiv:2605.30785  
https://arxiv.org/abs/2605.30785

Borrowed idea: context management as a dedicated optimization problem.

### Active Context Compression: Autonomous Memory Management in LLM Agents
arXiv:2601.07190  
https://arxiv.org/abs/2601.07190

Borrowed idea: explicit checkpoint/consolidation and history pruning.

### ExpeL: LLM Agents Are Experiential Learners
arXiv:2308.10144  
https://arxiv.org/abs/2308.10144

Borrowed idea: extract reusable lessons from prior trajectories.

### Reflexion: Language Agents with Verbal Reinforcement Learning
arXiv:2303.11366  
https://arxiv.org/abs/2303.11366

Borrowed idea: learn from feedback through persistent textual reflection.

### Generative Agents: Interactive Simulacra of Human Behavior
arXiv:2304.03442  
https://arxiv.org/abs/2304.03442

Borrowed idea: observation, reflection, planning, and memory retrieval as distinct mechanisms.

---

# 51. Skills and Lifelong Learning

### Voyager: An Open-Ended Embodied Agent with Large Language Models
arXiv:2305.16291  
https://arxiv.org/abs/2305.16291

Borrowed idea: executable code skill library, iterative repair, lifelong reuse.

### Harnessing LLM Agents with Skill Programs
arXiv:2605.17734  
https://arxiv.org/abs/2605.17734

Borrowed idea: programmatic skills that intervene in the agent loop.

### Formal Skill: Programmable Runtime Skills for Efficient and Accurate LLM Agents
arXiv:2605.19604  
https://arxiv.org/abs/2605.19604

Borrowed idea: formal runtime representation for skills with state/hooks.

### AutoSkill: Experience-Driven Lifelong Learning via Skill Self-Evolution
arXiv:2603.01145  
https://arxiv.org/abs/2603.01145

Borrowed idea: automatically derive and evolve reusable skills from interaction traces.

### SkillOps: Managing LLM Agent Skill Libraries as Self-Maintaining Software Ecosystems
arXiv:2605.13716  
https://arxiv.org/abs/2605.13716

Borrowed idea: contracts, dependency graphs, health checks, maintenance, deprecation.

### MemSkill: Learning and Evolving Memory Skills for Self-Evolving Agents
arXiv:2602.02474  
https://arxiv.org/abs/2602.02474

Borrowed idea: memory behavior itself can be represented as evolvable skills.

### Scaling Teams or Scaling Time? Memory Enabled Lifelong Learning in LLM Multi-Agent Systems
arXiv:2604.03295  
https://arxiv.org/abs/2604.03295

Borrowed idea: improve accumulated experience before reflexively increasing agent count.

---

# 52. Verification, Reflection, and Process Feedback

### VisCritic: Visual State Comparison as Process Reward for GUI Agents
arXiv:2606.24525  
https://arxiv.org/abs/2606.24525

Borrowed idea: compare pre/post environment state to verify actions.

### Process Reward Models for LLM Agents: Practical Framework and Directions
arXiv:2502.10325  
https://arxiv.org/abs/2502.10325

Borrowed idea: process-level feedback for agent trajectories.

### Self-Refine: Iterative Refinement with Self-Feedback
arXiv:2303.17651  
https://arxiv.org/abs/2303.17651

Borrowed idea: iterative generate-feedback-refine loop.

### GEPA: Reflective Prompt Evolution Can Outperform Reinforcement Learning
arXiv:2507.19457  
https://arxiv.org/abs/2507.19457

Borrowed idea: evolve textual policies from execution feedback.

### Scaling LLM Test-Time Compute Optimally can be More Effective than Scaling Model Parameters
arXiv:2408.03314  
https://arxiv.org/abs/2408.03314

Borrowed idea: treat search/verification inference as an allocatable compute budget.

---

# 53. Debate and Review

### Improving Factuality and Reasoning in Language Models through Multiagent Debate
arXiv:2305.14325  
https://arxiv.org/abs/2305.14325

Borrowed idea: multiple independent reasoning paths can expose mistakes.

### Can LLM Agents Really Debate? A Controlled Study of Multi-Agent Debate in Logical Reasoning
arXiv:2511.07784  
https://arxiv.org/abs/2511.07784

Borrowed lesson: diversity and intrinsic quality matter; majority pressure can hurt correction.

### MARS: toward more efficient multi-agent collaboration for LLM reasoning
arXiv:2509.20502  
https://arxiv.org/abs/2509.20502

Borrowed idea: author/reviewer/meta-reviewer structure can reduce unnecessary debate communication.

---

# 54. Safety, Access Control, and Provenance

### Agent-SafetyBench: Evaluating the Safety of LLM Agents
arXiv:2412.14470  
https://arxiv.org/abs/2412.14470

Borrowed lesson: agent safety needs system-level evaluation, not model-only evaluation.

### Taming Various Privilege Escalation in LLM-Based Agent Systems: A Mandatory Access Control Framework
arXiv:2601.11893  
https://arxiv.org/abs/2601.11893

Borrowed idea: least privilege and mandatory access control for agent-tool interactions.

### ClawGuard: A Runtime Security Framework for Tool-Augmented LLM Agents Against Indirect Prompt Injection
arXiv:2604.11790  
https://arxiv.org/abs/2604.11790

Borrowed idea: deterministic enforcement at tool-call boundaries.

### The Landscape of Prompt Injection Threats in LLM Agents: From Taxonomy to Analysis
arXiv:2602.10453  
https://arxiv.org/abs/2602.10453

Borrowed lesson: defenses must preserve legitimate context-dependent tool use.

### Safeguarding LLM Agents from Misalignment through Provenance Analysis
arXiv:2607.01236  
https://arxiv.org/abs/2607.01236

Borrowed idea: require proposed actions to be traceably supported by user intent and evidence.

### From Agent Traces to Trust: Evidence Tracing and Execution Provenance in LLM Agents
arXiv:2606.04990  
https://arxiv.org/abs/2606.04990

Borrowed idea: evidence and execution provenance as a first-class architecture layer.

### PROV-AGENT: Unified Provenance for Tracking AI Agent Interactions in Agentic Workflows
arXiv:2508.02866  
https://arxiv.org/abs/2508.02866

Borrowed idea: standardized provenance across multi-agent workflows and tools.

---

# 55. Evaluation and Long-Horizon Benchmarks

### OSWORLD: Benchmarking Multimodal Agents for Open-Ended Tasks in Real Computer Environments
arXiv:2404.07972  
https://arxiv.org/abs/2404.07972

Borrowed idea: execution-based evaluation in isolated reproducible environments.

### SWE-EVO: Benchmarking Coding Agents in Long-Horizon Software Evolution Scenarios
arXiv:2512.18470  
https://arxiv.org/abs/2512.18470

Borrowed lesson: long-horizon multi-file evolution is substantially harder than isolated issue repair.

### DeepPlanning: Benchmarking Long-Horizon Agentic Planning with Verifiable Constraints
arXiv:2601.18137  
https://arxiv.org/abs/2601.18137

Borrowed idea: evaluate global constraint satisfaction and information acquisition, not just local action quality.

### OS-Marathon: Benchmarking Computer-Use Agents on Long-Horizon Repetitive Tasks
arXiv:2601.20650  
https://arxiv.org/abs/2601.20650

Borrowed idea: identify repetitive workflows and distill them rather than reasoning from scratch indefinitely.

---

# Final Synthesis

The literature does not point toward a single magical agent algorithm.

It points toward a systems architecture.

The most robust design separates:

```text
reasoning
planning
scheduling
communication
memory
context
skills
verification
permissions
provenance
learning
evaluation
```

and gives each responsibility the mechanism best suited to it.

The LLM is therefore not "the agent."

The **entire runtime** is the agent.

The model is one of its reasoning engines.
