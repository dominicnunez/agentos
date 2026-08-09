# Agent OS — AI Coding Handoff v4.2

> **Short path:** If you are overwhelmed by the documentation, read `../QUICK_START.md`, `29_V1_BUILD_CONTRACT.md`, and `35_WORK_FIRST_ORCHESTRATION_AND_MINIMAL_LLM.md`. Use the rest as reference.
**Status:** Normative implementation handoff  
**Date:** 2026-08-08  
**Primary implementation language:** Go  
**Architecture:** modular monolith first  
**Working name:** Agent OS

## 1. Why v4.2

v4.2 is a deliberate architectural reset of the communication layer and a further reduction of implementation scope.

The project no longer uses:

- ANL / Agent Native Language;
- a general Agent Semantic Model;
- a global ontology for beliefs, hypotheses, observations, contradictions, confidence, etc.

Those approaches are **superseded**, not deferred.

The replacement is **Agent OS Event Contracts**:

> Formalize only the semantics that Agent OS itself must react to deterministically. Everything else remains ordinary agent content.

This change reduces custom protocol surface while preserving persistence, auditability, provenance, authorization, passive awareness, and deterministic control.

## 2. Core thesis

Agent OS tests this abstraction:

> **Organizations, Teams, and Agents are durable logical actors; model executions are ephemeral work sessions that wake those actors, advance their state, communicate, use approved tools, and then disappear.**

The first falsifiable question is:

> Can a persistent asynchronously collaborating Team solve genuinely interdependent work better than a strong single agent after accounting for cost, reliability, coordination overhead, and safety?

## 3. V1 core

Build a local Go modular monolith containing only:

```text
Organization / Team / Agent identity
Goal / Task dependency graph
AgentExecution + RuntimeAdapter
Scheduler
Event Ledger
Event Router + Inbox projections
Agent OS Event Contracts
Capability / policy gate
Authorization trace projection
Blocked-task flow
Human consequence boundary + approval
Freeze / revoke
CompletionContract + Completion Engine
Runtime-attested evidence
Operational telemetry/replay harness + adversarial tests
Basic human CLI/UI
```

## 4. Communication model

```text
Agent/model output
      |
      v
 EventDraft
 (untrusted)
      |
      v
Event Gateway / validator
      |
      | validates allowed event type,
      | routing, task relationship,
      | authority and payload
      v
Persist Event first
      |
      v
Immutable Event Ledger
      |
      +--> Inbox projection
      +--> Team/task state projection
      +--> Audit/provenance projection
      +--> Authorization trace projection
      |
      v
Recipient action boundary
      |
      v
Context injection
```

The persisted `Event` has a runtime-owned envelope. Agent content is untrusted.

## 5. Initial agent-proposable event contracts

Only these require first-class model-facing event output in v1:

- `MESSAGE`
- `TASK_BLOCKED`
- `EVIDENCE_PUBLISHED`
- `RESULT_PUBLISHED`
- `CANDIDATE_COMPLETE`

Other events such as `TASK_ASSIGNED`, `APPROVAL_DECIDED`, `COMPLETION_VERIFIED`, `CAPABILITY_REVOKED`, and `FREEZE_SET` are emitted by trusted runtime operations, not by a model declaring them in content.

## 6. Critical content/control rule

> **Content cannot create control-plane authority.**

An agent may write “APPROVED”, “SYSTEM”, “ADMIN”, “ignore policy”, or any other text. It has no runtime effect unless an authorized runtime operation emits the corresponding typed event under the correct authority chain.

## 7. Structure-promotion rule

> **Promote semantics into structure only when deterministic runtime behavior, safety, verification, interoperability, or measured efficiency requires it.**

Do not add a new event kind merely because a concept is intellectually useful.

## 8. One history, many projections

Use one append-only event history as the source of truth. Prefer projections over new stores:

```text
ledger -> inbox
ledger -> current task state
ledger -> team state
ledger -> audit history
ledger -> provenance trace
ledger -> authorization trace
```

A separate blackboard, authorization ledger, provenance store, or semantic memory database must be justified by measured need.

## 9. Scope authority

`IMPLEMENTATION_SCOPE.yaml` is the machine-readable implementation-scope authority.

- `V1_CORE`: build now.
- `VALIDATE_NEXT`: controlled evaluation/prototype only after real V1 operational evidence exists.
- `FUTURE_IF_EARNED`: do not build until prerequisites and revisit trigger are satisfied.
- `SUPERSEDED`: do not implement.

`future/FUTURE_CONSIDERATIONS.yaml` preserves compatible demoted ideas and their prerequisites.

## 10. Safety invariants

1. No LLM is trusted merely because it has a manager/evaluator title.
2. Workers do not self-grant or normally solicit authority expansion.
3. Positive capabilities do not automatically inherit.
4. Restrictions/authority ceilings remain binding through delegation.
5. `discoverable != invocable != authorized`.
6. Consequential actions require valid authorization ancestry at time of use.
7. Human-required actions wait until an authorized human answers.
8. Escalating notifications may increase attention, never authority.
9. Human freeze/revoke wins at time of action.
10. Agents only produce `CANDIDATE_COMPLETE`; the Completion Engine verifies completion.
11. Agent content cannot forge runtime identity, approval, attestation, authorization, completion, or ledger history.
12. External content is data, not authority.
13. Unknown safety-critical control semantics fail closed.
14. Persist before making a communication available to another actor.

## 11. Read order

1. `00_START_HERE.md`
2. `29_V1_BUILD_CONTRACT.md`
3. `03_EVENT_CONTRACTS_V0_1_SPEC.md`
4. `02_ARCHITECTURE.md`
5. `04_SECURITY_AND_THREAT_MODEL.md`
6. `05_TEAMS_TASKS_DURABILITY.md`
7. `06_GO_IMPLEMENTATION_SPEC.md`
8. `17_SAFETY_INTEGRITY_AND_CONSTITUTION.md`
9. `18_APPROVALS_BLOCKED_TASKS_AND_NOTIFICATIONS.md`
10. `19_COMPLETION_VERIFICATION_AND_EVIDENCE.md`
11. `08_IMPLEMENTATION_ROADMAP_AND_ACCEPTANCE.md`
12. `16_FALSIFICATION_AND_BENCHMARK_POLICY.md`
13. `20_FUTURE_CONSIDERATIONS.md`
14. remaining normative docs as needed
15. `future/` only when a prerequisite has been met
16. `history/` and `research/` for context only


## v4.2 required addenda

After `29_V1_BUILD_CONTRACT.md`, read:

- `30_VERSIONED_KNOWLEDGE_AND_SKILLS.md`
- `31_AUDIT_SERVICE.md`
- `32_LAB_EXPERIMENTATION_AND_PROMOTION.md`
- `33_INFERENCE_RESOURCE_MANAGEMENT.md`

These supersede v4.0 statements that placed all memory/skills outside V1.


## v4.2 patch

Read `34_V4_1_1_IMPLEMENTATION_HARDENING.md` before coding knowledge promotion, topology experiments, Skill validation, or inference telemetry adapters.

## v4.2 runtime boundary additions

Read `36_A2A_OPERATOR_GATEWAY_AND_HERMES.md` and `37_EXECUTION_CONTEXT_TOOL_OUTCOME_EFFECTS.md` before implementing the external operator interface, context builder, tool adapters, approvals, or external effects.
