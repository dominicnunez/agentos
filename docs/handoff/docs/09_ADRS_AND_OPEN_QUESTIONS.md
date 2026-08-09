# Architecture Decision Records and Open Questions

## Accepted v4.0 decisions

### ADR-001 — Go modular monolith first
Accepted.

### ADR-002 — Durable logical actors, ephemeral executions
Accepted.

### ADR-003 — Event sourcing with projections
Accepted. One event history first; avoid duplicate stores.

### ADR-004 — Event Contracts replace ANL and Agent Semantic Model
Accepted. ANL/ASM are superseded and must not be implemented.

### ADR-005 — Structure only runtime-significant semantics
Accepted. Natural-language/structured content remains content until a deterministic runtime requirement earns a typed contract.

### ADR-006 — Trusted envelope, untrusted content
Accepted. Runtime owns event identity/source/time/sequence/control metadata.

### ADR-007 — JSON baseline
Accepted. Canonical JSON for V1 persistence/API/context. TOON is a future benchmark candidate.

### ADR-008 — Persist before availability
Accepted.

### ADR-009 — Passive awareness at action boundaries
Accepted. No mid-token interrupt in V1.

### ADR-010 — Authority Non-Solicitation
Accepted. Worker returns blocked task instead of requesting expansion of its own authority.

### ADR-011 — No positive capability inheritance
Accepted. Restrictions/ceilings continue through delegation.

### ADR-012 — `discoverable != invocable != authorized`
Accepted.

### ADR-013 — Authorization trace is a projection
Accepted. Do not build a second authorization ledger.

### ADR-014 — Task dependency graph before PlanGraph engine
Accepted.

### ADR-015 — Typed rules before policy DSL
Accepted.

### ADR-016 — Human consequence boundaries
Accepted: financial, physical, public/external write, privilege/trust expansion, sensitive-data boundary expansion, destructive/irreversible, legal/binding, trusted-core/security.

### ADR-017 — Unanswered approvals wait
Accepted. Escalate attention, never authority.

### ADR-018 — Candidate completion + Completion Engine
Accepted.

### ADR-019 — Runtime-attested evidence distinct from agent claims
Accepted.

### ADR-020 — Evaluation before optimization
Accepted. Runtime optimization/Organization Health are future if earned.

### ADR-021 — Future systems require prerequisites
Accepted. `future/FUTURE_CONSIDERATIONS.yaml` is the deferral registry.

### ADR-022 — Agent OS may remain an internal operating capability
Accepted product assumption. External framework adoption is not required for success.

## Superseded decisions

- ANL as native semantic IPC — superseded.
- custom ANL grammar — superseded.
- Agent Semantic Model ontology — superseded.
- ANL/ASM federation payload — superseded.

Historical copies live under `history/`/`research/` only.

## Open questions worth measuring

1. Does async Team collaboration outperform strong single agents on genuinely interdependent tasks after cost?
2. What is the smallest useful set of Event Contracts?
3. Does TOON improve context efficiency enough without degrading accuracy?
4. When does a ledger projection cease to be sufficient as team memory?
5. How often does Authority Non-Solicitation create avoidable blocker churn?
6. Which collaboration topologies deserve runtime support after baseline benchmarking?


## ADR-037 — Minimal versioned institutional knowledge is V1 CORE

**Decision:** Accepted in v4.1.

Durable agents/teams retain auditable EXPERIENCE, LESSON, KNOWLEDGE, and PROCEDURE records across ExecutionProfile/model changes. The ledger remains history; knowledge is a versioned curated layer over evidence. Knowledge is not authority.

## ADR-038 — Instruction/reference Skills are V1 CORE

**Decision:** Accepted in v4.1.

Agents may propose reusable procedural skills inspired by proven external agent-skill patterns. Runtime validation/promotion/versioning controls activation. Skills do not grant capabilities and are not trusted runtime plugins.

## ADR-039 — Deterministic Audit Service is V1 CORE

**Decision:** Accepted in v4.1.

Scheduled/event-triggered software audits produce durable findings. Judgment-heavy LLM AuditWorker remains a bounded `VALIDATE NEXT` feature. Auditing observes; it does not receive executive authority.

## ADR-040 — Lab experimentation separates exploration from authority

**Decision:** Accepted conceptually; implementation tier `VALIDATE NEXT`.

Disposable high-freedom experiments run inside explicit sandbox/capability/resource budgets. Outputs are `EXPERIMENTAL_UNVERIFIED`; promotion requires independent validation. Parent may nominate but cannot unilaterally certify trust.

## ADR-041 — Inference access is an organizational resource

**Decision:** Accepted in v4.1.

V1 models subscription allowance, metered API budget, and local compute as InferencePools. Agents do not own models/quotas. Scheduler/resource manager selects feasible resources and protects configured continuity reserve.

## ADR-042 — Knowledge/Skill revision preserves history

**Decision:** Accepted.

Minor revisions and full rewrites produce new versions. Prior versions are superseded/stale/quarantined rather than silently overwritten.
## ADR-043 — Three occurrences create a pattern candidate, not truth

**Decision:** Accepted in v4.1.1.

Three related occurrences are the default minimum for proposing a repeated pattern. The threshold is configurable by consequence/task class. Subsequent evidence/experiments and appropriate validation determine promotion.

## ADR-044 — Nondeterministic conclusions are operator judgments

**Decision:** Accepted in v4.1.1.

Where deterministic/objective verification is unavailable, an authorized agent or human operator may judge according to consequence policy. The record identifies the method/operator and never presents judgment as deterministic proof.

## ADR-045 — Execution topology is empirical

**Decision:** Accepted in v4.1.1.

Agent OS has no global preference for Teams. Lab/benchmark evidence should determine whether a task class uses a single agent, Skill-assisted agent, verifier, parallel attempts, or async Team.

## ADR-046 — Skill safety is defense in depth

**Decision:** Accepted in v4.1.1.

Auditing supplements provenance, applicability, validation, versioning, capability enforcement, completion verification, revalidation and rollback/quarantine.

## ADR-047 — Usage telemetry uses best available evidence

**Decision:** Accepted in v4.1.1.

Inference usage may come from official APIs, supported provider CLIs, other supported telemetry, observed estimates, or conservative estimates. Every snapshot carries source/time/confidence. Deterministic adapters cache/rate-limit collection.
## ADR-048 — Work-First Orchestration

**Decision:** Accepted in v4.1.2.

Actual organizational work determines Task decomposition, deterministic workflows, Agent/Team use, Skills and human involvement. Benchmarks/Lab experiments are instruments for improving real work, not the workload driver.

## ADR-049 — Minimal Justified LLM Use

**Decision:** Accepted in v4.1.2.

Use the least nondeterministic mechanism sufficient. LLM inference is introduced only where adaptive reasoning, interpretation, generation, tool-use planning, or judgment provides justified value over conventional software/tools/procedures.

## ADR-050 — Task DAG is the V1 workflow representation

**Decision:** Accepted in v4.1.2.

Do not build a separate workflow DSL/engine in V1. Task nodes may identify execution as deterministic, tool, Agent, Team, human, or mixed.

## ADR-051 — Persistent Agent does not imply persistent inference

**Decision:** Accepted in v4.1.2.

An Agent is durable organizational identity/responsibility. AgentExecution is created only when model inference is needed. An Agent may own a workflow whose majority is deterministic.

## ADR-052 — Inference routing occurs after LLM justification

**Decision:** Accepted in v4.1.2.

The Resource Manager chooses among feasible inference pools/models only after the workflow determines that model inference is needed.
## ADR-053 — Minimal A2A Operator Gateway is V1 CORE

**Decision:** Accepted in v4.2.

Hermes is an intended external operator. Agent OS exposes a minimal A2A v1.0 work/status/artifact/input boundary. Internal communication remains Event Contracts.

## ADR-054 — A2A identity is not authority

**Decision:** Accepted in v4.2.

Authenticated peers map to scoped ExternalActor identities. Agent OS capability/consequence policy determines what they may cause.

## ADR-055 — A2A Task is not Agent OS Task

**Decision:** Accepted in v4.2.

External A2A task/context IDs correlate to internal Intent/Goal/Task DAG objects but do not define internal workflow semantics.

## ADR-056 — ExecutionContextManifest is V1 CORE

**Decision:** Accepted in v4.2.

Every model execution records exact Event/Knowledge/Skill/Artifact/tool/profile versions/materialization states actually available to it.

## ADR-057 — Deterministic recovery before cognitive recovery

**Decision:** Accepted in v4.2.

Tool adapters attempt safe known deterministic recovery/postcondition verification before spending a new model turn.

## ADR-058 — ToolOutcome is structured runtime evidence

**Decision:** Accepted in v4.2.

Tool outcomes include observed effect, postcondition verification, retryability, deterministic recovery and artifacts/error details.

## ADR-059 — Human approvals bind to exact effect fingerprints

**Decision:** Accepted in v4.2.

Approval authorizes the described protected effect, not general capability expansion. Changed material arguments may require a new approval.

## ADR-060 — Persist EffectObligation before consequential external effects

**Decision:** Accepted in v4.2.

Use durable outbox/obligation state, idempotency/reconciliation and explicit confirmation. Do not claim exactly-once behavior when unsupported.

## ADR-061 — SecretSource seam before secret platform

**Decision:** Accepted in v4.2.

Resolve secrets in deterministic adapters where possible; V1 needs only a small interface/simple implementation. Additional secret managers are later integrations.

## ADR-062 — Core remains a narrow waist

**Decision:** Accepted in v4.2.

Prefer existing handler/tool, tool+Skill, adapter or external integration before adding business-specific functionality to Agent OS core.
