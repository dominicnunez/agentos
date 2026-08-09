# Agent OS — AI Coding Handoff v4.2

**v4.2 supersedes v4.1.2 and all earlier handoffs.**

Start with **`QUICK_START.md`**. For implementation, then read `AGENTS.md`, `IMPLEMENTATION_SCOPE.yaml`, `docs/29_V1_BUILD_CONTRACT.md`, and `docs/35_WORK_FIRST_ORCHESTRATION_AND_MINIMAL_LLM.md`.

v4.1.1 keeps the v4 Event Contract architecture and makes four focused changes:

1. **Minimal versioned institutional knowledge is V1 CORE.** Durable agents/teams retain evidence-backed experience, lessons, knowledge, and procedures across model changes.
2. **Instruction/reference-based Skills are V1 CORE.** Agents may propose reusable skills; runtime validation/promotion/versioning prevents a learned procedure from silently becoming trusted authority or code.
3. **A deterministic Audit Service is V1 CORE.** Scheduled and event-triggered audits produce findings; judgment-heavy LLM audit workers remain `VALIDATE NEXT`.
4. **Inference Resource Management is V1 CORE.** Subscription quota, metered API budget, and local compute are first-class resource pools managed by the runtime, not owned by agents.

A bounded **Lab/Experiment** model is specified under `VALIDATE NEXT`: high-freedom disposable exploration inside containment, with promotion gates before results become trusted knowledge, skills, configuration, or real-world effects.

The core Event Contract rule remains unchanged:

> Structure only what software must understand. Agent content does not create control-plane authority.

The implementation tiers remain authoritative in `IMPLEMENTATION_SCOPE.yaml`. Do not build later-tier features unless explicitly promoted.
## v4.1.1 patch hardening

v4.1.1 does not change the v4.1 architecture. It clarifies:

- three related occurrences are the default minimum for creating a repeated-pattern candidate, not sufficient proof for active knowledge;
- evidence/experiments determine knowledge promotion;
- nondeterministic conclusions are explicitly recorded as operator judgment by an authorized agent or human;
- execution topology is empirical: single agent and team structures are alternatives to test, not a built-in preference for teams;
- Skills use defense in depth: validation, provenance, applicability, capability enforcement, auditing, versioning, and rollback;
- audit success is measured partly by how quickly institutional mistakes are detected, traced, corrected, or quarantined;
- inference usage telemetry uses the best legitimate source available (API, provider CLI, supported telemetry, observation, conservative estimate), with source/confidence/time attached;
- provider CLI telemetry is collected by deterministic adapters and cached/rate-limited rather than repeatedly interpreted by an LLM.
## v4.1.2 patch

v4.1.2 changes implementation philosophy, not the Event Contract architecture:

- actual work defines workflows/agents/Teams;
- benchmarking and Lab experiments are measurement tools, not the reason work exists;
- V1 Task dependency graphs are the workflow representation; no separate workflow DSL;
- Tasks can use deterministic software, tools, Agents, Teams, humans, or mixed decomposition;
- LLM inference is injected only when adaptive reasoning/interpretation/generation/judgment is justified;
- a persistent Agent may own mostly deterministic work and invoke `AgentExecution` only when needed;
- model/resource selection happens only after determining that an LLM is needed;
- operational telemetry is gathered from representative real work and controlled replays/held-out real tasks when comparisons are needed.
## v4.2 changes

v4.2 preserves the Event Contract/work-first/minimal-LLM architecture and adds concrete interoperability/runtime-hardening requirements learned from the latest Hermes Agent releases:

1. **Minimal A2A v1.0 Operator Gateway is V1 CORE.** Hermes is an intended external operator/manager of Agent OS.
2. **ExecutionContextManifest is V1 CORE.** Record exactly what each model execution actually had available.
3. **Structured ToolOutcome is V1 CORE.** Tools report observed effect, postconditions, retryability and deterministic recovery.
4. **Deterministic recovery before cognitive recovery.**
5. **Effect-bound approvals.** Human approvals fingerprint the exact protected effect/arguments rather than widening authority.
6. **Durable EffectObligation/outbox is V1 CORE for consequential external effects.** Persist-before-effect, idempotency/reconciliation, explicit confirmation.
7. **SecretSource seam is V1 CORE**, initially with a simple implementation; external secret managers remain later integrations.
8. Source-grounded knowledge validation, advanced context compaction, outbound A2A delegation, and additional secret-manager adapters remain gated.

Hermes as an external A2A operator is distinct from using Hermes as an internal Agent OS worker runtime.
