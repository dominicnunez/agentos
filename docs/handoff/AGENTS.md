# AGENTS.md — Agent OS v4.2

Read `QUICK_START.md`, `docs/29_V1_BUILD_CONTRACT.md`, and `docs/35_WORK_FIRST_ORCHESTRATION_AND_MINIMAL_LLM.md`, `docs/36_A2A_OPERATOR_GATEWAY_AND_HERMES.md`, and `docs/37_EXECUTION_CONTEXT_TOOL_OUTCOME_EFFECTS.md` before coding.

## Non-negotiable architecture

- Event Contracts, not ANL/ASM.
- Runtime-owned trusted envelope; model content is untrusted.
- Persistent Agent/Team identity; ephemeral AgentExecution.
- Single authoritative event ledger + projections initially.
- No positive capability inheritance.
- Workers return blocked work instead of soliciting more authority.
- Human consequence boundaries remain blocked until explicit decision.
- Agents only publish `CANDIDATE_COMPLETE`; Completion Engine verifies.

## v4.1 learning rules

- V1 includes versioned institutional knowledge and instruction/reference Skills.
- Do not overwrite knowledge/skill history; revisions create new versions.
- Knowledge/skills never grant capabilities or override policy.
- Agents may propose; they may not directly activate their own knowledge/skills.
- Do not load generated skill code into the trusted runtime.

## v4.1 audit/resource rules

- Audit Service is deterministic software first; do not create an Auditor Agent.
- Audit findings observe and trigger remediation; they do not gain executive authority.
- Agents do not own model/provider/quota choice. Use the Inference Resource Manager.
- Represent uncertain provider allowance as an estimate with confidence/basis.
- Protect configured continuity reserve.

## Scope discipline

`IMPLEMENTATION_SCOPE.yaml` is authoritative. The **minimal inbound A2A Operator Gateway is V1**; broader outbound federation/delegation remains later. Lab orchestration, semantic/vector retrieval, LLM AuditWorker, executable skill evolution, predictive resource optimization, Organization Health, and automatic organization optimization are not V1 unless explicitly promoted.
## v4.1.1 hardening rules

- Three related occurrences may justify a repeated-pattern candidate; never auto-activate knowledge from count alone.
- Repeated-pattern proposals must reference concrete occurrence events; promotion needs subsequent appropriate evidence/validation.
- Record nondeterministic conclusions as agent/human operator judgment, never deterministic proof.
- Do not bias routing toward Teams. Execution topology is empirical.
- Auditing is one layer of Skill safety, not the only layer.
- Usage telemetry must record source, observation time, and confidence.
- Supported provider CLI telemetry should be invoked/parsed by deterministic adapters and cached/rate-limited; do not build an LLM quota-status loop.
## v4.1.2 work-first / minimal-LLM rules

- Build workflows from actual organizational work, not from a predetermined Agent/Team topology.
- The V1 Task dependency graph is the workflow representation; do not invent a separate workflow DSL.
- A Task may be deterministic, tool-driven, Agent-owned, Team-owned, human-operated, or mixed.
- Use LLM inference only where adaptive reasoning, interpretation, generation, tool-use planning, or judgment provides justified value.
- Do not use an LLM for exact calculations, policy enforcement, known structured parsing, deterministic routing, quota telemetry collection, or other normal software work.
- A persistent Agent does not imply persistent inference. Create AgentExecution only when model inference is needed.
- Benchmarking/Lab experimentation measures and improves real work. Do not manufacture work merely to exercise the architecture.
- Model/resource selection occurs after deciding that model inference is justified.
## v4.2 Hermes/A2A/runtime rules

- A2A is a V1 external Operator Gateway; internal Agent OS communication remains Event Contracts.
- An A2A peer maps to an `ExternalActor`; protocol connectivity does not grant administrator authority.
- Hermes may submit/manage work and provide input only within explicitly granted capabilities.
- Human consequence boundaries still require the human; Hermes cannot self-approve them.
- Persist an ExecutionContextManifest for every model execution.
- Tool adapters return structured ToolOutcome; attempt safe deterministic recovery before spending another model turn.
- Approval records bind to an exact EffectFingerprint/arguments, not general privilege.
- Persist an EffectObligation before consequential external effects; record attempt/confirmation/reconciliation.
- Do not claim exactly-once effects where the destination cannot guarantee them.
- Keep credentials out of model context where a SecretSource/tool adapter can resolve/use them.
- Do not make core domain packages depend on A2A wire types.
