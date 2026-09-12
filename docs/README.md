# Agent OS documentation

## Use Agent OS

- [Five-minute governed workflow](guides/quickstart.md) - install, create durable direction, review work, and verify a completed Task.
- [User and Agent intake](guides/operator-intake.md) - setup, private local access, the organization dashboard, structured user tasks, and A2A intake.
- [User approval policy](guides/approval-policy.md) - actions that require approval and the fail-closed baseline.
- [Approval control](guides/approval-control.md) - the owner-only exact-effect decision boundary.
- [Completion review](guides/completion-review.md) - user review of model-backed completion candidates.
- [Planning and Task DAGs](architecture/planning-and-task-dags.md) - bounded planning, atomic graph admission, dependency evidence, and external root isolation.
- [Shared coordination](architecture/shared-coordination.md) - replayable same-Work peer Task context for Agent executions.
- [Governed Lab](architecture/lab.md) - contained experiments, unverified results, and nomination-only promotion candidates.
- [Organizational knowledge](architecture/organizational-knowledge.md) - evidence-backed versioned learning, lifecycle controls, and retrieval boundaries.
- [Governance inspection](operations/governance-inspection.md) - deterministic tenant-scoped runtime findings over chain-verified Event Contracts.
- [SQLite recovery](operations/sqlite-recovery.md) - backup, verification, restore, and rollback.
- [Effect reconciliation](operations/effect-reconciliation.md) - evidence-based recovery of uncertain external effects.
- [AI management system](governance/ai-management-system.md) - ISO/IEC 42001 readiness and the boundary between product controls and organizational conformity.

## Connect Agents and providers

- [A2A interoperability](integrations/a2a-interop.md) - the supported A2A profile and authorization boundary.
- [Codex subscription provider](integrations/codex-subscription-provider.md) - confined subscription-provider setup.
- [OpenAI API provider](integrations/openai-api-provider.md) - Responses API setup and approval requirements.
- [Structured model input](integrations/structured-model-input.md) - instruction roles, invocation-bound source evidence, and transport limitations.
- [Inference connections](integrations/inference-connections.md) - exact account identity, shared budgets, atomic policy updates, and remaining runtime integration.

## Security and releases

- [Threat model](threat-model.md) - protected assets, attack surfaces, controls, and residual risks.
- [Execution authority](security/execution-authority.md) - supply-chain boundaries, consequential tool capabilities, containment prerequisites, and staged promotion.
- [Event ledger integrity](security/event-ledger-integrity.md) - stored-byte chaining, verification boundaries, and residual tampering risks.
- [JSON boundaries](security/json-boundaries.md) - strict decoding and bounded untrusted input.
- [Incident replay](operations/incident-replay.md) - bounded, payload-free reconstruction of one durable Work conversation.
- [Release artifacts](operations/release.md) - reproducible Linux packages, corresponding source, and publication controls.
- [Security policy](../SECURITY.md) - private vulnerability reporting.

Engineering contracts, acceptance evidence, and release gates are grouped under [development](development/README.md).

## Directory standard

Only this index and the threat model live directly under `docs/`. Documents use
lowercase filenames with dashes, except conventional names such as `README.md`.

| Folder | Contents |
| --- | --- |
| `guides/` | User workflows and decisions |
| `architecture/` | Runtime contracts and organizational state |
| `integrations/` | Protocols, providers, and model inputs |
| `security/` | Authority, integrity, and input controls |
| `operations/` | Recovery, inspection, and release procedures |
| `governance/` | Product governance controls and claim boundaries |
| `development/` | Engineering contracts, checks, and technical readiness |
| `images/` | Documentation images |

Implemented wire schemas live in the repository-root [schemas directory](../schemas/README.md).
