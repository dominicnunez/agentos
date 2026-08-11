# Agent OS documentation

Start with the guide that matches the work you are doing.

## Operate Agent OS

- [Operator intake](OPERATOR_INTAKE.md) — authenticated Human and A2A request routing.
- [Human approval policy](APPROVAL_POLICY.md) — actions that require approval and the fail-closed baseline.
- [Approval control](APPROVAL_CONTROL.md) — the isolated exact-effect decision boundary.
- [Completion review](COMPLETION_REVIEW.md) — human review of model-backed completion candidates.
- [SQLite recovery](SQLITE_RECOVERY.md) — backup, verification, restore, and rollback.
- [Effect reconciliation](EFFECT_RECONCILIATION.md) — evidence-based recovery of uncertain external effects.

## Connect agents and providers

- [A2A interoperability](A2A_INTEROP.md) — the supported A2A profile and authorization boundary.
- [Codex subscription provider](CODEX_SUBSCRIPTION_PROVIDER.md) — confined subscription-provider configuration.
- [OpenAI API provider](OPENAI_API_PROVIDER.md) — Responses API configuration and approval requirements.

## Security and releases

- [Threat model](THREAT_MODEL.md) — protected assets, attack surfaces, controls, and residual risks.
- [Release artifacts](RELEASE.md) — reproducible Linux packages, corresponding source, and publication controls.
- [Security policy](../SECURITY.md) — private vulnerability reporting.

## Develop Agent OS

Engineering contracts, acceptance evidence, and release gates are grouped under [development](development/README.md).
