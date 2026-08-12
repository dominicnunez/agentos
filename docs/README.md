# Agent OS documentation

## Use Agent OS

- [User and Agent intake](OPERATOR_INTAKE.md) - setup, private local access, the organization console, structured user tasks, and A2A intake.
- [User approval policy](APPROVAL_POLICY.md) - actions that require approval and the fail-closed baseline.
- [Approval control](APPROVAL_CONTROL.md) - the owner-only exact-effect decision boundary.
- [Completion review](COMPLETION_REVIEW.md) - user review of model-backed completion candidates.
- [SQLite recovery](SQLITE_RECOVERY.md) - backup, verification, restore, and rollback.
- [Effect reconciliation](EFFECT_RECONCILIATION.md) - evidence-based recovery of uncertain external effects.

## Connect Agents and providers

- [A2A interoperability](A2A_INTEROP.md) - the supported A2A profile and authorization boundary.
- [Codex subscription provider](CODEX_SUBSCRIPTION_PROVIDER.md) - confined subscription-provider setup.
- [OpenAI API provider](OPENAI_API_PROVIDER.md) - Responses API setup and approval requirements.

## Security and releases

- [Threat model](THREAT_MODEL.md) - protected assets, attack surfaces, controls, and residual risks.
- [Release artifacts](RELEASE.md) - reproducible Linux packages, corresponding source, and publication controls.
- [Security policy](../SECURITY.md) - private vulnerability reporting.

Engineering contracts, acceptance evidence, and release gates are grouped under [development](development/README.md).
