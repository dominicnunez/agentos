# Competitive readiness

## Reference product

This comparison was verified against [`octoryn/octopus-agentos`](https://github.com/octoryn/octopus-agentos) on 2026-08-25. Its current repository describes a Packer-built AWS/Azure Ubuntu image containing browser VS Code, five AI command-line tools, cloud setup scripts, and eight open Octopus modules. The modules are included but explicitly require users to integrate them. Its multi-tenancy, SSO/RBAC, central policy, compliance reporting, and commercial operations plane are documented as planned rather than shipped.

Agent OS is a different implementation shape: one governed runtime owns durable organizational state, authority checks, event contracts, Task DAGs, execution context, completion evidence, Lab promotion, and the local dashboard. It should compete on a complete artificial-organization workflow, not by copying a bundle of unrelated AI CLIs.

## Verified comparison

| Capability | Agent OS | Octopus AgentOS repository | Priority |
|---|---|---|---:|
| Integrated governed organization kernel | Shipped and tested | Separate modules; users wire them | Defend |
| Durable Mission → Goal → Work → Task model | Shipped and tested, including initial dashboard strategy creation | Shared blackboard and runtime modules, not this integrated hierarchy | Defend |
| Reviewed natural-language intake | Shipped | AI CLIs are directly available | Defend |
| Explicit approval/effect/completion boundaries | Shipped | Policy/evidence modules are bundled; image also grants passwordless sudo | Defend |
| A2A boundary | Shipped using official SDK | MCP blackboard is the advertised coordination path | Defend |
| User-level Linux installation | Implemented and validated with user-scoped encrypted credentials on systemd 259; release still gated | Cloud image is the primary path | High |
| Browser control surface | Embedded governed dashboard | Browser VS Code | High |
| Five-minute governed example | Deterministic dashboard workflow documented and CI-proven; installed Linux lifecycle validated offline; live-provider demonstration remains separately gated | Three-step cloud positioning; real deployment verification remains pending | High |
| AWS/Azure marketplace image | Not planned for current V1 | Core product form | Medium |
| Keyless cloud model access | Not shipped | AWS Bedrock instance-role path | High |
| Managed cloud secrets | Local protected credential stores | KMS-file, SSM, and Secrets Manager scripts; env fallback | High |
| Multi-provider operator experience | Codex subscription and OpenAI API paths | Five AI CLIs | High after provider requirements |
| Documentation and dashboard languages | English documentation; English, Spanish, and Simplified Chinese dashboard catalogs with deterministic English fallback | English and Chinese docs | Medium |
| ISO/IEC 42001 evidence program | Readiness register started | ISO positioned as anchor; compliance plane planned | Critical |
| Governance-specific inspection | Deterministic runtime rules over tenant-scoped, chain-verified Event Contracts; local read-only dashboard report | Inspect provides a generic static workspace rule host | High |

## Product rules

- Do not weaken least privilege to match a faster demo. In particular, no passwordless-sudo agent workspace, shared root-equivalent browser password, or credential exposure to arbitrary tools.
- Prefer one complete governed workflow over a larger list of installed agents or tools.
- Distinguish shipped code from plans in every comparison and release claim.
- Treat ISO/IEC 42001 as an operating management system with evidence, review, and continual improvement—not as a badge attached to technical features.

## Near-term competitive path

The [installed Linux acceptance evidence](V1_RELEASE_READINESS.md), completed in
[Issue #135](https://github.com/dominicnunez/agentos/issues/135#issuecomment-5554247782)
and [PR #141](https://github.com/dominicnunez/agentos/pull/141), includes the
generated user service on Ubuntu 26.04 with systemd 259, user-scoped encrypted
credentials, UID-bound access, restart continuity, and backup/restore. Provider
networking was denied and no live provider credential was used. The
[five-minute dashboard workflow](../QUICKSTART.md#verification-evidence) has
separate deterministic CI coverage. These results do not establish live-provider
acceptance or authorize a release.

The [dashboard language catalogs](../OPERATOR_INTAKE.md) translate display text
only. Protocol values, identifiers, fingerprints, states, approval phrases, and
authority semantics remain canonical; the documentation is not fully translated.

1. Validate a bounded live-provider demonstration on a clean user-mode Linux installation after the separate provider test requirements and approvals are satisfied.
2. Extend the shipped AIMS inventory and governance inspection with
   operator-owned management records.
3. Add keyless/managed-secret provider adapters behind current credential and policy boundaries.
4. Keep installed Linux lifecycle and deterministic dashboard evidence current, and revalidate the intended release candidate before any Agent OS release decision.
