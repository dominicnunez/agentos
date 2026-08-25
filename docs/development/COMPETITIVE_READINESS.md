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
| User-level Linux installation | Shipped setup path; release still gated | Cloud image is the primary path | High |
| Browser control surface | Embedded governed dashboard | Browser VS Code | High |
| Five-minute governed example | Documented and CI-proven; clean installed Linux validation pending | Three-step cloud positioning; real deployment verification remains pending | High |
| AWS/Azure marketplace image | Not planned for current V1 | Core product form | Medium |
| Keyless cloud model access | Not shipped | AWS Bedrock instance-role path | High |
| Managed cloud secrets | Local protected credential stores | KMS-file, SSM, and Secrets Manager scripts; env fallback | High |
| Multi-provider operator experience | Codex subscription and OpenAI API paths | Five AI CLIs | High after provider requirements |
| Bilingual documentation | i18n-ready direction only | English and Chinese docs | Medium |
| ISO/IEC 42001 evidence program | Readiness register started | ISO positioned as anchor; compliance plane planned | Critical |

## Product rules

- Do not weaken least privilege to match a faster demo. In particular, no passwordless-sudo agent workspace, shared root-equivalent browser password, or credential exposure to arbitrary tools.
- Prefer one complete governed workflow over a larger list of installed agents or tools.
- Distinguish shipped code from plans in every comparison and release claim.
- Treat ISO/IEC 42001 as an operating management system with evidence, review, and continual improvement—not as a badge attached to technical features.

## Near-term competitive path

1. Validate the five-minute governed workflow on a clean user-mode Linux installation after the provider live-test approvals are satisfied.
2. Add AIMS inventory/evidence export and operator-owned management records.
3. Add keyless/managed-secret provider adapters behind current credential and policy boundaries.
4. Validate the installed Linux binary, quickstart, backup/restore, and full dashboard workflow before any Agent OS release decision.
