# ISO/IEC 42001 readiness

## Claim boundary

Target: make the Agent OS project auditable against ISO/IEC 42001:2023 and make Agent OS useful as a technical control and evidence system within an operator's AIMS.

Current claim: **readiness work in progress; not certified**.

ISO describes ISO/IEC 42001 as a management-system standard for establishing, implementing, maintaining, and continually improving responsible AI governance using a Plan-Do-Check-Act approach. A repository can supply controls and evidence, but conformity also depends on accountable organizational decisions and operating practice. Certification is a separate assessment performed by a competent certification body.

This document is a public readiness register, not a reproduction of the copyrighted standard and not legal or certification advice. A final conformity audit must use an authorized copy of the standard and the organization's approved AIMS scope.

## Intended certification scope

The proposed project scope is the design, development, security review, release, maintenance, and support of Agent OS as software for governing artificial organizations. The scope is provisional until approved by project leadership.

Operator deployments have their own scope and evidence. Product features may support that scope but cannot silently set the operator's policy, risk tolerance, legal obligations, or control applicability.

## Readiness register

| Area | Current evidence | State | Next evidence required |
|---|---|---:|---|
| Context and scope | Product boundary, architecture, threat model, build contract | PARTIAL | Approve the AIMS scope, interested parties, internal/external issues, and applicable obligations |
| Leadership and accountability | Approval policy and security-first development rules | PARTIAL | Approve AI policy, accountable owner, delegated roles, and review cadence |
| AI risks and opportunities | Threat model, adversarial tests, fail-closed authority and completion controls | PARTIAL | Versioned AI risk register, acceptance criteria, treatment owners, and residual-risk decisions |
| Objectives and planning | Durable Mission, Goal, Work, and Task model; evidence-backed Goal progress | PARTIAL | Approved AIMS objectives, measures, owners, target dates, and change plans |
| Resources and competence | Build contract, project guidance, CI, reviewed provider configuration | PARTIAL | Competence requirements, training/awareness evidence, communication plan, and resource review |
| Documented information | Versioned repository, release source/provenance controls, immutable handoff | PARTIAL | Document owner, approval, retention, review, supersession, and external-document controls |
| AI system inventory | Authenticated bounded JSON export of durable Agents, roles, lifecycle and configuration state, providers, models, runtime adapters, aggregate operations, closed projection-lifecycle Event Contract sources, explicit gaps, and an exact-byte detached SHA-256 checksum | PARTIAL | Operator-owned intended-purpose, accountable-owner, intended-user, data, dependency-risk, deployment, retention, and review records |
| Impact assessment | Consequence candidates and user approval boundaries | NOT STARTED | Repeatable AI impact assessment with affected-party, misuse, safety, rights, and residual-impact evidence |
| Responsible lifecycle | Reviewed Intent, bounded planning, Task DAGs, assignment, execution manifests, completion verification, Lab | PARTIAL | Lifecycle control mapping, acceptance criteria, monitored operation, retirement, and change-management records |
| Data and information | Input limits, untrusted-content treatment, artifact inspection, tenant isolation | PARTIAL | Data inventory, provenance, quality, access, retention, deletion, and privacy-impact procedures |
| Suppliers and third parties | Pinned Go modules, license bundle, provider policy, dependency and security CI | PARTIAL | Supplier evaluation, service-risk review, contract requirements, monitoring, and exit plans |
| Transparency and stakeholder information | README, operator docs, task/result views, completion and approval records | PARTIAL | Approved disclosure rules, user notices, limitations, escalation, and external reporting process |
| Security and resilience | Threat model, authentication/authorization, private user socket, A2A isolation, backup/recovery tests | PARTIAL | Operational security baselines, incident exercises, recovery objectives, and deployment evidence |
| Monitoring and measurement | Run telemetry, inference usage, durable outcomes, CI, acceptance status | PARTIAL | Approved metrics, thresholds, monitoring ownership, evaluation schedule, and effectiveness records |
| Internal audit | Independent PR review and security-first final review | PARTIAL | AIMS audit program, auditor independence/competence, findings, evidence samples, and closure tracking |
| Management review | No formal record | NOT STARTED | Scheduled review inputs, decisions, actions, owners, and follow-up evidence |
| Incident and corrective action | Fail-closed runtime behavior and issue tracking | PARTIAL | AI incident/nonconformity record, containment, root cause, corrective action, effectiveness review, and lessons learned |
| Control applicability | Architecture and governance controls exist | NOT STARTED | Approved Statement of Applicability with inclusion/exclusion rationale and evidence links |
| Certification | None | EXTERNAL | Readiness assessment, internal audit, management review, corrective-action closure, then independent certification audit |

States mean:

- `PASS`: implemented, approved, operated, and backed by current evidence for the stated scope;
- `PARTIAL`: useful controls or evidence exist, but the management-system requirement is not complete;
- `NOT STARTED`: no sufficient evidence exists;
- `EXTERNAL`: completion depends on an independent party.

## Evidence rules

1. A control is not `PASS` because code or a document exists. It must have an owner, approved purpose, operating evidence, and an effectiveness review.
2. Runtime events are evidence only after their integrity, tenant, actor, lifecycle, and retention boundaries are validated.
3. Model output and external-agent content are never policy decisions, approvals, impact assessments, audit conclusions, or completion authority.
4. User policy decisions remain explicit and fail closed when unanswered.
5. Evidence exports must be bounded, redactable where required, independently verifiable, and must not expose credentials, private prompts, or unrelated tenant data.
6. Findings outside an active pull request become prioritized Issues; in-scope findings are fixed before merge.

The current export is structurally minimized rather than a general ledger dump:
it omits free-text Mission, Goal, Work, and Task content while retaining the
minimum Agent purpose and configuration fields needed for the inventory. More
detailed evidence requires an explicit future disclosure/redaction policy.
The single downloaded tar contains the JSON artifact and its `.sha256`
companion, which verifies the exact JSON bytes; it
does not attest to ledger integrity, control effectiveness, or conformity.

## Delivery order

1. Keep the full governed organization loop usable: durable direction, reviewed work intake, planning, assignment, execution, completion, and evidence.
2. Add an AIMS inventory and evidence export based on public projections and typed event contracts.
3. Add operator-owned impact, risk, incident, corrective-action, audit, and management-review records without allowing models to approve them.
4. Add document-control and Statement-of-Applicability workflows.
5. Run an internal readiness audit against an authorized standard copy, close findings, then obtain an independent certification assessment.

## Authoritative public references

- [ISO/IEC 42001:2023 — Artificial intelligence management system](https://www.iso.org/standard/42001)
- [ISO overview of AI management systems](https://www.iso.org/artificial-intelligence/ai-management-systems)
- [ISO/IEC 42006:2025 — requirements for AIMS audit and certification bodies](https://www.iso.org/standard/42006)
