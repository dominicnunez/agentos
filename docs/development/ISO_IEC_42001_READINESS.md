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
| Context and scope | Product boundary, architecture, threat model, build contract, and hash-bound scope/context draft | PARTIAL | Approve the AIMS scope, interested parties, internal/external issues, and applicable-obligations process |
| Leadership and accountability | Approval policy, security-first development rules, and draft AI policy and role definitions | PARTIAL | Approve AI policy, accountable owner, named/delegated roles, conflicts, and review cadence |
| AI risks and opportunities | Threat model, adversarial tests, fail-closed authority/completion controls, and draft scoring method and initial register | PARTIAL | Review ratings and affected parties; approve treatments, owners, due conditions, and residual-risk decisions |
| Objectives and planning | Durable Mission, Goal, Work, and Task model; evidence-backed Goal progress; ten measurable draft AIMS objectives | PARTIAL | Approve objectives, measures, owners, evaluation timing, resources, and change plans; retain operating results |
| Resources and competence | Build contract, project guidance, CI, reviewed provider configuration, and draft competence/resource/communication procedure | PARTIAL | Approve role competence and resource criteria; perform assessments, awareness, communication, and resource review |
| Documented information | Versioned repository, release source/provenance controls, immutable handoff, startup/recovery-verified cryptographic event-chain evidence, and a closed hash-bound public AIMS document manifest with fail-closed CI verification | PARTIAL | Approve the draft records, establish confidential evidence and external-document controls, operate retention and review, and approve an external checkpoint/retention procedure |
| AI system inventory | Authenticated bounded JSON export of durable Agents, roles, lifecycle and configuration state, providers, models, runtime adapters, aggregate operations, closed projection-lifecycle Event Contract sources, explicit gaps, and an exact-byte detached SHA-256 checksum | PARTIAL | Operator-owned intended-purpose, accountable-owner, intended-user, data, dependency-risk, deployment, retention, and review records |
| Impact assessment | Consequence candidates, user approval boundaries, and draft repeatable impact method and initial affected-party risks | PARTIAL | Approve the method; perform and retain context-specific assessments, consultation where appropriate, treatments, and residual-impact decisions |
| Responsible lifecycle | Reviewed Intent, bounded planning, Task DAGs, assignment, execution manifests, completion verification, Lab | PARTIAL | Lifecycle control mapping, acceptance criteria, monitored operation, retirement, and change-management records |
| Data and information | Input limits, untrusted-content treatment, artifact inspection, tenant isolation | PARTIAL | Data inventory, provenance, quality, access, retention, deletion, and privacy-impact procedures |
| Suppliers and third parties | Pinned Go modules, license bundle, provider policy, dependency/security CI, and draft selection/register/monitoring/exit procedure | PARTIAL | Approve criteria and operate supplier/provider evaluations, contract/data review, monitoring, incident notice, and exit evidence |
| Transparency and stakeholder information | README, operator docs, task/result views, completion/approval records, and draft communication plan | PARTIAL | Approve intended-use, disclosure, accessibility, feedback, contestability, remedy, escalation, and external reporting rules; retain communications |
| Security and resilience | Threat model, authentication/authorization, private user socket, A2A isolation, backup/recovery tests, and bounded deterministic incident replay from a verified snapshot | PARTIAL | Operational security baselines, incident exercises, recovery objectives, and deployment evidence |
| Monitoring and measurement | Run telemetry, inference usage, durable outcomes, CI, acceptance status, and deterministic tenant-scoped runtime governance inspection | PARTIAL | Approved metrics, thresholds, monitoring ownership, evaluation schedule, and effectiveness records |
| Internal audit | Independent PR review, security-first final review, payload-free runtime inspection, and draft audit program | PARTIAL | Approve and operate the program using an authorized standard copy; record independence, samples, findings, corrections, and effectiveness closure |
| Management review | Draft input/output and readiness procedure | PARTIAL | Approve the procedure, operate a review over approved AIMS evidence, and retain decisions, owners, due conditions, and follow-up effectiveness |
| Incident and corrective action | Fail-closed runtime behavior, issue tracking, tamper-evident event evidence, deterministic Work replay, and draft incident/nonconformity procedure | PARTIAL | Approve and exercise intake, containment, cause, notification, correction, corrective action, effectiveness review, and lessons records |
| Control applicability | Architecture/governance controls and a draft project-specific control-theme register | PARTIAL | Use an authorized standard copy to approve a complete Statement of Applicability with exact inclusion/exclusion rationale and effectiveness evidence |
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

The repository now contains hash-bound public drafts for steps 3 and 4 plus a
deterministic bounded assessment bundle. Draft existence does not complete
either step: approval, operation, effectiveness evidence, internal audit,
management review, and the authorized-standard applicability decision remain
required.

## Authoritative public references

- [ISO/IEC 42001:2023 — Artificial intelligence management system](https://www.iso.org/standard/42001)
- [ISO overview of AI management systems](https://www.iso.org/artificial-intelligence/ai-management-systems)
- [ISO/IEC 42006:2025 — requirements for AIMS audit and certification bodies](https://www.iso.org/standard/42006)
