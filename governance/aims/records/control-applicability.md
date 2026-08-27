# Control applicability register

Status: **DRAFT**

This project-specific register organizes existing and proposed controls without
reproducing the copyrighted ISO/IEC 42001 control catalogue. It is not a final
Statement of Applicability and does not establish conformity. Final control
mapping, inclusion or exclusion rationale, implementation status, evidence,
and effectiveness conclusions must be reviewed against an authorized copy of
the standard and the approved AIMS scope.

| Control theme | Proposed applicability | Current project evidence | Open decision or evidence |
|---|---|---|---|
| AI policy and accountable direction | Applicable | Draft policy; security-first repository guidance; approval boundaries | Approve policy, scope, roles, communication, and review cadence |
| Internal organization and separation of duties | Applicable | Gateway identity, role/scope checks, capability and approval separation, independent PR review | Name role holders, document delegation/conflicts, operate competence review |
| Resources and competence | Applicable | Build contract, CI, engineering and operator documentation | Approve competence criteria, evidence assessments, resource review and awareness records |
| AI system inventory | Applicable | Bounded authenticated AIMS inventory export and provider/runtime configuration projections | Add approved purpose, owner, users, deployment, data, dependencies, retention and review records |
| Impact and risk assessment | Applicable | Threat model, adversarial tests, consequence boundaries, draft risk/impact method and register | Review affected parties, rate risks, approve treatments and residual decisions per material context |
| Responsible lifecycle and change management | Applicable | Reviewed Intent, planning, Task DAGs, execution manifests, completion, Lab containment, immutable contracts | Approve lifecycle criteria and material-change screening; retain operated evidence |
| Data and information governance | Applicable | Bounded inputs, artifact checks, tenant isolation, secrets separation, public-export minimization | Approve inventory, provenance, quality, access, privacy, retention, deletion and provider-transfer rules |
| User and affected-party information | Applicable | README, gateway and dashboard views, approval and completion records | Approve intended-use, limitations, notices, accessibility, feedback, contestability and remedy process |
| Third-party and supplier control | Applicable | Pinned dependencies/actions, license and vulnerability CI, provider isolation | Approve supplier criteria and register; complete provider contract/data/continuity/exit reviews |
| Operation, monitoring and human oversight | Applicable | Durable organization loop, exact approvals, telemetry, governance inspection, fail-closed recovery | Approve metrics, operational baseline, escalation, oversight usability and effectiveness review |
| Security, resilience and incident response | Applicable | Threat model, authenticated gateways, event integrity, backup/restore, replay and vulnerability handling | Approve recovery objectives, incident procedure, exercises, retention and external checkpoint boundary |
| Audit, management review and improvement | Applicable | CI, issue and review history, deterministic evidence and draft procedures | Establish independent audit program, management review, corrective-action and effectiveness records |

## Decision method

For every applicable control in the authorized standard, the approved
Statement of Applicability must identify the control reference, applicability,
rationale, implementation owner and status, evidence, effectiveness result,
exceptions, and review date. Exclusions require evidence that they are valid
for the approved scope and obligations; convenience or missing implementation
is not an exclusion rationale.

No model, Agent, CI result, repository contributor, or control owner may make
the final applicability, conformity, or certification decision. Control
existence is not control effectiveness, and a planned control is not an
implemented control.
