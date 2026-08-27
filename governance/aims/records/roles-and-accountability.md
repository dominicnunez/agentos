# AIMS roles and accountability

Status: **DRAFT**

One person may initially hold several roles, but responsibilities remain
distinct so a role holder can identify conflicts and obtain independent review
where required. Linux account ownership, repository administration, local
gateway access, Agent identity, and model-provider credentials do not by
themselves grant a governance role or approval authority.

| Role | Proposed accountability | Decisions reserved to the role |
|---|---|---|
| Accountable executive | Own the AIMS, policy, scope, resources, risk tolerance, and management review | Approve scope and policy; assign roles; approve resources; accept eligible residual risk; authorize certification assessment |
| AIMS manager | Maintain documented information, objectives, risk coordination, audit program, corrective actions, and review inputs | Admit controlled records after recorded approval; schedule reviews; escalate missed actions; never self-certify conformity |
| Technical owner | Maintain architecture, lifecycle controls, reliable operation, recovery, and engineering evidence | Approve technical design within delegated bounds; stop unsafe builds or operation; propose but not accept residual risk |
| Security owner | Maintain threat model, identity, authority, tenant isolation, vulnerability, incident, and security-test controls | Block unsafe change or release; classify security incidents; approve security treatment evidence within delegation |
| Product owner | Maintain intended purpose, user needs, limitations, usability, transparency, accessibility, and affected-party considerations | Approve product requirements and notices within delegated bounds; escalate material impact decisions |
| Data owner | Maintain data purpose, provenance, quality, access, retention, deletion, privacy, and provider-transfer decisions | Approve allowed data categories and handling within policy; block unapproved boundary expansion |
| Supplier owner | Evaluate and monitor providers, dependencies, integrations, contracts, continuity, and exit arrangements | Approve suppliers within delegated criteria; suspend or escalate suppliers that exceed tolerance |
| Incident owner | Coordinate containment, evidence preservation, communication, cause analysis, correction, and effectiveness review | Direct response within the approved plan; escalate consequential communications and binding decisions |
| Internal auditor | Independently evaluate the approved AIMS scope against established criteria and evidence | Report findings without management alteration; cannot audit work for which the auditor was operationally responsible without compensating independence |
| Contributor or automated system | Perform authorized work and produce bounded evidence | No policy approval, authority expansion, risk acceptance, audit closure, management review, or conformity/certification decision |

## Decision rules

- Approval must identify the authenticated decision maker, delegated role,
  exact subject and version, decision, time, conditions, and durable evidence
  reference.
- Delegation is explicit, scoped, time-bounded where appropriate, reviewable,
  and revocable. Silence or an unavailable role holder is not approval.
- A person must disclose conflicts between authorship, control ownership,
  residual-risk acceptance, incident review, and internal audit. Independent
  evidence or review is required where self-review would undermine confidence.
- Consequential decisions continue to follow Agent OS approval boundaries even
  when one person holds every project role.
- Role competence, availability, succession, and communication evidence must be
  reviewed before this document can be approved.

## Initial assignment proposal

Project leadership should explicitly identify the accountable executive and
named holders for every role, including any roles held by the same person. No
assignment is inferred from GitHub ownership, copyright, operating-system
identity, conversation participation, or possession of credentials. Until the
assignment decision is recorded, these are responsibility definitions only.
