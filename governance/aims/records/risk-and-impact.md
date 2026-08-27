# AI risk and impact management

Status: **DRAFT — no approval or residual-risk acceptance**

## Method

Every material product, provider, data, deployment, security, governance, or
intended-use change receives risk and impact screening before authorization.
The assessment records the affected system and lifecycle stage, source and
foreseeable misuse, affected parties, existing controls, likelihood, impact,
treatment, owner, due condition, residual rating, decision, and evidence.

Likelihood and impact are scored from 1 (rare or negligible) to 5 (expected or
catastrophic). The product is classified as:

- `Critical`: 20–25;
- `High`: 12–19;
- `Medium`: 6–11; and
- `Low`: 1–5.

Critical and High risks require treatment and an explicit accountable
residual-risk decision before the affected release or operation. An unanswered
decision fails closed. A model, Agent, supplier, contributor, or control owner
cannot accept its own residual risk. Risk acceptance never overrides law,
contract, user approval boundaries, or release gates.

An impact assessment considers intended and foreseeable unintended use;
individuals and groups directly or indirectly affected; safety, security,
privacy, autonomy, fairness, accessibility, economic, environmental, and
fundamental-rights consequences; severity, scale, duration, reversibility, and
distribution; vulnerable or disproportionately affected parties; transparency
and contestability; available alternatives; monitoring; incident response; and
retirement. Operator deployments require a context-specific assessment even
when this product assessment is current.

## Initial project risk and impact register

All ratings and treatments below are proposals requiring review. `Open` means
that no residual-risk decision has been made.

| ID | Risk and affected parties | Inherent rating | Existing treatment evidence | Proposed next treatment / owner role | State |
|---|---|---:|---|---|---|
| R-001 | An external or internal actor obtains authority for consequential effects; users, counterparties, and operators may suffer financial, legal, physical, privacy, or reputational harm | 3×5 = High | Authenticated boundaries, tenant-scoped actors, capabilities, exact-effect approval, expiry, fail-closed transactions | Complete adversarial authorization review and release evidence / Security owner | Open |
| R-002 | Prompt injection or malicious context steers an Agent toward unsafe action; users, data subjects, and external parties are affected | 4×4 = High | Untrusted-content treatment, deterministic authority checks, recursive authority-shaped input rejection, bounded context | Maintain injection evaluation set and verify controls after every context-source change / Security owner | Open |
| R-003 | Cross-tenant state or evidence is disclosed; organizations and data subjects lose confidentiality | 2×5 = Medium | Organization-bound ledger streams, gateway authorization, bounded public projections, tenant-isolation tests | Add periodic live-deployment access review and disclosure exercise / Security owner | Open |
| R-004 | Model or worker output incorrectly certifies task completion; users rely on incomplete or false results | 3×5 = High | Completion Contracts, exact artifacts, independent completion engine, structured user completion, fail-closed recovery | Define evaluation samples per execution kind and effectiveness threshold / Product owner | Open |
| R-005 | Ledger evidence is altered, truncated, or wholly replaced; investigators and accountable users receive misleading history | 3×4 = High | SHA-256 event chain, startup/recovery verification, backup/restore tests, deterministic replay | Approve retention and trusted external checkpoint strategy or explicitly accept the remaining whole-store replacement boundary / Security owner | Open |
| R-006 | A model provider, dependency, build service, or integration is compromised, unavailable, legally unsuitable, or changes behavior | 3×4 = High | Pinned modules and actions, dependency and license CI, provider isolation, fake-adapter tests | Establish supplier register, evaluation, monitoring, incident notice, data terms, and exit plan / Supplier owner | Open |
| R-007 | An artificial organization pursues a harmful, unlawful, deceptive, or misaligned Mission despite technically valid execution | 3×5 = High | Reviewed intent, explicit approval boundaries, durable strategic hierarchy, policy-limited effects | Add intended-use/prohibited-use decision, affected-party review, and Mission-level impact gate / Accountable executive | Open |
| R-008 | Failure, deadlock, data loss, or provider outage prevents safe continuation or recovery; users lose availability or make decisions from stale state | 3×4 = High | Durable SQLite state, restart tests, backup/restore, deterministic recovery, blocked-task state | Approve recovery objectives and perform scheduled recovery exercise / Technical owner | Open |
| R-009 | Models produce biased, inaccessible, or systematically lower-quality outcomes for affected groups | 3×4 = High | Human review and bounded completion controls reduce reliance but do not evaluate fairness | Define use-context evaluation, affected groups, accessibility criteria, monitoring, and contestability before relevant deployment / Product owner | Open |
| R-010 | Sensitive or personal information is over-collected, retained, exposed to providers, or reused beyond purpose | 3×5 = High | Input/output bounds, private user boundary, tenant isolation, secrets outside source control, explicit boundary-expansion approval | Establish data inventory, lawful-purpose review, minimization, retention/deletion, provider-data, and privacy-impact procedures / Data owner | Open |
| R-011 | Documentation or automation implies compliance, certification, control effectiveness, or evidence truth that has not been established | 2×4 = Medium | Explicit non-certification language, untrusted-evidence boundary, independent review | Add claim review to release checklist and controlled communications procedure / AIMS manager | Open |
| R-012 | User oversight becomes nominal, coerced, overloaded, or bypassed; affected people cannot understand or contest consequential decisions | 3×5 = High | Exact approval requests, separation of local access and approval authority, durable decision records | Define usability, response-time, delegation, accessibility, conflict, escalation, and contestability requirements / Product owner | Open |

## Change, monitoring, and review

A material change includes a new provider, model class, data category, gateway,
consequential effect, approval rule, execution kind, memory source, Lab
promotion path, user population, intended use, deployment context, supplier, or
security boundary. Screening occurs before implementation authorization when
the risk can affect architecture, and again before release or deployment using
actual evidence.

Incidents, audit findings, objective failures, provider changes, affected-party
feedback, new obligations, and control-test failures trigger reassessment.
Closed risks remain reviewable history. The approved register must record the
decision identity and date; this draft contains no accepted residual risk.
