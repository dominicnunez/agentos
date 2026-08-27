# Documented information control and retention

Status: **DRAFT — retention and approval rules are not approved**

## Creation and identification

Controlled AIMS information must be identifiable by stable ID, title, version,
owner, classification, lifecycle state, exact content digest, approval evidence
where applicable, review date, and supersession relationship. Content must be
appropriate for its audience, accurate to the available evidence, accessible
to authorized users, and protected from accidental or unauthorized change.

Public repository records use `governance/aims/manifest.json` and its fail-closed
verifier. Git and GitHub supply change history and review evidence but do not by
themselves authenticate a governance decision or prove that a process operated
effectively. Drafts cannot carry approval metadata. Approved or retired records
require an exact durable decision reference, approving identity, approval time,
and future review date.

## Classification and access

- `PUBLIC`: approved for public repository or publication after applicable
  review.
- `INTERNAL`: limited to authorized project participants.
- `CONFIDENTIAL`: business, supplier, vulnerability, audit, incident, personal,
  or deployment evidence requiring role-based access and disclosure review.
- `RESTRICTED`: credentials, cryptographic key material, highly sensitive
  personal data, active exploit detail, or similarly high-impact information
  held only in an approved purpose-specific system.

Only `PUBLIC` content may be committed under `governance/aims/`. Classification
does not grant access. Access follows least privilege, is reviewed, and is
revoked when purpose or role ends. Secrets and private keys never enter AIMS
documents, issue bodies, CI logs, source archives, or public evidence bundles.

## Review, change, and supersession

Owners review information by its recorded due date and after a material scope,
policy, risk, impact, legal, supplier, incident, system, control, or evidence
change. A changed approved record becomes a controlled new version and requires
approval appropriate to the decision. Retired records remain immutable history
and link reciprocally to their successor. Silent replacement, backdated
approval, self-approval by an automated system, and reuse of approval for a
different digest are prohibited.

## Proposed retention schedule

| Information class | Proposed minimum retention | Disposal or preservation rule |
|---|---|---|
| Approved policies, scope, roles, objectives, control applicability, and superseded versions | Life of the project plus 7 years | Preserve exact versions and approval history; review legal and contractual holds before disposal |
| Risk/impact decisions, internal audits, management reviews, incidents, nonconformities, corrective actions, and effectiveness evidence | 7 years after closure or supersession | Preserve chain of decision and evidence; extend for open action, claim, investigation, obligation, or hold |
| Release source, provenance, license evidence, security and acceptance evidence | Life of supported release plus 7 years | Retain immutable release/tag association and corresponding source obligations |
| Competence, awareness, delegation, and access-review evidence | Role end plus 7 years | Minimize personal data and restrict access; apply applicable employment/privacy duties |
| Supplier evaluations, agreements, performance, incidents, and exit evidence | Supplier relationship end plus 7 years | Preserve applicable contract, license, claim, and incident evidence |
| Routine operational telemetry without incident, audit, or decision value | Defined per approved deployment data schedule | Minimize and delete securely when purpose expires; do not retain merely because storage is available |

These periods are proposals, not legal conclusions. Applicable law, contract,
litigation hold, source-license duty, vulnerability handling, data-subject
rights, and deployment-specific requirements may require longer or shorter
retention. The approved obligations register controls conflicts.

## External documented information

The AIMS manager maintains an access-controlled register for standards,
regulations, contracts, licenses, provider terms, security advisories,
certificates, audit reports, and other externally originated information needed
to operate the AIMS. The register identifies source, title, issuer, applicable
scope, authoritative location, version or effective date, owner, access class,
license or use restriction, integrity evidence where available, last check,
next review, and affected internal records.

Copyrighted standards and confidential external documents are referenced, not
copied into the public repository. The final ISO/IEC 42001 readiness audit must
use an authorized current copy. A changed or unavailable external source
triggers applicability and change review.

## Storage, backup, recovery, and disposal

The owner selects storage proportionate to classification, integrity,
availability, confidentiality, retention, and recovery requirements. Backups
inherit the source classification and retention constraints. Recovery tests
must prove readability and integrity without exposing content. Disposal is
authorized, logged, appropriately secure for the medium, applied to replicas
where required, and suspended by holds. Deletion of governed evidence cannot
be initiated or certified solely by the system that benefits from its removal.
