# AI management system

Agent OS is being developed to support an organization operating an artificial intelligence management system (AIMS) aligned with [ISO/IEC 42001:2023](https://www.iso.org/standard/42001). ISO/IEC 42001 applies to an organization and its management system, not to a software binary in isolation. Installing Agent OS therefore does not make an operator compliant or certified.

Agent OS contributes technical controls and reviewable evidence:

- authenticated user and A2A boundaries;
- tenant-scoped durable Missions, Goals, Work, Tasks, Teams, and Agents;
- explicit authority, approval, capability, effect, and completion boundaries;
- bounded planning and execution context;
- append-only event contracts, recovery validation, and SQLite-backed state;
- provider and inference policy records;
- governed Lab experiments and promotion evidence;
- telemetry, completion evidence, and independent completion review.

The local organization dashboard can download a bounded JSON readiness artifact
from **System → Readiness evidence**. The export contains the current
tenant-scoped AI-system inventory, aggregate lifecycle counts, the typed Event
Contracts that source each public projection, explicit management-system gaps,
and a detached SHA-256 checksum over the exact downloaded JSON bytes.
`PROJECTION_AVAILABLE` means only that current
records exist in that bounded projection; it does not assert that a control is
effective or that a management-system requirement is satisfied. The export
excludes raw events and payloads, prompts, results,
artifacts, credentials, approvals, capabilities, and authority records. The
dashboard verifies the response before saving both
`agentos-aims-evidence.json` and `agentos-aims-evidence.json.sha256`. On Linux,
run `sha256sum -c agentos-aims-evidence.json.sha256` in their download directory
to verify the artifact again. The checksum is not a cryptographic attestation of
the SQLite ledger and does not convert readiness evidence into certification.

The operating organization remains responsible for its AIMS scope, AI policy, accountable roles, legal and stakeholder obligations, risk criteria, impact assessments, competence, supplier decisions, incident handling, internal audits, management reviews, corrective actions, document retention, and selected controls. Agent OS must fail closed where one of those decisions is required but has not been supplied.

The project must not describe itself or an operator as ISO/IEC 42001 certified until an appropriately qualified certification body has completed the applicable audit. [ISO/IEC 42006:2025](https://www.iso.org/standard/42006) defines additional requirements for bodies that audit and certify an AIMS.

Implementation status and the evidence plan are maintained in [ISO/IEC 42001 readiness](development/ISO_IEC_42001_READINESS.md).
