# AI management system

Agent OS is being developed to support an organization operating an artificial intelligence management system (AIMS) aligned with [ISO/IEC 42001:2023](https://www.iso.org/standard/42001). ISO/IEC 42001 applies to an organization and its management system, not to a software binary in isolation. Installing Agent OS therefore does not make an operator compliant or certified.

Agent OS contributes technical controls and reviewable evidence:

- authenticated user and A2A boundaries;
- tenant-scoped durable Missions, Goals, Work, Tasks, Teams, and Agents;
- explicit authority, approval, capability, effect, and completion boundaries;
- bounded planning and execution context;
- cryptographically chained Event Contracts, recovery validation, and SQLite-backed state;
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
dashboard verifies the response before saving one
`agentos-aims-evidence.tar` bundle. On Linux, extract it with
`tar -xf agentos-aims-evidence.tar`, then run
`sha256sum -c agentos-aims-evidence.json.sha256` to verify the JSON artifact
again. The checksum is not a cryptographic attestation of
the SQLite ledger and does not convert readiness evidence into certification.

Separately, Agent OS verifies a cryptographic stored-byte chain across the
durable SQLite event stream and binds its expected head to an Ed25519-signed
checkpoint outside SQLite. Startup, diagnostics, backup, and restore verify
both layers. The checkpoint is not included as a claim in the tenant-scoped
AIMS export and does not prove event truth, trusted time, control
effectiveness, conformity, or certification. See
[Event ledger integrity](EVENT_LEDGER_INTEGRITY.md).

The authenticated local user can also produce a bounded, payload-free
[deterministic incident replay](INCIDENT_REPLAY.md) for one durable Work
conversation. It reconstructs recorded order from a chain-verified snapshot;
it does not determine root cause, containment, corrective action, control
effectiveness, conformity, or certification.

The operating organization remains responsible for its AIMS scope, AI policy, accountable roles, legal and stakeholder obligations, risk criteria, impact assessments, competence, supplier decisions, incident handling, internal audits, management reviews, corrective actions, document retention, and selected controls. Agent OS must fail closed where one of those decisions is required but has not been supplied.

The project must not describe itself or an operator as ISO/IEC 42001 certified until an appropriately qualified certification body has completed the applicable audit. [ISO/IEC 42006:2025](https://www.iso.org/standard/42006) defines additional requirements for bodies that audit and certify an AIMS.

Implementation status and the evidence plan are maintained in [ISO/IEC 42001 readiness](development/ISO_IEC_42001_READINESS.md).
