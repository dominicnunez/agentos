# Agent OS repository guidance

Agent OS is a Go modular monolith for operating persistent AI-assisted organizations.
This file contains Agent OS contracts and workflow details; reusable working
preferences belong in the user's global agent guidance.

## Project-wide boundaries

- Internal coordination uses runtime-owned Event Contracts over one authoritative ledger; model-generated content is untrusted.
- Actual work determines the Task DAG and execution structure. Use model inference only where adaptive intelligence is justified.
- Authority and completion fail closed: workers cannot expand their own capabilities or certify their own completion.
- A2A is an external operator boundary, not internal IPC or implicit administrative authority.
- Keep deferred architecture deferred unless a human explicitly authorizes its implementation.
- Agent OS should support all providers Hermes offers where feasible, and use
  multiple configured providers/models for different agent tasks according to
  what the user has available. Do not constrain an organization to one provider.
  Record feasibility limits and unfinished coverage explicitly.
- Follow the Agent OS engineering authority boundaries in
  [`docs/development/repository-change-control.md`](docs/development/repository-change-control.md#engineering-authority-boundaries).

## Ledger selection and recovery evidence

- For filtered readers, check selection completeness in both directions before
  relying on shared validators. Derive each event kind's candidate predicate
  from its owning validator and its caller's applicability conditions before
  applying tenant, identity or time filters; a shared subsystem does not imply
  a shared identity scope.
- Inventory reference-bearing fields from owned payload types, including nested
  arrays, and map each to incoming selection or a justified exclusion. Retain
  every occurrence of a typed reference and its applicability fields in malformed
  JSON, including duplicate containers and discriminators, so exact validation
  can reject the source. First-value parsers and SQL path lookups do not provide
  sufficient discovery evidence.
- Tenant-scoped lookup does not prove a foreign claim against a global event ID
  is unrelated. Exercise that claim before excluding it, while keeping valid
  shared authority references from expanding unrelated history. Verify whether
  linked identifiers are globally unique or tenant-scoped in storage and recovery
  before treating cross-tenant evidence as unrelated.
- Reconstruct candidate sets from durable scope and time boundaries independently
  of claimed references. Compare required inputs and invariants with full recovery,
  including private dependencies and omitted candidates. Compare every stored
  metadata field with the owning writer and recovery rules; include forbidden
  fields in validation and byte preflight instead of dropping selected columns.
  Document deliberate exclusions rather than assuming parity.
- For cancellation changes, exercise the real transport/runtime shutdown path
  and interruptions across relevant admission writes. Verify forbidden publication,
  required accounting and preservation of already-committed decisions during
  recovery. One authority generation does not cover every runtime stop signal.

## Documentation layout and threat model

- Keep schemas in the repository-root `schemas/` directory.
- Reserve the root of `docs/` for `README.md` and `threat-model.md`. Put other
  documents in topic folders: `guides/` for user workflows, `architecture/` for
  runtime contracts, `integrations/` for protocols/providers, `security/` for
  controls, `operations/` for recovery/releases, `governance/` for product
  governance, and `development/` for engineering checks/evidence. Keep images
  in `docs/images/`.
- Keep usage instructions, implemented contracts, software-control evidence and
  fixtures consumed by repository checks in the repository. Keep competitive
  strategy and non-code certification planning outside it; do not link to those
  records from repository files.
- Update `docs/threat-model.md` in the same PR as code affecting architecture,
  trust boundaries, attack surfaces, controls, prerequisites, residual risks or
  severity. Describe the codebase at the document's revision without embedding
  or routinely refreshing a baseline commit hash. Distinguish implemented and
  incomplete controls from future prerequisites. Preserve its four numbered
  sections: Overview; Threat model, Trust boundaries and assumptions; Attack
  surface, mitigations and attacker stories; Criticality calibration. Retain
  subsection structure, explanatory prose and severity lists when updating content.

## Repository review automation and CI

- Follow the Agent OS [normal change path](docs/development/repository-change-control.md#normal-change-path),
  including its final-head review gate for follow-up commits.
- General reviews run automatically on pushes to ready PRs. Move a draft to ready
  before expecting automatic review. If an automatic review demonstrably did not
  start and a manual review is necessary, comment `@codex review` once.
- Security-sensitive code and changes to `docs/threat-model.md` require security
  review. After general review returns with no outstanding findings, request
  `@codex security review` once unless automation has already started it. Applicable
  CI need not finish before that request, but must pass before merge.
- For code changes, final-head checks include both push and PR CI where configured.
  Documentation-only PRs retain document validation and required reviews while
  skipping code checks through the existing change classifier.

## Security backlog and ISO/IEC 42001 decision

- Open GitHub issues are the authoritative security-gap backlog. Local gap
  Markdown files are historical/reference material; reconcile them with current
  issues and code rather than treating them as a separate active backlog.
- Issue #126 is excluded from active work and closed as not planned following
  the certification decision. Do not reopen or resume it unless the user
  explicitly changes that decision.
- Pursuit of official ISO/IEC 42001 certification is canceled for now. Code must
  continue to meet applicable requirements; preserve and implement software
  controls and verification. Canceling certification does not authorize removing
  safeguards.
- Non-code ISO/IEC 42001 work is indefinitely deferred. Preserve its status and
  supporting documentation outside GitHub issues so certification can resume
  later. Do not claim formal certification from code compliance or completed checks.
