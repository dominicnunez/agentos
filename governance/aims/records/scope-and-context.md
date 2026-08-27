# AIMS scope and organizational context

Status: **DRAFT**

## Proposed scope

The proposed Agent OS project AI management system covers the design,
development, security review, testing, release preparation, publication,
maintenance, vulnerability response, documentation, and support of the
`dominicnunez/agentos` software repository as a system for governing persistent
artificial organizations.

It covers project decisions and evidence concerning:

- the Go modular-monolith runtime, SQLite event ledger, user and A2A gateways,
  dashboard, provider adapters, completion and approval controls, Lab,
  organizational knowledge, evidence, replay, inspection, and coordination;
- source, dependency, build, test, review, release, provenance, recovery, and
  security processes controlled by the project;
- intended purpose, foreseeable misuse, user information, affected-party
  impacts, suppliers, incidents, corrective action, internal audit, management
  review, and continual improvement for the product lifecycle; and
- remote project work and the GitHub-hosted services and approved local systems
  used to perform that work.

## Boundaries and exclusions

The proposed scope does not claim management control over an operator's
independent Agent OS deployment, an external model or infrastructure provider's
internal operations, an A2A client's internal behavior, customer data or
business processes not controlled by the project, or an independent auditor or
certification body. Those parties and deployments require their own applicable
governance, risk, impact, legal, security, and operating evidence.

Interfaces, dependencies, contractual requirements, published documentation,
and risks at those boundaries remain in scope where the project can select,
control, monitor, disclose, replace, or respond to them. An exclusion cannot be
used to ignore an applicable obligation or a material risk created by Agent OS.
Governed web and document ingestion is outside the current product scope.

## Organizational context

Relevant internal issues include a small initial project team, possible
concentration of several accountable roles in one person, security-first and
fail-closed architecture, a public source repository, Linux-only initial
support, pre-release status, rapid AI-provider change, and the need to turn
technical evidence into operated management-system evidence.

Relevant external issues include evolving AI, privacy, cybersecurity, consumer,
employment, accessibility, intellectual-property, product-liability, export,
and sector-specific obligations; model and infrastructure concentration;
prompt injection and software-supply-chain threats; stakeholder expectations
for transparency and contestability; environmental and compute impacts; and
the distinction between product controls, operator conformity, and independent
certification.

Applicable legal, regulatory, contractual, and other obligations depend on
jurisdiction, intended use, affected people, data, suppliers, and deployment.
The project must maintain a reviewed obligations register and obtain qualified
advice where required; this draft does not determine legal applicability.

## Interested parties and needs

| Interested party | Relevant needs and expectations |
|---|---|
| Users and operators | Secure installation, explicit authority, reliable work, understandable approvals, recovery, accurate limitations, support, and exportable evidence |
| People affected by Agent OS-directed activity | Safety, legality, privacy, fairness, accessibility, notice where appropriate, contestability, remedy, and accountable oversight |
| Contributors and maintainers | Clear engineering boundaries, secure development, review independence, issue handling, competence, and sustainable workload |
| Customers and deploying organizations | Defined product purpose, supplier assurance, data and provider boundaries, incident notice, continuity, change information, and integration responsibilities |
| Model, infrastructure, and software suppliers | Clear technical and security requirements, supported interfaces, responsible use, vulnerability coordination, and contractual compliance |
| Regulators, auditors, and certification bodies | Accurate scope and claims, controlled information, traceable decisions, objective evidence, access within authority, and correction of nonconformities |
| Security researchers and the public | Safe disclosure path, timely response, truthful public communication, and protection from avoidable external harm |

## Interfaces and dependencies

The AIMS depends on authenticated project governance decisions; GitHub source,
issue, review, and CI evidence; controlled local development environments; Go,
Node.js, pnpm, and compiled dependencies; model providers and the Codex
subscription adapter when enabled; the official A2A SDK and the separate
Apache-licensed Agent OS A2A extension; release and provenance services; and
future independent assessment. Supplier control applies proportionately to
each dependency.

## Approval and review conditions

Before approval, project leadership must confirm the organizational boundary,
products and activities, locations and remote-work boundary, interested
parties, applicable obligations process, interfaces, exclusions, and named
accountability. The scope is reviewed at least annually and before a material
product, intended-use, organizational, legal, supplier, or deployment change.
