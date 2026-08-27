# AI and technology supplier management

Status: **DRAFT**

Suppliers include model and embedding providers, cloud and hosting services,
source and CI platforms, build and provenance services, Go and JavaScript
dependencies, official protocol SDKs, authentication or secret services, and
specialist or certification services that can affect the AIMS or Agent OS.

## Selection and evaluation

Before production reliance, the supplier owner records the supplied service or
component, intended use, owner, alternatives, dependency and subprocessor
chain, data categories and locations, access and credential model, security and
privacy posture, AI behavior and limitations, availability and recovery,
change and incident notice, vulnerability handling, licensing and intellectual
property, applicable obligations, contract terms, monitoring, concentration,
portability, termination, and exit or replacement plan.

Evaluation depth is proportional to risk. A model provider additionally
requires review of model identity and versioning, retention and training use,
prompt/output handling, safety behavior, geographic processing, rate and quota
failure, audit evidence, material-change notification, and how Agent OS can
disable or replace it without expanding authority.

Open-source dependencies require pinned or otherwise controlled resolution,
provenance where available, license evidence, vulnerability and maintenance
review, transitive-dependency visibility, and reproducible build testing.
Automated scanners are evidence sources, not supplier approval.

## Required supplier register fields

The controlled register records: supplier ID and legal/service name; supplied
item and version; lifecycle state; accountable owner; criticality; connected AI
system and data; evaluation and evidence references; approved uses and
conditions; prohibited uses; contract or license reference; risks and
treatments; monitoring signals and cadence; incident contacts; last and next
review; change history; continuity and exit plan; and approval identity, scope,
and time.

Credentials, confidential contracts, personal contacts, security attestations
with access restrictions, and customer data remain in the approved confidential
evidence system rather than this public repository.

## Monitoring, change, and exit

Critical suppliers are reviewed at least annually and after a material model,
service, ownership, legal, contract, data, security, availability, incident, or
subprocessor change. Monitoring considers vulnerabilities, advisories, service
and quality metrics, control evidence, billing or quota anomalies, complaints,
provider notices, and exit readiness.

An unapproved material change, expired evaluation, unavailable required
evidence, breached condition, or risk beyond tolerance suspends the affected
production use until an authorized decision. Supplier content, configuration,
or identity never grants Agent OS authority, approval, capability, effect
permission, completion status, or policy change.

Exit planning covers credential revocation, traffic stop, data export or
deletion, evidence retention, configuration and dependency removal, substitute
validation, user communication, and recovery. A supplier's failure must not
silently weaken fail-closed security or human approval boundaries.
