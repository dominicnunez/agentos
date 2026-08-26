# Organizational knowledge

Agent OS distinguishes immutable organizational history from curated current
learning. The event ledger records what happened. Versioned knowledge records
capture what an Agent, Team, or Organization currently believes is useful,
why it believes it, how it was validated, and when that belief stopped being
trusted.

This is deliberately not a general memory platform. It uses no embeddings,
vector database, semantic retrieval, automatic consolidation, or model-driven
promotion.

## Lifecycle

Knowledge has one stable identity and contiguous immutable revisions:

`CANDIDATE -> ACTIVE -> SUPERSEDED | STALE | QUARANTINED`

A candidate may also move directly to `QUARANTINED`. Every transition is a
runtime-owned Event Contract coupled atomically to its projection record.
Generic record writes cannot create knowledge.

A proposal records its type, exact scope, content, basis, author, concrete
provenance events, optional occurrence events, derived knowledge versions, and
artifact evidence. Repeated observations require at least three distinct event
references to create a candidate, but frequency never activates it.

Activation records a closed validation method, validator identity, validation
event references, and verification time. An external Agent cannot activate its
own proposal. Later terminal revisions preserve the prior validation record.

## Security boundary

All referenced events must already exist in the same Organization when a
transition is committed. Agent- and Team-scoped knowledge must bind to a
durable same-Organization roster record. Derived knowledge must reference an
exact earlier `ACTIVE` version in the same Organization. Startup reconstruction
rechecks evidence timing, tenant ownership, scope, lifecycle order, and derived
lineage from the event stream.

Retrieval accepts one exact Organization and scope, reads only current
`ACTIVE` records, and uses deterministic bounded text matching. Candidate,
superseded, stale, quarantined, cross-tenant, malformed, and unadmitted records
are excluded.

Knowledge remains untrusted context. It cannot grant a capability, satisfy an
approval, permit an effect, change policy, certify completion, establish event
truth, or demonstrate ISO/IEC 42001 conformity or certification.

## Execution-context status

The current boundary establishes safe lifecycle, provenance, recovery, and
retrieval contracts. Knowledge is not yet materialized into Agent execution
inputs. Enabling that requires selection and current-state validation inside
the same SQLite transaction that admits execution start, plus exact
completion-time replay of every manifested knowledge version. Until those
checks exist, execution manifests continue to contain no knowledge references.
