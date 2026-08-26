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

An Agent may publish an untrusted `KNOWLEDGE_PROPOSED` event, but that event is
input to curation and is not itself a knowledge projection. Runtime admission
creates the candidate. A candidate, active record, or stale record may be
revised into a later candidate under the same identity and scope with new
content and provenance. That correction is excluded from active retrieval
until it is independently activated again.

A proposal records its type, exact scope, content, basis, author, concrete
provenance events, optional occurrence events, derived knowledge versions, and
artifact evidence. Repeated observations require at least three distinct event
references to create a candidate, but frequency never activates it.

Activation records a closed validation method, validator identity and kind,
validation event references, and verification time that cannot predate its
evidence. `AGENT` identifies a
durable internal Agent; `EXTERNAL_AGENT` identifies an authenticated A2A actor.
Those identities are never interchangeable. Internal-Agent authorship must
reference a proposal bound to that Agent's exact admitted task execution. User and external-Agent
authorship must reference an authenticated gateway event. Repeated-pattern
activation requires validation evidence admitted after the candidate proposal.
No Agent may activate its own proposal. Later terminal revisions preserve the
prior validation record.

## Security boundary

All referenced events must already exist in the same Organization when a
transition is committed. Agent- and Team-scoped knowledge must bind to a
durable same-Organization roster record. Derived knowledge must contain at
least one lineage reference and every reference must identify an exact earlier
`ACTIVE` version in the same Organization. User or Agent judgments require an
exact `knowledge.validate` capability admission whose authenticated principal
kind matches the claimed validator; revocation and expiry are rechecked in the
activation transaction, and the lease must have been admitted by that same
Organization before the judgment. Freeze state is checked at both judgment
and activation, and authorization uses the activation event's single captured
timestamp. Startup reconstruction and offline backup verification
use the same Event-Contract admission validator and verified capability-lease
history to recheck evidence timing, tenant ownership, scope, lifecycle order,
validator attribution, lease expiry or revocation, and lineage.

Retrieval accepts one exact Organization and scope, reads one bounded SQLite
snapshot of current `ACTIVE` records, and uses deterministic text matching. Candidate,
superseded, stale, quarantined, cross-tenant, malformed, and unadmitted records
are excluded.

Storage migration quarantines pre-admission legacy knowledge records outside
the authoritative projection namespace while preserving their exact bytes for
review. It never silently promotes those incomplete historical records.

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
