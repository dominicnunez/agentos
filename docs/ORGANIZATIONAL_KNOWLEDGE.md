# Organizational knowledge

Agent OS distinguishes immutable organizational history from curated current
learning. The event ledger records what happened. Versioned knowledge records
capture what an Agent, Team, or Organization currently believes is useful,
why it believes it, how it was validated, and when that belief stopped being
trusted.

This is deliberately not a general memory platform. It uses no embeddings,
vector database, semantic retrieval, automatic consolidation, or model-driven
promotion.

The current runtime wire contract is
[`schemas/knowledge-record.schema.json`](schemas/knowledge-record.schema.json).
CI compares that closed schema's property and required-field sets with the Go
`KnowledgeRecord` type. The preserved handoff schema records the earlier design
snapshot and is not the active serialization contract.

## Lifecycle

Knowledge has one stable identity and contiguous immutable revisions:

`CANDIDATE -> ACTIVE -> SUPERSEDED | STALE | QUARANTINED`

A candidate may also move directly to `QUARANTINED`. Every transition is a
runtime-owned Event Contract coupled atomically to its projection record.
Generic record writes cannot create knowledge.

An Agent may publish an untrusted `KNOWLEDGE_PROPOSED` event, but that event is
input to curation and is not itself a knowledge projection. Runtime admission
creates the candidate only after the published proposal payload's knowledge
identity, type, title, content, basis, applicability, occurrence refs, and
artifact refs match the candidate and the emitting Agent's durable execution
is proven live. A candidate, active record, or stale record may be
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
reference a proposal bound to that Agent's exact admitted task execution. User
and external-Agent authorship must reference the complete canonical gateway
Event Contract, not only a decodable payload. Internal-Agent proposals must
also occur before the bound execution finishes or advances to another Task
revision. Every activation requires validation evidence admitted after the
current candidate proposal; repeated-pattern evidence additionally cannot
reuse an occurrence as its validation. No Agent may activate its own proposal.
Later terminal revisions, including direct candidate quarantine, preserve the
prior validation state and cannot mint a judgment while invalidating a record.

Validation method names do not create proof. Deterministic activation requires
a candidate-bound `KNOWLEDGE_VALIDATION_RECORDED` contract that references the
exact successful `TOOL_OUTCOME_RECORDED` contract produced by one admitted,
deterministic Task execution that remains open through validation admission,
with a recomputed verified
postcondition. Repeated-observation activation requires at least three
candidate-artifact-bound `EVIDENCE_PUBLISHED` contracts, each using the published
`summary` and nonempty `artifact_refs` payload, from distinct authenticated
Agent executions. Their correlation remains the durable Work correlation so
the exact execution can be proven; the candidate's validation references bind
the evidence to the knowledge record. User and Agent judgments require both an
exact-candidate typed `HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED`,
`A2A_KNOWLEDGE_JUDGMENT_RECEIVED`, or `KNOWLEDGE_JUDGMENT_PUBLISHED` statement
and the exact prior `CAPABILITY_CHECKED` contract it names. The statement binds
the knowledge ID, candidate version, validator identity and kind, source
channel, authorizing Task, artifacts, and decision. Generic user/A2A input is
not a judgment. A
capability check authorizes the judgment but is not itself proof that the
validator made one. Experimental and mixed activation remain fail-closed until an equally
specific relational evidence contract is admitted; an `AUDIT_NOTE` or other
unrelated recent event is never validation evidence.

## Security boundary

All referenced events must already exist in the same Organization when a
transition is committed, and a knowledge record's creation time cannot predate
its cited provenance or occurrence evidence. Agent- and Team-scoped knowledge must bind to a
durable same-Organization roster record. Derived knowledge must contain at
least one lineage reference and every reference must identify an exact earlier
`ACTIVE` version in the same Organization. Proposal and activation also require
that source version to remain current, and the derived record's creation time
cannot predate the cited source revision's activation; terminal invalidation preserves the
already-admitted historical lineage so dependent knowledge can fail closed.
User or Agent judgments require the typed exact-candidate statement described
above; internal-Agent statements must also be tied to the exact admitted
execution. Every statement names an exact `knowledge.validate` capability
check whose authenticated principal kind matches the claimed validator;
revocation and expiry are rechecked in the
activation transaction, and the lease must have been admitted by that same
Organization before the judgment. Freeze state is checked at the capability
check, the typed statement, and activation, and authorization uses the activation event's single captured
timestamp. Startup reconstruction and offline backup verification use the same
Event-Contract admission validator. Capability and freeze history is replayed
only when every lifecycle event has one exact, ordered durable record admitted
atomically with it; bare authority-shaped event labels grant nothing. Recovery
rechecks evidence timing, tenant ownership, scope, lifecycle order, validator
attribution, lease expiry or revocation, and lineage.

The deterministic Audit Service separately inspects every currently active
record for missing or cross-Organization evidence, invalidated current lineage,
and verification age. Verification age is deployment policy: Agent OS does not
invent a universal duration. If an active record exists without an explicitly
configured maximum age, the audit emits `knowledge_staleness_policy_missing`;
an expired record emits `knowledge_revalidation_due`. Findings do not mutate or
reactivate knowledge.

Retrieval accepts one exact Organization and scope, reads one bounded SQLite
snapshot of current `ACTIVE` records, ranks matching records by newest current
activation and stable record identity, and uses deterministic text matching.
Candidate, superseded, stale, quarantined, cross-tenant, malformed, and
unadmitted records are excluded.

Storage migration quarantines pre-admission legacy knowledge records outside
the authoritative projection namespace while preserving their exact bytes for
review. It never silently promotes those incomplete historical records.

Knowledge remains untrusted context. It cannot grant a capability, satisfy an
approval, permit an effect, change policy, certify completion, establish event
truth, or demonstrate ISO/IEC 42001 conformity or certification.

## Execution-context status

Agent executions receive a deterministic bounded selection of relevant current
knowledge. Selection, input materialization, the execution-input digest, the
exact context manifest, and execution start are admitted in one SQLite
transaction. A manifest failure rolls back the running Task transition. The
selector considers only exact `ACTIVE` revisions in the same
Organization whose scope is the Organization, the assigned Agent, or a Team
that contained the Agent at that start boundary. Relevance uses bounded
deterministic text matching against the durable Task; it does not use a model,
embedding, or semantic index. Derived knowledge is eligible only while every
exact source version and its transitive lineage remain current and `ACTIVE` at
the same boundary; invalidated lineage fails closed before model execution.

At most 16 full records and 96 KiB of knowledge are selected, newest activation
first with stable identity as the tie-breaker. The aggregate Agent input remains
subject to the separate 256 KiB execution-context limit. Records that do not
fit are not represented as materialized. Every included record is listed by
exact identity and version with `FULL` materialization in the
`ExecutionContextManifest`; the context-builder contract is `v2`.

Completion admission reconstructs knowledge history from sealed projection
Event Contracts at the execution-start sequence, reruns the same scope,
relevance, ordering, count, and byte rules, and requires an exact manifest
match before recomputing the execution-input digest. Later supersession,
staleness, or quarantine does not rewrite the historical input, while a record
that was not active and in scope at the start cannot be claimed by the
execution.
