# Completion review

Agent-generated results are candidate work, not completion authority. When no
registered deterministic verifier exists, a successful model outcome is
recorded and the Task remains `BLOCKED` until the configured user reviews the
exact durable evidence.

The review control is available only through the private Unix socket to the
verified installation owner. A2A Agents never receive this capability, and the
A2A protocol boundary exposes no completion-decision method.

## Event contracts

`COMPLETION_REVIEW_REQUESTED` binds a ReviewRequest to the organization, Task,
exact Task version and objective, `HUMAN_JUDGMENT` CompletionContract, recorded
ToolOutcome, candidate result, evidence references, and a SHA-256 fingerprint.

`COMPLETION_REVIEW_DECIDED` records the authenticated local identity, exact
fingerprint and evidence, decision, and optional feedback. The trusted event
envelope—not response text—supplies identity.

`APPROVE` permits `COMPLETION_VERIFIED` and then
`TASK_VERIFIED_COMPLETE`. It does not relabel the ToolOutcome as deterministic
evidence. `REJECT` fails the Task. `REVISE` requires feedback, resumes the Task,
and marks that feedback as untrusted content in the next execution context.

## Private user control

```text
GET  /v1/user/reviews?after={opaque-review-cursor}&limit={1..100}
GET  /v1/user/reviews/{task-id}
POST /v1/user/reviews/{task-id}
```

The collection GET returns pending organization-scoped reviews newest first
from a cursor-bounded SQLite projection, including internal child Tasks that
remain unavailable through A2A. It does not scan the organization's Task
history. The default page size is 50; `next_after` is an opaque ledger cursor
present only when another page exists.
The Task GET supplies the review ID, Task version, objective, fingerprint,
candidate, criteria, and evidence event references. A decision body is:

```json
{
  "review_id": "review-task-123-v7",
  "fingerprint": "<64 lowercase hexadecimal characters>",
  "decision": "APPROVE"
}
```

`REJECT` may include feedback. `REVISE` requires nonblank feedback. Strict size
limits and decoding apply. A stale fingerprint or review ID, different
organization, conflicting retry, or unauthenticated local account fails closed
before a decision event is written.

Delivery is idempotent for the same identity and exact content. Startup
recovery continues a durable decision from its last completed phase without
inventing judgment or replaying an uncertain model call.
An exact terminal-record read and the bounded recent-decision projection
perform the same continuation from the durable decision. If another authorized
user reconnects after the response was lost, Agent OS keeps the original
reviewer identity and never rewrites the decision merely to match the current
session.
