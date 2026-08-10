# Completion review

Agent-generated results are candidate work, not completion authority. When the
runtime has no registered deterministic verifier, a successful model outcome
is recorded and the Task remains `BLOCKED` until a dedicated reviewer decides
against the exact recorded evidence.

## Reviewer boundary

Only an authenticated direct-human principal with the dedicated `REVIEWER`
role has `review_completion`. `OPERATOR` does not inherit it. External Agents
never receive it, and the A2A surface has no completion-decision method.

The reviewer role has only `read_status` and `review_completion`. Candidate
result content is returned by the review endpoint for the bound pending review;
ordinary status/result access continues to hide unverified model output.

## Event contracts

`COMPLETION_REVIEW_REQUESTED` contains a `ReviewRequest` bound to:

- organization, Task, exact Task version, and immutable Task objective;
- the `HUMAN_JUDGMENT` CompletionContract;
- exactly one recorded ToolOutcome, published result, and completion candidate;
- a SHA-256 fingerprint over that immutable request.

`COMPLETION_REVIEW_DECIDED` records the authenticated reviewer identity,
`HUMAN_JUDGMENT` method, exact fingerprint and evidence references, decision,
and optional feedback. The trusted event envelope—not payload text—supplies the
reviewer's authority.

`APPROVE` permits the runtime to emit `COMPLETION_VERIFIED` linked to the
judgment event and then `TASK_VERIFIED_COMPLETE`. It does not change the
ToolOutcome's postcondition into deterministic evidence. `REJECT` fails the
Task. `REVISE` requires feedback, resumes the Task, and supplies that feedback
to the next AgentExecution as explicitly marked untrusted content referenced by
its ExecutionContextManifest.

## HTTP control

The dedicated control is separate from natural-language messages:

```text
GET  /v1/human/reviews/{task-id}
POST /v1/human/reviews/{task-id}
```

The GET response provides the review ID, Task version, objective, fingerprint,
candidate, criteria, and evidence event references. A decision body is:

```json
{
  "review_id": "review-task-123-v7",
  "fingerprint": "<64 lowercase hexadecimal characters>",
  "decision": "APPROVE"
}
```

`REJECT` may include feedback. `REVISE` must include nonblank feedback. Bodies
are strictly decoded and bounded. A stale fingerprint, stale review ID,
cross-organization Task, conflicting retry, or non-human caller fails closed
before a decision event is written.

Decision delivery is idempotent for the same reviewer and exact content.
Startup recovery continues a durable decision from the last completed phase
without inventing judgment or replaying an uncertain model call.

The earlier pre-V1 `COMPLETION_REVIEW_REQUIRED` notification carried a
different payload and is not interpreted as this authority contract. Recovery
leaves work containing that event safely blocked for manual reconciliation;
there is no compatibility path that guesses or upgrades a judgment.
