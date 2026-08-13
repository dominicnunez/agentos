# Agent OS V1 build contract

## Invariants

1. Work determines execution topology. A task DAG is the V1 workflow representation.
2. `Agent` is durable identity; `AgentExecution` is a bounded invocation. Persistent identity is never created merely to gain parallelism.
3. Use the least nondeterministic mechanism sufficient for the work. Model inference must be explicitly justified by task policy.
4. State changes become append-only Event Contracts through the Event Gateway. Projections may be rebuilt from the ledger.
5. A tool report is not proof of an effect. `ToolOutcome` distinguishes status, observed effect, verification, and retryability.
6. Completion is evaluated against an explicit `CompletionContract`, not an actor's self-report.
7. `ExecutionContextManifest` records what an execution was actually given.
8. A2A v1.0 is an external Agent/operator boundary. It does not become the internal communication model.
9. Approval decisions fail closed. An approval authorizes a fingerprinted effect, never a general privilege expansion.

## First slice

The shared intake boundary accepts bounded work from either the private user
gateway or A2A Operator Gateway, creates an Intent, Work, and single-node or
bounded multi-node Task DAG, executes either a deterministic handler or a configured `AgentExecution`,
records each transition, applies the completion engine, and returns terminal
task state.

Exact registered work stays on a no-inference planning path. For adaptive
work, the configured provider may propose child Tasks, but the runtime validates
the closed graph schema, adds the integration root, binds the Plan to the exact
confirmed Intent fingerprint, and commits the complete Task set atomically.
Only the root is externally addressable. Downstream executions receive exact
runtime-selected dependency result events as untrusted evidence.

The fake adapter is test-only and deliberately non-intelligent: it makes the
execution seam testable without becoming a production provider.

## Implemented V1 seams

The repository includes exact fail-closed capability checks, append-only
versioned records, provenance-gated institutional knowledge, deterministic audit
rules, reserve-aware inference selection and normalized usage snapshots, an
environment-backed `SecretSource`, a confined Codex subscription adapter, a
fixed-endpoint official OpenAI Responses adapter with bounded responses and no
redirects, and
fingerprinted persist-before-effect obligations with distinct attempted and
confirmed states.

Real-provider output is durably recorded as a result and
`CANDIDATE_COMPLETE`, but remains `BLOCKED` when its CompletionContract has no
runtime verifier. The verified local owner may approve, reject, or request
revision against a fingerprinted set of exact evidence events. External Agents
cannot decide completion.
User judgment remains `HUMAN_JUDGMENT`; it never turns nonempty model text or
an unchecked ToolOutcome into deterministic proof.

The A2A adapter exposes canonical A2A v1.0 Agent Card discovery and authenticated
JSON-RPC `SendMessage` and `GetTask`, with capability-gated status, result, and
input continuation. The wire contract and JSON-RPC transport use the official
`a2aproject/a2a-go/v2` SDK; the Agent OS wrapper still owns authentication,
authorization, strict decoding, and resource limits, while SQLite remains the
only task authority. Initial messages may omit `contextId`; continuation
requires the returned durable `taskId`. It does not expose legacy discovery,
method aliases, or custom REST task routes. A2A is disabled without a reviewed
external-actor registry. Each enabled actor has a unique secret reference,
deterministic role profile, own/organization work scope, status, expiry, and
request ceilings.
Credentials and A2A wire types remain outside core domain objects. Production
deployments must explicitly configure a provider adapter.

The A2A adapter and first-party user gateway translate into one principal-aware
Intake Service. The Intent records the authenticated principal ID/kind and source
channel. Known deterministic handlers are selected without inference; otherwise
unstructured natural-language work uses `AgentExecution` because interpretation
is justified. Unsupported execution mechanisms fail closed. Local user access
uses an owner-only Unix socket and Linux peer credentials, with no bearer token
or user actor registry. A2A retains its reviewed Agent registry, credential
lifecycle, concurrency, and request limits. Non-loopback A2A requires an
explicit remote switch, TLS 1.3 certificate and key files, and an HTTPS public
origin.

Task execution publishes a typed `RESULT_PUBLISHED` contract before
`CANDIDATE_COMPLETE`. Its payload and trusted envelope carry matching Artifact
references. A2A status includes it only after verified completion and only for
an external actor with the separate `read_result` capability; `read_status`
alone cannot expose result content. Status and result lookup use opaque,
tenant-scoped work and task IDs; a mismatched request is indistinguishable from
an unknown request.

SQLite maintains a durable work-request index from authenticated organization
and caller request ID to the authoritative correlation stream. New streams use
cryptographically random reservations checked against existing correlations;
caller-controlled legacy IDs and new internal keys never share a namespace.
The same transaction that projects a Task also maintains a tenant/task lookup,
so status polling does not rebuild unrelated projections. Startup materializes
both indexes for pre-index V1 ledgers before serving, preserving existing work
without a runtime legacy lookup path or duplicate submission.
New operator identifiers must satisfy the bounded canonical format. A
previously accepted noncanonical conversation, message, or Task identifier is
grandfathered only when an exact tenant-scoped durable binding or Event already
exists; the same shape cannot create new work or new input.

Authorized external input for an A2A-originated blocked `HUMAN` Task is
persisted with its A2A `messageId` before the
Task resumes. The runtime then records a deterministic structured outcome and
uses the Completion Engine to verify the Task; the external actor cannot mint a
completion event. Input does not make unavailable tool work or uncertain
adaptive recovery executable, and it never constitutes approval for a
consequential effect. Continuation phases are keyed by the durable input event;
delivery retry and startup recovery append only missing phases and reject
conflicting input.

The A2A and private-user work/input surfaces reject authority-shaped fields such as approval,
capability, authorization, effect-obligation, freeze, and policy overrides.
Ordinary operator text that claims approval remains untrusted task content. It
cannot change a prepared `HumanApproval`, and a protected effect remains pending
with its adapter unreachable until the separately authorized user lifecycle
records an exact decision.

Durable organization/work projections now commit atomically with their
authoritative transition events and can be rebuilt by replay. Startup validates
that state before opening the operator endpoint, preserves blocked work, runs
dependency-ready pending work, retries only known-safe interrupted deterministic
work, and blocks interrupted adaptive execution whose outcome is uncertain.

Agent-proposed addressed events use runtime-stamped sender and recipient
envelopes. The SQLite ledger commits each addressed Event Contract and its
Agent, Team, or Task inbox availability in one transaction. Available events
survive restart and are materialized, in event order, only at the next
applicable AgentExecution boundary; the execution manifest records their exact
event references. A blocked child emits a typed `TASK_BLOCKED` contract to its
parent Task without granting or otherwise changing authority; the payload
describes the unmet requirement rather than an authority transition.

Approval-required effects persist their complete obligation before notification.
Approval notification, acknowledgement, pending-decision, approval, and denial
are versioned durable records. Acknowledgement has no authority effect, and a
decision must come from a user identity explicitly authorized for the exact
organization, consequence boundary, and risk. Protected execution reloads that record
by `ApprovalRef` and revalidates organization, task, actor, action, resource,
scope, boundary, descriptor, authorization references, approval reference,
idempotency key, replay arguments, fingerprint, and expiry before an adapter
can run.

Every effect obligation also binds the acting identity, exact scope, and durable
capability references. Immediately before an adapter can transition the effect
to `ATTEMPTED`, one SQLite transaction reloads the latest approval, organization
freeze, and lease records, revalidates them at a shared time-of-use boundary,
records the authorization trace, and fails closed if authority is missing,
expired, revoked, frozen, corrupt, or unavailable.

Startup discovers durable `ATTEMPTED` obligations before serving. Reconciliation
uses a separate read-only destination-status interface and never invokes the
effect-writing adapter. A reviewed registry binds each HTTPS checker to an
exact organization, action, and resource; credentials remain in the adapter
boundary. A destination observation may transition an obligation
to `CONFIRMED` or `FAILED` only with durable evidence; unavailable support,
lookup errors, unknown or malformed status, missing evidence, and attempt drift
leave the obligation explicitly `ATTEMPTED` for operator resolution. No
production effect adapter or blind resend path is enabled.

Before a Work can enter `COMPLETED`, the runtime records one fingerprinted
`WORK_COMPLETION_EVALUATED` contract. It binds the exact confirmed Intent
fingerprint, immutable Plan revision, accepted completion criteria, every
`TASK_VERIFIED_COMPLETE` projection, its preceding runtime
`COMPLETION_VERIFIED` event, and the aggregated Artifact references. Recovery
revalidates that evidence and the `WORK_COMPLETED` transition; worker-authored
result or candidate-completion content cannot substitute for it.

Work is the terminalizable layer in the `Mission > Goal > Work > Task`
hierarchy. This evidence may contribute to later Goal progress evaluation, but
it neither achieves a Goal nor revises organizational direction.

Terminal work records one typed `RUN_TELEMETRY_RECORDED` Event Contract before
the work enters `COMPLETED` or `FAILED`. The telemetry module deterministically
projects every Task in the run's authoritative Event Contract stream into
verified or rejected outcome, execution mechanisms, wall time,
provider/model/token/cost use, tool calls, messages, blocks, retries, user
interventions, safety denials, and completion evidence. Unknown provider cost
remains explicitly incomplete rather than being reported as zero. Replay
validates an existing contract and never appends a duplicate.
