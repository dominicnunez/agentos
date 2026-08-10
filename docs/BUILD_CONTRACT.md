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

The shared intake boundary accepts bounded work from either the direct Human
Gateway or A2A Operator Gateway, creates an Intent, Goal, and single-node Task
DAG, executes either a deterministic handler or a fake-model `AgentExecution`,
records each transition, applies the completion engine, and returns terminal
task state.

The fake adapter is deliberately non-intelligent: it makes the execution seam testable without hiding deterministic work behind an LLM.

## Implemented V1 seams

The repository includes exact fail-closed capability checks, append-only
versioned records, provenance-gated institutional knowledge, deterministic audit
rules, reserve-aware inference selection and normalized usage snapshots, an
environment-backed `SecretSource`, a real OpenAI-compatible model adapter, and
fingerprinted persist-before-effect obligations with distinct attempted and
confirmed states.

The A2A adapter exposes canonical A2A v1.0 Agent Card discovery and authenticated
JSON-RPC `SendMessage` and `GetTask`, with capability-gated status, result, and
input continuation. It does not expose legacy discovery, method aliases, or
custom REST task routes. A2A is disabled without a reviewed external-actor
registry. Each enabled actor has a unique secret reference, deterministic role
profile, own/organization work scope, status, expiry, and request ceilings.
Credentials and A2A wire types remain outside core domain objects. Production
deployments must explicitly configure a provider adapter.

The A2A adapter and first-party Human Gateway translate into one principal-aware
Intake Service. The Intent records the authenticated principal ID/kind and source
channel. Known deterministic handlers are selected without inference; otherwise
unstructured natural-language work uses `AgentExecution` because interpretation
is justified. Unsupported execution mechanisms fail closed. Direct human chat
uses a credential distinct from external Agents and is a work/input surface, not a
trusted approval API.

Task execution publishes a typed `RESULT_PUBLISHED` contract before
`CANDIDATE_COMPLETE`. Its payload and trusted envelope carry matching Artifact
references. A2A status includes it only after verified completion and only for
an external actor with the separate `read_result` capability; `read_status`
alone cannot expose result content. Status and result lookup are scoped to the
authenticated actor's organization; a mismatched request is indistinguishable
from an unknown request.

Authorized external input for a blocked `HUMAN` Task is persisted with its A2A
`messageId` before the
Task resumes. The runtime then records a deterministic structured outcome and
uses the Completion Engine to verify the Task; the external actor cannot mint a
completion event. Input does not make unavailable tool work or uncertain
adaptive recovery executable, and it never constitutes approval for a
consequential effect. Continuation phases are keyed by the durable input event;
delivery retry and startup recovery append only missing phases and reject
conflicting input.

The A2A and direct-human work/input surfaces reject authority-shaped fields such as approval,
capability, authorization, effect-obligation, freeze, and policy overrides.
Ordinary operator text that claims approval remains untrusted task content. It
cannot change a prepared `HumanApproval`, and a protected effect remains pending
with its adapter unreachable until the separately authorized human lifecycle
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

Human-required effects persist their complete obligation before notification.
Approval notification, acknowledgement, pending-decision, approval, and denial
are versioned durable records. Acknowledgement has no authority effect, and a
decision must come from a human identity explicitly authorized for the exact
organization, consequence boundary, and risk. Protected execution reloads that record
by `ApprovalRef` and revalidates organization, task, action, resource, boundary,
fingerprint, and expiry before an adapter can run.

Every effect obligation also binds the acting identity, exact scope, and durable
capability references. Immediately before an adapter can transition the effect
to `ATTEMPTED`, one SQLite transaction reloads the latest approval, organization
freeze, and lease records, revalidates them at a shared time-of-use boundary,
records the authorization trace, and fails closed if authority is missing,
expired, revoked, frozen, corrupt, or unavailable.

Terminal work records one typed `RUN_TELEMETRY_RECORDED` Event Contract before
the goal enters `COMPLETED` or `FAILED`. The telemetry module deterministically
projects every Task in the run's authoritative Event Contract stream into
verified or rejected outcome, execution mechanisms, wall time,
provider/model/token/cost use, tool calls, messages, blocks, retries, human
interventions, safety denials, and completion evidence. Unknown provider cost
remains explicitly incomplete rather than being reported as zero. Replay
validates an existing contract and never appends a duplicate.
