# User and Agent intake

Agent OS accepts work through two boundaries that converge on the same durable
conversational intake and work model:

```text
User -> private Unix socket --+
                              +-> Intake -> confirmed Intent -> Work -> Task DAG
Agent -> A2A JSON-RPC -------+                         ^
                                                       |
Mission -> Goal ---------------------------------------+
```

Content never supplies its own identity, capabilities, organization, approval,
or policy. The authenticated boundary injects those values before work enters
the event ledger.

The Mission/Goal link is optional for ad hoc Work. When present, it must point
to an active Goal in the same organization and cannot be changed by later Task
output or Work state transitions. A user or Agent may identify that Goal during
the natural-language intake conversation. The normalizer may carry only an
exact Goal ID present in the cited message, and the Goal is shown in the
fingerprinted Intent review before confirmation. It may not invent or select a
Goal. Goal and Mission activity and tenant ownership are checked in the same
SQLite transaction that records confirmation. Planning fingerprints the exact
Mission and Goal revisions it uses. Retry and recovery retain that context; if
either revision changes before execution, Agent OS terminalizes the stale Work
so replacement Work can be reviewed against the current direction instead of
silently changing the original meaning. Work completion
contributes bounded evidence only; Goal progress and achievement require a
separate trusted evaluation, and bare achievement projection updates are
rejected.

Failed Work is replaced through the same conversational review boundary, not
by editing or reopening its Plan or Tasks. The user or authorized A2A Agent
must explicitly cite the exact failed Work ID in a source message. The
normalizer may carry only that exact reference into the fingerprinted Intent;
it cannot infer a predecessor. Authenticated task views expose the durable Work
ID. When the predecessor is Goal-bound, Agent OS deterministically copies that
durable Goal binding into the reviewed Intent instead of asking the operator to
repeat it. The review surface displays both identifiers. Confirmation atomically
rechecks that the Work is failed, belongs to the same organization and Goal, and
has no prior replacement. Agent OS then creates a fresh Intent, Work, Plan, and
Task DAG.
Approvals, capabilities, effect permission, artifacts, completion claims, and
execution state never flow across the replacement link. A replacement may
itself be replaced only after it fails. Forks, cycles, active or completed
predecessors, cross-organization references, and Lab-to-production replacement
all fail closed in V1.

## Setup and local user access

Run `agentos` with no arguments to start or resume setup. System installation
is the default. `agentos init --user` selects the current-account alternative.
Setup is not ready until one real model provider has passed its connection
check.

System mode uses:

- `/usr/local/bin/agentos` for the executable;
- `/etc/agentos` for non-secret configuration and encrypted credential blobs;
- `/var/lib/agentos` for the ledger, artifacts, and default workspace;
- `/var/cache/agentos` for cache data; and
- `/run/agentos/user.sock` for local user access.

The account that launches setup becomes the owner. If setup elevates through
`sudo`, Agent OS verifies the original account against the Linux account
database. If root launches setup directly, root is the owner. There is no
account picker, manually entered owner, user actor file, or local bearer token.

The system service runs as the restricted `agentos` account. systemd creates
the private socket with mode `0600` for the configured owner and passes the
listening descriptor to the service. Each request is accepted only when Linux
reports the matching peer UID. Merely reaching a TCP port cannot impersonate
the local user because this gateway has no TCP listener.

User mode follows the XDG directories beneath the current account and uses its
private runtime directory for `user.sock`. It may be selected by any verified
account, including root, but system mode remains the recommended default for a
machine-level installation. User mode requires systemd 256 or newer so its
encrypted service credentials can be bound to that Linux user; system mode
uses host-scoped systemd credentials.

`agentos doctor` remains useful to the configured system owner without
elevation, but reports service-private ledger and credential inspection as
informational. Run `sudo agentos doctor` for full at-rest verification and
`sudo agentos doctor --online` for the external provider check in system mode.

## Organization dashboard

After setup, `agentos` opens the embedded organization dashboard in the
owner's browser. SvelteKit is compiled to static assets inside the Go binary;
there is no production Node server. The dashboard uses the private user
gateway and never reads or edits the SQLite ledger directly.

The launcher binds an ephemeral IPv4 loopback port. Loopback is not treated as
identity. The verified installation owner receives a one-time 256-bit
bootstrap credential through a mode-`0600` temporary page, so the credential
does not appear in terminal output or the browser-launch command line. The
bridge exchanges it once for an in-memory, eight-hour bearer session, deletes
the bootstrap file, requires an exact Host and browser origin, sends no CORS
permission, and applies a strict hash-bound Content Security Policy. Browser
credentials never cross into the Unix gateway; the bridge connects separately
and Linux `SO_PEERCRED` re-establishes the configured owner.

The initial views are:

- Overview: current intake and governance queues.
- Work: submit natural-language work and inspect its narrow Task view.
- Approvals: inspect the exact prepared effect and approve or deny it.
- Reviews: judge exact candidate results and completion evidence.
- System: inspect the dashboard session and use the read-only `agentos doctor`
  report.

Natural-language input first enters a durable, resumable intake conversation.
The configured model may propose a structured Intent, but its output is
untrusted and must pass strict schema, provenance, size, completeness, and
consequence-candidate validation. Information only the user can supply keeps
the Intent in `AWAITING_USER_INPUT`; facts Agent OS can discover belong in the
later plan. A reviewable Intent has a clear objective, deliverables, testable
completion criteria, and no missing user inputs.

The review also carries an explicit `STANDARD` or `EXPERIMENT` mode. The
normalizer may propose `EXPERIMENT` only when the user or Agent explicitly asks
to treat the work as an experiment, experimental trial, or Lab run. Ordinary
testing and verification remain `STANDARD`. Mode is untrusted routing data,
not authority, and is covered by the Intent fingerprint. A confirmed experiment
uses only the runtime-owned deterministic no-effects Lab profile; text and A2A
metadata cannot supply or widen its containment or budget. V1 rejects an
experimental Intent that requires adaptive execution before persisting its
confirmation.

Every model-backed normalization records its prompt-contract version, provider,
model, execution profile, exact input event references, and provider-reported
token usage in the ledger. Exact message retries reuse the durable draft and do
not repeat inference. Intake is capped at 32 messages and 128 KiB of text per
conversation; a request that would exceed either limit is rejected before its
message is appended.

The dashboard presents the complete Intent for review. **Confirm exact Intent** binds the
current Linux user to the exact Intent version and SHA-256 fingerprint. Only
then may Agent OS create the Work and executable Task state. The confirmed
Intent is fingerprint-bound to a runtime-validated Plan. Exact deterministic
work skips planning inference; adaptive work may use the configured provider
to propose the smallest useful Task DAG. The complete graph is committed
atomically before scheduling, and model output cannot introduce authority or
new execution mechanisms. An unfinished
conversation is recovered from SQLite when the dashboard restarts. Confirmation
means Agent OS understood the requested work; it is never approval for a
financial, public, destructive, privileged, legal, deployment, or other
consequential effect.

An execution kind explicitly selected by the submitting user or Agent remains
bound to the intake conversation. When no kind was explicitly selected, Agent
OS reruns deterministic-first routing after every clarification against the
latest normalized objective; an inferred route from an earlier incomplete
draft is never treated as an operator choice.

Selecting **User task** before intake creates work that must wait for structured
user completion. The Task view collects every required field and file from the
Task's durable CompletionContract. A self-reported "done" message cannot
complete that Task. Only a user-operated Task advertises ordinary continuation.
If structured evidence or ordinary requested input was durably accepted but a
later continuation response was interrupted, the Task view exposes a
server-owned recovery signal in every unfinished state. The dashboard replays
only missing runtime phases from the exact durable event and does not solicit a
second message identity, replacement text, or evidence re-upload.

The Reviews view lists pending completion judgments, including internal planned
Tasks that are intentionally absent from A2A lookup. Its bounded recent history
includes those locally reviewable child Tasks so a lost terminal response can
be recovered without making the child addressable through A2A. The dashboard binds a
decision to the exact evidence fingerprint and states explicitly that judging
candidate completion does not approve a consequential effect. It also shows a
bounded ledger-derived history of recent terminal judgments so a fresh
ephemeral browser origin can recover the authoritative decision.

The local HTTP-shaped routes are private implementation boundaries carried over
the Unix socket:

- `POST /v1/user/messages`
- `GET /v1/user/intents/active`
- `POST /v1/user/intents/{conversation-id}/confirm`
- `GET /v1/user/tasks/recent`
- `GET /v1/user/tasks/{task-id}`
- `POST /v1/user/tasks/{task-id}/completion`
- `POST /v1/user/tasks/{task-id}/completion/recover`
- `POST /v1/user/tasks/{task-id}/input/recover`
- `GET /v1/user/reviews?after={task-id}&limit={1..100}`
- `GET /v1/user/reviews/recent?limit=20`
- `GET|POST /v1/user/reviews/{task-id}`
- `GET /v1/user/reviews/{task-id}/records/{review-id}`
- `GET /v1/control/approvals`
- `GET /v1/control/approvals/recent?limit=20`
- exact-effect approval operations beneath `/v1/control/approvals/{id}`

The recent-Task read is bound to the authenticated principal's latest confirmed
intake and exists only to recover an interrupted confirmation across dashboard
processes. Exact approval and completion-review reads plus their bounded
recent-decision projections expose terminal ledger state across the launcher's
changing loopback origin. Recent review decisions are selected by a bounded,
tenant-scoped SQLite query before their exact Task streams are validated. A
matching completion-review retry is replayed until
its downstream Task transition succeeds; a conflicting authorized decision is
shown as authoritative and releases the stale local retry.

The completion endpoint accepts strict JSON. Required files are size-bounded,
content-sniffed, stored privately by SHA-256, and recorded as untrusted user
evidence. Unknown fields, missing requirements, duplicate evidence, spoofed
media types, and mismatched origin fail closed.

## A2A Agent access

A2A is optional and disabled when no reviewed Agent registry is configured.
An enabled Agent uses the official A2A v1.0 JSON-RPC boundary and a unique
bearer credential supplied to the service as a systemd credential. The Agent
must support A2A; protocol compatibility alone grants no Agent OS authority.

The reviewed registry defines each Agent's stable ID, organization, role, work
scope, lifecycle, rate limit, concurrency limit, and credential reference.
There is no self-registration or trust-on-first-use path. The closed V1 roles
are `SUBMITTER`, `COLLABORATOR`, `OBSERVER`, `RESULT_READER`, and `OPERATOR`.
`OWN` scope is the default; `ORGANIZATION` scope deliberately allows access to
other principals' work within the same organization.

See [A2A interoperability](A2A_INTEROP.md) for the wire contract and Agent Card.
Remote A2A binding must be explicitly enabled and requires TLS plus an HTTPS
public URL. The local user socket remains separate.

## Routing and authority

Routing uses registered deterministic handlers first. Adaptive inference is
used separately for bounded Intent normalization, Task-DAG planning when it
adds value, and Agent execution. Planning records its exact input events and
model identity, reuses a valid durable Plan on retry, and keeps internal child
Tasks outside the A2A lookup boundary. Startup rebuilds the external task index
from each work stream's runtime-owned root identity and removes stale child
bindings left by older migrations. The
accepted structured Intent—not the raw conversation alone—is supplied to the
execution boundary. Intake text cannot select a provider, grant a capability,
decide an approval, or alter a CompletionContract.

Natural-language claims such as "I approve" or "the task is complete" remain
untrusted content. Approval requires the separate exact-effect operation, and
completion requires the evidence declared by the durable contract. Missing,
expired, stale, or mismatched authority fails closed.
