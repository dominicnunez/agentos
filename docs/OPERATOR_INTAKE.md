# User and Agent intake

Agent OS accepts work through two boundaries that converge on the same durable
conversational intake and work model:

```text
User -> private Unix socket --+
                              +-> Intake -> confirmed Intent -> Goal -> Task DAG
Agent -> A2A JSON-RPC -------+
```

Content never supplies its own identity, capabilities, organization, approval,
or policy. The authenticated boundary injects those values before work enters
the event ledger.

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

## Organization console

After setup, `agentos` opens the terminal organization console. It uses the
private user gateway; it never reads or edits the SQLite ledger directly.

The initial views are:

- Work: submit natural-language work and inspect its narrow Task view.
- Approvals: inspect the exact prepared effect and approve or deny it.
- Agents: view the authorized Agent roster as that capability is added.
- System: direct the user to the read-only `agentos doctor` report.

Natural-language input first enters a durable, resumable intake conversation.
The configured model may propose a structured Intent, but its output is
untrusted and must pass strict schema, provenance, size, completeness, and
consequence-candidate validation. Information only the user can supply keeps
the Intent in `AWAITING_USER_INPUT`; facts Agent OS can discover belong in the
later plan. A reviewable Intent has a clear objective, deliverables, testable
completion criteria, and no missing user inputs.

Every model-backed normalization records its prompt-contract version, provider,
model, execution profile, exact input event references, and provider-reported
token usage in the ledger. Exact message retries reuse the durable draft and do
not repeat inference. Intake is capped at 32 messages and 128 KiB of text per
conversation; a request that would exceed either limit is rejected before its
message is appended.

The console presents the complete Intent for review. `/confirm` binds the
current Linux user to the exact Intent version and SHA-256 fingerprint. Only
then may Agent OS create the Goal and executable Task state. The confirmed
Intent is fingerprint-bound to a runtime-validated Plan. Exact deterministic
work skips planning inference; adaptive work may use the configured provider
to propose the smallest useful Task DAG. The complete graph is committed
atomically before scheduling, and model output cannot introduce authority or
new execution mechanisms. An unfinished
conversation is recovered from SQLite when the console restarts. Confirmation
means Agent OS understood the requested work; it is never approval for a
financial, public, destructive, privileged, legal, deployment, or other
consequential effect.

An execution kind explicitly selected by the submitting user or Agent remains
bound to the intake conversation. When no kind was explicitly selected, Agent
OS reruns deterministic-first routing after every clarification against the
latest normalized objective; an inferred route from an earlier incomplete
draft is never treated as an operator choice.

`/user-task <instruction>` creates work that must wait for structured user
completion. `/complete <task-id>` collects every required field and file from
the Task's durable CompletionContract. A self-reported "done" message cannot
complete that Task.

`/reviews` lists pending completion judgments, including internal planned
Tasks that are intentionally absent from A2A lookup. The console binds a
decision to the exact evidence fingerprint and states explicitly that judging
candidate completion does not approve a consequential effect.

The local HTTP-shaped routes are private implementation boundaries carried over
the Unix socket:

- `POST /v1/user/messages`
- `GET /v1/user/intents/active`
- `POST /v1/user/intents/{conversation-id}/confirm`
- `GET /v1/user/tasks/{task-id}`
- `POST /v1/user/tasks/{task-id}/completion`
- `GET /v1/user/reviews?after={task-id}&limit={1..100}`
- `GET|POST /v1/user/reviews/{task-id}`
- `GET /v1/control/approvals`
- exact-effect approval operations beneath `/v1/control/approvals/{id}`

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
