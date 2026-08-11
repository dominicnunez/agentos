# A2A Agent interoperability profile

The Agent OS Operator Gateway is vendor-neutral. An external Agent must have
A2A v1.0 client capabilities to use it: Agent Card discovery, JSON-RPC 2.0,
the A2A `SendMessage` and `GetTask` methods, and support for A2A `Message`,
`Task`, `TaskStatus`, and `Artifact` wire shapes. The Agent must also present
its configured bearer credential and receives only the Agent OS capabilities
granted to that authenticated principal.

## Authorized external Agents

A2A is disabled unless `AGENTOS_A2A_ACTORS_FILE` names a valid, reviewed actor
registry. Each actor entry binds a unique ID and organization to a role, work
visibility scope, credential secret reference, deployment review reference,
expiration, concurrency ceiling, and per-minute request ceiling. The registry
contains no raw credential. At startup, the adapter resolves each secret,
validates that credentials and actor IDs are unique, and retains only a
credential digest.

The deterministic role profiles are:

| Role | Capabilities |
| --- | --- |
| `SUBMITTER` | `submit_work` |
| `COLLABORATOR` | `submit_work`, `read_status`, `provide_input` |
| `OBSERVER` | `read_status` |
| `RESULT_READER` | `read_status`, `read_result` |
| `OPERATOR` | all four work-plane capabilities |

These roles never include approval, capability administration, policy, freeze,
security administration, or protected-effect authority. A role is selected by
trusted configuration; an A2A message cannot claim or alter it.

`OWN` scope restricts status, results, and continuation to work initially
submitted by the same registered actor. `ORGANIZATION` scope deliberately
allows the granted read/input capabilities across that actor's organization.
Use `OWN` unless cross-operator collaboration is an explicit human policy
decision. Unknown, suspended, revoked, expired, over-rate, and over-concurrency
actors fail closed before their content reaches intake.

The gateway exposes only the A2A v1.0 surface:

- public discovery at `/.well-known/agent-card.json`;
- one advertised `JSONRPC` interface with protocol version `1.0`;
- authenticated JSON-RPC 2.0 `SendMessage` and `GetTask` methods at the
  advertised interface URL;
- an optional Apache-2.0 execution-kind extension identified by
  `https://github.com/dominicnunez/agentos-a2a-go/blob/main/spec/execution-kind-v1.md`;
- server-generated `contextId` when an initial message omits it, with a stable
  value derived from the authenticated organization, actor, and `messageId` so
  the same initial delivery remains idempotent across restart;
- a `SUBMITTER` receives a stable acceptance receipt containing only its task
  and context identifiers. An exact retry by that same authenticated principal
  returns the receipt without granting `read_status` or exposing later state;
- continuation only by durable `taskId`. A supplied continuation `contextId`
  must match the task; a different message in an existing context without
  `taskId` is rejected.

The A2A adapter translates into the same principal-aware Intake Service used by
the direct Human Gateway. This does not make the first-party human API part of
A2A and does not weaken A2A protocol isolation.

Execution-kind metadata is an untrusted routing hint. It grants no identity,
authority, approval, capability, completion status, or effect permission. The
legacy `agentos.execution_kind` metadata key is not accepted.

The gateway uses the official `a2aproject/a2a-go/v2` v2.4.0 message, task,
Agent Card, JSON-RPC client, and JSON-RPC server contracts. Agent OS retains a
boundary wrapper for authentication, actor lookup, organization and scope
enforcement, limits, exact media type and body bounds, strict supported-input
decoding, authority-field rejection, and private principal injection. SQLite
and Agent OS Event Contracts remain authoritative; the SDK task store is not
used.

The gateway does not serve the pre-1.0 discovery alias, legacy method names,
custom REST task endpoints, list, cancel, streaming, push, subscription, or
extended-card capabilities. Unsupported official methods return standard A2A
errors and cannot create ledger events. Protocol changes require review of the
wire and security boundaries plus a passing interoperability job before merge.

CI uses the official A2A Go client for Agent Card discovery, authenticated
submission, generated contexts, `GetTask`, deterministic and adaptive work,
blocked-task continuation, restart, and unsupported-method checks. A separate
dependency-free live-process fixture keeps the executable boundary test small.
Go conformance tests also cover status/result capability separation,
organization isolation, replay conflicts, and authority-field rejection.
Adversarial coverage checks weak or duplicate credentials, unknown
roles/config fields, actor expiry/revocation, request limits, and own-work
isolation.
