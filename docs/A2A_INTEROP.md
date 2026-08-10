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
- `contextId` as the caller-visible conversation key and `messageId` as the
  continuation-delivery idempotency key. Agent OS derives an opaque,
  tenant-scoped internal work key and task ID from the authenticated
  organization plus `contextId`.

The A2A adapter translates into the same principal-aware Intake Service used by
the direct Human Gateway. This does not make the first-party human API part of
A2A and does not weaken A2A protocol isolation.

The gateway does not serve the pre-1.0 discovery alias, legacy method names, or
custom REST task endpoints. Protocol changes require review of the wire and
security boundaries plus a passing interoperability job before merge.

CI uses a dependency-free A2A v1.0 client fixture against a live Agent OS
process. It verifies discovery plus deterministic and adaptive natural-language
work. Go conformance tests cover blocked-input continuation, status/result
capability separation, organization isolation, replay idempotency, and
authority-field rejection. Adversarial coverage also checks weak or duplicate
credentials, unknown roles/config fields, actor expiry/revocation, request
limits, and own-work isolation.
