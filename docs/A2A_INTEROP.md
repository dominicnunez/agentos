# A2A Agent interoperability profile

The Agent OS Operator Gateway is vendor-neutral. An external Agent must have
A2A v1.0 client capabilities to use it: Agent Card discovery, JSON-RPC 2.0,
the A2A `SendMessage` and `GetTask` methods, and support for A2A `Message`,
`Task`, `TaskStatus`, and `Artifact` wire shapes. The Agent must also present
its configured bearer credential and receives only the Agent OS capabilities
granted to that authenticated principal.

The gateway exposes only the A2A v1.0 surface:

- public discovery at `/.well-known/agent-card.json`;
- one advertised `JSONRPC` interface with protocol version `1.0`;
- authenticated JSON-RPC 2.0 `SendMessage` and `GetTask` methods at the
  advertised interface URL;
- `contextId` as the durable Agent OS work correlation and `messageId` as the
  continuation-delivery idempotency key.

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
authority-field rejection.
