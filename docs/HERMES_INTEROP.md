# Hermes interoperability profile

Agent OS V1 pins its tested Hermes client boundary to the Hermes v0.20.0
release at immutable commit
`3c27eb6234bf91b8ceee9e9071591b31e9b148cb` (release tag `v2026.8.3`).
The pin makes protocol drift explicit and keeps CI reproducible.

The Operator Gateway exposes only the A2A v1.0 surface:

- public discovery at `/.well-known/agent-card.json`;
- one advertised `JSONRPC` interface with protocol version `1.0`;
- authenticated JSON-RPC 2.0 `SendMessage` and `GetTask` methods at the
  advertised interface URL;
- A2A v1.0 `Message`, `Task`, `TaskStatus`, and `Artifact` wire shapes;
- `contextId` as the durable Agent OS work correlation and `messageId` as the
  continuation-delivery idempotency key.

The gateway does not serve the pre-1.0 discovery alias, legacy method names,
or custom REST task endpoints. A Hermes upgrade requires updating the immutable
pin, reviewing protocol and security changes, and passing the real-client
interoperability job before merge.

The pinned CI fixture imports Hermes's released A2A plugin code, runs its public
`a2a_discover` and `a2a_call` handlers against a live Agent OS process, and
asserts JSON-RPC v1.0 discovery plus a completed result. Go conformance tests
cover blocked-input continuation, status/result capability separation,
organization isolation, replay idempotency, and authority-field rejection.
