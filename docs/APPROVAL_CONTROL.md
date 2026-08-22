# Exact-effect approval control

Agent OS exposes approval work through the private user gateway. Its
authoritative boundary is the owner-only Unix socket, not a bearer registry or
actor file. Linux proves the connecting dashboard process's UID before the
request reaches the approval service. The owner-launched web dashboard uses
only its ephemeral, session-authenticated loopback bridge and cannot bypass
that Unix boundary.

For V1, the verified installation owner may decide approval requests belonging
to the configured organization. That local identity grant does not weaken any
other check: every transition reloads the durable approval and prepared effect,
checks its state and expiry, and compares the exact effect fingerprint.

## Dashboard flow

The local dashboard lists pending work through:

```text
GET /v1/control/approvals
```

`GET /v1/control/approvals/recent?limit=20` returns a bounded newest-first,
read-only ledger projection of terminal decisions in authoritative commit
sequence for the same authorized
owner. This keeps interrupted outcomes visible when a new dashboard launch has
a different ephemeral browser origin; it never makes a terminal approval
eligible for mutation.

It then displays the trusted action, resource, scope, consequence boundary,
risk, urgency, canonical descriptor, complete replay arguments, fingerprint,
expiry, and single-use status. These values come from the ledger and cannot be
supplied or replaced by the interface.

Approval requires typing `APPROVE <fingerprint-prefix>` after viewing the exact
effect. Denial requires `DENY`. The dashboard performs the durable lifecycle:

```text
PENDING -> NOTIFIED -> ACKNOWLEDGED -> PENDING_DECISION -> APPROVED | DENIED
```

The underlying strict JSON operations are:

- `GET /v1/control/approvals/{approval-id}`
- `POST /v1/control/approvals/{approval-id}/acknowledge`
- `POST /v1/control/approvals/{approval-id}/begin`
- `POST /v1/control/approvals/{approval-id}/decision`

Acknowledgement and decision-start contain only the current fingerprint:

```json
{"effect_fingerprint":"<64-character lowercase SHA-256 hex>"}
```

The final operation adds `APPROVE` or `DENY`:

```json
{"effect_fingerprint":"<64-character lowercase SHA-256 hex>","decision":"APPROVE"}
```

Unknown approvals, another organization, changed or non-pending effects,
expired approvals, stale fingerprints, and requests from any UID other than the
configured owner do not authorize a transition. Conversation text and A2A
messages never call this control.

An exact read of an expired nonterminal approval returns `410 Gone`. That
server-proven result lets the dashboard discard only the matching local retry
binding; expiry never becomes denial, approval, or permission to replace the
effect.

The fingerprint covers the obligation identity, organization, task, actor,
action, resource, scope, consequence boundary, descriptor, authorization
references, approval reference, idempotency key, and complete replay arguments.
Runtime status, counters, timestamps, and evidence are excluded because they
advance while that immutable effect is processed.
