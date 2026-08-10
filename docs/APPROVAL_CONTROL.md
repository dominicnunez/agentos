# Exact-effect approval control

Agent OS exposes approval decisions only through a dedicated control listener.
The listener is disabled unless `AGENTOS_APPROVAL_ACTORS_FILE` names a reviewed
registry. It defaults to `127.0.0.1:8082` and is never mounted on the Human or
A2A work listener.

An approval principal is a distinct security identity even when the same person
also operates a Human Gateway account. Startup fails if an actor ID or resolved
bearer credential is reused across the Human, A2A, or approval registries.
Operator, reviewer, and Agent credentials cannot authenticate to this control.

## Registry

The registry is a trusted deployment input. Grants are exact
`organization_id` + `boundary` + `risk` matches; there is no wildcard or
inheritance. Risk and urgency use the closed values `LOW`, `MEDIUM`, `HIGH`,
and `CRITICAL`.

```json
{
  "actors": [{
    "id": "release-approver",
    "organization_id": "org-default",
    "status": "ACTIVE",
    "token_ref": "AGENTOS_RELEASE_APPROVER_TOKEN",
    "review_ref": "security-review-approval-1",
    "expires_at": "2027-01-01T00:00:00Z",
    "max_concurrent": 1,
    "requests_per_minute": 30,
    "grants": [{
      "boundary": "AGENT_OS_DEPLOYMENT",
      "risk": "HIGH"
    }]
  }]
}
```

`token_ref` names a server-owned secret resolved at startup. Registry files do
not contain bearer credentials. Unknown fields, unknown grants, weak or expired
credentials, duplicate identities, and duplicate grants fail closed.

## Protocol

The control uses strict, size-limited JSON and four non-language operations:

- `GET /v1/control/approvals/{approval_id}`
- `POST /v1/control/approvals/{approval_id}/acknowledge`
- `POST /v1/control/approvals/{approval_id}/begin`
- `POST /v1/control/approvals/{approval_id}/decision`

Inspection returns the trusted ledger values for the exact approval and its
current `PENDING` effect, including action, resource, scope, canonical
descriptor, complete replay arguments, consequence boundary, risk, urgency,
fingerprint, expiry, and single-use status. Request bodies cannot supply or
override those authority-bearing values.

Acknowledgement and decision-start bodies contain only the exact fingerprint:

```json
{"effect_fingerprint":"<64-character lowercase SHA-256 hex>"}
```

The final decision adds one closed enum:

```json
{"effect_fingerprint":"<64-character lowercase SHA-256 hex>","decision":"APPROVE"}
```

The only other decision is `DENY`. Before every state transition, Agent OS
reloads the approval and prepared effect from the ledger, checks the exact
organization/boundary/risk grant and credential lifecycle, and compares the
fingerprint. Unknown approvals, cross-organization access, grant mismatches,
changed or non-pending effects, expired approvals, and stale fingerprints do
not authorize a transition.

The canonical fingerprint covers the obligation identity, organization, task,
actor, action, resource, scope, consequence boundary, descriptor,
authorization references, approval reference, idempotency key, and complete
replay arguments. Runtime status, attempt counters, timestamps, and evidence
are excluded because they advance while the same immutable effect is executed.

## Listener configuration

The control listener uses independent settings:

- `AGENTOS_CONTROL_LISTEN_ADDR` (default `127.0.0.1:8082`)
- `AGENTOS_CONTROL_ALLOW_REMOTE` (default `false`)
- `AGENTOS_CONTROL_TLS_CERT_FILE`
- `AGENTOS_CONTROL_TLS_KEY_FILE`

Remote binding requires the explicit remote switch and TLS 1.3 certificate/key
pair. Setting any control-listener option without an approval registry fails
startup. Keep the listener on a loopback or separately protected management
network. No consequential effect-writing adapter is enabled by this control.
