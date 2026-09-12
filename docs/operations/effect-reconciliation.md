# Effect reconciliation

Agent OS reconciles crash-ambiguous `ATTEMPTED` effects through a separate,
read-only status boundary. Reconciliation never calls the adapter that performs
the effect and has no resend operation.

The boundary is disabled unless `AGENTOS_EFFECT_RECONCILERS_FILE` names a
reviewed registry. Each binding is exact for one organization, action, and
resource:

```json
{
  "reconcilers": [{
    "organization_id": "org-default",
    "action": "send",
    "resource": "customer-system",
    "status": "ACTIVE",
    "status_url": "https://status.example/effects",
    "token_ref": "AGENTOS_EFFECT_STATUS_TOKEN",
    "review_ref": "reviewed-reconciler-1",
    "expires_at": "2027-01-01T00:00:00Z"
  }]
}
```

The registry contains no raw credential. Startup resolves `token_ref` through
the SecretSource. Bindings may be `ACTIVE`, `SUSPENDED`, or `REVOKED`; an
inactive, expired, missing, or non-exact binding leaves the obligation
`ATTEMPTED`.

## HTTPS status contract

Agent OS sends one authenticated `GET` request to the configured exact HTTPS
URL. Redirects are not followed. The request carries:

- `X-AgentOS-Effect-Obligation-ID`;
- `X-AgentOS-Idempotency-Key`; and
- `X-AgentOS-Effect-Fingerprint`.

The endpoint must be observational: receiving the request must not create,
retry, or modify the effect. A successful response is a bounded
`application/json` object that echoes all three identifiers:

```json
{
  "effect_obligation_id": "effect-1",
  "idempotency_key": "delivery-1",
  "effect_fingerprint": "sha256-value",
  "state": "CONFIRMED",
  "evidence_refs": ["destination-receipt-1"]
}
```

`state` may be `CONFIRMED`, `FAILED`, or `UNKNOWN`. A terminal state is durable
only when the echoed identifiers match exactly, evidence is non-empty, and the
authoritative attempt has not changed while the status check was in flight.
HTTP errors, redirects, oversized or malformed responses, unknown fields,
identity mismatch, missing evidence, and `UNKNOWN` preserve explicit
uncertainty.

This status adapter does not enable a production effect-writing adapter.
