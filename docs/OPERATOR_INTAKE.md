# Operator intake

Agent OS V1 accepts natural-language work through two first-class adapters:

```text
Human -> Human Gateway ----+
                            +-> Intake Service -> Intent -> Goal -> Task DAG
Agent --> A2A Gateway -----+
```

The adapters authenticate different principal kinds and translate their wire
formats into one `intake.Message`. A principal carries an ID, kind,
organization, source channel, and explicit capabilities. The resulting Intent
durably records that provenance, and the runtime appends a channel-specific
`HUMAN_WORK_ACCEPTED` or `A2A_WORK_ACCEPTED` Event Contract before execution.
Human and external-agent credentials are
distinct, and neither identity receives implicit administrative authority.

## Routing

Routing follows the minimal-justified-LLM rule:

- work matching the registered `echo` handler is `DETERMINISTIC`;
- other unstructured natural-language work is `AGENT`, because interpretation
  is intrinsic and no deterministic handler is registered;
- a bounded `DETERMINISTIC`, `AGENT`, or `HUMAN` hint may be requested by the
  adapter for conformance and engineering use;
- unavailable `TOOL`, `TEAM`, and `MIXED` routes fail closed until their real
  runtime mechanisms exist.

The router is deterministic and never calls a model merely to choose an
execution kind. The current Agent route uses the fake model adapter so V1
behavior remains testable without a provider credential.

## Direct human API

`POST /v1/human/messages` accepts:

```json
{
  "conversation_id": "conversation-1",
  "message_id": "message-1",
  "text": "draft a concise release update"
}
```

`GET /v1/human/tasks/{task-id}` returns the narrow authorized Task view. The
same message endpoint continues blocked `HUMAN` work when a new message uses the
same conversation ID. Message IDs are persisted for retry idempotency.

The authenticated human and external Agent may observe or continue work only
when their explicit `read_status`, `read_result`, or `provide_input`
capabilities and work-visibility scope allow it. `OWN` is the default external
Agent scope; cross-principal organization access requires the deliberate
`ORGANIZATION` scope. Each input event retains the actual principal and channel
provenance.

An external Agent must implement the A2A v1.0 client capabilities described in
[A2A Agent interoperability profile](A2A_INTEROP.md) before it can use the A2A
Gateway. Protocol support does not grant Agent OS authority: the authenticated
principal still needs each configured Agent OS capability for the requested
operation.

External Agent identities come only from the trusted startup registry. The
gateway ignores content-supplied identity, organization, role, and capability
claims. A2A has no self-registration or trust-on-first-use path.

## Authority boundary

Both adapters reject authority-shaped structured fields before persistence.
Natural-language statements such as “I approve” remain ordinary work or input
content. They never emit `APPROVAL_DECIDED`, grant capability, alter policy, or
execute a consequential effect. A future human approval UI may coexist with the
conversation UI, but it must call the separate exact-effect approval service
through an authenticated trusted control—not reinterpret chat text.
