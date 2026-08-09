# A2A v1.0 Operator Gateway — Implementation Notes

## Status

V1 CORE.

Use the **official A2A v1.0 specification** for wire behavior:

`https://a2a-protocol.org/latest/specification/`

Do not invent an Agent-OS-specific protocol and label it A2A.

## Boundary rule

A2A is implemented only in the external adapter:

```text
api/a2a / internal/operator/a2a
        |
        v
application commands / internal DTOs
        |
        v
Intent / Goal / Task / Event domain
```

Core domain packages do not import A2A wire types.

## Minimum V1 mapping

| External A2A concept | Agent OS mapping |
|---|---|
| Agent Card | Agent OS operator/service capability description |
| authenticated peer | `ExternalActor` |
| Message / work submission | create/continue `IntentEnvelope` + internal work |
| A2A Task/context ID | `A2ATaskMapping` |
| task progress/status | projection from internal Goal/Task state |
| input-needed / continuation | internal blocked/missing-input state + authorized new input |
| Artifact | authorized Agent OS Artifact/result |
| completed external task | aggregate internal verified completion/result |

## Authority

An A2A message is input, not authority.

Before accepting work or continuation:

1. authenticate/map peer;
2. load `ExternalActor`;
3. check its scoped capability;
4. apply organization policy;
5. persist internal intent/input/event;
6. execute normal work path.

Human-required approvals remain human-required.

## Interoperability test target

Pin a known Hermes release/configuration during development/release testing.

Test both:

- protocol-level fake/conformance peer in ordinary CI;
- real Hermes integration in an integration/release profile.

Do not make unit tests depend on a live external service.
