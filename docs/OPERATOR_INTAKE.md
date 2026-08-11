# Operator intake

Agent OS V1 accepts natural-language work through two first-class adapters:

```text
Human -> Human Gateway ----+
                            +-> Intake Service -> Intent -> Goal -> Task DAG
Agent --> A2A Gateway -----+
```

The adapters authenticate different principal kinds and translate their wire
formats into one `intake.Message`. A principal carries an ID, kind,
organization, source channel, explicit capabilities, and work scope. The
resulting Intent durably records that provenance. The runtime appends a
channel-specific `HUMAN_WORK_ACCEPTED` or `A2A_WORK_ACCEPTED` Event Contract
before execution.

## Startup configuration

Agent OS refuses to start unless at least one authenticated operator gateway is
configured. Set `AGENTOS_HUMAN_ACTORS_FILE` for direct Human access,
`AGENTOS_A2A_ACTORS_FILE` for A2A-compatible Agent access, or both. Each file is
a reviewed local registry with this structure:

```json
{
  "actors": [
    {
      "id": "operator-1",
      "organization_id": "organization-1",
      "status": "ACTIVE",
      "role": "OPERATOR",
      "work_scope": "ORGANIZATION",
      "token_ref": "AGENTOS_OPERATOR_TOKEN",
      "review_ref": "deployment-review-1",
      "expires_at": "2030-12-31T23:59:59Z",
      "max_concurrent": 4,
      "requests_per_minute": 60
    }
  ]
}
```

This example is a Human registry. Before use, replace every example identity,
review reference, limit, and expiry with values approved for the deployment.
Set the environment variable named by `token_ref` to a unique bearer credential
of at least 32 characters; the credential itself never belongs in the registry
or source control. For this example, set `AGENTOS_HUMAN_ACTORS_FILE` to the
registry path and set `AGENTOS_OPERATOR_TOKEN` in the runtime environment before
running `make run` or the packaged `agentos` executable.

An A2A registry uses the same lifecycle, rate, credential-reference, and review
fields, but its roles and work scope follow the exact schema in
[A2A interoperability](A2A_INTEROP.md). Human and A2A credentials must be
distinct. Keep the default loopback listener unless remote access is required.
For a non-loopback listener, set `AGENTOS_LISTEN_ADDR` to the intended
`host:port`, set `AGENTOS_ALLOW_REMOTE=true`, and set both
`AGENTOS_TLS_CERT_FILE` and `AGENTOS_TLS_KEY_FILE`. When A2A is enabled, also
set `AGENTOS_PUBLIC_URL` to the externally reachable HTTPS origin. Agent OS
rejects incomplete TLS settings, remote plaintext, and remote A2A exposure
without that HTTPS public URL.

## Routing

Routing follows the minimal-justified-LLM rule:

- registered `echo` work is `DETERMINISTIC`;
- other unstructured natural-language work is `AGENT`;
- a bounded `DETERMINISTIC`, `AGENT`, or `HUMAN` hint may be requested;
- unavailable `TOOL`, `TEAM`, and `MIXED` routes fail closed.

The router is deterministic. The Agent route uses the fake model adapter by
default so V1 remains testable without a provider credential. A deployment may
explicitly select the confined Codex subscription adapter described in
[Codex subscription provider](CODEX_SUBSCRIPTION_PROVIDER.md) or the official
[OpenAI API provider](OPENAI_API_PROVIDER.md). Intake text does not select or
reconfigure providers.

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
same message endpoint continues blocked `HUMAN` work when a new message uses
the same conversation ID. Message IDs are persisted for retry idempotency.

With the example Human registry and the default loopback listener running, this
request verifies authenticated intake without placing the credential in the
command or source control:

```sh
printf 'Authorization: Bearer %s\n' "$AGENTOS_OPERATOR_TOKEN" | \
curl --fail --silent --show-error \
  --request POST http://127.0.0.1:8080/v1/human/messages \
  --header 'Content-Type: application/json' \
  --header @- \
  --data '{"conversation_id":"setup-verification","message_id":"message-1","text":"echo ready"}'
```

The response is HTTP 200 with a generated task ID and timestamp, plus these
stable values:

```json
{
  "task_id": "<generated-task-id>",
  "conversation_id": "setup-verification",
  "state": "COMPLETED",
  "result": "ready",
  "updated_at": "<RFC3339 timestamp>"
}
```

Human identities come only from the reviewed `AGENTOS_HUMAN_ACTORS_FILE`
registry. Roles, expiry, concurrency, and request-rate limits are enforced
before intake. `review_ref` records deployment review of a trusted bootstrap
entry; it is not a runtime approval or capability grant. Human and Agent
registries cannot reuse a credential.

Model completion judgment uses a separate Human control documented in
[Completion review](COMPLETION_REVIEW.md). Only the dedicated `REVIEWER` role
can access `/v1/human/reviews/{task-id}`. `OPERATOR` and all external-Agent
roles remain work-plane identities and cannot finalize a model candidate.

## A2A intake

An external Agent must implement the A2A v1.0 client capabilities in
[A2A interoperability](A2A_INTEROP.md). Protocol support does not grant Agent
OS authority: the authenticated principal still needs every configured
capability for the operation.

External Agent identities come only from `AGENTOS_A2A_ACTORS_FILE`. The gateway
ignores content-supplied identity, organization, role, and capability claims.
There is no self-registration or trust-on-first-use path. Actor and
organization identifiers are validated when either reviewed registry is loaded,
so an unusable identity fails startup instead of authenticating into a gateway
that will reject every request.

Both work channels enforce `read_status`, `read_result`, `provide_input`, and
`submit_work` independently. `OWN` is the default Agent scope; deliberate
`ORGANIZATION` scope permits cross-principal access within one organization.
Internal work keys and task IDs are tenant-scoped and do not expose the
caller-supplied conversation ID.

For A2A, an initial message may omit `contextId`; Agent OS returns a durable
generated context. Continuing blocked work requires the returned `taskId`, and
any supplied `contextId` must match that durable task. The execution-kind
extension is only a routing hint and never modifies the authenticated
principal or its capabilities.

## Authority boundary

Both adapters reject authority-shaped structured fields before persistence.
Natural-language statements such as “I approve” remain ordinary content. They
never emit `APPROVAL_DECIDED`, grant capability, alter policy, or execute a
consequential effect. Any approval UI must call the separate exact-effect
approval service through an authenticated trusted control—not reinterpret chat
text.

Completion review follows that same separation: reviewer decisions bind exact
ledger evidence and are never inferred from chat. Human review is recorded as
`HUMAN_JUDGMENT`; it never relabels a ToolOutcome as deterministically verified.
