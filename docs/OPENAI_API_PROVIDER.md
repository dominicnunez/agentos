# OpenAI API provider

Agent OS can use the official OpenAI
[Responses API](https://developers.openai.com/api/reference/responses/overview)
for model-only `AgentExecution`. This is a separate provider from the Codex
subscription adapter. An OpenAI API key is billable Platform access; a ChatGPT
or Codex subscription is not an API credential.

## Security profile

The adapter is disabled unless deployment configuration selects it. When
selected, it:

- sends requests only to `https://api.openai.com/v1/responses`; there is no
  endpoint override or OpenAI-compatible mode;
- resolves the API key through the server-owned secret source at startup and
  again for each request, without storing it in model configuration;
- disables ambient HTTP proxies, requires HTTPS with TLS 1.2 or newer, rejects
  redirects, and uses a 20-second request deadline;
- sends `tool_choice: "none"`, an empty tool list, `store: false`,
  `truncation: "disabled"`, and a 4,096-token output ceiling;
- accepts only completed assistant text plus non-authority-bearing reasoning
  metadata, rejecting tool calls, citations, refusals, incomplete responses,
  unexpected model identity, and changed execution-profile fields;
- bounds prompts to 256 KiB, returned text to 256 KiB, and the full HTTP
  response to 1 MiB;
- records provider-reported input, output, and total tokens while leaving cost
  unknown; and
- does not automatically retry. A timeout can represent a request that reached
  the billable provider, so retry is an explicit workflow decision.

Provider output remains untrusted work content. It cannot grant authority,
create an approval, invoke an Agent OS tool, or certify its own completion.
Provider error bodies are not copied into task errors. Agent OS retains
client/provider request identifiers for troubleshooting without logging the API
key or prompt in the error.

The runtime durably records a successful OpenAI response as a result and
completion candidate, then leaves the task `BLOCKED` when no independent
verifier exists for its CompletionContract. It emits
`COMPLETION_REVIEW_REQUIRED` instead of converting nonempty model text into
deterministic proof. The output can become verified completion only through an
approved independent or human judgment path; that continuation is not yet part
of this V1 provider slice.

## Configuration

Select the provider with three exact settings:

- `AGENTOS_MODEL_PROVIDER=openai-api`
- `AGENTOS_OPENAI_API_KEY_REF` set to the name of a server-owned environment
  secret, normally `OPENAI_API_KEY`
- `AGENTOS_OPENAI_MODEL` set to an exact pinned model snapshot

For example:

```powershell
$env:AGENTOS_MODEL_PROVIDER = "openai-api"
$env:AGENTOS_OPENAI_API_KEY_REF = "OPENAI_API_KEY"
$env:OPENAI_API_KEY = "<project-scoped key>"
$env:AGENTOS_OPENAI_MODEL = "<reviewed model snapshot>"
```

Use a project-scoped service-account key with only the required model access;
do not use an organization admin key. OpenAI recommends server-side key loading
and pinned model versions for stable behavior in its
[API overview](https://developers.openai.com/api/reference/overview).

## Approval and data controls

Adding the disabled adapter does not send data or incur provider charges.
Enabling it for a deployment requires the established approvals for:

- the financial boundary, including an OpenAI project spend limit and alerts;
  and
- every sensitive-data class that will cross from Agent OS to OpenAI.

An unanswered decision fails closed: keep the fake or confined subscription
provider selected. `store: false` prevents application-state storage of the
Response object, but it is not a promise of zero provider retention. Review the
current OpenAI
[data controls](https://platform.openai.com/docs/models/default-usage-policies-by-endpoint)
for the organization before approving a data boundary. Never place secrets,
credentials, or unapproved sensitive data in an execution prompt.

Pricing is intentionally not embedded in the binary because it changes outside
the Agent OS release cycle. Provider token usage is durable; monetary controls
remain with the reviewed OpenAI project until Agent OS has a separately
approved, current pricing source and budget contract.
