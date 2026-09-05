# Codex subscription provider

[`dominicnunez/codex-sdk-go`](https://github.com/dominicnunez/codex-sdk-go)
is the selected candidate for Agent OS subscription-backed inference. It uses
the authenticated Codex CLI app-server protocol; it does not reinterpret a
ChatGPT subscription as an OpenAI Platform API key.

## Required security profile

The adapter must:

- be disabled unless explicitly selected by deployment configuration;
- require an absolute, reviewed Codex binary path;
- use an ephemeral Codex thread for each bounded `AgentExecution`;
- run in a dedicated empty working directory with read-only, restricted
  filesystem access and restricted network access;
- reject every command, file-change, web-search, MCP, permission, and approval
  request rather than mapping it into Agent OS authority;
- pass only the materialized execution prompt and declared model settings;
- bound execution time and response size;
- require the CLI-reported model to match the requested model before sending
  the prompt, require the `openai` provider, and reject any model reroute;
- reject reported usage above the adapter's one-million-token safety ceiling;
- record the subscription adapter identity, validated CLI-reported model, and
  token usage while leaving subscription cost explicitly unknown; and
- close the app-server process during Agent OS shutdown.

Codex approval handlers are not Agent OS approval handlers. A Codex request for
permission must fail; it must never create or consume a `HumanApproval`.

## SDK boundary

The pinned SDK supplies process management, authentication, transport, typed
protocol objects, and confinement parameters. Its high-level run result drops
the model reported by `thread/start`, so Agent OS owns the bounded model-only
thread/turn sequence through the SDK client. It validates the raw start response
for duplicate or case-aliased identity fields before SDK decoding, checks the
reported model and provider before `turn/start`, and subscribes to model reroute
notifications before creating the thread. Reroutes cancel the turn and reject
its output; they do not silently change the authorized model.

The requested model remains in the adapter descriptor and execution manifest.
Successful usage records take their model from the observed start response,
never from a fallback to configuration. Item, completion, and usage evidence
must identify the same fresh thread and turn, including when notifications
arrive before the turn-start response. Failed turns request interruption with
a separate two-second bound when their turn ID is known.

This is evidence reported by the reviewed CLI/provider protocol, not a signed
attestation of a cloud deployment or model weights. A compromised CLI or a
provider that conceals rerouting is outside that assurance. No live-provider
test is implied by the synthetic protocol regressions.

The provider still treats SDK and Codex output as untrusted. A turn is rejected
if the stream reports an error, lacks token telemetry, attempts a command, file
change, web search, MCP call, or returns any item outside the narrow
conversation-only allowlist.

In addition to the existing 256 KiB response and 512 KiB textual stream limits,
the direct lifecycle bounds notifications to 16,384 messages, 512 KiB per
message, 4 MiB in aggregate, and 1,024 completed items. Identity and notification
validation errors do not include notification bodies or reported model names.

The Agent OS service now accepts a configured `ModelAdapter` and derives each
`ExecutionContextManifest` from that adapter's descriptor. This removes the
previous fake-provider hard-coding and is the integration seam for the secured
Codex adapter.

## Setup

Select `Codex subscription` during `agentos` setup. Agent OS discovers the
launching account's Codex executable and credential file when possible. Every
detected choice is shown in a scrollable picker, with manual path entry last.
Paths must resolve to bounded regular files and the credential file cannot be
group- or world-readable. Agent OS requires that credential source to belong to
the verified setup account and confirms that the inspected file is still the
file it opened before reading it. In system mode, the resolved executable must
be root-owned, not group- or world-writable, service-readable, and outside user
and temporary directories. This prevents the restricted service from executing
a binary that the configured owner can replace after setup.

After authenticating the confined app-server, setup retrieves the subscription's
visible model list and presents it as a picker. The selected model and reviewed
binary path are stored in non-secret Agent OS configuration. Agent OS imports
the credential into an authenticated encrypted state file, keeps its encryption
key in a separate systemd encrypted credential, and materializes a private
temporary file only while the provider process runs. Rotated credentials are
sealed back into state before the refresh is accepted. The original
provider-owned credential path is not retained in Agent OS configuration, and
credential material is never copied into the repository, workspace, or logs.

Setup also records a reviewed 30-day token allowance, continuity reserve, and
authorization expiry for the exact subscription model and restricted execution
profile. Subscription access is quota-consuming even when it has no per-call
monetary price. The runtime therefore atomically reserves its conservative
per-request token ceiling before every call and reconciles SDK-reported usage
afterward. Missing, expired, replayed, concurrent, or exhausted authorization
fails closed.

The app-server receives a fresh isolated home and a minimal child environment.
Each inference receives a fresh empty working directory, `never` approval,
read-only sandboxing, no platform-default readable roots, disabled web search,
and a 20-second queue-and-execution deadline, leaving headroom inside the HTTP
server's 30-second response window. The outer Agent OS submission boundary has
a context-aware 25-second deadline that includes service serialization. Shell,
unified execution, multi-agent collaboration, hooks, goals, memory, remote
plugins, network proxying, and web
search are disabled in the thread-scoped Codex configuration before inference
begins. Only the credential refresh callback is registered; all
authority-bearing callbacks fail with method-not-found. Subscription cost
remains unknown rather than being invented.
