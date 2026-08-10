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
- record the SDK-reported provider, model, and token usage while leaving
  subscription cost explicitly unknown; and
- close the app-server process during Agent OS shutdown.

Codex approval handlers are not Agent OS approval handlers. A Codex request for
permission must fail; it must never create or consume a `HumanApproval`.

## SDK boundary

The pinned SDK revision includes the reviewed high-level confinement controls:
an absolute working directory on both thread and turn requests, thread-scoped
configuration, read-only sandbox mode, and granular restricted-readable roots.
It also normalizes or rejects turn working directories at the protocol
boundary. Agent OS therefore uses the SDK's single turn lifecycle instead of
maintaining a second protocol implementation.

The provider still treats SDK and Codex output as untrusted. A turn is rejected
if the stream reports an error, lacks token telemetry, attempts a command, file
change, web search, MCP call, or returns any item outside the narrow
conversation-only allowlist.

The Agent OS service now accepts a configured `ModelAdapter` and derives each
`ExecutionContextManifest` from that adapter's descriptor. This removes the
previous fake-provider hard-coding and is the integration seam for the secured
Codex adapter.

## Deployment configuration

The provider remains disabled by default. Selecting it requires all four exact
settings:

- `AGENTOS_MODEL_PROVIDER=codex-subscription`
- `AGENTOS_CODEX_BINARY` set to an absolute, clean path to a reviewed regular
  Codex binary (symlinks are rejected)
- `AGENTOS_CODEX_CREDENTIALS_FILE` set to an absolute, clean path to an SDK
  credential file (symlinks and group/world-readable Unix files are rejected)
- `AGENTOS_CODEX_MODEL` set to the exact model identifier

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
