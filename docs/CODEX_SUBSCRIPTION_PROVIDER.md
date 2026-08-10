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

## SDK prerequisite

The current SDK high-level `RunOptions` exposes approval policy but does not
forward `cwd` or sandbox settings into `thread/start`. Although the lower-level
protocol types support both, duplicating the SDK's turn lifecycle inside Agent
OS would create a second, fragile protocol implementation.

Before enabling this provider, extend the SDK's high-level run options to carry
an absolute working directory and explicit sandbox mode through
`buildThreadParams`, with tests proving those fields are sent. Agent OS must
then pin the reviewed SDK revision and fail startup if the restricted profile
cannot be established.

The Agent OS service now accepts a configured `ModelAdapter` and derives each
`ExecutionContextManifest` from that adapter's descriptor. This removes the
previous fake-provider hard-coding and is the integration seam for the secured
Codex adapter.
