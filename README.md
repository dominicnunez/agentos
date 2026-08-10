# Agent OS

Agent OS is an event-driven runtime for durable artificial organizations. This repository builds Agent OS V1 from the smallest useful vertical slice:

```text
Human --> Human Gateway --+
                           +--> shared Intake Router --> Intent --> Goal --> Task DAG
Agent -----A2A Gateway ----+                              |          |
                                                          +--> Event Gateway --> SQLite ledger
                                                                     |
                                                 deterministic handler or AgentExecution
                                                                     |
                                                             Completion Engine
```

The runtime is a Go modular monolith. Internal modules communicate through versioned event contracts, not A2A. SQLite is the authoritative append-only event ledger. Durable Organization, Team, Agent, Intent, Goal, Task, and recipient-inbox projections survive restart. LLM execution is explicit and limited to tasks whose `model_inference_policy` permits it.

## Quick start

Requires Go 1.26.5.

```sh
go test ./...
AGENTOS_A2A_ACTORS_FILE=./a2a-actors.json \
AGENTOS_A2A_AGENT_TOKEN=replace-with-at-least-32-random-characters \
AGENTOS_HUMAN_ACTORS_FILE=./human-actors.json \
AGENTOS_HUMAN_OPERATOR_TOKEN=replace-with-another-32-character-secret \
go run ./cmd/agentos
```

`a2a-actors.json` is a trusted deployment input. A minimal least-privilege
registry entry is:

```json
{
  "actors": [{
    "id": "assistant-agent",
    "organization_id": "org-default",
    "status": "ACTIVE",
    "role": "COLLABORATOR",
    "work_scope": "OWN",
    "token_ref": "AGENTOS_A2A_AGENT_TOKEN",
    "review_ref": "reviewed-config-1",
    "expires_at": "2027-01-01T00:00:00Z",
    "max_concurrent": 2,
    "requests_per_minute": 30
  }]
}
```

`human-actors.json` uses the same fields, with `role` set to `CONTRIBUTOR`,
`OBSERVER`, `RESULT_READER`, `OPERATOR`, or the dedicated completion `REVIEWER`, `work_scope` set to
`ORGANIZATION`, and its own `token_ref`, `review_ref`, expiry, and limits.
Credentials are resolved from each `token_ref`
and never stored in a registry file. Each registry is optional, but startup
requires at least one. The server listens on `127.0.0.1:8080` by default.
Remote binding additionally requires `AGENTOS_ALLOW_REMOTE=true`, an HTTPS
`AGENTOS_PUBLIC_URL`, and both `AGENTOS_TLS_CERT_FILE` and
`AGENTOS_TLS_KEY_FILE`; remote plaintext is rejected.

Interrupted external-effect status checks are separately disabled unless
`AGENTOS_EFFECT_RECONCILERS_FILE` names an exact-scope HTTPS registry. See
[docs/EFFECT_RECONCILIATION.md](docs/EFFECT_RECONCILIATION.md) for its
read-only protocol and fail-closed configuration.

Online SQLite backup, offline verification, and no-overwrite restore are
available through `go run ./cmd/agentos-recovery`. See
[docs/SQLITE_RECOVERY.md](docs/SQLITE_RECOVERY.md) for the tested recovery and
rollback procedure.

Enable the repository-owned commit and push checks once per checkout:

```powershell
.\scripts\install-git-hooks.cmd
```

On Unix-like systems, run `./scripts/install-git-hooks.sh` instead. The commit hook checks staged-diff integrity, formatting, tests, and architecture boundaries. The push hook adds module consistency, build, vet, pinned GolangCI-Lint and `govulncheck` passes, and an advisory Gallow audit. GitHub Actions independently enforces the same checks, race-tests the unit suite, and publishes Gallow findings on pull requests. First runs download the pinned tools through the Go toolchain.

Set `AGENTOS_PUBLIC_URL` to the reachable origin; non-loopback deployments must
use HTTPS. Then submit a minimal A2A v1.0 JSON-RPC task:

```sh
curl -X POST http://localhost:8080/ \
  -H 'Content-Type: application/json' \
	-H 'Authorization: Bearer replace-with-at-least-32-random-characters' \
	-d '{"jsonrpc":"2.0","id":"rpc-1","method":"SendMessage","params":{"message":{"messageId":"message-1","contextId":"request-1","role":"ROLE_USER","parts":[{"text":"echo hello","mediaType":"text/plain"}]}}}'
```

The current deterministic handler supports `echo <text>`. The optional A2A
Message metadata entry `"agentos.execution_kind":"AGENT"` exercises the fake
model adapter; `HUMAN` exercises blocked-input continuation. Runtime data
defaults to `agentos.db`.

The confined Codex subscription provider is available only when explicitly
selected with `AGENTOS_MODEL_PROVIDER=codex-subscription` and the exact binary,
SDK credential-file, and model settings documented in
[docs/CODEX_SUBSCRIPTION_PROVIDER.md](docs/CODEX_SUBSCRIPTION_PROVIDER.md).
Merely installing Codex or setting its normal user configuration never enables
external inference in Agent OS.

The official OpenAI Responses API is separately available with
`AGENTOS_MODEL_PROVIDER=openai-api`, a server-owned API-key reference, and an
exact model snapshot. The provider has a fixed OpenAI endpoint, disables model
tools and response storage, rejects redirects and authority-bearing output, and
does not retry billable calls automatically. Enabling it requires the existing
financial and sensitive-data-boundary approvals described in
[docs/OPENAI_API_PROVIDER.md](docs/OPENAI_API_PROVIDER.md). Real-provider output
is durably recorded as a completion candidate but remains blocked when no
runtime verifier exists. Only a dedicated authenticated Human `REVIEWER` can
decide that exact fingerprinted candidate; model text never certifies itself as
complete. Keep both real providers disabled until the release-candidate,
fake-provider security/recovery, bounded live-test-plan, financial-approval, and
sensitive-data-boundary gates in
[docs/V1_RELEASE_READINESS.md](docs/V1_RELEASE_READINESS.md) are satisfied.

Release/security testing may explicitly select
`AGENTOS_MODEL_PROVIDER=fake-review`. This loopback-only adapter performs no
network access, fails startup on a remote listener, and deliberately has no
deterministic completion verifier. The backup/restart pilot can therefore
exercise the dedicated reviewer path without a provider credential or charge.
It is test evidence, not a real model provider.

Direct human natural-language intake uses a distinct authenticated endpoint and
the same internal router:

```sh
curl -X POST http://localhost:8080/v1/human/messages \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer replace-with-another-32-character-secret' \
  -d '{"conversation_id":"human-1","message_id":"message-1","text":"draft a concise release update"}'
```

Registered `echo` work stays deterministic. Other unstructured natural-language
work routes to the current fake `AgentExecution`; the router itself does not use
an LLM. Human and external-Agent credentials must be distinct. Conversation text never
constitutes a trusted approval decision.

Completion review uses a separate human-only control; it is not a chat command
and is not exposed through A2A. See
[docs/COMPLETION_REVIEW.md](docs/COMPLETION_REVIEW.md).

## Testing status

The repository is ready to test as an early V1 vertical slice. The automated suite covers the deterministic and fake-agent paths, event-backed projection rebuilds, restart-safe Agent/Team/Task inbox delivery at execution boundaries, pending-task recovery, durable human-approval wait/decision state, fail-closed reconciliation of uncertain attempted effects without blind resend, completion verification, Task DAG readiness, the authenticated A2A and protected-effect boundary, vendor-neutral A2A v1.0 interoperability, exact capability checks, single-use effect approvals, unified per-run operational telemetry, institutional knowledge guards, inference reserves, and deterministic audit checks.

This is not a full V1 release sign-off. [docs/V1_ACCEPTANCE_STATUS.md](docs/V1_ACCEPTANCE_STATUS.md) maps the normative architecture checklist to automated evidence; [docs/V1_RELEASE_READINESS.md](docs/V1_RELEASE_READINESS.md) tracks the separate operational release gates, and [docs/RELEASE.md](docs/RELEASE.md) defines reproducible artifacts and their publication boundary.

## Scope

Included: core organizational/work objects, task dependencies, deterministic and agent execution paths, event gateway and ledger, durable addressed messages and inboxes, execution manifests, structured tool outcomes, completion contracts, an inbound A2A boundary, exact capability checks, durable effect-bound human approvals, versioned knowledge and inference seams, and fingerprinted persist-before-effect coordination.

Deferred: federation, workflow DSLs, semantic/vector memory, Lab, optimizers,
broad tool ecosystems, production external-effect adapters, and reconciliation
workers.

See [docs/BUILD_CONTRACT.md](docs/BUILD_CONTRACT.md), [docs/OPERATOR_INTAKE.md](docs/OPERATOR_INTAKE.md), [docs/COMPLETION_REVIEW.md](docs/COMPLETION_REVIEW.md), [docs/APPROVAL_POLICY.md](docs/APPROVAL_POLICY.md), [docs/A2A_INTEROP.md](docs/A2A_INTEROP.md), [docs/EFFECT_RECONCILIATION.md](docs/EFFECT_RECONCILIATION.md), [docs/SQLITE_RECOVERY.md](docs/SQLITE_RECOVERY.md), [docs/CODEX_SUBSCRIPTION_PROVIDER.md](docs/CODEX_SUBSCRIPTION_PROVIDER.md), and [docs/OPENAI_API_PROVIDER.md](docs/OPENAI_API_PROVIDER.md).

## Authoritative architecture handoff

The complete, unmodified original AI coding handoff is preserved in [docs/handoff/](docs/handoff/README.md). Its manifest remains at [docs/handoff/MANIFEST.json](docs/handoff/MANIFEST.json) for byte-level integrity verification.
