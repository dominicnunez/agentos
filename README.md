# Agent OS

Agent OS is an event-driven runtime for durable artificial organizations. This repository builds Agent OS V1 from the smallest useful vertical slice:

```text
Human --> Human Gateway --+
                           +--> shared Intake Router --> Intent --> Goal --> Task DAG
Hermes ----A2A Gateway ----+                              |          |
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
AGENTOS_OPERATOR_TOKEN=agent-change-me \
AGENTOS_HUMAN_TOKEN=human-change-me \
go run ./cmd/agentos
```

Enable the repository-owned commit and push checks once per checkout:

```powershell
.\scripts\install-git-hooks.cmd
```

On Unix-like systems, run `./scripts/install-git-hooks.sh` instead. The commit hook checks staged-diff integrity, formatting, tests, and architecture boundaries. The push hook adds module consistency, build, vet, a pinned blocking GolangCI-Lint pass, and an advisory Gallow audit. GitHub Actions independently enforces the same lint configuration, race-tests the unit suite, and publishes Gallow findings on pull requests. The first push-hook lint run downloads the pinned linter release through the Go toolchain.

Set `AGENTOS_PUBLIC_URL` to the externally reachable HTTP(S) origin in deployed
environments. Then submit a minimal A2A v1.0 JSON-RPC task:

```sh
curl -X POST http://localhost:8080/ \
  -H 'Content-Type: application/json' \
	-H 'Authorization: Bearer change-me' \
	-d '{"jsonrpc":"2.0","id":"rpc-1","method":"SendMessage","params":{"message":{"messageId":"message-1","contextId":"request-1","role":"ROLE_USER","parts":[{"text":"echo hello","mediaType":"text/plain"}]}}}'
```

The current deterministic handler supports `echo <text>`. The optional A2A
Message metadata entry `"agentos.execution_kind":"AGENT"` exercises the fake
model adapter; `HUMAN` exercises blocked-input continuation. Runtime data
defaults to `agentos.db`.

Direct human natural-language intake uses a distinct authenticated endpoint and
the same internal router:

```sh
curl -X POST http://localhost:8080/v1/human/messages \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer human-change-me' \
  -d '{"conversation_id":"human-1","message_id":"message-1","text":"draft a concise release update"}'
```

Registered `echo` work stays deterministic. Other unstructured natural-language
work routes to the current fake `AgentExecution`; the router itself does not use
an LLM. Human and Hermes credentials must be distinct. Conversation text never
constitutes a trusted approval decision.

## Testing status

The repository is ready to test as an early V1 vertical slice. The automated suite covers the deterministic and fake-agent paths, event-backed projection rebuilds, restart-safe Agent/Team/Task inbox delivery at execution boundaries, pending-task recovery, durable human-approval wait/decision state, fail-closed handling of uncertain interrupted work, completion verification, Task DAG readiness, the authenticated A2A and protected-effect boundary, pinned real-client Hermes v0.20.0 interoperability, exact capability checks, single-use effect approvals, unified per-run operational telemetry, institutional knowledge guards, inference reserves, and deterministic audit checks.

This is not a full V1 acceptance sign-off. [docs/V1_ACCEPTANCE_STATUS.md](docs/V1_ACCEPTANCE_STATUS.md) maps the normative checklist to current evidence and keeps incomplete items visible.

## Scope

Included: core organizational/work objects, task dependencies, deterministic and agent execution paths, event gateway and ledger, durable addressed messages and inboxes, execution manifests, structured tool outcomes, completion contracts, an inbound A2A boundary, exact capability checks, durable effect-bound human approvals, versioned knowledge and inference seams, and fingerprinted persist-before-effect coordination.

Deferred: federation, workflow DSLs, semantic/vector memory, Lab, optimizers, production provider wiring, broad tool ecosystems, production external-effect adapters, and reconciliation workers.

See [docs/BUILD_CONTRACT.md](docs/BUILD_CONTRACT.md), [docs/OPERATOR_INTAKE.md](docs/OPERATOR_INTAKE.md), [docs/APPROVAL_POLICY.md](docs/APPROVAL_POLICY.md), and [docs/HERMES_INTEROP.md](docs/HERMES_INTEROP.md).

## Authoritative architecture handoff

The complete, unmodified original AI coding handoff is preserved in [docs/handoff/](docs/handoff/README.md). Its manifest remains at [docs/handoff/MANIFEST.json](docs/handoff/MANIFEST.json) for byte-level integrity verification.
