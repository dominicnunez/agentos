# Agent OS

Agent OS is an event-driven runtime for durable artificial organizations. This repository begins the v4.2 build contract with the smallest useful vertical slice:

```text
Hermes --A2A--> Operator Gateway --> Intent --> Goal --> Task DAG
                                               |          |
                                               +--> Event Gateway --> SQLite ledger
                                                          |
                                      deterministic handler or AgentExecution
                                                          |
                                                  Completion Engine
```

The runtime is a Go modular monolith. Internal modules communicate through versioned event contracts, not A2A. SQLite is the authoritative append-only event ledger. Durable Organization, Team, Agent, Intent, Goal, and Task projections can be rebuilt from those events. LLM execution is explicit and limited to tasks whose `model_inference_policy` permits it.

## Quick start

Requires Go 1.26.5.

```sh
go test ./...
AGENTOS_OPERATOR_TOKEN=change-me go run ./cmd/agentos
```

Enable the repository-owned commit and push checks once per checkout:

```powershell
.\scripts\install-git-hooks.cmd
```

On Unix-like systems, run `./scripts/install-git-hooks.sh` instead. The commit hook checks staged-diff integrity, formatting, tests, and architecture boundaries. The push hook adds module consistency, build, vet, and an advisory Gallow audit. GitHub Actions independently enforces the blocking checks, race-tests the unit suite, and publishes Gallow findings on pull requests.

Then submit a minimal A2A task:

```sh
curl -X POST http://localhost:8080/a2a/v1/tasks/send \
  -H 'Content-Type: application/json' \
	-H 'Authorization: Bearer change-me' \
  -d '{"id":"request-1","message":{"role":"user","parts":[{"type":"text","text":"echo hello"}]},"metadata":{"execution_kind":"DETERMINISTIC"}}'
```

The current deterministic handler supports `echo <text>`. Use `execution_kind: AGENT` to exercise the fake model adapter. Runtime data defaults to `agentos.db`.

## Testing status

The repository is ready to test as an early V1 vertical slice. The automated suite covers the deterministic and fake-agent paths, event-backed projection rebuilds, pending-task restart recovery, fail-closed handling of uncertain interrupted agent work, completion verification, Task DAG readiness, the authenticated A2A boundary, exact capability checks, single-use effect approvals, institutional knowledge guards, inference reserves, and deterministic audit checks.

This is not a full V1 acceptance sign-off. [docs/V1_ACCEPTANCE_STATUS.md](docs/V1_ACCEPTANCE_STATUS.md) maps the normative checklist to current evidence and keeps incomplete items visible.

## Scope

Included: core organizational/work objects, task dependencies, deterministic and agent execution paths, event gateway and ledger, execution manifests, structured tool outcomes, completion contracts, an inbound A2A boundary, exact capability checks, versioned knowledge and inference seams, and fingerprinted persist-before-effect coordination.

Deferred: federation, workflow DSLs, semantic/vector memory, Lab, optimizers, production provider wiring, broad tool ecosystems, production external-effect adapters, and reconciliation workers.

See [docs/BUILD_CONTRACT.md](docs/BUILD_CONTRACT.md) and [docs/APPROVAL_POLICY.md](docs/APPROVAL_POLICY.md).

## Authoritative architecture handoff

The complete, unmodified Agent OS v4.2 AI coding handoff is preserved in [docs/handoff/](docs/handoff/README.md). Its original manifest remains at [docs/handoff/MANIFEST.json](docs/handoff/MANIFEST.json) for byte-level integrity verification.
