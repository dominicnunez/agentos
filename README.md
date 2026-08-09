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

The runtime is a Go modular monolith. Internal modules communicate through versioned event contracts, not A2A. SQLite is the authoritative append-only event ledger. LLM execution is explicit and limited to tasks whose `model_inference_policy` permits it.

## Quick start

Requires Go 1.24+.

```sh
go test ./...
AGENTOS_OPERATOR_TOKEN=change-me go run ./cmd/agentos
```

Then submit a minimal A2A task:

```sh
curl -X POST http://localhost:8080/a2a/v1/tasks/send \
  -H 'Content-Type: application/json' \
	-H 'Authorization: Bearer change-me' \
  -d '{"id":"request-1","message":{"role":"user","parts":[{"type":"text","text":"echo hello"}]},"metadata":{"execution_kind":"DETERMINISTIC"}}'
```

The current deterministic handler supports `echo <text>`. Use `execution_kind: AGENT` to exercise the fake model adapter. Runtime data defaults to `agentos.db`.

## Scope

Included: core organizational/work objects, task dependencies, deterministic and agent execution paths, event gateway and ledger, execution manifests, structured tool outcomes, completion contracts, and an inbound A2A boundary.

Deferred: federation, workflow DSLs, semantic/vector memory, Lab, optimizers, production model providers, broad tool ecosystems, and external-effect execution.

See [docs/BUILD_CONTRACT.md](docs/BUILD_CONTRACT.md) and [docs/APPROVAL_POLICY.md](docs/APPROVAL_POLICY.md).
