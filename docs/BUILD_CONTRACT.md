# v4.2 build contract

## Invariants

1. Work determines execution topology. A task DAG is the V1 workflow representation.
2. `Agent` is durable identity; `AgentExecution` is a bounded invocation. Persistent identity is never created merely to gain parallelism.
3. Use the least nondeterministic mechanism sufficient for the work. Model inference must be explicitly justified by task policy.
4. State changes become append-only Event Contracts through the Event Gateway. Projections may be rebuilt from the ledger.
5. A tool report is not proof of an effect. `ToolOutcome` distinguishes status, observed effect, verification, and retryability.
6. Completion is evaluated against an explicit `CompletionContract`, not an actor's self-report.
7. `ExecutionContextManifest` records what an execution was actually given.
8. A2A v1.0 is an external Hermes/operator boundary. It does not become the internal communication model.
9. Approval decisions fail closed. An approval authorizes a fingerprinted effect, never a general privilege expansion.

## First slice

The operator gateway accepts a bounded A2A task, creates an Intent, Goal, and single-node Task DAG, executes either a deterministic handler or a fake-model `AgentExecution`, records each transition, applies the completion engine, and returns terminal task state.

The fake adapter is deliberately non-intelligent: it makes the execution seam testable without hiding deterministic work behind an LLM.

## Not yet implemented

External effects and durable effect obligations, full capability issuance, production identity/authentication, schedulers with leases/recovery, projections, real model adapters, multi-node A2A interoperability, knowledge/skills, and advanced resource management are subsequent slices.
