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

## Implemented V1 seams

The repository includes exact fail-closed capability checks, append-only
versioned records, provenance-gated institutional knowledge, deterministic audit
rules, reserve-aware inference selection and normalized usage snapshots, an
environment-backed `SecretSource`, a real OpenAI-compatible model adapter, and
fingerprinted persist-before-effect obligations with distinct attempted and
confirmed states.

The A2A adapter supports authenticated discovery and submission plus
capability-gated status and input continuation. Production deployments must
replace the example static bearer binding at their ingress boundary and
explicitly configure a provider adapter. Credentials and A2A wire types remain
outside core domain objects.

Durable organization/work projections now commit atomically with their
authoritative transition events and can be rebuilt by replay. Startup validates
that state before opening the operator endpoint, preserves blocked work, runs
dependency-ready pending work, retries only known-safe interrupted deterministic
work, and blocks interrupted adaptive execution whose outcome is uncertain.
