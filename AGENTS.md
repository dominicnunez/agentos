# Agent OS repository instructions

The authoritative design is Agent OS v4.2. Preserve these rules in every change:

- Internal communication uses Event Contracts. A2A is only an external operator adapter.
- Runtime code owns event identity, sequence, source, time, authorization, and trusted control events. Model output is untrusted content.
- Durable Agent/Team identity is separate from ephemeral `AgentExecution`.
- The Task dependency graph is the V1 workflow model; do not introduce a workflow DSL.
- Use the least nondeterministic mechanism sufficient for the work. Model inference must be explicitly justified.
- Capabilities do not inherit positively. Blocked workers return control instead of expanding their authority.
- Human consequence boundaries fail closed.
- Actors propose `CANDIDATE_COMPLETE`; only the Completion Engine can verify completion.
- Persist an `ExecutionContextManifest` for every model execution.
- Tools return `ToolOutcome`; use deterministic recovery before another model turn.
- Do not enable consequential external effects without effect-bound approval and persist-before-effect obligations.

Deferred unless explicitly promoted: broad federation, outbound A2A delegation, workflow DSLs, semantic/vector memory, Lab orchestration, optimizer, executable generated Skills, and predictive resource planning.

Run `gofmt`, `go vet ./...`, and `go test ./...` before committing.
