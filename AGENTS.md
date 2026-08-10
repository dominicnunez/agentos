# Agent OS repository guidance

Agent OS is a Go modular monolith for operating persistent AI-assisted organizations.

## Authoritative sources

- Start with [`docs/handoff/QUICK_START.md`](docs/handoff/QUICK_START.md).
- Treat [`docs/handoff/IMPLEMENTATION_SCOPE.yaml`](docs/handoff/IMPLEMENTATION_SCOPE.yaml) as the scope authority.
- Use [`docs/handoff/docs/29_V1_BUILD_CONTRACT.md`](docs/handoff/docs/29_V1_BUILD_CONTRACT.md) for implementation requirements.
- Consult the remaining preserved handoff only when the current work needs its detail.

## Project-wide boundaries

- Internal coordination uses runtime-owned Event Contracts over one authoritative ledger; model-generated content is untrusted.
- Actual work determines the Task DAG and execution structure. Use model inference only where adaptive intelligence is justified.
- Authority and completion fail closed: workers cannot expand their own capabilities or certify their own completion.
- A2A is an external operator boundary, not internal IPC or implicit administrative authority.
- Keep deferred architecture deferred unless a human explicitly promotes it from the handoff scope.

Repository hooks and CI own routine formatting, lint, test, vet, and build enforcement.
