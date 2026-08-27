# Shared coordination

Agent OS gives an executing Agent a bounded view of the other Tasks in its
current Work. This lets an Agent understand who is doing what without creating
a second blackboard, treating messages as state, or exposing internal Tasks
through A2A.

## Execution snapshot

Immediately before an Agent Task enters `RUNNING`, the same SQLite transaction
selects the latest admitted revision of every other Task in that Work. Each
peer entry contains only:

- Task identity and exact version;
- parent relationship and dependencies;
- description and execution kind;
- durable status; and
- current assignee type and identity, when assigned.

The list is ordered by Task identity, limited to 15 peers, and included in the
existing 256 KiB aggregate Agent-input limit. The execution-context manifest
records every selected Task ID and version under `coordination_refs` using
context-builder version `v3`.

Completion and recovery reconstruct the same pre-start snapshot from sealed
Task Event Contracts. A peer revision admitted after the execution start does
not rewrite the input that the Agent received. Missing, malformed,
noncontiguous, cross-Work, duplicated, oversized, or substituted state fails
closed.

## Trust boundary

Peer state is untrusted coordination context. It does not:

- grant authority, capability, approval, or effect permission;
- allow one Agent to mutate another Task;
- prove that a completed peer produced a correct result; or
- replace dependency-result evidence, inbox messages, completion contracts,
  or runtime policy checks.

An Agent must still consume exact dependency evidence when its Task declares a
dependency. Messages and handoffs continue to use addressed Event Contracts.
The peer snapshot is internal runtime context and is not added to the public
A2A Task surface.
