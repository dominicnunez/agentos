# Planning and Task DAGs

Agent OS uses `Mission > Goal > Work > Task` as its organizational hierarchy.
This document covers the bounded execution layers: one Work owns an accepted
Intent and immutable Plan, and its Tasks form that Plan's executable DAG.
Completing Work proves only that bounded undertaking; it cannot by itself mark
a longer-lived Goal achieved or change the Mission.

Mission and Goal are durable, organization-scoped projections. Mission carries
enduring direction and is revised or retired rather than completed. Goal
carries measurable success criteria and is either a finite target or a
continuous objective. Revisions append new projection versions while retaining
the Mission/organization identity. A bare projection update cannot mark any
Goal achieved; that transition remains fail-closed until it is coupled to the
separate durable progress-evidence path. A Work may bind to one active Goal in
the same organization; that binding, its Intent, and its objective are
immutable after creation. Goal linkage is optional for ad hoc Work.

After an Intent is confirmed, Agent OS converts it into the smallest useful
dependency graph of bounded Tasks. The graph is a runtime coordination
contract, not a grant of authority.

Known exact work, such as the registered deterministic `echo` operation, does
not invoke a planning model. Natural-language Agent work uses the configured
provider once to propose child work units. Agent OS then applies a closed JSON
schema and rejects unknown fields, unsupported execution kinds, duplicate or
unknown dependencies, cycles, oversized graphs, invented deterministic
operations, and more than 16 total Tasks. The runtime always creates the root
integration Task itself.

Every accepted Plan records:

- the exact confirmed Intent fingerprint;
- the exact Mission and Goal projection events and versions for Goal-bound Work;
- a SHA-256 fingerprint of the complete Plan;
- the prompt-contract version and configured provider identity;
- the exact input Event references; and
- provider-reported inference usage.

Adaptive planning receives those exact Mission and Goal revisions as
organizational direction, clearly separated from authority. The Plan
fingerprint covers their immutable references. Agent execution materializes
the same revisions into its input and records them in the execution manifest.
If either projection changes before execution, Agent OS fails the stale Task
with `STRATEGIC_CONTEXT_CHANGED`; the Work then becomes terminal so replacement
Work can receive a new reviewed Intent and Plan. The SQLite execution-start
transaction rechecks the exact references for Agent, deterministic, and
user-operated work so a concurrent revision cannot cross the time-of-use
boundary. Agent execution also rejects strategic context that cannot fit its
bounded input before execution start is persisted. Agent OS does not silently
reinterpret the Plan under new organizational direction. Ad hoc Work carries
no strategic references.

The complete Task graph is committed to SQLite in one transaction. A failed
write leaves no Task executable, and recovery rejects a partial or conflicting
graph. Delivery retries reuse the durable Plan instead of repeating planning
inference.

An Agent assignment is revalidated again when execution actually starts. In
the same SQLite transaction that moves the Task from `PENDING` to `RUNNING`,
Agent OS requires the exact assigned Agent, blueprint, and execution profile to
remain active and records their immutable revision references. If a roster or
Lab promotion changed that assignment first, dispatch fails rather than
silently selecting a replacement. If execution start committed first, a later
deactivation prevents future dispatches but does not cancel that already
admitted invocation. Interrupted adaptive work remains uncertain and is never
blindly replayed.

Dependencies become eligible only after their durable Task state is verified
complete. A downstream AgentExecution receives only the latest valid
`RESULT_PUBLISHED` contract for each declared dependency. The execution
manifest records those exact Event IDs. Result summaries and artifacts remain
untrusted work evidence; they cannot authorize effects, approve requests,
change policy, expand capabilities, or certify completion.

Planning and each AgentExecution use separate bounded provider turns. Once an
operator's Intent confirmation has been accepted and made durable, a client
disconnect or expired request deadline cannot cancel outcome persistence and
strand a Task as `RUNNING`. The scheduler reloads authoritative state after
each selected Task before choosing more work.

Planning resumes only when the ledger proves that no adaptive planning context
was manifested. A validated durable Plan is materialized without another
inference call. Once a model context exists, every error, timeout, malformed
result, or failed Plan write is a non-replayable planning attempt in both the
running process and startup recovery. Agent OS durably records the failure,
then `RUN_TELEMETRY_RECORDED` with timing, available provider usage, and exact
failure evidence, and only then terminalizes the Work through
`WORK_PLANNING_FAILED`. A crash between those steps resumes the existing
failure decision instead of invoking the provider or inventing a different
reason. Planning rejected before a model turn, including an unsupported exact
operation, follows the same telemetry-before-terminal transition and is not
silently left active.

A failed dependency deterministically terminalizes every affected pending or
blocked dependent through `TASK_DEPENDENCY_FAILED`; failure then propagates to
the runtime root and Work instead of leaving work active forever. Once the
root fails, remaining nonterminal sibling work is stopped through
`TASK_WORK_FAILED` before another Task is selected. The events' stable codes
and exact Task IDs are machine contract data, independent of user-facing
language.

The externally visible A2A Task is always the runtime-owned root. Internal
child IDs are not registered for external lookup, and their intermediate
status or result cannot replace the root Task's public state. Authenticated
local completion control resolves child Tasks through a separate
organization-scoped projection path, so required reviews remain possible
without expanding the A2A address space. The local review list is paginated so
the operator can discover pending internal review IDs without knowing them in
advance.

Real model outputs without a registered deterministic verifier still require
an independent completion judgment. A pending review prevents parent
remediation from substituting model judgment for that review. Approval wakes
newly eligible dependents immediately; rejection follows the deterministic
failure path. Other child work that cannot proceed emits a typed blocked-work
contract to its accountable parent. When a deeper DAG dependent is the next
actionable Task, the runtime supplies that exact same-work blocked contract as
bounded evidence. The scheduler never treats blocked work as completed or lets
another Task inherit missing authority.

A bounded remediation pass may report and route a child block, but it cannot
invent a DAG mutation or treat the dependency as complete. When an Agent root
has no governing parent and the block remains unresolved, the runtime records
`TASK_REMEDIATION_FAILED`, fails the root, and terminalizes the Work rather than
advertising an input request that no authorized V1 transition can consume.

Intent, Work, and Task projection histories keep one immutable correlation
boundary across every version. Rebuild fails closed if any historical record
crosses that boundary, even if a later version changes it back.
