# Planning and Task DAGs

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
- a SHA-256 fingerprint of the complete Plan;
- the prompt-contract version and configured provider identity;
- the exact input Event references; and
- provider-reported inference usage.

The complete Task graph is committed to SQLite in one transaction. A failed
write leaves no Task executable, and recovery rejects a partial or conflicting
graph. Delivery retries reuse the durable Plan instead of repeating planning
inference.

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

Recovery resumes planning only when the ledger proves that no adaptive
planning context was manifested. A validated durable Plan is materialized
without another inference call. An interrupted model-planning attempt is not
replayed: the Goal records `GOAL_PLANNING_FAILED` with a stable code and becomes
terminal so accepted work cannot remain silently active or duplicate provider
cost.

A failed dependency deterministically terminalizes every affected pending or
blocked dependent through `TASK_DEPENDENCY_FAILED`; failure then propagates to
the runtime root and Goal instead of leaving work active forever. Once the
root fails, remaining nonterminal sibling work is stopped through
`TASK_GOAL_FAILED` before another Task is selected. The events' stable codes
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
actionable Task, the runtime supplies that exact same-goal blocked contract as
bounded evidence. The scheduler never treats blocked work as completed or lets
another Task inherit missing authority.

A bounded remediation pass may report and route a child block, but it cannot
invent a DAG mutation or treat the dependency as complete. When an Agent root
has no governing parent and the block remains unresolved, the runtime records
`TASK_REMEDIATION_FAILED`, fails the root, and terminalizes the Goal rather than
advertising an input request that no authorized V1 transition can consume.

Intent, Goal, and Task projection histories keep one immutable correlation
boundary across every version. Rebuild fails closed if any historical record
crosses that boundary, even if a later version changes it back.
