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

A failed dependency deterministically terminalizes every affected pending or
blocked dependent through `TASK_DEPENDENCY_FAILED`; failure then propagates to
the runtime root and Goal instead of leaving work active forever. The event's
stable `DEPENDENCY_FAILED` code and exact failed Task IDs are machine contract
data, independent of user-facing language.

The externally visible A2A Task is always the runtime-owned root. Internal
child IDs are not registered for external lookup, and their intermediate
status or result cannot replace the root Task's public state. Authenticated
local completion control resolves child Tasks through a separate
organization-scoped projection path, so required reviews remain possible
without expanding the A2A address space.

Real model outputs without a registered deterministic verifier still require
an independent completion judgment. A pending review prevents parent
remediation from substituting model judgment for that review. Approval wakes
newly eligible dependents immediately; rejection follows the deterministic
failure path. Other child work that cannot proceed emits a typed blocked-work
contract to its parent, and the scheduler never treats blocked work as
completed or lets the parent inherit missing authority.
