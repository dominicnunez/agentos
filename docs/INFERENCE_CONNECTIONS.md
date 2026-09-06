# Inference connections and shared budgets

The runtime supports multiple configured provider connections at once. Explicit
installation rules or a policy-constrained broker select accounts for task execution,
planning, and intent normalization. Tasks pin immutable execution profiles; changing an Agent's current
profile or the installation default cannot redirect an already assigned task.
The supported adapters remain OpenAI API and Codex subscription. This is not a
claim of complete Hermes coverage or support for advanced model transports.

## Installation configuration

Version-2 installation files may contain `provider_routing`. An existing file
without that field retains its single unnamed provider. Named connections require
version-2 inference policies and an explicit route for every inference purpose.
For example, add this section alongside two fully configured provider entries:

```json
"provider_routing": {
  "task_default": "primary",
  "task_connections": { "research": "auxiliary" },
  "planning": "auxiliary",
  "normalization": "auxiliary"
}
```

The two provider policies must use `connection_id` values `primary` and `auxiliary`.
Each retains its exact provider/model/profile, credential reference, authorization,
and per-route limits. Set policy `version` to `2` and include the same reviewed
`organization_budget` in each, for example for zero-cost subscription accounting:

```json
"organization_budget": {
  "window_duration_seconds": 3600,
  "max_tokens_per_window": 100000,
  "continuity_reserve_tokens": 10000,
  "max_cost_nano_usd_per_window": 0,
  "max_concurrent_requests": 2
}
```

These fragments extend a complete reviewed installation; they are not standalone
configuration files. Choose limits appropriate to the authorized work. Metered
routes also require current pricing and a sufficient organization monetary cap.
All policies in an applied set must share organization, author, authorization time,
and organization limits. Changing accounts or routes never resets accumulated usage.

Task rules match exact planned task keys (lowercase letters/digits/hyphens, up to
64 characters). Without routing requirements, a key without a rule uses the explicit
task default. A rule cannot select an absent account or weaken task, completion,
confidentiality, or budget requirements.

Provision every referenced encrypted credential before applying the configuration.
Codex connections need distinct mutable sealed credential stores. After editing
the installed configuration, run `agentos doctor` for offline readiness checks and
`agentos setup providers` to validate the full set, regenerate service credential
directives, and restart the service if it is running. User-mode application requires
the installation owner; system mode uses the existing administrator setup flow.
The apply command does not probe providers or recollect credentials. The singular
`agentos setup provider` wizard remains for single-provider installations and
rejects routed configurations instead of replacing their provider list.

Startup recovers accounting once and activates the reviewed policy set atomically.
Every adapter receives a credential source limited to its configured reference and
a private runtime directory under `connections/<connection_id>`. Partial startup
closes initialized adapters in reverse order. Doctor and generated systemd units
cover every configured account.

## Selection from task and purpose requirements

An account can participate in broker selection when its version-2 inference policy
includes a reviewed `catalog` and organization `routing` policy. The catalog declares
`capabilities`, `local`, `context_tokens`, `output_tokens`, `data_classes`, and
`valid_until`. Identity comes from the enclosing policy. Expiration must fall within
the policy authorization period. Zero capacity means unknown and cannot satisfy a
positive token requirement. Current adapters may advertise only `text`; configuration
cannot activate vision, tools, streaming, or other unimplemented transports.

The policy's `routing` object contains `organization_id`, `locality`, approved
`data_classes`, and optional `allowed_providers` and `denied_providers`. Every active
named account must agree on these organization rules. Change them through the same
atomic policy-set activation used for shared budgets. Both admission and historical
replay enforce agreement.

Add `task_requirements`, `planning_requirements`, and `normalization_requirements`
to `provider_routing` to select accounts independently for each purpose. Each is a
complete requirements object, for example:

```json
{
  "organization_id": "organization-1",
  "capabilities": ["text"],
  "input_tokens": 8000,
  "output_tokens": 1000,
  "locality": "CLOUD_ALLOWED",
  "data_class": "internal",
  "preferred_connections": ["auxiliary", "primary"]
}
```

Use the installation's actual organization and reviewed token/classification limits.
Optional `connection_id` is a hard account constraint; provider allow/deny lists and
`max_cost_nano_usd` further restrict eligibility. `LOCAL_ONLY` prohibits cloud accounts
regardless of preferences. Local catalog metadata does not establish model identity
or confinement, or enable an unsupported local provider setup.

`task_requirements_by_key` maps exact task keys to complete requirements, replacing
the default requirements for those tasks. Explicit `task_connections` remain hard
constraints and conflicting account choices fail. When no preferences are supplied,
the configured task, planning, or normalization account becomes the first preference.
Preferences apply only after all eligibility checks. Catalog-enabled purposes must
have requirements; startup rejects missing requirements.

The registry selects from a verified ledger snapshot of active policy and account
and organization budgets. It rejects stale catalog metadata, unsupported capabilities,
prohibited destinations/classifications, insufficient context/output capacity, and
unaffordable reservations. Ties use connection ID order. Selection does not reserve
capacity or call a provider; dispatch performs atomic admission again. A budget or
policy change can therefore deny a previously selected route.

Task selection occurs once before preparing its Agent and saving the Task DAG.
Saved tasks retain their constraints across configuration changes. Planning and
normalization select an adapter per attempt before recording their context. Execution
manifests and reservations carry the same requirements; admission and replay compare
them. A failed provider call does not trigger an implicit retry on another account.

Broker decisions also record the selected policy fingerprint, ledger cutoff, ordering
reason and reservation estimate. Replay reconstructs policies and charges at that
cutoff and checks the selected account's eligibility, including organization freeze,
account concurrency and shared budgets. A later refund or policy revision cannot
justify an earlier decision. This check establishes selected-account eligibility;
it does not rank accounts absent from the runtime's configured registry or reproduce
the recorded count of rejected candidates.

## Connection identity

A version-2 inference policy binds a `connection_id`, organization, exact provider,
model, execution profile, authorization, route limits, and organization budget.
Connection IDs contain 1–128 lowercase ASCII letters, digits, hyphens, or underscores.
They identify configured accounts, not endpoints or credentials.

`inference.NewConnectionRegistry` accepts adapters from trusted composition and
wraps each in a `GuardedAdapter` bound to its connection. Lookup requires the exact
ID, including when two accounts use the same provider and model. A lookup has no
implicit default or fallback. Credential resolution and adapter cleanup remain the
responsibility of runtime composition; the registry neither stores credential
metadata nor resolves secrets.

```mermaid
flowchart LR
    Composition[Trusted runtime composition] --> Registry[Connection registry]
    Requirements[Task or purpose requirements] --> Selection[Verified ledger selection]
    Registry --> Selection
    Selection --> Context[Save task or attempt context]
    Context --> Guard[Guarded adapter for exact connection]
    Guard --> Admission[Ledger reservation]
    Admission --> Limits[Route and organization limits]
    Limits --> Call[Bound provider adapter]
    Call --> Reconcile[Durable reconciliation]
```

## Resource accounting

Each connection policy requires an explicit organization budget: accounting window,
token allowance, continuity reserve, monetary allowance in nano-USD, and concurrent
request limit. A zero monetary allowance permits zero-cost reservations only.
These limits supplement route limits. Organization totals include all providers,
models, and legacy requests; adding an account or changing a model does not allocate
a fresh organization budget. Connection concurrency is tracked separately, while
organization concurrency covers all outstanding calls.

Admission reserves the maximum authorized input, output, and estimated cost before
invoking an adapter. Successful reconciliation records actual usage; unsent requests
release their reservations. Interrupted calls become uncertain and retain their
full charge. Outstanding reservations continue to count across a window rollover.
Usage that violates a reservation retains the conservative charge.

Every active connection must agree on organization limits. Use
`SQLite.ActivateInferencePolicies` to change a reviewed set atomically. Members must
belong to one organization, have distinct connection IDs, and share author,
authorization time, and organization budget. Changing shared limits requires all
affected connections. A missing member, invalid policy, or outstanding call on a
replaced connection rolls back the entire set. Changing limits preserves prior use.

## History, migration, and recovery

Storage v10 adds connection IDs to inference policies and reservations and permits
one active policy per organization and connection. Historical version-1 policies
retain their original serialization and fingerprints. Their empty connection ID
identifies the legacy singleton; it is never guessed or assigned to a new account.

New reservation events record `admitted_at`, which must equal the accounting row's
admission time and fall within its route window, authorization period, and pricing
validity period. Older events did not bind a precise
admission timestamp. Shared accounting therefore counts their recorded route windows
conservatively when those windows overlap the organization window, rather than
trusting an unbound timestamp. Historical bytes remain unchanged by migration.

Validation matches policies, reservations, and reconciliations to exact events,
checks connection revision lifetimes, and reconstructs shared charges in event order.
Replay maintains running totals within each organization window and rebuilds them
when a policy or window changes; its decisions are tested against a scan-based
reference implementation.
A refund recorded later cannot justify an earlier over-budget admission. Atomic
policy-set changes may contain adjacent revisions with temporarily differing limits;
they must share an authorization and restore agreement before any other event or
the end of history. Recovery validates accounting before marking calls uncertain.
It does not repeat provider calls.

Planning and normalization reservations bind the exact runtime context event when
one exists. Admission and replay verify its account, model identity, routing requirements, and execution
scope. Named connections cannot drop or substitute that reference during replay.
Historical singleton contexts without references and standalone accounting requests
without application context retain their earlier accounting contract.

## Remaining integration

Constraint-preserving fallback, health/latency-aware selection, richer selection and
rejection audit reasons, advanced model transports, additional provider adapters,
and their setup flows remain implementation work. Shared admission performs snapshot validation; its cost on long
histories requires further performance assessment at larger deployment scales.

Tests cover distinct and identical-model accounts, simultaneous reservations,
connection substitution, shared token/cost/concurrency limits, atomic updates,
historical budget decisions, migration, and restart recovery using synthetic models.
These tests do not constitute live-provider or confinement verification.

Concurrent-submission tests verify that separate workflows preserve account
identity in task manifests and usage. The service currently serializes execution;
these tests do not claim parallel model calls.
