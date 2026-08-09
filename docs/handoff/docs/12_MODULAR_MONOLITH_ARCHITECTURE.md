# Modular Monolith Architecture

## 1. Decision

One Go repository, one primary runtime process, one initial database, one composition root, strict module ownership.

## 2. Suggested boundaries

```text
events          canonical Event/EventDraft contracts + gateway
ledger          append/read/replay/integrity
projections     inbox/task/team/audit/authorization projections
actors          Agent identity/blueprints/profiles
teams           membership/team lifecycle
organizations   organization identity/minimal policy binding
tasks           Goal/Task/TaskContract dependency graph
scheduler       deterministic runnable/waiting/retry logic
runtimeadapter  execution adapters
models          provider abstraction
capabilities    leases/action-resource-scope checks
policy          root/human/org typed rules
approvals       pending/ack/decision
completion      contracts/verifiers/verified transition
tools           capability-gated tool execution
api             REST/SSE/CLI/UI DTOs
```

## 3. Same process does not mean bypass

Agent A to Agent B still goes:

```text
EventDraft -> Event Gateway -> persist -> projection/router -> recipient
```

Do not call another agent's implementation directly to bypass persistence/policy.

Ordinary internal software calls such as scheduler-to-repository may use normal typed interfaces. Do not fake network boundaries inside the monolith.

## 4. Database ownership

A shared SQLite database is acceptable. Modules own their tables/queries logically and should not casually query another module's tables.

## 5. Extraction rule

Extract a service only for demonstrated need such as:

- fault isolation;
- hostile-code security isolation;
- independent scale;
- GPU/resource specialization;
- externally exposed federation boundary;
- independent deployment.

Do not pre-split by conceptual “plane.”
