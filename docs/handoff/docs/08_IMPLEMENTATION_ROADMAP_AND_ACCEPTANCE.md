# Implementation Roadmap and Acceptance

## Phase 0 — repository/kernel skeleton

- Go modular monolith;
- module boundaries/Archguard;
- SQLite migrations;
- deterministic fake adapter;
- event ledger append/read/replay;
- basic projections.

## Phase 1 — actors/tasks/execution

- Organization, Team, Agent, AgentExecution;
- Goal/Task dependency graph;
- TaskContract;
- RuntimeAdapter;
- basic scheduler.

## Phase 2 — Event Contracts + async collaboration

- EventDraft/Event gateway;
- `MESSAGE`, `TASK_BLOCKED`, `EVIDENCE_PUBLISHED`, `RESULT_PUBLISHED`, `CANDIDATE_COMPLETE`;
- persist-before-availability;
- inbox/team/task projections;
- action-boundary message surfacing;
- priority policy.

## Phase 3 — authority and human boundary

- capabilities;
- authorization trace projection;
- no positive inheritance;
- blocked-task parent remediation;
- human consequence classifier;
- pending approval;
- freeze/revoke/time-of-use check.

## Phase 4 — completion/evidence

- CompletionContract;
- candidate completion;
- deterministic verifiers;
- runtime-attested evidence;
- verified completion state.

## Phase 5 — representative real work + measurement

Operate on representative actual organizational work.

For each real task/workflow:

- represent it with the Task dependency graph;
- use deterministic handlers/tools wherever sufficient;
- introduce AgentExecution/Team reasoning only where justified;
- record outcome, cost, latency, blockers, human intervention, and inference usage;
- preserve replayable evidence where practical.

Controlled comparisons belong under `VALIDATE_NEXT` when a real task class creates uncertainty about execution structure.

## V1 acceptance

V1 is accepted only when:

1. Team/Agent identity survives restart;
2. an agent can send a lateral message without planner relay;
3. recipient sees it at next applicable action boundary;
4. no recipient sees a message whose persistence failed;
5. a blocked worker returns control without authority expansion;
6. a child assignment receives no unintended positive capability inheritance;
7. human-required action waits indefinitely until decision;
8. acknowledgement cannot approve;
9. freeze/revoke prevents action at time of use;
10. agent text cannot forge approval/identity/completion/runtime attestation;
11. `CANDIDATE_COMPLETE` cannot bypass Completion Engine;
12. duplicate delivery cannot duplicate a consequential effect;
13. restart preserves pending work/approval/inbox state;
14. operational telemetry records verified outcome, execution mechanism, cost, time, model use (if any), messages, blocks, retries, and human intervention.
15. every model execution has an accurate ExecutionContextManifest;
16. deterministic ToolOutcome/postcondition failures cannot be hidden by a success string;
17. known safe deterministic recovery is attempted before cognitive recovery where implemented;
18. Hermes can discover/submit/continue work through the pinned A2A Operator Gateway integration;
19. Hermes/A2A identity cannot bypass capability or human-approval boundaries;
20. protected external effects use exact effect-bound approval and durable EffectObligation/reconciliation before production use.

## Stop rule

Do not build `VALIDATE_NEXT` or `FUTURE_IF_EARNED` merely because V1 code compiles. First operate on representative real work, collect baseline operational measurements, and run the safety acceptance suite.


## v4.1.1 sequencing

After the core ledger/events/tasks/capability/completion path works, add before declaring V1 complete:

1. versioned KnowledgeRecord + simple retrieval;
2. instruction/reference Skill package + candidate/activation/versioning;
3. deterministic AuditService + findings;
4. InferencePool + basic deterministic resource selection/usage telemetry.

Then operate the core runtime on representative real work. Build workflows and Agent/Team structures from what the work actually requires. Record outcomes, cost, latency, blockers, model usage, and human intervention. Use controlled replays or Lab experiments on real task classes when evidence is needed to choose between deterministic workflows, single Agents, Skills, verifiers, parallel attempts, or Teams. Build full Lab orchestration, LLM AuditWorker, semantic retrieval, executable skill assets, and resource forecasting only when operational needs and measurements justify them.
## v4.1.2 sequencing rule

Do not make a benchmark the product roadmap.

As soon as core safety/durability/completion primitives are usable:

1. select representative actual work;
2. represent it as a Task dependency graph;
3. use deterministic handlers/tools wherever sufficient;
4. inject AgentExecution/Team reasoning only where justified;
5. record operational evidence;
6. use controlled comparisons only when a real execution-structure question exists.

This produces an organization that learns how to do its own work rather than a research harness looking for tasks.
## v4.2 sequencing additions

Before handing Agent OS to Hermes as operator:

1. ExternalActor identity/capability mapping;
2. minimal A2A v1.0 Agent Card/server;
3. A2A work -> Intent/Goal/Task translation;
4. progress/blocked/input/result mapping;
5. pinned Hermes interoperability tests.

Before trusting model/tool traces operationally:

6. ExecutionContextManifest;
7. structured ToolOutcome + deterministic recovery.

Before enabling consequential external writes:

8. effect-bound approval fingerprints;
9. EffectObligation outbox/recovery/reconciliation;
10. simple SecretSource seam where credentials are needed.

These additions do not authorize full federation or general plugin infrastructure.
