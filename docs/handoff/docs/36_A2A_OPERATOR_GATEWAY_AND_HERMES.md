# A2A Operator Gateway and Hermes Handoff — v4.2

## 1. Decision

A minimal **A2A v1.0 Operator Gateway** is V1 CORE because Hermes Agent is a concrete intended external operator/manager of Agent OS.

A2A is an external interoperability boundary.

> **Internal Agent OS communication remains Event Contracts.**

```text
Human
  |
  v
Hermes Agent
  |
  | A2A v1.0
  v
Agent OS A2A Operator Gateway
  |
  | authenticate -> authorize -> translate
  v
Intent / Goal / Task / Event Contracts / Ledger
```

Do not replace the internal Event Gateway, Task model, or Event Contracts with A2A objects.

## 2. External actor identity

An authenticated A2A peer maps to an Agent OS `ExternalActor`.

Example:

```text
ExternalActorID: hermes-primary
Protocol: A2A
Credential/identity binding: configured
OrganizationID: org-1
Capability refs:
  submit_work
  read_visible_status
  provide_task_input
  read_visible_artifacts
```

A2A connectivity does not imply administrator authority.

> **A2A establishes/communicates peer identity; Agent OS policy determines what that peer may cause.**

Hermes remains subordinate to all capability, data, approval, freeze/revoke, and consequence rules.

## 3. V1 inbound management surface

The V1 gateway should support the minimum operator path:

1. Agent Card discovery;
2. authenticated message/task submission;
3. map submitted work to an `IntentEnvelope`;
4. create or continue the corresponding internal Goal/Task work;
5. expose task status/progress;
6. expose result Artifacts;
7. request/provide missing task input;
8. translate internal `TASK_BLOCKED` into an input-needed/status update;
9. maintain correlation between A2A external task/context IDs and internal Intent/Goal/Task IDs.

## 4. A2A Task != Agent OS Task

Do not make the models identical.

```text
A2A Task
= external interoperability/session object

Agent OS Goal/Task DAG
= internal organizational work model
```

One A2A task may map to:

```text
A2A task
  -> IntentEnvelope
  -> Goal
      -> Task A
      -> Task B
          -> Task B1
          -> Task B2
      -> Task C
```

The gateway reports aggregate/progress status externally without leaking internal data the peer is not authorized to see.

## 5. Work-level interface, not privileged control API

A2A should primarily express:

- desired work;
- task continuation;
- new information;
- status;
- results/artifacts.

Do not expose broad remote operations such as:

```text
grant arbitrary capability
rewrite ledger
activate skill directly
change root policy
rewrite knowledge directly
disable human freeze
```

Hermes may request work whose normal execution happens to require these transitions; normal internal authorization/governance remains authoritative.

## 6. Blocked work

Example:

```text
internal:
TASK_BLOCKED
reason = MISSING_INPUT
missing = target customer segment

A2A external status/message:
"Can't continue — target customer segment is required."
```

Hermes may supply the missing information if authorized/known.

The supplied input is persisted with source provenance:

```text
source_external_actor = hermes-primary
a2a_task/context = ...
```

The parent Task may then resume.

## 7. Human approval boundary

Hermes is not a substitute for required human approval.

Example:

```text
Hermes requests production deployment
    ->
Agent OS consequence classifier
    ->
HUMAN_APPROVAL
    ->
wait until authorized human decides
```

Hermes may surface the pending decision to the human.

It may not self-approve because it is the operator.

## 8. A2A conformance/interoperability testing

V1 acceptance includes a pinned Hermes integration configuration/release and tests. The initial compatibility target from this handoff is Hermes Agent **v0.20.0** with A2A v1.0; upgrades require rerunning conformance/integration tests:

```text
Hermes discovers Agent OS Agent Card
Hermes submits work
Agent OS persists Intent/Task
Agent OS reports progress
Agent OS reports blocked/input-needed state
Hermes supplies input
Agent OS resumes
Agent OS returns result Artifact
```

Treat Hermes/A2A implementation bugs as adapter/conformance issues, not reasons to weaken Agent OS internals.

## 9. V1 directionality

Required:

```text
Hermes / external A2A peer -> Agent OS Operator Gateway
```

Agent OS may respond/status-stream as required by that task/session.

Not required in V1:

- Agent OS discovering arbitrary external A2A workers for delegation;
- remote agent marketplace;
- dynamic federation;
- cross-organization trust negotiation.

Those remain later features.

## 10. Hermes RuntimeAdapter remains separate

Using Hermes as an **internal worker runtime** is a different capability from Hermes operating Agent OS externally.

```text
External operator:
Hermes --A2A--> Agent OS

Possible later internal worker:
Agent OS --RuntimeAdapter--> Hermes execution
```

The second remains `VALIDATE_NEXT` unless explicitly promoted.

## 11. Protocol isolation

The gateway is an adapter module.

Suggested Go boundary:

```go
type OperatorGateway interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
}

type ExternalWorkSubmission struct {
    ExternalActorID string
    ExternalTaskID  string
    Content         []Part
}
```

Do not make core domain packages depend on A2A-specific wire types.

Translate at the boundary into internal application commands/Events.
