# Teams, Tasks, and Durability

## 1. Durable identity

`Agent` and `Team` are durable logical entities. `AgentExecution` is ephemeral.

The system may sleep with no LLM process alive and later resume from persisted state/events.

## 2. Team

V1 Team fields:

```text
ID
OrganizationID
Name
Mission?
MemberAgentIDs[]
Status
CreatedAt
```

Team state is a projection over events/tasks/artifacts, not a separate semantic world model.

## 3. Task

V1 Task:

```text
ID
GoalID
ParentID?
DependsOn[]
AssigneeType = AGENT | TEAM
AssigneeID
TaskContractVersion
Status
```

This graph is sufficient for V1 planning/dependencies.

## 4. TaskContract

Versioned immutable contract containing:

- objective;
- success criteria;
- hard constraints;
- forbidden effects/actions;
- allowed resources/capability requirements;
- budget;
- approval requirements;
- expected evidence/artifacts.

Changing the contract creates a new version. Workers cannot silently redefine success.

## 5. Blocked task

A worker that cannot continue publishes `TASK_BLOCKED`.

The delegating/governing actor may:

- provide information;
- rescope;
- split;
- reassign;
- cancel;
- create a new separately authorized assignment.

No ordinary worker self-service permission escalation.

## 6. Passive awareness

Messages/evidence are persisted first, routed to recipient projections, then surfaced at deterministic action boundaries.

No planner relay is required for every lateral communication.

## 7. Restart

After process restart:

- rebuild projections from events (or future checkpoints);
- preserve pending approvals;
- preserve inbox availability state;
- preserve Task status/dependencies;
- avoid duplicating consequential effects through idempotency/action records.
