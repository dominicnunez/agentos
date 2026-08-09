# Vision and Product Direction

## 1. Product thesis

Agent OS is infrastructure for building and operating persistent artificial organizations.

It does **not** need to be sold as a standalone product to be valuable. A successful Agent OS can be an internal capability used to build and operate multiple products, services, or businesses.

```text
                         Agent OS
                            |
          +-----------------+-----------------+
          |                 |                 |
          v                 v                 v
    Organization A    Organization B    Organization C
     product/service   product/service   product/service
```

The success criterion is operational leverage, not framework adoption.

## 2. Durable abstractions

### Organization

Owns mission/goals, teams, policy boundary, budget envelope, and durable history.

### Team

A durable collaborative actor with members, mission, task participation, inbox/state projections, and history.

### Agent

A durable logical identity. It can survive model/provider/profile changes.

### AgentExecution

An ephemeral invocation/session used to advance an Agent or Team. No LLM process needs to stay alive while the organization sleeps.

## 3. Human UX

Humans should be able to express goals in ordinary language and inspect:

- organization/team structure;
- active/waiting work;
- messages/events;
- blocked work;
- approvals;
- verified completion;
- audit/history;
- cost/resource usage.

Technical IDs and event schemas belong in Advanced/Audit views.

## 4. Build organizations, not bureaucracy

Human company metaphors are useful UX, not mandatory machine structure.

Do not create CEO/VP/manager hierarchies merely because human companies have them. The system should prefer the smallest useful number of persistent actors and coordination layers.

## 5. Long-term vision

If earned through real operational evidence and controlled evaluation, Agent OS may later support:

- richer institutional knowledge;
- SOP/skills;
- organization evaluation/health;
- controlled organizational experiments;
- model/reasoning/team optimization;
- research/self-improvement organizations;
- external federation;
- multi-organization operation.

Those are future capabilities, not V1 assumptions. See `../future/FUTURE_CONSIDERATIONS.md`.
## v4.1.2 — Work-first operating philosophy

Agent OS is infrastructure for getting real organizational work done.

The system does not seek to maximize the number of Agents, Teams, model calls, or agentic steps.

Actual objectives determine whether work is handled by normal software, tools/APIs, one Agent, a Team, a human operator, or a mixture.

Persistent Agents represent durable responsibility/experience; they need not invoke an LLM for every step.
## v4.2 — Hermes operator relationship

Agent OS may remain headless/infrastructure-oriented while Hermes serves as a human-facing operator/chief-of-staff layer.

```text
Human
  -> Hermes
      -> A2A Operator Gateway
          -> Agent OS
              -> businesses/organizations
```

This avoids duplicating Hermes interaction surfaces while keeping Agent OS authority, audit, persistence, and organizational execution independent.
