# Future / Historical Design Note — Incident Response Operational Resilience

**Status:** Non-normative. Preserved from v3.2 because useful portions may be revisited after prerequisites are met.  
**Important:** Any ANL or Agent Semantic Model assumptions in this preserved note are superseded by v4.0 Event Contracts and MUST NOT be implemented.  
**Authority:** `../FUTURE_CONSIDERATIONS.md` determines whether/when an idea may be revisited.

---

# Incident Response and Operational Resilience — v3.2

## 1. V1 principle

Incident handling should initially **compose existing safety primitives**, not become a large standalone subsystem.

Required primitives:

- freeze;
- revoke capability;
- isolate/disable actor execution;
- preserve immutable evidence;
- snapshot/export relevant state;
- query provenance/authorization traces.

A human/operator workflow can compose these.

## 2. V1 emergency controls

At minimum:

- freeze external/consequential actions;
- stop new model/tool executions for an actor/team;
- revoke a capability lease;
- inspect ledger events around an incident.

Human emergency controls operate below model reasoning and are checked at time of action.

## 3. Blast radius

Do not build a dedicated graph database in v1.

Use stable event/message/artifact references to query:

- data accessed;
- messages sent;
- recipient/team;
- tasks/decisions affected;
- external actions caused.

Add optimized graph projections only if traces become too slow/complex.

## 4. Reversibility honesty

Distinguish:

```text
CONFIGURATION_REVERSIBLE
EFFECT_REVERSIBLE
```

Rolling back configuration cannot unsend a message or make disclosed information unknown.

## 5. Dormant work

Long-sleeping work should eventually revalidate task/policy/capability/environment before resuming. This can be introduced when the runtime actually supports long dormancy beyond the first benchmark.

## 6. Production resilience later

Hash-linked checkpoints, external encrypted backups, restore drills, distributed ordering, and split-brain handling are production-hardening work, not v1 thesis requirements.
