# v4.1.1 Implementation Hardening

## 1. Status

This is a patch to v4.1. It does **not** introduce a new architectural plane or major subsystem.

The purpose is to remove implementation ambiguity discovered during design review.

## 2. Pattern candidacy: three occurrences is a trigger, not truth

Default V1 rule:

> **Three related occurrences are the minimum default evidence count for creating a repeated-pattern candidate.**

This means:

```text
occurrence #1 -> experience
occurrence #2 -> repeated observation
occurrence #3 -> repeated-pattern candidate may be proposed
```

It does **not** mean:

```text
three occurrences -> ACTIVE organizational knowledge
```

The threshold is configurable by task class/consequence. High-consequence business, safety, financial, legal, or public-facing claims may require more evidence before even becoming a promotion candidate.

Core distinction:

> **Frequency determines when a pattern deserves investigation. Evidence determines whether it deserves promotion.**

A repeated-pattern knowledge proposal must reference at least three concrete occurrence/event records under the default policy.

Automatic clustering/pattern discovery is not required in V1. A human or agent may propose the candidate; runtime validation checks its evidence references.

## 3. Promotion after pattern detection

Preferred flow:

```text
3+ related occurrences
        |
        v
PATTERN_CANDIDATE
        |
        v
subsequent observation and/or deliberate experiment
        |
        v
KNOWLEDGE_CANDIDATE
        |
        v
validation appropriate to consequence
        |
        v
ACTIVE / REJECTED / QUARANTINED
```

For deterministic domains, tests/environment checks should dominate.

For empirical business claims, controlled or repeated observations may be required.

For claims that remain fundamentally judgmental, do not falsely label them deterministic.

## 4. Nondeterministic judgment belongs to an operator

When objective verification is unavailable, an authorized **operator** makes the judgment.

An operator may be:

- an appropriately authorized agent for low/material internal judgment; or
- a human where consequence policy requires human authority.

Record:

```text
method
operator identity/type
evidence reviewed
time
scope
confidence/limitations if applicable
```

Existing Completion assurance categories map as:

```text
INDEPENDENT_ADJUDICATION -> agent/operator judgment
HUMAN_JUDGMENT          -> human operator judgment
```

The record must never imply that operator judgment is deterministic proof.

## 5. Auditing corrects institutional mistakes; it cannot guarantee none exist

Agent OS is allowed to be wrong.

The goal is not impossible perfect knowledge. The goal is to make institutional mistakes:

- visible;
- attributable;
- challengeable;
- auditable;
- reversible where practical;
- correctable before they compound.

Useful audit/correction telemetry includes:

```text
time_to_contradiction
time_to_audit_finding
time_to_correction
downstream_tasks_affected
repeat_failures_before_correction
stale_knowledge_usage_count
bad_skill_rollback_time
```

Do not optimize these into a single authoritative score in V1. Record raw measurements first.

## 6. Topology is empirical, not ideological

Agent OS does **not** assume Teams are better than single agents.

The system must be able to support and later experiment among execution structures such as:

```text
single agent
single agent + Skill
single agent + verifier
parallel independent agents
async Team
```

The Lab may compare these configurations for a task class.

The long-term objective is:

> **Use the smallest execution structure that produces the required verified outcome at acceptable cost, latency, reliability, and risk.**

A healthy Agent OS may assign most simple work to one agent.

## 7. Skills use defense in depth

Auditing is important but is not the sole defense against bad Skills.

V1 Skill safety relies on:

1. provenance;
2. explicit applicability/scope;
3. validation evidence;
4. version history;
5. capability/consequence enforcement at action time;
6. Completion Engine verification of work;
7. dependency/version revalidation triggers;
8. deterministic audits;
9. quarantine/supersession/rollback.

A Skill is procedural knowledge, not authority.

Bad procedures are possible just as in human organizations. Agent OS should make them easier to detect, trace, revise, and roll back.

## 8. Inference usage telemetry: best available evidence

Resource management must tolerate imperfect provider telemetry.

Use this source preference:

```text
1. official machine-readable API/usage endpoint
2. supported provider CLI/status command
3. other supported provider telemetry
4. estimate from Agent OS observed usage
5. conservative estimate
```

Local hardware telemetry may be read directly from the local runtime/system.

Every usage snapshot records:

```text
source
observed_at
confidence
remaining estimate/value if available
unit
reset/window information if available
basis/details
```

Unknown or estimated values remain explicitly uncertain.

## 9. CLI telemetry is an adapter concern

If a subscription provider exposes useful quota/rate-limit data only through a supported CLI, Agent OS may install/use that CLI through a provider-specific deterministic adapter.

Preferred flow:

```text
InferenceUsageAdapter
        |
        v
provider API / CLI / local telemetry
        |
        v
parse + normalize
        |
        v
UsageSnapshot
```

Do **not** make an LLM repeatedly open an interactive shell and reason over status output when deterministic software can invoke/parse it.

## 10. Cache/rate-limit telemetry collection

Usage polling itself may consume resources or create provider load.

Therefore:

- cache observations;
- use configurable minimum refresh intervals;
- refresh more aggressively only near meaningful thresholds/resets;
- immediately incorporate provider-reported rate-limit/reset events;
- do not query before every inference by default.

The exact cadence is configuration/empirical tuning, not a hard architectural constant.

## 11. Patch invariants

v4.1.1 adds no permission to:

- auto-promote three occurrences into trusted knowledge;
- let an operator judgment masquerade as deterministic proof;
- prefer Teams regardless of evidence;
- let Skills bypass capabilities;
- let audits silently rewrite trusted state;
- invent exact provider quota values when telemetry is uncertain;
- let agent content create trusted resource telemetry.
