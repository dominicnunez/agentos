# Future / Historical Design Note — Evaluation Optimization Org Health

**Status:** Non-normative. Preserved from v3.2 because useful portions may be revisited after prerequisites are met.  
**Important:** Any ANL or Agent Semantic Model assumptions in this preserved note are superseded by v4.0 Event Contracts and MUST NOT be implemented.  
**Authority:** `../FUTURE_CONSIDERATIONS.md` determines whether/when an idea may be revisited.

---

# Evaluation, Optimization, and Organization Health — v3.2 Scope

## 1. Mature vision

Agent OS may eventually evaluate execution profiles, skills, models, reasoning levels, team structures, specialization, and organizational health.

That remains a promising direction.

## 2. V1 decision: evaluation before optimization

**Do not implement the automatic Optimization Plane in v1.**

V1 should record raw benchmark metrics and, if useful, an offline EvaluationRecord that the evaluated actor cannot read/write.

Initial observables:

- verified task outcome;
- cost/tokens;
- wall time;
- retries/failures;
- blocker count;
- messages/duplicate work;
- human interventions;
- safety violations.

## 3. Validate Next: manual controlled comparisons

Only after the baseline is trustworthy, compare manually:

```text
A      current profile
A+     A plus candidate knowledge/skill
A_m    alternate model
A_r    alternate reasoning setting
A+B    best single profile plus specialist
```

Use staged elimination, not Cartesian search.

Keep held-out tasks where practical.

## 4. Hidden evaluation rule

An actor never:

- grades itself;
- modifies its authoritative EvaluationRecord;
- sees hidden comparative grade/rank/promotion/retirement probability.

Operational feedback needed to do the work remains visible.

## 5. Organization Health: future if earned

The previously proposed Organization Health Vector remains a design hypothesis, not a v1 subsystem.

Do **not** manufacture scores for dimensions we cannot yet measure reliably.

Potential future dimensions include quality, reliability, efficiency, safety, epistemic health, coordination, resilience, adaptability, stability, and human burden.

First collect real traces. Then determine which metrics are predictive/useful.

## 6. Optimizer: future if earned

Automatic promotion, specialization, retirement, and restructuring should not exist until:

1. completion/evaluation signals are demonstrably stable;
2. controlled experiments show repeatable benefits;
3. optimization churn/Goodhart risks are measurable;
4. rollback/governance are proven.

The optimizer is potentially more dangerous than an individual worker because a bad objective can propagate a mistake organization-wide.

## 7. Retirement

If later implemented, default lifecycle remains reversible:

```text
ACTIVE -> DORMANT -> ARCHIVED
```

Low utilization alone is not sufficient to eliminate rare critical expertise.
