# Inference Resource Management — v4.1.1

## 1. Decision

A minimal deterministic **Inference Resource Manager** is V1 CORE.

Inference access is an organizational resource. It does not belong to an Agent identity.

> **Agent intelligence != inference entitlement.**

## 2. Why this is core

Persistent organizations may have fundamentally different inference resources simultaneously:

- subscription allowance with reset/rolling windows;
- metered API with monetary/token cost;
- local GPU/accelerator capacity that renews continuously;
- multiple providers/subscriptions with different strengths and limits.

The organization must stay operational while using these resources effectively.

## 3. InferencePool

V1 supports access modes:

```text
SUBSCRIPTION
METERED_API
LOCAL
```

Suggested fields:

```text
PoolID
Provider
AccessMode
AllowedModels[]
Availability
ConcurrencyLimit?
DataPolicy
EstimatedRemaining?
RemainingConfidence?
ResetAt/Window?          subscription
MeteredBudgetRemaining?  API
LocalCapacity?           GPU/queue/concurrency
ReservePolicy
```

Provider quota may be exact, approximate, or opaque. Track confidence/basis rather than pretending uncertain allowance is exact.

## 4. Resource request

An Agent requests execution, not a preferred entitlement:

```text
TaskClass
MinimumCapability/Quality requirements
Urgency
DataClass
Latency constraint?
Task/Organization priority
```

Selection order:

1. hard capability/policy/data constraints;
2. feasible inference pools/models;
3. reserve/availability/resource policy;
4. simple deterministic selection rule.

Do not optimize cost/quality among unauthorized or technically incapable choices.

## 5. Resource economics

### Perishable capacity — subscription
Unused capacity may expire/reset. Conserve enough reserve but avoid systematic waste.

### Monetary capacity — metered API
Unused budget retains value. Do not spend merely because a calendar period is ending unless human policy explicitly treats it that way.

### Renewable capacity — local compute
Idle local capacity has low opportunity cost but still has queue/power/thermal/resource limits.

## 6. V1 policy

Keep selection simple:

- minimum reserve per subscription pool;
- maximum metered spend;
- priority classes;
- allowed/denied models by data class;
- local-first or provider-preference rules where configured;
- fallback/escalation only through explicit deterministic policy.

No LLM “treasurer agent” in V1.

## 7. Usage telemetry

For every execution record:

```text
PoolID
Provider/model/profile
TaskID/task class
started/ended
usage units/tokens if known
estimated subscription consumption if known
metered monetary cost if known
local duration/resource telemetry if known
completion outcome later linked
```

This evidence enables later routing experiments without baking guesses into V1.

## 8. Continuity reserve

Do not optimize solely for minimum spend or maximum uptime.

The policy goal is:

> Maximize useful organizational work while preserving a configurable continuity reserve for expected and unexpected higher-priority work.

## 9. Validate next

Once actual usage exists, test:

- burn forecasting;
- target consumption curves toward subscription reset;
- local -> cloud escalation;
- task-class model routing;
- cross-provider verifier selection;
- surplus-capacity Lab allocation;
- quality/cost/usage trade-offs.

## 10. Future if earned

- predictive demand/reserve optimization;
- multi-GPU scheduling;
- energy-aware scheduling;
- provider/subscription ROI analysis;
- purchasing recommendations.

Human approval remains required for expanding paid financial commitments/subscriptions/budgets.
## 11. Usage telemetry source ladder — v4.1.1

Resource management uses the best legitimate telemetry source available:

1. official machine-readable provider API/usage endpoint;
2. supported provider CLI/status command;
3. other supported provider telemetry;
4. Agent OS estimate derived from observed usage;
5. conservative estimate.

Local runtimes may expose direct local hardware/runtime telemetry.

Every normalized `UsageSnapshot` records source, observation time, confidence, unit/basis, remaining estimate/value if known, and reset/window information if known.

## 12. Provider CLI adapters

A provider-specific deterministic adapter may invoke a supported CLI when that is the best available usage source.

The LLM should not repeatedly operate an interactive terminal merely to read quota/status if deterministic Go code can invoke, parse, cache, and normalize the information.

Provider tooling is an adapter implementation detail; the Resource Manager consumes normalized snapshots.

## 13. Telemetry caching

Do not poll usage before every inference by default.

Telemetry adapters expose cache/freshness policy. Refresh may become more frequent near:

- configured reserve threshold;
- provider limit;
- reset window;
- anomalous burn.

Provider-returned rate-limit/reset data may update the snapshot immediately.

Exact refresh cadence remains configurable and should later be tuned from evidence.
## 14. v4.1.2 — Inference is downstream of work-mechanism selection

The Resource Manager does not answer whether a Task should use an LLM.

Work orchestration first determines that model capability is justified.

Only then does the Resource Manager select among feasible subscription/API/local pools/models.

```text
work requirement
 -> deterministic/tool sufficient? yes -> no inference
 -> no
 -> model capability justified?
 -> yes
 -> Resource Manager selects inference pool/model
```

This prevents model-routing sophistication from increasing unnecessary model usage.
