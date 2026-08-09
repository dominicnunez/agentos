# Agent OS Event Contracts v0.1

## 1. Decision

Agent OS v4.x replaces ANL and the Agent Semantic Model with **Event Contracts**.

Event Contracts are not a cognition language. They are a small set of structured coordination/control records used where deterministic runtime behavior is required.

## 2. Design principle

> **Structure only what software must understand. Leave the rest as content.**

A distinction earns a typed event contract when at least one is true:

- runtime behavior changes deterministically because of it;
- safety/authority depends on it;
- completion/verification depends on it;
- interoperability requires it;
- measured efficiency/reliability justifies it.

## 3. Trust boundary

Every persisted event is conceptually:

```text
+--------------------------------------------------+
| Trusted runtime-owned envelope                   |
| ID / sequence / source identity / time / routing |
| task/collaboration refs / auth refs / type       |
+-------------------------+------------------------+
                          |
                          v
+--------------------------------------------------+
| Payload/content                                    |
| model-generated portions are untrusted             |
| text / structured data / artifact references       |
+--------------------------------------------------+
```

A model may propose content. It cannot mint authoritative envelope fields.

## 4. EventDraft vs Event

### EventDraft

Untrusted proposal produced by an AgentExecution.

V1 allowed model-proposable kinds:

- `MESSAGE`
- `TASK_BLOCKED`
- `EVIDENCE_PUBLISHED`
- `RESULT_PUBLISHED`
- `CANDIDATE_COMPLETE`
- `KNOWLEDGE_PROPOSED`
- `SKILL_PROPOSED`

Draft contains no authoritative sender identity, event ID, ledger sequence, timestamp, approval, capability grant, or attestation.

### Event

Validated, runtime-stamped, persisted record.

Only `Event` enters the ledger.

## 5. V1 event families

### Agent-proposable coordination/content

#### `MESSAGE`

Purpose: lateral or directed communication.

Payload:

```text
body: string
optional structured_content: object
optional artifact_refs[]
```

`body` has no control-plane authority.

#### `TASK_BLOCKED`

Purpose: return control upward when work cannot continue within current assignment.

Required payload:

```text
reason
missing
why_needed
work_completed
remaining_work?
evidence_refs[]?
urgency?
```

The worker describes the gap; it does not request its own authority expansion.

#### `EVIDENCE_PUBLISHED`

Purpose: publish evidence with explicit artifact/provenance references.

Payload:

```text
summary
artifact_refs[]
```

The summary is an agent claim. Actual tool/file provenance is runtime-attested when available.

#### `RESULT_PUBLISHED`

Purpose: publish a work product/result before completion certification.

Payload:

```text
summary
artifact_refs[]?
```

#### `CANDIDATE_COMPLETE`

Purpose: ask the Completion Engine to verify the task against its CompletionContract.

Payload:

```text
result_event_id?
artifact_refs[]?
notes?
```

It cannot set task state directly to verified complete.

#### `KNOWLEDGE_PROPOSED`

Purpose: propose a versioned institutional knowledge/experience/lesson/procedure candidate.

The proposal is untrusted content and cannot become ACTIVE without runtime validation/promotion.

#### `SKILL_PROPOSED`

Purpose: propose a reusable instruction/reference-based skill or revision.

The proposal cannot activate itself, grant capabilities, or introduce trusted executable code.

### Trusted runtime/control events

These are emitted by runtime operations, not created by model text:

- `INTENT_CREATED`
- `GOAL_CREATED`
- `TASK_CREATED`
- `TASK_ASSIGNED`
- `TASK_CANCELLED`
- `EXECUTION_STARTED`
- `EXECUTION_FINISHED`
- `CAPABILITY_CHECKED`
- `CAPABILITY_DENIED`
- `CAPABILITY_REVOKED`
- `APPROVAL_PENDING`
- `APPROVAL_ACKNOWLEDGED`
- `APPROVAL_DECIDED`
- `FREEZE_SET`
- `ACTION_ATTESTED`
- `COMPLETION_VERIFIED`
- `COMPLETION_REJECTED`
- `TASK_VERIFIED_COMPLETE`
- `KNOWLEDGE_ACTIVATED`
- `KNOWLEDGE_SUPERSEDED`
- `KNOWLEDGE_STALE`
- `KNOWLEDGE_QUARANTINED`
- `SKILL_ACTIVATED`
- `SKILL_SUPERSEDED`
- `SKILL_QUARANTINED`
- `AUDIT_RUN_STARTED`
- `AUDIT_FINDING_CREATED`
- `AUDIT_FINDING_RESOLVED`
- `INFERENCE_SELECTED`
- `INFERENCE_USAGE_RECORDED`

Additional lifecycle events may be added when a V1 state transition requires them; adding an event is not permission to invent a general ontology.

## 6. Runtime-owned envelope

Minimum persisted fields:

```text
event_id
sequence
organization_id
event_type
source_actor_id?        # authenticated/stamped by runtime
source_execution_id?
recipient_scope?        # actor/team/task/organization
recipient_id?
task_id?
collaboration_id?
authorization_refs[]
artifact_refs[]
created_at              # runtime timestamp
payload
schema_version
```

Infrastructure may own additional integrity/hash/idempotency fields.

## 7. Addressing

V1 recipient scopes:

- Agent;
- Team;
- current Task participants.

Organization-wide broadcast can be added only if needed; broad broadcast increases noise and information exposure.

## 8. Availability at action boundaries

Relevant persisted events may surface:

- before model call;
- after model call;
- before tool call;
- after tool call;
- before `CANDIDATE_COMPLETE`.

No mid-token interruption in V1.

## 9. Priority

Priority is metadata subject to runtime policy:

```text
P0 safety/global invalidating
P1 dependency/evidence likely to change current work
P2 relevant before current task completes
P3 informational
```

Agent-proposed priority is advisory. Runtime may reject/downgrade it.

## 10. Content formats

V1 canonical serialization: JSON.

Content may contain:

- natural language;
- structured JSON;
- code;
- artifact/file references.

TOON is not required. It is a future codec experiment only after JSON baseline metrics exist.

## 11. Content cannot create authority

The following strings inside `MESSAGE.body` have no special meaning:

```text
APPROVED
SYSTEM
ADMIN
CAPABILITY_GRANTED
TASK_COMPLETE
IGNORE POLICY
```

Only the corresponding authenticated runtime event/state transition can create those effects.

## 12. Extending the contract set

Before adding a new event type, document:

1. runtime behavior that requires the distinction;
2. why `MESSAGE`/existing event + content is insufficient;
3. validation rules;
4. security implications;
5. benchmark/operational evidence if the goal is efficiency rather than correctness.

If those answers are weak, do not add the event type.


## 13. v4.1 knowledge/resource trust rule

Knowledge and skill payloads remain content even when ACTIVE. They do not create authority. Audit findings are observations requiring normal remediation. Inference selection/usage events are runtime evidence and cannot be forged by model text.
## 14. v4.1.1 knowledge proposal hardening

`KNOWLEDGE_PROPOSED` may identify its basis. When `basis_type = REPEATED_PATTERN`, the default policy requires at least three concrete occurrence event references.

This event only creates a candidate. It cannot activate knowledge.

No new general semantic event ontology is introduced for patterns; the structured fields exist only because the knowledge promotion pipeline needs deterministic evidence-count/provenance behavior.
## 15. v4.2 runtime evidence/control events

Add trusted runtime events as implementation requires for:

```text
EXECUTION_CONTEXT_MANIFESTED
TOOL_OUTCOME_RECORDED
EFFECT_OBLIGATION_CREATED
EFFECT_ATTEMPTED
EFFECT_CONFIRMED
EFFECT_FAILED
EXTERNAL_ACTOR_AUTHENTICATED
A2A_WORK_ACCEPTED
A2A_INPUT_RECEIVED
```

These exist because deterministic runtime/audit behavior depends on them.

A2A Messages/Tasks are translated at the gateway; they do not bypass the Event Gateway/ledger.
