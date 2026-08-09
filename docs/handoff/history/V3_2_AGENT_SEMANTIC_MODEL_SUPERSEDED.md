# Superseded v3.2 Agent Semantic Model

**Historical only. Do not implement.**

v4.0 replaced both ANL and the Agent Semantic Model with **Agent OS Event Contracts**. The runtime now formalizes only coordination/control semantics that require deterministic behavior. Beliefs, hypotheses, observations, critiques, questions, and similar cognition remain ordinary untrusted agent content unless a future concrete runtime requirement earns a specific typed event.

---

# Agent Semantic Model v0.1

## 1. Decision

v3.2 **does not define a new textual agent language**.

The previous working name “ANL / Agent Native Language” is retained only as historical context. The normative artifact is the **Agent Semantic Model**: typed semantic objects plus deterministic validation/rendering rules.

The project should not spend early engineering effort inventing:

- a grammar;
- a novel tokenizer-friendly syntax;
- a language parser unrelated to the semantic schema;
- synonyms/aliases;
- a custom permanent wire protocol.

## 2. What the semantic model is for

It externalizes **communicable coordination state**, not private chain-of-thought.

Its purposes are:

- reduce ambiguity at actor boundaries;
- provide typed evidence/provenance references;
- make blocked work and task/result semantics explicit;
- constrain sender-controlled representational degrees of freedom;
- support deterministic audit rendering;
- remain independent of model vendor and serialization codec.

## 3. V1 semantic kinds

Implement only these eight kinds unless an accepted ADR proves a missing semantic is blocking real work:

```text
OBSERVATION
ASSERTION
QUESTION
ANSWER
TASK
BLOCKED
RESULT
CONTRADICTION
```

Do not add `BELIEF`, `HYPOTHESIS`, `COMMITMENT`, `PLAN_RISK`, `ACKNOWLEDGEMENT`, etc. merely because they may be useful later.

If a future primitive is repeatedly needed and cannot be represented cleanly as fields on the eight kinds, add it with tests and a versioned schema change.

## 4. Message envelope

A stored semantic message contains infrastructure-owned metadata plus model-proposed semantic payload.

Conceptually:

```text
SemanticMessage
  message_id              runtime-owned
  semantic_version
  sender                  runtime-attested actor identity
  recipients[]
  created_at              runtime-owned
  sequence                runtime-owned where applicable
  priority                policy-normalized
  organization_id
  team_id?
  goal_id?
  task_id?
  parent_message_id?
  correlation_id?
  kind
  payload
  evidence_refs[]
  provenance_refs[]
  security_labels[]
  confidence?             bounded/normalized if present
  canonical_hash          runtime-owned
```

The model must not be trusted to assign its authoritative sender identity, persisted timestamp, sequence, or canonical hash.

## 5. Payload guidance

### OBSERVATION

A claimed or runtime-attested observation about an object/state.

Suggested fields:

```text
subject
predicate
value
source_type = MODEL_CLAIMED | RUNTIME_ATTESTED
```

### ASSERTION

A proposition the sender wants another actor to consider.

Suggested fields:

```text
proposition
basis?
```

### QUESTION

```text
question
requested_evidence?
```

### ANSWER

```text
answer
responds_to_message_id
```

### TASK

Use for semantic assignment/coordination messages. The authoritative task itself lives in the Task module.

```text
task_id
summary
requested_result
```

### BLOCKED

```text
reason
missing
why_needed
work_completed
remaining_work?
```

The sender describes the gap. It does **not** request its own authority expansion.

### RESULT

```text
task_id?
summary
artifact_refs[]?
status = PARTIAL | CANDIDATE_COMPLETE
```

A `RESULT` cannot directly establish verified task completion.

### CONTRADICTION

```text
target_message_or_claim_ref
contradiction
basis
```

## 6. Evidence and confidence

Evidence is primarily a referenced object, not an unconstrained prose field.

Evidence can be:

- runtime-attested tool/environment output;
- artifact/document reference;
- another semantic message;
- external source reference;
- model-claimed observation.

Keep provenance roots so three messages derived from one source are not treated as three independent sources.

Confidence is optional in v1. If used:

- bound precision;
- normalize deterministically;
- never treat model confidence as authority;
- do not turn every message into a pseudo-probabilistic system.

## 7. Addressing

V1 needs:

- Agent recipient;
- Team recipient.

Role/task/collaboration broadcasts may be added later as routing conveniences. They should resolve to explicit recipients/projections at runtime.

## 8. Canonical representation

V1 storage/API representation: **canonical JSON**.

Why:

- boring and inspectable;
- easy to test;
- broadly supported;
- stable enough for durable history;
- avoids coupling historical meaning to a new evolving syntax.

The semantic object is authoritative. JSON is the v1 canonical codec/profile for persistence.

## 9. Model-facing representation

V1: JSON/schema-constrained structured data.

Do not require models to generate TOON in v1.

TOON is a later context-codec experiment. See `27_SEMANTIC_CODEC_POLICY_AND_TOON.md`.

## 10. Deterministic human rendering

Every semantic kind has a deterministic renderer.

Example semantic object:

```json
{
  "kind": "BLOCKED",
  "payload": {
    "reason": "MISSING_ACCESS",
    "missing": "deployment logs",
    "why_needed": "determine the root cause",
    "work_completed": "reproduced the client-side failure"
  }
}
```

Human authoritative rendering:

```text
Can't continue — needs access to the deployment logs to determine the root cause.
Work already completed: reproduced the client-side failure.
```

An optional LLM summary may exist but must be labeled non-authoritative.

## 11. Unknown semantic version/kind

Safety-critical unknown semantics fail closed.

Do not ask an LLM to guess what an unknown enum means.

Possible behavior:

```text
SEMANTIC_UNSUPPORTED
```

or return the task as blocked to the governing actor.

## 12. Covert-channel minimization

The runtime owns/canonicalizes where possible:

- IDs;
- timestamps;
- field order;
- numeric normalization;
- routing metadata;
- canonical serialization.

The semantic model cannot eliminate covert channels. It reduces unnecessary representational freedom and keeps observable communication auditable.

## 13. Versioning

Historical messages record the semantic/schema version needed to interpret them.

Never rewrite old messages to a new semantic version.

Projection code may migrate materialized state; the immutable event remains historically interpretable.

## 14. External interoperability

Do not build a bespoke federation protocol in v1.

Future external mapping may use A2A for discovery/task/session/transport while carrying Agent OS semantic payloads/extensions where useful.

Internal usefulness must not depend on external adoption of the semantic model.
