# Future / Historical Design Note — Memory Knowledge Sops Skills

**Status:** Non-normative. Preserved from v3.2 because useful portions may be revisited after prerequisites are met.  
**Important:** Any ANL or Agent Semantic Model assumptions in this preserved note are superseded by v4.0 Event Contracts and MUST NOT be implemented.  
**Authority:** `../FUTURE_CONSIDERATIONS.md` determines whether/when an idea may be revisited.

---

# Memory, Knowledge, SOPs, and Skills — v3.2 Scope

## 1. Mature vision

Agent OS should eventually retain useful organizational knowledge, validated procedures, and perhaps executable skills with provenance and revalidation.

## 2. V1 decision

Do **not** build a multi-layer memory/SOP/skill platform in v1.

V1 durability covers:

- authoritative events;
- task/team state projections;
- artifact references;
- context selected from existing state.

That is enough for the core collaboration benchmark.

## 3. Validate Next: one KnowledgeRecord

If persistent learned knowledge becomes necessary, begin with one abstraction:

```text
KnowledgeRecord
  ID
  Kind = FACT | LESSON | PROCEDURE
  ContentRef / structured content
  ProvenanceRefs[]
  Confidence?
  Scope
  SecurityLabels[]
  CreatedAt
  LastVerifiedAt?
  Status = CANDIDATE | ACTIVE | QUARANTINED | RETIRED
```

Do not create separate databases/types for episodic memory, semantic memory, lessons, SOPs, and skills until their behavior truly diverges.

## 4. Future promotion

Possible later progression:

```text
experience
 -> candidate knowledge
 -> validation
 -> active knowledge
 -> candidate procedure
 -> procedure tests
 -> possible executable skill
```

An executable skill never bypasses normal capabilities, completion checks, or audit.

## 5. Poisoning/staleness

Any persistent knowledge system must eventually track provenance, security scope, environment/version applicability, contradiction, and revalidation.

External content never becomes trusted knowledge merely because a model summarizes it.

## 6. Sensitive payloads

Keep sensitive content separable from immutable structural history. The ledger may preserve that an artifact existed/was accessed/influenced a decision even when the underlying payload is later removed under policy.
