# Versioned Institutional Knowledge and Skills — v4.1.1

## 1. Decision

Persistent institutional learning is **V1 CORE**.

The system does not build a general “AI memory platform.” It implements the minimum durable, auditable knowledge layer required for an Agent/Team/Organization to learn from experience and retain that learning when the underlying LLM, provider, or ExecutionProfile changes.

> **Persist experience cheaply; promote knowledge cautiously.**

## 2. Why this is core

A durable Agent identity that loses useful lessons whenever its model changes is not meaningfully durable.

```text
Durable Agent / Team / Organization
          |
          +-- task/event history
          +-- validated institutional knowledge
          +-- versioned reusable skills
          |
          +-- ExecutionProfile may change independently
                Model A -> Model B -> Local Model C
```

The accumulated organizational experience belongs to the logical organization, not the model vendor.

## 3. Ledger versus knowledge

```text
Event Ledger                  Knowledge Store
------------                  ---------------
what happened                 what we currently retain/learn
immutable history             versioned current institutional knowledge
raw evidence                  distilled reusable content
source of provenance          projection/curation over history
```

Knowledge must link back to ledger events/artifacts whenever practical.

## 4. V1 knowledge types

Use common storage/versioning infrastructure but preserve different semantics:

### EXPERIENCE
Historical observation about a particular task/incident.

Example: “Task 188 failed because the vendor endpoint returned 410.”

### LESSON
Generalized advice inferred from one or more experiences.

Example: “Verify the supported API version before deployment.”

### KNOWLEDGE
A current claim about the environment/domain.

Example: “Payment API v5 is the current production API.”

### PROCEDURE
A reusable human/model-readable procedure.

Example: “Steps to deploy PaymentService safely.”

These are not identical. They may later receive different validation/expiry behavior.

## 5. V1 lifecycle

```text
CANDIDATE
   |
   | validation/corroboration appropriate to risk
   v
ACTIVE
   |
   +--> SUPERSEDED    new version replaces it
   +--> STALE         applicability may no longer hold
   +--> QUARANTINED   suspected unsafe/incorrect/poisoned
```

Do not overwrite prior versions.

## 6. Versioning

A stable Knowledge ID has monotonic versions:

```text
KNOW-42 v1
  -> KNOW-42 v2 minor correction
  -> KNOW-42 v3 major rewrite
```

Suggested change class:

- `CORRECTION`
- `MINOR_REVISION`
- `MAJOR_REVISION`
- `SUPERSEDE`

Historical versions remain auditable even after they are no longer active.

## 7. Required V1 fields

```text
KnowledgeID
Version
Type
Scope                 AGENT | TEAM | ORGANIZATION
Status
Title/Summary
Content
Applicability?        simple tags/text initially
ProvenanceEventRefs[]
EvidenceArtifactRefs[]
ProposedBy
CreatedAt
LastVerifiedAt?
SupersedesVersion?
ChangeClass?
ChangeReason?
```

## 8. Knowledge is not authority

Knowledge cannot:

- grant a capability;
- approve an action;
- change a TaskContract;
- override policy;
- modify the Constitution;
- turn agent text into a trusted event.

> **Knowledge informs behavior. Policy/authorization controls behavior.**

## 9. Agent/human revisions

Agents and humans may propose revisions through the same audited system. The runtime records who proposed and who/what validated the revision.

No invisible mutation.

## 10. Simple V1 retrieval

Start with boring retrieval:

- scope;
- task/service/domain tags;
- text search;
- recency;
- status = ACTIVE;
- optional last-verified/applicability filters.

Do not require embeddings, graph databases, rerankers, or a semantic ontology in V1.

## 11. Skill model

A Skill is **versioned reusable procedural knowledge packaged for on-demand use**.

Borrow the useful idea from systems such as Hermes: keep reusable procedures outside the permanent prompt and load them progressively only when relevant.

V1 skill package may contain:

```text
skill.yaml
instructions.md
references/
assets/            # static templates/examples only in V1
```

V1 does **not** allow a generated skill to become trusted Go/plugin code.

## 12. Skill lifecycle

```text
SKILL_CANDIDATE
      |
      | validation/tests
      v
ACTIVE
      |
      +--> SUPERSEDED
      +--> QUARANTINED
      +--> RETIRED
```

Agent can propose a skill or revision. Agent cannot directly activate it.

## 13. Skill requirements

Minimum fields:

```text
SkillID
Version
Name
Description
Scope
Status
InstructionsRef
ReferenceRefs[]
RequiredCapabilityClasses[]
ProvenanceEventRefs[]
EvidenceRefs[]
CreatedBy
CreatedAt
LastVerifiedAt?
SupersedesVersion?
```

## 14. Skill != capability

An active “Deploy Production” skill may describe exactly how to deploy production. It does not grant `production.write`.

Runtime performs normal capability and consequence checks at every real action.

## 15. Promotion signals

Difficulty alone does not justify a new skill. Useful signals include:

- repeated verified successful workflow;
- human correction followed by verified success;
- Completion Engine rejection followed by a generalizable correction;
- recurring multi-step task class;
- multiple independent experiences supporting the same procedure.

V1 may allow manual/agent explicit proposals. Automatic extraction belongs to `VALIDATE NEXT`.

## 16. Executable skill assets

Generated scripts/code are **not V1 trusted skill behavior**.

If later enabled, they must be untrusted artifacts executed through sandbox/capability enforcement and must never be dynamically loaded into the Agent OS Trusted Computing Base merely because a skill generated them.
## 17. Repeated-pattern candidacy — v4.1.1

Default minimum for a repeated-pattern candidate is **three related occurrences**.

This is a trigger for investigation, not proof. A `KNOWLEDGE_PROPOSED` record whose basis is `REPEATED_PATTERN` must reference at least three occurrence events under the default policy.

The threshold may be raised by task class/consequence.

> Frequency determines when a pattern deserves investigation. Evidence determines whether it deserves promotion.

After pattern candidacy, use subsequent evidence and/or deliberate experiments before promotion to ACTIVE knowledge where appropriate.

## 18. Knowledge validation method

Knowledge promotion/revision should record how it was validated, for example:

- `DETERMINISTIC_CHECK`
- `EXPERIMENTAL_EVIDENCE`
- `REPEATED_OBSERVATION`
- `INDEPENDENT_AGENT_JUDGMENT`
- `HUMAN_JUDGMENT`
- `MIXED`

Judgment is not deterministic proof. The operator identity and supporting evidence remain auditable.

## 19. Skill defense in depth

Auditing supplements rather than replaces:

- provenance;
- applicability/scope;
- validation evidence;
- version history;
- capability/consequence enforcement;
- Completion Engine verification;
- dependency/version revalidation;
- quarantine/supersession/rollback.

Bad Skills can exist. The design objective is rapid detection/correction and bounded consequences, not impossible perfect procedures.
## v4.2 source-grounding note

For factual knowledge backed by external sources, a future `VALIDATE_NEXT` capability may check whether cited artifacts actually support the proposed claim.

This is evidence validation, not a return to ANL/ASM or a general semantic ontology.

V1 continues to preserve evidence/provenance so such validation can be added later.
