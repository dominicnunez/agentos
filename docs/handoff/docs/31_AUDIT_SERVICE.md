# Audit Service — v4.1.1

## 1. Decision

A deterministic **Audit Service** is V1 CORE.

Do not create a permanent omnipotent “Auditor Agent” in V1.

> **Auditing is independent observation, not executive authority.**

## 2. Why deterministic first

Many important audits do not require an LLM:

- ledger integrity;
- dangling/missing references;
- actions missing valid authorization trace;
- expired/revoked capability inconsistencies;
- CompletionContract/evidence inconsistencies;
- stuck/stale tasks;
- stale pending approvals;
- knowledge missing provenance;
- ACTIVE knowledge depending on changed versions/environment;
- quarantined/stale knowledge being surfaced as active;
- skills missing provenance/validation;
- skill dependencies that changed;
- unexpected resource/budget anomalies.

These should be normal software checks.

## 3. Audit triggers

Support both:

### Scheduled
Configurable interval per audit rule/class.

Do not hard-code “daily/weekly/monthly” as architecture. A deployment chooses cadence.

### Event-triggered
Examples:

- model/provider/tool version change;
- significant skill revision;
- dependency/environment change;
- incident/freeze event;
- repeated Completion Engine rejection;
- capability/policy change;
- knowledge activation/supersession;
- inference resource anomaly.

## 4. AuditFinding

Audit Service emits a durable finding, for example:

```text
AUDIT_FINDING
id
rule_id
severity
scope
summary
evidence_refs[]
related_knowledge_ids[]?
related_skill_ids[]?
created_at
status = OPEN | ACKNOWLEDGED | RESOLVED | DISMISSED
```

A finding does not silently alter trusted knowledge, policy, skills, or capabilities.

It enters normal remediation/governance.

## 5. Knowledge audit examples

```text
Knowledge: Payment API procedure v7
Last verified: 90 days ago
Dependency recorded: API v4
Current dependency: API v5

=> AUDIT_FINDING: revalidation required
```

A successful revalidation can update `LastVerifiedAt`; a change produces a new version.

## 6. Skill audit examples

- referenced tool version changed;
- active procedure has not passed its relevant tests after environment update;
- active skill references a quarantined knowledge item;
- skill requires capability class no longer available;
- supporting artifact was revoked/invalidated.

## 7. LLM AuditWorker — VALIDATE NEXT

Judgment-heavy audits may later invoke a bounded independent model execution for:

- sampled SOP/procedure quality;
- recurring failure patterns;
- suspicious but technically passing completion;
- contradictory knowledge;
- whether evidence actually supports a proposed lesson.

The AuditWorker returns a candidate finding. It does not receive executive authority.

Increase model/context/evidence independence with consequence.

## 8. Anti-bureaucracy rule

Do not deep-audit everything.

Use:

- cheap deterministic checks broadly;
- risk-based triggers;
- sampling for judgment-heavy review;
- measured false-positive/confirmed-finding rates.

Do not create “auditor of the auditor” recursive agent hierarchy. Audit quality can be measured by deterministic statistics and occasional human evaluation.
## 9. Institutional correction objective — v4.1.1

Auditing is not expected to guarantee that incorrect knowledge or bad Skills never become active.

The system should instead minimize the duration and blast radius of institutional mistakes.

Record raw correction metrics where feasible:

- time from contradictory evidence to audit finding;
- time from audit finding to correction/quarantine;
- downstream tasks affected;
- repeat failures before correction;
- stale knowledge uses;
- Skill rollback/revision time.

Do not collapse these into an authoritative health score in V1.

## 10. Pattern/knowledge audit

Repeated occurrence count is not proof.

Audit rules may flag:

- ACTIVE repeated-pattern knowledge lacking post-pattern validation;
- high-consequence knowledge supported only by a minimal occurrence count;
- knowledge whose evidence roots are not actually independent;
- knowledge repeatedly contradicted by later tasks;
- Skills derived from stale/quarantined knowledge.

The output remains `AUDIT_FINDING`; remediation follows normal knowledge/Skill governance.
## v4.2 runtime-evidence audits

Add deterministic rules for:

- `ExecutionContextManifest` refs resolve and exact version/materialization states are internally consistent;
- an execution cannot be audited as having used a Skill/Knowledge version absent from its manifest;
- ToolOutcome says SUCCESS while required postconditions failed;
- deterministic recovery exceeded the original authorized effect;
- protected external effect lacks an EffectObligation;
- EffectObligation remains ATTEMPTED/uncertain beyond its reconciliation policy;
- confirmed effect lacks confirmation evidence where evidence is available;
- approval EffectFingerprint does not match the obligation/effect actually attempted;
- disabled/revoked ExternalActor continues to submit A2A work;
- A2A task mappings reference missing/unauthorized internal objects.

These produce `AUDIT_FINDING`; they do not silently repair policy or execute the effect.
