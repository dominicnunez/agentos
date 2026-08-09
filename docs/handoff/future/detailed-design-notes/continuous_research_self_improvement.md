# Future / Historical Design Note — Continuous Research Self Improvement

**Status:** Non-normative. Preserved from v3.2 because useful portions may be revisited after prerequisites are met.  
**Important:** Any ANL or Agent Semantic Model assumptions in this preserved note are superseded by v4.0 Event Contracts and MUST NOT be implemented.  
**Authority:** `../FUTURE_CONSIDERATIONS.md` determines whether/when an idea may be revisited.

---

# Continuous Research and Self-Improvement — Future Reference Organization

## 1. Decision

The Research & Continuous Improvement concept remains valuable, but **it is not part of the v1 kernel and is not an early subsystem**.

It should eventually be built **using normal Agent OS primitives** as a reference organization.

That is an important architectural test: if research/self-improvement requires special kernel magic, the ordinary organization primitives may be incomplete.

## 2. Future workflow

```text
external source
 -> UNTRUSTED artifact
 -> candidate claim
 -> critique/validation
 -> improvement hypothesis
 -> sandbox implementation
 -> controlled benchmark
 -> independent evaluation
 -> recommendation
 -> governance
 -> reversible rollout
```

A paper, repository, README, or web page is evidence/data, never system authority.

## 3. Autonomy boundary

Future research agents may autonomously research, code, test, and benchmark inside the approved envelope.

Deployment rules remain:

- ordinary Agent OS runtime code change -> human approval before deployment;
- trusted-core/security/evaluator/governance change -> stricter security-admin approval.

## 4. Do not build yet

V1 contains no:

- arXiv crawler;
- research-specific agent roles;
- autonomous patch deployment;
- knowledge-promotion pipeline;
- SOP/skill generation platform.

First prove the kernel it would use.
