# Lab Experimentation and Promotion — v4.1.1

## 1. Status

The Lab is `VALIDATE NEXT`, built from V1 primitives after the core runtime is performing representative real work and producing operational measurements.

The architectural principle is accepted now:

> **Separate freedom to explore from authority to affect reality.**

## 2. Purpose

Trusted operational procedures should not be mutated while searching for better approaches.

Instead, Agent OS can create disposable isolated experiments:

```text
trusted baseline
   +-- Experiment A
   +-- Experiment B
   +-- Experiment C
   +-- Experiment D
```

Explorers may fail cheaply. Useful results cross a promotion gate before becoming trusted.

## 3. Experiment composition

Prefer composition over a giant new subsystem:

```text
Experiment =
  bounded Task/Goal
  + isolated workspace/sandbox
  + special capability profile
  + explicit resource budget
  + ephemeral AgentExecution(s)
  + artifacts/results
  + EXPERIMENTAL_UNVERIFIED trust label
```

## 4. High freedom, hard walls

Inside an approved disposable sandbox, experimentation may allow:

- arbitrary temporary files;
- temporary databases;
- package installation;
- test/prototype code;
- multiple approaches/models;
- child explorer executions;
- candidate knowledge/skill generation.

It may not automatically:

- write production;
- contact customers/public;
- modify active policy;
- grant authority;
- change active knowledge/skills;
- spend outside the experiment resource budget;
- promote itself.

## 5. Promotion

Parent/commissioning actor may say “promising,” but cannot alone certify trust.

```text
Experiment result
   -> PROMOTION_CANDIDATE
   -> independent reproduction/evidence
   -> CompletionContract/tests
   -> security/capability review as needed
   -> trusted knowledge / skill / configuration candidate
```

For high-consequence system/runtime changes, normal human approval boundaries still apply.

## 6. Cherry-picking protection

The more alternatives explored, the stronger fresh confirmation should be.

A winner among many candidates should be re-tested against held-out/fresh tasks before promotion.

## 7. Resource control

Experiments always receive explicit limits such as:

```text
MaxExecutions
MaxTokens/UsageUnits
MaxMeteredCost
MaxWallTime
MaxChildren
AllowedInferencePools[]
```

Surplus subscription/local capacity may later be allocated to the Lab, but business continuity reserve always wins.
## 9. Topology experiments — v4.1.1

The Lab is explicitly allowed to compare execution structure itself.

Candidate configurations may include:

- single agent;
- single agent + active Skill;
- single agent + verifier;
- parallel independent attempts;
- asynchronous Team.

Agent OS is not premised on Teams being universally superior.

Long-term selection should prefer the smallest structure that reaches the required verified outcome at acceptable cost, latency, reliability, and risk.

Do not add an automatic topology selector until real-work/Lab evidence demonstrates stable task-class differences.
## 10. v4.1.2 — Work-derived experiments

Lab experiments should normally originate from uncertainty or inefficiency observed in actual organizational work.

The Lab may test whether the real task class should use:

- deterministic workflow;
- one Agent;
- one Agent + Skill;
- verifier;
- parallel attempts;
- async Team.

Use controlled replay or held-out representative tasks where possible.

Do not create synthetic workload merely to justify a preferred topology.
