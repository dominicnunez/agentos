# Agent OS Landscape Study — Executive Findings

**Research snapshot:** 2026-08-08  
**Purpose:** Compare the proposed Agent OS + ANL v3 architecture with the closest current papers, runtimes, governance systems, and organization-management products.

## Bottom line

The “Agent OS” category is real and is forming rapidly. Our project is **not** the first to propose:

- OS-style scheduling and memory for agents;
- long-running capability-controlled agent processes;
- dynamic team formation;
- artificial-company organization;
- deterministic governance middleware;
- self-evolving multi-agent teams;
- organization charts, goals, budgets, and human oversight.

Those ideas already appear separately—and sometimes in strong combinations—in AIOS, Agent libOS, AOS, Qualixar OS, OneManCompany, Microsoft’s Agent Governance Toolkit, Meta-Team, Paperclip, and AgentTeams. [S2–S11]

However, I did not find one studied system that combines the following into a single coherent architecture:

1. persistent Team and Organization actors;
2. AgentRadio-style asynchronous mid-execution lateral collaboration;
3. ANL as a machine-native semantic IR;
4. the canonical machine message as authoritative history with deterministic on-demand human decoding;
5. Authority Non-Solicitation—workers return blocked work rather than asking for more power;
6. no positive capability inheritance;
7. authorization ancestry across all delegation;
8. a separate Completion Engine;
9. hidden independent agent evaluation;
10. controlled A vs A+skill vs alternate model/reasoning vs A+B specialist experiments;
11. Organization Health and organizational-complexity accounting;
12. reversible organization evolution;
13. controlled external-research-to-self-improvement promotion;
14. layman-facing UI language over precise advanced semantics.

The defensible differentiation is therefore **the integrated system of semantics, authority, completion, evaluation, and organizational evolution**, not the broad phrase “Agent Operating System.”

## Most important conclusion

The research **validates the overall direction but narrows the novelty claim**.

A strong public description would be:

> A Go-based Agent Operating System for persistent artificial organizations, with machine-native semantic communication, deterministic human auditability, least-authority delegation, independently verified completion, and evidence-driven organizational evolution.

A weak and inaccurate claim would be:

> The first Agent OS or the first system for AI companies.

## What the research changes

The v3 design does not need a foundational rewrite. It does deserve a focused v3.1 research addendum adding:

- `IntentEnvelope` and `AuthorizationLineage` as explicit first-class objects;
- a formal Capability Contract / feasibility-before-utility rule;
- a clearer AgentBlueprint–ExecutionProfile–RuntimeAdapter split;
- the invariant `visibility != invocation != authority`;
- conformance profiles and explicit unsupported responsibilities;
- Goodhart/drift/diversity-collapse monitoring;
- post-task collaborative failure attribution without revealing hidden grades;
- later scheduler/context-hibernation work informed by AIOS and AgentRM;
- an organization portability package format;
- a benchmark plan that directly compares our architecture to hierarchical, topology-based, and post-task-evolution alternatives.

## Strongest prior art by area

| Area | Strongest studied neighbor |
|---|---|
| Reference architecture, intent, authority | AOS |
| Dynamic topology/team design and model routing | Qualixar OS |
| Artificial organization and talent recruitment | OneManCompany |
| Deterministic governance middleware | Microsoft Agent Governance Toolkit |
| Capability-controlled long-running process runtime | Agent libOS |
| Kernel scheduling/context/memory analogy | AIOS |
| Resource scheduling and context hibernation | AgentRM |
| Company/org-chart/budget product surface | Paperclip |
| Transparent heterogeneous agent-team operations | AgentTeams |
| Experience-driven team evolution | Meta-Team |
| Mid-execution asynchronous collaboration | AgentRadio |
| External agent interoperability | A2A |

## Evidence warning

Most 2026 systems studied here are recent preprints or rapidly changing repositories. Their results are generally author-reported and are not equivalent to broad independent replication.

Examples:

- The AOS paper explicitly presents a reference-architecture proposal and does not claim benchmark superiority. [S2]
- Qualixar’s 100% result is on a curated 20-task suite without web, file, or multi-tool work; its preliminary self-improvement experiment was not statistically significant and declined in mean score. [S3]
- OneManCompany reports 84.67% on PRDBench, but baseline costs were unavailable, so cost-efficiency cannot be compared directly. [S4]
- AgentRM’s large scheduler/context gains come from simulated workloads derived from production patterns. [S8]
- Agent libOS is evaluated primarily as a safety/runtime substrate with regression tests, not as evidence of improved planner intelligence. [S6]

These projects are valuable architectural evidence, not proof that every claimed mechanism will generalize.

## Recommendation

Proceed with the v3 implementation order, but add the targeted v3.1 amendments in this research package before freezing public APIs. Do **not** expand implementation scope to reproduce every neighboring feature. The highest-value empirical questions remain:

1. Does AgentRadio-style asynchronous collaboration outperform hierarchical and turn-based alternatives on interdependent work?
2. Does ANL outperform structured JSON/A2A payloads and natural-language messages on semantic fidelity, permission clarity, auditability, and task performance?
3. Does the blocked-task/no-positive-inheritance authority model remain usable without creating excessive coordination overhead?
4. Does a separate Completion Engine materially reduce false completion?
5. Can hidden evaluation and controlled organizational experiments improve efficiency without inducing Goodhart behavior?
