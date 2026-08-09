# Falsification, Operational Measurement, and Controlled Evaluation


## 1. Work-first thesis

Hypothesis:

> Agent OS can perform real organizational work by composing deterministic software, tools, Agents, Teams, Skills, and human operators while using LLM inference only where it creates justified value.

Primary evidence comes from representative real work.

The system does not exist to prove Teams are better or to maximize agentic execution.

## 2. Operational measures

Record by real task class:

- verified outcome;
- execution mechanism/topology;
- deterministic vs LLM steps;
- model/provider/profile where used;
- token/provider/tool cost;
- wall time;
- retries/failures/rework;
- duplicate work;
- messages/collaboration;
- blocked-task frequency;
- human interventions;
- safety denials/violations;
- completion assurance.

## 3. Controlled comparison trigger

Do not infer structure superiority from unrelated tasks of different difficulty.

When a real recurring task class creates uncertainty about the best execution structure, compare using replayable/matched/held-out real tasks where practical.

Candidates may include:

- deterministic workflow;
- single Agent;
- Agent + Skill;
- Agent + verifier;
- parallel independent attempts;
- async Team.

Prefer the smallest structure that reaches the required verified outcome at acceptable cost, latency, reliability and risk.

If Teams provide no material advantage for a task class, do not use them there. If LLM inference provides no material value for a workflow step, remove it.

## 4. Event Contract minimality thesis

Hypothesis:

> A tiny typed coordination/control vocabulary plus ordinary content is sufficient.

Track how often developers want new event kinds and whether existing `MESSAGE` + content actually fails a deterministic requirement.

Do not expand contracts because a concept “sounds semantic.”

## 5. TOON/codec thesis — future

Only after JSON baseline exists, benchmark TOON on representative context payloads for:

- tokens;
- latency;
- parsing/accuracy;
- model-specific failure modes.

If benefit is weak, keep JSON.

## 6. Blocked-task authority thesis

Measure whether Authority Non-Solicitation reduces unnecessary privilege without creating unacceptable task-blocker churn.

## 7. Completion thesis

Measure false-complete/false-reject rates for:

- worker self-report;
- LLM reviewer;
- CompletionContract + deterministic/runtime evidence.

The Completion Engine must earn complexity through lower false-complete risk.
## 8. Topology neutrality

The benchmark is not designed to prove Teams win.

Lab/benchmark candidates may include single-agent, Skill-assisted single-agent, verifier, parallel, and async-Team configurations.

A successful Agent OS may learn that most work belongs to a single agent and reserve Team collaboration for specific interdependent classes.
## 9. Minimal-LLM falsification rule — v4.1.2

For recurring workflow steps, compare LLM-backed behavior against simpler deterministic/tool implementations when practical.

If conventional software achieves equivalent required outcomes with greater reliability/lower cost, prefer it.

Conversely, do not force rigid deterministic automation where an LLM materially improves success/adaptability and remains within policy/resource constraints.
