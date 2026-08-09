# Benchmark and Validation Plan

## 1. Collaboration benchmark

Compare on the same interdependent task corpus:

A. strong single agent  
B. sequential workflow  
C. independent parallel ensemble  
D. turn-based group chat  
E. hierarchical Explore–Execute–Review team inspired by OMC  
F. topology selected from a Qualixar-style candidate catalog  
G. AgentRadio-style asynchronous team  
H. asynchronous team plus post-task Meta-Team-style evolution

Control:

- model family where possible;
- tool access;
- starting information;
- maximum cost/tokens/time;
- Completion Contract;
- evaluator;
- task samples.

Measure:

- verified completion;
- time to decisive evidence;
- correction speed;
- duplicate work;
- useful cross-agent message rate;
- message noise;
- cost;
- human burden;
- safety events.

## 2. ANL benchmark

Compare:

1. natural-language messages;
2. structured JSON/A2A-style task messages;
3. ANL structured model form;
4. ANL canonical wire form where model consumes a decoded structured view.

Measure:

- semantic field preservation;
- permission/constraint loss;
- evidence/provenance loss;
- clarification turns;
- task quality;
- token/latency cost;
- cross-model consistency;
- human audit accuracy;
- deterministic render coverage.

Falsification:

If ANL consistently degrades task quality without compensating safety/audit/efficiency gains, revise or narrow ANL.

## 3. Authority model benchmark

Compare:

- agent permission requests;
- blocked-task return with parent remediation;
- preconfigured broad capabilities;
- explicit per-assignment capabilities.

Measure:

- unauthorized action rate;
- unnecessary privilege;
- blocker frequency;
- time to resolution;
- parent coordination cost;
- human approval count;
- task completion.

Goal: prove the stricter authority model does not make ordinary work unusably bureaucratic.

## 4. Completion Engine benchmark

Collect tasks where agents tend to declare success prematurely.

Compare:

- self-reported completion;
- LLM reviewer completion;
- Completion Contract + deterministic checks;
- mixed deterministic/independent adjudication.

Measure false-complete and false-reject rates.

## 5. Optimization benchmark

Candidate sequence:

```text
A
A+skill
A with alternate model
A with alternate reasoning budget
best single-agent candidate
best candidate + specialist B
```

Use staged elimination and held-out evaluation.

Measure:

- marginal quality;
- cost;
- latency;
- coordination overhead;
- reliability;
- tail risk;
- organizational complexity.

## 6. Self-improvement benchmark

Compare:

- no learning;
- centralized post-task summary;
- per-agent independent reflection;
- Meta-Team-style distributed evidence exchange;
- our hidden-evaluator + operational-feedback + controlled-experiment approach.

Critical controls:

- separate evolution and evaluation tasks;
- prevent evaluation secrets from entering memory;
- version models/tools/prompts/memory;
- require minimum gain and cooldown.

## 7. Safety acceptance

Execute the v3 adversarial catalog plus new cases derived from neighboring systems:

- process visibility without authority;
- child process/profile attenuation;
- capability feasibility before utility selection;
- evaluator score inflation;
- diversity collapse;
- organization package supply-chain import;
- stale context hibernation/revival;
- middleware bypass attempt.

## 8. Evidence standard

A mechanism should not become an architectural default merely because:

- a paper reports one benchmark;
- a demo works;
- an LLM judge likes it;
- the implementation exists.

Promotion requires evidence under our workload, Completion Contract, cost envelope, and safety model.
