# Future Considerations Registry — v4.2

These ideas are preserved but are **not V1 implementation requirements** unless explicitly promoted. Each item states prerequisites and a concrete revisit trigger.

## FC-001 — TOON context codec

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** JSON Event Contract baseline is stable; representative context traces exist; token cost is material

**Revisit when:** Benchmark JSON vs TOON on tokens, latency, parse/accuracy failures across actual models

**Notes:** Promote only if net benefit is material; no semantic dependence on codec.

## FC-002 — Additional collaboration topologies

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Representative real work and operational telemetry exist; at least one task class has uncertainty about execution structure

**Revisit when:** Need controlled comparisons among sequential, parallel, turn-based, worker+verifier or other manually configured structures

**Notes:** Use replayable/matched/held-out real task classes; do not create work merely to benchmark a topology.

## FC-003 — Automatic collaboration topology selector

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Multiple manually configured topologies have repeatable, task-class-specific strengths and enough labeled runs

**Revisit when:** Manual selection becomes costly and prediction can be evaluated held-out

**Notes:** Do not build before topologies themselves prove value.

## FC-004 — Advanced knowledge retrieval / consolidation

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** V1 versioned KnowledgeRecord is in regular use and simple scope/tag/text retrieval has measured misses or context cost

**Revisit when:** Need embeddings/reranking/consolidation to improve retrieval measurably

**Notes:** V1 knowledge store is active; this item is only advanced retrieval/consolidation, not whether memory exists.

## FC-005 — Advanced knowledge taxonomy / lifecycle automation

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** V1 EXPERIENCE/LESSON/KNOWLEDGE/PROCEDURE types show concrete lifecycle differences that current fields cannot handle

**Revisit when:** Measured maintenance/revalidation needs justify richer classes

**Notes:** Do not introduce ontology or many states simply for elegance.

## FC-006 — Automated SOP/procedure promotion and induction

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** V1 procedures and Skills are repeatedly used and have reliable CompletionContract outcomes

**Revisit when:** Manual/explicit candidate creation becomes a bottleneck and automatic induction can be evaluated

**Notes:** Instruction/reference procedures already exist in V1; this is automation, not basic storage.

## FC-007 — Executable skill assets and automatic skill evolution

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Instruction/reference Skills prove value; sandbox/security/tests and held-out evaluation are strong

**Revisit when:** Measured benefit justifies generated scripts/code or evolutionary revisions

**Notes:** Generated code remains untrusted/sandboxed and never becomes trusted-core plugin merely via skill promotion.

## FC-008 — Offline EvaluationRecord

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Completion Engine produces trustworthy verified outcomes and representative operational task samples exist

**Revisit when:** Need durable cross-run comparisons of profiles/structures beyond raw operational telemetry

**Notes:** Keep separate from worker-visible operational feedback; evaluation is an analysis layer over real work.

## FC-009 — Post-task collaborative failure attribution

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Evaluation records stable; enough multi-agent failures to analyze

**Revisit when:** Centralized trace cannot reliably explain distributed failure

**Notes:** Gather bounded evidence from participants without exposing hidden comparative grades.

## FC-010 — Evaluator Goodhart/drift monitoring

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Evaluation is used repeatedly for decisions and stable metrics exist

**Revisit when:** Score inflation, calibration drift, task selection skew or diversity collapse becomes plausible

**Notes:** Monitor evaluator health before optimizer.

## FC-011 — Manual organizational experiments

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Evaluation stable; enough repeatable task classes

**Revisit when:** Need compare A vs A+skill/model/reasoning vs A+B

**Notes:** Use held-out tasks and staged elimination.

## FC-012 — Organization Health Vector

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Raw metrics/evaluation dimensions have validated meaning over meaningful sample size

**Revisit when:** Humans need a stable multidimensional operational summary

**Notes:** Hard safety gates override any aggregate; avoid early arbitrary scoring.

## FC-013 — Automatic organization optimizer

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Evaluation, held-outs, causal controls, health metrics, rollback and experiment framework all stable

**Revisit when:** Manual experiment selection becomes bottleneck and predictive optimizer can be evaluated

**Notes:** Optimizer may amplify errors; treat as high-risk subsystem.

## FC-014 — Dormancy/retirement recommendations

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Persistent specialized agents actually exist; evaluation can estimate marginal contribution and rare-capability risk

**Revisit when:** Organizational complexity becomes measurable cost

**Notes:** Default ACTIVE -> DORMANT -> ARCHIVED; no automatic destructive deletion.

## FC-015 — Natural-language organization builder

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Core organization/team/task primitives stable and can be represented safely

**Revisit when:** Human setup friction becomes meaningful

**Notes:** Compile proposal for inspection; do not grant consequential authority from prose without policy checks.

## FC-016 — Portable organization package

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Organization/agent/policy/skill schemas stable across versions

**Revisit when:** Need reuse/share/move organizations

**Notes:** No embedded live secrets; imported package begins untrusted/quarantined.

## FC-017 — Multiple organizations/businesses per runtime

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Single-organization runtime stable and a second real business/use case exists

**Revisit when:** Operational leverage requires co-hosting multiple organizations

**Notes:** Start with logical isolation; do not imply strong tenant security.

## FC-018 — Strong multi-tenant isolation

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Multiple independent trust domains or customers require shared infrastructure

**Revisit when:** Cross-organization data/authority risk becomes real

**Notes:** Requires trust-domain model, storage isolation, secret separation, tests.

## FC-019 — Hermes RuntimeAdapter as internal Agent OS worker

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** A2A Operator Gateway and core RuntimeAdapter boundary are stable; a concrete need exists for Agent OS to execute Hermes internally

**Revisit when:** An internal task class benefits from Hermes-specific capabilities

**Notes:** Distinct from V1 Hermes external operator via A2A; must not bypass Event Gateway/capability enforcement.

## FC-020 — Outbound A2A client/delegation and broader federation

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Inbound A2A Operator Gateway is stable; a real need exists for Agent OS to discover/delegate to remote external agents

**Revisit when:** Need outbound remote-agent work delegation beyond the Hermes operator relationship

**Notes:** Internal Event Contracts remain canonical; do not turn A2A into internal IPC.

## FC-021 — Full federation gateway

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** At least two external runtimes/providers require interoperable trust/routing/version negotiation

**Revisit when:** Point adapters become repetitive/unsafe

**Notes:** Separate trust boundary may justify extraction.

## FC-022 — MLFQ/resource-aware scheduler

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Basic scheduler stable; concurrency/resource contention causes measurable tail latency/starvation

**Revisit when:** Scheduler metrics show priority/resource pathology

**Notes:** Borrow from AgentRM only when workload exists.

## FC-023 — Zombie/stall detection

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Longer real executions and waiting states exist

**Revisit when:** Stalls consume resources or block progress

**Notes:** Distinguish legitimate WAIT from execution stall.

## FC-024 — Context compaction with explicit materialization correctness

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** ExecutionContextManifest is stable; real contexts create measurable context pressure

**Revisit when:** Need pruning/summarization while preserving exact FULL/SUMMARY/REFERENCE_ONLY/OMITTED/UNAVAILABLE state

**Notes:** Never imply a full Skill/knowledge item remains loaded after compression when only a summary/reference remains.

## FC-025 — Checkpoints

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Ledger replay latency becomes measurable

**Revisit when:** Restart/rebuild exceeds accepted operational target

**Notes:** Ledger remains authoritative; checkpoints are acceleration only.

## FC-026 — Postgres / durable broker / distributed workers

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** SQLite/in-process limits are measured or isolation/independent scaling is needed

**Revisit when:** Concurrency, fault isolation, throughput or remote compute demands it

**Notes:** Possible Postgres/NATS/gRPC; domain must remain decoupled.

## FC-027 — Distributed consensus / multi-writer ledger

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Single-writer authority is a proven availability/scale bottleneck and requirements justify complexity

**Revisit when:** Need multiple authoritative writers across failure domains

**Notes:** Do not implement casually; partition safety matters.

## FC-028 — Multi-channel approval escalation platform

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Real critical approvals remain unanswered and one notifier is insufficient

**Revisit when:** Operational incidents demonstrate need for SMS/push/backup approvers/quiet-hour policy

**Notes:** Risk/urgency semantics already exist; attention can escalate, authority cannot.

## FC-029 — Multi-human governance

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** More than one human actually needs distinct authority

**Revisit when:** Owner/security/finance/manager separation is operationally needed

**Notes:** Define role precedence and optional multi-party approval.

## FC-030 — Incident-response automation subsystem

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Freeze/revoke/isolate/evidence primitives exist and incidents recur

**Revisit when:** Manual runbooks are too slow/error-prone

**Notes:** Compose existing primitives; avoid duplicate control plane.

## FC-031 — Supply-chain trust platform

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** External tools/code/skills/packages are routinely activated

**Revisit when:** Manual provenance/quarantine no longer sufficient

**Notes:** Start with source/hash/trust-state metadata; add signatures/scanning later.

## FC-032 — Encrypted/deletable sensitive artifact store

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Real confidential/PII/secrets must be processed

**Revisit when:** Immutable ledger payload retention conflicts with privacy/security needs

**Notes:** Keep structural history while sensitive payload/key can be deleted under policy.

## FC-033 — Information-flow/data classification

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Multiple sensitive data classes/providers/trust domains exist

**Revisit when:** Derived-data export decisions need systematic classification

**Notes:** Derived data inherits/combines classifications.

## FC-034 — High-assurance communication profiles

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Covert-channel risk is material for a concrete deployment

**Revisit when:** Need to bound timing/size/routing/semantic-choice channels

**Notes:** Do not claim covert channels can be eliminated.

## FC-035 — Tamper-evident signed checkpoints/backups

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Production value makes ledger destruction/tampering a material business risk

**Revisit when:** Disaster-recovery/security requirements become formal

**Notes:** Hash-link events, protected keys, off-system backups, restore drills.

## FC-036 — Research & Continuous Improvement organization

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Core runtime + evaluation + safe experimentation + knowledge promotion are stable

**Revisit when:** Agent OS can evaluate improvements without circular self-grading

**Notes:** Reference organization built on Agent OS, not kernel subsystem.

## FC-037 — Automated Agent OS code self-improvement pipeline

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Research org stable; sandbox CI/benchmark/evaluator trust boundaries strong

**Revisit when:** Measured value from candidate patches justifies automation

**Notes:** Agents may code/test/propose; runtime deployment still human-approved; trusted core stricter.

## FC-038 — Automatic model/reasoning/team optimization

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Evaluation/experiments/held-outs are reliable over many tasks

**Revisit when:** Manual configuration materially limits performance/cost

**Notes:** Staged elimination; exploration budget; no exhaustive grid.

## FC-039 — Deterministic event presentation templates

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Audit UI needs consistent human rendering beyond raw envelope/content

**Revisit when:** Humans misread event/control distinction or need stable reports

**Notes:** Render event fields deterministically; do not recreate semantic ontology.

## FC-040 — Domain-specific structured content schemas

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** A concrete business application needs machine-readable domain content

**Revisit when:** Free-form content blocks deterministic application logic

**Notes:** Schema remains application/domain scoped unless core runtime behavior truly depends on it.

## FC-041 — Shared blackboard as dedicated store

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Event projections cannot meet shared-current-state performance/consistency needs

**Revisit when:** Measured projection limitations

**Notes:** Default remains ledger + projection; dedicated store cannot become competing truth.

## FC-042 — Advanced PlanGraph/versioned planning engine

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Task ParentID/DependsOn cannot represent required replanning/alternatives

**Revisit when:** Real workflows need speculative branches/plan versions/diffs

**Notes:** Do not introduce before simple Task DAG fails concretely.

## FC-043 — Complex policy DSL / external policy engine

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Typed rules become repetitive/unmaintainable and policy complexity is real

**Revisit when:** Rule volume/composition requires a policy language

**Notes:** Evaluate existing engines before inventing one.

## FC-044 — Bounded LLM AuditWorker

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Deterministic AuditService is stable; judgment-heavy audit questions recur

**Revisit when:** Sampled model review can find confirmed issues deterministic rules cannot

**Notes:** Temporary independent execution; produces candidate findings only; no executive authority.

## FC-045 — Advanced audit scheduling/compliance framework

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Real audit volume/regulatory requirements make basic configurable intervals insufficient

**Revisit when:** Need policy-driven sampling, compliance packs, or complex audit evidence workflows

**Notes:** Avoid building PagerDuty/GRC platform before concrete requirements.

## FC-046 — Lab / Experiment orchestration

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Core scheduler, sandbox, capability gate, Completion Engine, and inference budgets are working

**Revisit when:** Need disposable parallel prototypes or controlled self-improvement experiments

**Notes:** Freedom inside containment; outputs EXPERIMENTAL_UNVERIFIED until promotion.

## FC-047 — Lab promotion gate and held-out reproduction

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Experiment objects produce candidate knowledge/skills/configurations

**Revisit when:** Need systematic promotion without cherry-picking

**Notes:** Commissioning parent may nominate but not unilaterally certify trust.

## FC-048 — Surplus inference capacity allocation to Lab/maintenance

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** InferencePool telemetry and continuity reserve are reliable

**Revisit when:** Subscription/local capacity is routinely underused near reset/idle periods

**Notes:** Allocate only to useful backlog/validation/audits/experiments; do not burn quota for its own sake.

## FC-049 — Inference burn forecasting and target curves

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Sufficient execution usage history by pool/model/task class

**Revisit when:** Static reserve policy either wastes perishable quota or threatens continuity

**Notes:** Represent quota uncertainty and forecast error explicitly.

## FC-050 — Automatic local/cloud/model escalation policy

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Completion/cost/latency data exists for same task classes across multiple pools

**Revisit when:** Can evaluate local-first vs frontier-first vs escalation strategies held-out

**Notes:** Repeated weak-model failures can cost more than starting strong; test rather than assume.

## FC-051 — Cross-provider verifier routing

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Two or more independent provider/model pools and verification tasks exist

**Revisit when:** Need more independent model adjudication than same-provider review

**Notes:** Deterministic verification still preferred whenever possible.

## FC-052 — Predictive capacity planning / multi-GPU / energy-aware scheduling

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Multiple local accelerators or material power/thermal/queue constraints exist

**Revisit when:** Basic local InferencePool abstraction becomes insufficient

**Notes:** Do not build datacenter scheduler for one local machine.

## FC-053 — Automatic subscription/provider ROI and purchase recommendations

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Longitudinal usage/outcome/cost data across providers exists

**Revisit when:** Organization can quantify whether adding/removing paid access creates value

**Notes:** Any actual new financial commitment still crosses human approval boundary.

## FC-054 — Automatic contradiction detection and knowledge revalidation

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** V1 knowledge store has enough active records and dependency metadata to make automated checks useful

**Revisit when:** Audits find recurring stale/contradictory knowledge manually

**Notes:** Detection produces findings/candidates; do not silently rewrite active knowledge.

## FC-055 — Cross-team knowledge promotion

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Multiple teams exist and useful knowledge repeatedly applies beyond source team

**Revisit when:** Duplication or repeated rediscovery becomes measurable

**Notes:** Promotion changes visibility/scope, not authority; respect data boundaries.

## FC-056 — Skill A/B testing and evolutionary revision

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** V1 Skills have repeatable tasks, tests, and enough executions

**Revisit when:** Need evidence-based revision instead of subjective edits

**Notes:** Use held-out evaluation; promising variant does not auto-replace active skill.

## FC-057 — Automatic repeated-pattern clustering/detection

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** V1 knowledge store has enough experiences and manual/agent pattern proposals to measure recall/false-positive rates

**Revisit when:** Manual pattern discovery causes repeated missed opportunities or excessive human/agent review effort

**Notes:** Three occurrences is the default candidacy threshold, not proof. Automated clustering proposes candidates only.

## FC-058 — Risk-adaptive pattern candidacy thresholds

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Enough pattern candidates exist across task/consequence classes to measure false promotions and missed reusable patterns

**Revisit when:** A single default threshold produces too many weak high-risk candidates or delays harmless reusable lessons

**Notes:** Threshold controls investigation/candidacy, never automatic truth.

## FC-059 — Inference telemetry refresh-policy optimization

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** Multiple telemetry sources/providers have observed polling cost, staleness, and reset behavior

**Revisit when:** Static cache intervals are materially stale or wasteful

**Notes:** Provider APIs/CLIs remain adapter details; optimize refresh without relying on LLM interpretation.

## FC-060 — Generic workflow DSL / dedicated workflow engine

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** V1 Task dependency graph repeatedly fails to represent actual organizational workflows cleanly

**Revisit when:** Real work requires constructs that cannot be handled by Task dependencies, execution kinds, handlers, and ordinary code without substantial duplication

**Notes:** Do not create a workflow language merely because Agent OS executes workflows. Prefer the Task DAG and Go composition first.

## FC-061 — Source-grounded knowledge validation

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** V1 knowledge promotion/audit is in regular use with external factual sources

**Revisit when:** Need to verify whether cited evidence actually supports candidate/active claims

**Notes:** Return supported/partial/contradicted/unverifiable evidence status; does not require a general semantic ontology.

## FC-062 — Additional SecretSource integrations

**Tier:** `VALIDATE_NEXT`

**Prerequisites:** V1 SecretSource seam is stable and real deployments use external secret managers

**Revisit when:** Need 1Password/Bitwarden/Vault/OS keychain or equivalent

**Notes:** Keep credentials out of model context where adapters can resolve/use them directly.

## FC-063 — Context hibernation and advanced resume compaction

**Tier:** `FUTURE_IF_EARNED`

**Prerequisites:** Long-sleeping work/context restoration becomes a material cost/quality problem after simple checkpoints/context reconstruction

**Revisit when:** Resume/context loading dominates cost or quality and simple materialization is insufficient

**Notes:** Preserve ExecutionContextManifest/history and measure information-retention failure explicitly.

