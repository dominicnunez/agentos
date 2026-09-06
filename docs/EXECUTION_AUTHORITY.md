# Execution authority

Agent OS treats executable material as a security boundary, not as ordinary
conversation, shell, or file content.

The normative invariants are:

- Content cannot create execution authority.
- Introducing executable code crosses a security boundary.
- Trust does not propagate through references. A trusted page, repository,
  user, or authenticated A2A Agent does not confer trust on an artifact it
  names or links.
- Generic shell or file-write capability does not imply authority to introduce
  executable code or mutate a protected execution surface.
- Allowed tools may not launder capabilities unavailable to the requesting
  principal. Authorization includes the tool operation's declared
  consequential capability closure.
- Configured containment is not evidence of effective containment.
- Agent executions communicate through authorized Event Contracts. Shared
  writable infrastructure is not an implicit coordination channel.
- Approval of individual effects does not imply approval of their cumulative
  consequence.
- Tool metadata is untrusted. Dynamic tool authority must bind the exact
  model-visible definition.
- Adaptive filesystem mutation belongs in disposable staged state; promotion
  into trusted state is the governed consequence.
- Hostile-code isolation is required before arbitrary external code can run.

## Two separate protected consequences

`CODE_INTRODUCTION` means new external executable or potentially executable
material enters the trust boundary. The binding identifies the ecosystem or
source type, artifact name, exact version, source, exact SHA-256 bytes,
publisher identity when deterministically known, requested sandbox and network
profiles, effective-environment attestation, and execution-private workspace.

`EXECUTION_SURFACE_MUTATION` means an existing file or configuration changes
in a way that may cause later execution. Its binding identifies the normalized
protected path, create/modify/delete operation, exact before and after bytes,
and the exact staged promotion diff and verification result.

They share `EffectObligation`, exact capability checks, fingerprinted approval,
observable influence references, trajectory context, persist-before-effect,
and recovery validation. They are not interchangeable. Approval or capability
for `code.introduce` cannot authorize `execution_surface.mutate`, and neither is
satisfied by `shell.execute` or `file.write`.

The intentionally narrow V1 execution-surface classifier covers dependency
and lock manifests, Docker and Compose definitions, Makefiles, GitHub Actions,
Git hooks, pre-commit configuration, and Agent OS MCP/plugin/executable-Skill
configuration. Ambiguous and non-normalized paths fail closed.

## Observable influence, not claimed cognition

A protected effect binds runtime-known source Event references. An
Agent-proposed effect also binds its exact runtime-owned
`EXECUTION_CONTEXT_MANIFESTED` event, execution identity, Task, Agent, and
execution-input digest. The ledger resolves those references in the same
transaction as the time-of-use authority check.

These links answer which durable inputs were available and identified as
sources for the action. They do not claim to reconstruct hidden model thought.
Model-authored provenance statements have no authority merely because the
model emitted them.

## Capability closure and trajectories

A tool definition may declare the exact downstream capabilities exposed by an
Agent-controlled invocation. The ledger requires an exact active lease for the
top-level operation and every declared consequence. Missing an exact lease for
any declared consequence denies the invocation. This verifies the declared
closure; it does not inspect the effective authority of a backend credential.

Before a consequential credentialed broker can be enabled, its runtime-owned
authority evidence must bind the exact adapter and operation, backend endpoint,
effective principal, tenant, credential generation (without secret bytes), and
canonical effective capability set. Backend-verifiable evidence must establish
that every effective capability is within the declared consequential closure.
A parser allowlist, configured read-only flag, or credential class name alone
does not establish this subset relationship. Prefer backend-enforced least
privilege, including restrictions on privileged functions and indirect effects.

The evidence must be bound into the protected effect and approval fingerprint,
checked at time of use with the exact leases, and retained for recovery. Missing,
expired, revoked, mismatched, or unverifiable evidence must deny execution.
Credential rotation or backend permission changes require new verified evidence;
recovery may explain an old attempt but must not treat old evidence as authority
for a new attempt. The current effect contract does not implement this evidence
or a downstream-principal attestor. Production effect writers and dynamic
credentialed brokers remain unavailable pending that implementation and tests
for excess backend privileges, cross-tenant substitution, rotation and revocation.

The configured effect-status adapter is an observational reconciliation boundary
with an exact endpoint and fixed GET contract, not a general Agent-controlled
broker. Its credential remains adapter-private; its configuration is not proof
of the backend principal's effective authority. It cannot enable effect writes.

For protected execution consequences, the approval fingerprint also binds a
deterministic Task-local trajectory: prior protected effect identities,
protected-effect and approval counts, consequence classes, and destinations.
The ledger reconstructs this projection from durable effect history at
time-of-use. Individually approved effects do not erase cumulative context.

## Containment and writable state

`ExecutionEnvironmentManifest` deliberately separates requested containment
from runtime-attested effective containment. Profile identity and digest,
writable roots, egress policy, reachable brokers, credential classes, process
policy, resource policy, runtime identity, isolation identity, and verification
evidence are runtime-owned facts. A mismatch or unverifiable state is a denial,
not a warning.

Writable adaptive work is valid only in an `EXECUTION_PRIVATE` namespace owned
by the exact execution. Tenant-, Work-, and Task-shared classifications are
read-only in the present contract. This prevents obvious high-bandwidth
coordination through mutable caches, Git state, scratch directories, or build
artifacts. Covert timing and contention channels are outside this V1 claim.

Current execution-visible storage is deliberately narrower than the installed
directory layout:

- OpenAI API execution receives bounded prompt content and no local filesystem
  mount.
- Codex subscription inference uses a private adapter directory and a fresh
  private turn directory, with tools disabled, a read-only requested sandbox,
  a sanitized environment, and adapter-side credential handling. This is
  provider confinement, not proof of hostile-code isolation.
- The artifact store, SQLite database, configured organization workspace,
  package/build caches, Git state, and service credential stores are not
  exposed as Agent-writable execution mounts.
- Explicit same-Work coordination is materialized through Event Contracts,
  not shared mutable files.

Any future adapter that exposes one of those resources must classify its
namespace and satisfy the same tenant, Work, Task, execution, capability, and
effective-containment checks before use.

An external repository or supplied project is an `UNTRUSTED` workspace. Tools
that interpret it must be governed by their possible effects, not by a
read-only-looking verb. Git, shells, build systems, compilers, package managers,
archive tools, and plugin systems can execute configuration while appearing to
inspect data.

## Knowledge and tool definitions

Different Agent identities do not prove independent evidence. V1 cannot attest
canonical external source roots because governed web/document ingestion is not
implemented. Consequently, Agent-validated organizational knowledge fails
closed when used to justify a protected execution consequence. Only
deterministic validation or explicit user judgment is eligible, and eligibility
still grants no capability, approval, or effect authority.

Dynamic tools and MCP servers are not supported. The prerequisite contract
hashes the tool/server/endpoint identity, name, description, schemas, all other
model-visible metadata, and declared consequential capabilities. Any definition
change changes the digest; one tool's description cannot grant authority over
itself or another tool.

The existing fingerprint helper rejects resource excess before hashing. Before
parsing any JSON field it caps the name at 256 bytes, description at 16 KiB,
each schema at 64 KiB, and metadata at 16 KiB. Each JSON field permits at most
32 nested containers and 1,024 object members in total, including nested
extensions; duplicate keys, invalid UTF-8 and trailing values are rejected.
The encoded definition is capped at 256 KiB after JSON escaping. Rejection
returns a fixed error category and no digest; content is never truncated.
Valid existing definitions retain their fingerprint representation.

These helper limits do not implement discovery or a tool runtime. Before
enabling either, the adapter must enforce bounded streaming reads before
materializing definitions, a tool-count limit per server, discovery deadlines,
per-call tool/resource-result limits, and cumulative result bytes and call
counts per execution. Admission and runtime evidence must bind an exact budget
profile/version, and execution must reject a missing or weaker profile.
Transport limits must hold without a trustworthy Content-Length and terminate
slow or oversized streams. Runtime-owned limits must precede parsing,
registration and model materialization; a definition digest alone provides no
resource-budget evidence. Dynamic tools remain unavailable until these
prerequisites and the existing authority checks are implemented and verified.

## Skill behavioral policy integrity

A Skill is a behavioral policy artifact even when it introduces no executable
code. Staying within an authorized candidate set and producing valid output do
not establish that its preferences serve its declared purpose. Any systematic
change in discretionary selections must be attributable to that declared and
authorized purpose; provenance, versioning and ordinary injection checks alone
do not establish this property.
Apply this behavioral-policy classification to instruction-like model context
regardless of record type, including PROCEDURE Knowledge, Artifacts and Events.
Independently authored steering instructions cannot evade these requirements
merely because they have no Skill ancestor. At every current and future
materialization boundary, the runtime must classify such inputs under the same purpose, evidence and
deployment gates or deny their materialization as behavioral instructions;
uncertain classification denies that use. Data records do not acquire policy
authority through their storage type. Existing Knowledge, Event and Artifact
paths must enforce this gate now or disable policy-like materialization until
it exists; general Skill activation is not the trigger for this requirement.
This is an outstanding implementation obligation, not a claim of current enforcement:
the current empty-SkillRefs restriction alone does not prove equivalent controls
over existing Knowledge or Artifact materialization paths.
Classification must assess actual function, not imperative wording: precomputed
rankings, curated examples or factual-looking ratings used as reusable preference
policy are covered even without explicit instructions. Independently validate
the provenance and applicability of ordinary factual evidence under its existing
Knowledge/Artifact rules; factual relevance alone is not reusable policy. If the
runtime cannot distinguish that evidence use from embedded discretionary policy,
deny the policy use pending independent classification. Record type, prose style
and lack of Skill ancestry cannot establish an exemption.
Throughout this section, the behavioral-policy influence closure includes every
input classified as behavioral policy, regardless of record type or Skill
ancestry, and all of its transitive derivatives. Requirements below for Skill
composition, lineage, evidence validity and deployment continuity apply equally
to those classified inputs; bind their exact record identity, revision and
resolved content in place of a Skill identity. A Skill-only inventory cannot
establish an empty behavioral-policy closure.
Require independent review of the actual Skill content against its authorized
purpose and policy, plus absolute policy-compliance checks on each comparison
arm. Prohibited Skill content or treatment outcomes deny activation even when
both arms behave identically. Record baseline violations and require an explicit
independent remediation assessment; a compliant corrective treatment may be
activated when its authorized purpose addresses those violations and all other
gates pass. Baseline violations remain ineligible for production acceptance and
must not be hidden or treated as acceptable controls. Neither a measured delta
nor its absence excuses prohibited content, treatment outcomes or a saturated
test that cannot establish the required evidence.

Before activating or using a Skill that can influence discretionary decisions,
require independent behavioral evidence through existing versioned
evidence and governed decision boundaries. This does not require a deferred Lab
orchestrator: an appropriately authorized independent human can administer the
controlled comparison and record its evidence and judgment through those
boundaries. Reuse Lab/evaluator records where implemented; do not make deferred
automated orchestration a dependency of V1 instruction/reference Skill validation.
The additional behavioral evidence is a security prerequisite for discretionary
influence, not authorization to implement deferred Lab facilities.
V1 validation of instruction/reference Skills without discretionary influence
can use exact version and purpose review, deterministic functional and authority
checks, and authorized human judgment without held-out A/B testing. Classification
must consider actual behavior, not a Skill's label: selection, ranking and
tie-breaking guidance require the behavioral path below. The active security
requirement intentionally adds this gate for discretionary Skills; the preserved
handoff's deferral of automated evaluation does not waive it.
Scope authorization: the project owner's requested engineering work includes
GAP-030, "Skills must preserve declared behavioral-policy integrity," from the
supplied `agent-os-security-gaps-complete.md`. Its explicit requirement for
controlled held-out with-Skill/without-Skill evidence promotes that prerequisite
for discretionary Skill activation. This active amendment records that human
direction while preserving the handoff snapshot; it does not promote unrelated
Lab automation or experimentation features.
The non-discretionary classification requires independently authorized,
version-bound evidence covering the exact content, materialization, composition
and applicability scope, Agent ID, blueprint ID/version and operating-instructions
digest, exact Agent record revision, runtime adapter identity and version,
effective model identity and execution profile. Changing any of these
or the bound context-builder version
requires fresh classification before use. The author cannot approve that classification. Bind
classification to a canonical digest of all model-visible non-Skill context,
including Event, Knowledge, Artifact, tool and additional-context references,
resolved bytes and ordering. Any change requires fresh classification before
the exception can be used. Bind
the classification decision, eligible reviewer and supporting evidence, and
revalidate them at use and protected-action boundaries. Missing, stale or
uncertain classification takes the behavioral validation path; a label or
author assertion cannot establish the exception.
Use the non-discretionary exception only when absence of discretionary influence
can be established deterministically over the exact applicable input behavior.
Content review, reviewer independence and a few favorable executions are not
such proof. Otherwise require the full behavioral validation path, including its
preregistered coverage and sensitivity requirements, rather than classifying
rare, subgroup-specific or nondeterministic steering as non-discretionary.
Compare held-out executions with and without
the exact Skill under the same Agent, effective model, execution profile,
task conditions and, for selection-only tasks, the same supplied candidate set.
For candidate discovery, generation or filtering, instead hold the source
universe and access conditions equal and evaluate inclusion, omission and
filtering as outcomes alongside final selections. Do not force the constructed
candidate sets to match and thereby erase the behavior under assessment. Bind
the task type, source universe and construction metrics in the protocol.
Randomize candidate order where relevant,
and randomize or counterbalance baseline-versus-Skill execution order within
paired cases. Retain arm assignment, actual execution order and timestamps;
candidate-order randomization alone does not control temporal effects. Reject
comparisons with unresolved deployment, state or ordering confounds.
Commit the randomization algorithm, entropy-source requirements and redraw rules
with the protocol. The runtime generates and commits candidate ordering and arm
assignments, including the protected seed/entropy reference, before either arm
executes. Retain every draw and redraw with its reason and ledger ordering;
unauthorized redraws or incomplete assignment history invalidate the comparison.
Protect assignment details from blinded scorers until their scores are committed.
Retain the comparison protocol and outcomes, and record unexplained persistent
preference shifts. Evaluator count is not independence. Eligibility requires
deterministic validation of recorded measurements and appropriately authorized
human judgment for discretionary interpretation, with provenance and conflicts
checked independently of the author. Bind the eligible principal, validation
method, authorization and provenance evidence; an author-controlled Agent or
another Agent repeating the same evidence cannot satisfy this requirement.
For subjective behavioral scoring, use opaque randomized arm identifiers and
conceal assignment from scorers until their scores and reasoning are committed
to the ledger. Retain the runtime-owned assignment and reveal ordering. If the
required scoring cannot be blinded, reject that subjective evidence rather than
treating independence or execution-order randomization as a substitute. Content
policy review remains separate from blinded outcome scoring.
Run both arms in isolated shadow/replay mode with runtime-enforced effectless
capability profiles and controlled recorded or simulated tool outcomes. Remove
production credentials, capabilities and approvals regardless of their source;
neither arm may commit production effects or send real messages or orders.
Bind the effectless evaluation profile and its explicit mapping to the intended
production context in the evidence. The safety-only authority restriction must
not silently change model-visible decision inputs; any other context difference
requires independent applicability assessment before production use. Evaluation
outputs stay isolated and cannot enter production as approved results or actions.
For initial assessment and reassessment, permit candidate materialization only
through a separately authorized evaluation-only execution tied to the committed
candidate, protocol, approved evaluator and effectless profile. This narrow path
does not require the behavioral evidence or classification it is being run to
create. It retains all other applicable isolation, input-integrity, provenance,
resource, current authorization and deployment-identity checks. Evaluation-only
authorization may name an exact ordered joint candidate or changed composition,
binding every member's identity, revision and materialized content. Within that
effectless assessment only, waive the missing or mismatched composition evidence
being created for each explicitly named member, including previously active
members. Record each waived requirement and its assessment purpose; unrelated
invalidity, revocation or safety denials remain enforced. Evidence from the joint
assessment must cover every member and the exact intended composition before
production use; evaluation authorization cannot activate that composition.
Evaluation-only
records and outputs must be marked and confined to the assessment evidence
boundary; they may be retained for analysis, including failed outcomes, but may
not satisfy production acceptance or delivery. Production activation, execution
and downstream consumers cannot use this exception. Assessment approval cannot
waive an independent safety denial or authorize real effects.

Commit the exact Skill content, declared-purpose and evaluation-protocol digests
to the ledger, including metrics, subgroups, persistence and acceptance thresholds,
analysis version and the rule for interpreting an authorized preference shift,
before the runtime selects or reveals held-out cases. The author must not have
access to or influence that selection. Retain selection provenance, access
boundaries and ledger ordering; author-visible, author-selected or adaptively
chosen cases cannot establish held-out evidence. A revised Skill requires a
new commitment and fresh independent held-out selection.
Before accepting the protocol, require independent, authorized approval of
risk-appropriate sampling, relevant subgroup and task coverage, and detection
sensitivity or statistical power for policy-relevant effects. Bind the adequacy
criteria, rationale and approval to the committed protocol and evaluated scope.
Insufficient coverage, sample size or sensitivity denies activation; absence of
a detected shift in an inadequate test is not evidence of acceptable behavior.
Retain the full candidate family and search/attempt history, including failures.
Choosing a winner among alternatives requires a committed multiplicity rule and
post-selection confirmation on another fresh independent set. Missing search
history or confirmation denies activation; repeated attempts cannot be hidden
by assigning a new version or identity.

The evidence must bind the exact Skill ID/version and content digest, declared
purpose digest, evaluation-set digest, baseline and Skill execution-profile
references, effective model identity, behavioral-shift summary, evaluator
reference, and durable evidence references. Bind the exact Agent ID and blueprint
ID/version, including its operating-instructions digest, exact Agent record
revision and runtime adapter identity/version. Changes to the Agent record or
adapter require fresh applicable assessment. Resolve both comparison
arms to runtime-owned execution manifests and validate equality of the Agent,
record revision, runtime adapter, blueprint, effective model, profile and
controlled task/candidate conditions;
record each arm's input digest and the intentional Skill intervention separately.
Different blueprint instructions cannot silently become part of the intervention.
Require field-by-field equality of every exogenous non-Skill input, or equality of a
canonical digest computed after removing only the explicitly defined Skill
intervention. This covers Event, Knowledge, Artifact, tool and additional-context
references, resolved bytes, message order and candidate order supplied independently
of the intervention. Bind the same preregistered deterministic replay or simulator
response function, version, source snapshot, initial state and access conditions
across arms. Isolate per-arm state under that same transition function. Realized
tool calls, responses and subsequent trajectory ordering caused by the intervention
are outcomes or mediators, not exogenous inputs required to be byte-identical.
Retain the complete trajectories and deterministically verify each response and
state transition against the committed environment. Unexplained environment or
input differences invalidate the comparison; do not erase legitimate behavioral
differences by forcing identical tool trajectories. Pair both arms
within each preregistered randomized ordering; different full input digests alone
do not prove a controlled comparison.
Include an additional purpose-neutral control matched to the Skill's rendered
token length and placement, while retaining the required no-Skill baseline.
Independently validate and bind the neutral control content and its own influence
assessment. Record rendered inputs and effective token budgets and establish
that no arm truncated, displaced or silently omitted common input. Evaluate
footprint effects separately from policy semantics; if rendering or neutral
control effects cannot be excluded, deny the proposed purpose attribution.
Before admitting a comparison, resolve the candidate's transitive influence
lineage in the case inputs and reject any case containing prior outputs or
derivatives influenced by that candidate, including Knowledge and Artifacts.
Apply this contamination check to reassessment as well as initial trials and
retain its evidence. A case with unavailable or ambiguous lineage cannot serve
as a control; acquire fresh uncontaminated cases instead of comparing two
already-influenced arms.
Bind outcomes, computed metrics, behavioral-shift summary, adequacy approval and
activation decision to the exact preregistered protocol digest and ledger
commitment. Validate the commitment's ordering before case selection or reveal,
and deterministically verify that the retained analysis used its exact metrics,
thresholds, subgroups and analysis version. Missing bindings or post-hoc analysis
substitution denies acceptance of the evidence.
Bind the exact context-builder version in classification and behavioral evidence,
both comparison arms and production applicability checks. A changed prompt
assembly version requires fresh applicable assessment even if all other
identities and resolved content remain unchanged.
Also bind the exact TaskContract version and canonical content digest in
classification, behavioral evidence and both comparison arms, and validate them
at production use. Changed constraints, success criteria or other contract
content require fresh applicable assessment even within the same task class.
Reconcile every selected case and arm assignment to its recorded outcome or
explicit terminal failure, including timeouts, refusals and cancellations. Commit
the missing-data, failure and retry handling rules before case selection; retain
all attempts and apply those rules without selectively dropping unsuccessful
pairs. Incomplete accounting denies evidence acceptance. Independently reassess
coverage and sensitivity after attrition under the committed adequacy criteria;
insufficient evidence requires new assessment, not a favorable summary of only
successful cases.

Bind the Skill's exact materialization state and the resolved instruction bytes,
summaries and referenced-asset digests actually supplied to the model. Bind an
evaluated applicability scope of task and candidate classes. At each use, require
the same materialization and covered scope; a different representation, newly
resolved asset or expanded scope requires fresh applicable evidence. A package
digest or organization-wide purpose authorization does not establish coverage
of untested materializations or task classes.
Bind an independently evaluated non-Skill context class with deterministic
membership criteria, or the exact canonical non-Skill input digest, in behavioral
evidence. Validate production Events, Knowledge, Artifacts, tools and additional
context against that boundary at every use and acceptance. New or changed inputs
outside it require reassessment; matching a broad task class alone is insufficient.
Record the concrete context digest and applicability decision for each use.
Bind the complete ordered set of co-materialized Skills, including every exact
version, materialization and resolved-content digest, in both comparison arms
and in the resulting applicability evidence. Evaluate the production composition,
with only the declared intervention removed from its paired baseline. Individual
Skill approvals do not establish safety of their combination. Adding, removing,
reordering or changing a co-materialized Skill requires fresh applicable evidence
before using the changed composition.

Bind the organization and exact policy version, authorized objective, and the
Skill's exact ownership/applicability scope. For Team-scoped Skills, bind the
Team ID and record revision, relevant roster/membership revision and the Agent's
current membership in classification, behavioral evidence and purpose decisions.
Validate those bindings during assessment, activation and every use/acceptance;
roster or membership changes require a fresh applicability decision and fresh
assessment wherever evaluated conditions change. Cross-Team use cannot inherit
the prior Team's authorization. Bind the
runtime-owned authorization decision that permits this declared purpose for
that scope. Resolve and validate those references at activation against the
declared applicability scope and policy; reusable activation does not require
an active Task and grants no permission for a particular execution. At every
production materialization or use, additionally resolve the concrete Intent,
Work and Task objective revisions and, when the Work is linked to a Goal, that
Goal's objective revision. For ad hoc Work, bind the absence of Goal linkage and
use the Intent/Work/Task objective; do not invent or require a Goal. Validate the
current linkage as well as any linked Goal revision. Require
a runtime-owned applicability decision that the Skill purpose serves that
specific work. Bind those exact work references and decision to the execution;
changed objectives require a fresh applicability decision and any newly required
evidence. Broad organization or task-class approval cannot substitute for this
work-specific check. Revalidate the same work bindings at every
materialization/use boundary; a purpose hash,
model explanation or evaluator assertion cannot authorize its own objective.
Missing, revoked, superseded or mismatched purpose authorization denies
activation and further use, quarantines any stale active version, and requires
a new applicable assessment and authorization. An already-active status cannot
bypass a later policy change or revocation.
At every materialization/use boundary, also resolve and validate the behavioral
evidence itself, its applicability and freshness, evaluator eligibility and
authorization, and independent provenance. Revoked, stale, unavailable,
unverifiable or mismatched evidence or evaluator eligibility denies use.
Quarantine the accepted Skill basis and its dependent uses when that basis is
expired, revoked or invalidated. A request outside otherwise valid scope, with
the wrong context or Team, instead denies and quarantines that attempted
execution and its outputs; it must not invalidate unrelated uses covered by
the still-valid basis. Apply this distinction to all quarantine rules here:
request mismatch alone cannot quarantine the shared Skill version. Restore an
invalidated basis only through new applicable evidence and authorization.
Bind an independently authorized, versioned, risk-appropriate freshness policy
to every assessment and classification, including a finite maximum age, explicit
expiry, trusted assessment time and revalidation triggers for distribution drift
and changed evaluator assumptions. Check the current policy and trusted runtime
time at every use and acceptance boundary. Missing policy, unverifiable time,
expiry or a triggered revalidation condition denies use until fresh evidence is
admitted; absence of a revocation is not a freshness determination.
Carry these exact Skill, composition, classification, authorization and evidence
references through the execution manifest and resulting outputs and proposed
effects. Before accepting a protected downstream action, including a tool call
or effect commit, resolve and revalidate their current validity, applicability,
freshness, evaluator eligibility and independent provenance. Missing lineage or
revocation after materialization denies the action and quarantines affected
outputs; a running execution or previously accepted output cannot bypass this
check. Apply the same check to recovered or replayed proposals before action.
Preserve the complete transitive behavioral-policy provenance closure in every durable
derivative, including copied or summarized results, Knowledge, Artifacts and
new Skills, and in executions consuming those derivatives. Resolve and validate
the originating classifications, authorizations and behavioral evidence at the
same use and acceptance boundaries even when the consumer has no direct
SkillRefs. Missing or unverifiable lineage denies use. Invalidating an originating
basis quarantines its dependent descendants from further use, publication or
delivery; copying, promotion or a new record identity cannot erase that basis.
Enforce finite runtime-owned limits on lineage depth, distinct references, edges,
total resolved bytes and ledger reads both when admitting derivatives and when
resolving them. Bind the versioned limits in the execution policy; missing limits
deny admission and use. Detect cycles and reject cyclic or over-limit closure
before materialization or protected acceptance, bounding work as each edge or
record is read rather than after full traversal. Never truncate lineage into a
successful validation; shared ancestors may be deduplicated without omitting
their validity checks.
Apply the same lineage and current-validity checks before result acceptance,
publication and delivery, including model-only text with no tool call or effect
commit. Deny publication or delivery and quarantine the affected result if any
required classification, authorization or behavioral basis has become invalid;
an earlier acceptance cannot authorize later delivery after revocation.
Independently validate the concrete production result, proposed selection and
effect against current applicable absolute policy and the specific work
objective at these same acceptance, publication, delivery and action boundaries.
Use deterministic checks where the policy is mechanically decidable and an
appropriately authorized independent judgment otherwise; unavailable or
inconclusive validation denies acceptance. Historical behavioral evidence does
not authorize a prohibited current outcome. Bind the exact output/effect digest,
policy and work revisions, checker eligibility and decision to the governed
acceptance record, alongside the evidence-validity checks below.
Serialize these validity checks and each governed acceptance record in one
authoritative ledger transaction with policy, classification and evidence
revocations. Bind the checked versions and ledger position to the accepted
operation; a revocation ordered first denies it. Dispatch external effects only
from that committed state through the existing effect boundary. A queued action,
publication or delivery requires its own current admission when dispatched;
recovery cannot treat a prior check as a reusable authorization. Do not claim
that ledger ordering can undo an external effect already committed before a
later revocation.

A persistent shift requires an explicit explanation tied to the authorized
objective and an independent
assessment of that explanation. Missing, mismatched or unexplained evidence
must deny activation. A changed Skill, purpose, model or relevant execution
conditions requires fresh applicable evidence rather than inheriting a prior
assessment. Evaluation evidence grants no capabilities or effect approvals.
Evidence reuse and non-discretionary classification require a runtime-verifiable
immutable or attested model deployment revision, bound alongside the reported
model identity and checked at every use and acceptance boundary. A mutable model
name alone does not establish continuity. If the provider cannot supply this
binding, deny behavioral-policy activation and use; fresh trials alone cannot establish that
an unobservable deployment change did not occur afterward. A changed revision
requires fresh applicable validation. This gate does not restrict existing
executions whose verified transitive behavioral-policy influence closure is empty. Compute
that closure from all content or references actually exposed to the model,
including direct Skills, independently classified non-Skill policies and all
derived inputs. Runtime-proven OMITTED or UNAVAILABLE
entries that expose neither content nor a reference are retained in the manifest
but do not add influence lineage. A state label alone is insufficient: any exposed
summary, identifier or reference remains subject to the gate, and later resolution
requires a new check before exposure. Apply this exposure rule to the lineage
validity gates above as well. Empty direct SkillRefs
alone do not establish this exemption. It does not imply that an adapter attests model
weights or deployment revisions.

This is an activation prerequisite, not an implemented evaluator or a guarantee
about unobserved model behavior. Current execution manifests require empty
`SkillRefs`; there is no general Skill materialization or activation runtime.
Lab may retain a `SKILL` promotion-candidate record, but nomination is not
activation and does not satisfy this evidence requirement. Keep activation
unavailable until its evidence validation, independent review, exact version
binding and rejection tests are implemented.

## Current implementation status

### Organization freeze and model admission

A committed organization freeze prevents new inference reservations for intake,
planning, and Task execution. The ledger reads the exact tenant's admitted
freeze state in the same transaction as budget reservation. Missing freeze
history means no freeze has been set; malformed or mismatched authority fails
closed. A later admitted release permits new reservations under the existing
inference policy. Restart preserves the freeze, and recovery rejects any
reservation admitted while that organization was frozen.

An inference reservation that commits before the freeze remains an admitted
call. Its usage or uncertain outcome must still be reconciled while frozen;
the freeze does not establish that the request was never sent. This admission
boundary does not cancel an already-dispatched provider request or implement
execution-, Agent-, or Work-scoped active quarantine. Active context cancellation,
coordination suspension, and controlled release remain prerequisites for a
broader runtime security hold before long-running high-autonomy execution.

| Control | Status |
|---|---|
| Separate `CODE_INTRODUCTION` and `EXECUTION_SURFACE_MUTATION` contracts | Implemented |
| Exact artifact, staged-promotion, influence, trajectory, and capability-closure fingerprinting | Implemented |
| Transactional consequential-capability and trajectory checks | Implemented |
| Runtime-owned execution-manifest influence validation | Implemented |
| Exact tool-definition digest contract | Fail-closed prerequisite; dynamic tools are unavailable |
| Effective environment attestation contract | Fail-closed prerequisite; no production hostile-code sandbox exists |
| Execution-private writable workspace contract | Implemented domain boundary; no mutable coding runtime exists |
| Provenance independence for protected use | Implemented conservatively; Agent-only corroboration is ineligible |
| Independent Skill behavioral policy-integrity evidence | Required before activation; evaluator and activation runtime are not implemented |
| Arbitrary shell, package installation, external code, container image, MCP, plugin, or executable Skill execution | Unsupported and denied |
| Staged adaptive editing and promotion runtime | Deferred until a write-capable coding runtime exists |

Package popularity, age, stars, download count, and model reputation are not
authority. Future execution must prefer exact identity, version, bytes,
authorization, effective environment, and narrow brokered credentials.
