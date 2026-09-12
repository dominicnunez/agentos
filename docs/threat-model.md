# 1. Overview

Agent OS is a Linux-only Go modular monolith for operating persistent organizations of human and AI actors. Its authoritative state is an append-oriented SQLite event ledger with versioned projections and a SHA-256 event-integrity chain. Work enters through an installation-owner dashboard/private Unix-socket API or an optional A2A JSON-RPC gateway. The runtime normalizes intent, requires explicit confirmation, creates bounded Task DAGs, dispatches supported execution mechanisms, records results, and separates completion review and consequential-effect approval from model output.

The runtime supports multiple configured provider connections, including multiple accounts using the same provider and model. Explicit installation rules or a policy-constrained broker select connections independently for intent normalization, planning, and Task execution. Supported production model adapters remain OpenAI API and Codex subscription. Exact connection identities, immutable execution profiles, routing requirements, and shared organization budgets constrain selection and dispatch. Automatic cross-provider fallback and general tool-using model execution are not implemented.

The principal assets are organization, Mission, Goal, Work, Task, identity, knowledge, approval, capability, freeze, effect, completion, inference-policy, budget, and audit records; provider and A2A credentials; private prompts/results/artifacts; and release artifacts. V1 has no production effect-writing adapter, and its model adapters are intentionally model-only. Prompt injection cannot directly invoke a shipped shell or external-effect mechanism, but it can influence bounded work, consume inference budget, disclose selected context to a provider, poison later decisions, or mislead an operator.

There is no published release yet according to `SECURITY.md`, so release-publication risk is prospective. The intended default is a single-machine system installation with a restricted `agentos` service account and one verified Linux owner. User-mode installations run with the owner's authority. Organization scoping remains security-relevant for external actors, ledger objects, model context, and budgets, without implying strong hosted multi-tenant isolation.

This model describes code commit `1cc542b3729b4dbf7165cf62d63df4f6299ec57d`, including its CI controls. Implemented controls, incomplete controls, and prerequisites for unsupported execution mechanisms are distinguished below. A passing test is evidence for a control, not proof of comprehensive security.

# 2. Threat model, Trust boundaries and assumptions

## Assets and security objectives

* Preserve organization-confined ledger state, results, artifacts, knowledge, approvals, and model context.
* Prevent content or model output from becoming identity, capability, policy, execution, approval, completion, or effect authority.
* Bind every model invocation to its authorized organization, purpose, connection, model, profile, source context, and resource reservation.
* Prevent provider selection from weakening confidentiality, locality, capability, or shared-budget requirements.
* Protect provider, A2A, reconciliation, TLS, and dashboard credentials, including separation between configured provider accounts.
* Prevent duplicate or unauthorized consequential effects and conservatively account for uncertain effects and provider calls.
* Preserve exact evidence and valid provenance across dispatch, result admission, completion, Goal evaluation, and recovery.
* Contain affected live work after a committed organization freeze without treating cancellation as proof that remote computation stopped or external effects were reversed.
* Detect inconsistent ledger modification, stale or cross-organization admissions, and unsupported or partially migrated state.

## Trust boundaries

1. **Setup/elevation to installation:** Setup binds authority to the verified invoking Linux account. System setup accepts direct root or validates `SUDO_UID`, `SUDO_GID`, and `SUDO_USER` through fixed-path `getent`. Configuration, provider catalogs, routing policies, inference limits, A2A registries, certificates, and reconciliation registries are operator-controlled trusted inputs. Their declarations do not independently prove provider capabilities, effective containment, or backend privilege.

2. **Local process to private gateway:** The gateway is an absolute-path Unix socket, mode `0600`, owned by the configured user. Linux `SO_PEERCRED` is captured at accept time and checked by local-access authorization. Request headers cannot establish this identity. The restricted service account does not acquire owner approval authority merely by running the service.

3. **Browser to dashboard bridge:** An owner-launched process exposes an ephemeral IPv4 loopback HTTP listener, separate from the private gateway. A 256-bit one-time bootstrap token establishes an expiring bearer session. Browser content, extensions, websites, and other local processes are untrusted.

4. **Network to A2A:** Requests, headers, JSON-RPC IDs, message text, Task IDs, extension metadata, and timing are attacker-controlled when A2A is enabled. A reviewed registry maps a server-side bearer to an exact actor, organization, role, scope, expiry, concurrency limit, and rate limit. Remote exposure requires explicit enablement and TLS.

5. **Content to authority:** Operator text, A2A text, model output, dependency results, knowledge content, and artifacts are untrusted data. Runtime-generated event envelopes, admitted identities, source bindings, fingerprints, authorization records, and typed decisions are control data. Authentication of an instruction does not authenticate an artifact it references. Source handles establish invocation membership, not authority or factual truth.

6. **Persistent knowledge to model context:** Stored knowledge can influence future executions. New model context requires independently admitted factual-reference classification and exact valid source lineage. Reusable behavioral instructions do not become eligible merely because they are stored as Knowledge, feedback, or reference material.

7. **Runtime to provider connection/subprocess:** Bounded context and credentials cross into the selected OpenAI or Codex connection. Account identity is distinct from provider/model identity. The runtime depends on provider transport and availability but does not trust provider output for authority or semantic correctness. Disclosure of authorized context to the selected cloud provider is inherent. Codex is an operator-selected executable and dependency.

8. **Committed authority to live execution:** Organization freeze records are durable authority; running contexts, provider calls, scheduler activity, and output publication are separate runtime mechanisms. Containment must observe committed history and retain an intervening hold even if a later release is visible. Cancellation is cooperative and cannot be atomic with remote receipt.

9. **Runtime to persistence and inspection:** SQLite and private artifact files hold authoritative state and sensitive content. Event-chain and admission validation detect inconsistent retained state but do not protect against complete privileged replacement. Dashboard inspection, incident replay, and evidence exports are separate disclosure surfaces that must expose only authorized bounded projections.

10. **Effect/reconciliation and executable material:** External destinations and configured status services are separate trust domains. Code introduction and protected execution-surface mutation are distinct governed consequences. A declared tool capability set does not prove that its backend credential has no additional authority. Production effect writers, dynamic credentialed tools, and hostile-code execution remain unavailable.

11. **Source/build/release:** Repository code, pinned workflow definitions, lockfiles, and build scripts are developer-controlled. Pull-request content and dependencies are untrusted supply-chain inputs. CI and reproducibility provide evidence; they do not establish production authorization or signed release identity.

## Assumptions and exclusions

Root, the configured owner account, the kernel, and trusted service utilities are assumed uncompromised. Root can replace the binary, socket, ledger, credentials, configuration, or peer-credential result and is out of scope as an adversary. The owner deliberately possesses organization-management, review, and approval authority; preventing that owner from making an informed but bad decision is not an authorization goal.

This exclusion does not excuse a bug that lets an ordinary website, external actor, model response, or lower-privileged process obtain owner authority or redirect approved context without authorization.

Developer fixtures and fake adapters are not production network paths. V1 does not claim hostile-code container/process isolation. Arbitrary shell execution, package installation, external code, container images, MCP tools, plugins, and executable Skills cannot be enabled merely through plan text or configuration declarations. General Skill activation and its behavioral-policy evaluator are not implemented.

Strong hostile-tenant isolation, host-administrator resistance, encrypted general ledger/artifact storage, external audit anchoring, comprehensive incident response, and prevention of covert timing channels are outside the present implementation claim.

# 3. Attack surface, mitigations and attacker stories

## Local gateway and dashboard

A malicious local user may try to connect to the private API and approve effects. Socket ownership and mode, socket-activation validation, kernel peer credentials, bounded concurrency, and request limits reject unauthorized UIDs. Compromise of the owner account defeats this boundary by design.

A malicious website may target loopback through CSRF or DNS rebinding, and a local process may race bootstrap establishment. The bridge requires the exact IPv4 loopback Host, exact Origin for bootstrap, rejects conflicting Origins afterward, uses no cookies or CORS grant, and authorizes an allowlist of routes. Bootstrap/session credentials are kept out of launcher arguments and terminal output. The bootstrap page is private, bootstrap use is bounded, and the session expires. CSP, frame denial, no-referrer, `nosniff`, and escaped frontend rendering reduce session theft.

An XSS or malicious extension that steals the bearer can exercise the available owner UI authority. Severity depends on the reachable actions and disclosed data, not merely on requiring an open dashboard. Durable intake, input, approval, and review records support recovery from lost responses without treating a repeated request as a new decision.

## A2A, exact JSON, and organization authorization

A remote attacker may guess or steal a bearer, replay it, enumerate Task IDs, cross organizations, submit authority-shaped JSON, or exhaust inference budget. The A2A gateway enforces narrow body limits, JSON content type, supported methods and extensions, strict schemas, a restricted message-part shape, and recursive rejection of authority-like fields. Actor credentials have minimum length, uniqueness, expiry, status, rate, and concurrency requirements. In-memory authentication uses hashed lookup keys. Capabilities are checked separately for submission, confirmation, status, result, and input; Task resolution is organization-scoped and exposes only runtime root Tasks.

Shared security-boundary JSON decoding rejects duplicate members, escaped duplicate spellings, case aliases for closed fields, unknown closed-schema fields, invalid UTF-8, excessive nesting, and trailing top-level values. Domain limits may be stricter. Open provider metadata and intentional raw data remain open where required; the implementation does not claim every JSON decoder is a closed schema. Decoder failures use fixed categories instead of echoing attacker-controlled names or values.

Bearers remain replayable while valid until expiry or an effective registry update/rotation. Internet-edge volumetric protection is external. Per-process limits do not prevent aggregate abuse by many provisioned actors. An organization-binding or capability error can expose durable results or spend another organization's provider budget even when the submitted JSON is well formed.

## Prompt injection, planning, and execution admission

An attacker may embed instructions to invent authority, switch Goals, substitute a predecessor Work item, alter reviewed execution mode, expand scope, create graph bombs, or claim completion. Intake and planning use bounded structured contracts, exact source provenance, explicit confirmation, closed output schemas, a 16-Task ceiling, execution-kind allowlists, and DAG validation. Goal, Mission, and replacement references are rechecked transactionally. Replacement Work creates fresh reviewed state and does not inherit prior approvals, capabilities, effects, or completion evidence.

Production model requests separate runtime instructions, operator/task messages, and low-privilege strategy, knowledge, coordination, and dependency data. Runtime-issued source handles bind the organization, invocation, complete input, and message position. Digests and fingerprints bind the structured request. Unknown or substituted handles fail validation, and adapters without structured-input support are rejected rather than receiving an unbound flattened prompt.

Transport envelopes and role separation are defense in depth. Neither current transport supplies a native privilege level below user for every data message, and classification does not prove model resistance to prompt injection.

Execution admission binds the accepted Intent, current strategic revisions, exact Agent/blueprint/profile, pending Task revision, and transaction-selected context to the execution manifest and start transition. Shared coordination is bounded and limited to admitted same-Work peers. Model-authored evidence requires an exact running execution and remains an untrusted claim. Completion and Goal achievement require their own deterministic evidence and authorization checks.

OpenAI disables model tools and automatic billable retries and constrains response identity and transport behavior. Codex uses private adapter/turn state, a sanitized environment, requested read-only confinement, disabled tool features, and rejection of side-effect stream items. These measures do not establish hostile-code isolation. Prompt injection can still produce poor bounded work, bias review, or misuse context already supplied to the provider.

## Knowledge, Skills, and persistent behavioral influence

An attacker may try to preserve malicious instructions as institutional knowledge, forge independent validation, omit an invalidated ancestor, or reuse stale knowledge after execution starts.

New execution context requires exact `ACTIVE` knowledge revisions with independently human-admitted `FACTUAL_REFERENCE` classification. A human author cannot classify their own candidate. Missing classification, behavioral-policy classification, or invalid transitive lineage excludes the record from new model context. Ordinary search and historical records retain their own contracts and do not silently acquire this eligibility.

Selection is organization- and Agent/Team-scope constrained, deterministic, bounded, and bound into the execution manifest. Knowledge and lineage validity are checked at relevant inference, outcome, Task/Work completion, and Goal-evaluation boundaries. Historical replay evaluates the state at those boundaries rather than rewriting history using present-day classifications.

Factual classification is accountable human judgment, not a semantic proof or keyword filter. Eligible knowledge can still be false or adversarially persuasive. Knowledge cannot grant capabilities, authorize effects, or certify completion.

Skills and other reusable behavioral-policy material require separate integrity and activation evidence. Current execution manifests require empty Skill references; a Lab promotion candidate is not activation. The broader evaluator and activation runtime remain prerequisites. Unsupported policy materialization must not be described as an implemented, proven-safe Skill system.

## Provider routing, credentials, and inference spend

An attacker may try to select a more permissive account, substitute credentials for another connection, exploit a model-generated Task key, bypass locality/data-class restrictions, use stale catalog evidence, or reset spending limits by changing providers.

Each named connection binds an exact provider, model, profile, reviewed policy, and credential source. Runtime composition restricts each adapter to its configured secret reference and private connection directory; mutable Codex credential stores cannot be shared. Tasks retain immutable assigned profiles and routing requirements. Planning and normalization select a connection for each admitted attempt.

Routing intersects Task-specific requirements with mandatory defaults. Preferences do not override eligibility. Provider allow/deny rules, locality, data class, capability, capacity, cost, and applicable health/evaluation constraints are checked against reviewed policy. Catalog claims and health/evaluation references are operator-reviewed inputs, not independently measured runtime facts. Configuration cannot activate unsupported transports.

Selection is advisory. Dispatch revalidates the binding and reserves resources transactionally. Shared organization token, monetary, and concurrency limits include other configured accounts and relevant historical usage. Account addition, model changes, policy changes, and accounting-window transitions must not erase outstanding charges. Historical validation reconstructs admission-time eligibility; a later refund cannot justify an earlier overspend.

Each request permits one provider invocation. There is no implicit cross-account fallback. Fresh application attempts require the applicable lifecycle and non-dispatch/retry evidence; cancellation or a new request ID alone does not authorize repetition.

Completed and validated terminal responses retain applicable observed usage. Requests proven unsent release reservations; uncertain calls retain conservative charges. A failed or incomplete response is not successful model output or completion evidence.

Provider failures are reduced to fixed runtime-owned categories before entering new work evidence. Original diagnostic text and error chains are discarded while required cancellation, non-dispatch, and usage facts are retained. This does not redact model output, setup diagnostics, or historical ledger contents.

## Approval, capabilities, effects, and executable-code boundaries

A worker may attempt approval laundering, principal-kind substitution, stale approval reuse, effect substitution, cumulative escalation, or duplicate send. Authority requires exact current leases matching the principal, originating Task, action, resource, and scope; positive authority does not inherit. Approvals bind the organization, Task, effect obligation, boundary, fingerprint, expiry, and decision identity.

Transactional effect admission rereads approval, freeze, and capability authority, consumes applicable single-use approval, and persists `ATTEMPTED` before adapter invocation. Uncertain effects require reconciliation rather than blind resend. A ToolOutcome or nonempty result is not proof of an external effect.

`CODE_INTRODUCTION` and `EXECUTION_SURFACE_MUTATION` are separate protected consequences. Generic shell or file-write authority does not satisfy either. Their contracts bind exact artifact bytes or before/after state, staged-promotion evidence, runtime-known influence references, declared consequential capabilities, and Task-local cumulative effect context.

Declared capability closure does not prove the effective privilege of a backend credential. Backend-principal authority attestation, production effective-environment verification, and hostile-code isolation remain prerequisites for future credentialed tools and external-code execution. No production effect-writing adapter, general dynamic tool runtime, or mutable coding runtime is present.

The optional reconciliation adapter uses an operator-reviewed HTTPS endpoint and fixed read-only request contract with redirects disabled. Its configured endpoint is not restricted from private network addresses. An SSRF or token-disclosure report must establish how an attacker controls the destination or request beyond the trusted configuration boundary.

## Organization freezes and active containment

An attacker or misaligned execution may try to continue inference, publish output, coordinate with peers, or resume old work after a freeze or a rapid freeze/release sequence.

Organization freezes are durable and checked during new inference and protected-operation admission. The current implementation also cancels matching cooperative live contexts, observes committed hold history, and retains an intervening freeze across subsequent release. Dispatch and publication checks suppress affected ordinary output and completion candidates. Interrupted Tasks can enter a durable suspended state requiring security reconciliation. Planning and normalization bind their attempts and interruption state to durable context records. An independent watchdog requests cancellation when authoritative containment state cannot be observed within its bounded window.

Authority observation uses a separate database connection so unrelated writer validation does not block safety reads. Actual database unavailability still triggers bounded local cancellation; the separate reader does not bypass committed authority or extend the watchdog's deadline.

Accounting and interruption evidence remain necessary while frozen. Actual returned usage is retained; definitely unsent calls require appropriate evidence before zero-charge reconciliation. A committed completion-verification boundary can support recovery of that already-verified transition without rerunning the handler.

This is partial active containment, not comprehensive quarantine. Exact authorized release and suspended-task resumption, the complete coordination/output and external-effect audit, and complete dispatch/cancellation timelines and historical enforcement remain unfinished under issue #178. Execution-, Agent-, and Work-scoped quarantine is not claimed.

Local cancellation cannot be atomic with provider receipt, force a noncooperative remote service to stop, or reverse an external effect. The watchdog bounds the local cancellation decision; it is not a hard guarantee of remote termination. These limits must remain explicit before broader long-running or high-autonomy execution is enabled.

## Secrets, artifacts, persistence, inspection, and recovery

Secrets use protected credential sources, including systemd credential loading and authenticated encrypted Codex state with a separately protected key. Sensitive readers enforce relevant ownership, permissions, regular-file, size, symlink, and race checks. Credentials still exist in process memory and provider requests.

Artifacts are size/count bounded, content-addressed, private, origin-labelled, and MIME-sniffed. Names cannot authorize path traversal. Artifacts are not malware-scanned and must remain untrusted to future renderers or execution mechanisms.

SQLite uses parameterized access, schema/application-ID checks, ordered migration, and event-coupled projection validation. A one-to-one SHA-256 chain binds exact stored event bytes and ordering. Startup, verification, backup, and restore detect inconsistent chains and invalid causal admissions. Recovery also validates authority, identities, organization scope, manifests, and applicable historical accounting and completion rules.

Cancelled queries release their database resources, and private in-memory authority survives discarded working or observation connections. These controls preserve observable committed state during cancellation; they do not make an unavailable database authoritative or provide durable storage for an in-memory ledger.

The chain is not a signature or external checkpoint. Removing a valid suffix with its integrity records can leave an internally consistent shorter history. A sufficiently privileged attacker can replace the database and recompute the chain. At-rest confidentiality, secure deletion, externally anchored rollback detection, and signed audit attestation remain external or unimplemented.

Governance inspection, incident replay, and evidence export are authenticated local-owner surfaces with organization scoping, bounded payloads, and restricted projections. They do not expose a general raw-ledger query interface. Replay reports evidence relationships, not proven root cause, and does not re-execute work. Export checksums detect byte changes but do not establish signed origin, control effectiveness, or certification.

Sustained valid submissions, artifacts, histories, and expensive verification can still consume disk or processing capacity. Operational capacity monitoring and filesystem quotas remain necessary.

## Setup and supply chain

Setup faces wrong-owner binding, PATH substitution, symlink replacement, unsafe service units, and partial installation. Critical commands use fixed paths; sensitive writes are atomic; directory ownership, modes, executable provenance, and runtime locations are validated. Systemd applies a dedicated account and restrictive service settings. User mode intentionally retains the owner's authority.

CI pins actions and tool versions, controls frontend dependency installation, audits dependencies, verifies embedded assets, exercises race/adversarial tests and bounded fuzzing, checks architecture, and builds reproducible archives with checksums, SBOMs, licenses, provenance, and corresponding source.

Ordinary documentation-only changes skip code-related steps while retaining document validation and required job results. The classifier checks a complete local Git diff, including deleted paths and file modes; scripts, schemas, workflows, dependencies, configuration, executable files, and unknown paths still require full checks. Uncertain classification also requires full checks. Release verification fails when its frontend prerequisite does not succeed. Required reviews remain independent, including security review for threat-model changes. The classifier and workflow definitions remain trusted source inputs subject to review; adding a code consumer of an allowed document path requires revisiting that classification. See `docs/development/ci.md` for the exact scope.

These checks do not prove live-provider behavior, deployment safety, or absence of vulnerabilities. Provenance is unsigned, publication is separately controlled, and compromise of repository administration, builders, toolchains, or dependency sources remains a supply-chain threat.

# 4. Criticality calibration

Severity follows the demonstrated attacker-controlled path, crossed trust boundary, reachable authority, affected data, and deployment configuration. Unsupported future adapters must not inflate current exploit impact; their absence must not excuse present authorization, confidentiality, or durable-control corruption.

## Critical

* Remote or low-privileged compromise leading to root/system-owner code execution, broad credential theft, or arbitrary modification of trusted service binaries/configuration.
* A release-pipeline compromise producing distributed backdoored binaries under the expected trusted release identity.
* A ledger, approval, or execution-boundary bypass enabling arbitrary high-impact irreversible effects when a production effect mechanism is actually available.

Critical generally requires broad compromise, trusted release compromise, or demonstrated high-impact execution/effects. Prompt injection alone is not Critical in the current model-only runtime. Priority to finish a prerequisite before future high-autonomy operation is distinct from current vulnerability severity.

## High

* A2A authentication or authorization bypass exposing another organization's private results, permitting victim-budget inference, or confirming/continuing Work as another actor.
* Provider-routing or account-binding bypass disclosing sensitive context to an unauthorized destination, crossing credential boundaries, or materially defeating organization-wide resource authorization.
* Dashboard bootstrap/session compromise or XSS that enables attacker-controlled use of owner approval, review, or sensitive-data access.
* Capability, approval, provenance, or completion-admission bypass granting durable authority or accepting attacker-controlled evidence as trusted completion.
* Provider confinement failure allowing model-controlled shell, filesystem, or network execution under the service account.
* Freeze/release or suspended-execution bypass with demonstrated access to substantial continued computation, sensitive publication, or consequential operations.

## Medium

* Authenticated denial of service through bypassed request, Task, history, token, or response bounds; persistent disk exhaustion; or meaningful bounded inference overspend.
* Knowledge poisoning or prompt injection that influences bounded work or operator judgment without bypassing runtime authorization or disclosing highly sensitive data.
* Partial containment failures whose demonstrated impact is continued bounded model work or suppressed-output handling, without a higher-impact execution path.
* Artifact or inspection-output confusion that materially misleads review without obtaining owner authority.
* Recovery-validation gaps requiring restricted database write access, where the attacker gains meaningful impact beyond that access itself.
* SSRF through configuration writable by a delegated actor who was not authorized to choose network destinations.

## Low

* Minor metadata disclosure, bounded error/timing differences, or terminal/log manipulation without credential or authority impact.
* Missing hardening headers on a non-browser response or harmless acceptance differences without a demonstrated security consequence.
* Local denial of service requiring the configured owner's own account, absent a meaningful additional trust-boundary crossing.
* Claims requiring developer-only fixtures, deliberate root-controlled configuration, or execution mechanisms absent from V1 are not applicable to the production threat model unless a real attacker-controlled path is established. Any independently demonstrated low-impact weakness should be assessed on its actual effect.
