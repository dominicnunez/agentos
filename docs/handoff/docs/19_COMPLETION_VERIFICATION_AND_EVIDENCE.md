# Completion, Verification, and Evidence

## Completion invariant

Agent publishes `CANDIDATE_COMPLETE`. It cannot directly set `TASK_VERIFIED_COMPLETE`.

## CompletionContract

Versioned task verification criteria:

- deterministic checks;
- executable tests;
- objective measured outcomes;
- artifact requirements;
- forbidden effects;
- independent/human judgment only where unavoidable.

Worker cannot silently rewrite these criteria.

## Verification preference

Use, in order where applicable:

1. deterministic environment predicate;
2. executable test/known answer;
3. objective measurable outcome;
4. formal rubric over observable evidence;
5. independent model adjudication;
6. human judgment.

Do not create fake deterministic proxies for inherently subjective quality.

## Evidence

Distinguish:

- agent-claimed evidence/content;
- runtime-attested observation/action/tool result.

Artifact/provenance references are structured because completion/audit/safety can depend on them.

## Independent tests

Important acceptance should not depend solely on tests authored by the implementation agent.

## No-action can be correct

“no change needed,” “wait,” and “insufficient evidence” are legitimate outcomes.
## v4.1.1 — nondeterministic operator judgment

When objective verification is unavailable, the remaining judgment must be explicitly attributed to an authorized operator.

- `INDEPENDENT_ADJUDICATION` may be performed by an appropriately authorized independent agent/operator.
- `HUMAN_JUDGMENT` is performed by a human operator.

Record the operator, evidence reviewed, and method. Do not render an operator judgment as deterministic verification.

Human consequence boundaries continue to determine when only a human may decide.
## v4.2 — execution/tool/effect evidence

Completion/audit may reference:

- `ExecutionContextManifest` to establish what information was actually available;
- `ToolOutcome` to establish observed tool behavior/postconditions;
- `EffectObligation` confirmation evidence for consequential external effects.

A model claim that an external action happened is weaker than confirmed runtime/effect evidence.
