# Repository change control

## Active control

GitHub repository ruleset [`Protect main`](https://github.com/dominicnunez/agentos/settings/rules/21749124) is active for the default branch (`main`).

Observed on 2026-08-28 through GitHub's repository ruleset API:

- ruleset ID: `21749124`;
- enforcement: `active`;
- target: `~DEFAULT_BRANCH`, currently `main`;
- changes must arrive through a pull request;
- the pull-request branch must be current with `main`;
- review conversations must be resolved;
- required approving reviews: `0`;
- required GitHub Actions checks:
  - `CI verification (pull_request)`;
  - `Dashboard frontend`;
  - `Release artifact verification`;
- branch deletion and non-fast-forward updates are denied; and
- repository administrators have an `always` bypass for emergency recovery.

The live machine-readable configuration is available from GitHub's [ruleset API](https://api.github.com/repos/dominicnunez/agentos/rulesets/21749124). GitHub's legacy branch-protection fields do not describe repository rulesets; the ruleset API and observed rule-suite results are the authoritative evidence for this control.

## Normal change path

1. Create a branch from the current `main`.
2. Make a bounded change and retain its reviewable commit history.
3. Open a pull request.
4. Bring the branch current with `main`.
5. Resolve every review conversation.
6. Require the exact configured checks to pass at the merge head.
7. Merge through GitHub without selecting an administrator bypass.

Zero required approving reviews is deliberate while the project has one accountable maintainer. Automated review and the project's security-first final review remain development evidence, but neither is misrepresented as an independent organizational approval.

## Emergency bypass

The administrator bypass exists only to restore repository availability when the normal pull-request path cannot operate safely. Administrator identity alone is not routine approval authority.

Before using the bypass, record an Issue containing:

- the failure that prevents the normal path;
- why waiting would cause greater security, integrity, or recovery harm;
- the accountable person invoking the bypass;
- the exact target branch and proposed commit;
- the requirement being bypassed;
- the smallest permitted scope and expiry condition; and
- the rollback or forward-recovery plan.

After use:

1. record GitHub's observed result and exact resulting commit;
2. restore normal enforcement as soon as possible;
3. open a reviewed follow-up pull request or corrective-action Issue;
4. run the checks that could not run before the bypass;
5. review whether the bypass was necessary and effective; and
6. retain the decision and evidence in the applicable AIMS records.

A bypass does not authorize a release, deployment, external publication, certification claim, financial consequence, sensitive-data expansion, or any other separately governed action.

## Validation status

- Active configuration: observed and verified on 2026-08-28.
- Conforming pull-request path: this documentation change supplies the first post-enforcement evidence once its exact merge head passes every required check and GitHub accepts the merge without bypass.
- Nonconforming-update rejection: still requires a credential or actor that cannot use the repository-administrator bypass. Testing with the current administrator identity would not prove fail-closed behavior and could change `main`, so that test remains intentionally unperformed until a bounded non-bypass actor is available.

This evidence supports repository change control only. It does not establish AIMS conformity or ISO/IEC 42001 certification.
