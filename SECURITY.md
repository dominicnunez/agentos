# Security policy

## Supported code

Agent OS has no published release yet. Security fixes target the current
`main` branch and the latest V1 release candidate after one exists. Older
unreleased commits and moved development branches are not supported versions.

## Report a vulnerability privately

Do not open a public issue for a suspected vulnerability. Use the repository's
[private vulnerability reporting](https://github.com/dominicnunez/agentos/security/advisories/new)
channel so credentials, exploit details, and affected data are not disclosed.

Include, when available:

- the affected commit, binary version, and deployment shape;
- the trust boundary and principal role involved;
- a minimal reproduction that does not contain live secrets or personal data;
- the observed and expected fail-closed behavior;
- the practical impact and whether exploitation is ongoing; and
- any suggested regression test or containment.

Do not test against systems or data you do not own or have permission to use.
Do not include bearer credentials, provider keys, production ledgers, or other
sensitive material in a report.

Receipt, severity, remediation, and coordinated disclosure are evaluated per
report; this project does not promise a fixed response or publication deadline.
Non-sensitive bugs and feature requests may use normal GitHub issues after the
issue tracker is enabled.

## Security posture

The V1 runtime treats model output and operator text as untrusted, separates
work, completion-review, and approval credentials, and keeps real providers and
consequential adapters disabled by default. See
[docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) for trust boundaries, controls,
known residual risks, and verification expectations.
