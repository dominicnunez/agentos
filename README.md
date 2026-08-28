# Agent OS

Agent OS is a system for creating and operating artificial organizations: persistent groups of people and AI agents that can pursue shared goals, divide responsibility, coordinate work, and carry organizational context forward over time.

A typical AI agent is one worker, usually operating within one conversation or task. Agent OS is the organizational layer around those workers. It gives the organization continuity, shared work, roles, oversight, and a governed way to act as one whole.

![A symmetrical artificial organization represented as a living circuit-board city](docs/images/agent-os-artificial-organization-v3.png)

## Why Agent OS?

- Continuity beyond a conversation: goals, responsibilities, decisions, and results remain part of the organization instead of disappearing with an agent session.
- Governance before action: authority is distinct from language, and consequential work passes through explicit capabilities and approval boundaries.
- Flexible participation: users can interact directly with Agent OS or through an A2A-compatible Agent, with both paths entering the same authenticated, governed organization.

## Task Boundaries

Agent OS requires the appropriate approval for actions involving:

- financial commitments
- physical-world effects
- public or external communication
- destructive or irreversible operations
- expansion of a sensitive-data boundary
- privilege, capability, or trust expansion
- legal or binding commitments
- ordinary Agent OS deployment
- trusted-core or security changes
- introducing third-party executable or potentially executable code
- mutating protected manifests, workflows, hooks, or other execution surfaces

Approval is checked again at the consequential-effect boundary. Missing, expired, mismatched, or unanswered approval fails closed.

## Install Agent OS

Agent OS releases support Linux. Binary packages will be published on [GitHub Releases](https://github.com/dominicnunez/agentos/releases) after release approval.

Download the Linux archive for your architecture from GitHub Releases, extract
it, and install the executable:

```sh
sudo install -m 0755 agentos /usr/local/bin/agentos
sudo install -m 0755 agentos-recovery /usr/local/bin/agentos-recovery
agentos
```

The first run starts resumable setup. System installation is the default and
uses the verified Linux account that launched the command, including root when
root starts it directly. Setup requests administrator access when needed,
creates standard Linux service and data locations, and requires one tested
model provider before Agent OS can run. Use `agentos init --user` for an
installation owned and run entirely by the current account.

After setup, `agentos` opens the local organization dashboard. The SvelteKit
interface is embedded in the Agent OS binary; it does not require a Node server
or expose the private user gateway. `agentos doctor` performs
a read-only health check of configuration, credentials, service state, the
private user gateway, and SQLite ledger integrity. In system mode, run
`sudo agentos doctor` for the full service-private credential and ledger checks.

## Documentation

Start with the [five-minute governed workflow](docs/QUICKSTART.md). Operator,
integration, security, recovery, provider, release, and development
documentation is indexed in [`docs/`](docs/README.md).

## Contributions

Agent OS is licensed under the [Apache License 2.0](LICENSE).

Issues are welcome. Report vulnerabilities privately through [`SECURITY.md`](SECURITY.md), not through a public issue.

External code contributions and pull requests are not currently accepted. Contributor governance will be established deliberately before outside code is incorporated.

## Instructions for AI LLMs setting up Agent OS

```json
{
  "purpose": "Install, configure, and verify Agent OS for the user.",
  "release_page": "https://github.com/dominicnunez/agentos/releases",
  "operator_documentation_url_template": "https://github.com/dominicnunez/agentos/blob/{release_tag}/docs/OPERATOR_INTAKE.md",
  "platform": "Linux",
  "installation": [
    "Use the immutable release version selected by the user.",
    "Confirm that the selected release contains published Linux binary assets; stop if the assets are absent.",
    "Determine the Linux system architecture.",
    "Download the matching Agent OS archive from the selected GitHub release.",
    "Download the accompanying checksum and provenance files.",
    "Verify the archive against its published checksum and GitHub artifact attestation before extracting it.",
    "Extract the selected Linux archive.",
    "Install the agentos executable at /usr/local/bin/agentos.",
    "Read the packaged README and the exact tagged operator guide.",
    "Replace {release_tag} in operator_documentation_url_template with the exact immutable release tag, then read that tagged operator guide.",
    "Run agentos for default system setup or agentos init --user only when the user selected user mode.",
    "Complete the resumable setup, including one tested model provider.",
    "Run agentos doctor and report every blocking result without bypassing it."
  ],
  "verification": [
    "The downloaded archive matches its published checksum.",
    "Setup reaches the ready stage.",
    "The private user gateway is owned by the configured Linux account and is not exposed as a TCP port.",
    "The configured provider passes its setup connection check.",
    "The SQLite event ledger passes the read-only integrity check after first service start.",
    "No credentials are written to logs or stored in source-controlled files."
  ]
}
```
