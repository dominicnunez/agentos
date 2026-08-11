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

- financial commitments;
- physical-world effects;
- public or external communication;
- destructive or irreversible operations;
- expansion of a sensitive-data boundary;
- privilege, capability, or trust expansion;
- legal or binding commitments;
- ordinary Agent OS deployment;
- trusted-core or security changes.

Approval is checked again at the consequential-effect boundary. Missing, expired, mismatched, or unanswered approval fails closed.

## Install Agent OS

Agent OS releases support Linux. Binary packages will be published on [GitHub Releases](https://github.com/dominicnunez/agentos/releases) after release approval.

To build Agent OS from Git, install Go 1.26.5 and Git, then run:

```sh
git clone https://github.com/dominicnunez/agentos.git
cd agentos
make test
```

Before starting Agent OS, configure at least one authenticated Human or A2A gateway and its environment-backed credential by following the [operator intake guide](docs/OPERATOR_INTAKE.md). Agent OS fails closed when neither gateway is configured. Then run `make run`.

The committed `Makefile` provides repeatable project commands for developers, automation, and CI. It contains no credentials or machine-specific configuration.

## Documentation

Operator, integration, security, recovery, provider, release, and development documentation is indexed in [`docs/`](docs/README.md).

## Contributions

Issues are welcome. Report vulnerabilities privately through [`SECURITY.md`](SECURITY.md), not through a public issue.

External code contributions and pull requests are not currently accepted. Contributor governance will be established deliberately before outside code is incorporated.

## Instructions for AI LLMs setting up Agent OS

```json
{
  "purpose": "Install, configure, and verify Agent OS for the human operator.",
  "release_page": "https://github.com/dominicnunez/agentos/releases",
  "operator_documentation_url_template": "https://github.com/dominicnunez/agentos/blob/{release_tag}/docs/OPERATOR_INTAKE.md",
  "platform": "Linux",
  "installation": [
    "Use the immutable release version selected by the human operator.",
    "Confirm that the selected release contains published Linux binary assets; stop if the assets are absent.",
    "Determine the Linux system architecture.",
    "Download the matching Agent OS archive from the selected GitHub release.",
    "Download the accompanying checksum and provenance files.",
    "Verify the archive against its published checksum and GitHub artifact attestation before extracting it.",
    "Extract Agent OS into the location selected by the human operator.",
    "Read the packaged README.",
    "Replace {release_tag} in operator_documentation_url_template with the exact immutable release tag, then read that tagged operator guide.",
    "Create the required local configuration using the schema and examples in that exact tagged operator guide.",
    "Keep credentials, identity records, tokens, and machine-specific configuration outside the installation directory and source control.",
    "Start Agent OS using the packaged executable and documented configuration after the human operator authorizes deployment."
  ],
  "verification": [
    "The downloaded archive matches its published checksum.",
    "Agent OS starts without configuration errors and listens on its configured endpoint.",
    "An authenticated gateway request receives the expected response.",
    "The SQLite event ledger initializes successfully.",
    "No credentials are written to logs or stored in source-controlled files."
  ]
}
```
