# Five-minute governed workflow

This walkthrough creates durable organizational direction and completes one
reviewed task through the local dashboard. It uses the built-in deterministic
echo grammar, so Agent OS preserves the instruction literally and does not call
a model, shell, or external service for either normalization or execution. The
runtime does invoke its internal `builtin.echo` handler and records the result
as a structured `ToolOutcome`. The configured provider remains required for
ordinary natural-language work.

Agent OS releases are Linux-only. This user-mode walkthrough requires systemd
256 or newer for encrypted service credentials. Check before setup:

```sh
systemctl --version
```

If the first line reports a version below 256, use the system-mode installation
described in the [operator guide](OPERATOR_INTAKE.md) instead of this
walkthrough. If the
[release page](https://github.com/dominicnunez/agentos/releases) has no approved
binary release, there is no supported public installation to use for this
walkthrough yet.

## 1. Install and initialize

Download, verify, and extract the matching approved release archive as described
in the [README](../README.md#install-agent-os). Install only the two binaries at
this stage; do not run the README's final bare `agentos` command, because that
starts system-mode setup:

```sh
sudo install -m 0755 agentos /usr/local/bin/agentos
sudo install -m 0755 agentos-recovery /usr/local/bin/agentos-recovery
```

Initialize current-user mode explicitly:

```sh
agentos init --user
```

Complete the resumable setup and its provider connection check. At **Service:**,
select **Enable and start** or **Start once**; do not select **Leave stopped**
for this walkthrough. Do not paste a provider secret into a shell command,
`.env` file, repository, task, or chat; enter it only at Agent OS's private
setup prompt.

Verify the installed state:

```sh
agentos doctor
```

Continue only when every required check reports `PASS`. An `INFO` provider
result means the command performed configuration-only checks; setup already
performed the required provider metadata check.

## 2. Open the organization

Run:

```sh
agentos
```

The command opens the embedded dashboard through a short-lived loopback bridge.
The user gateway itself remains on the private Unix socket and is not exposed as
a network service.

Open **Organization** and create this initial direction:

- Mission: `Operate a governed artificial organization`
- Goal: `Complete a verified first task`
- Mode: `Target`
- Success criterion: `The reviewed deterministic task completes with a durable result`

Select **Create Mission and Goal** once. Initial strategy is a durable one-time
transition; refreshing or restarting cannot create a duplicate.

## 3. Submit reviewed work

Open **Work** and enter:

```text
echo Agent OS completed reviewed work
```

Select the Goal created above. Set **Execution** to **Deterministic handler** and
select **Send**.

Agent OS presents an Intent contract before work exists. Confirm that it retains
the selected Goal, requests deterministic execution, and has no unexpected
requirements or consequence candidates. Select **Confirm exact Intent** only if
the contract matches what you submitted.

## 4. Verify the result

The task should finish with:

```text
Agent OS completed reviewed work
```

Return to **Organization** and verify the durable chain:

```text
Mission → Goal → Work → Task
```

Refresh the dashboard or close and reopen `agentos`; the same organization and
completed result should remain available from SQLite.

This walkthrough demonstrates review-before-work and least-nondeterministic
routing. It intentionally requests no consequential effect, so no approval is
created. Financial, destructive, public, privilege-expanding, sensitive-data,
legal, physical-world, deployment, and trusted-core effects remain separately
approval-gated and fail closed.

## Verification evidence

CI exercises the same dashboard bridge, kernel-authenticated user gateway,
durable strategy creation, Intent confirmation, Work/Task admission, execution,
result recovery, and organization projection in
[`TestDashboardCompletesDurableAgentWorkThroughKernelAuthenticatedGateway`](../cmd/agentos/dashboard_loop_linux_test.go).
The CI adapter is non-network test infrastructure and cannot be configured by a
production installation.
