#!/usr/bin/env python3
"""Run the pinned Hermes v0.20.0 A2A client against Agent OS."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hermes-root", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()

    hermes_root = Path(args.hermes_root).resolve()
    sys.path.insert(0, str(hermes_root))
    from plugins.platforms.a2a import tools  # pylint: disable=import-outside-toplevel

    discovery = tools.a2a_discover({"url": args.url})
    if "JSONRPC v1.0" not in discovery or "Agent OS Operator Gateway" not in discovery:
        raise RuntimeError(f"Hermes discovery did not select A2A v1.0 JSON-RPC:\n{discovery}")

    # Use Hermes's public a2a_call handler with an isolated in-memory peer
    # configuration so the real bearer-auth and discovery paths are exercised.
    tools._load_config = lambda: {  # noqa: SLF001 - intentional pinned-client fixture
        "a2a_agents": {
            "agentos": {
                "url": args.url,
                "auth": {"type": "bearer", "token": args.token},
                "timeout": 30,
            }
        }
    }
    reply = tools.a2a_call({"agent": "agentos", "message": "echo hello-from-hermes"})
    if "hello-from-hermes" not in reply or "completed" not in reply:
        raise RuntimeError(f"Hermes SendMessage did not receive the completed result:\n{reply}")

    natural_reply = tools.a2a_call(
        {"agent": "agentos", "message": "draft a concise operator update"}
    )
    if "fake-model: draft a concise operator update" not in natural_reply or "completed" not in natural_reply:
        raise RuntimeError(
            "Hermes natural-language work did not traverse Agent OS intake:\n"
            f"{natural_reply}"
        )

    print(discovery)
    print(reply)
    print(natural_reply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
