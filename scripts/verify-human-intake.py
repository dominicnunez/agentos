#!/usr/bin/env python3
"""Exercise the live first-party human natural-language intake boundary."""

from __future__ import annotations

import argparse
import json
import urllib.request


def request_json(request: urllib.request.Request) -> dict:
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()

    body = json.dumps(
        {
            "conversation_id": "human-live-interop",
            "message_id": "human-message-1",
            "text": "draft a concise human operator update",
        }
    ).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {args.token}",
        "Content-Type": "application/json",
    }
    submitted = request_json(
        urllib.request.Request(
            args.url.rstrip("/") + "/v1/human/messages",
            data=body,
            headers=headers,
            method="POST",
        )
    )
    if submitted.get("state") != "COMPLETED" or submitted.get("result") != "fake-model: draft a concise human operator update":
        raise RuntimeError(f"direct human intake returned an unexpected result: {submitted}")

    status = request_json(
        urllib.request.Request(
            args.url.rstrip("/") + "/v1/human/tasks/" + submitted["task_id"],
            headers={"Authorization": f"Bearer {args.token}"},
            method="GET",
        )
    )
    if status != submitted:
        raise RuntimeError(f"direct human status did not reproduce the submitted view: {status}")

    print(json.dumps(submitted, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
