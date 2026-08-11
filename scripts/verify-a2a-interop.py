#!/usr/bin/env python3
"""Exercise the public A2A v1.0 boundary as a generic external Agent."""

from __future__ import annotations

import argparse
import json
from urllib import request

EXECUTION_KIND_URI = "https://github.com/dominicnunez/agentos-a2a-go/blob/main/spec/execution-kind-v1.md"


def load_json(url: str, token: str | None = None, body: dict | None = None) -> dict:
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with request.urlopen(request.Request(url, data=data, headers=headers), timeout=30) as response:
        return json.load(response)


def send_message(
    url: str,
    token: str,
    rpc_id: str,
    message_id: str,
    text: str,
    *,
    context_id: str | None = None,
    task_id: str | None = None,
    execution_kind: str | None = None,
) -> dict:
    message = {
        "messageId": message_id,
        "role": "ROLE_USER",
        "parts": [{"text": text, "mediaType": "text/plain"}],
    }
    if context_id:
        message["contextId"] = context_id
    if task_id:
        message["taskId"] = task_id
    if execution_kind:
        message["extensions"] = [EXECUTION_KIND_URI]
        message["metadata"] = {EXECUTION_KIND_URI: {"kind": execution_kind}}
    response = load_json(
        url.rstrip("/") + "/",
        token,
        {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": "SendMessage",
            "params": {
                "message": message
            },
        },
    )
    if response.get("error"):
        raise RuntimeError(f"A2A SendMessage failed: {response}")
    return response["result"]["task"]


def result_text(task: dict) -> str:
    return "\n".join(
        part.get("text", "")
        for artifact in task.get("artifacts", [])
        for part in artifact.get("parts", [])
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--token", required=True)
    args = parser.parse_args()

    card = load_json(args.url.rstrip("/") + "/.well-known/agent-card.json")
    interfaces = card.get("supportedInterfaces", [])
    if card.get("name") != "Agent OS Operator Gateway" or not any(
        interface.get("protocolBinding") == "JSONRPC"
        and interface.get("protocolVersion") == "1.0"
        for interface in interfaces
    ):
        raise RuntimeError(f"Agent Card does not advertise A2A v1.0 JSON-RPC: {card}")
    extensions = card.get("capabilities", {}).get("extensions", [])
    if not any(extension.get("uri") == EXECUTION_KIND_URI for extension in extensions):
        raise RuntimeError(f"Agent Card does not advertise execution-kind v1: {card}")

    deterministic = send_message(
        args.url,
        args.token,
        "rpc-1",
        "message-1",
        "echo hello-from-agent",
        execution_kind="DETERMINISTIC",
    )
    if deterministic.get("status", {}).get("state") != "TASK_STATE_COMPLETED" or "hello-from-agent" not in result_text(deterministic):
        raise RuntimeError(f"deterministic A2A work did not complete: {deterministic}")

    adaptive = send_message(
        args.url,
        args.token,
        "rpc-2",
        "message-2",
        "draft a concise operator update",
    )
    if adaptive.get("status", {}).get("state") != "TASK_STATE_COMPLETED" or "fake-model: draft a concise operator update" not in result_text(adaptive):
        raise RuntimeError(f"natural-language A2A work did not traverse Agent OS intake: {adaptive}")

    print(json.dumps({"agent_card": card, "deterministic": deterministic, "adaptive": adaptive}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
