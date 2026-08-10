#!/usr/bin/env python3
"""Exercise restart-safe Human/A2A work against a disposable recovered ledger."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


def request_json(url: str, token: str, body: dict | None = None) -> dict:
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    data = None
    method = "GET"
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
        method = "POST"
    with urllib.request.urlopen(
        urllib.request.Request(url, data=data, headers=headers, method=method), timeout=30
    ) as response:
        return json.loads(response.read().decode("utf-8"))


def expect_unauthorized(url: str, token: str, body: dict) -> None:
    try:
        request_json(url, token, body)
    except urllib.error.HTTPError as error:
        if error.code == 401:
            return
        raise RuntimeError(f"credential rejection returned HTTP {error.code}") from error
    raise RuntimeError("inactive credential was accepted")


def a2a_send(url: str, token: str, rpc_id: str, message_id: str, context_id: str, text: str, kind: str = "") -> dict:
    message: dict = {
        "messageId": message_id,
        "contextId": context_id,
        "role": "ROLE_USER",
        "parts": [{"text": text, "mediaType": "text/plain"}],
    }
    if kind:
        message["metadata"] = {"agentos.execution_kind": kind}
    envelope = request_json(
        url.rstrip("/") + "/",
        token,
        {
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": "SendMessage",
            "params": {"message": message},
        },
    )
    if envelope.get("error"):
        raise RuntimeError(f"A2A SendMessage failed: {envelope}")
    return envelope["result"]["task"]


def a2a_get(url: str, token: str, rpc_id: str, task_id: str) -> dict:
    envelope = request_json(
        url.rstrip("/") + "/",
        token,
        {"jsonrpc": "2.0", "id": rpc_id, "method": "GetTask", "params": {"id": task_id}},
    )
    if envelope.get("error"):
        raise RuntimeError(f"A2A GetTask failed: {envelope}")
    return envelope["result"]


def task_text(task: dict) -> str:
    return "\n".join(
        part.get("text", "")
        for artifact in task.get("artifacts", [])
        for part in artifact.get("parts", [])
    )


def human_send(url: str, token: str, conversation_id: str, message_id: str, text: str, kind: str = "") -> dict:
    body = {"conversation_id": conversation_id, "message_id": message_id, "text": text}
    if kind:
        body["execution_kind"] = kind
    return request_json(url.rstrip("/") + "/v1/human/messages", token, body)


def write_state(path: Path, state: dict) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        json.dump(state, output, sort_keys=True)


def seed(args: argparse.Namespace) -> None:
    deterministic = a2a_send(
        args.url, args.agent_token, "pilot-rpc-1", "pilot-agent-message-1",
        "pilot-agent-completed", "echo durable-agent-result",
    )
    if deterministic.get("status", {}).get("state") != "TASK_STATE_COMPLETED" or task_text(deterministic) != "durable-agent-result":
        raise RuntimeError(f"seed A2A result is invalid: {deterministic}")
    blocked_agent = a2a_send(
        args.url, args.agent_token, "pilot-rpc-2", "pilot-agent-message-2",
        "pilot-agent-blocked", "request operator input", "HUMAN",
    )
    if blocked_agent.get("status", {}).get("state") != "TASK_STATE_INPUT_REQUIRED":
        raise RuntimeError(f"seed A2A task did not block: {blocked_agent}")

    completed_human = human_send(
        args.url, args.human_token, "pilot-human-completed", "pilot-human-message-1",
        "echo durable-human-result",
    )
    if completed_human.get("state") != "COMPLETED" or completed_human.get("result") != "durable-human-result":
        raise RuntimeError(f"seed Human result is invalid: {completed_human}")
    blocked_human = human_send(
        args.url, args.human_token, "pilot-human-blocked", "pilot-human-message-2",
        "request human input", "HUMAN",
    )
    if blocked_human.get("state") != "INPUT_REQUIRED":
        raise RuntimeError(f"seed Human task did not block: {blocked_human}")

    revocable = a2a_send(
        args.url, args.revoked_token, "pilot-rpc-3", "pilot-revocable-message-1",
        "pilot-revocable", "echo pre-revocation",
    )
    if revocable.get("status", {}).get("state") != "TASK_STATE_COMPLETED":
        raise RuntimeError(f"revocable credential was not active during seed: {revocable}")
    expect_unauthorized(
        args.url.rstrip("/") + "/", args.expired_token,
        {"jsonrpc": "2.0", "id": "pilot-expired", "method": "GetTask", "params": {"id": deterministic["id"]}},
    )

    write_state(
        args.state,
        {
            "agent_completed_task": deterministic["id"],
            "agent_blocked_task": blocked_agent["id"],
            "human_completed_task": completed_human["task_id"],
            "human_blocked_task": blocked_human["task_id"],
        },
    )


def recover(args: argparse.Namespace) -> None:
    state = json.loads(args.state.read_text(encoding="utf-8"))
    deterministic = a2a_get(args.url, args.reader_token, "pilot-rpc-4", state["agent_completed_task"])
    if deterministic.get("status", {}).get("state") != "TASK_STATE_COMPLETED" or task_text(deterministic) != "durable-agent-result":
        raise RuntimeError(f"recovered A2A result is invalid: {deterministic}")
    completed_human = request_json(
        args.url.rstrip("/") + "/v1/human/tasks/" + state["human_completed_task"], args.human_token
    )
    if completed_human.get("state") != "COMPLETED" or completed_human.get("result") != "durable-human-result":
        raise RuntimeError(f"recovered Human result is invalid: {completed_human}")

    continued_agent = a2a_send(
        args.url, args.agent_token, "pilot-rpc-5", "pilot-agent-message-3",
        "pilot-agent-blocked", "authorized continuation after restore",
    )
    if continued_agent.get("id") != state["agent_blocked_task"] or continued_agent.get("status", {}).get("state") != "TASK_STATE_COMPLETED":
        raise RuntimeError(f"A2A continuation failed after restore: {continued_agent}")
    continued_human = human_send(
        args.url, args.human_token, "pilot-human-blocked", "pilot-human-message-3",
        "authorized human continuation after restore",
    )
    if continued_human.get("task_id") != state["human_blocked_task"] or continued_human.get("state") != "COMPLETED":
        raise RuntimeError(f"Human continuation failed after restore: {continued_human}")

    probe = {"jsonrpc": "2.0", "id": "pilot-revoked", "method": "GetTask", "params": {"id": state["agent_completed_task"]}}
    expect_unauthorized(args.url.rstrip("/") + "/", args.revoked_token, probe)
    expect_unauthorized(args.url.rstrip("/") + "/", args.expired_token, probe)
    print(json.dumps({"status": "PASS", "recovered": state}, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("seed", "recover"))
    parser.add_argument("--url", required=True)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--agent-token", required=True)
    parser.add_argument("--reader-token", required=True)
    parser.add_argument("--revoked-token", required=True)
    parser.add_argument("--expired-token", required=True)
    parser.add_argument("--human-token", required=True)
    args = parser.parse_args()
    if args.phase == "seed":
        seed(args)
    else:
        recover(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
