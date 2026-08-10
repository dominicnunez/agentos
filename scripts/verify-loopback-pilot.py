#!/usr/bin/env python3
"""Exercise restart-safe Human/A2A work against a disposable recovered ledger."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path


def request_json_response(
    url: str, token: str, body: dict | None = None
) -> tuple[dict, dict[str, str]]:
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
        return json.loads(response.read().decode("utf-8")), dict(response.headers.items())


def request_json(url: str, token: str, body: dict | None = None) -> dict:
    payload, _ = request_json_response(url, token, body)
    return payload


def expect_http_error(
    url: str, token: str, expected_status: int, body: dict | None = None
) -> None:
    try:
        request_json(url, token, body)
    except urllib.error.HTTPError as error:
        if error.code == expected_status:
            return
        raise RuntimeError(
            f"expected HTTP {expected_status}, received HTTP {error.code}"
        ) from error
    raise RuntimeError(f"request unexpectedly succeeded; expected HTTP {expected_status}")


def expect_unauthorized(url: str, token: str, body: dict) -> None:
    expect_http_error(url, token, 401, body)


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


def review_url(url: str, task_id: str) -> str:
    return url.rstrip("/") + "/v1/human/reviews/" + task_id


def valid_review(review: dict, task_id: str, objective: str, candidate: str) -> bool:
    fingerprint = review.get("fingerprint", "")
    return (
        review.get("task_id") == task_id
        and review.get("objective") == objective
        and review.get("candidate_result") == candidate
        and review.get("state") == "PENDING"
        and len(review.get("evidence_refs", [])) == 3
        and len(fingerprint) == 64
        and all(character in "0123456789abcdef" for character in fingerprint)
    )


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

    review_objective = "draft a release candidate note"
    pending_review = human_send(
        args.url, args.human_token, "pilot-human-review", "pilot-human-message-3",
        review_objective,
    )
    if pending_review.get("state") != "INPUT_REQUIRED" or pending_review.get("result"):
        raise RuntimeError(f"unverified fake-review result escaped before review: {pending_review}")
    pending_url = review_url(args.url, pending_review["task_id"])
    expect_http_error(pending_url, args.human_token, 403)
    expect_http_error(pending_url, args.agent_token, 401)
    expect_http_error(
        args.url.rstrip("/") + "/v1/human/messages",
        args.reviewer_token,
        403,
        {
            "conversation_id": "reviewer-cannot-submit",
            "message_id": "reviewer-message-1",
            "text": "attempt ordinary work",
        },
    )
    review, headers = request_json_response(pending_url, args.reviewer_token)
    if not valid_review(
        review,
        pending_review["task_id"],
        review_objective,
        "fake-review-model: " + review_objective,
    ):
        raise RuntimeError(f"pending completion review is invalid: {review}")
    if headers.get("Cache-Control") != "no-store":
        raise RuntimeError("completion review response may be cached")

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
            "review_task": pending_review["task_id"],
            "review_id": review["review_id"],
            "review_fingerprint": review["fingerprint"],
            "review_objective": review_objective,
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

    pending_url = review_url(args.url, state["review_task"])
    review, headers = request_json_response(pending_url, args.reviewer_token)
    if not valid_review(
        review,
        state["review_task"],
        state["review_objective"],
        "fake-review-model: " + state["review_objective"],
    ) or review.get("review_id") != state["review_id"] or review.get("fingerprint") != state["review_fingerprint"]:
        raise RuntimeError(f"completion review changed across restore: {review}")
    if headers.get("Cache-Control") != "no-store":
        raise RuntimeError("restored completion review response may be cached")
    stale = {
        "review_id": state["review_id"],
        "fingerprint": "0" * 64,
        "decision": "APPROVE",
    }
    expect_http_error(pending_url, args.reviewer_token, 409, stale)
    decision = {
        "review_id": state["review_id"],
        "fingerprint": state["review_fingerprint"],
        "decision": "APPROVE",
    }
    decided, decision_headers = request_json_response(
        pending_url, args.reviewer_token, decision
    )
    if decided.get("state") != "APPROVE" or decision_headers.get("Cache-Control") != "no-store":
        raise RuntimeError(f"exact completion review failed: {decided}")
    replayed = request_json(pending_url, args.reviewer_token, decision)
    if replayed.get("state") != "APPROVE":
        raise RuntimeError(f"completion review replay was not idempotent: {replayed}")
    reviewed_task = request_json(
        args.url.rstrip("/") + "/v1/human/tasks/" + state["review_task"],
        args.human_token,
    )
    if reviewed_task.get("state") != "COMPLETED" or reviewed_task.get("result") != "fake-review-model: " + state["review_objective"]:
        raise RuntimeError(f"reviewed task did not complete after restore: {reviewed_task}")

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
    parser.add_argument("--reviewer-token", required=True)
    args = parser.parse_args()
    if args.phase == "seed":
        seed(args)
    else:
        recover(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
