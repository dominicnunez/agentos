"""Decide whether a complete Git change needs code checks; uncertainty runs them."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess


ROOT_DOCS = {b"AGENTS.md", b"CHANGELOG.md", b"CONTRIBUTING.md", b"README.md", b"SECURITY.md"}
DOC_SUFFIXES = (b".md", b".txt", b".png", b".jpg", b".jpeg", b".gif", b".webp", b".svg", b".pdf")


def is_document(path: bytes) -> bool:
    if path in ROOT_DOCS:
        return True
    if path.startswith(b"docs/") and path.endswith(DOC_SUFFIXES):
        return True
    return path.startswith(b"governance/aims/") and path.endswith(b".md")


def code_checks_needed(repo: Path, event_name: str, event: dict, head: str, ref: str) -> bool:
    def git(*args: str) -> bytes:
        return subprocess.run(
            ["git", "-C", str(repo), *args], check=True, capture_output=True, timeout=30,
        ).stdout

    def commit(value: str) -> str:
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
            raise ValueError("exact commit required")
        if git("cat-file", "-t", value).strip() != b"commit":
            raise ValueError("commit object required")
        return value

    try:
        head = commit(head)
        if git("rev-parse", "HEAD").strip().decode("ascii") != head:
            return True
        if event_name == "pull_request":
            base = commit(event["pull_request"]["base"]["sha"])
        elif event_name == "push" and ref.startswith("refs/heads/"):
            if event["after"] != head or event.get("deleted", False):
                return True
            before = event["before"]
            if before == "0" * 40:
                branch = event["repository"]["default_branch"]
                if not isinstance(branch, str) or not branch:
                    return True
                base_ref = "refs/remotes/origin/" + branch
                git("check-ref-format", base_ref)
                bases = git("merge-base", "--all", base_ref, head).split()
                if len(bases) != 1:
                    return True
                base = commit(bases[0].decode("ascii"))
            else:
                base = commit(before)
        else:
            # Manual runs, tags, and unrecognized events keep full verification.
            return True
        raw = git("diff", "--raw", "-z", "--no-renames", "--no-ext-diff", "--no-textconv", base, head, "--")
        if not raw or not raw.endswith(b"\0"):
            return True
        parts = raw[:-1].split(b"\0")
        if len(parts) % 2:
            return True
        for index in range(0, len(parts), 2):
            metadata, path = parts[index:index + 2]
            # Release source archives require every tracked name to be UTF-8.
            path.decode("utf-8")
            fields = metadata.removeprefix(b":").split()
            if not metadata.startswith(b":") or len(fields) != 5 or fields[4] not in (b"A", b"D", b"M"):
                return True
            # Executable and symlink changes are not ordinary documentation.
            if any(mode not in (b"000000", b"100644") for mode in fields[:2]):
                return True
            if not is_document(path):
                return True
            # The release builder requires this packaged file to exist.
            if path == b"README.md" and fields[1] == b"000000":
                return True
        return False
    except (KeyError, TypeError, ValueError, UnicodeError, OSError, subprocess.SubprocessError):
        return True


def main() -> None:
    try:
        event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
        needed = code_checks_needed(
            Path.cwd(), os.environ.get("GITHUB_EVENT_NAME", ""), event,
            os.environ.get("GITHUB_SHA", ""), os.environ.get("GITHUB_REF", ""),
        )
    except (KeyError, OSError, ValueError):
        needed = True
    value = "true" if needed else "false"
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        output.write(f"code={value}\n")
    print("Code checks required." if needed else "Documentation-only change; code checks skipped.")


if __name__ == "__main__":
    main()
