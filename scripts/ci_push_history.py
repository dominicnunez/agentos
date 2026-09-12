"""Select an ancestor boundary for an existing branch's push verification."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


def push_history_baseline(repo: Path, before: str, head: str) -> str:
    """Include all current history after the previous and new heads diverged.

    Acquisition belongs to the authenticated checkout steps. Missing objects,
    unrelated histories, and ambiguous merge bases must not reduce coverage.
    """
    for revision in (before, head):
        if not re.fullmatch(r"[0-9a-f]{40}", revision) or revision == "0" * 40:
            raise ValueError("push history requires exact nonzero commit SHAs")

    def git(*args: str) -> list[str]:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True, capture_output=True, text=True, timeout=30,
        )
        return result.stdout.split()

    # Reject tags/tree objects even if Git could peel them for merge-base.
    for revision in (before, head):
        if git("cat-file", "-t", revision) != ["commit"]:
            raise ValueError("push history references must be commit objects")
    bases = git("merge-base", "--all", before, head)
    if len(bases) != 1:
        raise ValueError("push history requires one unambiguous common ancestor")
    baseline = bases[0]
    if baseline == head:
        # A rewind introduces no commits. Still verify the assessed snapshot
        # and its incoming transitions, as for a tag of an existing commit.
        parents = git("rev-list", "--parents", "-n", "1", head)
        if len(parents) < 2:
            raise ValueError("push history cannot assess a root commit")
        baseline = parents[1]
    return baseline


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            raise ValueError("usage: ci_push_history.py BEFORE HEAD")
        print(push_history_baseline(Path.cwd(), sys.argv[1], sys.argv[2]))
    except (ValueError, subprocess.SubprocessError) as exc:
        print(f"Push history resolution failed: {exc}", file=sys.stderr)
        sys.exit(1)
