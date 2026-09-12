from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.ci_push_history import push_history_baseline


class PushHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / "source"
        self.repo.mkdir()
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "History fixture")
        self.git("config", "user.email", "history@example.invalid")
        self.git("config", "core.autocrlf", "false")
        self.root = self.commit("root")

    def git(self, *args: str, repo: Path | None = None) -> str:
        return subprocess.run(
            ["git", "-C", str(repo or self.repo), *args],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    def commit(self, value: str) -> str:
        (self.repo / "record").write_text(value + "\n", encoding="utf-8")
        self.git("add", "record")
        self.git("commit", "-qm", value)
        return self.git("rev-parse", "HEAD")

    def test_fast_forward_keeps_previous_head(self) -> None:
        before = self.commit("before")
        head = self.commit("head")
        self.assertEqual(push_history_baseline(self.repo, before, head), before)

    def test_rebase_checks_all_new_lineage(self) -> None:
        before = self.commit("old branch")
        self.git("checkout", "-q", "-B", "main", self.root)
        intermediate = self.commit("new base")
        head = self.commit("rebased branch")
        baseline = push_history_baseline(self.repo, before, head)
        self.assertEqual(baseline, self.root)
        self.assertEqual(
            self.git("rev-list", "--reverse", head, "--not", baseline).splitlines(),
            [intermediate, head],
        )

    def test_replaced_head_must_be_fetched_before_resolution(self) -> None:
        before = self.commit("old branch")
        self.git("checkout", "-q", "-B", "main", self.root)
        head = self.commit("replacement")
        bare = Path(self.temp.name) / "remote.git"
        clone = Path(self.temp.name) / "checkout"
        self.git("clone", "-q", "--bare", "--no-local", str(self.repo), str(bare))
        # Retain the old object as GitHub does for a recent push, without
        # advertising a ref that would make a full clone fetch it automatically.
        self.git("fetch", "-q", str(self.repo), before, repo=bare)
        self.git("config", "uploadpack.allowAnySHA1InWant", "true", repo=bare)
        self.git("clone", "-q", "--no-local", str(bare), str(clone))
        with self.assertRaises(subprocess.CalledProcessError):
            push_history_baseline(clone, before, head)
        self.git("fetch", "-q", "origin", before, repo=clone)
        self.assertEqual(push_history_baseline(clone, before, head), self.root)

    def test_rewind_still_checks_target_commit(self) -> None:
        head = self.commit("rewind target")
        before = self.commit("old tip")
        self.assertEqual(push_history_baseline(self.repo, before, head), self.root)
        self.assertEqual(push_history_baseline(self.repo, head, head), self.root)

    def test_ambiguous_merge_bases_fail(self) -> None:
        left = self.commit("left")
        self.git("checkout", "-q", "-B", "main", self.root)
        right = self.commit("right")
        tree = self.git("rev-parse", "HEAD^{tree}")
        before = self.git("commit-tree", tree, "-p", left, "-p", right, "-m", "first merge")
        head = self.git("commit-tree", tree, "-p", right, "-p", left, "-m", "second merge")
        with self.assertRaisesRegex(ValueError, "unambiguous"):
            push_history_baseline(self.repo, before, head)

    def test_missing_unrelated_and_invalid_history_fail(self) -> None:
        head = self.commit("head")
        with self.assertRaises(subprocess.CalledProcessError):
            push_history_baseline(self.repo, "f" * 40, head)
        for invalid in ("0" * 40, "main", "--all", head.upper()):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                push_history_baseline(self.repo, invalid, head)
        tree = self.git("rev-parse", "HEAD^{tree}")
        with self.assertRaises(ValueError):
            push_history_baseline(self.repo, tree, head)
        self.git("checkout", "-q", "--orphan", "unrelated")
        unrelated = self.commit("unrelated root")
        with self.assertRaises(subprocess.CalledProcessError):
            push_history_baseline(self.repo, unrelated, head)
        with self.assertRaises(ValueError):
            push_history_baseline(self.repo, head, self.root)


if __name__ == "__main__":
    unittest.main()
